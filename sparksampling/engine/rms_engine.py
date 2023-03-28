from concurrent.futures import ThreadPoolExecutor
from graphlib import CycleError, TopologicalSorter
from pprint import pformat
from typing import Dict, List

from sparksampling.engine.base_engine import SparkBaseEngine
from sparksampling.engine_factory import EngineFactory
from sparksampling.error import BadParamError
from sparksampling.file_format import FileFormatFactory
from sparksampling.mixin import LogMixin, acquire_worker, record_job_id
from sparksampling.proto.sampling_service_pb2 import RelationSamplingRequest
from sparksampling.sample import SamplingFactory
from sparksampling.utilities import check_spark_session


def _check_param(conf):
    def check_duplicated_name(name):
        if name not in name_set:
            name_set.add(name)
        else:
            raise BadParamError(f"Duplicated name: {name}")

    name_set = set()
    for sampling_stage in conf.get("sampling_stages", []):
        check_duplicated_name(sampling_stage["name"])
        if not (sampling_stage.get("input_path") or sampling_stage.get("input_name")):
            raise BadParamError(
                f"Need at least one sampling source(input_path or input_name) for relation {pformat(sampling_stage)}"
            )

    for relation_stage in conf.get("relation_stage", []):
        # check duplicate name
        check_duplicated_name(relation_stage["name"])
        for relation in relation_stage.get("relations", []):
            if not (relation.get("relation_path") or relation.get("relation_name")):
                raise BadParamError(
                    f"Need at least one relation source(relation_path or relation_name) for relation {pformat(relation_stage)}"
                )
            if not (relation.get("relation_col") or relation.get("input_col")):
                raise BadParamError(
                    f"Need at least one col(relation_col or input_col) for relation {pformat(relation_stage)}"
                )


def _set_default(conf):
    # allow empty stage for export or sampling only job
    conf.setdefault("sampling_stages", [])
    conf.setdefault("relation_stages", [])

    default_sampling_method = conf.get("default_sampling_method")
    default_sampling_conf = conf.get("default_sampling_conf")
    default_file_format = conf.get("default_file_format")
    default_format_conf = conf.get("default_format_conf")
    for sampling_stage in conf["sampling_stages"]:
        sampling_stage.setdefault("sampling_method", default_sampling_method)
        sampling_stage.setdefault("sampling_conf", default_sampling_conf)
        sampling_stage.setdefault("file_format", default_file_format)
        sampling_stage.setdefault("format_conf", default_format_conf)
    for relation_stage in conf["relation_stages"]:
        relation_stage.setdefault("file_format", default_file_format)
        relation_stage.setdefault("format_conf", default_format_conf)
        for relation in relation_stage.get("relations", []):
            if not relation.get("relation_col"):
                relation["relation_col"] = relation["input_col"]
            if not relation.get("input_col"):
                relation["input_col"] = relation["relation_col"]
            relation.setdefault("file_format", default_file_format)
            relation.setdefault("format_conf", default_format_conf)
    return conf


class JobStage(LogMixin):
    def __init__(self, spark, stage_config, name_df_map):
        self.spark = spark
        self.stage_config = stage_config
        self.name_df_map = name_df_map

        self._df = None
        self.sampled_path = None
        self.file_format_imp = None
        self.sample_imp = None
        self.pre_metas = []
        self.post_metas = []

        self._init_sample_imp()
        self._init_file_format()

    def _init_file_format(self):
        self.file_format_imp = FileFormatFactory.get_file_imp(
            self.spark, self.file_format, self.format_conf
        )

    def _init_sample_imp(self):
        if not (self.sampling_method and self.sampling_conf):
            return
        self.sample_imp = SamplingFactory.get_sampling_imp(self.sampling_method, self.sampling_conf)

    def _build_relation_dataframe(self, input_df, relation):
        relation_name = relation.get("relation_name")
        if relation_name:
            relation_df = self.name_df_map[relation_name]
        else:
            # using relation_path to construct dataframe
            relation_file_format = FileFormatFactory.get_file_imp(
                self.spark, relation["file_format"], relation["format_conf"]
            )
            # no need to worry if spark read the same csv twice, spark only read once
            relation_df = relation_file_format.read(relation["relation_path"])

        # python's way for self._df.join(relation_df, self._df.input_col == relation_df.relation_col, 'semi')
        cond = [
            getattr(input_df, relation["input_col"])
            == getattr(relation_df, relation["relation_col"])
        ]
        return input_df.join(relation_df, cond, how="semi")

    def build_dataframe(self, pre_hook=None, post_hook=None):
        self.log.debug(f"Starting building stage {self.name}")
        if self.input_name:
            self._df = self.name_df_map[self.input_name]
        else:
            self._df = self.file_format_imp.read(self.input_path)

        if pre_hook:
            self._df, self.pre_metas = pre_hook(self._df)

        if self.sample_imp:
            self._df = self.sample_imp.run(self._df)

        for relation in self.relations:
            self._df = self._build_relation_dataframe(self._df, relation)

        if post_hook:
            self._df, self.post_metas = post_hook(self._df)

        self.name_df_map[self.name] = self._df

    @property
    def relations(self):
        return self.stage_config.get("relations", [])

    @property
    def name(self):
        return self.stage_config.get("name")

    @property
    def sampling_method(self):
        return self.stage_config.get("sampling_method")

    @property
    def sampling_conf(self):
        return self.stage_config.get("sampling_conf")

    @property
    def file_format(self):
        return self.stage_config.get("file_format")

    @property
    def format_conf(self):
        return self.stage_config.get("format_conf")

    @property
    def output_path(self):
        return self.stage_config.get("output_path")

    @property
    def output_col(self):
        return self.stage_config.get("output_col")

    @property
    def input_name(self):
        return self.stage_config.get("input_name")

    @property
    def input_path(self):
        return self.stage_config.get("input_path")

    def export_dataframe(self):
        if self.output_path:
            self.log.debug(f"Exporting stage {self.name} to {self.output_path}")
            self.sampled_path = self.file_format_imp.write(
                self._df, self.output_path, self.output_col
            )
        else:
            self.log.debug(f"Nothing to export for {self.name}")


class RMSEngine(SparkBaseEngine):
    # Relationship Mapping Sampling
    def __init__(
        self,
        parent,
        sampling_stages: List[Dict],
        relation_stages: List[Dict],
        job_id: str,
        dry_run: bool,
    ):
        super(RMSEngine, self).__init__()
        self.parent = parent
        self.sampling_stages = sampling_stages
        self.relation_stages = relation_stages
        self.job_id = job_id
        self.dry_run = dry_run

        self.name_stage_map = dict()
        self._init_name_stage_map()

    def _init_name_stage_map(self):
        for sampling_stage in self.sampling_stages:
            self.name_stage_map[sampling_stage["name"]] = sampling_stage
            self.name_stage_map["type"] = "sample"
        for relation_stages in self.relation_stages:
            self.name_stage_map[relation_stages["name"]] = relation_stages
            self.name_stage_map["type"] = "relation"

    @acquire_worker
    @check_spark_session
    @record_job_id
    def submit(self, *args, **kwargs):
        try:
            build_order = self._get_stage_order()
        except CycleError as e:
            raise BadParamError(f"Loop detected: {str(e)}")

        spark_job_seq = self._process_spark_seq(build_order)
        result = self._get_seq_result(spark_job_seq)
        return result

    def _get_stage_order(self):
        topological_map = dict()

        for sampling_stage in self.sampling_stages:
            rely_set = topological_map.setdefault(sampling_stage["name"], set())
            rely_name = sampling_stage.get("input_name")
            if rely_name:
                rely_set.add(rely_name)

        for relation_stage in self.relation_stages:
            rely_set = topological_map.setdefault(relation_stage["name"], set())
            rely_name = relation_stage.get("input_name")
            if rely_name:
                rely_set.add(rely_name)
            for relation in relation_stage.get("relations", []):
                relation_name = relation.get("relation_name")
                if relation_name:
                    rely_set.add(relation_name)
        self.log.debug(f"topological_map is {pformat(topological_map)}")
        build_order = list(TopologicalSorter(topological_map).static_order())
        self.log.debug(f"topological sort result: {build_order}")
        return build_order

    def _process_spark_seq(self, build_order):
        job_seq = []
        name_df_map = dict()
        for name in build_order:
            stage = JobStage(self.spark, self.name_stage_map[name], name_df_map)
            stage.build_dataframe(self.pre_hook, self.post_hook)
            job_seq.append(stage)
        if self.dry_run:
            self.log.info("Dry run, skip export dataframe")
            return
        self.log.info("Generated spark job sequence, start exporting dataframe...")

        executors = int(self.spark.conf.get("spark.executor.instances", "1"))
        with ThreadPoolExecutor(max_workers=executors) as executor:
            for stage in job_seq:
                executor.submit(stage.export_dataframe)
        return job_seq

    def _get_seq_result(self, seq):
        self.log.debug("Start collecting result...")
        if self.dry_run:
            self.log.info("Dry run, nothing output")
            return []
        result = []
        for job in seq:
            if job.sampled_path:
                result.append(
                    {
                        "name": job.name,
                        "sampled_path": job.sampled_path,
                        "hook_msg": job.pre_metas + job.post_metas,
                    }
                )
        self.log.info(f"result collected: \n{pformat(result)}")
        return result

    @classmethod
    def config(cls, kwargs):
        _check_param(kwargs)
        try:
            kwargs = _set_default(kwargs)
            sampling_stages = kwargs.pop("sampling_stages")
            relation_stages = kwargs.pop("relation_stages")
            job_id = kwargs.pop("job_id")
            dry_run = kwargs.pop("dry_run", False)
        except KeyError as e:
            cls.log.info(f"Missing required parameters {str(e)}")
            raise BadParamError(f"Missing required parameters {str(e)}")

        config_dict = {
            "sampling_stages": sampling_stages,
            "relation_stages": relation_stages,
            "job_id": job_id,
            "dry_run": dry_run,
        }
        cls.log.info(f"Initializing job conf... \n {pformat(config_dict)}")
        return config_dict

    @classmethod
    def is_matching(cls, request_type):
        return request_type == RelationSamplingRequest


EngineFactory.register(RMSEngine)
