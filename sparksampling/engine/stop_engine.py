from sparksampling.engine.base_engine import SparkBaseEngine
from sparksampling.engine_factory import EngineFactory


class SparkStopEngine(SparkBaseEngine):
    guarantee_worker = 1

    @classmethod
    def stop(cls, parent, job_id=None):
        cls.log.info(f"Send Stop Job to Spark: {job_id}")
        parent.spark.sparkContext.cancelJobGroup(job_id)


EngineFactory.register(SparkStopEngine)
