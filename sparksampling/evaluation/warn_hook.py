from pyspark.sql import DataFrame

from sparksampling.evaluation.base_hook import BaseEvaluationHook


class EmptyDetectHook(BaseEvaluationHook):
    def _process(self, df: DataFrame):
        msg = {"is_not_empty": bool(df.take(1))}

        return df, msg


EmptyDetectHook.register_pre_hook_all()
EmptyDetectHook.register_post_hook_all()
