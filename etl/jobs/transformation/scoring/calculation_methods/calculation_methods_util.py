from pyspark.sql.types import LongType, StructType, StructField, StringType


def get_model_score_schema():
    schema = StructType([
        StructField('pdcm_model_id', LongType(), False),
        StructField('score', StringType(), False)])
    return schema
