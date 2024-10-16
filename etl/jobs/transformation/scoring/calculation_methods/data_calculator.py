from pyspark.sql import DataFrame
from pyspark.sql.functions import col, size, when
from pyspark.sql.types import IntegerType


def calculate_data_score(search_index_df: DataFrame) -> DataFrame:
    # Possible datasets
    all_datasets = ['mutation', 'biomarkers', 'copy number alteration', 'expression', 'patient treatment',
                    'model treatment', 'publication']
    max_number_datasets = len(all_datasets)
    # Data score will be calculated as the percentage of datasets available for each model:
    # score = number of datasets available * 100 / max number of possible datasets
    search_index_df = search_index_df.withColumn("dataset_available_count", size(col("dataset_available")))
    search_index_df = search_index_df.withColumn(
        "dataset_available_count",
        when(search_index_df.dataset_available_count < 0, 0).otherwise(search_index_df.dataset_available_count))
    search_index_df = search_index_df.withColumn("score", (col("dataset_available_count")) * 100 / max_number_datasets)
    search_index_df = search_index_df.withColumn("score", col("score").cast(IntegerType()))
    return search_index_df.select("pdcm_model_id", "score")
