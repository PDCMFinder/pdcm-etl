from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit, concat, col, concat_ws, collect_list
from pyspark.sql.types import StructType, StructField, StringType, LongType

from etl.jobs.transformation.scoring.calculation_methods.data_calculator import calculate_data_score
from etl.jobs.transformation.scoring.calculation_methods.pdx_metadata_calculator import calculate_pdx_metadata_score

"""
Adds a `scores` column to search_index_df. `scores` is a JSON column containing zero or more model characterization 
scores for each model. A model characterization score is calculated based on the information defined for a 
model characterization definition, which are configured in the model_characterizations.yaml file. 
"""


def add_scores_column(
        search_index_df: DataFrame,
        model_characterizations_conf_df: DataFrame,
        raw_external_resources_df: DataFrame) -> DataFrame:

    spark = SparkSession.builder.getOrCreate()
    schema = StructType([
        StructField('pdcm_model_id', LongType(), False),
        StructField('score_entry', StringType(), False)])

    score_per_model_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema=schema)

    #  Iterate through the different model characterizations to calculate the respective score
    model_characterization_conf_list = [row.asDict() for row in model_characterizations_conf_df.collect()]
    for model_characterization in model_characterization_conf_list:
        calculation_method = model_characterization['calculation_method']
        score_name = model_characterization['score_name']

        if calculation_method == "calculate_pdx_metadata_score":
            molecular_char_score_df = calculate_pdx_metadata_score(search_index_df, raw_external_resources_df)
        elif calculation_method == "calculate_data_score":
            molecular_char_score_df = calculate_data_score(search_index_df)
        else:
            molecular_char_score_df = None

        molecular_char_score_df = molecular_char_score_df.withColumn(
            "score_entry",
            concat(lit("\""), lit(score_name + "\": "), col("score")))
        molecular_char_score_df = molecular_char_score_df.select("pdcm_model_id", "score_entry")
        score_per_model_df = score_per_model_df.union(molecular_char_score_df)

    agg_score_per_model_df = score_per_model_df \
        .groupby("pdcm_model_id") \
        .agg(concat_ws(", ", collect_list(score_per_model_df.score_entry)).alias("scores"))
    agg_score_per_model_df = agg_score_per_model_df.withColumn("scores", concat(lit("{"), col("scores"), lit("}")))
    search_index_df = search_index_df.join(agg_score_per_model_df, on=["pdcm_model_id"], how='left')
    return search_index_df
