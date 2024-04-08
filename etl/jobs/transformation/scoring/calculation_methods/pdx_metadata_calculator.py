from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from etl.jobs.transformation.scoring.calculation_methods.generic_metadata_calculator import calculate_model_metadata_score
from etl.jobs.transformation.scoring.weights_per_fields import common_weights, pdx_only_weights


def calculate_pdx_metadata_score(search_index_df: DataFrame, raw_external_resources_df: DataFrame) -> DataFrame:

    input_df = search_index_df.where("model_type = 'PDX'")
    
      # For models which are not PDX, this score is set to zero
    non_pdx_df = search_index_df.where("model_type != 'PDX'").select("pdcm_model_id", lit(0).alias("score"))
    
    # If there is no data to process, return
    if input_df.count() == 0:
        if non_pdx_df.count() == 0:
            return input_df.select("pdcm_model_id").withColumn("score", lit(""))
        else:
            return non_pdx_df
    
    column_weights = common_weights.copy()
    column_weights.update(pdx_only_weights)
    
    # def calculate_metadata_score(input_df: DataFrame, raw_external_resources_df: DataFrame, column_weights: dict) -> DataFrame:
    score_df = calculate_model_metadata_score(input_df, raw_external_resources_df, column_weights)
    
    score_df = score_df.union(non_pdx_df)

    return score_df
