from pyspark.sql import DataFrame
from pyspark.sql.functions import lit

from etl.jobs.transformation.scoring.calculation_methods.generic_metadata_calculator import calculate_model_metadata_score
from etl.jobs.transformation.scoring.weights_per_fields import common_weights, in_vitro_only_weights


def calculate_in_vitro_metadata_score(search_index_df: DataFrame, raw_external_resources_df: DataFrame) -> DataFrame:

    input_df = search_index_df.where("model_type in ('organoid', 'cell line') ")
    
    # For the rest of models, this score is set to zero
    non_in_vitro_df = search_index_df.where("model_type not in ('organoid', 'cell line')").select("pdcm_model_id", lit(0).alias("score"))
    
    # If there is no data to process, return
    if input_df.count() == 0:
        if non_in_vitro_df.count() == 0:
            return input_df.select("pdcm_model_id").withColumn("score", lit(""))
        else:
            return non_in_vitro_df
    
    column_weights = common_weights.copy()
    column_weights.update(in_vitro_only_weights)
    
    score_df = calculate_model_metadata_score(input_df, raw_external_resources_df, column_weights)
    
    score_df = score_df.union(non_in_vitro_df)

    return score_df
