import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from etl.constants import Constants
from etl.jobs.util.id_assigner import add_id
from etl.jobs.util.molecular_characterization_fk_assigner import set_fk_molecular_characterization


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw immunemarkers data
                    [2]: Parquet file path with molecular characterization data
                    [3]: Output file
    """
    raw_immunemarkers_parquet_path = argv[1]
    molecular_characterization_parquet_path = argv[2]

    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    raw_immunemarkers_df = spark.read.parquet(raw_immunemarkers_parquet_path)
    molecular_characterization_df = spark.read.parquet(molecular_characterization_parquet_path)

    immunemarkers_molecular_data_df = transform_immunemarkers_molecular_data(
        raw_immunemarkers_df,
        molecular_characterization_df)

    immunemarkers_molecular_data_df.write.mode("overwrite").parquet(output_path)


def transform_immunemarkers_molecular_data(
        raw_immunemarkers_df: DataFrame,
        molecular_characterization_df: DataFrame) -> DataFrame:
    immunemarkers_df = get_immunemarkers_df(raw_immunemarkers_df)

    immunemarkers_df = set_fk_molecular_characterization(immunemarkers_df, 'immunemarker', molecular_characterization_df)

    immunemarkers_df = immunemarkers_df.withColumnRenamed(Constants.DATA_SOURCE_COLUMN, "data_source")
    immunemarkers_df = add_id(immunemarkers_df, "id")
    return immunemarkers_df


def get_immunemarkers_df(raw_immunemarkers_df: DataFrame) -> DataFrame:
    return raw_immunemarkers_df.select(
        "sample_id",
        "marker_type",
        "marker_name",
        "marker_value",
        "essential_or_additional_details",
        "platform_id",
        Constants.DATA_SOURCE_COLUMN).drop_duplicates()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
