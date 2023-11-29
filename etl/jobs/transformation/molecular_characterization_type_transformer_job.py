import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with host strain data.
    :param list argv: the list elements should be:
                    [1]: Output file
    """
    output_path = argv[1]

    molecular_characterization_type_df = transform_molecular_characterization_type()
    molecular_characterization_type_df.write.mode("overwrite").parquet(output_path)


def create_dataframe() -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    schema = StructType([StructField("name", StringType(), False)])
    data = [["biomarker"], ["copy number alteration"], ["mutation"], ["expression"], ["immunemarker"]]
    return spark.createDataFrame(data=data, schema=schema)


def transform_molecular_characterization_type() -> DataFrame:
    """ This transformation does nor take data as input, as its values are fixed so we can generate the
    dataframe manually """
    molecular_characterization_type_df = create_dataframe()
    molecular_characterization_type_df = add_id(molecular_characterization_type_df, "id")
    molecular_characterization_type_df = get_columns_expected_order(molecular_characterization_type_df)
    return molecular_characterization_type_df


def get_columns_expected_order(molecular_characterization_type_df: DataFrame) -> DataFrame:
    return molecular_characterization_type_df.select("id", "name")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
