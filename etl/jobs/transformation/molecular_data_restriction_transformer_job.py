import sys

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql.functions import col, trim
from pyspark.sql.types import StructType, StructField, MapType, StringType

from etl.constants import Constants
from etl.jobs.util.cleaner import trim_all
from etl.jobs.util.id_assigner import add_id

from ast import literal_eval


def main(argv):
    """
    Creates a parquet file with the molecular data tables that have restricted access.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sharing data
                    [2]: Output file
    """
    restricted_tables_by_provider_dic_str = argv[1]
    output_path = argv[2]

    restricted_tables_by_provider_dic = literal_eval(restricted_tables_by_provider_dic_str)

    spark = SparkSession.builder.getOrCreate()
    molecular_data_restriction_df = transform_molecular_data_restriction(restricted_tables_by_provider_dic, spark)
    molecular_data_restriction_df.write.mode("overwrite").parquet(output_path)


def transform_molecular_data_restriction(restricted_tables_by_provider_dic, spark) -> DataFrame:
    data = []
    for key in restricted_tables_by_provider_dic:
        provider = key
        list_tables = restricted_tables_by_provider_dic[key]
        for table in list_tables:
            data.append((provider, table))

    schema = StructType([
        StructField('data_source', StringType(), True),
        StructField('molecular_data_table', StringType(), True)])
    molecular_data_restriction_df = spark.createDataFrame(data=data, schema=schema)
    return molecular_data_restriction_df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
