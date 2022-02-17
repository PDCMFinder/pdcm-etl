from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession, Column
from etl.constants import Constants
import sys

from etl.entities_registry import get_columns_by_entity_name


def main(argv):
    spark = SparkSession.builder.getOrCreate()
    db_user = argv[1]
    db_password = argv[2]
    db_host = argv[3]
    db_port = argv[4]
    db_name = argv[5]
    input_parquet_path = argv[6]
    table_name = argv[7]
    output_path = argv[8]

    api_dataframe = spark.read.parquet(input_parquet_path)
    properties = {
        "user": db_user,
        "password": db_password,
        "stringtype": "unspecified",
        "driver": "org.postgresql.Driver",
    }
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    api_dataframe = api_dataframe.drop(Constants.DATA_SOURCE_COLUMN)
    columns = get_columns_by_entity_name(table_name)
    api_dataframe = api_dataframe.select(columns)
    api_dataframe.write.option("truncate", "true").mode("overwrite").jdbc(
        jdbc_url,
        table_name,
        properties=properties,
    )
    spark.sparkContext.emptyRDD().saveAsTextFile(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))