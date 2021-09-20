from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession, Column
import sys


def main(argv):
    spark = SparkSession.builder.getOrCreate()
    db_user = argv[1]
    db_password = argv[2]
    db_host = argv[3]
    db_port = argv[4]
    db_name = argv[5]
    input_parquet_paths = argv[6].split("|")
    table_names = argv[7].split("|")
    output_path = argv[8]

    api_dataframes = [
        spark.read.parquet(input_path) for input_path in input_parquet_paths
    ]
    properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver",
    }
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"

    table_dataframe_map = zip(table_names, api_dataframes)

    for table_name, dataframe in table_dataframe_map:
        dataframe.write.option("truncate", "true").mode("overwrite").jdbc(
            jdbc_url,
            table_name,
            properties=properties,
        )
    spark.sparkContext.emptyRDD().saveAsTextFile(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))