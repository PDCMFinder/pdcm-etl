import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col

from etl.jobs.util.cleaner import init_cap_and_trim_all
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    Creates a parquet file with tissue data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw sample data
                    [2]: Output file
    """
    raw_sharing_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    raw_sharing_df = spark.read.parquet(raw_sharing_parquet_path)
    project_group_df = transform_project_group(raw_sharing_df)
    project_group_df.write.mode("overwrite").parquet(output_path)


def transform_project_group(raw_sharing_df: DataFrame) -> DataFrame:
    project_group = get_project_group_from_sharing(raw_sharing_df)
    project_group = project_group.drop_duplicates()
    project_group = add_id(project_group, "id")
    project_group = project_group.select("id", "name")
    return project_group


def get_project_group_from_sharing(raw_sharing_df: DataFrame) -> DataFrame:
    return raw_sharing_df.select(init_cap_and_trim_all("project").alias("name"))


if __name__ == "__main__":
    sys.exit(main(sys.argv))
