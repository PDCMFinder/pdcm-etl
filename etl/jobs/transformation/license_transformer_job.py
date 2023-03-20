import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import IntegerType, StructField, StructType, StringType


def main(argv):
    """
    Creates a parquet file with the different possible values for licenses.
    """
    output_path = argv[1]

    license_df = transform_license()
    license_df.write.mode("overwrite").parquet(output_path)


def transform_license() -> DataFrame:

    spark = SparkSession.builder.getOrCreate()

    schema = StructType([
        StructField('id', IntegerType(), False),
        StructField('name', StringType(), False),
        StructField('url', StringType(), False)
    ])

    data = [
        (1, "EMBL-EBI", "https://www.ebi.ac.uk/about/terms-of-use"),
        (2, "CC0", "https://creativecommons.org/publicdomain/zero/1.0/")
    ]

    df = spark.createDataFrame(data=data, schema=schema)
    return df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
