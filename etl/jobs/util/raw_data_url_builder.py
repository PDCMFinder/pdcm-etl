import re

from pyspark.sql import DataFrame
from pyspark.sql import Column
from pyspark.sql.functions import col, udf, lit, when
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

import findspark
findspark.init()


def build_raw_data_url(df: DataFrame, col_name) -> DataFrame:
    convert_raw_data_value_to_url_udf = udf(lambda x: convert_raw_data_value_to_url(x), StringType())
    df = df.withColumn(
        col_name,
        when(
            col("raw_data_file").isNotNull(),
            convert_raw_data_value_to_url_udf(df.raw_data_file))
        .otherwise(None))
    return df


def convert_raw_data_value_to_url(raw_data_value: str) -> Column:
    formattedAccesionLink = ""
    ENAprojectRe = "^PRJ[EDN][A-Z][0-9]{0,15}$"
    ENAaccessionRe = "^[EDS]R[SXRP][0-9]{6,}$"
    EGAaccessionRe = "^EGA[A-Za-z0-9]+$"
    GEOaccessionRe = "^GSM[A-Za-z0-9]+$"

    if raw_data_value is None:
        return None

    if re.search(ENAaccessionRe, raw_data_value) or re.search(ENAprojectRe, raw_data_value):
        formattedAccesionLink = f"https://www.ebi.ac.uk/ena/data/view/{raw_data_value}"

    elif re.search(EGAaccessionRe, raw_data_value):
        formattedAccesionLink = f"https://www.ebi.ac.uk/ega/search/site/{raw_data_value}"

    elif re.search(GEOaccessionRe, raw_data_value):
        formattedAccesionLink = f"https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc={raw_data_value}"
    else:
        raw_data_value = ""
    print("formattedAccesionLink ", formattedAccesionLink)
    return lit("") if raw_data_value == "" else lit(f"{raw_data_value},{formattedAccesionLink}")

def test():
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(
        [
            (1, "foo", "EGA123"),  # create your data here, be consistent in the types.
            (2, "bar", None),
        ],
        ["id", "label", "raw_data_file"]  # add your column names here
    )
    df = build_raw_data_url(df, "d")
    df.show()

if __name__ == "__main__":

    test()
