import re

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import StringType


def build_raw_data_url(df: DataFrame, col_name) -> DataFrame:
    convert_raw_data_value_to_url_udf = udf(lambda x: convert_raw_data_value_to_url(x), StringType())
    df = df.withColumn(
        col_name,
        when(
            col("raw_data_url").isNotNull(),
            convert_raw_data_value_to_url_udf(df.raw_data_url))
        .otherwise(None))
    return df


def convert_raw_data_value_to_url(raw_data_value: str):
    result = ""
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

    if formattedAccesionLink != "":
        result = f"{raw_data_value},{formattedAccesionLink}"
    return result

