import sys

from pyspark.sql import SparkSession

from etl.constants import Constants
from etl.entities_registry import get_columns_by_entity_name
from etl.jobs.util.cleaner import null_values_to_empty_string, replace_substring
from etl.jobs.util.dataframe_functions import flatten_array_columns
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, expr
from pyspark.sql.types import StringType


def main(argv):
    """
    Reads a tsv file using spark and writes it to a parquet file.
    :param list argv: the list elements should be:
                    [1]: Input Path
                    [2]: Dataframe name
                    [3]: Output file
    """
    parquet_path = argv[1]
    entity = argv[2]
    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()

    df = spark.read.parquet(parquet_path).drop(Constants.DATA_SOURCE_COLUMN)
    columns = get_columns_by_entity_name(entity)
    df = flatten_array_columns(df)
    df = df.select(columns)

    df = clean_df(df)

    # df.coalesce(1).write \
    df.write.option("sep", "\t").option("quote", "\u0000").option("header", "true").mode(
        "overwrite"
    ).csv(output_path)

# Clean df so it can produce a clean csv file
def clean_df(df: DataFrame) -> DataFrame:

    # Null values need to be treated as empty strings so the copy to the database works
    df = null_values_to_empty_string(df)

    df = escape_new_character_symbols_df(df)

    return df

# If not escaped, new character symbols can produce a wrong csv
def escape_new_character_symbols_df(df: DataFrame) -> DataFrame:
    
    # Null values need to be treated as empty strings so the copy to the database works
    df = null_values_to_empty_string(df)

    # Define the substring to replace and the replacement
    old_substring = "\n"
    new_substring = "\\\\n"

    replace_substring_udf = udf(
        lambda text: replace_substring(text, old_substring, new_substring), StringType()
    )

    df = df.select(
        [
            replace_substring_udf(
                col(c),
            ).alias(c)
            if dtype == "string"
            else col(c)
            for c, dtype in df.dtypes
        ]
    )

    # Replace substring in all string columns
    df = df.select(
        [
            expr(f"regexp_replace({c}, '{old_substring}', '{new_substring}')").alias(c)
            if dtype == StringType().simpleString()
            else col(c)
            for c, dtype in df.dtypes
        ]
    )

    return df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
