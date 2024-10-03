from typing import List, Dict
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from chispa.dataframe_comparer import *


def is_array(string_value):
    if isinstance(string_value, str):
        return string_value.startswith("[") and string_value.endswith("]")
    else:
        return isinstance(string_value, list)


def convert_comma_separated_string_to_array(value: str):
    result = []
    for x in value.split(","):
        x = x.strip()
        x = x.strip("'")
        result.append(x)
    return result


def convert_to_dataframe(spark: SparkSession, dict_list: List[Dict]) -> DataFrame:
    # Passing an explicit schema allows to have null (None) values in the data without issues
    schema = []
    for x in dict_list[0]:
        if is_array(dict_list[0][x]):
            schema.append(StructField(x, ArrayType(StringType()), True))
        else:
            schema.append(StructField(x, StringType(), True))
    schema = StructType(schema)

    data = []
    for element in dict_list:
        for x in element.keys():
            value = str(element[x])
            if value.startswith("[") and value.endswith("]"):
                content = value[1 : len(value) - 1]
                element[x] = convert_comma_separated_string_to_array(content)

        data.append(element)

    return spark.createDataFrame([Row(**x) for x in data], schema=schema)


def assert_df_are_equal_ignore_id(df_a: DataFrame, df_b: DataFrame):
    df_a = df_a.drop("id")
    df_b = df_b.drop("id")
    assert_df_equality(
        df_a,
        df_b,
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True,
    )


def assert_df_are_equal(df_a: DataFrame, df_b: DataFrame):
    assert_df_equality(
        df_a,
        df_b,
        ignore_nullable=True,
        ignore_row_order=True,
        ignore_column_order=True,
    )
