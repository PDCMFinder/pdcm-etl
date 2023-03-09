from pyspark.sql import DataFrame
from pyspark.sql.functions import transform, concat, lit, array_join, when, size

from etl.constants import Constants


def join_dfs(df_a: DataFrame, df_b: DataFrame, col_df_a: str, col_df_b: str, how: str) -> DataFrame:
    """
    Joins 2 dataframes

    Parameters:
        df_a (DataFrame): Dataframe A
        df_b (DataFrame): Dataframe B
        col_df_a (str): Name of the column in Dataframe A to use in the join
        col_df_b (str): Name of the column in Dataframe B to use in the join
        how (str): Mode of the join (right, left, etc)

    Returns:
    DataFrame: The dataframe with the join of both dataframes

   """
    cond = df_a[col_df_a] == df_b[col_df_b]
    join_df = df_a.join(df_b, cond, how=how)

    # Delete one of the columns that will become duplicated after the join
    join_df = join_df.drop(df_b[col_df_b])
    return join_df


def join_left_dfs(df_a: DataFrame, df_b: DataFrame, col_df_a: str, col_df_b: str) -> DataFrame:
    """
    Left join between 2 dataframes

    Parameters:
        df_a (DataFrame): Dataframe A
        df_b (DataFrame): Dataframe B
        col_df_a (str): Name of the column in Dataframe A to use in the join
        col_df_b (str): Name of the column in Dataframe B to use in the join

    Returns:
    DataFrame: The dataframe with the left join of both dataframes

   """
    return join_dfs(df_a, df_b, col_df_a, col_df_b, 'left')


def transform_to_fk(
        df_a: DataFrame,
        df_b: DataFrame,
        col_df_a: str,
        col_df_b: str,
        reference_column_id: str,
        new_column_name: str) -> DataFrame:

    # Select only needed values from the reference side: the id and the column to be used in the comparison
    df_b_ref = df_b.select(reference_column_id, col_df_b)

    # Standardize id column name
    df_b_ref = df_b_ref.withColumnRenamed(reference_column_id, "id_ref")

    join_df = join_left_dfs(df_a, df_b_ref, col_df_a, col_df_b)
    join_df = join_df.withColumnRenamed("id_ref", new_column_name)
    return join_df


def flatten_array_columns(df: DataFrame):
    for col_name, dtype in df.dtypes:
        if "array" in dtype:
            if "string" in dtype:
                df = df.withColumn(
                    col_name,
                    transform(col_name, lambda v: concat(lit('"'), v, lit('"'))),
                )
            df = df.withColumn(
                col_name,
                when(
                    df[col_name].isNotNull() & (size(df[col_name]) > 0),
                    concat(lit("{"), array_join(col_name, ","), lit("}")),
                ).otherwise(lit(None)),
            )
    return df
