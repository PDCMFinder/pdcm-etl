from pyspark.sql import DataFrame


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
    df_a_ref = df_a.withColumnRenamed(col_df_a, "column_to_join")
    df_b_ref = df_b.withColumnRenamed(col_df_b, "column_to_join")
    join_df = df_a_ref.join(df_b_ref, on=['column_to_join'], how=how)
    join_df = join_df.drop("column_to_join")
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
    df_b_ref = df_b.withColumnRenamed(reference_column_id, "id_ref")
    join_df = join_left_dfs(df_a, df_b_ref, col_df_a, col_df_b)
    join_df = join_df.withColumnRenamed("id_ref", new_column_name)

    print("FK CREATED")
    join_df.show()
    return join_df

