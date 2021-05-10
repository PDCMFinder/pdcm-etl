from pyspark.sql import Column
from pyspark.sql.functions import regexp_replace, col, trim


def remove_no_break_space(column_name: str) -> Column:
    return regexp_replace(col(column_name), u"\u00A0", " ")


def trim_all(column_name: str) -> Column:
    return trim(remove_no_break_space(column_name))
