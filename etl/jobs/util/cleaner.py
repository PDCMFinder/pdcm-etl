import re

from pyspark.sql import Column
from pyspark.sql.functions import regexp_replace, col, trim, initcap, lower


def remove_no_break_space(column_name: str) -> Column:
    return regexp_replace(col(column_name), u"\u00A0", " ")


def trim_all_str(text: str) -> str:
    return re.sub(u"\u00A0", " ", text).strip()


def trim_all(column_name: str) -> Column:
    return trim(remove_no_break_space(column_name))


def init_cap_and_trim_all(column_name: str) -> Column:
    return initcap(trim_all(column_name))


def lower_and_trim_all(column_name: str) -> Column:
    return lower(trim_all(column_name))
