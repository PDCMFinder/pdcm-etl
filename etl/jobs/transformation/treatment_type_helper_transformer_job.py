import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf, split, coalesce
from pyspark.sql.types import StringType, ArrayType

KEYWORDS_BY_TYPE = [
    {"type": "Hormone Therapy", "keywords": ["hormone therapy"]},
    {
        "type": "Immunotherapy",
        "keywords": ["cytokine", "immunotherapeutic", "immunomodulatory"],
    },
    {
        "type": "Targeted Therapy",
        "keywords": [
            "targeted therapy",
            "targeting",
            "anti-hgf monoclonal antibody tak-701",
        ],
    },
    {"type": "Chemotherapy", "keywords": ["chemotherapy", "chemotherapeutic"]},
    {
        "type": "Surgery",
        "keywords": ["surgery", "mammoplasty", "ectomy", "biopsy", "plasty"],
    },
    {
        "type": "Radiation Therapy",
        "keywords": ["radiation therapy"],
    },
]


def calculate_type(treatment_name: str, ancestors: list) -> list:
    """
    Calculates the treatment types for a given treatment based on its list of ancestors extracted from an ontology.

    The `ancestors` list corresponds to ontology terms for which `treatment_name` is a descendant. The `treatment_name`
    is the label of an ontology term related to potential cancer treatments, and the function identifies its types
    based on its hierarchical position within the ontology.

    Treatment types are not mutually exclusive, so the function allows for multiple values by returning a list
    of categories that correspond to the treatment's ancestor terms.

    :param str treatment_name: The harmonized name of the treatment, corresponding to an ontology term label.
    :param list ancestors: A list of strings representing the ontology term names of all the ancestors of `treatment_name`.
    :return: A list containing the types of the treatment based on its ontology ancestors.
    :rtype: list
    """
    if not ancestors:
        ancestors = []
    types = []

    # Put all in a single list to compare with keywords
    ancestors.append(treatment_name)

    lower_case_names = [x.lower() for x in ancestors]

    for entry in KEYWORDS_BY_TYPE:
        keywords = entry["keywords"]

        # Check if any of the keywords matches exactly one of the ancestor terms
        if lists_intersect(keywords, lower_case_names):
            types.append(entry["type"])

        # Check if any of the ancestor terms contains the a keyword. This potentially is slower to process, so only
        # done when no intersection between both lists was found
        elif any_ancestors_contain_keyword(lower_case_names, keywords):
            types.append(entry["type"])

    return types


def lists_intersect(list_a: list, list_b: list) -> bool:
    intersection = set(list_a).intersection(list_b)
    return intersection


def any_ancestors_contain_keyword(ancestors: list, keywords: list) -> bool:
    for ancestor in ancestors:
        for keyword in keywords:
            if keyword in ancestor:
                return True
    return False


# Register the function as a UDF
calculate_type_udf = udf(calculate_type, ArrayType(StringType()))


def main(argv):
    """
    Creates a parquet file with the calculated types for treatments.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with the treatment names and their harmonised terms
                    [2]: Output file
    """
    treatment_name_harmonisation_helper_parquet_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    treatment_name_harmonisation_df = spark.read.parquet(
        treatment_name_harmonisation_helper_parquet_path
    )

    treatment_type_df = transform_treatment_type_helper(treatment_name_harmonisation_df)

    treatment_type_df.write.mode("overwrite").parquet(output_path)


def transform_treatment_type_helper(
    treatment_name_harmonisation_df: DataFrame,
) -> DataFrame:
    df = treatment_name_harmonisation_df

    # We try to use the harmonise treatment name, but if not available, we use the original treatment name
    df = df.withColumn("not_null_treatment_name", coalesce(df["term_name"], df["name"]))

    # Ancestors come in a string where individual ancestors are separated by "|"
    df = df.withColumn("ancestors_as_list", split(df.ancestors, ","))

    # Add the new column using the UDF
    df = df.withColumn(
        "treatment_types",
        calculate_type_udf(df["not_null_treatment_name"], df["ancestors_as_list"]),
    )

    df = df.select("name", "term_name", "term_id", "treatment_types", "class")

    return df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
