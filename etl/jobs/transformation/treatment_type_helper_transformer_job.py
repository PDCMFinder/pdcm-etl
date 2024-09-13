import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

from etl.jobs.util.cleaner import lower_and_trim_all

KEYWORDS_BY_TYPE = [
    {"type": "Hormone therapy", "keywords": ["hormone therapy"]},
    {"type": "Immunotherapy", "keywords": ["cytokine"]},
    {
        "type": "Targeted Therapy",
        "keywords": [
            "targeted therapy",
            "protein inhibitor",
            "targeting",
            "anti-hgf monoclonal antibody tak-701",
        ],
    },
    {"type": "Immunotherapy", "keywords": ["immunotherapeutic", "immunomodulatory"]},
    {"type": "Chemotherapy", "keywords": ["chemotherapy", "chemotherapeutic"]},
    {
        "type": "Surgery",
        # "keywords": ["mammoplasty", "mastectomy", "lumpectomy", "lymphadenectomy"],
        "keywords": ["mammoplasty", "ectomy"],
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
    types = []

    # Put all in a single list to compare with keywords
    ancestors.append(treatment_name)

    lower_case_names = [x.lower() for x in ancestors]
    print("lower_case_names", lower_case_names)

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
calculate_type_udf = udf(calculate_type, StringType())


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

    # treatment_type_df.write.mode("overwrite").parquet(output_path)


def transform_treatment_type_helper(
    treatment_name_harmonisation_df: DataFrame,
) -> DataFrame:
    df = treatment_name_harmonisation_df
    df = df.limit(5)

    # Add the new column using the UDF
    df_with_new_col = df.withColumn(
        "treatment_types", calculate_type_udf(df["term_name"], df["ancestors"])
    )
    df_with_new_col.show()

    # df.show()
    # df_rows = treatment_name_harmonisation_df.collect()
    # for row in df_rows:

    #     type = calculate_type(row['term_name'], row['ancestors'])
    #     row.asDict()
    #     row['type'] = type
    #     print("row", row)

    return df


if __name__ == "__main__":
    sys.exit(main(sys.argv))
