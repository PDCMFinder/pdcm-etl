import json
from pyspark.sql import SparkSession

from etl.jobs.transformation.links_generation.molecular_data_links_builder import \
    add_links_in_molecular_data_table
from tests.etl.workflow.links_generation.links_generation_tests_utils import create_resources_df, \
    create_resources_reference_data_df
from tests.util import assert_df_are_equal_ignore_id


def create_molecular_data_df():
    spark = SparkSession.builder.getOrCreate()
    columns = ["id", "hgnc_symbol", "amino_acid_change", "variation_id",
               "chromosome", "seq_start_position", "alt_allele", "ref_allele"]
    data = [(1, "NUP58", "T315I", "rs121913512&CM002803&COSM1304&COSM96871", None, None, None, None),
            (2, "NRAS", "Q61R", "", None, None, None, None),
            (3, "WEE1", "P504L", "", None, None, None, None),
            (4, "BRAF", "V600E", "", None, None, None, None)]
    df_input = spark.createDataFrame(data=data, schema=columns)

    return df_input


def create_molecular_data_with_hgnc_symbol_only_df():
    spark = SparkSession.builder.getOrCreate()
    columns = ["id", "hgnc_symbol"]
    data = [(1, "NUP58"),
            (2, "NRAS"),
            (3, "WEE1"),
            (4, "BRAF")]
    df_input = spark.createDataFrame(data=data, schema=columns)

    return df_input


def test_add_links_in_molecular_data_table_hgnc_symbol_only():
    spark = SparkSession.builder.getOrCreate()

    # Input data: molecular data containing only hgnc symbol (no amino acid change)
    columns = ["id", "hgnc_symbol"]
    data = [(1, "NUP58"),
            (2, "NRAS"),
            (3, "WEE1"),
            (4, "BRAF")]
    data_df = spark.createDataFrame(data=data, schema=columns)

    resources_df = create_resources_df()
    resources_data_df = create_resources_reference_data_df()

    data_df = add_links_in_molecular_data_table(data_df, resources_df, resources_data_df)

    links_row_1 = [
        {
            "column": "hgnc_symbol",
            "resource": "Civic",
            "link": "https://civicdb.org/links/entrez_name/NUP58"
        }
    ]

    links_row_2 = [
        {
            "column": "hgnc_symbol",
            "resource": "OncoMx",
            "link": "https://oncomx.org/searchview/?gene=BRAF"
        },
        {
            "column": "hgnc_symbol",
            "resource": "Civic",
            "link": "https://civicdb.org/links/entrez_name/BRAF"
        }
    ]
    expected_data = [
        (1, json.dumps(links_row_1)),
        (2, None),
        (3, None),
        (4, json.dumps(links_row_2))
    ]
    expected_df = spark.createDataFrame(expected_data, ["id", "external_db_links"])

    data_df_to_assert = data_df.select("id", "external_db_links")

    assert_df_are_equal_ignore_id(data_df_to_assert, expected_df)


def test_add_links_in_molecular_data_table_with_aac():
    resources_df = create_resources_df()

    # Input data: molecular data containing hgnc_symbol, amino_acid_change, variant_id
    spark = SparkSession.builder.getOrCreate()

    columns = ["id", "hgnc_symbol", "amino_acid_change", "variation_id",
               "chromosome", "seq_start_position", "alt_allele", "ref_allele"]
    data = [(1, "NUP58", "T315I", "rs121913512&CM002803&COSM1304&COSM96871", "", "", "", ""),
            (2, "NRAS", "Q61R", "rs123", "4", "54728055", "G", "A"),
            (3, "WEE1", "P504L", "-", "", "", "", ""),
            (4, "BRAF", "V600E", "", "", "", "", "")]

    data_df = spark.createDataFrame(data=data, schema=columns)

    resources_data_df = create_resources_reference_data_df()

    data_df = add_links_in_molecular_data_table(data_df, resources_df, resources_data_df)

    # Assert links where generated
    links_row_1 = [
        {
            "column": "hgnc_symbol",
            "resource": "Civic",
            "link": "https://civicdb.org/links/entrez_name/NUP58"
        },
        {
            "column": "amino_acid_change",
            "resource": "dbSNP",
            "link": "https://www.ncbi.nlm.nih.gov/snp/rs121913512"
        },
        {
            "column": "amino_acid_change",
            "resource": "COSMIC",
            "link": "https://cancer.sanger.ac.uk/cosmic/mutation/overview?id=1304"
        }
    ]

    links_row_2 = [
        {
            "column": "amino_acid_change",
            "resource": "dbSNP",
            "link": "https://www.ncbi.nlm.nih.gov/snp/rs123"
        },
        {
            "column": "amino_acid_change",
            "resource": "OpenCravat",
            "link": "https://run.opencravat.org/webapps/variantreport/index.html?" +
                    "alt_base=G&chrom=chr4&pos=54728055&ref_base=A"
        }
    ]

    links_row_4 = [
        {
            "column": "hgnc_symbol",
            "resource": "OncoMx",
            "link": "https://oncomx.org/searchview/?gene=BRAF"
        },
        {
            "column": "hgnc_symbol",
            "resource": "Civic",
            "link": "https://civicdb.org/links/entrez_name/BRAF"
        },
        {
            "column": "amino_acid_change",
            "resource": "Civic",
            "link": "https://civicdb.org/links?idtype=variant&id=12"
        }
    ]
    expected_data = [
        ("molecular_data_id_1", json.dumps(links_row_1)),
        ("molecular_data_id_2", json.dumps(links_row_2)),
        ("molecular_data_id_3", '[]'),
        ("molecular_data_id_4", json.dumps(links_row_4))
    ]
    expected_df = spark.createDataFrame(expected_data, ["molecular_data_id", "external_db_links"])

    data_df_to_assert = data_df.select("molecular_data_id", "external_db_links")

    assert_df_are_equal_ignore_id(data_df_to_assert, expected_df)
