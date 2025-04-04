from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    StructType,
    StringType,
    StructField,
)
from pyspark.sql.functions import (
    col,
    lit,
    expr,
    when,
    udf,
    to_json,
    struct,
    concat,
    concat_ws,
    collect_list,
)

import requests


# Adds links to resources describing the treatments
def add_treatment_links(treatment_df: DataFrame, resources_df: DataFrame):
    spark: SparkSession = SparkSession.builder.getOrCreate()

    # Schema for the df each method is going to return
    schema = StructType(
        [
            StructField("name", StringType(), False),
            StructField("resource_label", StringType(), False),
            StructField("link", StringType(), True),
        ]
    )
    all_links_df = spark.createDataFrame(data=[], schema=schema)
    resources_list = [row.asDict() for row in resources_df.collect()]

    for resource in resources_list:
        if resource["link_building_method"] == "ChEMBLInlineLink":
            print("Create links for ChEMBL")
            tmp_df = find_chembl_links(treatment_df, resource)
            all_links_df = all_links_df.unionAll(tmp_df)

        elif resource["link_building_method"] == "PubChemInlineLink":
            print("Create links for PubChem")
            tmp_df = find_pubchem_links(treatment_df, resource)
            all_links_df = all_links_df.unionAll(tmp_df)

    treatment_names_links_column_df = create_treatment_links_column(all_links_df)

    # # Join back to the original data frame to add the new column to it
    treatment_df = treatment_df.join(
        treatment_names_links_column_df, on=["name"], how="left"
    )

    return treatment_df


def find_chembl_links(treatment_names_df: DataFrame, resource) -> DataFrame:
    get_chembl_id_udf = udf(get_chembl_id, StringType())
    treatment_names_df = treatment_names_df.withColumn(
        "chembl_id", get_chembl_id_udf("name")
    )

    data_df = treatment_names_df.withColumn("resource_label", lit(resource["label"]))
    links_df = data_df.withColumn("link", lit(resource["link_template"]))

    links_df = links_df.withColumn(
        "link",
        when(col("chembl_id").isNull(), None).otherwise(
            expr("regexp_replace(link, 'ChEMBL_ID', chembl_id)")
        ),
    )

    return links_df.select("name", "resource_label", "link")


# Tries to find the ChEMBL id for the treatment name. If not exact name is found, tries with synonym search.
# Returns None if no match found
def get_chembl_id(treatment_name: str) -> str:
    chembl_id = find_chembl_id_by_name(treatment_name)
    # print(f"By name {treatment_name} is {chembl_id}")
    if chembl_id is None:
        chembl_id = find_chembl_id_by_synonym(treatment_name)
        # print(f"By synonym {treatment_name} is {chembl_id}")
    return chembl_id


def find_chembl_id_by_name(input: str) -> str:
    chembl_id = None
    url = f"https://www.ebi.ac.uk/chembl/api/data/molecule?pref_name__iexact={input}&format=json"

    try:
        response = requests.get(url, timeout=10)  # Add a timeout to avoid long waits
        if response.status_code == 200:  # Ensure the request was successful
            data = response.json()

            # Check if 'page_meta' exists safely
            if (
                data
                and "page_meta" in data
                and data["page_meta"].get("total_count", 0) == 1
            ):
                chembl_id = data["molecules"][0].get("molecule_chembl_id", None)
        else:
            print(
                f"Failed to retrieve data for {input}, status code: {response.status_code}"
            )
    except requests.exceptions.RequestException as e:
        # Handle connection errors or other request issues
        print(f"An error occurred: {e}")

    return chembl_id


def find_chembl_id_by_synonym(input: str) -> str:
    chembl_id = None
    url = f"https://www.ebi.ac.uk/chembl/api/data/molecule/search?q={input}&format=json"

    try:
        response = requests.get(
            url, timeout=10
        )  # Add a timeout to avoid hanging requests
        if response.status_code == 200:  # Ensure the request was successful
            data = response.json()

            # Safely check for 'page_meta' and 'molecules' in the response
            if (
                data
                and "page_meta" in data
                and data["page_meta"].get("total_count", 0) > 0
            ):
                for molecule in data.get(
                    "molecules", []
                ):  # Safely get 'molecules' list
                    synonyms = molecule.get("molecule_synonyms", [])
                    # Check if any synonyms match the input
                    matching_synonyms = [
                        synonym
                        for synonym in synonyms
                        if synonym["molecule_synonym"].lower() == input.lower()
                    ]
                    if matching_synonyms:
                        chembl_id = molecule.get("molecule_chembl_id", None)
                        break  # Stop after finding the first matching synonym
        else:
            print(
                f"Failed to retrieve data for {input}, status code: {response.status_code}"
            )
    except requests.exceptions.RequestException as e:
        # Handle any network issues or connection errors
        print(f"An error occurred: {e}")

    return chembl_id


def find_pubchem_links(treatment_names_df: DataFrame, resource) -> DataFrame:
    get_pubchem_id_udf = udf(get_pubchem_id, StringType())
    treatment_names_df = treatment_names_df.withColumn(
        "pubchem_id", get_pubchem_id_udf("name")
    )

    data_df = treatment_names_df.withColumn("resource_label", lit(resource["label"]))
    links_df = data_df.withColumn("link", lit(resource["link_template"]))

    links_df = links_df.withColumn(
        "link",
        when(col("pubchem_id").isNull(), None).otherwise(
            expr("regexp_replace(link, 'PubChem_ID', pubchem_id)")
        ),
    )

    return links_df.select("name", "resource_label", "link")


# Tries to find the PubChem id for the treatment name. For now no search by synonyms
def get_pubchem_id(treatment_name: str) -> str:
    pubchem_id = find_pubchem_id_by_name(treatment_name)
    return pubchem_id


def find_pubchem_id_by_name(input: str) -> str:
    pubchem_id = None
    url = f"https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/{input}/cids/TXT"

    try:
        response = requests.get(
            url, timeout=10
        )  # Add a timeout to prevent indefinite waiting

        if response.status_code == 200:  # Ensure response is successful
            response_entries = response.text.split("\n")
            if response_entries:
                pubchem_id = response_entries[0]  # Take the first entry if available

    except requests.ConnectTimeout:
        # Handle connection timeout and return None
        print(f"Connection to PubChem timed out for {input}.")
    except Exception as e:
        # Handle other potential exceptions
        print(f"An error occurred: {e}")

    return pubchem_id


# Takes a df with the columns <"name", "resource_label", "link"> and returns a df
# with columns <"name", "external_db_links"> where "treatment_links" is a JSON with the information to build links in the UI
def create_treatment_links_column(links_df: DataFrame) -> DataFrame:
    # Only interested in cases where links where found. This filter here causes `external_db_links` to br null if no links found.
    links_df = links_df.where("link is not null")

    links_json_entry_column_df = links_df.withColumn(
        "json_entry", to_json(struct("resource_label", "link"))
    )

    # Avoids issues when a treatment name appears more than once (rare scenario where the official name for the treatment is not mapped
    # but the alias is)
    links_json_entry_column_df = links_json_entry_column_df.dropDuplicates()

    treatment_names_links_column_df = links_json_entry_column_df.groupby("name").agg(
        concat_ws(", ", collect_list(links_json_entry_column_df.json_entry)).alias(
            "external_db_links"
        )
    )
    treatment_names_links_column_df = treatment_names_links_column_df.withColumn(
        "external_db_links",
        concat(lit("["), col("external_db_links"), concat(lit("]"))),
    )
    return treatment_names_links_column_df
