import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array_join,
    collect_list,
    col,
    concat_ws,
    collect_set,
    size,
    when,
    lit,
    array,
    lower,
    udf,
    split,
    array_intersect,
)
from pyspark.sql.types import ArrayType, StringType, DoubleType

from etl.jobs.util.dataframe_functions import join_left_dfs, join_dfs
from etl.jobs.util.id_assigner import add_id
from etl.workflow.config import PdcmConfig

cancer_systems = [
    "Breast Cancer",
    "Cardiovascular Cancer",
    "Connective and Soft Tissue Cancer",
    "Digestive System Cancer",
    "Endocrine Cancer",
    "Eye Cancer",
    "Head and Neck Cancer",
    "Hematopoietic and Lymphoid System Cancer",
    "Nervous System Cancer",
    "Peritoneal and Retroperitoneal Cancer",
    "Reproductive System Cancer",
    "Respiratory Tract Cancer",
    "Thoracic Cancer",
    "Skin Cancer",
    "Urinary System Cancer",
    "Unclassified",
]


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw molecular markers
                    [2]: Output file
    """
    model_parquet_path = argv[1]
    molecular_characterization_parquet_path = argv[2]
    molecular_characterization_type_parquet_path = argv[3]
    patient_sample_parquet_path = argv[4]
    patient_snapshot_parquet_path = argv[5]
    patient_parquet_path = argv[6]
    ethnicity_df_parquet_path = argv[7]
    xenograft_sample_parquet_path = argv[8]
    diagnosis_parquet_path = argv[9]
    tumour_type_parquet_path = argv[10]
    tissue_parquet_path = argv[11]
    gene_marker_parquet_path = argv[12]
    mutation_marker_parquet_path = argv[13]
    mutation_measurement_data_parquet_path = argv[14]
    cna_data_parquet_path = argv[15]
    expression_data_parquet_path = argv[16]
    cytogenetics_data_parquet_path = argv[17]
    provider_group_parquet_path = argv[18]
    project_group_parquet_path = argv[19]
    sample_to_ontology_parquet_path = argv[20]
    ontology_term_diagnosis_parquet_path = argv[21]
    output_path = argv[22]

    spark = SparkSession.builder.getOrCreate()
    model_df = spark.read.parquet(model_parquet_path)
    molecular_characterization_df = spark.read.parquet(
        molecular_characterization_parquet_path
    )
    molecular_characterization_type_df = spark.read.parquet(
        molecular_characterization_type_parquet_path
    )
    patient_sample_df = spark.read.parquet(patient_sample_parquet_path)
    ethnicity_df = spark.read.parquet(ethnicity_df_parquet_path)
    patient_snapshot_df = spark.read.parquet(patient_snapshot_parquet_path)
    patient_df = spark.read.parquet(patient_parquet_path)
    xenograft_sample_df = spark.read.parquet(xenograft_sample_parquet_path)
    diagnosis_df = spark.read.parquet(diagnosis_parquet_path)
    tumour_type_df = spark.read.parquet(tumour_type_parquet_path)
    tissue_df = spark.read.parquet(tissue_parquet_path)
    gene_marker_df = spark.read.parquet(gene_marker_parquet_path)
    mutation_marker_df = spark.read.parquet(mutation_marker_parquet_path)
    mutation_measurement_data_df = spark.read.parquet(
        mutation_measurement_data_parquet_path
    )
    cna_data_df = spark.read.parquet(cna_data_parquet_path)
    expression_data_df = spark.read.parquet(expression_data_parquet_path)
    cytogenetics_data_df = spark.read.parquet(cytogenetics_data_parquet_path)
    provider_group_df = spark.read.parquet(provider_group_parquet_path)
    project_group_df = spark.read.parquet(project_group_parquet_path)
    sample_to_ontology_df = spark.read.parquet(sample_to_ontology_parquet_path)
    ontology_term_diagnosis_df = spark.read.parquet(
        ontology_term_diagnosis_parquet_path
    )

    # TODO Add Brest Cancer Biomarkers column
    # TODO Add Cancer System column

    search_index_df = transform_search_index(
        model_df,
        patient_sample_df,
        patient_snapshot_df,
        patient_df,
        ethnicity_df,
        xenograft_sample_df,
        diagnosis_df,
        tumour_type_df,
        tissue_df,
        gene_marker_df,
        molecular_characterization_df,
        molecular_characterization_type_df,
        mutation_marker_df,
        mutation_measurement_data_df,
        cna_data_df,
        expression_data_df,
        cytogenetics_data_df,
        provider_group_df,
        project_group_df,
        sample_to_ontology_df,
        ontology_term_diagnosis_df,
    )
    search_index_df.write.mode("overwrite").parquet(output_path)


def transform_search_index(
    model_df,
    patient_sample_df,
    patient_snapshot_df,
    patient_df,
    ethnicity_df,
    xenograft_sample_df,
    diagnosis_df,
    tumour_type_df,
    tissue_df,
    gene_marker_df,
    molecular_characterization_df,
    molecular_characterization_type_df,
    mutation_marker_df,
    mutation_measurement_data_df,
    cna_data_df,
    expression_data_df,
    cytogenetics_data_df,
    provider_group_df,
    project_group_df,
    sample_to_ontology_df,
    ontology_term_diagnosis_df,
) -> DataFrame:
    search_index_df = model_df.withColumnRenamed("id", "pdcm_model_id")

    patient_sample_ext_df = extend_patient_sample(
        patient_sample_df,
        patient_df,
        diagnosis_df,
        tissue_df,
        ethnicity_df,
        patient_snapshot_df,
        tumour_type_df,
        provider_group_df,
        project_group_df,
        sample_to_ontology_df,
        ontology_term_diagnosis_df,
    )
    search_index_df = search_index_df.withColumn("temp_model_id", col("pdcm_model_id"))
    search_index_df = join_left_dfs(
        search_index_df, patient_sample_ext_df, "temp_model_id", "model_id"
    )

    # Adding molecular data availability
    molecular_characterization_df = molecular_characterization_df.withColumnRenamed(
        "id", "mol_char_id"
    )
    molecular_characterization_type_df = (
        molecular_characterization_type_df.withColumnRenamed(
            "name", "molecular_characterization_type_name"
        )
    )
    molecular_characterization_with_type_df = join_dfs(
        molecular_characterization_df,
        molecular_characterization_type_df,
        "molecular_characterization_type_id",
        "id",
        "inner",
    )
    patient_sample_mol_char_df = join_dfs(
        patient_sample_df,
        molecular_characterization_with_type_df,
        "id",
        "patient_sample_id",
        "inner",
    )
    patient_sample_mol_char_df = patient_sample_mol_char_df.select(
        "model_id", "mol_char_id", "molecular_characterization_type_name"
    ).distinct()

    xenograft_sample_mol_char_df = join_dfs(
        xenograft_sample_df,
        molecular_characterization_with_type_df,
        "id",
        "xenograft_sample_id",
        "inner",
    )
    xenograft_sample_mol_char_df = xenograft_sample_mol_char_df.select(
        "model_id", "mol_char_id", "molecular_characterization_type_name"
    ).distinct()

    model_mol_char_type_df = patient_sample_mol_char_df.union(
        xenograft_sample_mol_char_df
    )

    model_mol_char_availability_df = model_mol_char_type_df.groupby("model_id").agg(
        collect_set("molecular_characterization_type_name").alias("dataset_available")
    )

    search_index_df = search_index_df.withColumn("temp_model_id", col("pdcm_model_id"))
    search_index_df = join_left_dfs(
        search_index_df, model_mol_char_availability_df, "temp_model_id", "model_id"
    )

    # Adding mutation data availability by gene variant
    # Generate table model_id, mol_char_id, mol_char_type
    # Generate tables mol_char_id, tmp_symbol/gene_variant
    # Join left model_mol_char with mol_char_gene
    # then group by model, mol_char_type to collect the set of gene_variants and then pivot over mol_char_type
    mutation_marker_df = add_gene_symbol(mutation_marker_df, gene_marker_df)
    mutation_marker_df = mutation_marker_df.select(
        "id", "gene_symbol", "amino_acid_change"
    )
    mutation_marker_df = mutation_marker_df.withColumn(
        "gene_variant",
        when(
            col("amino_acid_change").isNotNull(),
            concat_ws("/", "gene_symbol", "amino_acid_change"),
        ).otherwise(col("gene_symbol")),
    )
    mutation_mol_char_df = mutation_measurement_data_df.select(
        "mutation_marker_id", "molecular_characterization_id"
    )
    mutation_mol_char_df = join_dfs(
        mutation_mol_char_df, mutation_marker_df, "mutation_marker_id", "id", "inner"
    )
    mutation_mol_char_df = mutation_mol_char_df.select(
        "molecular_characterization_id", "gene_variant"
    )

    # Adding CNA data availability by gene
    cna_data_df = add_gene_symbol(cna_data_df, gene_marker_df)
    cna_data_df = cna_data_df.withColumnRenamed("gene_symbol", "gene_variant")
    cna_data_df = cna_data_df.select(
        "molecular_characterization_id", "gene_variant"
    ).distinct()

    # Adding expression data availability by gene
    expression_data_df = add_gene_symbol(expression_data_df, gene_marker_df)
    expression_data_df = expression_data_df.withColumnRenamed(
        "gene_symbol", "gene_variant"
    )
    expression_data_df = expression_data_df.select(
        "molecular_characterization_id", "gene_variant"
    ).distinct()

    # Adding cytogenetics data availability by gene and result
    cytogenetics_mol_char_df = add_gene_symbol(cytogenetics_data_df, gene_marker_df)
    cytogenetics_mol_char_df = cytogenetics_mol_char_df.withColumnRenamed(
        "gene_symbol", "gene_variant"
    )
    cytogenetics_mol_char_df = cytogenetics_mol_char_df.select(
        "molecular_characterization_id", "gene_variant"
    ).distinct()

    mol_gene_df = (
        mutation_mol_char_df.union(cna_data_df)
        .union(expression_data_df)
        .union(cytogenetics_mol_char_df)
    )

    model_mol_char_type_gene_df = join_dfs(
        model_mol_char_type_df,
        mol_gene_df,
        "mol_char_id",
        "molecular_characterization_id",
        "inner",
    )

    model_mol_char_availability_by_gene_df = (
        model_mol_char_type_gene_df.groupby("model_id")
        .pivot("molecular_characterization_type_name")
        .agg(collect_set("gene_variant"))
    )
    molecular_data_col_map = {
        "copy number alteration": "makers_with_cna_data",
        "mutation": "makers_with_mutation_data",
        "expression": "makers_with_expression_data",
        "cytogenetics": "makers_with_cytogenetics_data",
    }

    for category, col_name in molecular_data_col_map.items():
        if category in model_mol_char_availability_by_gene_df.columns:
            model_mol_char_availability_by_gene_df = (
                model_mol_char_availability_by_gene_df.withColumnRenamed(
                    category, col_name
                )
            )
        else:
            model_mol_char_availability_by_gene_df = (
                model_mol_char_availability_by_gene_df.withColumn(
                    col_name, array().astype(ArrayType(StringType()))
                )
            )

    search_index_df = search_index_df.withColumn("temp_model_id", col("pdcm_model_id"))
    search_index_df = join_left_dfs(
        search_index_df,
        model_mol_char_availability_by_gene_df,
        "temp_model_id",
        "model_id",
    )
    # Adding breast cancer biomarkers data
    breast_cancer_biomarkers_df = add_gene_symbol(cytogenetics_data_df, gene_marker_df)
    breast_cancer_biomarkers_df = breast_cancer_biomarkers_df.withColumnRenamed(
        "gene_symbol", "breast_cancer_biomarker"
    )
    breast_cancer_biomarkers_df = breast_cancer_biomarkers_df.where(
        col("gene_symbol").isin(["ERBB2", "ESR1", "PGR"])
    )
    gene_display_map = {"ERBB2": "HER2/ERBB2", "ESR1": "ER/ESR1", "PGR": "PR/PGR"}

    map_display_breast_cancer_gene = (
        lambda gene: gene_display_map[gene] if gene in gene_display_map else gene
    )
    map_display_breast_cancer_gene_udf = udf(
        map_display_breast_cancer_gene, StringType()
    )
    breast_cancer_biomarkers_df = breast_cancer_biomarkers_df.select(
        "molecular_characterization_id",
        map_display_breast_cancer_gene_udf("breast_cancer_biomarker").alias(
            "breast_cancer_biomarker"
        ),
        "marker_status",
    ).distinct()

    breast_cancer_biomarkers_df = breast_cancer_biomarkers_df.withColumn(
        "breast_cancer_biomarker",
        concat_ws(" ", "breast_cancer_biomarker", "marker_status"),
    )

    model_breast_cancer_biomarkers_df = join_dfs(
        model_mol_char_type_df,
        breast_cancer_biomarkers_df,
        "mol_char_id",
        "molecular_characterization_id",
        "inner",
    )

    model_breast_cancer_biomarkers_df = model_breast_cancer_biomarkers_df.select(
        "model_id", "breast_cancer_biomarker"
    ).distinct()
    model_breast_cancer_biomarkers_df = model_breast_cancer_biomarkers_df.groupby(
        "model_id"
    ).agg(collect_set("breast_cancer_biomarker").alias("breast_cancer_biomarkers"))
    search_index_df = search_index_df.withColumn("temp_model_id", col("pdcm_model_id"))
    search_index_df = join_left_dfs(
        search_index_df,
        model_breast_cancer_biomarkers_df,
        "temp_model_id",
        "model_id",
    )

    search_index_df = search_index_df.select(
        "pdcm_model_id",
        "external_model_id",
        "data_source",
        "histology",
        "search_terms",
        "cancer_system",
        "dataset_available",
        "primary_site",
        "collection_site",
        "tumour_type",
        "patient_age",
        "patient_sex",
        "patient_ethnicity",
        "patient_treatment_status",
        "makers_with_cna_data",
        "makers_with_mutation_data",
        "makers_with_expression_data",
        "makers_with_cytogenetics_data",
        "breast_cancer_biomarkers",
        "project_name",
    ).distinct()
    return search_index_df


def add_gene_symbol(mol_char_data_df: DataFrame, gene_marker_df: DataFrame):
    """
    Takes in a molecular characterization dataframe and adds
    a gene symbol using a gene_marker dataframe.
    """
    gene_marker_df = gene_marker_df.where(col("approved_symbol").isNotNull())
    gene_marker_df = gene_marker_df.select("id", "approved_symbol")
    gene_marker_df = gene_marker_df.withColumnRenamed("approved_symbol", "gene_symbol")
    mol_char_data_df = join_dfs(
        mol_char_data_df, gene_marker_df, "gene_marker_id", "id", "inner"
    )
    return mol_char_data_df


def extend_patient_sample(
    patient_sample_df,
    patient_df,
    diagnosis_df,
    tissue_df,
    ethnicity_df,
    patient_snapshot_df,
    tumour_type_df,
    provider_group_df,
    project_group_df,
    sample_to_ontology_df,
    ontology_term_diagnosis_df,
):
    """
    Takes in a patient sample DataFrame and extends it with
     diagnosis, tumour, and patient information
    """
    # Adding diagnosis, primary_site, collection_site and tumour_type data to patient_sample
    sample_to_ontology_term_df = join_dfs(
        sample_to_ontology_df,
        ontology_term_diagnosis_df,
        "ontology_term_id",
        "id",
        "inner",
    )
    sample_to_ontology_term_df = sample_to_ontology_term_df.withColumn(
        "search_terms", split(concat_ws(",", "term_name", "ancestors"), ",")
    )
    sample_to_ontology_term_df = sample_to_ontology_term_df.withColumn(
        "cancer_system",
        array_intersect(array(*map(lit, cancer_systems)), col("search_terms")),
    )
    sample_to_ontology_term_df = sample_to_ontology_term_df.withColumn(
        "cancer_system", col("cancer_system").getItem(0)
    )
    sample_to_ontology_term_df = sample_to_ontology_term_df.withColumn(
        "histology", col("term_name")
    )
    patient_sample_ext_df = join_left_dfs(
        patient_sample_df, sample_to_ontology_term_df, "id", "sample_id"
    )

    primary_site_df = tissue_df.withColumnRenamed("name", "primary_site")
    patient_sample_ext_df = join_left_dfs(
        patient_sample_ext_df, primary_site_df, "primary_site_id", "id"
    )

    collection_site_df = tissue_df.withColumnRenamed("name", "collection_site")
    patient_sample_ext_df = join_left_dfs(
        patient_sample_ext_df, collection_site_df, "collection_site_id", "id"
    )

    # Adding age, sex, ethnicity and project to patient_sample
    project_group_df = project_group_df.withColumnRenamed("name", "project_name")
    provider_group_df = join_left_dfs(
        provider_group_df, project_group_df, "project_group_id", "id"
    )
    patient_df = join_left_dfs(patient_df, provider_group_df, "provider_group_id", "id")
    patient_df = patient_df.withColumnRenamed("sex", "patient_sex")
    patient_df = patient_df.withColumn(
        "patient_sex",
        when(lower(col("patient_sex")).contains("not"), "Not Specified").otherwise(
            lower(col("patient_sex"))
        ),
    )
    ethnicity_df = ethnicity_df.withColumnRenamed("name", "patient_ethnicity")
    patient_df = join_left_dfs(patient_df, ethnicity_df, "ethnicity_id", "id")

    patient_snapshot_df = join_left_dfs(
        patient_snapshot_df, patient_df, "patient_id", "id"
    )

    patient_snapshot_df = patient_snapshot_df.withColumnRenamed(
        "age_in_years_at_collection", "patient_age"
    )
    bin_age_udf = udf(_bin_age, StringType())
    patient_snapshot_df = patient_snapshot_df.withColumn(
        "patient_age", bin_age_udf("patient_age")
    )
    patient_snapshot_df = patient_snapshot_df.withColumn(
        "patient_treatment_status",
        when(
            lower(col("treatment_naive_at_collection")) == "yes",
            lit("Treatment naive"),
        )
        .when(
            lower(col("treatment_naive_at_collection")) == "no",
            lit("Not treatment naive"),
        )
        .otherwise(lit("Not specified")),
    )
    patient_sample_ext_df = join_left_dfs(
        patient_sample_ext_df, patient_snapshot_df, "id", "sample_id"
    )

    # Adding tumour_type name to patient_sample
    tumour_type_df = tumour_type_df.withColumnRenamed("name", "tumour_type")
    patient_sample_ext_df = join_left_dfs(
        patient_sample_ext_df, tumour_type_df, "tumour_type_id", "id"
    )

    return patient_sample_ext_df


def _bin_age(age_str: str):
    if age_str is None or "not" in age_str.lower():
        return "Not specified"

    if "months" in age_str:
        return "0 - 23 months"
    try:
        age = float(age_str)
        if age < 2:
            return "0 - 23 months"

        bin_ranges = [(2, 10)] + [(10 * i, 10 * (i + 1)) for i in range(1, 10)]
        for bin_range in bin_ranges:
            if bin_range[0] <= age <= bin_range[1]:
                return f"{bin_range[0]} - {bin_range[1] - 1}"
    except ValueError:
        return "Not specified"

    return age_str


if __name__ == "__main__":
    sys.exit(main(sys.argv))
