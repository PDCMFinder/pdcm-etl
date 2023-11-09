import etl.jobs.transformation.ethnicity_transformer_job
import etl.jobs.transformation.provider_group_transformer_job
import etl.jobs.transformation.provider_type_transformer_job
import etl.jobs.transformation.model_transformer_job
import etl.jobs.transformation.license_transformer_job
import etl.jobs.transformation.cell_model_transformer_job
import etl.jobs.transformation.cell_sample_transformer_job
import etl.jobs.transformation.publication_group_transformer_job
import etl.jobs.transformation.contact_people_transformer_job
import etl.jobs.transformation.contact_form_transformer_job
import etl.jobs.transformation.source_database_transformer_job
import etl.jobs.transformation.quality_assurance_transformer_job
import etl.jobs.transformation.patient_transformer_job
import etl.jobs.transformation.tissue_transformer_job
import etl.jobs.transformation.tumour_type_transformer_job
import etl.jobs.transformation.patient_sample_transformer_job
import etl.jobs.transformation.xenograft_sample_transformer_job
import etl.jobs.transformation.engraftment_site_transformer_job
import etl.jobs.transformation.engraftment_type_transformer_job
import etl.jobs.transformation.engraftment_sample_state_transformer_job
import etl.jobs.transformation.engraftment_sample_type_transformer_job
import etl.jobs.transformation.accessibility_group_transformer_job
import etl.jobs.transformation.host_strain_transformer_job
import etl.jobs.transformation.project_group_transformer_job
import etl.jobs.transformation.treatment_transformer_job
import etl.jobs.transformation.response_transformer_job
import etl.jobs.transformation.response_classification_transformer_job
import etl.jobs.transformation.molecular_characterization_type_transformer_job
import etl.jobs.transformation.platform_transformer_job
import etl.jobs.transformation.molecular_characterization_transformer_job
import etl.jobs.transformation.gene_helper_transformer_job
import etl.jobs.transformation.initial_cna_molecular_data_transformer_job
import etl.jobs.transformation.initial_biomarker_molecular_data_transformer_job
import etl.jobs.transformation.initial_expression_molecular_data_transformer_job
import etl.jobs.transformation.initial_mutation_molecular_data_transformer_job
import etl.jobs.transformation.cna_molecular_data_transformer_job
import etl.jobs.transformation.biomarker_molecular_data_transformer_job
import etl.jobs.transformation.immunemarker_molecular_data_transformer_job
import etl.jobs.transformation.expression_molecular_data_transformer_job
import etl.jobs.transformation.mutation_measurement_data_transformer_job
import etl.jobs.transformation.gene_marker_transformer_job
import etl.jobs.transformation.image_study_transformer_job
import etl.jobs.transformation.model_image_transformer_job
import etl.jobs.transformation.xenograft_model_specimen_transformer_job
import etl.jobs.transformation.ontology_term_diagnosis_transformer_job
import etl.jobs.transformation.ontology_term_treatment_transformer_job
import etl.jobs.transformation.ontology_term_regimen_transformer_job
import etl.jobs.transformation.sample_to_ontology_transformer_job
import etl.jobs.transformation.treatment_protocol_transformer_job
import etl.jobs.transformation.treatment_and_component_helper_transformer_job
import etl.jobs.transformation.treatment_component_transformer_job
import etl.jobs.transformation.treatment_to_ontology_transformer_job
import etl.jobs.transformation.regimen_to_ontology_transformer_job
import etl.jobs.transformation.search_facet_transformer_job
import etl.jobs.transformation.model_metadata_transformer_job
import etl.jobs.transformation.search_index_patient_sample_transformer_job
import etl.jobs.transformation.search_index_molecular_characterization_transformer_job
import etl.jobs.transformation.search_index_molecular_data_transformer_job
import etl.jobs.transformation.search_index_transformer_job
import etl.jobs.transformation.regimen_to_treatment_transformer_job
import etl.jobs.transformation.treatment_harmonisation_helper_transformer_job
import etl.jobs.transformation.molecular_data_restriction_transformer_job
import etl.jobs.transformation.available_molecular_data_columns_transformer_job
from etl.constants import Constants


def get_spark_job_by_entity_name(entity_name):
    return entities[entity_name]["spark_job"]


def get_columns_by_entity_name(entity_name):
    return entities[entity_name]["expected_database_columns"]


def get_all_entities_names():
    return list(entities.keys())


# Some entities (exceptional cases) are not to be stored into the database because they are just temporary entities
# that help with other transformations.
def get_all_entities_names_to_store_db():
    entities_names_to_store_db = []
    for k in entities:
        if len(entities[k]["expected_database_columns"]) > 0:
            entities_names_to_store_db.append(k)

    return entities_names_to_store_db


entities = {
    Constants.ETHNICITY_ENTITY: {
        "spark_job": etl.jobs.transformation.ethnicity_transformer_job.main,
        "expected_database_columns": ["id", "name"]
    },
    Constants.PATIENT_ENTITY: {
        "spark_job": etl.jobs.transformation.patient_transformer_job.main,
        "expected_database_columns": [
            "id",
            "external_patient_id",
            "sex",
            "history",
            "ethnicity_id",
            "ethnicity_assessment_method",
            "initial_diagnosis",
            "age_at_initial_diagnosis",
            "provider_group_id"
        ]
    },
    Constants.PROVIDER_TYPE_ENTITY: {
        "spark_job": etl.jobs.transformation.provider_type_transformer_job.main,
        "expected_database_columns": ["id", "name"]
    },
    Constants.PROVIDER_GROUP_ENTITY: {
        "spark_job": etl.jobs.transformation.provider_group_transformer_job.main,
        "expected_database_columns": [
            "id",
            "name",
            "abbreviation",
            "description",
            "provider_type_id",
            "project_group_id"
        ]
    },
    Constants.PUBLICATION_GROUP_ENTITY: {
        "spark_job": etl.jobs.transformation.publication_group_transformer_job.main,
        "expected_database_columns": ["id", "pubmed_ids"]
    },
    Constants.MODEL_INFORMATION_ENTITY: {
        "spark_job": etl.jobs.transformation.model_transformer_job.main,
        "expected_database_columns": [
            "id",
            "external_model_id",
            "data_source",
            "publication_group_id",
            "accessibility_group_id",
            "contact_people_id",
            "contact_form_id",
            "source_database_id",
            "license_id"
        ]
    },
    Constants.LICENSE_ENTITY: {
        "spark_job": etl.jobs.transformation.license_transformer_job.main,
        "expected_database_columns": [
            "id",
            "name",
            "url"
        ]
    },
    Constants.CELL_MODEL_ENTITY: {
        "spark_job": etl.jobs.transformation.cell_model_transformer_job.main,
        "expected_database_columns": [
            "id",
            "name",
            "type",
            "growth_properties",
            "parent_id",
            "origin_patient_sample_id",
            "comments",
            "model_id",
            "supplier",
            "external_ids"
        ]
    },
    Constants.CELL_SAMPLE_ENTITY: {
        "spark_job": etl.jobs.transformation.cell_sample_transformer_job.main,
        "expected_database_columns": [
            "id",
            "external_cell_sample_id",
            "model_id",
            "platform_id"
        ]
    },
    Constants.CONTACT_PEOPLE_ENTITY: {
        "spark_job": etl.jobs.transformation.contact_people_transformer_job.main,
        "expected_database_columns": ["id", "name_list", "email_list"]
    },
    Constants.CONTACT_FORM_ENTITY: {
        "spark_job": etl.jobs.transformation.contact_form_transformer_job.main,
        "expected_database_columns": ["id", "form_url"]
    },
    Constants.SOURCE_DATABASE_ENTITY: {
        "spark_job": etl.jobs.transformation.source_database_transformer_job.main,
        "expected_database_columns": ["id", "database_url"]
    },
    Constants.QUALITY_ASSURANCE_ENTITY: {
        "spark_job": etl.jobs.transformation.quality_assurance_transformer_job.main,
        "expected_database_columns": [
            "id",
            "description",
            "passages_tested",
            "validation_technique",
            "validation_host_strain_nomenclature",
            "model_id"
        ]
    },
    Constants.TISSUE_ENTITY: {
        "spark_job": etl.jobs.transformation.tissue_transformer_job.main,
        "expected_database_columns": ["id", "name"]
    },
    Constants.TUMOUR_TYPE_ENTITY: {
        "spark_job": etl.jobs.transformation.tumour_type_transformer_job.main,
        "expected_database_columns": ["id", "name"]
    },
    Constants.PATIENT_SAMPLE_ENTITY: {
        "spark_job": etl.jobs.transformation.patient_sample_transformer_job.main,
        "expected_database_columns": [
            "id",
            "external_patient_sample_id",
            "patient_id",
            "diagnosis",
            "grade",
            "grading_system",
            "stage",
            "staging_system",
            "primary_site_id",
            "collection_site_id",
            "prior_treatment",
            "tumour_type_id",
            "age_in_years_at_collection",
            "collection_event",
            "collection_date",
            "months_since_collection_1",
            "treatment_naive_at_collection",
            "virology_status",
            "model_id"
        ]
    },
    Constants.XENOGRAFT_SAMPLE_ENTITY: {
        "spark_job": etl.jobs.transformation.xenograft_sample_transformer_job.main,
        "expected_database_columns": [
            "id",
            "external_xenograft_sample_id",
            "passage",
            "host_strain_id",
            "model_id",
            "platform_id",
        ]
    },
    Constants.ENGRAFTMENT_SITE_ENTITY: {
        "spark_job": etl.jobs.transformation.engraftment_site_transformer_job.main,
        "expected_database_columns": ["id", "name"]
    },
    Constants.ENGRAFTMENT_TYPE_ENTITY: {
        "spark_job": etl.jobs.transformation.engraftment_type_transformer_job.main,
        "expected_database_columns": ["id", "name"]
    },
    Constants.ENGRAFTMENT_SAMPLE_STATE_ENTITY: {
        "spark_job": etl.jobs.transformation.engraftment_sample_state_transformer_job.main,
        "expected_database_columns": ["id", "name"]
    },
    Constants.ENGRAFTMENT_SAMPLE_TYPE_ENTITY: {
        "spark_job": etl.jobs.transformation.engraftment_sample_type_transformer_job.main,
        "expected_database_columns": ["id", "name"]
    },
    Constants.ACCESSIBILITY_GROUP_ENTITY: {
        "spark_job": etl.jobs.transformation.accessibility_group_transformer_job.main,
        "expected_database_columns": [
            "id",
            "europdx_access_modalities",
            "accessibility"
        ]
    },
    Constants.HOST_STRAIN_ENTITY: {
        "spark_job": etl.jobs.transformation.host_strain_transformer_job.main,
        "expected_database_columns": ["id", "name", "nomenclature"]
    },
    Constants.PROJECT_GROUP_ENTITY: {
        "spark_job": etl.jobs.transformation.project_group_transformer_job.main,
        "expected_database_columns": ["id", "name"]
    },
    Constants.TREATMENT_ENTITY: {
        "spark_job": etl.jobs.transformation.treatment_transformer_job.main,
        "expected_database_columns": ["id", "name", "type", "data_source"]
    },
    Constants.RESPONSE_ENTITY: {
        "spark_job": etl.jobs.transformation.response_transformer_job.main,
        "expected_database_columns": ["id", "name"]
    },
    Constants.RESPONSE_CLASSIFICATION_ENTITY: {
        "spark_job": etl.jobs.transformation.response_classification_transformer_job.main,
        "expected_database_columns": ["id", "name"]
    },
    Constants.MOLECULAR_CHARACTERIZATION_TYPE_ENTITY: {
        "spark_job": etl.jobs.transformation.molecular_characterization_type_transformer_job.main,
        "expected_database_columns": ["id", "name"]
    },
    Constants.MOLECULAR_CHARACTERIZATION_ENTITY: {
        "spark_job": etl.jobs.transformation.molecular_characterization_transformer_job.main,
        "expected_database_columns": [
            "id",
            "molecular_characterization_type_id",
            "platform_id",
            "raw_data_url",
            "patient_sample_id",
            "xenograft_sample_id",
            "cell_sample_id",
            "external_db_links"
        ]
    },
    Constants.PLATFORM_ENTITY: {
        "spark_job": etl.jobs.transformation.platform_transformer_job.main,
        "expected_database_columns": [
            "id",
            "library_strategy",
            "provider_group_id",
            "instrument_model",
            "library_selection"
        ]
    },
    Constants.GENE_HELPER_ENTITY: {
        "spark_job": etl.jobs.transformation.gene_helper_transformer_job.main,
        "expected_database_columns": []
    },
    Constants.INITIAL_CNA_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.initial_cna_molecular_data_transformer_job.main,
        "expected_database_columns": []
    },
    Constants.INITIAL_BIOMARKER_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.initial_biomarker_molecular_data_transformer_job.main,
        "expected_database_columns": []
    },
    Constants.INITIAL_EXPRESSION_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.initial_expression_molecular_data_transformer_job.main,
        "expected_database_columns": []
    },
    Constants.INITIAL_MUTATION_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.initial_mutation_molecular_data_transformer_job.main,
        "expected_database_columns": []
    },
    Constants.CNA_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.cna_molecular_data_transformer_job.main,
        "expected_database_columns": [
            "id",
            "hgnc_symbol",
            "chromosome",
            "strand",
            "log10r_cna",
            "log2r_cna",
            "seq_start_position",
            "seq_end_position",
            "copy_number_status",
            "gistic_value",
            "picnic_value",
            "ensembl_gene_id",
            "ncbi_gene_id",
            "non_harmonised_symbol",
            "harmonisation_result",
            "molecular_characterization_id",
            "data_source",
            "external_db_links"
        ]
    },
    Constants.BIOMARKER_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.biomarker_molecular_data_transformer_job.main,
        "expected_database_columns": [
            "id",
            "biomarker",
            "biomarker_status",
            "essential_or_additional_marker",
            "non_harmonised_symbol",
            "harmonisation_result",
            "molecular_characterization_id",
            "data_source",
            "external_db_links"
        ]
    },
    Constants.IMMUNEMARKER_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.immunemarker_molecular_data_transformer_job.main,
        "expected_database_columns": [
            "id",
            "marker_name",
            "marker_value",
            "essential_or_additional_details",
            "molecular_characterization_id",
            "data_source"
        ]
    },
    Constants.EXPRESSION_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.expression_molecular_data_transformer_job.main,
        "expected_database_columns": [
            "id",
            "hgnc_symbol",
            "z_score",
            "rnaseq_coverage",
            "rnaseq_fpkm",
            "rnaseq_tpm",
            "rnaseq_count",
            "affy_hgea_probe_id",
            "affy_hgea_expression_value",
            "illumina_hgea_probe_id",
            "illumina_hgea_expression_value",
            "ensembl_gene_id",
            "ncbi_gene_id",
            "non_harmonised_symbol",
            "harmonisation_result",
            "molecular_characterization_id",
            "data_source",
            "external_db_links"
        ]
    },
    Constants.MUTATION_MEASUREMENT_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.mutation_measurement_data_transformer_job.main,
        "expected_database_columns": [
            "id",
            "hgnc_symbol",
            "amino_acid_change",
            "chromosome",
            "strand",
            "consequence",
            "read_depth",
            "allele_frequency",
            "seq_start_position",
            "ref_allele",
            "alt_allele",
            "biotype",
            "coding_sequence_change",
            "variant_class",
            "codon_change",
            "functional_prediction",
            "ncbi_transcript_id",
            "ensembl_transcript_id",
            "variation_id",
            "ensembl_gene_id",
            "ncbi_gene_id",
            "molecular_characterization_id",
            "non_harmonised_symbol",
            "harmonisation_result",
            "data_source",
            "external_db_links"
        ]
    },
    Constants.GENE_MARKER_ENTITY: {
        "spark_job": etl.jobs.transformation.gene_marker_transformer_job.main,
        "expected_database_columns": [
            "id",
            "hgnc_id",
            "approved_symbol",
            "approved_name",
            "previous_symbols",
            "alias_symbols",
            "accession_numbers",
            "refseq_ids",
            "alias_names",
            "ensembl_gene_id",
            "ncbi_gene_id"
        ]
    },
    Constants.IMAGE_STUDY_ENTITY: {
        "spark_job": etl.jobs.transformation.image_study_transformer_job.main,
        "expected_database_columns": [
            "id",
            "study_id",
            "title",
            "description",
            "licence",
            "contact",
            "sample_organism",
            "sample_description",
            "sample_preparation_protocol",
            "imaging_instrument",
            "image_acquisition_parameters",
            "imaging_method"
        ]
    },
    Constants.MODEL_IMAGE_MODULE: {
        "spark_job": etl.jobs.transformation.model_image_transformer_job.main,
        "expected_database_columns": [
            "id",
            "model_id",
            "url",
            "description",
            "sample_type",
            "passage",
            "magnification",
            "staining"
        ]
    },
    Constants.ONTOLOGY_TERM_DIAGNOSIS_ENTITY: {
        "spark_job": etl.jobs.transformation.ontology_term_diagnosis_transformer_job.main,
        "expected_database_columns": ["id", "term_id", "term_name", "term_url", "is_a", "ancestors"]
    },
    Constants.ONTOLOGY_TERM_TREATMENT_ENTITY: {
        "spark_job": etl.jobs.transformation.ontology_term_treatment_transformer_job.main,
        "expected_database_columns": ["id", "term_id", "term_name", "is_a"]
    },
    Constants.ONTOLOGY_TERM_REGIMEN_ENTITY: {
        "spark_job": etl.jobs.transformation.ontology_term_regimen_transformer_job.main,
        "expected_database_columns": ["id", "term_id", "term_name", "is_a"]
    },
    Constants.REGIMENT_TO_TREATMENT_ENTITY: {
        "spark_job": etl.jobs.transformation.regimen_to_treatment_transformer_job.main,
        "expected_database_columns": ["id", "regimen_ontology_term_id", "treatment_ontology_term_id"]
    },
    Constants.XENOGRAFT_MODEL_SPECIMEN_ENTITY: {
        "spark_job": etl.jobs.transformation.xenograft_model_specimen_transformer_job.main,
        "expected_database_columns": [
            "id",
            "passage_number",
            "engraftment_site_id",
            "engraftment_type_id",
            "engraftment_sample_type_id",
            "engraftment_sample_state_id",
            "host_strain_id",
            "model_id"
        ]
    },
    Constants.SAMPLE_TO_ONTOLOGY_ENTITY: {
        "spark_job": etl.jobs.transformation.sample_to_ontology_transformer_job.main,
        "expected_database_columns": ["id", "sample_id", "ontology_term_id"]
    },
    Constants.TREATMENT_PROTOCOL_ENTITY: {
        "spark_job": etl.jobs.transformation.treatment_protocol_transformer_job.main,
        "expected_database_columns": [
            "id",
            "model_id",
            "patient_id",
            "treatment_target",
            "response_id",
            "response_classification_id"
        ]
    },
    Constants.TREATMENT_AND_COMPONENT_HELPER_ENTITY: {
        "spark_job": etl.jobs.transformation.treatment_and_component_helper_transformer_job.main,
        "expected_database_columns": []
    },
    Constants.TREATMENT_COMPONENT_ENTITY: {
        "spark_job": etl.jobs.transformation.treatment_component_transformer_job.main,
        "expected_database_columns": [
            "id",
            "dose",
            "treatment_protocol_id",
            "treatment_id"
        ]
    },
    Constants.TREATMENT_TO_ONTOLOGY_ENTITY: {
        "spark_job": etl.jobs.transformation.treatment_to_ontology_transformer_job.main,
        "expected_database_columns": [
            "id",
            "treatment_id",
            "ontology_term_id"
        ]
    },
    Constants.REGIMEN_TO_ONTOLOGY_ENTITY: {
        "spark_job": etl.jobs.transformation.regimen_to_ontology_transformer_job.main,
        "expected_database_columns": [
            "id",
            "regimen_id",
            "ontology_term_id"
        ]
    },
    Constants.TREATMENT_HARMONISATION_HELPER_ENTITY: {
        "spark_job": etl.jobs.transformation.treatment_harmonisation_helper_transformer_job.main,
        "expected_database_columns": []
    },

    Constants.MODEL_METADATA: {
        "spark_job": etl.jobs.transformation.model_metadata_transformer_job.main,
        "expected_database_columns": []
    },

    Constants.SEARCH_INDEX_PATIENT_SAMPLE_ENTITY: {
        "spark_job": etl.jobs.transformation.search_index_patient_sample_transformer_job.main,
        "expected_database_columns": []
    },

    Constants.SEARCH_INDEX_MOLECULAR_CHARACTERIZATION_ENTITY: {
        "spark_job": etl.jobs.transformation.search_index_molecular_characterization_transformer_job.main,
        "expected_database_columns": []
    },

    Constants.SEARCH_INDEX_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.search_index_molecular_data_transformer_job.main,
        "expected_database_columns": []
    },

    Constants.SEARCH_INDEX_ENTITY: {
        "spark_job": etl.jobs.transformation.search_index_transformer_job.main,
        "expected_database_columns": [
            "pdcm_model_id",
            "external_model_id",
            "data_source",
            "project_name",
            "provider_name",
            "model_type",
            "histology",
            "search_terms",
            "cancer_system",
            "dataset_available",
            "license_name",
            "license_url",
            "primary_site",
            "collection_site",
            "tumour_type",
            "cancer_grade",
            "cancer_grading_system",
            "cancer_stage",
            "cancer_staging_system",
            "patient_age",
            "patient_sex",
            "patient_history",
            "patient_ethnicity",
            "patient_ethnicity_assessment_method",
            "patient_initial_diagnosis",
            "patient_treatment_status",
            "patient_age_at_initial_diagnosis",
            "patient_sample_id",
            "patient_sample_collection_date",
            "patient_sample_collection_event",
            "patient_sample_months_since_collection_1",
            "patient_sample_virology_status",
            "patient_sample_sharable",
            "patient_sample_treated_at_collection",
            "patient_sample_treated_prior_to_collection",
            "pdx_model_publications",
            "quality_assurance",
            "xenograft_model_specimens",
            "model_images",
            "markers_with_cna_data",
            "markers_with_mutation_data",
            "markers_with_expression_data",
            "markers_with_biomarker_data",
            "breast_cancer_biomarkers",
            "immunemarkers_names",
            "treatment_list",
            "model_treatment_list",
            "custom_treatment_type_list",
            "raw_data_resources",
            "cancer_annotation_resources",
            "scores"
        ]
    },
    Constants.SEARCH_FACET_ENTITY: {
        "spark_job": etl.jobs.transformation.search_facet_transformer_job.main,
        "expected_database_columns": [
            "facet_section",
            "facet_name",
            "facet_column",
            "facet_options",
            "facet_example"
        ]
    },
    Constants.MOLECULAR_DATA_RESTRICTION_ENTITY: {
        "spark_job": etl.jobs.transformation.molecular_data_restriction_transformer_job.main,
        "expected_database_columns": [
            "data_source",
            "molecular_data_table"
        ]
    },
    Constants.AVAILABLE_MOLECULAR_DATA_COLUMNS_ENTITY: {
        "spark_job": etl.jobs.transformation.available_molecular_data_columns_transformer_job.main,
        "expected_database_columns": [
            "data_source",
            "not_empty_cols",
            "molecular_characterization_type"
        ]
    }

}

