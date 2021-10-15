import etl.jobs.transformation.diagnosis_transformer_job
import etl.jobs.transformation.ethnicity_transformer_job
import etl.jobs.transformation.provider_group_transformer_job
import etl.jobs.transformation.provider_type_transformer_job
import etl.jobs.transformation.model_transformer_job
import etl.jobs.transformation.cell_model_transformer_job
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
import etl.jobs.transformation.patient_snapshot_transformer_job
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
import etl.jobs.transformation.cna_molecular_data_transformer_job
import etl.jobs.transformation.cytogenetics_molecular_data_transformer_job
import etl.jobs.transformation.expression_molecular_data_transformer_job
import etl.jobs.transformation.mutation_marker_transformer_job
import etl.jobs.transformation.mutation_measurement_data_transformer_job
import etl.jobs.transformation.gene_marker_transformer_job
import etl.jobs.transformation.specimen_transformer_job
import etl.jobs.transformation.ontology_term_diagnosis_transformer_job
import etl.jobs.transformation.ontology_term_treatment_transformer_job
import etl.jobs.transformation.ontology_term_regimen_transformer_job
import etl.jobs.transformation.sample_to_ontology_transformer_job
import etl.jobs.transformation.patient_treatment_transformer_job
import etl.jobs.transformation.search_facet_transformer_job
import etl.jobs.transformation.search_index_transformer_job
from etl.constants import Constants


def get_spark_job_by_entity_name(entity_name):
    return entities[entity_name]["spark_job"]


def get_columns_by_entity_name(entity_name):
    return entities[entity_name]["expected_database_columns"]


def get_all_entities_names():
    return list(entities.keys())


entities = {
    Constants.DIAGNOSIS_ENTITY: {
        "spark_job": etl.jobs.transformation.diagnosis_transformer_job.main,
        "expected_database_columns": ["id", "name"]
    },
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
            "initial_diagnosis_id",
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
    Constants.MODEL_ENTITY: {
        "spark_job": etl.jobs.transformation.model_transformer_job.main,
        "expected_database_columns": [
            "id",
            "external_model_id",
            "data_source",
            "publication_group_id",
            "accessibility_group_id",
            "contact_people_id",
            "contact_form_id",
            "source_database_id"
        ]
    },
    Constants.CELL_MODEL_ENTITY: {
        "spark_job": etl.jobs.transformation.cell_model_transformer_job.main,
        "expected_database_columns": [
            "id",
            "external_model_id",
            "name",
            "type",
            "growth_properties",
            "parent_id",
            "origin_patient_sample_id",
            "comments",
            "model_id",
            "supplier",
            "external_ids",
            "provider_abb"
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
            "diagnosis_id",
            "external_patient_sample_id",
            "grade",
            "grading_system",
            "stage",
            "staging_system",
            "primary_site_id",
            "collection_site_id",
            "raw_data_url",
            "prior_treatment",
            "tumour_type_id",
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
            "raw_data_url",
            "platform_id",
        ]
    },
    Constants.PATIENT_SNAPSHOT_ENTITY: {
        "spark_job": etl.jobs.transformation.patient_snapshot_transformer_job.main,
        "expected_database_columns": [
            "id",
            "patient_id",
            "age_in_years_at_collection",
            "collection_event",
            "collection_date",
            "months_since_collection_1",
            "treatment_naive_at_collection",
            "virology_status",
            "sample_id"
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
        "expected_database_columns": ["id", "name"]
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
            "patient_sample_id",
            "xenograft_sample_id"
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
    Constants.CNA_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.cna_molecular_data_transformer_job.main,
        "expected_database_columns": [
            "id",
            "log10r_cna",
            "log2r_cna",
            "copy_number_status",
            "gistic_value",
            "picnic_value",
            "gene_marker_id",
            "non_harmonised_symbol",
            "harmonisation_result",
            "molecular_characterization_id"
        ]
    },
    Constants.CYTOGENETICS_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.cytogenetics_molecular_data_transformer_job.main,
        "expected_database_columns": [
            "id",
            "marker_status",
            "essential_or_additional_marker",
            "gene_marker_id",
            "non_harmonised_symbol",
            "harmonisation_result",
            "molecular_characterization_id"
        ]
    },
    Constants.EXPRESSION_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.expression_molecular_data_transformer_job.main,
        "expected_database_columns": [
            "id",
            "z_score",
            "rnaseq_coverage",
            "rnaseq_fpkm",
            "rnaseq_tpm",
            "rnaseq_count",
            "affy_hgea_probe_id",
            "affy_hgea_expression_value",
            "illumina_hgea_probe_id",
            "illumina_hgea_expression_value",
            "gene_marker_id",
            "non_harmonised_symbol",
            "harmonisation_result",
            "molecular_characterization_id",
        ]
    },
    Constants.MUTATION_MARKER_ENTITY: {
        "spark_job": etl.jobs.transformation.mutation_marker_transformer_job.main,
        "expected_database_columns": [
            "id",
            "biotype",
            "coding_sequence_change",
            "variant_class",
            "codon_change",
            "amino_acid_change",
            "consequence",
            "functional_prediction",
            "seq_start_position",
            "ref_allele",
            "alt_allele",
            "ncbi_transcript_id",
            "ensembl_transcript_id",
            "variation_id",
            "gene_marker_id",
            "non_harmonised_symbol",
            "harmonisation_result"
        ]
    },
    Constants.MUTATION_MEASUREMENT_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.mutation_measurement_data_transformer_job.main,
        "expected_database_columns": [
            "id",
            "read_depth",
            "allele_frequency",
            "mutation_marker_id",
            "molecular_characterization_id"
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
    Constants.SPECIMEN_ENTITY: {
        "spark_job": etl.jobs.transformation.specimen_transformer_job.main,
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
    Constants.PATIENT_TREATMENT_ENTITY: {
        "spark_job": etl.jobs.transformation.patient_treatment_transformer_job.main,
        "expected_database_columns": [
            "id",
            "patient_id",
            "treatment_id",
            "treatment_dose",
            "treatment_starting_date",
            "treatment_duration",
            "treatment_event",
            "elapsed_time",
            "response_id",
            "response_classification_id",
            "model_id"
        ]
    },

    Constants.SEARCH_INDEX_ENTITY: {
        "spark_job": etl.jobs.transformation.search_index_transformer_job.main,
        "expected_database_columns": [
            "pdcm_model_id",
            "external_model_id",
            "data_source",
            "project_name",
            "histology",
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
            "breast_cancer_biomarkers"
        ]
    },
    Constants.SEARCH_FACET_ENTITY: {
        "spark_job": etl.jobs.transformation.search_facet_transformer_job.main,
        "expected_database_columns": [
            "facet_section",
            "facet_name",
            "facet_column",
            "facet_options"
        ]
    }
}

