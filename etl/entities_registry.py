from etl.workflow.transformer import *
import etl.jobs.transformation.diagnosis_transformer_job
import etl.jobs.transformation.ethnicity_transformer_job
import etl.jobs.transformation.provider_group_transformer_job
import etl.jobs.transformation.provider_type_transformer_job
import etl.jobs.transformation.model_transformer_job
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
import etl.jobs.transformation.engraftment_material_transformer_job
import etl.jobs.transformation.engraftment_sample_state_transformer_job
import etl.jobs.transformation.engraftment_sample_type_transformer_job
import etl.jobs.transformation.accessibility_group_transformer_job
import etl.jobs.transformation.host_strain_transformer_job
import etl.jobs.transformation.project_group_transformer_job
import etl.jobs.transformation.treatment_transformer_job
import etl.jobs.transformation.response_transformer_job
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


def get_spark_job_by_entity_name(entity_name):
    return entities[entity_name]['spark_job']


def get_transformation_class_by_entity_name(entity_name):
    return entities[entity_name]['transformation_class']


def get_columns_by_entity_name(entity_name):
    return entities[entity_name]['expected_database_columns']


def get_all_entities_names():
    return list(entities.keys())


def get_all_transformation_classes():
    transformation_classes = []
    for entity in entities:
        transformation_classes.append(get_transformation_class_by_entity_name(entity))
    return transformation_classes


entities = {
    Constants.DIAGNOSIS_ENTITY: {
        "spark_job": etl.jobs.transformation.diagnosis_transformer_job.main,
        "transformation_class": TransformDiagnosis(),
        "expected_database_columns": ["id", "name"]
    },
    Constants.ETHNICITY_ENTITY: {
        "spark_job": etl.jobs.transformation.ethnicity_transformer_job.main,
        "transformation_class": TransformEthnicity(),
        "expected_database_columns": ["id", "name"]
    },
    Constants.PATIENT_ENTITY: {
        "spark_job": etl.jobs.transformation.patient_transformer_job.main,
        "transformation_class": TransformPatient(),
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
        "transformation_class": TransformProviderType(),
        "expected_database_columns": ["id", "name"]
    },
    Constants.PROVIDER_GROUP_ENTITY: {
        "spark_job": etl.jobs.transformation.provider_group_transformer_job.main,
        "transformation_class": TransformProviderGroup(),
        "expected_database_columns": ["id", "name", "abbreviation", "description", "provider_type_id"]
    },
    Constants.PUBLICATION_GROUP_ENTITY: {
        "spark_job": etl.jobs.transformation.publication_group_transformer_job.main,
        "transformation_class": TransformPublicationGroup(),
        "expected_database_columns": ["id", "pub_med_ids"]
    },
    Constants.MODEL_ENTITY: {
        "spark_job": etl.jobs.transformation.model_transformer_job.main,
        "transformation_class": TransformModel(),
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
    Constants.CONTACT_PEOPLE_ENTITY: {
        "spark_job": etl.jobs.transformation.contact_people_transformer_job.main,
        "transformation_class": TransformContactPeople(),
        "expected_database_columns": ["id", "name_list", "email_list"]
    },
    Constants.CONTACT_FORM_ENTITY: {
        "spark_job": etl.jobs.transformation.contact_form_transformer_job.main,
        "transformation_class": TransformContactForm(),
        "expected_database_columns": ["id", "form_url"]
    },
    Constants.SOURCE_DATABASE_ENTITY: {
        "spark_job": etl.jobs.transformation.source_database_transformer_job.main,
        "transformation_class": TransformSourceDatabase(),
        "expected_database_columns": ["id", "database_url"]
    },
    Constants.QUALITY_ASSURANCE_ENTITY: {
        "spark_job": etl.jobs.transformation.quality_assurance_transformer_job.main,
        "transformation_class": TransformQualityAssurance(),
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
        "transformation_class": TransformTissue(),
        "expected_database_columns": ["id", "name"]
    },
    Constants.TUMOUR_TYPE_ENTITY: {
        "spark_job": etl.jobs.transformation.tumour_type_transformer_job.main,
        "transformation_class": TransformTumourType(),
        "expected_database_columns": ["id", "name"]
    },
    Constants.PATIENT_SAMPLE_ENTITY: {
        "spark_job": etl.jobs.transformation.patient_sample_transformer_job.main,
        "transformation_class": TransformPatientSample(),
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
        "transformation_class": TransformXenograftSample(),
        "expected_database_columns": [
            "id",
            "external_xenograft_sample_id",
            "passage",
            "host_strain_id",
            "model_id",
            "raw_data_url",
            "platform_id"
        ]
    },
    Constants.PATIENT_SNAPSHOT_ENTITY: {
        "spark_job": etl.jobs.transformation.patient_snapshot_transformer_job.main,
        "transformation_class": TransformPatientSnapshot(),
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
        "transformation_class": TransformEngraftmentSite(),
        "expected_database_columns": ["id", "name"]
    },
    Constants.ENGRAFTMENT_TYPE_ENTITY: {
        "spark_job": etl.jobs.transformation.engraftment_type_transformer_job.main,
        "transformation_class": TransformEngraftmentType(),
        "expected_database_columns": ["id", "name"]
    },
    Constants.ENGRAFTMENT_MATERIAL_ENTITY: {
        "spark_job": etl.jobs.transformation.engraftment_material_transformer_job.main,
        "transformation_class": TransformEngraftmentMaterial(),
        "expected_database_columns": ["id", "name"]
    },
    Constants.ENGRAFTMENT_SAMPLE_STATE_ENTITY: {
        "spark_job": etl.jobs.transformation.engraftment_sample_state_transformer_job.main,
        "transformation_class": TransformEngraftmentSampleState(),
        "expected_database_columns": ["id", "name"]
    },
    Constants.ENGRAFTMENT_SAMPLE_TYPE_ENTITY: {
        "spark_job": etl.jobs.transformation.engraftment_sample_type_transformer_job.main,
        "transformation_class": TransformEngraftmentSampleType(),
        "expected_database_columns": ["id", "name"]
    },
    Constants.ACCESSIBILITY_GROUP_ENTITY: {
        "spark_job": etl.jobs.transformation.accessibility_group_transformer_job.main,
        "transformation_class": TransformAccessibilityGroup(),
        "expected_database_columns": ["id", "europdx_access_modalities", "accessibility"]
    },
    Constants.HOST_STRAIN_ENTITY: {
        "spark_job": etl.jobs.transformation.host_strain_transformer_job.main,
        "transformation_class": TransformHostStrain(),
        "expected_database_columns": ["id", "name", "nomenclature"]
    },
    Constants.PROJECT_GROUP_ENTITY: {
        "spark_job": etl.jobs.transformation.project_group_transformer_job.main,
        "transformation_class": TransformProjectGroup(),
        "expected_database_columns": ["id", "name"]
    },
    Constants.TREATMENT_ENTITY: {
        "spark_job": etl.jobs.transformation.treatment_transformer_job.main,
        "transformation_class": TransformTreatment(),
        "expected_database_columns": ["id", "name"]
    },
    Constants.RESPONSE_ENTITY: {
        "spark_job": etl.jobs.transformation.response_transformer_job.main,
        "transformation_class": TransformResponse(),
        "expected_database_columns": ["id", "description", "classification"]
    },
    Constants.MOLECULAR_CHARACTERIZATION_TYPE_ENTITY: {
        "spark_job": etl.jobs.transformation.molecular_characterization_type_transformer_job.main,
        "transformation_class": TransformMolecularCharacterizationType(),
        "expected_database_columns": ["id", "name"]
    },
    Constants.MOLECULAR_CHARACTERIZATION_ENTITY: {
        "spark_job": etl.jobs.transformation.molecular_characterization_transformer_job.main,
        "transformation_class": TransformMolecularCharacterization(),
        "expected_database_columns": [
            "id", "molecular_characterization_type_id", "platform_id", "patient_sample_id", "xenograft_sample_id"]
    },
    Constants.PLATFORM_ENTITY: {
        "spark_job": etl.jobs.transformation.platform_transformer_job.main,
        "transformation_class": TransformPlatform(),
        "expected_database_columns": [
            "id", "library_strategy", "provider_group_id", "instrument_model", "library_selection"]
    },
    Constants.CNA_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.cna_molecular_data_transformer_job.main,
        "transformation_class": TransformCnaMolecularData(),
        "expected_database_columns": [
            "id",
            "log10r_cna",
            "log2r_cna",
            "copy_number_status",
            "gistic_value",
            "picnic_value",
            "tmp_symbol",
            "molecular_characterization_id"
        ]
    },
    Constants.CYTOGENETICS_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.cytogenetics_molecular_data_transformer_job.main,
        "transformation_class": TransformCytogeneticsMolecularData(),
        "expected_database_columns": [
            "id", "marker_status", "essential_or_additional_marker", "tmp_symbol", "molecular_characterization_id"]
    },
    Constants.EXPRESSION_MOLECULAR_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.expression_molecular_data_transformer_job.main,
        "transformation_class": TransformExpressionMolecularData(),
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
            "tmp_symbol",
            "molecular_characterization_id"]
    },
    Constants.MUTATION_MARKER_ENTITY: {
        "spark_job": etl.jobs.transformation.mutation_marker_transformer_job.main,
        "transformation_class": TransformMutationMarker(),
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
            "tmp_symbol"]
    },
    Constants.MUTATION_MEASUREMENT_DATA_ENTITY: {
        "spark_job": etl.jobs.transformation.mutation_measurement_data_transformer_job.main,
        "transformation_class": TransformMutationMeasurementData(),
        "expected_database_columns": [
            "id", "read_depth", "allele_frequency", "mutation_marker_id", "molecular_characterization_id"]
    },
    Constants.GENE_MARKER_ENTITY: {
        "spark_job": etl.jobs.transformation.gene_marker_transformer_job.main,
        "transformation_class": TransformGeneMarker(),
        "expected_database_columns": [
            "id",
            "hgnc_id",
            "approved_name",
            "previous_symbols",
            "alias_symbols",
            "accession_numbers",
            "refseq_ids",
            "alias_names",
            "ensembl_gene_id",
            "ncbi_gene_id"]
    },
    Constants.ONTOLOGY_TERM_DIAGNOSIS_ENTITY: {
        "spark_job": etl.jobs.transformation.ontology_term_diagnosis_transformer_job.main,
        "transformation_class": TransformOntologyTermDiagnosis(),
        "expected_database_columns": ["id", "term_id", "term_name", "is_a"]
    },
    Constants.ONTOLOGY_TERM_TREATMENT_ENTITY: {
        "spark_job": etl.jobs.transformation.ontology_term_treatment_transformer_job.main,
        "transformation_class": TransformOntologyTermTreatment(),
        "expected_database_columns": ["id", "term_id", "term_name", "is_a"]
    },
    Constants.SPECIMEN_ENTITY: {
        "spark_job": etl.jobs.transformation.specimen_transformer_job.main,
        "transformation_class": TransformSpecimen(),
        "expected_database_columns": [
            "id",
            "passage_number",
            "engraftment_site_id",
            "engraftment_type_id",
            "engraftment_material_id",
            "host_strain_id",
            "model_id"
        ]
    }
}

