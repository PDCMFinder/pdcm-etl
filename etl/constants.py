class Constants:
    RAW_DIRECTORY = "raw"
    TRANSFORMED_DIRECTORY = "transformed"
    DATABASE_FORMATTED = "database_formatted"
    REPORTS_DIRECTORY = "reports"

    # File ids names
    SOURCE_MODULE = "source"
    SAMPLE_PLATFORM_MODULE = "sampleplatform"
    PATIENT_MODULE = "patient"
    SAMPLE_MODULE = "sample"
    SHARING_MODULE = "sharing"
    MODEL_MODULE = "model"
    CELL_MODEL_MODULE = "cell-model"
    MODEL_VALIDATION_MODULE = "model_validation"
    DRUG_DOSING_MODULE = "drug-dosing"
    PATIENT_TREATMENT_MODULE = "patient-treatment"
    CNA_MODULE = "cna"
    CYTOGENETICS_MODULE = "cyto"
    EXPRESSION_MODULE = "expression"
    MUTATION_MODULE = "mutation"
    MOLECULAR_DATA_SAMPLE_MODULE = "molchar_sample"
    MOLECULAR_DATA_PLATFORM_MODULE = "molchar_platform"
    MOLECULAR_DATA_PLATFORM_WEB_MODULE = "molchar_platform_web"
    GENE_MARKER_MODULE = "markers"
    ONTOLOGY_MODULE = "ontology"
    MAPPING_DIAGNOSIS_MODULE = "mapping_diagnosis"
    MAPPING_TREATMENTS_MODULE = "mapping_treatments"

    DATA_SOURCE_COLUMN = "data_source_tmp"

    # Entities names
    DIAGNOSIS_ENTITY = "diagnosis"
    ETHNICITY_ENTITY = "ethnicity"
    MODEL_INFORMATION_ENTITY = "model_information"
    CELL_MODEL_ENTITY = "cell_model"
    CELL_SAMPLE_ENTITY = "cell_sample"
    CONTACT_PEOPLE_ENTITY = "contact_people"
    CONTACT_FORM_ENTITY = "contact_form"
    SOURCE_DATABASE_ENTITY = "source_database"
    QUALITY_ASSURANCE_ENTITY = "quality_assurance"
    PATIENT_ENTITY = "patient"
    PROVIDER_GROUP_ENTITY = "provider_group"
    PROVIDER_TYPE_ENTITY = "provider_type"
    PUBLICATION_GROUP_ENTITY = "publication_group"
    TISSUE_ENTITY = "tissue"
    TUMOUR_TYPE_ENTITY = "tumour_type"
    PATIENT_SAMPLE_ENTITY = "patient_sample"
    XENOGRAFT_SAMPLE_ENTITY = "xenograft_sample"
    PATIENT_SNAPSHOT_ENTITY = "patient_snapshot"
    XENOGRAFT_MODEL_SPECIMEN_ENTITY = "xenograft_model_specimen"
    ENGRAFTMENT_SITE_ENTITY = "engraftment_site"
    ENGRAFTMENT_TYPE_ENTITY = "engraftment_type"
    ENGRAFTMENT_SAMPLE_STATE_ENTITY = "engraftment_sample_state"
    ENGRAFTMENT_SAMPLE_TYPE_ENTITY = "engraftment_sample_type"
    ACCESSIBILITY_GROUP_ENTITY = "accessibility_group"
    HOST_STRAIN_ENTITY = "host_strain"
    PROJECT_GROUP_ENTITY = "project_group"
    TREATMENT_ENTITY = "treatment"
    RESPONSE_ENTITY = "response"
    RESPONSE_CLASSIFICATION_ENTITY = "response_classification"
    MOLECULAR_CHARACTERIZATION_TYPE_ENTITY = "molecular_characterization_type"
    PLATFORM_ENTITY = "platform"
    MOLECULAR_CHARACTERIZATION_ENTITY = "molecular_characterization"
    CNA_MOLECULAR_DATA_ENTITY = "cna_molecular_data"
    CYTOGENETICS_MOLECULAR_DATA_ENTITY = "cytogenetics_molecular_data"
    EXPRESSION_MOLECULAR_DATA_ENTITY = "expression_molecular_data"
    MUTATION_MARKER_ENTITY = "mutation_marker"
    MUTATION_MEASUREMENT_DATA_ENTITY = "mutation_measurement_data"
    GENE_MARKER_ENTITY = "gene_marker"
    ONTOLOGY_TERM_DIAGNOSIS_ENTITY = "ontology_term_diagnosis"
    ONTOLOGY_TERM_TREATMENT_ENTITY = "ontology_term_treatment"
    ONTOLOGY_TERM_REGIMEN_ENTITY = "ontology_term_regimen"
    REGIMENT_TO_TREATMENT_ENTITY = "regimen_to_treatment"
    SAMPLE_TO_ONTOLOGY_ENTITY = "sample_to_ontology"
    TREATMENT_PROTOCOL_ENTITY = "treatment_protocol"
    TREATMENT_COMPONENT_ENTITY = "treatment_component"
    TREATMENT_TO_ONTOLOGY_ENTITY = "treatment_to_ontology"
    REGIMEN_TO_ONTOLOGY_ENTITY = "regimen_to_ontology"
    SEARCH_INDEX_ENTITY = "search_index"
    SEARCH_FACET_ENTITY = "search_facet"

    # Helper Entities (do not get stored into the database, they just provide data to other transformations)
    TREATMENT_AND_COMPONENT_HELPER_ENTITY = "treatment_and_component_helper"
    TREATMENT_HARMONISATION_HELPER_ENTITY = "treatment_harmonisation_helper"

