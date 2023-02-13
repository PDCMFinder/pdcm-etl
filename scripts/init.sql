CREATE SCHEMA IF NOT EXISTS pdcm_api;

COMMENT ON SCHEMA pdcm_api IS
  'Patient-derived cancer models (PDCMs) are a powerful oncology research platform for studying tumour biology, mechanisms of drug response and resistance and for testing personalised medicine. Distributed nature of repositories for PDCMs (xenografts, organoids and cell lines) and the use of different metadata standards for describing model''s characteristics make it difficult for researchers to identify suitable PDCM models relevant to specific cancer research questions. PDCM Finder aims to solve this problem by providing harmonized and integrated model attributes to support consistent searching across the originating resources';

DROP TABLE IF EXISTS ethnicity CASCADE;

CREATE TABLE ethnicity (
    id BIGINT NOT NULL,
    name TEXT
);

DROP TABLE IF EXISTS provider_type CASCADE;

CREATE TABLE provider_type (
    id BIGINT NOT NULL,
    name TEXT
);

DROP TABLE IF EXISTS provider_group CASCADE;

CREATE TABLE provider_group (
    id BIGINT NOT NULL,
    name TEXT,
    abbreviation TEXT,
    description TEXT,
    provider_type_id BIGINT,
    project_group_id BIGINT
);

DROP TABLE IF EXISTS patient CASCADE;

CREATE TABLE patient (
    id BIGINT NOT NULL,
    external_patient_id TEXT NOT NULL,
    sex TEXT NOT NULL,
    history TEXT,
    ethnicity_id BIGINT,
    ethnicity_assessment_method TEXT,
    initial_diagnosis TEXT,
    age_at_initial_diagnosis TEXT,
    provider_group_id BIGINT
);

DROP TABLE IF EXISTS publication_group CASCADE;

CREATE TABLE publication_group (
    id BIGINT NOT NULL,
    pubmed_ids TEXT NOT NULL
);

DROP TABLE IF EXISTS accessibility_group CASCADE;

CREATE TABLE accessibility_group (
    id BIGINT NOT NULL,
    europdx_access_modalities TEXT,
    accessibility TEXT
);

DROP TABLE IF EXISTS contact_people CASCADE;

CREATE TABLE contact_people (
    id BIGINT NOT NULL,
    name_list TEXT,
    email_list TEXT
);

DROP TABLE IF EXISTS contact_form CASCADE;

CREATE TABLE contact_form (
    id BIGINT NOT NULL,
    form_url TEXT NOT NULL
);

DROP TABLE IF EXISTS source_database CASCADE;

CREATE TABLE source_database (
    id BIGINT NOT NULL,
    database_url TEXT NOT NULL
);

DROP TABLE IF EXISTS model_information CASCADE;

CREATE TABLE model_information (
    id BIGINT NOT NULL,
    external_model_id TEXT,
    data_source varchar,
    publication_group_id BIGINT,
    accessibility_group_id BIGINT,
    contact_people_id BIGINT,
    contact_form_id BIGINT,
    source_database_id BIGINT
);

DROP TABLE IF EXISTS cell_model CASCADE;

CREATE TABLE cell_model (
    id BIGINT NOT NULL,
    name TEXT,
    type TEXT,
    growth_properties TEXT,
    parent_id TEXT,
    origin_patient_sample_id TEXT,
    comments TEXT,
    model_id BIGINT,
    supplier TEXT,
    external_ids TEXT
);

DROP TABLE IF EXISTS cell_sample CASCADE;

CREATE TABLE cell_sample (
    id BIGINT NOT NULL,
    external_cell_sample_id TEXT,
    model_id BIGINT,
    platform_id BIGINT
);

DROP TABLE IF EXISTS quality_assurance CASCADE;

CREATE TABLE quality_assurance (
    id BIGINT NOT NULL,
    description TEXT,
    passages_tested TEXT,
    validation_technique TEXT,
    validation_host_strain_nomenclature TEXT,
    model_id BIGINT NOT NULL
);

DROP TABLE IF EXISTS tissue CASCADE;

CREATE TABLE tissue (
    id BIGINT NOT NULL,
    name TEXT
);

DROP TABLE IF EXISTS tumour_type CASCADE;

CREATE TABLE tumour_type (
    id BIGINT NOT NULL,
    name TEXT
);

DROP TABLE IF EXISTS patient_sample CASCADE;

CREATE TABLE patient_sample (
    id BIGINT NOT NULL,
    external_patient_sample_id TEXT,
    patient_id BIGINT,
    diagnosis TEXT,
    grade TEXT,
    grading_system TEXT,
    stage TEXT,
    staging_system TEXT,
    primary_site_id BIGINT,
    collection_site_id BIGINT,
    prior_treatment TEXT,
    tumour_type_id BIGINT,
    age_in_years_at_collection TEXT,
    collection_event TEXT,
    collection_date TEXT,
    months_since_collection_1 TEXT,
    treatment_naive_at_collection TEXT,
    virology_status TEXT,
    model_id BIGINT
);

DROP TABLE IF EXISTS xenograft_sample CASCADE;

CREATE TABLE xenograft_sample (
    id BIGINT NOT NULL,
    external_xenograft_sample_id TEXT,
    passage VARCHAR,
    host_strain_id BIGINT,
    model_id BIGINT,
    platform_id BIGINT

);

DROP TABLE IF EXISTS engraftment_site CASCADE;

CREATE TABLE engraftment_site (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

DROP TABLE IF EXISTS engraftment_type CASCADE;

CREATE TABLE engraftment_type (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);


DROP TABLE IF EXISTS engraftment_sample_state CASCADE;

CREATE TABLE engraftment_sample_state (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

DROP TABLE IF EXISTS engraftment_sample_type CASCADE;

CREATE TABLE engraftment_sample_type (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

DROP TABLE IF EXISTS host_strain CASCADE;

CREATE TABLE host_strain (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    nomenclature TEXT NOT NULL
);


DROP TABLE IF EXISTS project_group CASCADE;

CREATE TABLE project_group (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

DROP TABLE IF EXISTS treatment CASCADE;

CREATE TABLE treatment (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    data_source TEXT NOT NULL
);

DROP TABLE IF EXISTS response CASCADE;

CREATE TABLE response (
    id BIGINT NOT NULL,
    name TEXT
);

DROP TABLE IF EXISTS response_classification CASCADE;

CREATE TABLE response_classification (
    id BIGINT NOT NULL,
    name TEXT
);

DROP TABLE IF EXISTS molecular_characterization_type CASCADE;

CREATE TABLE molecular_characterization_type (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

DROP TABLE IF EXISTS platform CASCADE;

CREATE TABLE platform (
    id BIGINT NOT NULL,
    library_strategy TEXT,
    provider_group_id BIGINT,
    instrument_model TEXT,
    library_selection TEXT
);

DROP TABLE IF EXISTS molecular_characterization CASCADE;

CREATE TABLE molecular_characterization (
    id BIGINT NOT NULL,
    molecular_characterization_type_id BIGINT NOT NULL,
    platform_id BIGINT NOT NULL,
    raw_data_url TEXT,
    patient_sample_id BIGINT,
    xenograft_sample_id BIGINT,
    cell_sample_id BIGINT
);

DROP TABLE IF EXISTS cna_molecular_data CASCADE;

CREATE TABLE cna_molecular_data (
    id BIGINT NOT NULL,
    hgnc_symbol TEXT,
    log10r_cna TEXT,
    log2r_cna TEXT,
    copy_number_status TEXT,
    gistic_value TEXT,
    picnic_value TEXT,
    non_harmonised_symbol TEXT,
    harmonisation_result TEXT,
    molecular_characterization_id BIGINT,
    data_source TEXT,
    external_db_links JSON
);

DROP TABLE IF EXISTS cytogenetics_molecular_data CASCADE;

CREATE TABLE cytogenetics_molecular_data (
    id BIGINT NOT NULL,
    hgnc_symbol TEXT,
    marker_status TEXT,
    essential_or_additional_marker TEXT,
    non_harmonised_symbol TEXT,
    harmonisation_result TEXT,
    molecular_characterization_id BIGINT,
    data_source TEXT,
    external_db_links JSON
);

DROP TABLE IF EXISTS expression_molecular_data CASCADE;

CREATE TABLE expression_molecular_data (
    id BIGINT NOT NULL,
    hgnc_symbol TEXT,
    z_score TEXT,
    rnaseq_coverage TEXT,
    rnaseq_fpkm TEXT,
    rnaseq_tpm TEXT,
    rnaseq_count TEXT,
    affy_hgea_probe_id TEXT,
    affy_hgea_expression_value TEXT,
    illumina_hgea_probe_id TEXT,
    illumina_hgea_expression_value TEXT,
    non_harmonised_symbol TEXT,
    harmonisation_result TEXT,
    molecular_characterization_id BIGINT,
    data_source TEXT,
    external_db_links JSON
);

DROP TABLE IF EXISTS mutation_measurement_data CASCADE;

CREATE TABLE mutation_measurement_data (
    id BIGINT NOT NULL,
    hgnc_symbol TEXT,
    amino_acid_change TEXT,
    consequence TEXT,
    read_depth TEXT,
    allele_frequency TEXT,
    seq_start_position TEXT,
    ref_allele TEXT,
    alt_allele TEXT,
    biotype TEXT,
    coding_sequence_change TEXT,
    variant_class TEXT,
    codon_change TEXT,
    functional_prediction TEXT,
    ncbi_transcript_id TEXT,
    ensembl_transcript_id TEXT,
    variation_id TEXT,
    molecular_characterization_id BIGINT,
    non_harmonised_symbol TEXT,
    harmonisation_result TEXT,
    data_source TEXT,
    external_db_links JSON
);

DROP TABLE IF EXISTS xenograft_model_specimen CASCADE;

CREATE TABLE xenograft_model_specimen (
    id BIGINT NOT NULL,
    passage_number TEXT NOT NULL,
    engraftment_site_id BIGINT,
    engraftment_type_id BIGINT,
    engraftment_sample_type_id BIGINT,
    engraftment_sample_state_id BIGINT,
    host_strain_id BIGINT,
    model_id BIGINT
);

DROP TABLE IF EXISTS gene_marker CASCADE;

CREATE TABLE gene_marker (
    id BIGINT NOT NULL,
    hgnc_id TEXT NOT NULL,
    approved_symbol TEXT,
    approved_name TEXT,
    previous_symbols TEXT,
    alias_symbols TEXT,
    accession_numbers TEXT,
    refseq_ids TEXT,
    alias_names TEXT,
    ensembl_gene_id TEXT,
    ncbi_gene_id TEXT
);

DROP TABLE IF EXISTS ontology_term_diagnosis CASCADE;

CREATE TABLE ontology_term_diagnosis(
    id BIGINT NOT NULL,
    term_id TEXT NOT NULL,
    term_name TEXT,
    term_url TEXT,
    is_a TEXT,
    ancestors TEXT
);

DROP TABLE IF EXISTS ontology_term_treatment CASCADE;

CREATE TABLE ontology_term_treatment(
    id BIGINT NOT NULL,
    term_id TEXT NOT NULL,
    term_name TEXT,
    is_a TEXT
);

DROP TABLE IF EXISTS ontology_term_regimen CASCADE;

CREATE TABLE ontology_term_regimen(
    id BIGINT NOT NULL,
    term_id TEXT NOT NULL,
    term_name TEXT,
    is_a TEXT
);

DROP TABLE IF EXISTS sample_to_ontology CASCADE;

CREATE TABLE sample_to_ontology(
    id BIGINT NOT NULL,
    sample_id BIGINT,
    ontology_term_id BIGINT
);

DROP TABLE IF EXISTS treatment_to_ontology CASCADE;

CREATE TABLE treatment_to_ontology (
    id BIGINT NOT NULL,
    treatment_id BIGINT,
    ontology_term_id BIGINT
);

DROP TABLE IF EXISTS regimen_to_ontology CASCADE;

CREATE TABLE regimen_to_ontology (
    id BIGINT NOT NULL,
    regimen_id BIGINT,
    ontology_term_id BIGINT
);

DROP TABLE IF EXISTS regimen_to_treatment CASCADE;

CREATE TABLE regimen_to_treatment (
    id BIGINT NOT NULL,
    regimen_ontology_term_id BIGINT,
    treatment_ontology_term_id BIGINT
);

DROP TABLE IF EXISTS treatment_protocol CASCADE;

CREATE TABLE treatment_protocol (
    id BIGINT NOT NULL,
    model_id BIGINT,
    patient_id BIGINT,
    treatment_target TEXT,
    response_id BIGINT,
    response_classification_id BIGINT

);

DROP TABLE IF EXISTS treatment_component CASCADE;

CREATE TABLE treatment_component (
    id BIGINT NOT NULL,
    dose TEXT,
    treatment_protocol_id BIGINT,
    treatment_id BIGINT

);

DROP TABLE IF EXISTS search_index CASCADE;

CREATE TABLE search_index (
    pdcm_model_id BIGINT NOT NULL,
    external_model_id TEXT NOT NULL,
    data_source TEXT,
    project_name TEXT,
    provider_name TEXT,
    model_type TEXT,
    histology TEXT,
    search_terms TEXT[],
    cancer_system TEXT,
    dataset_available TEXT[],
    primary_site TEXT,
    collection_site TEXT,
    tumour_type TEXT,
    cancer_grade TEXT,
    cancer_grading_system TEXT,
    cancer_stage TEXT,
    cancer_staging_system TEXT,
    patient_age TEXT,
    patient_sex TEXT,
    patient_ethnicity TEXT,
    patient_treatment_status TEXT,
    makers_with_cna_data TEXT[],
    makers_with_mutation_data TEXT[],
    makers_with_expression_data TEXT[],
    makers_with_cytogenetics_data TEXT[],
    breast_cancer_biomarkers TEXT[],
    treatment_list TEXT[],
    model_treatment_list TEXT[]
);

DROP TABLE IF EXISTS search_facet CASCADE;

CREATE TABLE search_facet (
    facet_section TEXT,
    facet_name TEXT,
    facet_column TEXT,
    facet_options TEXT[],
    facet_example TEXT
);

DROP TABLE IF EXISTS molecular_data_restriction CASCADE;

CREATE TABLE molecular_data_restriction (
    data_source TEXT,
    molecular_data_table TEXT
);

DROP TABLE IF EXISTS available_molecular_data_columns CASCADE;
CREATE TABLE available_molecular_data_columns (
    data_source TEXT,
    not_empty_cols TEXT[],
    molecular_characterization_type TEXT
);

DROP TABLE IF EXISTS release_info CASCADE;
CREATE TABLE release_info (
    name TEXT,
    date TIMESTAMP,
    providers TEXT[]
);
