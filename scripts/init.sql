CREATE TABLE diagnosis (
    id BIGINT NOT NULL,
    name TEXT
);

CREATE TABLE ethnicity (
    id BIGINT NOT NULL,
    name TEXT
);

CREATE TABLE provider_type (
    id BIGINT NOT NULL,
    name TEXT
);

CREATE TABLE provider_group (
    id BIGINT NOT NULL,
    name TEXT,
    abbreviation TEXT,
    description TEXT,
    provider_type_id BIGINT
);

CREATE TABLE patient (
    id BIGINT NOT NULL,
    external_patient_id TEXT NOT NULL,
    sex TEXT NOT NULL,
    history TEXT,
    ethnicity_id BIGINT,
    ethnicity_assessment_method TEXT,
    initial_diagnosis_id BIGINT,
    age_at_initial_diagnosis TEXT,
    provider_group_id BIGINT
);

CREATE TABLE publication_group (
    id BIGINT NOT NULL,
    pubmed_ids TEXT NOT NULL
);

CREATE TABLE accessibility_group (
    id BIGINT NOT NULL,
    europdx_access_modalities TEXT,
    accessibility TEXT
);

CREATE TABLE contact_people (
    id BIGINT NOT NULL,
    name_list TEXT NOT NULL,
    email_list TEXT NOT NULL
);

CREATE TABLE contact_form (
    id BIGINT NOT NULL,
    form_url TEXT NOT NULL
);

CREATE TABLE source_database (
    id BIGINT NOT NULL,
    database_url TEXT NOT NULL
);

CREATE TABLE model (
    id BIGINT NOT NULL,
    external_model_id TEXT,
    data_source varchar,
    publication_group_id BIGINT,
    accessibility_group_id BIGINT,
    contact_people_id BIGINT,
    contact_form_id BIGINT,
    source_database_id BIGINT
);

CREATE TABLE quality_assurance (
    id BIGINT NOT NULL,
    description TEXT,
    passages_tested TEXT,
    validation_technique TEXT,
    validation_host_strain_nomenclature TEXT,
    model_id BIGINT NOT NULL
);

CREATE TABLE tissue (
    id BIGINT NOT NULL,
    name TEXT
);

CREATE TABLE tumour_type (
    id BIGINT NOT NULL,
    name TEXT
);

CREATE TABLE patient_sample (
    id BIGINT NOT NULL,
    diagnosis_id BIGINT,
    external_patient_sample_id TEXT,
    grade TEXT,
    grading_system TEXT,
    stage TEXT,
    staging_system TEXT,
    primary_site_id BIGINT,
    collection_site_id BIGINT,
    raw_data_url TEXT,
    prior_treatment TEXT,
    tumour_type_id BIGINT,
    model_id BIGINT
);

CREATE TABLE xenograft_sample (
    id BIGINT NOT NULL,
    external_xenograft_sample_id TEXT,
    passage VARCHAR,
    host_strain_id BIGINT,
    model_id BIGINT,
    raw_data_url VARCHAR,
    platform_id BIGINT

);

CREATE TABLE patient_snapshot (
    id BIGINT NOT NULL,
    patient_id BIGINT,
    age_in_years_at_collection TEXT,
    collection_event TEXT,
    collection_date TEXT,
    months_since_collection_1 TEXT,
    treatment_naive_at_collection TEXT,
    virology_status TEXT,
    sample_id BIGINT
);

CREATE TABLE engraftment_site (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE engraftment_type (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE engraftment_material (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE engraftment_sample_state (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE engraftment_sample_type (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE host_strain (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    nomenclature TEXT NOT NULL
);

CREATE TABLE project_group (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE treatment (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE response (
    id BIGINT NOT NULL,
    description TEXT,
    classification TEXT
);

CREATE TABLE molecular_characterization_type (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

CREATE TABLE platform (
    id BIGINT NOT NULL,
    library_strategy TEXT,
    provider_group_id BIGINT,
    instrument_model TEXT,
    library_selection TEXT
);

CREATE TABLE molecular_characterization (
    id BIGINT NOT NULL,
    molecular_characterization_type_id BIGINT NOT NULL,
    platform_id BIGINT NOT NULL,
    patient_sample_id BIGINT,
    xenograft_sample_id BIGINT
);

CREATE TABLE cna_molecular_data (
    id BIGINT NOT NULL,
    log10r_cna TEXT,
    log2r_cna TEXT,
    copy_number_status TEXT,
    gistic_value TEXT,
    picnic_value TEXT,
    --gene_marker_id BIGINT,
    tmp_symbol TEXT,
    molecular_characterization_id BIGINT
    --loci_marker_id BIGINT
);


CREATE TABLE cytogenetics_molecular_data (
    id BIGINT NOT NULL,
    marker_status TEXT,
    essential_or_additional_marker TEXT,
    --gene_marker_id BIGINT,
    tmp_symbol TEXT,
    molecular_characterization_id BIGINT
);

CREATE TABLE expression_molecular_data (
    id BIGINT NOT NULL,
    z_score TEXT,
    rnaseq_coverage TEXT,
    rnaseq_fpkm TEXT,
    rnaseq_tpm TEXT,
    rnaseq_count TEXT,
    affy_hgea_probe_id TEXT,
    affy_hgea_expression_value TEXT,
    illumina_hgea_probe_id TEXT,
    illumina_hgea_expression_value TEXT,
    --gene_marker_id BIGINT,
    tmp_symbol TEXT,
    molecular_characterization_id BIGINT
    --loci_marker_id BIGINT
);

CREATE TABLE mutation_marker (
    id BIGINT NOT NULL,
    biotype TEXT,
    coding_sequence_change TEXT,
    variant_class TEXT,
    codon_change TEXT,
    amino_acid_change TEXT,
    consequence TEXT,
    functional_prediction TEXT,
    seq_start_position TEXT,
    ref_allele TEXT,
    alt_allele TEXT,
    ncbi_transcript_id TEXT,
    ensembl_transcript_id TEXT,
    variation_id TEXT,
    --gene_marker_id BIGINT,
    tmp_symbol TEXT
    --loci_marker_id BIGINT
);

CREATE TABLE mutation_measurement_data (
    id BIGINT NOT NULL,
    read_depth TEXT,
    allele_frequency TEXT,
    mutation_marker_id BIGINT,
    molecular_characterization_id BIGINT
);

CREATE TABLE specimen (
    id BIGINT NOT NULL,
    passage_number TEXT NOT NULL,
    engraftment_site_id BIGINT,
    engraftment_type_id BIGINT,
    engraftment_material_id BIGINT,
    host_strain_id BIGINT,
    model_id BIGINT
);

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

CREATE TABLE ontology_term_diagnosis(
    id BIGINT NOT NULL,
    term_id TEXT NOT NULL,
    term_name TEXT,
    is_a TEXT
);

CREATE TABLE ontology_term_treatment(
    id BIGINT NOT NULL,
    term_id TEXT NOT NULL,
    term_name TEXT,
    is_a TEXT
);

CREATE TABLE ontology_term_regimen(
    id BIGINT NOT NULL,
    term_id TEXT NOT NULL,
    term_name TEXT,
    is_a TEXT
);

CREATE TABLE search_index (
    pdcm_model_id BIGINT NOT NULL,
    external_model_id TEXT NOT NULL,
    data_source TEXT,
    histology TEXT,
    data_available TEXT,
    primary_site TEXT,
    collection_site TEXT,
    tumour_type TEXT,
    patient_age TEXT,
    patient_sex TEXT,
    patient_ethnicity TEXT
);