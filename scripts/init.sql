CREATE TABLE diagnosis (
    id BIGINT NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
);

CREATE TABLE ethnicity (
    id BIGINT NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
);

CREATE TABLE provider_type (
    id BIGINT NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
);

CREATE TABLE provider_group (
    id BIGINT NOT NULL,
    name TEXT,
    abbreviation TEXT,
    provider_type_id BIGINT,
    PRIMARY KEY (id)
);

ALTER TABLE provider_group
    ADD CONSTRAINT fk_provider_group_provider_type
    FOREIGN KEY (provider_type_id)
    REFERENCES provider_type (id);

CREATE TABLE patient (
    id BIGINT NOT NULL,
    external_patient_id TEXT NOT NULL,
    sex TEXT NOT NULL,
    history TEXT,
    ethnicity_id BIGINT,
    ethnicity_assessment_method TEXT,
    initial_diagnosis_id BIGINT,
    age_at_initial_diagnosis TEXT,
    provider_group_id BIGINT,
    PRIMARY KEY (id),
    CONSTRAINT uq_external_patient_id UNIQUE(external_patient_id)
);

ALTER TABLE patient
    ADD CONSTRAINT fk_patient_diagnosis
    FOREIGN KEY (initial_diagnosis_id)
    REFERENCES diagnosis (id);

ALTER TABLE patient
    ADD CONSTRAINT fk_patient_provider_group
    FOREIGN KEY (provider_group_id)
    REFERENCES provider_group (id);

CREATE TABLE publication_group (
    id BIGINT NOT NULL,
    pub_med_id TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE accessibility_group (
    id BIGINT NOT NULL,
    europdx_access_modalities TEXT,
    accessibility TEXT,
    PRIMARY KEY (id)
);

CREATE TABLE contact_people (
    id BIGINT NOT NULL,
    name_list TEXT NOT NULL,
    email_list TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE contact_form (
    id BIGINT NOT NULL,
    form_url TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE source_database (
    id BIGINT NOT NULL,
    database_url TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE model (
    id BIGINT NOT NULL,
    external_model_id TEXT,
    data_source varchar,
    publication_group_id BIGINT,
    accessibility_group_id BIGINT,
    contact_people_id BIGINT,
    contact_form_id BIGINT,
    source_database_id BIGINT,
    PRIMARY KEY (id)
);

ALTER TABLE model
    ADD CONSTRAINT fk_model_publication_group
    FOREIGN KEY (publication_group_id)
    REFERENCES publication_group (id);

ALTER TABLE model
    ADD CONSTRAINT fk_model_accessibility_group
    FOREIGN KEY (accessibility_group_id)
    REFERENCES accessibility_group (id);

ALTER TABLE model
    ADD CONSTRAINT fk_model_contact_people
    FOREIGN KEY (contact_people_id)
    REFERENCES contact_people (id);

ALTER TABLE model
    ADD CONSTRAINT fk_model_contact_form
    FOREIGN KEY (contact_form_id)
    REFERENCES contact_form (id);

ALTER TABLE model
    ADD CONSTRAINT fk_model_source_database
    FOREIGN KEY (source_database_id)
    REFERENCES source_database (id);

CREATE TABLE quality_assurance (
    id BIGINT NOT NULL,
    description TEXT,
    passages_tested TEXT,
    validation_technique TEXT,
    validation_host_strain_full TEXT,
    model_id BIGINT NOT NULL,
    PRIMARY KEY (id)
);

ALTER TABLE quality_assurance
    ADD CONSTRAINT fk_quality_assurance_model
    FOREIGN KEY (model_id)
    REFERENCES model (id);

CREATE TABLE tissue (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE tumour_type (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id)
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
    model_id BIGINT,
    PRIMARY KEY (id)
);

ALTER TABLE patient_sample
    ADD CONSTRAINT fk_patient_sample_diagnosis
    FOREIGN KEY (diagnosis_id)
    REFERENCES diagnosis (id);

ALTER TABLE patient_sample
    ADD CONSTRAINT fk_patient_primary_site
    FOREIGN KEY (primary_site_id)
    REFERENCES tissue (id);

ALTER TABLE patient_sample
    ADD CONSTRAINT fk_patient_collection_site
    FOREIGN KEY (collection_site_id)
    REFERENCES tissue (id);

ALTER TABLE patient_sample
    ADD CONSTRAINT fk_patient_sample_tumour_type
    FOREIGN KEY (tumour_type_id)
    REFERENCES tumour_type (id);

ALTER TABLE patient_sample
    ADD CONSTRAINT fk_patient_sample_model
    FOREIGN KEY (model_id)
    REFERENCES model (id);

CREATE TABLE xenograft_sample (
    id BIGINT NOT NULL,
    external_xenograft_sample_id TEXT,
    PRIMARY KEY (id)
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
    sample_id BIGINT,
    PRIMARY KEY (id)
);

ALTER TABLE patient_snapshot
    ADD CONSTRAINT fk_patient_snapshot_patient
    FOREIGN KEY (patient_id)
    REFERENCES patient (id);

ALTER TABLE patient_snapshot
    ADD CONSTRAINT fk_patient_snapshot_patient_sample
    FOREIGN KEY (sample_id)
    REFERENCES patient_sample (id);

CREATE TABLE engraftment_site (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE engraftment_type (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE engraftment_material (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE engraftment_sample_state (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE engraftment_sample_type (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE host_strain (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    nomenclature TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE project_group (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE treatment (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE response (
    id BIGINT NOT NULL,
    description TEXT,
    classification TEXT,
    PRIMARY KEY (id)
);

CREATE TABLE molecular_characterization_type (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE platform (
    id BIGINT NOT NULL,
    library_strategy TEXT NOT NULL,
    provider_group_id BIGINT,
    instrument_model TEXT,
    library_selection TEXT,
    PRIMARY KEY (id)
);

ALTER TABLE platform
    ADD CONSTRAINT fk_platform_provider_group
    FOREIGN KEY (provider_group_id)
    REFERENCES provider_group (id);

CREATE TABLE molecular_characterization (
    id BIGINT NOT NULL,
    molecular_characterization_type_id BIGINT NOT NULL,
    platform_id BIGINT NOT NULL,
    patient_sample_id BIGINT,
    xenograft_sample_id BIGINT,
    PRIMARY KEY (id)
);

ALTER TABLE molecular_characterization
    ADD CONSTRAINT fk_molecular_characterization_mchar_type
    FOREIGN KEY (molecular_characterization_type_id)
    REFERENCES molecular_characterization_type (id);

ALTER TABLE molecular_characterization
    ADD CONSTRAINT fk_molecular_characterization_platform
    FOREIGN KEY (platform_id)
    REFERENCES platform (id);

ALTER TABLE molecular_characterization
    ADD CONSTRAINT fk_molecular_characterization_patient_sample
    FOREIGN KEY (patient_sample_id)
    REFERENCES patient_sample (id);

ALTER TABLE molecular_characterization
    ADD CONSTRAINT fk_molecular_characterization_xenograft_sample
    FOREIGN KEY (xenograft_sample_id)
    REFERENCES xenograft_sample (id);

CREATE TABLE cna_molecular_data (
    id BIGINT NOT NULL,
    log10r_cna TEXT,
    log2r_cna TEXT,
    copy_number_status TEXT,
    gistic_value TEXT,
    picnic_value TEXT,
    --gene_marker_id BIGINT,
    molecular_characterization_id BIGINT,
    --loci_marker_id BIGINT,
    PRIMARY KEY (id)
);

ALTER TABLE cna_molecular_data
    ADD CONSTRAINT fk_cna_molecular_data_mol_char
    FOREIGN KEY (molecular_characterization_id)
    REFERENCES molecular_characterization (id);

CREATE TABLE cytogenetics_molecular_data (
    id BIGINT NOT NULL,
    marker_status TEXT,
    essential_or_additional_marker TEXT,
    --gene_marker_id BIGINT,
    tmp_symbol TEXT,
    molecular_characterization_id BIGINT,
    PRIMARY KEY (id)
);

ALTER TABLE cytogenetics_molecular_data
    ADD CONSTRAINT fk_cytogenetics_molecular_data_mol_char
    FOREIGN KEY (molecular_characterization_id)
    REFERENCES molecular_characterization (id);

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
    molecular_characterization_id BIGINT,
    --loci_marker_id BIGINT,
    PRIMARY KEY (id)
);

ALTER TABLE expression_molecular_data
    ADD CONSTRAINT fk_expression_molecular_data_mol_char
    FOREIGN KEY (molecular_characterization_id)
    REFERENCES molecular_characterization (id);

CREATE TABLE mutation_marker (
    id BIGINT NOT NULL,
    biotype TEXT,
    coding_sequence_change TEXT,
    variant_class TEXT,
    codon_change TEXT,
    aminoacid_change TEXT,
    consequence TEXT,
    functional_prediction TEXT,
    seq_start_position TEXT,
    ref_allele TEXT,
    alt_allele TEXT,
    ncbi_transcript_id TEXT,
    ensembl_transcript_id TEXT,
    variation_id TEXT,
    --gene_marker_id BIGINT,
    tmp_symbol TEXT,
    --loci_marker_id BIGINT,
    PRIMARY KEY (id)
);