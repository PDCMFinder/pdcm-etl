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
    xenograft_sample_id TEXT,
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