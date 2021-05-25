CREATE TABLE diagnosis (
    id INTEGER NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
);

CREATE TABLE ethnicity (
    id INTEGER NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
);

CREATE TABLE provider_type (
    id INTEGER NOT NULL,
    name TEXT,
    PRIMARY KEY (id)
);

CREATE TABLE provider_group (
    id INTEGER NOT NULL,
    name TEXT,
    abbreviation TEXT,
    provider_type_id INTEGER,
    PRIMARY KEY (id)
);

ALTER TABLE provider_group
    ADD CONSTRAINT fk_provider_group_provider_type
    FOREIGN KEY (provider_type_id)
    REFERENCES provider_type (id);

CREATE TABLE patient (
    id INTEGER NOT NULL,
    external_id TEXT NOT NULL,
    sex TEXT NOT NULL,
    history TEXT,
    ethnicity_id INTEGER,
    ethnicity_assessment_method TEXT,
    initial_diagnosis_id INTEGER,
    age_at_initial_diagnosis TEXT,
    provider_group_id INTEGER,
    PRIMARY KEY (id),
    CONSTRAINT uq_external_id UNIQUE(external_id)
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
    id INTEGER NOT NULL,
    pub_med_id TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE model (
    id INTEGER NOT NULL,
    source_pdx_id TEXT,
    data_source varchar,
    publication_group_id INTEGER,
    PRIMARY KEY (id)
);

ALTER TABLE model
    ADD CONSTRAINT fk_model_publication_group
    FOREIGN KEY (publication_group_id)
    REFERENCES publication_group (id);

CREATE TABLE tissue (
    id INTEGER NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE tumour_type (
    id INTEGER NOT NULL,
    name TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE patient_sample (
    id INTEGER NOT NULL,
    diagnosis_id INTEGER,
    source_sample_id TEXT,
    grade TEXT,
    grade_classification TEXT,
    stage TEXT,
    stage_classification TEXT,
    origin_tissue_id INTEGER,
    sample_site_id INTEGER,
    raw_data_url TEXT,
    tumour_type_id INTEGER,
    model_id INTEGER,
    PRIMARY KEY (id)
);

ALTER TABLE patient_sample
    ADD CONSTRAINT fk_patient_sample_diagnosis
    FOREIGN KEY (diagnosis_id)
    REFERENCES diagnosis (id);

ALTER TABLE patient_sample
    ADD CONSTRAINT fk_patient_sample_tissue_ori
    FOREIGN KEY (origin_tissue_id)
    REFERENCES tissue (id);

ALTER TABLE patient_sample
    ADD CONSTRAINT fk_patient_sample_tissue_site
    FOREIGN KEY (sample_site_id)
    REFERENCES tissue (id);

ALTER TABLE patient_sample
    ADD CONSTRAINT fk_patient_sample_tumour_type
    FOREIGN KEY (tumour_type_id)
    REFERENCES tumour_type (id);

ALTER TABLE patient_sample
    ADD CONSTRAINT fk_patient_sample_model
    FOREIGN KEY (model_id)
    REFERENCES model (id);
