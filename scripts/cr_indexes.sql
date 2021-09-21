ALTER TABLE diagnosis ADD CONSTRAINT pk_diagnosis PRIMARY KEY  (id);
ALTER TABLE ethnicity ADD CONSTRAINT pk_ethnicity PRIMARY KEY (id);
ALTER TABLE provider_type ADD CONSTRAINT pk_provider_type PRIMARY KEY (id);
ALTER TABLE provider_group ADD CONSTRAINT pk_provider_group PRIMARY KEY (id);
ALTER TABLE patient ADD CONSTRAINT pk_patient PRIMARY KEY (id);
ALTER TABLE publication_group ADD CONSTRAINT pk_publication_group PRIMARY KEY (id);
ALTER TABLE accessibility_group ADD CONSTRAINT pk_accessibility_group PRIMARY KEY (id);
ALTER TABLE contact_people ADD CONSTRAINT pk_contact_people PRIMARY KEY (id);
ALTER TABLE contact_form ADD CONSTRAINT pk_contact_form PRIMARY KEY (id);
ALTER TABLE source_database ADD CONSTRAINT pk_source_database PRIMARY KEY (id);
ALTER TABLE model ADD CONSTRAINT pk_model PRIMARY KEY (id);
ALTER TABLE quality_assurance ADD CONSTRAINT pk_quality_assurance PRIMARY KEY (id);
ALTER TABLE tissue ADD CONSTRAINT pk_tissue PRIMARY KEY (id);
ALTER TABLE tumour_type ADD CONSTRAINT pk_tumour_type PRIMARY KEY (id);
ALTER TABLE patient_sample ADD CONSTRAINT pk_patient_sample PRIMARY KEY (id);
ALTER TABLE xenograft_sample ADD CONSTRAINT pk_xenograft_sample PRIMARY KEY (id);
ALTER TABLE patient_snapshot ADD CONSTRAINT pk_patient_snapshot PRIMARY KEY (id);


ALTER TABLE engraftment_site ADD CONSTRAINT pk_engraftment_site PRIMARY KEY (id);
ALTER TABLE engraftment_material ADD CONSTRAINT pk_engraftment_material PRIMARY KEY (id);
ALTER TABLE engraftment_sample_state ADD CONSTRAINT pk_engraftment_sample_state PRIMARY KEY (id);
ALTER TABLE engraftment_sample_type ADD CONSTRAINT pk_engraftment_sample_type PRIMARY KEY (id);
ALTER TABLE engraftment_type ADD CONSTRAINT pk_engraftment_type PRIMARY KEY (id);

ALTER TABLE host_strain ADD CONSTRAINT pk_host_strain PRIMARY KEY (id);
ALTER TABLE project_group ADD CONSTRAINT pk_project_group PRIMARY KEY (id);
ALTER TABLE treatment ADD CONSTRAINT pk_treatment PRIMARY KEY (id);
ALTER TABLE response ADD CONSTRAINT pk_response PRIMARY KEY (id);
ALTER TABLE molecular_characterization_type ADD CONSTRAINT pk_molecular_characterization_type PRIMARY KEY (id);
ALTER TABLE platform ADD CONSTRAINT pk_platform PRIMARY KEY (id);
ALTER TABLE molecular_characterization ADD CONSTRAINT pk_molecular_characterization PRIMARY KEY (id);
ALTER TABLE cna_molecular_data ADD CONSTRAINT pk_cna_molecular_data PRIMARY KEY (id);
ALTER TABLE cytogenetics_molecular_data ADD CONSTRAINT pk_cytogenetics_molecular_data PRIMARY KEY (id);
ALTER TABLE expression_molecular_data ADD CONSTRAINT pk_expression_molecular_data PRIMARY KEY (id);
ALTER TABLE mutation_marker ADD CONSTRAINT pk_mutation_marker PRIMARY KEY (id);
ALTER TABLE mutation_measurement_data ADD CONSTRAINT pk_mutation_measurement_data PRIMARY KEY (id);
ALTER TABLE specimen ADD CONSTRAINT pk_specimen PRIMARY KEY (id);
