ALTER TABLE ethnicity DROP CONSTRAINT IF EXISTS pk_ethnicity CASCADE;
ALTER TABLE ethnicity ADD CONSTRAINT pk_ethnicity PRIMARY KEY (id);

ALTER TABLE provider_type DROP CONSTRAINT IF EXISTS pk_provider_type CASCADE;
ALTER TABLE provider_type ADD CONSTRAINT pk_provider_type PRIMARY KEY (id);

ALTER TABLE provider_group DROP CONSTRAINT IF EXISTS pk_provider_group CASCADE;
ALTER TABLE provider_group ADD CONSTRAINT pk_provider_group PRIMARY KEY (id);

ALTER TABLE patient DROP CONSTRAINT IF EXISTS pk_patient CASCADE;
ALTER TABLE patient ADD CONSTRAINT pk_patient PRIMARY KEY (id);

ALTER TABLE publication_group DROP CONSTRAINT IF EXISTS pk_publication_group CASCADE;
ALTER TABLE publication_group ADD CONSTRAINT pk_publication_group PRIMARY KEY (id);

ALTER TABLE accessibility_group DROP CONSTRAINT IF EXISTS pk_accessibility_group CASCADE;
ALTER TABLE accessibility_group ADD CONSTRAINT pk_accessibility_group PRIMARY KEY (id);

ALTER TABLE contact_people DROP CONSTRAINT IF EXISTS pk_contact_people CASCADE;
ALTER TABLE contact_people ADD CONSTRAINT pk_contact_people PRIMARY KEY (id);

ALTER TABLE contact_form DROP CONSTRAINT IF EXISTS pk_contact_form CASCADE;
ALTER TABLE contact_form ADD CONSTRAINT pk_contact_form PRIMARY KEY (id);

ALTER TABLE source_database DROP CONSTRAINT IF EXISTS pk_source_database CASCADE;
ALTER TABLE source_database ADD CONSTRAINT pk_source_database PRIMARY KEY (id);

ALTER TABLE model_information DROP CONSTRAINT IF EXISTS pk_model_information CASCADE;
ALTER TABLE model_information ADD CONSTRAINT pk_model_information PRIMARY KEY (id);

ALTER TABLE license DROP CONSTRAINT IF EXISTS pk_license CASCADE;
ALTER TABLE license ADD CONSTRAINT pk_license PRIMARY KEY (id);

ALTER TABLE cell_model DROP CONSTRAINT IF EXISTS pk_cell_model CASCADE;
ALTER TABLE cell_model ADD CONSTRAINT pk_cell_model PRIMARY KEY (id);

ALTER TABLE cell_sample DROP CONSTRAINT IF EXISTS pk_cell_sample CASCADE;
ALTER TABLE cell_sample ADD CONSTRAINT pk_cell_sample PRIMARY KEY (id);

ALTER TABLE quality_assurance DROP CONSTRAINT IF EXISTS pk_quality_assurance CASCADE;
ALTER TABLE quality_assurance ADD CONSTRAINT pk_quality_assurance PRIMARY KEY (id);

ALTER TABLE tissue DROP CONSTRAINT IF EXISTS pk_tissue CASCADE;
ALTER TABLE tissue ADD CONSTRAINT pk_tissue PRIMARY KEY (id);

ALTER TABLE tumour_type DROP CONSTRAINT IF EXISTS pk_tumour_type CASCADE;
ALTER TABLE tumour_type ADD CONSTRAINT pk_tumour_type PRIMARY KEY (id);

ALTER TABLE patient_sample DROP CONSTRAINT IF EXISTS pk_patient_sample CASCADE;
ALTER TABLE patient_sample ADD CONSTRAINT pk_patient_sample PRIMARY KEY (id);

ALTER TABLE xenograft_sample DROP CONSTRAINT IF EXISTS pk_xenograft_sample CASCADE;
ALTER TABLE xenograft_sample ADD CONSTRAINT pk_xenograft_sample PRIMARY KEY (id);

ALTER TABLE engraftment_type DROP CONSTRAINT IF EXISTS pk_engraftment_type CASCADE;
ALTER TABLE engraftment_type ADD CONSTRAINT pk_engraftment_type PRIMARY KEY (id);

ALTER TABLE engraftment_site DROP CONSTRAINT IF EXISTS pk_engraftment_site CASCADE;
ALTER TABLE engraftment_site ADD CONSTRAINT pk_engraftment_site PRIMARY KEY (id);

ALTER TABLE engraftment_sample_state DROP CONSTRAINT IF EXISTS pk_engraftment_sample_state CASCADE;
ALTER TABLE engraftment_sample_state ADD CONSTRAINT pk_engraftment_sample_state PRIMARY KEY (id);

ALTER TABLE engraftment_sample_type DROP CONSTRAINT IF EXISTS pk_engraftment_sample_type CASCADE;
ALTER TABLE engraftment_sample_type ADD CONSTRAINT pk_engraftment_sample_type PRIMARY KEY (id);

ALTER TABLE host_strain DROP CONSTRAINT IF EXISTS pk_host_strain CASCADE;
ALTER TABLE host_strain ADD CONSTRAINT pk_host_strain PRIMARY KEY (id);

ALTER TABLE project_group DROP CONSTRAINT IF EXISTS pk_project_group CASCADE;
ALTER TABLE project_group ADD CONSTRAINT pk_project_group PRIMARY KEY (id);

ALTER TABLE treatment DROP CONSTRAINT IF EXISTS pk_treatment CASCADE;
ALTER TABLE treatment ADD CONSTRAINT pk_treatment PRIMARY KEY (id);

ALTER TABLE response DROP CONSTRAINT IF EXISTS pk_response CASCADE;
ALTER TABLE response ADD CONSTRAINT pk_response PRIMARY KEY (id);

ALTER TABLE response_classification DROP CONSTRAINT IF EXISTS pk_response_classification CASCADE;
ALTER TABLE response_classification ADD CONSTRAINT pk_response_classification PRIMARY KEY (id);

ALTER TABLE molecular_characterization_type DROP CONSTRAINT IF EXISTS pk_molecular_characterization_type CASCADE;
ALTER TABLE molecular_characterization_type ADD CONSTRAINT pk_molecular_characterization_type PRIMARY KEY (id);

ALTER TABLE platform DROP CONSTRAINT IF EXISTS pk_platform CASCADE;
ALTER TABLE platform ADD CONSTRAINT pk_platform PRIMARY KEY (id);

ALTER TABLE molecular_characterization DROP CONSTRAINT IF EXISTS pk_molecular_characterization CASCADE;
ALTER TABLE molecular_characterization ADD CONSTRAINT pk_molecular_characterization PRIMARY KEY (id);

-- Not creating pks for molchar tables to improve performance under the assumption that the ETL is creating ids that
-- make each record unique.
--ALTER TABLE cna_molecular_data ADD CONSTRAINT pk_cna_molecular_data PRIMARY KEY (id);
--ALTER TABLE biomarker_molecular_data ADD CONSTRAINT pk_biomarker_molecular_data PRIMARY KEY (id);
--ALTER TABLE expression_molecular_data ADD CONSTRAINT pk_expression_molecular_data PRIMARY KEY (id);
--ALTER TABLE mutation_measurement_data ADD CONSTRAINT pk_mutation_measurement_data PRIMARY KEY (id);

CREATE INDEX idx_cna_mol_char ON cna_molecular_data(molecular_characterization_id);
CREATE INDEX idx_biomarker_mol_char ON biomarker_molecular_data(molecular_characterization_id);
CREATE INDEX idx_expression_mol_char ON expression_molecular_data(molecular_characterization_id);
CREATE INDEX idx_mutation_mol_char ON mutation_measurement_data(molecular_characterization_id);

ALTER TABLE xenograft_model_specimen DROP CONSTRAINT IF EXISTS pk_xenograft_model_specimen CASCADE;
ALTER TABLE xenograft_model_specimen ADD CONSTRAINT pk_xenograft_model_specimen PRIMARY KEY (id);

ALTER TABLE gene_marker DROP CONSTRAINT IF EXISTS pk_gene_marker CASCADE;
ALTER TABLE gene_marker ADD CONSTRAINT pk_gene_marker PRIMARY KEY (id);

ALTER TABLE ontology_term_diagnosis DROP CONSTRAINT IF EXISTS pk_ontology_term_diagnosis CASCADE;
ALTER TABLE ontology_term_diagnosis ADD CONSTRAINT pk_ontology_term_diagnosis PRIMARY KEY (id);

ALTER TABLE ontology_term_treatment DROP CONSTRAINT IF EXISTS pk_ontology_term_treatment CASCADE;
ALTER TABLE ontology_term_treatment ADD CONSTRAINT pk_ontology_term_treatment PRIMARY KEY (id);

ALTER TABLE ontology_term_regimen DROP CONSTRAINT IF EXISTS pk_ontology_term_regimen CASCADE;
ALTER TABLE ontology_term_regimen ADD CONSTRAINT pk_ontology_term_regimen PRIMARY KEY (id);

ALTER TABLE sample_to_ontology DROP CONSTRAINT IF EXISTS pk_sample_to_ontology CASCADE;
ALTER TABLE sample_to_ontology ADD CONSTRAINT pk_sample_to_ontology PRIMARY KEY (id);

ALTER TABLE treatment_protocol DROP CONSTRAINT IF EXISTS pk_treatment_protocol CASCADE;
ALTER TABLE treatment_protocol ADD CONSTRAINT pk_treatment_protocol PRIMARY KEY (id);

ALTER TABLE treatment_component DROP CONSTRAINT IF EXISTS pk_treatment_component CASCADE;
ALTER TABLE treatment_component ADD CONSTRAINT pk_treatment_component PRIMARY KEY (id);

ALTER TABLE image_study DROP CONSTRAINT IF EXISTS pk_image_study CASCADE;
ALTER TABLE image_study ADD CONSTRAINT pk_image_study PRIMARY KEY (id);

ALTER TABLE model_image DROP CONSTRAINT IF EXISTS pk_model_image CASCADE;
ALTER TABLE model_image ADD CONSTRAINT pk_model_image PRIMARY KEY (id);

ALTER TABLE node DROP CONSTRAINT IF EXISTS pk_node CASCADE;
ALTER TABLE node ADD CONSTRAINT pk_node PRIMARY KEY (id);

ALTER TABLE edge DROP CONSTRAINT IF EXISTS pk_edge CASCADE;
ALTER TABLE edge ADD CONSTRAINT pk_edge PRIMARY KEY (previous_node, next_node);
