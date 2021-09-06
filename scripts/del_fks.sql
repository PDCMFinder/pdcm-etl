ALTER TABLE provider_group DROP CONSTRAINT IF EXISTS fk_provider_group_provider_type CASCADE;

ALTER TABLE patient DROP CONSTRAINT IF EXISTS fk_patient_diagnosis CASCADE;

ALTER TABLE patient DROP CONSTRAINT IF EXISTS fk_patient_provider_group CASCADE;

ALTER TABLE model DROP CONSTRAINT IF EXISTS fk_model_publication_group CASCADE;

ALTER TABLE model DROP CONSTRAINT IF EXISTS fk_model_accessibility_group CASCADE;

ALTER TABLE model DROP CONSTRAINT IF EXISTS fk_model_contact_people CASCADE;

ALTER TABLE model DROP CONSTRAINT IF EXISTS fk_model_contact_form CASCADE;

ALTER TABLE model DROP CONSTRAINT IF EXISTS fk_model_source_database CASCADE;

ALTER TABLE quality_assurance DROP CONSTRAINT IF EXISTS fk_quality_assurance_model CASCADE;

ALTER TABLE patient_sample DROP CONSTRAINT IF EXISTS fk_patient_sample_diagnosis CASCADE;

ALTER TABLE patient_sample DROP CONSTRAINT IF EXISTS fk_patient_primary_site CASCADE;

ALTER TABLE patient_sample DROP CONSTRAINT IF EXISTS fk_patient_collection_site CASCADE;

ALTER TABLE patient_sample DROP CONSTRAINT IF EXISTS fk_patient_sample_tumour_type CASCADE;

ALTER TABLE patient_sample DROP CONSTRAINT IF EXISTS fk_patient_sample_model CASCADE;

ALTER TABLE patient_snapshot DROP CONSTRAINT IF EXISTS fk_patient_snapshot_patient CASCADE;

ALTER TABLE patient_snapshot DROP CONSTRAINT IF EXISTS fk_patient_snapshot_patient_sample CASCADE;

ALTER TABLE platform DROP CONSTRAINT IF EXISTS fk_platform_provider_group CASCADE;

ALTER TABLE molecular_characterization DROP CONSTRAINT IF EXISTS fk_molecular_characterization_mchar_type CASCADE;

ALTER TABLE molecular_characterization DROP CONSTRAINT IF EXISTS fk_molecular_characterization_platform CASCADE;

ALTER TABLE molecular_characterization DROP CONSTRAINT IF EXISTS fk_molecular_characterization_patient_sample CASCADE;

ALTER TABLE molecular_characterization DROP CONSTRAINT IF EXISTS fk_molecular_characterization_xenograft_sample CASCADE;

ALTER TABLE cna_molecular_data DROP CONSTRAINT IF EXISTS fk_cna_molecular_data_mol_char CASCADE;

ALTER TABLE cytogenetics_molecular_data DROP CONSTRAINT IF EXISTS fk_cytogenetics_molecular_data_mol_char CASCADE;

ALTER TABLE expression_molecular_data DROP CONSTRAINT IF EXISTS fk_expression_molecular_data_mol_char CASCADE;