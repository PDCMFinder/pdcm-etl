-- molecular_characterization_vw view. In the default schema.
CREATE OR REPLACE VIEW molecular_characterization_vw AS
SELECT
	mi.external_model_id AS model_id,
	mi.data_source,
	mol_char.source,
	mol_char.sample_id,
	xs.passage AS xenograft_passage,
	mol_char.raw_data_url,
  CASE
    WHEN mol_char.data_type = 'biomarker' THEN 'bio markers' ELSE mol_char.data_type 
  END AS data_type,
 	pf.instrument_model AS platform_name,
	mol_char.id AS molecular_characterization_id,
  external_db_links
FROM
(
SELECT
	mc.*, mct.name AS data_type,
	CASE
		WHEN patient_sample_id IS NOT NULL THEN 'patient'
		WHEN xenograft_sample_id IS NOT NULL THEN 'xenograft'
		WHEN cell_sample_id IS NOT NULL THEN 'cell'
		ELSE 'unknown'
	END AS source,
	CASE
		WHEN patient_sample_id IS NOT NULL THEN (SELECT model_id FROM patient_sample WHERE id = patient_sample_id)
		WHEN xenograft_sample_id IS NOT NULL THEN (SELECT model_id FROM xenograft_sample WHERE id = xenograft_sample_id)
		WHEN cell_sample_id IS NOT NULL THEN (SELECT model_id FROM cell_sample WHERE id = cell_sample_id)
	END AS pdcm_model_id,
	CASE
		WHEN patient_sample_id IS NOT NULL THEN (SELECT external_patient_sample_id FROM patient_sample WHERE id = patient_sample_id)
		WHEN xenograft_sample_id IS NOT NULL THEN  (SELECT external_xenograft_sample_id FROM xenograft_sample WHERE id = xenograft_sample_id)
		WHEN cell_sample_id IS NOT NULL THEN (SELECT external_cell_sample_id FROM cell_sample WHERE id = cell_sample_id)
		ELSE null
	END AS sample_id
FROM
  molecular_characterization mc
  JOIN molecular_characterization_type mct ON mc.molecular_characterization_type_id = mct.id
) mol_char
  JOIN model_information mi ON mol_char.pdcm_model_id = mi.id
  JOIN platform pf ON pf.id = mol_char.platform_id
  LEFT JOIN xenograft_sample xs ON xs.id = mol_char.xenograft_sample_id;

COMMENT ON VIEW molecular_characterization_vw IS
  $$Molecular characterization

  Molecular characterization with model external ids.$$;

COMMENT ON COLUMN molecular_characterization_vw.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN molecular_characterization_vw.data_source IS 'Data source of the model (provider abbreviation)';
COMMENT ON COLUMN molecular_characterization_vw.source IS 'Sample origin (patient, xenograft, cell, unknown)';
COMMENT ON COLUMN molecular_characterization_vw.sample_id IS 'Sample external id';
COMMENT ON COLUMN molecular_characterization_vw.xenograft_passage IS 'Unique identifier for the xenograft sample. Given by the provider';
COMMENT ON COLUMN molecular_characterization_vw.raw_data_url IS 'Identifiers used to build links to external resources with raw data';
COMMENT ON COLUMN molecular_characterization_vw.data_type IS 'Molecular data type';
COMMENT ON COLUMN molecular_characterization_vw.platform_name IS 'Platform instrument_model';
COMMENT ON COLUMN molecular_characterization_vw.molecular_characterization_id IS 'Reference to the molecular_characterization_type table';
COMMENT ON COLUMN molecular_characterization_vw.external_db_links IS 'JSON column with links to external resources';


-- model_information view

DROP VIEW IF EXISTS pdcm_api.model_information CASCADE;

CREATE VIEW pdcm_api.model_information AS
 SELECT * from model_information;

COMMENT ON VIEW pdcm_api.model_information IS
  $$Model information (without joins)

  Model creation.$$;

COMMENT ON COLUMN pdcm_api.model_information.id IS 'Internal identifier';
COMMENT ON COLUMN pdcm_api.model_information.external_model_id IS 'Unique identifier of the model. Given by the provider';
COMMENT ON COLUMN pdcm_api.model_information.data_source IS 'Abbreviation of the provider. Added explicitly here to help with queries';
COMMENT ON COLUMN pdcm_api.model_information.publication_group_id IS 'Reference to the publication_group table. Corresponds to the publications the model is part of';
COMMENT ON COLUMN pdcm_api.model_information.accessibility_group_id IS 'Reference to the accessibility_group table';
COMMENT ON COLUMN pdcm_api.model_information.contact_people_id IS 'Reference to the contact_people table';
COMMENT ON COLUMN pdcm_api.model_information.contact_form_id IS 'Reference to the contact_form table';
COMMENT ON COLUMN pdcm_api.model_information.source_database_id IS 'Reference to the source_database table';
COMMENT ON COLUMN pdcm_api.model_information.license_id IS 'Reference to the license table';

-- model_metadata view

DROP VIEW IF EXISTS pdcm_api.model_metadata CASCADE;

CREATE VIEW pdcm_api.model_metadata AS
SELECT
  mi.external_model_id AS model_id,
  mi.data_source,
  si.provider_name,
  si.model_type AS type,
  hs.name AS host_strain_name,
  hs.nomenclature AS host_strain_nomenclature,
  es.name AS engraftment_site,
  et.name AS engraftment_type,
  est.name AS engraftment_sample_type,
  ess.name AS engraftment_sample_state,
  xms.passage_number,
  si.histology,
  si.cancer_system,
  si.primary_site,
  si.collection_site,
  si.tumour_type AS tumor_type,
  si.cancer_grade,
  si.cancer_grading_system,
  si.cancer_stage,
  si.patient_age,
  si.patient_sex,
  si.patient_ethnicity,
  si.patient_treatment_status,
  pg.pubmed_ids,
  ag.europdx_access_modalities,
  ag.accessibility,
  cp.name_list AS contact_name_list,
  cp.email_list AS contact_email_list,
  cf.form_url AS contact_form_url,
  sd.database_url AS source_database_url
FROM
  model_information mi
  JOIN search_index si ON si.pdcm_model_id = mi.id
  LEFT JOIN xenograft_model_specimen xms ON xms.model_id = mi.id
  LEFT JOIN host_strain hs ON hs.id = xms.host_strain_id
  LEFT JOIN engraftment_site es ON es.id = xms.engraftment_site_id
  LEFT JOIN engraftment_type et ON et.id = xms.engraftment_type_id
  LEFT JOIN engraftment_sample_type est ON est.id = xms.engraftment_sample_type_id
  LEFT JOIN engraftment_sample_state ess ON ess.id = xms.engraftment_sample_state_id
  LEFT JOIN publication_group pg ON pg.id = mi.publication_group_id
  LEFT JOIN accessibility_group ag ON ag.id = mi.accessibility_group_id
  LEFT JOIN contact_people cp ON cp.id = mi.contact_people_id
  LEFT JOIN contact_form cf ON cf.id = mi.contact_form_id
  LEFT JOIN source_database sd ON sd.id = mi.source_database_id;


COMMENT ON VIEW pdcm_api.model_metadata IS
  $$Model metadata (joined with other tables)

  Metadata associated to a model.$$;

COMMENT ON COLUMN pdcm_api.model_metadata.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN pdcm_api.model_metadata.data_source IS 'Data source of the model (provider abbreviation)';
COMMENT ON COLUMN pdcm_api.model_metadata.provider_name IS 'Provider full name';
COMMENT ON COLUMN pdcm_api.model_metadata.type IS 'Model type (xenograft, cell_line, …)';
COMMENT ON COLUMN pdcm_api.model_metadata.host_strain_name IS 'Host mouse strain name (e.g. NOD-SCID, NSG, etc)';
COMMENT ON COLUMN pdcm_api.model_metadata.host_strain_nomenclature IS 'The full nomenclature form of the host mouse strain name';
COMMENT ON COLUMN pdcm_api.model_metadata.engraftment_site IS 'Organ or anatomical site used for the tumour engraftment (e.g. mammary fat pad, Right flank)';
COMMENT ON COLUMN pdcm_api.model_metadata.engraftment_type IS 'Orthotopic: if the tumour was engrafted at a corresponding anatomical site. Heterotopic: If grafted subcuteanously';
COMMENT ON COLUMN pdcm_api.model_metadata.engraftment_sample_type IS 'Description of the type of material grafted into the mouse. (e.g. tissue fragments, cell suspension)';
COMMENT ON COLUMN pdcm_api.model_metadata.engraftment_sample_state IS 'PDX Engraftment material state (e.g. fresh or frozen)';
COMMENT ON COLUMN pdcm_api.model_metadata.passage_number IS 'Passage number';
COMMENT ON COLUMN pdcm_api.model_metadata.histology IS 'Diagnosis at time of collection of the patient tumor';
COMMENT ON COLUMN pdcm_api.model_metadata.cancer_system IS 'Cancer System';
COMMENT ON COLUMN pdcm_api.model_metadata.primary_site IS 'Site of the primary tumor where primary cancer is originating from (may not correspond to the site of the current tissue sample)';
COMMENT ON COLUMN pdcm_api.model_metadata.collection_site IS 'Site of collection of the tissue sample (can be different than the primary site if tumour type is metastatic)';
COMMENT ON COLUMN pdcm_api.model_metadata.tumor_type IS 'Collected tumor type';
COMMENT ON COLUMN pdcm_api.model_metadata.cancer_grade IS 'The implanted tumor grade value';
COMMENT ON COLUMN pdcm_api.model_metadata.cancer_grading_system IS 'Stage classification system used to describe the stage';
COMMENT ON COLUMN pdcm_api.model_metadata.cancer_stage IS 'Stage of the patient at the time of collection';
COMMENT ON COLUMN pdcm_api.model_metadata.patient_age IS 'Patient age at collection';
COMMENT ON COLUMN pdcm_api.model_metadata.patient_sex IS 'Sex of the patient';
COMMENT ON COLUMN pdcm_api.model_metadata.patient_ethnicity IS 'Patient Ethnic group';
COMMENT ON COLUMN pdcm_api.model_metadata.patient_treatment_status IS 'Patient treatment status';
COMMENT ON COLUMN pdcm_api.model_metadata.pubmed_ids IS 'PubMed ids related to the model';
COMMENT ON COLUMN pdcm_api.model_metadata.europdx_access_modalities IS 'If a model is part of EUROPDX consortium, then this field defines if the model is accessible for transnational access through the EDIReX infrastructure, or only on a collaborative basis';
COMMENT ON COLUMN pdcm_api.model_metadata.accessibility IS 'Defines any limitation of access of the model per type of users like academia only, industry and academia, or national limitation';
COMMENT ON COLUMN pdcm_api.model_metadata.contact_name_list IS 'List of names of the contact people for the model';
COMMENT ON COLUMN pdcm_api.model_metadata.contact_email_list IS 'Emails of the contact names for the model';
COMMENT ON COLUMN pdcm_api.model_metadata.contact_form_url IS 'URL to the providers resource for each model';
COMMENT ON COLUMN pdcm_api.model_metadata.source_database_url IS 'URL to the source database for each model';


-- model_quality_assurance view

DROP VIEW IF EXISTS pdcm_api.model_quality_assurance CASCADE;

CREATE VIEW pdcm_api.model_quality_assurance AS
SELECT
  mi.external_model_id AS model_id,
  mi.data_source,
  qa.description,
  qa.passages_tested,
  qa.validation_technique,
  qa.validation_host_strain_nomenclature
FROM
  quality_assurance qa
  JOIN model_information mi ON qa.model_id = mi.id;

COMMENT ON VIEW pdcm_api.model_quality_assurance IS
  $$Quality assurance

  Quality assurance data related to a model.$$;

COMMENT ON COLUMN pdcm_api.model_quality_assurance.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN pdcm_api.model_quality_assurance.data_source IS 'Data source of the model (provider abbreviation)';
COMMENT ON COLUMN pdcm_api.model_quality_assurance.description IS 'Short description of what was compared and what was the result: (e.g. high, good, moderate concordance between xenograft, ''model validated against histological features of same diagnosis'' or ''not determined'')';
COMMENT ON COLUMN pdcm_api.model_quality_assurance.passages_tested IS 'List of all passages where validation was performed. Passage 0 correspond to first engraftment';
COMMENT ON COLUMN pdcm_api.model_quality_assurance.validation_technique IS 'Any technique used to validate PDX against their original patient tumour, including fingerprinting, histology, immunohistochemistry';
COMMENT ON COLUMN pdcm_api.model_quality_assurance.validation_host_strain_nomenclature IS 'Validation host mouse strain, following mouse strain nomenclature from MGI JAX';

-- contact_people view

DROP VIEW IF EXISTS pdcm_api.contact_people CASCADE;

CREATE VIEW pdcm_api.contact_people AS
 SELECT * from contact_people;

COMMENT ON VIEW pdcm_api.contact_people IS 'Contact information';
COMMENT ON COLUMN pdcm_api.contact_people.id IS 'Internal identifier';
COMMENT ON COLUMN pdcm_api.contact_people.name_list IS 'Contact person (should match that included in email_list column)';
COMMENT ON COLUMN pdcm_api.contact_people.email_list IS 'Contact email for any requests from users about models. If multiple, included as comma separated list';


-- contact_form view

DROP VIEW IF EXISTS pdcm_api.contact_form CASCADE;

CREATE VIEW pdcm_api.contact_form AS
 SELECT * from contact_form;

COMMENT ON VIEW pdcm_api.contact_form IS 'Contact form';
COMMENT ON COLUMN pdcm_api.contact_form.id IS 'Internal identifier';
COMMENT ON COLUMN pdcm_api.contact_form.form_url IS 'Contact form link';

-- source_database view

DROP VIEW IF EXISTS pdcm_api.source_database CASCADE;

CREATE VIEW pdcm_api.source_database AS
 SELECT * from source_database;

COMMENT ON VIEW pdcm_api.source_database IS 'Institution public database';
COMMENT ON COLUMN pdcm_api.source_database.id IS 'Internal identifier';
COMMENT ON COLUMN pdcm_api.source_database.database_url IS 'Link of the institution public database';

-- engraftment_site view

DROP VIEW IF EXISTS pdcm_api.engraftment_site CASCADE;

CREATE VIEW pdcm_api.engraftment_site AS
 SELECT * from engraftment_site;

COMMENT ON VIEW pdcm_api.engraftment_site IS 'Organ or anatomical site used for the PDX tumour engraftment (e.g. mammary fat pad, Right flank)';
COMMENT ON COLUMN pdcm_api.engraftment_site.id IS 'Internal identifier';
COMMENT ON COLUMN pdcm_api.engraftment_site.name IS 'Engraftment site';

-- engraftment_type view

DROP VIEW IF EXISTS pdcm_api.engraftment_type CASCADE;

CREATE VIEW pdcm_api.engraftment_type AS
 SELECT * from engraftment_type;

COMMENT ON VIEW pdcm_api.engraftment_type IS 'PDX Engraftment Type: Orthotopic if the tumour was engrafted at a corresponding anatomical site (e.g. patient tumour of primary site breast was grafted in mouse mammary fat pad). If grafted subcuteanously hererotopic is used';
COMMENT ON COLUMN pdcm_api.engraftment_type.id IS 'Internal identifier';
COMMENT ON COLUMN pdcm_api.engraftment_type.name IS 'Engraftment type';

-- engraftment_sample_type view

DROP VIEW IF EXISTS pdcm_api.engraftment_sample_type CASCADE;

CREATE VIEW pdcm_api.engraftment_sample_type AS
 SELECT * from engraftment_sample_type;

COMMENT ON VIEW pdcm_api.engraftment_sample_type IS 'Description of the type of  material grafted into the mouse. (e.g. tissue fragments, cell suspension)';
COMMENT ON COLUMN pdcm_api.engraftment_sample_type.id IS 'Internal identifier';
COMMENT ON COLUMN pdcm_api.engraftment_sample_type.name IS 'Engraftment sample type';

-- engraftment_sample_state view

DROP VIEW IF EXISTS pdcm_api.engraftment_sample_state CASCADE;

CREATE VIEW pdcm_api.engraftment_sample_state AS
 SELECT * from engraftment_sample_state;

COMMENT ON VIEW pdcm_api.engraftment_sample_state IS 'PDX Engraftment material state (e.g. fresh or frozen)';
COMMENT ON COLUMN pdcm_api.engraftment_sample_state.id IS 'Internal identifier';
COMMENT ON COLUMN pdcm_api.engraftment_sample_state.name IS 'Engraftment sample state';

-- xenograft_model_specimen view

DROP VIEW IF EXISTS pdcm_api.xenograft_model_specimen CASCADE;

CREATE VIEW pdcm_api.xenograft_model_specimen AS
 SELECT * from xenograft_model_specimen;

COMMENT ON VIEW pdcm_api.xenograft_model_specimen IS 'Xenograft Model Specimen. Represents a Xenografted mouse that has participated in the line creation and characterisation in some meaningful way.  E.g., the specimen provided a tumor that was characterized and used as quality assurance or drug dosing data';
COMMENT ON COLUMN pdcm_api.xenograft_model_specimen.id IS 'Internal identifier';
COMMENT ON COLUMN pdcm_api.xenograft_model_specimen.passage_number IS 'Indicate the passage number of the sample where the PDX sample was harvested (where passage 0 corresponds to first engraftment)';
COMMENT ON COLUMN pdcm_api.xenograft_model_specimen.engraftment_site_id IS 'Reference to the engraftment_site table';
COMMENT ON COLUMN pdcm_api.xenograft_model_specimen.engraftment_type_id IS 'Reference to the engraftment_type table';
COMMENT ON COLUMN pdcm_api.xenograft_model_specimen.engraftment_sample_type_id IS 'Reference to the engraftment_sample_type type';
COMMENT ON COLUMN pdcm_api.xenograft_model_specimen.engraftment_sample_state_id IS 'Reference to the engraftment_sample_state table';
COMMENT ON COLUMN pdcm_api.xenograft_model_specimen.host_strain_id IS 'Reference to the host_strain table';
COMMENT ON COLUMN pdcm_api.xenograft_model_specimen.model_id IS 'Reference to the model_information table';

-- host_strain view

DROP VIEW IF EXISTS pdcm_api.host_strain CASCADE;

CREATE VIEW pdcm_api.host_strain AS
 SELECT * from host_strain;

COMMENT ON VIEW pdcm_api.host_strain IS 'Host strain information';
COMMENT ON COLUMN pdcm_api.host_strain.id IS 'Internal identifier';
COMMENT ON COLUMN pdcm_api.host_strain.name IS 'Host mouse strain name (e.g. NOD-SCID, NSG, etc)';
COMMENT ON COLUMN pdcm_api.host_strain.nomenclature IS 'The full nomenclature form of the host mouse strain name';

-- quality_assurance view

DROP VIEW IF EXISTS pdcm_api.quality_assurance CASCADE;

CREATE VIEW pdcm_api.quality_assurance AS
 SELECT * from quality_assurance;

COMMENT ON VIEW pdcm_api.quality_assurance IS 'Cell Sample';
COMMENT ON COLUMN pdcm_api.quality_assurance.id IS 'Internal identifier';
COMMENT ON COLUMN pdcm_api.quality_assurance.description IS 'Short description of what was compared and what was the result: (e.g. high, good, moderate concordance between xenograft, ''model validated against histological features of same diagnosis'' or ''not determined'') - It needs to be clear if the model is validated or not.';
COMMENT ON COLUMN pdcm_api.quality_assurance.passages_tested IS 'Provide a list of all passages where validation was performed. Passage 0 correspond to first engraftment (if this is not the case please define how passages are numbered)';
COMMENT ON COLUMN pdcm_api.quality_assurance.validation_technique IS 'Any technique used to validate PDX against their original patient tumour, including fingerprinting, histology, immunohistochemistry';
COMMENT ON COLUMN pdcm_api.quality_assurance.validation_host_strain_nomenclature IS 'Validation host mouse strain, following mouse strain nomenclature from MGI JAX';
COMMENT ON COLUMN pdcm_api.quality_assurance.model_id IS 'Reference to the model_information table';

-- publication_group view

DROP VIEW IF EXISTS pdcm_api.publication_group CASCADE;

CREATE VIEW pdcm_api.publication_group AS
 SELECT * from publication_group;

COMMENT ON VIEW pdcm_api.publication_group IS 'Groups of publications that are associated to one or more models';
COMMENT ON COLUMN pdcm_api.publication_group.id IS 'Internal identifier';
COMMENT ON COLUMN pdcm_api.publication_group.pubmed_ids IS 'pubmed IDs separated by commas';

-- mutation_data_table view

DROP VIEW IF EXISTS pdcm_api.mutation_data_table CASCADE;

CREATE VIEW pdcm_api.mutation_data_table
AS SELECT mmd.molecular_characterization_id,
          mmd.hgnc_symbol,
          mmd.non_harmonised_symbol,
          mmd.amino_acid_change,
          mmd.chromosome,
          mmd.strand,
          mmd.consequence,
          mmd.read_depth,
          mmd.allele_frequency,
          mmd.seq_start_position,
          mmd.ref_allele,
          mmd.alt_allele,
          mmd.biotype,
          mmd.external_db_links,
          mmd.data_source,
          ( mmd.* ) :: text AS text
   FROM   mutation_measurement_data mmd
   WHERE (mmd.data_source, 'mutation_measurement_data') NOT IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction);

COMMENT ON VIEW pdcm_api.mutation_data_table IS 'Mutation measurement data';
COMMENT ON COLUMN pdcm_api.mutation_data_table.molecular_characterization_id IS 'Reference to the molecular_characterization_ table';
COMMENT ON COLUMN pdcm_api.mutation_data_table.hgnc_symbol IS 'Gene symbol';
COMMENT ON COLUMN pdcm_api.mutation_data_table.non_harmonised_symbol IS 'Original symbol as reported by the provider';
COMMENT ON COLUMN pdcm_api.mutation_data_table.amino_acid_change IS 'Changes in the amino acid due to the variant';
COMMENT ON COLUMN pdcm_api.mutation_data_table.chromosome IS 'Chromosome where the mutation occurs';
COMMENT ON COLUMN pdcm_api.mutation_data_table.strand IS 'Orientation of the DNA strand where a mutation is located';
COMMENT ON COLUMN pdcm_api.mutation_data_table.consequence IS 'Genomic consequence of this variant, for example: insertion of a codon caused frameshift variation will be considered frameshift variant ';
COMMENT ON COLUMN pdcm_api.mutation_data_table.read_depth IS 'Read depth, the number of times each individual base was sequenced';
COMMENT ON COLUMN pdcm_api.mutation_data_table.allele_frequency IS 'Allele frequency, the relative frequency of an allele in a population';
COMMENT ON COLUMN pdcm_api.mutation_data_table.seq_start_position IS 'Location on the genome at which the variant is found';
COMMENT ON COLUMN pdcm_api.mutation_data_table.ref_allele IS 'The base seen in the reference genome';
COMMENT ON COLUMN pdcm_api.mutation_data_table.alt_allele IS 'The base other than the reference allele seen at the locus';
COMMENT ON COLUMN pdcm_api.mutation_data_table.biotype IS 'Biotype of the transcript or regulatory feature eg. protein coding, non coding';
COMMENT ON COLUMN pdcm_api.mutation_data_table.external_db_links IS 'JSON column with links to external resources';
COMMENT ON COLUMN pdcm_api.mutation_data_table.data_source IS 'Data source (abbreviation of the provider)';
COMMENT ON COLUMN pdcm_api.mutation_data_table.text IS 'Text representation of the row';
-- model_molecular_metadata materialized view: Model molecular metadata

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.model_molecular_metadata CASCADE;

CREATE MATERIALIZED VIEW pdcm_api.model_molecular_metadata AS
 SELECT
	mcv.*,
	CASE
		WHEN mcv.data_type = 'mutation'::text AND EXISTS (SELECT 1 FROM mutation_measurement_data WHERE molecular_characterization_id=mcv.molecular_characterization_id) THEN 'TRUE'::text
		WHEN mcv.data_type = 'expression'::text AND EXISTS (SELECT 1 FROM expression_molecular_data WHERE molecular_characterization_id=mcv.molecular_characterization_id) THEN 'TRUE'::text
		WHEN mcv.data_type = 'copy number alteration'::text AND EXISTS (SELECT 1 FROM cna_molecular_data WHERE molecular_characterization_id=mcv.molecular_characterization_id) THEN 'TRUE'::text
		WHEN mcv.data_type = 'bio markers'::text AND EXISTS (SELECT 1 FROM biomarker_molecular_data WHERE molecular_characterization_id=mcv.molecular_characterization_id) THEN 'TRUE'::text
		ELSE 'FALSE'::text
	END AS data_exists,
	CASE
		WHEN mcv.data_type = 'mutation'::text AND (mcv.data_source, 'mutation_measurement_data') IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction) THEN 'TRUE'::text
		WHEN mcv.data_type = 'expression'::text AND (mcv.data_source, 'expression_molecular_data') IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction) THEN 'TRUE'::text
		WHEN mcv.data_type = 'copy number alteration'::text AND (mcv.data_source, 'cna_molecular_data') IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction) THEN 'TRUE'::text
		WHEN mcv.data_type = 'bio markers'::text AND (mcv.data_source, 'biomarker_molecular_data') IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction) THEN 'TRUE'::text
		ELSE 'FALSE'::text
	END AS data_restricted
FROM molecular_characterization_vw mcv
WHERE mcv.data_type != 'immunemarker'; -- immunemarker data goes in a separate section


COMMENT ON MATERIALIZED VIEW pdcm_api.model_molecular_metadata IS
  $$Model Molecular Metadata

  Information about the molecular data that is available for each sample.$$;

COMMENT ON COLUMN pdcm_api.model_molecular_metadata.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.data_source IS 'Data source of the model';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.source IS '(patient, xenograft, cell)';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.sample_id IS 'Sample identifier given by the provider';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.xenograft_passage IS 'Passage number of the sample';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.raw_data_url IS 'URL where the raw data could be found';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.data_type IS 'Type of molecular data';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.platform_name IS 'Name of the platform technology used to produce the molecular characterization';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.data_exists IS 'True or False depending on whether or not there is molecular data for this sample';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.data_restricted IS 'True or False depending on whether or not the molecular data is restricted (meaning provider need to be contacted directly to get the data from them).';


-- mutation_data_extended view

DROP VIEW IF EXISTS pdcm_api.mutation_data_extended CASCADE;

CREATE VIEW pdcm_api.mutation_data_extended
AS
SELECT
	mmm.model_id,
	mmm.sample_id,
	mmm.source,
	mmd.hgnc_symbol,
	mmd.amino_acid_change,
	mmd.consequence,
	mmd.read_depth,
	mmd.allele_frequency,
	mmd.seq_start_position,
	mmd.ref_allele,
	mmd.alt_allele,
	mmd.data_source,
	mmd.external_db_links
FROM
	mutation_measurement_data mmd, pdcm_api.model_molecular_metadata mmm
WHERE (mmd.data_source, 'mutation_measurement_data') NOT IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction)
AND mmm.molecular_characterization_id = mmd.molecular_characterization_id;

COMMENT ON VIEW pdcm_api.mutation_data_extended IS
  $$Mutation molecular data

  Mutation data with the model and sample it comes from.$$;

COMMENT ON COLUMN pdcm_api.mutation_data_extended.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN pdcm_api.mutation_data_extended.data_source IS 'Data source of the model (provider abbreviation)';
COMMENT ON COLUMN pdcm_api.mutation_data_extended.source IS '(patient, xenograft, cell)';
COMMENT ON COLUMN pdcm_api.mutation_data_extended.sample_id IS 'Sample identifier given by the provider';
COMMENT ON COLUMN pdcm_api.mutation_data_extended.hgnc_symbol IS 'Gene symbol';
COMMENT ON COLUMN pdcm_api.mutation_data_extended.amino_acid_change IS 'Changes in the amino acid due to the variant';
COMMENT ON COLUMN pdcm_api.mutation_data_extended.consequence IS 'Genomic consequence of this variant, for example: insertion of a codon caused frameshift variation will be considered frameshift variant ';
COMMENT ON COLUMN pdcm_api.mutation_data_extended.read_depth IS 'Read depth, the number of times each individual base was sequenced';
COMMENT ON COLUMN pdcm_api.mutation_data_extended.allele_frequency IS 'Allele frequency, the relative frequency of an allele in a population';
COMMENT ON COLUMN pdcm_api.mutation_data_extended.seq_start_position IS 'Location on the genome at which the variant is found';
COMMENT ON COLUMN pdcm_api.mutation_data_extended.ref_allele IS 'The base seen in the reference genome';
COMMENT ON COLUMN pdcm_api.mutation_data_extended.alt_allele IS 'The base other than the reference allele seen at the locus';

-- expression_data_table view

DROP VIEW IF EXISTS pdcm_api.expression_data_table;

CREATE VIEW pdcm_api.expression_data_table
AS
  SELECT emd.molecular_characterization_id,
         emd.hgnc_symbol,
         emd.non_harmonised_symbol,
         emd.rnaseq_coverage,
         emd.rnaseq_fpkm,
         emd.rnaseq_tpm,
         emd.rnaseq_count,
         emd.affy_hgea_probe_id,
         emd.affy_hgea_expression_value,
         emd.illumina_hgea_probe_id,
         emd.illumina_hgea_expression_value,
         emd.z_score,
         emd.external_db_links,
         ( emd.* ) :: text AS text
  FROM   expression_molecular_data emd
  WHERE (emd.data_source, 'expression_molecular_data') NOT IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction);

-- expression_data_extended view

DROP VIEW IF EXISTS pdcm_api.expression_data_extended;

CREATE VIEW pdcm_api.expression_data_extended
AS
SELECT
	mmm.model_id,
	mmm.data_source,
	mmm.source,
	mmm.sample_id,
	emd.hgnc_symbol,
	emd.rnaseq_coverage,
	emd.rnaseq_fpkm,
	emd.rnaseq_tpm,
	emd.rnaseq_count,
	emd.affy_hgea_probe_id,
	emd.affy_hgea_expression_value,
	emd.illumina_hgea_probe_id,
	emd.illumina_hgea_expression_value,
	emd.z_score,
    emd.external_db_links
FROM   expression_molecular_data emd, pdcm_api.model_molecular_metadata mmm
WHERE (emd.data_source, 'expression_molecular_data') NOT IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction)
AND emd.molecular_characterization_id = mmm.molecular_characterization_id;

COMMENT ON VIEW pdcm_api.expression_data_extended IS
  $$Expression molecular data

  Expression data with the model and sample it comes from.$$;

COMMENT ON COLUMN pdcm_api.expression_data_extended.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN pdcm_api.expression_data_extended.data_source IS 'Data source of the model (provider abbreviation)';
COMMENT ON COLUMN pdcm_api.expression_data_extended.source IS '(patient, xenograft, cell)';
COMMENT ON COLUMN pdcm_api.expression_data_extended.sample_id IS 'Sample identifier given by the provider';
COMMENT ON COLUMN pdcm_api.expression_data_extended.hgnc_symbol IS 'Gene symbol';
COMMENT ON COLUMN pdcm_api.expression_data_extended.rnaseq_coverage IS 'The ratio between the number of bases of the mapped reads by the number of bases of a reference';
COMMENT ON COLUMN pdcm_api.expression_data_extended.rnaseq_fpkm IS 'Gene expression value represented in Fragments per kilo base of transcript per million mapped fragments (FPKM)';
COMMENT ON COLUMN pdcm_api.expression_data_extended.rnaseq_tpm IS 'Gene expression value represented in transcript per million (TPM)';
COMMENT ON COLUMN pdcm_api.expression_data_extended.rnaseq_count IS 'Read counts of the gene';
COMMENT ON COLUMN pdcm_api.expression_data_extended.affy_hgea_probe_id IS 'Affymetrix probe identifier';
COMMENT ON COLUMN pdcm_api.expression_data_extended.affy_hgea_expression_value IS 'Expresion value captured using Affymetrix arrays';
COMMENT ON COLUMN pdcm_api.expression_data_extended.illumina_hgea_probe_id IS 'Illumina probe identifier';
COMMENT ON COLUMN pdcm_api.expression_data_extended.illumina_hgea_expression_value IS 'Expresion value captured using Illumina arrays';
COMMENT ON COLUMN pdcm_api.expression_data_extended.z_score IS 'Z-score representing the gene expression level';
COMMENT ON COLUMN pdcm_api.expression_data_extended.external_db_links IS 'Links to external resources';

-- biomarker_data_table view

DROP VIEW IF EXISTS pdcm_api.biomarker_data_table CASCADE;

CREATE VIEW pdcm_api.biomarker_data_table
AS
  SELECT cmd.molecular_characterization_id,
         cmd.biomarker,
         cmd.non_harmonised_symbol,
         cmd.biomarker_status AS result,
         REPLACE(cmd.external_db_links::text, 'hgnc_symbol', 'biomarker')::json AS external_db_links,
         ( cmd.* ) :: text AS text,
         cmd.data_source
  FROM   biomarker_molecular_data cmd
  WHERE (cmd.data_source, 'biomarker_molecular_data') NOT IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction);

COMMENT ON VIEW pdcm_api.biomarker_data_table IS
  $$Biomarker molecular data

  Biomarker data with the model and sample it comes from.$$;

COMMENT ON COLUMN pdcm_api.biomarker_data_table.molecular_characterization_id IS 'Reference to the molecular_characterization_ table';
COMMENT ON COLUMN pdcm_api.biomarker_data_table.biomarker IS 'Gene symbol';
COMMENT ON COLUMN pdcm_api.biomarker_data_table.non_harmonised_symbol IS 'Original symbol as reported by the provider';
COMMENT ON COLUMN pdcm_api.biomarker_data_table.result IS 'Presence or absence of the biomarker';
COMMENT ON COLUMN pdcm_api.biomarker_data_table.external_db_links IS 'Links to external resources';
COMMENT ON COLUMN pdcm_api.biomarker_data_table.text IS 'Text representation of the row';
COMMENT ON COLUMN pdcm_api.biomarker_data_table.data_source IS 'Data source of the model (provider abbreviation)';

-- biomarker_data_extended view

DROP VIEW IF EXISTS pdcm_api.biomarker_data_extended;

CREATE VIEW pdcm_api.biomarker_data_extended
AS
SELECT
	mmm.model_id,
	mmm.data_source,
	mmm.source,
	mmm.sample_id,
	cmd.biomarker,
	cmd.non_harmonised_symbol,
	cmd.biomarker_status AS result,
	cmd.external_db_links
FROM   biomarker_molecular_data cmd, pdcm_api.model_molecular_metadata mmm
WHERE (cmd.data_source, 'biomarker_molecular_data') NOT IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction)
AND cmd.molecular_characterization_id = mmm.molecular_characterization_id;

COMMENT ON VIEW pdcm_api.biomarker_data_extended IS
  $$Biomarker molecular data

  Biomarker data with the model and sample it comes from.$$;

COMMENT ON COLUMN pdcm_api.biomarker_data_extended.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN pdcm_api.biomarker_data_extended.data_source IS 'Data source of the model (provider abbreviation)';
COMMENT ON COLUMN pdcm_api.biomarker_data_extended.source IS '(patient, xenograft, cell)';
COMMENT ON COLUMN pdcm_api.biomarker_data_extended.sample_id IS 'Sample identifier given by the provider';
COMMENT ON COLUMN pdcm_api.biomarker_data_extended.biomarker IS 'Gene symbol';
COMMENT ON COLUMN pdcm_api.biomarker_data_extended.result IS 'Presence or absence of the biomarker';
COMMENT ON COLUMN pdcm_api.biomarker_data_extended.external_db_links IS 'Links to external resources';


-- immunemarker_data_table view

DROP VIEW IF EXISTS pdcm_api.immunemarker_data_table CASCADE;

CREATE VIEW pdcm_api.immunemarker_data_table
AS
  SELECT imd.molecular_characterization_id,
         imd.marker_type,
         imd.marker_name,
         imd.marker_value,
         imd.essential_or_additional_details
  FROM   immunemarker_molecular_data imd;

COMMENT ON VIEW pdcm_api.immunemarker_data_table IS 'Immunemarker molecular data';
COMMENT ON COLUMN pdcm_api.immunemarker_data_table.molecular_characterization_id IS 'Reference to the molecular_characterization_ table';
COMMENT ON COLUMN pdcm_api.immunemarker_data_table.marker_type IS 'Marker type';
COMMENT ON COLUMN pdcm_api.immunemarker_data_table.marker_name IS 'Name of the immune marker';
COMMENT ON COLUMN pdcm_api.immunemarker_data_table.marker_value IS 'Value or measurement associated with the immune marker';
COMMENT ON COLUMN pdcm_api.immunemarker_data_table.essential_or_additional_details IS 'Additional details or notes about the immune marker';


CREATE OR REPLACE VIEW pdcm_api.immunemarker_data_extended
AS
SELECT
	mcv.model_id,
	mcv.data_source,
	mcv.source,
	mcv.sample_id,
  imd.marker_type,
	imd.marker_name,
	imd.marker_value,
	imd.essential_or_additional_details
FROM   immunemarker_molecular_data imd, molecular_characterization_vw mcv
WHERE imd.molecular_characterization_id = mcv.molecular_characterization_id;


COMMENT ON VIEW pdcm_api.immunemarker_data_extended IS
  $$Immunemarkers molecular data

  Immunemarkers data with the model and sample it comes from.$$;

COMMENT ON COLUMN pdcm_api.immunemarker_data_extended.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN pdcm_api.immunemarker_data_extended.data_source IS 'Data source of the model (provider abbreviation)';
COMMENT ON COLUMN pdcm_api.immunemarker_data_extended.source IS '(patient, xenograft, cell)';
COMMENT ON COLUMN pdcm_api.immunemarker_data_extended.sample_id IS 'Sample identifier given by the provider';
COMMENT ON COLUMN pdcm_api.immunemarker_data_extended.marker_type IS 'Type of the immune marker';
COMMENT ON COLUMN pdcm_api.immunemarker_data_extended.marker_name IS 'Name of the immune marker';
COMMENT ON COLUMN pdcm_api.immunemarker_data_extended.marker_value IS 'Value or measurement associated with the immune marker';
COMMENT ON COLUMN pdcm_api.immunemarker_data_extended.essential_or_additional_details IS 'Additional details or notes about the immune marker';

-- cna_data_table view

DROP VIEW IF EXISTS pdcm_api.cna_data_table;

CREATE VIEW pdcm_api.cna_data_table
AS
  SELECT
         cnamd.id,
         cnamd.hgnc_symbol,
         cnamd.chromosome,
         cnamd.strand,
         cnamd.log10r_cna,
         cnamd.log2r_cna,
         cnamd.seq_start_position,
         cnamd.seq_end_position,
         cnamd.copy_number_status,
         cnamd.gistic_value,
         cnamd.picnic_value,
         cnamd.non_harmonised_symbol,
         cnamd.harmonisation_result,
         cnamd.molecular_characterization_id,
         cnamd.external_db_links,
         ( cnamd.* ) :: text AS text,
         cnamd.data_source
  FROM   cna_molecular_data cnamd
  WHERE (cnamd.data_source, 'cna_molecular_data') NOT IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction);

COMMENT ON VIEW pdcm_api.cna_data_table IS
  $$CNA molecular data

  CNA molecular data without joins to other tables.$$;
COMMENT ON COLUMN pdcm_api.cna_data_table.id IS 'Internal identifier';
COMMENT ON COLUMN pdcm_api.cna_data_table.hgnc_symbol IS 'Gene symbol';
COMMENT ON COLUMN pdcm_api.cna_data_table.chromosome IS 'Chromosome where the DNA copy occurs';
COMMENT ON COLUMN pdcm_api.cna_data_table.strand IS 'Orientation of the DNA strand associated with the observed copy number changes, whether it is the positive or negative strand';
COMMENT ON COLUMN pdcm_api.cna_data_table.log10r_cna IS 'Log10 scaled copy number variation ratio';
COMMENT ON COLUMN pdcm_api.cna_data_table.log2r_cna IS 'Log2 scaled copy number variation ratio';
COMMENT ON COLUMN pdcm_api.cna_data_table.seq_start_position IS 'Starting position of a genomic sequence or region that is associated with a copy number alteration';
COMMENT ON COLUMN pdcm_api.cna_data_table.seq_end_position IS 'Ending position of a genomic sequence or region that is associated with a copy number alteration';
COMMENT ON COLUMN pdcm_api.cna_data_table.copy_number_status IS 'Details whether there was a gain or loss of function. Categorized into gain, loss';
COMMENT ON COLUMN pdcm_api.cna_data_table.gistic_value IS 'Score predicted using GISTIC tool for the copy number variation';
COMMENT ON COLUMN pdcm_api.cna_data_table.picnic_value IS 'Score predicted using PICNIC algorithm for the copy number variation';
COMMENT ON COLUMN pdcm_api.cna_data_table.non_harmonised_symbol IS 'Original symbol as reported by the provider';
COMMENT ON COLUMN pdcm_api.cna_data_table.harmonisation_result IS 'Result of the symbol harmonisation process';
COMMENT ON COLUMN pdcm_api.cna_data_table.molecular_characterization_id IS 'Reference to the molecular_characterization_ table';
COMMENT ON COLUMN pdcm_api.cna_data_table.data_source IS 'Data source (abbreviation of the provider)';
COMMENT ON COLUMN pdcm_api.cna_data_table.external_db_links IS 'JSON column with links to external resources';
  
-- cna_data_extended view

DROP VIEW IF EXISTS pdcm_api.cna_data_extended;

CREATE VIEW pdcm_api.cna_data_extended
AS
SELECT
	mmm.model_id,
	mmm.data_source,
	mmm.source,
	mmm.sample_id,
	cnamd.hgnc_symbol,
	cnamd.chromosome,
  cnamd.strand,
	cnamd.log10r_cna,
	cnamd.log2r_cna,
	cnamd.seq_start_position,
  cnamd.seq_end_position,
	cnamd.copy_number_status,
	cnamd.gistic_value,
	cnamd.picnic_value,
	cnamd.external_db_links
FROM   cna_molecular_data cnamd, pdcm_api.model_molecular_metadata mmm
WHERE (cnamd.data_source, 'cna_molecular_data') NOT IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction)
AND cnamd.molecular_characterization_id = mmm.molecular_characterization_id;

COMMENT ON VIEW pdcm_api.cna_data_extended IS
  $$CNA molecular data

  CNA data with the model and sample it comes from.$$;

COMMENT ON COLUMN pdcm_api.cna_data_extended.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN pdcm_api.cna_data_extended.data_source IS 'Data source of the model (provider abbreviation)';
COMMENT ON COLUMN pdcm_api.cna_data_extended.source IS '(patient, xenograft, cell)';
COMMENT ON COLUMN pdcm_api.cna_data_extended.sample_id IS 'Sample identifier given by the provider';
COMMENT ON COLUMN pdcm_api.cna_data_extended.hgnc_symbol IS 'Gene symbol';
COMMENT ON COLUMN pdcm_api.cna_data_extended.chromosome IS 'Chromosome where the DNA copy occurs';
COMMENT ON COLUMN pdcm_api.cna_data_extended.strand IS 'Orientation of the DNA strand associated with the observed copy number changes, whether it is the positive or negative strand';
COMMENT ON COLUMN pdcm_api.cna_data_extended.log10r_cna IS 'Log10 scaled copy number variation ratio';
COMMENT ON COLUMN pdcm_api.cna_data_extended.log2r_cna IS 'Log2 scaled copy number variation ratio';
COMMENT ON COLUMN pdcm_api.cna_data_extended.seq_start_position IS 'Starting position of a genomic sequence or region that is associated with a copy number alteration';
COMMENT ON COLUMN pdcm_api.cna_data_extended.seq_end_position IS 'Ending position of a genomic sequence or region that is associated with a copy number alteration';
COMMENT ON COLUMN pdcm_api.cna_data_extended.copy_number_status IS 'Details whether there was a gain or loss of function. Categorized into gain, loss';
COMMENT ON COLUMN pdcm_api.cna_data_extended.gistic_value IS 'Score predicted using GISTIC tool for the copy number variation';
COMMENT ON COLUMN pdcm_api.cna_data_extended.picnic_value IS 'Score predicted using PICNIC algorithm for the copy number variation';
COMMENT ON COLUMN pdcm_api.cna_data_extended.external_db_links IS 'Links to external resources';

DROP VIEW IF EXISTS pdcm_api.molecular_data_restriction;

CREATE VIEW pdcm_api.molecular_data_restriction
AS
  SELECT mdr.*
  FROM   molecular_data_restriction mdr;

COMMENT ON VIEW pdcm_api.molecular_data_restriction IS 'Internal table to store molecular tables which data cannot be displayed to the user. Configured at provider level.';
COMMENT ON COLUMN pdcm_api.molecular_data_restriction.data_source IS 'Provider with the restriction';
COMMENT ON COLUMN pdcm_api.molecular_data_restriction.molecular_data_table IS 'Table whose data cannot be showed';

-- search_index materialized view: Index to search for models

DROP VIEW IF EXISTS pdcm_api.search_index;

CREATE VIEW pdcm_api.search_index
AS
 SELECT search_index.*,
        CASE
            WHEN 'publication' = ANY(dataset_available)
                THEN cardinality(dataset_available) - 1
            ELSE
                cardinality(dataset_available)
            END as model_dataset_type_count,
        CASE 
	          WHEN project_name = 'PIVOT'
 	              or lower(histology) like '%childhood%'
	              or patient_age like '%19' or patient_age like '2 - 9'
                or patient_age like '%months' 
            THEN true 
            ELSE false 
        END as paediatric
 FROM search_index;

COMMENT ON VIEW pdcm_api.search_index IS 'Helper table to show results in a search';
COMMENT ON COLUMN pdcm_api.search_index.pdcm_model_id IS 'Internal id of the model';
COMMENT ON COLUMN pdcm_api.search_index.external_model_id IS 'Internal of the model given by the provider';
COMMENT ON COLUMN pdcm_api.search_index.data_source IS 'Datasource (provider abbreviation)';
COMMENT ON COLUMN pdcm_api.search_index.project_name IS 'Project of the model';
COMMENT ON COLUMN pdcm_api.search_index.provider_name IS 'Provider name';
COMMENT ON COLUMN pdcm_api.search_index.model_type IS 'Type of model';
COMMENT ON COLUMN pdcm_api.search_index.histology IS 'Harmonised patient sample diagnosis';
COMMENT ON COLUMN pdcm_api.search_index.search_terms IS 'All diagnosis related (by ontology relations) to the model';
COMMENT ON COLUMN pdcm_api.search_index.cancer_system IS 'Cancer system of the model';
COMMENT ON COLUMN pdcm_api.search_index.dataset_available IS 'List of datasets reported for the model (like cna, expression, publications, etc)';
COMMENT ON COLUMN pdcm_api.search_index.cancer_system IS 'Cancer system of the model';
COMMENT ON COLUMN pdcm_api.search_index.license_name IS 'License name for the model';
COMMENT ON COLUMN pdcm_api.search_index.license_url IS 'Url of the license';
COMMENT ON COLUMN pdcm_api.search_index.primary_site IS 'Site of the primary tumor where primary cancer is originating from (may not correspond to the site of the current tissue sample)';
COMMENT ON COLUMN pdcm_api.search_index.collection_site IS 'Site of collection of the tissue sample (can be different than the primary site if tumour type is metastatic).';
COMMENT ON COLUMN pdcm_api.search_index.tumour_type IS 'Collected tumour type';
COMMENT ON COLUMN pdcm_api.search_index.cancer_grade IS 'The implanted tumour grade value';
COMMENT ON COLUMN pdcm_api.search_index.cancer_grading_system IS 'Grade classification corresponding used to describe the stage, add the version if available';
COMMENT ON COLUMN pdcm_api.search_index.cancer_stage IS 'Stage of the patient at the time of collection';
COMMENT ON COLUMN pdcm_api.search_index.cancer_staging_system IS 'Stage classification system used to describe the stage, add the version if available';
COMMENT ON COLUMN pdcm_api.search_index.patient_age IS 'Patient age at collection';
COMMENT ON COLUMN pdcm_api.search_index.patient_sex IS 'Patient sex';
COMMENT ON COLUMN pdcm_api.search_index.patient_history IS 'Cancer relevant comorbidity or environmental exposure';
COMMENT ON COLUMN pdcm_api.search_index.patient_ethnicity IS 'Patient Ethnic group. Can be derived from self-assessment or genetic analysis';
COMMENT ON COLUMN pdcm_api.search_index.patient_ethnicity_assessment_method IS 'Patient Ethnic group assessment method';
COMMENT ON COLUMN pdcm_api.search_index.patient_initial_diagnosis IS 'Diagnosis of the patient when first diagnosed at age_at_initial_diagnosis - this can be different than the diagnosis at the time of collection which is collected in the sample section';
COMMENT ON COLUMN pdcm_api.search_index.patient_treatment_status IS 'Patient treatment status';
COMMENT ON COLUMN pdcm_api.search_index.patient_age_at_initial_diagnosis IS 'This is the age of first diagnostic. Can be prior to the age at which the tissue sample was collected for implant';
COMMENT ON COLUMN pdcm_api.search_index.patient_sample_id IS 'Patient sample identifier given by the provider';
COMMENT ON COLUMN pdcm_api.search_index.patient_sample_collection_date IS 'Date of collections. Important for understanding the time relationship between models generated for the same patient';
COMMENT ON COLUMN pdcm_api.search_index.patient_sample_collection_event IS 'Collection event corresponding to each time a patient was sampled to generate a cancer model, subsequent collection events are incremented by 1';
COMMENT ON COLUMN pdcm_api.search_index.patient_sample_months_since_collection_1 IS 'The time difference between the 1st collection event and the current one (in months)';
COMMENT ON COLUMN pdcm_api.search_index.patient_sample_virology_status IS 'Positive virology status at the time of collection. Any relevant virology information which can influence cancer like EBV, HIV, HPV status';
COMMENT ON COLUMN pdcm_api.search_index.patient_sample_sharable IS 'Indicates if patient treatment information is available and sharable';
COMMENT ON COLUMN pdcm_api.search_index.patient_sample_treated_at_collection IS 'Indicates if the patient was being treated for cancer (radiotherapy, chemotherapy, targeted therapy, hormono-therapy) at the time of collection';
COMMENT ON COLUMN pdcm_api.search_index.patient_sample_treated_prior_to_collection IS 'Indicates if the patient was previously treated prior to the collection (radiotherapy, chemotherapy, targeted therapy, hormono-therapy)';
COMMENT ON COLUMN pdcm_api.search_index.pdx_model_publications IS 'Publications that are associated to one or more models (PubMed IDs separated by commas)';
COMMENT ON COLUMN pdcm_api.search_index.quality_assurance IS 'Quality assurance data';
COMMENT ON COLUMN pdcm_api.search_index.xenograft_model_specimens IS 'Represents a xenografted mouse that has participated in the line creation and characterisation in some meaningful way. E.g., the specimen provided a tumor that was characterized and used as quality assurance or drug dosing data';
COMMENT ON COLUMN pdcm_api.search_index.model_images IS 'Images associated with the model';
COMMENT ON COLUMN pdcm_api.search_index.markers_with_cna_data IS 'Marker list in associate CNA data';
COMMENT ON COLUMN pdcm_api.search_index.markers_with_mutation_data IS 'Marker list in associate mutation data';
COMMENT ON COLUMN pdcm_api.search_index.markers_with_expression_data IS 'Marker list in associate expression data';
COMMENT ON COLUMN pdcm_api.search_index.markers_with_biomarker_data IS 'Marker list in associate biomarker data';
COMMENT ON COLUMN pdcm_api.search_index.breast_cancer_biomarkers IS 'List of biomarkers associated to breast cancer';
COMMENT ON COLUMN pdcm_api.search_index.treatment_list IS 'Patient treatment data';
COMMENT ON COLUMN pdcm_api.search_index.model_treatment_list IS 'Drug dosing data';
COMMENT ON COLUMN pdcm_api.search_index.custom_treatment_type_list IS 'Treatment types + patient treatment status (Excluding "Not Provided")';
COMMENT ON COLUMN pdcm_api.search_index.raw_data_resources IS 'List of resources (calculated from raw data links) the model links to';
COMMENT ON COLUMN pdcm_api.search_index.cancer_annotation_resources IS 'List of resources (calculated from cancer annotation links) the model links to';
COMMENT ON COLUMN pdcm_api.search_index.scores IS 'Model characterizations scores';


-- search_facet materialized view: Facets information

DROP VIEW IF EXISTS pdcm_api.search_facet;

CREATE VIEW pdcm_api.search_facet
AS
 SELECT search_facet.*
   FROM search_facet;

COMMENT ON VIEW pdcm_api.search_facet IS 'Helper table to show filter options';
COMMENT ON COLUMN pdcm_api.search_facet.facet_section IS 'Facet section';
COMMENT ON COLUMN pdcm_api.search_facet.facet_name IS 'Facet name';
COMMENT ON COLUMN pdcm_api.search_facet.facet_column IS 'Facet column';
COMMENT ON COLUMN pdcm_api.search_facet.facet_options IS 'List of possible options';
COMMENT ON COLUMN pdcm_api.search_facet.facet_example IS 'Facet example';

-- release_info view: Name, date and list of processed providers

DROP VIEW IF EXISTS pdcm_api.release_info;

CREATE VIEW pdcm_api.release_info
AS
 SELECT release_info.*
   FROM release_info;

COMMENT ON VIEW pdcm_api.release_info IS 'Table that shows columns with data per data source / molecular data table';
COMMENT ON COLUMN pdcm_api.release_info.name IS 'Name of the release';
COMMENT ON COLUMN pdcm_api.release_info.date IS 'Date of the release';
COMMENT ON COLUMN pdcm_api.release_info.providers IS 'List of processed providers';


-- provider_group view: Providers information

DROP VIEW IF EXISTS pdcm_api.provider_group;

CREATE VIEW pdcm_api.provider_group
AS
 SELECT provider_group.*
   FROM provider_group;

COMMENT ON VIEW pdcm_api.provider_group IS 'Information of data providers';
COMMENT ON COLUMN pdcm_api.provider_group.id IS 'Internal identifier';
COMMENT ON COLUMN pdcm_api.provider_group.name IS 'Provider name';
COMMENT ON COLUMN pdcm_api.provider_group.abbreviation IS 'Provider abbreviation';
COMMENT ON COLUMN pdcm_api.provider_group.description IS 'A description of the provider';
COMMENT ON COLUMN pdcm_api.provider_group.provider_type_id IS 'Reference to the provider type';
COMMENT ON COLUMN pdcm_api.provider_group.project_group_id IS 'Reference to the project the provider belongs to';


--------------------------------- Materialized Views -------------------------------------------------------------------

-- Available columns by provider and molecular characterization type

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.available_molecular_data_columns;

CREATE MATERIALIZED VIEW pdcm_api.available_molecular_data_columns
AS SELECT available_molecular_data_columns.*
   FROM   available_molecular_data_columns;

COMMENT ON MATERIALIZED VIEW pdcm_api.available_molecular_data_columns IS 'Table that shows columns with data per data source / molecular data table';
COMMENT ON COLUMN pdcm_api.available_molecular_data_columns.data_source IS 'Data source';
COMMENT ON COLUMN pdcm_api.available_molecular_data_columns.not_empty_cols IS 'List of columns that have data';
COMMENT ON COLUMN pdcm_api.available_molecular_data_columns.molecular_characterization_type IS 'Type of molecular data table';

-- details_molecular_data materialized view: Molecular data details and availability

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.details_molecular_data;

CREATE MATERIALIZED VIEW pdcm_api.details_molecular_data AS
 SELECT molecular_characterization.id,
    ps.external_patient_sample_id AS patient_sample_id,
    ps.model_id AS patient_model_id,
    xs.external_xenograft_sample_id AS xenograft_sample_id,
    xs.model_id AS xenograft_model_id,
    xs.passage AS xenograft_passage,
	cs.external_cell_sample_id AS cell_sample_id,
	cs.model_id AS cell_model_id,
    molecular_characterization.raw_data_url,
    data_type.name AS data_type,
    platform.id AS platform_id,
    platform.instrument_model AS platform_name,
        CASE
            WHEN data_type.name = 'mutation'::text AND (molecular_characterization.id IN ( SELECT DISTINCT mutation_data_table.molecular_characterization_id
               FROM pdcm_api.mutation_data_table)) THEN 'TRUE'::text
            WHEN data_type.name = 'expression'::text AND (molecular_characterization.id IN ( SELECT DISTINCT expression_data_table.molecular_characterization_id
               FROM pdcm_api.expression_data_table)) THEN 'TRUE'::text
            WHEN data_type.name = 'copy number alteration'::text AND (molecular_characterization.id IN ( SELECT DISTINCT cna_data_table.molecular_characterization_id
               FROM pdcm_api.cna_data_table)) THEN 'TRUE'::text
            WHEN data_type.name = 'biomarker'::text AND (molecular_characterization.id IN ( SELECT DISTINCT biomarker_data_table.molecular_characterization_id
               FROM pdcm_api.biomarker_data_table)) THEN 'TRUE'::text
            ELSE 'FALSE'::text
        END AS data_availability,
    external_db_links
   FROM molecular_characterization
     JOIN platform ON molecular_characterization.platform_id = platform.id
     JOIN molecular_characterization_type data_type ON molecular_characterization.molecular_characterization_type_id = data_type.id
     LEFT JOIN patient_sample ps ON molecular_characterization.patient_sample_id = ps.id
     LEFT JOIN xenograft_sample xs ON molecular_characterization.xenograft_sample_id = xs.id
	 LEFT JOIN cell_sample cs ON molecular_characterization.cell_sample_id = cs.id;

COMMENT ON MATERIALIZED VIEW pdcm_api.details_molecular_data IS 'Content for Molecular Data section';
COMMENT ON COLUMN pdcm_api.details_molecular_data.id IS 'Reference to the molecular_characterization_ table';
COMMENT ON COLUMN pdcm_api.details_molecular_data.patient_sample_id IS 'Reference to the patient_sample table';
COMMENT ON COLUMN pdcm_api.details_molecular_data.patient_model_id IS 'Model related to the patient sample. Reference to the model_information table';
COMMENT ON COLUMN pdcm_api.details_molecular_data.xenograft_sample_id IS 'Reference to the xenograft_sample table';
COMMENT ON COLUMN pdcm_api.details_molecular_data.xenograft_model_id IS 'Model related to the xenograft sample. Reference to the model_information table';
COMMENT ON COLUMN pdcm_api.details_molecular_data.cell_sample_id IS 'Reference to the cell_sample table';
COMMENT ON COLUMN pdcm_api.details_molecular_data.cell_model_id IS ' Model related to the xenograft sample. Reference to the model_information table';
COMMENT ON COLUMN pdcm_api.details_molecular_data.raw_data_url IS 'Identifiers used to build links to external resources with raw data';
COMMENT ON COLUMN pdcm_api.details_molecular_data.data_type IS 'Molecular data type';
COMMENT ON COLUMN pdcm_api.details_molecular_data.platform_id IS 'Reference to the platform table';
COMMENT ON COLUMN pdcm_api.details_molecular_data.platform_name IS 'Platform instrument_model';
COMMENT ON COLUMN pdcm_api.details_molecular_data.data_availability IS 'Indicates if there is data for this molecular data type';
COMMENT ON COLUMN pdcm_api.details_molecular_data.external_db_links IS 'JSON column with links to external resources';


/*
  DATA OVERVIEW VIEWS
*/

-- models_by_cancer materialized view: Models by cancer

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.models_by_cancer;

CREATE MATERIALIZED VIEW pdcm_api.models_by_cancer AS
 SELECT search_index.cancer_system,
    search_index.histology,
    count(*) AS count
   FROM search_index
  GROUP BY search_index.cancer_system, search_index.histology;

COMMENT ON MATERIALIZED VIEW pdcm_api.models_by_cancer IS 'Count of models per diagnosis';
COMMENT ON COLUMN pdcm_api.models_by_cancer.cancer_system IS 'Cancer system';
COMMENT ON COLUMN pdcm_api.models_by_cancer.histology IS 'List of columns that have data';
COMMENT ON COLUMN pdcm_api.models_by_cancer.count IS 'List of columns that have data';

-- models_by_mutated_gene materialized view: Models by mutated gene

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.models_by_mutated_gene;

CREATE MATERIALIZED VIEW pdcm_api.models_by_mutated_gene AS
 SELECT (SPLIT_PART(unnest(search_index.markers_with_mutation_data), '/', 1)) AS mutated_gene,
    count(DISTINCT search_index.pdcm_model_id) AS count
   FROM search_index
  GROUP BY mutated_gene ;

COMMENT ON MATERIALIZED VIEW pdcm_api.models_by_mutated_gene IS 'Count of models per diagnosis';
COMMENT ON COLUMN pdcm_api.models_by_cancer.cancer_system IS 'Cancer system';
COMMENT ON COLUMN pdcm_api.models_by_cancer.histology IS 'Histology';
COMMENT ON COLUMN pdcm_api.models_by_cancer.count IS 'Number of models';

-- models_by_dataset_availability materialized view: Molecular data availability information by model

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.models_by_dataset_availability;

CREATE MATERIALIZED VIEW pdcm_api.models_by_dataset_availability AS
 SELECT unnest(search_index.dataset_available) AS dataset_availability,
    count(DISTINCT search_index.pdcm_model_id) AS count
   FROM search_index
  GROUP BY (unnest(search_index.dataset_available));

COMMENT ON MATERIALIZED VIEW pdcm_api.models_by_dataset_availability IS 'Count of models per available datasets';
COMMENT ON COLUMN pdcm_api.models_by_dataset_availability.dataset_availability IS 'Cancer system';
COMMENT ON COLUMN pdcm_api.models_by_dataset_availability.count IS 'Number of models';

-- dosing_studies materialized view: Treatment information linked to the model

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.dosing_studies;

CREATE MATERIALIZED VIEW pdcm_api.dosing_studies AS
 SELECT model_id,
       protocol_id,
       string_agg(treatment, ' And ') treatment,
       response,
       string_agg(dose, ' + ') AS dose
FROM   (SELECT model_id,
               protocol_id,
               ( CASE
                   WHEN treatment_harmonised IS NOT NULL THEN
                   treatment_harmonised
                   ELSE treatment_raw
                 END ) treatment,
               response,
               dose
        FROM   (SELECT model_id,
                       tp.id         protocol_id,
                       ott.term_name treatment_harmonised,
                       r.NAME        response,
                       tc.dose       dose,
                       t.NAME        treatment_raw
                FROM   model_information m,
                       treatment_protocol tp,
                       response r,
                       treatment_component tc,
                       treatment t
                       LEFT JOIN treatment_to_ontology tont
                              ON ( t.id = tont.treatment_id )
                       LEFT JOIN ontology_term_treatment ott
                              ON ( tont.ontology_term_id = ott.id )
                WHERE  treatment_target = 'drug dosing'
                       AND m.id = tp.model_id
                       AND tp.response_id = r.id
                       AND tc.treatment_protocol_id = tp.id
                       AND tc.treatment_id = t.id
                       AND t.data_source = m.data_source) a)b
GROUP  BY model_id,
          protocol_id,
          response;

COMMENT ON MATERIALIZED VIEW pdcm_api.dosing_studies IS 'Dosing studies section data';
COMMENT ON COLUMN pdcm_api.dosing_studies.model_id IS 'Reference to the model_information table';
COMMENT ON COLUMN pdcm_api.dosing_studies.protocol_id IS 'Reference to the treatment_protocol table';
COMMENT ON COLUMN pdcm_api.dosing_studies.treatment IS 'Treatment names';
COMMENT ON COLUMN pdcm_api.dosing_studies.response IS 'Response to the treatment';
COMMENT ON COLUMN pdcm_api.dosing_studies.dose IS 'Dose used';

-- patient_treatment materialized view: Treatment information linked to the patient

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.patient_treatment;

CREATE MATERIALIZED VIEW pdcm_api.patient_treatment AS
 SELECT model_id,
       protocol_id,
       string_agg(treatment, ' And ') treatment,
       response,
       string_agg(dose, ' + ') AS dose
 FROM   (SELECT model_id,
               protocol_id,
               ( CASE
                   WHEN treatment_harmonised IS NOT NULL THEN
                   treatment_harmonised
                   ELSE treatment_raw
                 END ) treatment,
               response,
               dose
        FROM   (SELECT m.id    model_id,
                       tp.id         protocol_id,
                       ott.term_name treatment_harmonised,
                       r.NAME        response,
                       tc.dose       dose,
                       t.NAME        treatment_raw
                FROM
					   patient_sample ps,
					   model_information m,
                       treatment_protocol tp,
                       response r,
                       treatment_component tc,
                       treatment t
                       LEFT JOIN treatment_to_ontology tont
                              ON ( t.id = tont.treatment_id )
                       LEFT JOIN ontology_term_treatment ott
                              ON ( tont.ontology_term_id = ott.id )
                WHERE  treatment_target = 'patient'
				       AND tp.patient_id = ps.patient_id
                       AND m.id = ps.model_id
                       AND tp.response_id = r.id
                       AND tc.treatment_protocol_id = tp.id
                       AND tc.treatment_id = t.id
                       AND t.data_source = m.data_source) a)b
 GROUP  BY model_id,
          protocol_id,
          response;

COMMENT ON MATERIALIZED VIEW pdcm_api.patient_treatment IS 'Dosing studies section data';
COMMENT ON COLUMN pdcm_api.patient_treatment.model_id IS 'Reference to the model_information table';
COMMENT ON COLUMN pdcm_api.patient_treatment.protocol_id IS 'Reference to the treatment_protocol table';
COMMENT ON COLUMN pdcm_api.patient_treatment.treatment IS 'Treatment names';
COMMENT ON COLUMN pdcm_api.patient_treatment.response IS 'Response to the treatment';
COMMENT ON COLUMN pdcm_api.patient_treatment.dose IS 'Dose used';

-- models_by_treatment materialized view: Models by treatment

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.models_by_treatment;

CREATE MATERIALIZED VIEW pdcm_api.models_by_treatment AS
   SELECT treatment,
          COUNT(DISTINCT pdcm_model_id) AS count
   FROM   (SELECT UNNEST(search_index.treatment_list) AS treatment,
                  search_index.pdcm_model_id
           FROM   search_index) treatment_model
   WHERE  treatment_model.treatment NOT LIKE '% = %'
   GROUP  BY ( treatment );

COMMENT ON MATERIALIZED VIEW pdcm_api.models_by_treatment IS 'Dosing studies section data';
COMMENT ON COLUMN pdcm_api.models_by_treatment.treatment IS 'Treatment name';
COMMENT ON COLUMN pdcm_api.models_by_treatment.count IS 'Number of models';

-- models_by_type materialized view: Models by type

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.models_by_type;

CREATE materialized VIEW pdcm_api.models_by_type AS
SELECT
  model_type,
  count(1)
FROM
  search_index
GROUP BY
  model_type;

COMMENT ON MATERIALIZED VIEW pdcm_api.models_by_type IS 'Dosing studies section data';
COMMENT ON COLUMN pdcm_api.models_by_type.model_type IS 'Treatment name';
COMMENT ON COLUMN pdcm_api.models_by_type.count IS 'Number of models';


-- search_facet_options materialized view: Values for dropdowns/inputs in filters

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.search_facet_options;

CREATE MATERIALIZED VIEW pdcm_api.search_facet_options
AS
 SELECT search_facet.facet_column,
    unnest(search_facet.facet_options) AS option
   FROM search_facet
WITH DATA;

COMMENT ON MATERIALIZED VIEW pdcm_api.search_facet_options IS 'Facet options (filters)';
COMMENT ON COLUMN pdcm_api.search_facet_options.facet_column IS 'Facet column';
COMMENT ON COLUMN pdcm_api.search_facet_options.option IS 'Facet value';

-- patient_treatment_extended materialized view: patient treatment data + model information

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.patient_treatment_extended;

CREATE MATERIALIZED VIEW pdcm_api.patient_treatment_extended AS
SELECT
	external_model_id AS model_id,
	data_source,
	external_patient_id AS patient_id,
	patient_age,
	patient_sex,
	patient_ethnicity,
	patient_treatment_status,
	histology,
	string_agg(a.treatment, ' + ') AS treatment,
	response,
	string_agg(dose, ' + ') AS dose
FROM (
	SELECT
		tp.id,
		si.external_model_id,
		si.data_source,
		tp.treatment_target,
		p.external_patient_id,
		si.patient_age,
		si.patient_sex,
		si.patient_ethnicity,
		si.patient_treatment_status,
		si.histology,
		CASE WHEN ott.term_name IS NULL THEN t.name ELSE ott.term_name END AS treatment,
		r.name AS response,
		tc.dose
	FROM treatment_protocol tp
	JOIN patient_sample ps on ps.patient_id = tp.patient_id
	JOIN patient p on p.id = ps.patient_id
	JOIN search_index si on si.pdcm_model_id = ps.model_id
	JOIN treatment_component tc on tc.treatment_protocol_id = tp.id
	JOIN response r on r.id = tp.response_id
	JOIN treatment t on t.id = tc.treatment_id
	LEFT JOIN treatment_to_ontology tont ON t.id = tont.treatment_id
	LEFT JOIN ontology_term_treatment ott ON tont.ontology_term_id = ott.id
	WHERE treatment_target = 'patient'
	AND t.data_source=si.data_source
	) a
GROUP BY
	id, external_model_id, data_source, external_patient_id, patient_age, patient_sex,
	patient_ethnicity, patient_treatment_status, histology, response;

COMMENT ON MATERIALIZED VIEW pdcm_api.patient_treatment_extended IS
  $$Patient treatment

  Information about treatments in patients$$;

COMMENT ON COLUMN pdcm_api.patient_treatment_extended.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.data_source IS 'Data source of the model';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.patient_id IS 'Anonymous/de-identified provider ID for the patient';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.patient_age IS 'Patient age at collection';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.patient_sex IS 'Sex of the patient';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.patient_ethnicity IS 'Patient Ethnic group';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.patient_treatment_status IS 'Status of the patient treatment';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.histology IS 'Diagnosis at time of collection of the patient tumor';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.treatment IS 'Treatment name. It can be surgery, radiotherapy,  drug name  or drug combination';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.response IS 'Response of prior treatment';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.dose IS 'Treatment dose and unit';

-- drug_dosing_extended materialized view: drug dosing treatment data + model information

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.drug_dosing_extended;

CREATE MATERIALIZED VIEW pdcm_api.drug_dosing_extended AS
SELECT
	external_model_id AS model_id,
	data_source,
	histology,
	string_agg(a.treatment, ' + ') AS treatment,
	response,
	string_agg(dose, ' + ') AS dose
FROM (
	SELECT
		tp.id,
		si.external_model_id,
		si.data_source,
		si.patient_age,
		si.patient_sex,
		si.patient_ethnicity,
		si.patient_treatment_status,
		si.histology,
		CASE WHEN ott.term_name IS NULL THEN t.name ELSE ott.term_name END AS treatment,
		r.name AS response,
		tc.dose
	FROM treatment_protocol tp
	JOIN search_index si on si.pdcm_model_id = tp.model_id
	JOIN treatment_component tc on tc.treatment_protocol_id = tp.id
	JOIN response r on r.id = tp.response_id
	JOIN treatment t on t.id = tc.treatment_id
	LEFT JOIN treatment_to_ontology tont ON t.id = tont.treatment_id
	LEFT JOIN ontology_term_treatment ott ON tont.ontology_term_id = ott.id
	WHERE treatment_target = 'drug dosing'
	AND t.data_source=si.data_source
	) a
GROUP BY
	id, external_model_id, data_source, histology, response;

COMMENT ON MATERIALIZED VIEW pdcm_api.drug_dosing_extended IS
  $$Drug dosing

  Information about treatments in models$$;

COMMENT ON COLUMN pdcm_api.drug_dosing_extended.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN pdcm_api.drug_dosing_extended.data_source IS 'Data source of the model';
COMMENT ON COLUMN pdcm_api.drug_dosing_extended.histology IS 'Diagnosis at time of collection of the patient tumor';
COMMENT ON COLUMN pdcm_api.drug_dosing_extended.treatment IS 'Treatment name. It can be surgery, radiotherapy,  drug name  or drug combination';
COMMENT ON COLUMN pdcm_api.drug_dosing_extended.response IS 'Response of prior treatment';
COMMENT ON COLUMN pdcm_api.drug_dosing_extended.dose IS 'Treatment dose and unit';

-- models_by_primary_site materialized view: model count by primary site for Data Overview page

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.models_by_primary_site;

CREATE materialized VIEW pdcm_api.models_by_primary_site AS
SELECT
  primary_site,
  count(1)
FROM
  search_index
GROUP BY
  primary_site;

COMMENT ON MATERIALIZED VIEW pdcm_api.models_by_primary_site IS
  $$Models by primary site

  Count of models by primary site$$;

COMMENT ON COLUMN pdcm_api.models_by_primary_site.primary_site IS 'Primary site';
COMMENT ON COLUMN pdcm_api.models_by_primary_site.count IS 'Number of models';

-- models_by_anatomical_system_and_diagnosis materialized view: model count by anatomical system and diagnosis for Data Overview page

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.models_by_anatomical_system_and_diagnosis;

CREATE materialized VIEW pdcm_api.models_by_anatomical_system_and_diagnosis AS
SELECT
  cancer_system,
  histology,
  count(1)
FROM
  search_index
GROUP BY
  cancer_system, histology;

COMMENT ON MATERIALIZED VIEW pdcm_api.models_by_anatomical_system_and_diagnosis IS
  $$Models by anatomical system and diagnosis

  Count of models by anatomical system and diagnosis$$;

COMMENT ON COLUMN pdcm_api.models_by_anatomical_system_and_diagnosis.cancer_system IS 'Cancer system';
COMMENT ON COLUMN pdcm_api.models_by_anatomical_system_and_diagnosis.histology IS 'Histology';
COMMENT ON COLUMN pdcm_api.models_by_anatomical_system_and_diagnosis.count IS 'Number of models';

-- models_by_tumour_type materialized view: model count by tumour type for Data Overview page

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.models_by_tumour_type;

CREATE materialized VIEW pdcm_api.models_by_tumour_type AS
SELECT
  tumour_type,
  count(1)
FROM
  search_index
GROUP BY
  tumour_type;

COMMENT ON MATERIALIZED VIEW pdcm_api.models_by_tumour_type IS
  $$Models by tumour type

  Count of models by tumour type$$;

COMMENT ON COLUMN pdcm_api.models_by_tumour_type.tumour_type IS 'Tumour type';
COMMENT ON COLUMN pdcm_api.models_by_tumour_type.count IS 'Number of models';

-- models_by_patient_age materialized view: model count by patient age for Data Overview page

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.models_by_patient_age;

CREATE materialized VIEW pdcm_api.models_by_patient_age AS
SELECT
  patient_age,
  count(1)
FROM
  search_index
GROUP BY
  patient_age;

COMMENT ON MATERIALIZED VIEW pdcm_api.models_by_patient_age IS
  $$Models by patient age

  Count of models by patient age$$;

COMMENT ON COLUMN pdcm_api.models_by_patient_age.patient_age IS 'Patient age';
COMMENT ON COLUMN pdcm_api.models_by_patient_age.count IS 'Number of models';

-- models_by_patient_sex materialized view: model count by patient sex for Data Overview page

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.models_by_patient_sex;

CREATE materialized VIEW pdcm_api.models_by_patient_sex AS
SELECT
  patient_sex,
  count(1)
FROM
  search_index
GROUP BY
  patient_sex;

COMMENT ON MATERIALIZED VIEW pdcm_api.models_by_patient_sex IS
  $$Models by patient sex

  Count of models by patient sex$$;

COMMENT ON COLUMN pdcm_api.models_by_patient_sex.patient_sex IS 'Patient sex';
COMMENT ON COLUMN pdcm_api.models_by_patient_sex.count IS 'Number of models';

-- models_by_patient_ethnicity materialized view: model count by patient ethnicity for Data Overview page

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.models_by_patient_ethnicity;

CREATE materialized VIEW pdcm_api.models_by_patient_ethnicity AS
SELECT
  patient_ethnicity,
  count(1)
FROM
  search_index
GROUP BY
  patient_ethnicity;

COMMENT ON MATERIALIZED VIEW pdcm_api.models_by_patient_ethnicity IS
  $$Models by patient ethnicity

  Count of models by patient ethnicity$$;

COMMENT ON COLUMN pdcm_api.models_by_patient_ethnicity.patient_ethnicity IS 'Patient ethnicity';
COMMENT ON COLUMN pdcm_api.models_by_patient_ethnicity.count IS 'Number of models';

DROP MATERIALIZED VIEW IF exists pdcm_api.info;

CREATE MATERIALIZED VIEW pdcm_api.info AS
 SELECT 'total_models' AS key, (SELECT COUNT(1) from search_index) AS value;

COMMENT ON MATERIALIZED VIEW pdcm_api.info IS 'General metrics in key value formar';
COMMENT ON COLUMN pdcm_api.info.key IS 'Key';
COMMENT ON COLUMN pdcm_api.info.value IS 'Value';

