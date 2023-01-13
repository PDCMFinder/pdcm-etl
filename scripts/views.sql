-------------------------------------------Views -----------------------------------------------------------------------
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


COMMENT ON VIEW pdcm_api.model_metadata IS 'Metadata associated to a model';

COMMENT ON COLUMN pdcm_api.model_information.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN pdcm_api.model_information.data_source IS 'Data source of the model (provider abbreviation)';
COMMENT ON COLUMN pdcm_api.model_information.provider_name IS 'Provider full name';
COMMENT ON COLUMN pdcm_api.model_information.type IS 'Model type (xenograft, cell_line, …)';
COMMENT ON COLUMN pdcm_api.model_information.host_strain_name IS 'Host mouse strain name (e.g. NOD-SCID, NSG, etc)';
COMMENT ON COLUMN pdcm_api.model_information.host_strain_nomenclature IS 'The full nomenclature form of the host mouse strain name';
COMMENT ON COLUMN pdcm_api.model_information.engraftment_site IS 'Organ or anatomical site used for the tumour engraftment (e.g. mammary fat pad, Right flank)';
COMMENT ON COLUMN pdcm_api.model_information.engraftment_type IS 'Orthotopic: if the tumour was engrafted at a corresponding anatomical site. Hererotopic: If grafted subcuteanously';
COMMENT ON COLUMN pdcm_api.model_information.engraftment_sample_type IS 'Description of the type of material grafted into the mouse. (e.g. tissue fragments, cell suspension)';
COMMENT ON COLUMN pdcm_api.model_information.engraftment_sample_state IS 'PDX Engraftment material state (e.g. fresh or frozen)';
COMMENT ON COLUMN pdcm_api.model_information.passage_number IS 'Passage number';
COMMENT ON COLUMN pdcm_api.model_information.histology IS 'Diagnosis at time of collection of the patient tumor';
COMMENT ON COLUMN pdcm_api.model_information.cancer_system IS 'Cancer System';
COMMENT ON COLUMN pdcm_api.model_information.primary_site IS 'Site of the primary tumor where primary cancer is originating from (may not correspond to the site of the current tissue sample)';
COMMENT ON COLUMN pdcm_api.model_information.collection_site IS 'Site of collection of the tissue sample (can be different than the primary site if tumour type is metastatic)';
COMMENT ON COLUMN pdcm_api.model_information.tumor_type IS 'Collected tumor type';
COMMENT ON COLUMN pdcm_api.model_information.cancer_grade IS 'The implanted tumor grade value';
COMMENT ON COLUMN pdcm_api.model_information.cancer_grading_system IS 'Stage classification system used to describe the stage';
COMMENT ON COLUMN pdcm_api.model_information.cancer_stage IS 'Stage of the patient at the time of collection';
COMMENT ON COLUMN pdcm_api.model_information.patient_age IS 'Patient age at collection';
COMMENT ON COLUMN pdcm_api.model_information.patient_sex IS 'Sex of the patient';
COMMENT ON COLUMN pdcm_api.model_information.patient_ethnicity IS 'Patient Ethnic group';
COMMENT ON COLUMN pdcm_api.model_information.patient_treatment_status IS 'Patient treatment status';
COMMENT ON COLUMN pdcm_api.model_information.pubmed_ids IS 'PubMed ids related to the model';
COMMENT ON COLUMN pdcm_api.model_information.europdx_access_modalities IS 'If a model is part of EUROPDX consortium, then this field defines if the model is accessible for transnational access through the EDIReX infrastructure, or only on a collaborative basis';
COMMENT ON COLUMN pdcm_api.model_information.accessibility IS 'Defines any limitation of access of the model per type of users like academia only, industry and academia, or national limitation';
COMMENT ON COLUMN pdcm_api.model_information.contact_name_list IS 'List of names of the contact people for the model';
COMMENT ON COLUMN pdcm_api.model_information.contact_email_list IS 'Emails of the contact names for the model';
COMMENT ON COLUMN pdcm_api.model_information.contact_form_url IS 'URL to the providers resource for each model';
COMMENT ON COLUMN pdcm_api.model_information.source_database_url IS 'URL to the source database for each model';


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

COMMENT ON VIEW pdcm_api.model_quality_assurance IS 'Quality assurance data related to a model';

COMMENT ON COLUMN pdcm_api.model_quality_assurance.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN pdcm_api.model_quality_assurance.data_source IS 'Data source of the model (provider abbreviation)';
COMMENT ON COLUMN pdcm_api.model_quality_assurance.description IS 'Short description of what was compared and what was the result: (e.g. high, good, moderate concordance between xenograft, ''model validated against histological features of same diagnosis'' or ''not determined'')';
COMMENT ON COLUMN pdcm_api.model_quality_assurance.passages_tested IS 'List of all passages where validation was performed. Passage 0 correspond to first engraftment';
COMMENT ON COLUMN pdcm_api.model_quality_assurance.validation_technique IS 'Any technique used to validate PDX against their original patient tumour, including fingerprinting, histology, immunohistochemistry';
COMMENT ON COLUMN pdcm_api.model_quality_assurance.validation_host_strain_nomenclature IS 'Validation host mouse strain, following mouse strain nomenclature from MGI JAX';

-- model_information view

DROP VIEW IF EXISTS pdcm_api.model_information CASCADE;

CREATE VIEW pdcm_api.model_information AS
 SELECT * from model_information;

COMMENT ON VIEW pdcm_api.model_information IS 'Model information';
COMMENT ON COLUMN pdcm_api.model_information.id IS 'Record identifier';
COMMENT ON COLUMN pdcm_api.model_information.external_model_id IS 'Identifier model given by the provider';
COMMENT ON COLUMN pdcm_api.model_information.data_source IS 'Provider abbreviation';
COMMENT ON COLUMN pdcm_api.model_information.publication_group_id IS 'Publication group the model is connected to';
COMMENT ON COLUMN pdcm_api.model_information.accessibility_group_id IS 'Accessibility group the model is connected to';
COMMENT ON COLUMN pdcm_api.model_information.contact_people_id IS 'Id referencing the contact people for this model';
COMMENT ON COLUMN pdcm_api.model_information.contact_people_id IS 'Id referencing the source database for the model';


-- contact_people view

DROP VIEW IF EXISTS pdcm_api.contact_people CASCADE;

CREATE VIEW pdcm_api.contact_people AS
 SELECT * from contact_people;

-- contact_form view

DROP VIEW IF EXISTS pdcm_api.contact_form CASCADE;

CREATE VIEW pdcm_api.contact_form AS
 SELECT * from contact_form;

-- source_database view

DROP VIEW IF EXISTS pdcm_api.source_database CASCADE;

CREATE VIEW pdcm_api.source_database AS
 SELECT * from source_database;

-- engraftment_site view

DROP VIEW IF EXISTS pdcm_api.engraftment_site CASCADE;

CREATE VIEW pdcm_api.engraftment_site AS
 SELECT * from engraftment_site;

-- engraftment_type view

DROP VIEW IF EXISTS pdcm_api.engraftment_type CASCADE;

CREATE VIEW pdcm_api.engraftment_type AS
 SELECT * from engraftment_type;

-- engraftment_sample_type view

DROP VIEW IF EXISTS pdcm_api.engraftment_sample_type CASCADE;

CREATE VIEW pdcm_api.engraftment_sample_type AS
 SELECT * from engraftment_sample_type;

-- engraftment_sample_state view

DROP VIEW IF EXISTS pdcm_api.engraftment_sample_state CASCADE;

CREATE VIEW pdcm_api.engraftment_sample_state AS
 SELECT * from engraftment_sample_state;

-- xenograft_model_specimen view

DROP VIEW IF EXISTS pdcm_api.xenograft_model_specimen CASCADE;

CREATE VIEW pdcm_api.xenograft_model_specimen AS
 SELECT * from xenograft_model_specimen;

-- host_strain view

DROP VIEW IF EXISTS pdcm_api.host_strain CASCADE;

CREATE VIEW pdcm_api.host_strain AS
 SELECT * from host_strain;

-- quality_assurance view

DROP VIEW IF EXISTS pdcm_api.quality_assurance CASCADE;

CREATE VIEW pdcm_api.quality_assurance AS
 SELECT * from quality_assurance;

-- publication_group view

DROP VIEW IF EXISTS pdcm_api.publication_group CASCADE;

CREATE VIEW pdcm_api.publication_group AS
 SELECT * from publication_group;

-- mutation_data_table view

DROP VIEW IF EXISTS pdcm_api.mutation_data_table CASCADE;

CREATE VIEW pdcm_api.mutation_data_table
AS SELECT mmd.molecular_characterization_id,
          mmd.hgnc_symbol,
          mmd.amino_acid_change,
          mmd.consequence,
          mmd.read_depth,
          mmd.allele_frequency,
          mmd.seq_start_position,
          mmd.ref_allele,
          mmd.alt_allele,
          ( mmd.* ) :: text AS text
   FROM   mutation_measurement_data mmd
   WHERE (mmd.data_source, 'mutation_measurement_data') NOT IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction);

COMMENT ON VIEW pdcm_api.mutation_data_table IS 'Mutation data';
COMMENT ON COLUMN pdcm_api.mutation_data_table.molecular_characterization_id IS 'Molecular characterization id';

-- expression_data_table view

DROP VIEW IF EXISTS pdcm_api.expression_data_table;

CREATE VIEW pdcm_api.expression_data_table
AS
  SELECT emd.molecular_characterization_id,
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
         ( emd.* ) :: text AS text
  FROM   expression_molecular_data emd
  WHERE (emd.data_source, 'expression_molecular_data') NOT IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction);

-- cytogenetics_data_table view

DROP VIEW IF EXISTS pdcm_api.cytogenetics_data_table;

CREATE VIEW pdcm_api.cytogenetics_data_table
AS
  SELECT cmd.molecular_characterization_id,
         cmd.hgnc_symbol,
         cmd.marker_status AS result,
         ( cmd.* ) :: text AS text
  FROM   cytogenetics_molecular_data cmd
  WHERE (cmd.data_source, 'cytogenetics_molecular_data') NOT IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction);

-- cna_data_table view

DROP VIEW IF EXISTS pdcm_api.cna_data_table;

CREATE VIEW pdcm_api.cna_data_table
AS
  SELECT cnamd.hgnc_symbol,
         cnamd.id,
         cnamd.log10r_cna,
         cnamd.log2r_cna,
         cnamd.copy_number_status,
         cnamd.gistic_value,
         cnamd.picnic_value,
         cnamd.non_harmonised_symbol,
         cnamd.harmonisation_result,
         cnamd.molecular_characterization_id,
         ( cnamd.* ) :: text AS text
  FROM   cna_molecular_data cnamd
  WHERE (cnamd.data_source, 'cna_molecular_data') NOT IN (SELECT data_source, molecular_data_table FROM molecular_data_restriction);

DROP VIEW IF EXISTS pdcm_api.molecular_data_restriction;

CREATE VIEW pdcm_api.molecular_data_restriction
AS
  SELECT mdr.*
  FROM   molecular_data_restriction mdr;

-- search_index materialized view: Index to search for models

DROP VIEW IF EXISTS pdcm_api.search_index;

CREATE VIEW pdcm_api.search_index
AS
 SELECT search_index.*, cardinality(dataset_available) as model_dataset_type_count
   FROM search_index;


-- search_facet materialized view: Facets information

DROP VIEW IF EXISTS pdcm_api.search_facet;

CREATE VIEW pdcm_api.search_facet
AS
 SELECT search_facet.*
   FROM search_facet;

-- release_info view: Name, date and list of processed providers

DROP VIEW IF EXISTS pdcm_api.release_info;

CREATE VIEW pdcm_api.release_info
AS
 SELECT release_info.*
   FROM release_info;

--------------------------------- Materialized Views -------------------------------------------------------------------

-- Available columns by provider and molecular characterization type

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.available_molecular_data_columns;

CREATE MATERIALIZED VIEW pdcm_api.available_molecular_data_columns
AS SELECT available_molecular_data_columns.*
   FROM   available_molecular_data_columns;

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
            WHEN data_type.name = 'cytogenetics'::text AND (molecular_characterization.id IN ( SELECT DISTINCT cytogenetics_data_table.molecular_characterization_id
               FROM pdcm_api.cytogenetics_data_table)) THEN 'TRUE'::text
            ELSE 'FALSE'::text
        END AS data_availability
   FROM molecular_characterization
     JOIN platform ON molecular_characterization.platform_id = platform.id
     JOIN molecular_characterization_type data_type ON molecular_characterization.molecular_characterization_type_id = data_type.id
     LEFT JOIN patient_sample ps ON molecular_characterization.patient_sample_id = ps.id
     LEFT JOIN xenograft_sample xs ON molecular_characterization.xenograft_sample_id = xs.id
	 LEFT JOIN cell_sample cs ON molecular_characterization.cell_sample_id = cs.id;

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

-- models_by_mutated_gene materialized view: Models by mutated gene

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.models_by_mutated_gene;

CREATE MATERIALIZED VIEW pdcm_api.models_by_mutated_gene AS
 SELECT (SPLIT_PART(unnest(search_index.makers_with_mutation_data), '/', 1)) AS mutated_gene,
    count(DISTINCT search_index.pdcm_model_id) AS count
   FROM search_index
  GROUP BY mutated_gene ;

-- models_by_dataset_availability materialized view: Molecular data availability information by model

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.models_by_dataset_availability;

CREATE MATERIALIZED VIEW pdcm_api.models_by_dataset_availability AS
 SELECT unnest(search_index.dataset_available) AS dataset_availability,
    count(DISTINCT search_index.pdcm_model_id) AS count
   FROM search_index
  GROUP BY (unnest(search_index.dataset_available));

-- dosing_studies materialized view: Treatment information linked to the model

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.dosing_studies;

CREATE MATERIALIZED VIEW pdcm_api.dosing_studies AS
 SELECT model_id,
       String_agg(treatment, ' And ') treatment,
       response,
       dose
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
          response,
          dose;

-- patient_treatment materialized view: Treatment information linked to the patient

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.patient_treatment;

CREATE MATERIALIZED VIEW pdcm_api.patient_treatment AS
 SELECT model_id,
       String_agg(treatment, ' And ') treatment,
       response,
       dose
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
          response,
          dose;

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


-- search_facet_options materialized view: Values for dropdowns/inputs in filters

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.search_facet_options;

CREATE MATERIALIZED VIEW pdcm_api.search_facet_options
AS
 SELECT search_facet.facet_column,
    unnest(search_facet.facet_options) AS option
   FROM search_facet
WITH DATA;

-- model_molecular_metadata materialized view: Model molecular metadata

DROP MATERIALIZED VIEW IF EXISTS pdcm_api.details_molecular_data;

CREATE MATERIALIZED VIEW pdcm_api.model_molecular_metadata AS
SELECT
mol_char.data_availability,
	mi.external_model_id AS model_id,
	mi.data_source,
	mol_char.source,
	mol_char.sample_id,
	xs.passage AS xenograft_passage,
	mol_char.raw_data_url,
	mol_char.data_type,
	pf.instrument_model AS platform_name
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
	END AS sample_id,
	CASE
		WHEN mct.name = 'mutation'::text AND (mc.id IN ( SELECT DISTINCT mutation_data_table.molecular_characterization_id
		   FROM pdcm_api.mutation_data_table)) THEN 'TRUE'::text
		WHEN mct.name = 'expression'::text AND (mc.id IN ( SELECT DISTINCT expression_data_table.molecular_characterization_id
		   FROM pdcm_api.expression_data_table)) THEN 'TRUE'::text
		WHEN mct.name = 'copy number alteration'::text AND (mc.id IN ( SELECT DISTINCT cna_data_table.molecular_characterization_id
		   FROM pdcm_api.cna_data_table)) THEN 'TRUE'::text
		WHEN mct.name = 'cytogenetics'::text AND (mc.id IN ( SELECT DISTINCT cytogenetics_data_table.molecular_characterization_id
		   FROM pdcm_api.cytogenetics_data_table)) THEN 'TRUE'::text
		ELSE 'FALSE'::text
	END AS data_availability
FROM
  molecular_characterization mc
  JOIN molecular_characterization_type mct ON mc.molecular_characterization_type_id = mct.id
) mol_char
  JOIN model_information mi ON mol_char.pdcm_model_id = mi.id
  JOIN platform pf ON pf.id = mol_char.platform_id
  LEFT JOIN xenograft_sample xs ON xs.id = mol_char.xenograft_sample_id;

COMMENT ON MATERIALIZED VIEW pdcm_api.model_molecular_metadata IS 'Information about the molecular data that is available for each sample';

COMMENT ON COLUMN pdcm_api.model_molecular_metadata.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.data_source IS 'Data source of the model';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.source IS '(patient, xenograft, cell)';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.sample_id IS 'Sample identifier given by the provider';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.xenograft_passage IS 'Passage number of the sample';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.raw_data_url IS 'URL where the raw data could be found';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.data_type IS 'Type of molecular data';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.platform_name IS 'Name of the platform technology used to produce the molecular characterization';
COMMENT ON COLUMN pdcm_api.model_molecular_metadata.data_availability IS 'True or False depending on whether or not there is molecular data for this sample';


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

COMMENT ON MATERIALIZED VIEW pdcm_api.patient_treatment_extended IS 'Patient treatment data';

COMMENT ON COLUMN pdcm_api.patient_treatment_extended.model_id IS 'Full name of the model used by provider';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.data_source IS 'Data source of the model';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.patient_id IS 'Anonymous/de-identified provider ID for the patient';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.patient_age IS 'Patient age at collection';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.patient_sex IS 'Sex of the patient';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.patient_ethnicity IS 'Patient Ethnic group';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.patient_treatment_status IS 'Status of the patient treatment';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.histology IS 'Diagnosis at time of collection of the patient tumor';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.treatment IS 'Treatment name. It can be surgery, radotherapy,  drug name  or drug combination';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.response IS 'Response of prior treatment';
COMMENT ON COLUMN pdcm_api.patient_treatment_extended.dose IS 'Treatment dose and unit';
