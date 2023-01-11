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

