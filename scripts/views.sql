CREATE MATERIALIZED VIEW details_molecular_data AS
 SELECT molecular_characterization.id,
    ps.external_patient_sample_id AS patient_sample_id,
    ps.model_id AS patient_model_id,
    xs.external_xenograft_sample_id AS xenograft_sample_id,
    xs.model_id AS xenograft_model_id,
    xs.passage AS xenograft_passage,
    molecular_characterization.raw_data_url,
    data_type.name AS data_type,
    platform.id AS platform_id,
    platform.instrument_model AS platform_name
   FROM ((((molecular_characterization
     JOIN platform ON ((molecular_characterization.platform_id = platform.id)))
     JOIN molecular_characterization_type data_type ON ((molecular_characterization.molecular_characterization_type_id = data_type.id)))
     LEFT JOIN patient_sample ps ON ((molecular_characterization.patient_sample_id = ps.id)))
     LEFT JOIN xenograft_sample xs ON ((molecular_characterization.xenograft_sample_id = xs.id)));

CREATE MATERIALIZED VIEW mutation_data_table AS
 SELECT mmpxmg.molecular_characterization_id,
    mmpxmg.hgnc_symbol,
    mmpxmg.amino_acid_change,
    mmpxmg.consequence,
    mmpxmg.read_depth,
    mmpxmg.allele_frequency,
    mmpxmg.seq_start_position,
    mmpxmg.ref_allele,
    mmpxmg.alt_allele,
    (mmpxmg.*)::text AS text
   FROM ( SELECT mc.id AS molecular_characterization_id,
            gm.approved_symbol AS hgnc_symbol,
            mm.amino_acid_change,
            mm.consequence,
            mmd.read_depth,
            mmd.allele_frequency,
            mm.seq_start_position,
            mm.ref_allele,
            mm.alt_allele
           FROM (((((mutation_measurement_data mmd
             LEFT JOIN molecular_characterization mc ON ((mc.id = mmd.molecular_characterization_id)))
             LEFT JOIN patient_sample ps ON ((mc.patient_sample_id = ps.id)))
             LEFT JOIN xenograft_sample xs ON ((mc.xenograft_sample_id = xs.id)))
             LEFT JOIN mutation_marker mm ON ((mmd.mutation_marker_id = mm.id)))
             JOIN gene_marker gm ON ((mm.gene_marker_id = gm.id)))) mmpxmg;

CREATE MATERIALIZED VIEW mutation_data_table_columns AS
 SELECT temp.molecular_characterization_id,
    array_agg(temp.not_empty_cols) AS not_empty_cols
   FROM ( SELECT DISTINCT mutation_data_table.molecular_characterization_id,
            jsonb_object_keys(jsonb_strip_nulls(to_jsonb(mutation_data_table.*))) AS not_empty_cols
           FROM mutation_data_table) temp
  GROUP BY temp.molecular_characterization_id;

CREATE MATERIALIZED VIEW expression_data_table AS
 SELECT mcxexpxgm.molecular_characterization_id,
    mcxexpxgm.hgnc_symbol,
    mcxexpxgm.rnaseq_coverage,
    mcxexpxgm.rnaseq_fpkm,
    mcxexpxgm.rnaseq_tpm,
    mcxexpxgm.rnaseq_count,
    mcxexpxgm.affy_hgea_probe_id,
    mcxexpxgm.affy_hgea_expression_value,
    mcxexpxgm.illumina_hgea_probe_id,
    mcxexpxgm.illumina_hgea_expression_value,
    mcxexpxgm.z_score,
    (mcxexpxgm.*)::text AS text
   FROM ( SELECT emd.molecular_characterization_id,
            gm.approved_symbol AS hgnc_symbol,
            emd.rnaseq_coverage,
            emd.rnaseq_fpkm,
            emd.rnaseq_tpm,
            emd.rnaseq_count,
            emd.affy_hgea_probe_id,
            emd.affy_hgea_expression_value,
            emd.illumina_hgea_probe_id,
            emd.illumina_hgea_expression_value,
            emd.z_score
           FROM (expression_molecular_data emd
             JOIN gene_marker gm ON ((gm.id = emd.gene_marker_id)))) mcxexpxgm;

CREATE MATERIALIZED VIEW expression_data_table_columns AS
 SELECT temp.molecular_characterization_id,
    array_agg(temp.not_empty_cols) AS not_empty_cols
   FROM ( SELECT DISTINCT expression_data_table.molecular_characterization_id,
            jsonb_object_keys(jsonb_strip_nulls(to_jsonb(expression_data_table.*))) AS not_empty_cols
           FROM expression_data_table) temp
  GROUP BY temp.molecular_characterization_id;

CREATE MATERIALIZED VIEW cytogenetics_data_table AS
 SELECT ct.molecular_characterization_id,
    ct.hgnc_symbol,
    ct.result,
    (ct.*)::text AS text
   FROM ( SELECT mc.id AS molecular_characterization_id,
            gm.approved_symbol AS hgnc_symbol,
            c.marker_status AS result
           FROM ((cytogenetics_molecular_data c
             LEFT JOIN molecular_characterization mc ON ((mc.id = c.molecular_characterization_id)))
             LEFT JOIN gene_marker gm ON ((c.gene_marker_id = gm.id)))) ct;

CREATE MATERIALIZED VIEW cna_data_table AS
 SELECT cnat.hgnc_symbol,
    cnat.id,
    cnat.log10r_cna,
    cnat.log2r_cna,
    cnat.copy_number_status,
    cnat.gistic_value,
    cnat.picnic_value,
    cnat.gene_marker_id,
    cnat.non_harmonised_symbol,
    cnat.harmonisation_result,
    cnat.molecular_characterization_id,
    (cnat.*)::text AS text
   FROM ( SELECT gm.approved_symbol AS hgnc_symbol,
            cna.id,
            cna.log10r_cna,
            cna.log2r_cna,
            cna.copy_number_status,
            cna.gistic_value,
            cna.picnic_value,
            cna.gene_marker_id,
            cna.non_harmonised_symbol,
            cna.harmonisation_result,
            cna.molecular_characterization_id
           FROM ((cna_molecular_data cna
             LEFT JOIN molecular_characterization mc ON ((mc.id = cna.molecular_characterization_id)))
             LEFT JOIN gene_marker gm ON ((cna.gene_marker_id = gm.id)))) cnat;

CREATE MATERIALIZED VIEW cna_data_table_columns AS
 SELECT temp.molecular_characterization_id,
    array_agg(temp.not_empty_cols) AS not_empty_cols
   FROM ( SELECT DISTINCT cna_data_table.molecular_characterization_id,
            jsonb_object_keys(jsonb_strip_nulls(to_jsonb(cna_data_table.*))) AS not_empty_cols
           FROM cna_data_table) temp
  GROUP BY temp.molecular_characterization_id;

CREATE MATERIALIZED VIEW models_by_cancer AS
 SELECT search_index.cancer_system,
    search_index.histology,
    count(*) AS count
   FROM search_index
  GROUP BY search_index.cancer_system, search_index.histology;

CREATE MATERIALIZED VIEW models_by_mutated_gene AS
 SELECT "left"(unnest(search_index.makers_with_mutation_data), (strpos(unnest(search_index.makers_with_mutation_data), '/'::text) - 1)) AS mutated_gene,
    count(DISTINCT search_index.pdcm_model_id) AS count
   FROM search_index
  GROUP BY ("left"(unnest(search_index.makers_with_mutation_data), (strpos(unnest(search_index.makers_with_mutation_data), '/'::text) - 1)));

CREATE MATERIALIZED VIEW models_by_dataset_availability AS
 SELECT unnest(search_index.dataset_available) AS dataset_availability,
    count(DISTINCT search_index.pdcm_model_id) AS count
   FROM search_index
  GROUP BY (unnest(search_index.dataset_available));

CREATE MATERIALIZED VIEW dosing_studies AS
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

CREATE MATERIALIZED VIEW patient_treatment AS
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
					   patient_snapshot psnap,
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
				       AND tp.patient_id = psnap.patient_id
				       AND psnap.sample_id = ps.id
                       AND m.id = ps.model_id
                       AND tp.response_id = r.id
                       AND tc.treatment_protocol_id = tp.id
                       AND tc.treatment_id = t.id
                       AND t.data_source = m.data_source) a)b
 GROUP  BY model_id,
          response,
          dose;

CREATE MATERIALIZED VIEW models_by_treatment AS
 SELECT unnest(search_index.treatment_list) AS treatment,
    count(DISTINCT search_index.pdcm_model_id) AS count
   FROM search_index
  GROUP BY (unnest(search_index.treatment_list));

CREATE materialized VIEW models_by_type AS
SELECT
  model_type,
  count(1) 
FROM
  search_index
GROUP BY
  model_type;
