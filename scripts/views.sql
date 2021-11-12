CREATE MATERIALIZED VIEW details_molecular_data AS SELECT molecular_characterization.id, ps.external_patient_sample_id as patient_sample_id, ps.model_id as patient_model_id, xs.external_xenograft_sample_id as xenograft_sample_id, xs.model_id as xenograft_model_id, xs.passage as xenograft_passage, molecular_characterization.raw_data_url, data_type.name as data_type, platform.id as platform_id, platform.instrument_model as platform_name FROM molecular_characterization INNER JOIN  platform on molecular_characterization.platform_id = platform.id INNER JOIN molecular_characterization_type data_type on molecular_characterization.molecular_characterization_type_id = data_type.id LEFT JOIN patient_sample ps on molecular_characterization.patient_sample_id = ps.id LEFT JOIN xenograft_sample xs on molecular_characterization.xenograft_sample_id = xs.id;

CREATE MATERIALIZED VIEW mutation_data_table AS SELECT *, mmpxmg::text as text FROM (SELECT mc.id as molecular_characterization_id, gm.approved_symbol as hgnc_symbol, amino_acid_change, consequence, read_depth, allele_frequency, seq_start_position, ref_allele, alt_allele FROM mutation_measurement_data mmd LEFT JOIN molecular_characterization mc on mc.id = mmd.molecular_characterization_id LEFT JOIN  patient_sample ps on mc.patient_sample_id = ps.id LEFT JOIN xenograft_sample xs on mc.xenograft_sample_id = xs.id LEFT JOIN mutation_marker mm on mmd.mutation_marker_id = mm.id INNER JOIN gene_marker gm on mm.gene_marker_id = gm.id) as mmpxmg;

CREATE MATERIALIZED VIEW mutation_data_table_columns AS SELECT molecular_characterization_id, array_agg(not_empty_cols) as not_empty_cols FROM (SELECT DISTINCT molecular_characterization_id, jsonb_object_keys(jsonb_strip_nulls(to_jsonb(mutation_data_table))) not_empty_cols FROM mutation_data_table) temp GROUP BY molecular_characterization_id;

CREATE MATERIALIZED VIEW expression_data_table AS SELECT *, mcxexpxgm::text as text FROM (SELECT emd.molecular_characterization_id, gm.approved_symbol as hgnc_symbol, emd.rnaseq_coverage, emd.rnaseq_fpkm, emd.rnaseq_tpm, emd.rnaseq_count, emd.affy_hgea_probe_id, emd.affy_hgea_expression_value, emd.illumina_hgea_probe_id, emd.illumina_hgea_expression_value, emd.z_score FROM expression_molecular_data emd INNER JOIN gene_marker gm on gm.id = emd.gene_marker_id) as mcxexpxgm;



CREATE MATERIALIZED VIEW expression_data_table_columns AS SELECT molecular_characterization_id, array_agg(not_empty_cols) as not_empty_cols FROM (SELECT DISTINCT molecular_characterization_id, jsonb_object_keys(jsonb_strip_nulls(to_jsonb(expression_data_table))) not_empty_cols FROM expression_data_table) temp GROUP BY molecular_characterization_id;

CREATE MATERIALIZED VIEW expression_data_table_columns_temp AS  SELECT
                   molecular_characterization_id,
                   (array_agg(rnaseq_coverage) FILTER (WHERE rnaseq_coverage IS NOT NULL))[1] as rnaseq_coverage,
                   (array_agg(rnaseq_fpkm) FILTER (WHERE rnaseq_fpkm IS NOT NULL))[1] as rnaseq_fpkm,
                   (array_agg(rnaseq_tpm) FILTER (WHERE rnaseq_tpm IS NOT NULL))[1] as rnaseq_tpm,
                   (array_agg(rnaseq_count) FILTER (WHERE rnaseq_count IS NOT NULL))[1] as rnaseq_count,
                   (array_agg(affy_hgea_probe_id) FILTER (WHERE affy_hgea_probe_id IS NOT NULL))[1] as affy_hgea_probe_id,
                   (array_agg(affy_hgea_expression_value) FILTER (WHERE affy_hgea_expression_value IS NOT NULL))[1] as affy_hgea_expression_value,
                   (array_agg(illumina_hgea_probe_id) FILTER (WHERE illumina_hgea_probe_id IS NOT NULL))[1] as illumina_hgea_probe_id,
                   (array_agg(illumina_hgea_expression_value) FILTER (WHERE illumina_hgea_expression_value IS NOT NULL))[1] as illumina_hgea_expression_value,
                   (array_agg(z_score) FILTER (WHERE z_score IS NOT NULL))[1] as z_score
            FROM expression_data_table GROUP BY molecular_characterization_id;

CREATE MATERIALIZED VIEW expression_data_table_columns AS
    SELECT molecular_characterization_id, array_agg(not_empty_cols) as not_empty_cols FROM (
        SELECT DISTINCT molecular_characterization_id, jsonb_object_keys(jsonb_strip_nulls(to_jsonb(data_availability))) not_empty_cols FROM expression_data_table_columns_temp data_availability ) tmp GROUP BY molecular_characterization_id;


CREATE MATERIALIZED VIEW cytogenetics_data_table AS SELECT *, ct::text as text FROM (
    SELECT mc.id as molecular_characterization_id,
           gm.approved_symbol as hgnc_symbol,
           c.marker_status as result
    FROM cytogenetics_molecular_data c LEFT JOIN molecular_characterization mc ON mc.id = c.molecular_characterization_id LEFT JOIN gene_marker gm on c.gene_marker_id = gm.id
    ) as ct;


CREATE MATERIALIZED VIEW cna_data_table AS SELECT *, cnat::text as text FROM (
    SELECT gm.approved_symbol as hgnc_symbol,
           cna.*
    FROM cna_molecular_data cna LEFT JOIN molecular_characterization mc ON mc.id = cna.molecular_characterization_id LEFT JOIN gene_marker gm on cna.gene_marker_id = gm.id
    ) as cnat;

CREATE MATERIALIZED VIEW cna_data_table_columns AS SELECT molecular_characterization_id, array_agg(not_empty_cols) as not_empty_cols FROM (SELECT DISTINCT molecular_characterization_id, jsonb_object_keys(jsonb_strip_nulls(to_jsonb(cna_data_table))) not_empty_cols FROM cna_data_table) temp GROUP BY molecular_characterization_id;


CREATE MATERIALIZED VIEW models_by_cancer AS SELECT cancer_system, histology, count(*) FROM search_index GROUP BY cancer_system, histology;

CREATE MATERIALIZED VIEW models_by_mutated_gene AS SELECT left(unnest(makers_with_mutation_data), strpos(unnest(makers_with_mutation_data), '/') - 1) as mutated_gene, count(DISTINCT pdcm_model_id) FROM search_index GROUP BY mutated_gene;


CREATE MATERIALIZED VIEW models_by_dataset_availability AS SELECT unnest(dataset_available) as dataset_availability, COUNT(DISTINCT pdcm_model_id) FROM search_index GROUP BY  dataset_availability;

DROP MATERIALIZED VIEW models_by_mutated_gene;