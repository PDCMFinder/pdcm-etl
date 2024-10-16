----------------- Data Overview (Cohorts) ------------------

-- data_overview_mutation_cohorts

DROP MATERIALIZED VIEW IF exists pdcm_api.data_overview_mutation_cohorts;

CREATE MATERIALIZED VIEW pdcm_api.data_overview_mutation_cohorts AS 
SELECT 
  model_id, 
  sample_id, 
  hgnc_symbol as symbol, 
  amino_acid_change, 
  consequence, 
  si.data_source as provider, 
  model_type as type, 
  cancer_system, 
  read_depth, 
  seq_start_position, 
  ref_allele, 
  alt_allele 
FROM 
  pdcm_api.mutation_data_extended mut, 
  search_index si 
WHERE 
  mut.model_id = si.external_model_id 
  AND mut.data_source = si.data_source 
  AND cancer_system != 'Unclassified' 
  and hgnc_symbol in (
    'ALK', 'BCL2', 'BRAF', 'BRCA1', 'BRCA2', 
    'EGFR', 'ESR1', 'PGR', 'FGFR2', 'FGFR3', 
    'ERBB2', 'IDH1', 'IDH2', 'IRF4', 'KRAS', 
    'MYC', 'PIK3CA', 'RET', 'ROS1'
  );

CREATE INDEX mutation_cohorts_cancer_system_idx
  ON pdcm_api.data_overview_mutation_cohorts (cancer_system);

CREATE INDEX mutation_cohorts_type_idx
  ON pdcm_api.data_overview_mutation_cohorts (type);


-- data_overview_expression_cohorts

DROP MATERIALIZED VIEW IF exists pdcm_api.data_overview_expression_cohorts;

CREATE MATERIALIZED VIEW pdcm_api.data_overview_expression_cohorts AS 
SELECT 
  model_id, 
  sample_id, 
  hgnc_symbol as symbol, 
  rnaseq_fpkm, 
  si.data_source as provider, 
  model_type as type, 
  cancer_system, 
  log(2.0, rnaseq_fpkm + 0.001) as rnaseq_fpkm_log
FROM 
  pdcm_api.expression_data_extended exp, 
  search_index si 
WHERE 
  exp.model_id = si.external_model_id 
  AND exp.data_source = si.data_source 
  AND cancer_system != 'Unclassified' 
  AND rnaseq_fpkm is not null
  AND hgnc_symbol in (
    'ALK', 'BCL2', 'BRAF', 'BRCA1', 'BRCA2', 
    'EGFR', 'ESR1', 'PGR', 'FGFR2', 'FGFR3', 
    'ERBB2', 'IDH1', 'IDH2', 'IRF4', 'KRAS', 
    'MYC', 'PIK3CA', 'RET', 'ROS1'
  );

CREATE INDEX expression_cohorts_symbol_idx
  ON pdcm_api.data_overview_mutation_cohorts (symbol);

CREATE INDEX expression_cohorts_type_idx
  ON pdcm_api.data_overview_mutation_cohorts (type);
