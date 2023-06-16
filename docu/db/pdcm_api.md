**NOTE**: this document is generated, do not edit manually.

# Documentation of tables and views in schema `pdcm_api`

## Tables


## Views
### cna_data_extended

CNA molecular data

  CNA data with the model and sample it comes from.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|model_id|text|Full name of the model used by provider|
|data_source|character varying|Data source of the model (provider abbreviation)|
|source|text|(patient, xenograft, cell)|
|sample_id|text|Sample identifier given by the provider|
|hgnc_symbol|text|Gene symbol|
|chromosome|text|-|
|strand|text|-|
|log10r_cna|text|Log10 scaled copy number variation ratio|
|log2r_cna|text|Log2 scaled copy number variation ratio|
|seq_start_position|numeric|-|
|seq_end_position|numeric|-|
|copy_number_status|text|Details whether there was a gain or loss of function. Categorized into gain, loss|
|gistic_value|text|Score predicted using GISTIC tool for the copy number variation|
|picnic_value|text|Score predicted using PICNIC algorithm for the copy number variation|
|external_db_links|json|Links to external resources|





---
### cna_data_table



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|hgnc_symbol|text|-|
|chromosome|text|-|
|strand|text|-|
|log10r_cna|text|-|
|log2r_cna|text|-|
|seq_start_position|numeric|-|
|seq_end_position|numeric|-|
|copy_number_status|text|-|
|gistic_value|text|-|
|picnic_value|text|-|
|non_harmonised_symbol|text|-|
|harmonisation_result|text|-|
|molecular_characterization_id|bigint|-|
|external_db_links|json|-|
|text|text|-|





---
### contact_form



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|form_url|text|-|





---
### contact_people



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|name_list|text|-|
|email_list|text|-|





---
### cytogenetics_data_extended

Cytogenetics molecular data

  Cytogenetics data with the model and sample it comes from.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|model_id|text|Full name of the model used by provider|
|data_source|character varying|Data source of the model (provider abbreviation)|
|source|text|(patient, xenograft, cell)|
|sample_id|text|Sample identifier given by the provider|
|hgnc_symbol|text|Gene symbol|
|non_harmonised_symbol|text|-|
|result|text|Presence or absence of the cytogenetic marker|
|external_db_links|json|Links to external resources|





---
### cytogenetics_data_table



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|molecular_characterization_id|bigint|-|
|hgnc_symbol|text|-|
|non_harmonised_symbol|text|-|
|result|text|-|
|external_db_links|json|-|
|text|text|-|





---
### engraftment_sample_state



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|name|text|-|





---
### engraftment_sample_type



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|name|text|-|





---
### engraftment_site



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|name|text|-|





---
### engraftment_type



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|name|text|-|





---
### expression_data_extended

Expression molecular data

  Expression data with the model and sample it comes from.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|model_id|text|Full name of the model used by provider|
|data_source|character varying|Data source of the model (provider abbreviation)|
|source|text|(patient, xenograft, cell)|
|sample_id|text|Sample identifier given by the provider|
|hgnc_symbol|text|Gene symbol|
|rnaseq_coverage|text|The ratio between the number of bases of the mapped reads by the number of bases of a reference|
|rnaseq_fpkm|text|Gene expression value represented in Fragments per kilo base of transcript per million mapped fragments (FPKM)|
|rnaseq_tpm|text|Gene expression value represented in transcript per million (TPM)|
|rnaseq_count|text|Read counts of the gene|
|affy_hgea_probe_id|text|Affymetrix probe identifier|
|affy_hgea_expression_value|text|Expresion value captured using Affymetrix arrays|
|illumina_hgea_probe_id|text|Illumina probe identifier|
|illumina_hgea_expression_value|text|Expresion value captured using Illumina arrays|
|z_score|text|Z-score representing the gene expression level|
|external_db_links|json|Links to external resources|





---
### expression_data_table



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|molecular_characterization_id|bigint|-|
|hgnc_symbol|text|-|
|non_harmonised_symbol|text|-|
|rnaseq_coverage|text|-|
|rnaseq_fpkm|text|-|
|rnaseq_tpm|text|-|
|rnaseq_count|text|-|
|affy_hgea_probe_id|text|-|
|affy_hgea_expression_value|text|-|
|illumina_hgea_probe_id|text|-|
|illumina_hgea_expression_value|text|-|
|z_score|text|-|
|external_db_links|json|-|
|text|text|-|





---
### host_strain



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|name|text|-|
|nomenclature|text|-|





---
### model_information



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|external_model_id|text|-|
|data_source|character varying|-|
|publication_group_id|bigint|-|
|accessibility_group_id|bigint|-|
|contact_people_id|bigint|-|
|contact_form_id|bigint|-|
|source_database_id|bigint|-|
|license_id|bigint|-|





---
### model_metadata

Model metadata

  Metadata associated to a model.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|model_id|text|Full name of the model used by provider|
|data_source|character varying|Data source of the model (provider abbreviation)|
|provider_name|text|Provider full name|
|type|text|Model type (xenograft, cell_line, …)|
|host_strain_name|text|Host mouse strain name (e.g. NOD-SCID, NSG, etc)|
|host_strain_nomenclature|text|The full nomenclature form of the host mouse strain name|
|engraftment_site|text|Organ or anatomical site used for the tumour engraftment (e.g. mammary fat pad, Right flank)|
|engraftment_type|text|Orthotopic: if the tumour was engrafted at a corresponding anatomical site. Hererotopic: If grafted subcuteanously|
|engraftment_sample_type|text|Description of the type of material grafted into the mouse. (e.g. tissue fragments, cell suspension)|
|engraftment_sample_state|text|PDX Engraftment material state (e.g. fresh or frozen)|
|passage_number|text|Passage number|
|histology|text|Diagnosis at time of collection of the patient tumor|
|cancer_system|text|Cancer System|
|primary_site|text|Site of the primary tumor where primary cancer is originating from (may not correspond to the site of the current tissue sample)|
|collection_site|text|Site of collection of the tissue sample (can be different than the primary site if tumour type is metastatic)|
|tumor_type|text|Collected tumor type|
|cancer_grade|text|The implanted tumor grade value|
|cancer_grading_system|text|Stage classification system used to describe the stage|
|cancer_stage|text|Stage of the patient at the time of collection|
|patient_age|text|Patient age at collection|
|patient_sex|text|Sex of the patient|
|patient_ethnicity|text|Patient Ethnic group|
|patient_treatment_status|text|Patient treatment status|
|pubmed_ids|text|PubMed ids related to the model|
|europdx_access_modalities|text|If a model is part of EUROPDX consortium, then this field defines if the model is accessible for transnational access through the EDIReX infrastructure, or only on a collaborative basis|
|accessibility|text|Defines any limitation of access of the model per type of users like academia only, industry and academia, or national limitation|
|contact_name_list|text|List of names of the contact people for the model|
|contact_email_list|text|Emails of the contact names for the model|
|contact_form_url|text|URL to the providers resource for each model|
|source_database_url|text|URL to the source database for each model|





---
### model_quality_assurance

Quality assurance

  Quality assurance data related to a model.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|model_id|text|Full name of the model used by provider|
|data_source|character varying|Data source of the model (provider abbreviation)|
|description|text|Short description of what was compared and what was the result: (e.g. high, good, moderate concordance between xenograft, 'model validated against histological features of same diagnosis' or 'not determined')|
|passages_tested|text|List of all passages where validation was performed. Passage 0 correspond to first engraftment|
|validation_technique|text|Any technique used to validate PDX against their original patient tumour, including fingerprinting, histology, immunohistochemistry|
|validation_host_strain_nomenclature|text|Validation host mouse strain, following mouse strain nomenclature from MGI JAX|





---
### molecular_data_restriction



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|data_source|text|-|
|molecular_data_table|text|-|





---
### mutation_data_extended

Mutation molecular data

  Mutation data with the model and sample it comes from.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|model_id|text|Full name of the model used by provider|
|sample_id|text|Sample identifier given by the provider|
|source|text|(patient, xenograft, cell)|
|hgnc_symbol|text|Gene symbol|
|amino_acid_change|text|Changes in the amino acid due to the variant|
|consequence|text|Genomic consequence of this variant, for example: insertion of a codon caused frameshift variation will be considered frameshift variant |
|read_depth|text|Read depth, the number of times each individual base was sequenced|
|allele_frequency|text|Allele frequency, the relative frequency of an allele in a population|
|seq_start_position|text|Location on the genome at which the variant is found|
|ref_allele|text|The base seen in the reference genome|
|alt_allele|text|The base other than the reference allele seen at the locus|
|data_source|text|Data source of the model (provider abbreviation)|
|external_db_links|json|-|





---
### mutation_data_table



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|molecular_characterization_id|bigint|-|
|hgnc_symbol|text|-|
|non_harmonised_symbol|text|-|
|amino_acid_change|text|-|
|chromosome|text|-|
|strand|text|-|
|consequence|text|-|
|read_depth|text|-|
|allele_frequency|text|-|
|seq_start_position|text|-|
|ref_allele|text|-|
|alt_allele|text|-|
|external_db_links|json|-|
|text|text|-|





---
### provider_group



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|name|text|-|
|abbreviation|text|-|
|description|text|-|
|provider_type_id|bigint|-|
|project_group_id|bigint|-|





---
### publication_group



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|pubmed_ids|text|-|





---
### quality_assurance



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|description|text|-|
|passages_tested|text|-|
|validation_technique|text|-|
|validation_host_strain_nomenclature|text|-|
|model_id|bigint|-|





---
### release_info



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|name|text|-|
|date|timestamp without time zone|-|
|providers|text[]|-|





---
### search_facet



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|facet_section|text|-|
|facet_name|text|-|
|facet_column|text|-|
|facet_options|text[]|-|
|facet_example|text|-|





---
### search_index



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|pdcm_model_id|bigint|-|
|external_model_id|text|-|
|data_source|text|-|
|project_name|text|-|
|provider_name|text|-|
|model_type|text|-|
|histology|text|-|
|search_terms|text[]|-|
|cancer_system|text|-|
|dataset_available|text[]|-|
|license_name|text|-|
|license_url|text|-|
|primary_site|text|-|
|collection_site|text|-|
|tumour_type|text|-|
|cancer_grade|text|-|
|cancer_grading_system|text|-|
|cancer_stage|text|-|
|cancer_staging_system|text|-|
|patient_age|text|-|
|patient_sex|text|-|
|patient_history|text|-|
|patient_ethnicity|text|-|
|patient_ethnicity_assessment_method|text|-|
|patient_initial_diagnosis|text|-|
|patient_treatment_status|text|-|
|patient_age_at_initial_diagnosis|text|-|
|patient_sample_id|text|-|
|patient_sample_collection_date|text|-|
|patient_sample_collection_event|text|-|
|patient_sample_months_since_collection_1|text|-|
|patient_sample_virology_status|text|-|
|patient_sample_sharable|text|-|
|patient_sample_treated_at_collection|text|-|
|patient_sample_treated_prior_to_collection|text|-|
|pdx_model_publications|text|-|
|quality_assurance|json|-|
|xenograft_model_specimens|json|-|
|makers_with_cna_data|text[]|-|
|makers_with_mutation_data|text[]|-|
|makers_with_expression_data|text[]|-|
|makers_with_cytogenetics_data|text[]|-|
|breast_cancer_biomarkers|text[]|-|
|treatment_list|text[]|-|
|model_treatment_list|text[]|-|
|score|numeric|-|
|model_dataset_type_count|integer|-|





---
### source_database



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|database_url|text|-|





---
### xenograft_model_specimen



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|passage_number|text|-|
|engraftment_site_id|bigint|-|
|engraftment_type_id|bigint|-|
|engraftment_sample_type_id|bigint|-|
|engraftment_sample_state_id|bigint|-|
|host_strain_id|bigint|-|
|model_id|bigint|-|





---
