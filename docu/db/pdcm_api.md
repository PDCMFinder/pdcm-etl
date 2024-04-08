**NOTE**: this document is generated, do not edit manually.

# Documentation of tables and views in schema `pdcm_api`

## Tables


## Views
### biomarker_data_extended

Biomarker molecular data

  Biomarker data with the model and sample it comes from.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|model_id|text|Full name of the model used by provider|
|data_source|character varying|Data source of the model (provider abbreviation)|
|source|text|(patient, xenograft, cell)|
|sample_id|text|Sample identifier given by the provider|
|biomarker|text|Gene symbol|
|non_harmonised_symbol|text|Original symbol as reported by the provider|
|result|text|Presence or absence of the biomarker|
|external_db_links|json|Links to external resources|





---
### biomarker_data_table

Biomarker molecular data

  Biomarker data with the model and sample it comes from.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|molecular_characterization_id|bigint|Reference to the molecular_characterization_ table|
|biomarker|text|Gene symbol|
|non_harmonised_symbol|text|Original symbol as reported by the provider|
|result|text|Presence or absence of the biomarker|
|external_db_links|json|Links to external resources|
|text|text|Text representation of the row|
|data_source|text|Data source of the model (provider abbreviation)|





---
### cell_model

Cell model

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|model_name|text|Most common name associate with model. Please use the CCLE name if available|
|model_name_aliases|text|model_name_aliases|
|type|text|Type of organoid or cell model|
|growth_properties|text|Observed growth properties of the related model|
|growth_media|text|Base media formulation the model was grown in|
|media_id|text|Unique identifier for each media formulation (Catalogue number)|
|parent_id|text|model Id of the model used to generate the model|
|origin_patient_sample_id|text|Unique ID of the patient tumour sample used to generate the model|
|model_id|bigint|Reference to model_information_table|
|plate_coating|text|Coating on plate model was grown in|
|other_plate_coating|text|Other coating on plate model was grown in (not mentioned above)|
|passage_number|text|Passage number at time of sequencing/screening|
|contaminated|text|Is there contamination present in the model|
|contamination_details|text|What are the details of the contamination|
|supplements|text|Additional supplements the model was grown with|
|drug|text|Additional drug/compounds the model was grown with|
|drug_concentration|text|Concentration of Additional drug/compounds the model was grown with|





---
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
|chromosome|text|Chromosome where the DNA copy occurs|
|strand|text|Orientation of the DNA strand associated with the observed copy number changes, whether it is the positive or negative strand|
|log10r_cna|numeric|Log10 scaled copy number variation ratio|
|log2r_cna|numeric|Log2 scaled copy number variation ratio|
|seq_start_position|numeric|Starting position of a genomic sequence or region that is associated with a copy number alteration|
|seq_end_position|numeric|Ending position of a genomic sequence or region that is associated with a copy number alteration|
|copy_number_status|text|Details whether there was a gain or loss of function. Categorized into gain, loss|
|gistic_value|text|Score predicted using GISTIC tool for the copy number variation|
|picnic_value|text|Score predicted using PICNIC algorithm for the copy number variation|
|external_db_links|json|Links to external resources|





---
### cna_data_table

CNA molecular data

  CNA molecular data without joins to other tables.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|hgnc_symbol|text|Gene symbol|
|chromosome|text|Chromosome where the DNA copy occurs|
|strand|text|Orientation of the DNA strand associated with the observed copy number changes, whether it is the positive or negative strand|
|log10r_cna|numeric|Log10 scaled copy number variation ratio|
|log2r_cna|numeric|Log2 scaled copy number variation ratio|
|seq_start_position|numeric|Starting position of a genomic sequence or region that is associated with a copy number alteration|
|seq_end_position|numeric|Ending position of a genomic sequence or region that is associated with a copy number alteration|
|copy_number_status|text|Details whether there was a gain or loss of function. Categorized into gain, loss|
|gistic_value|text|Score predicted using GISTIC tool for the copy number variation|
|picnic_value|text|Score predicted using PICNIC algorithm for the copy number variation|
|non_harmonised_symbol|text|Original symbol as reported by the provider|
|harmonisation_result|text|Result of the symbol harmonisation process|
|molecular_characterization_id|bigint|Reference to the molecular_characterization_ table|
|external_db_links|json|JSON column with links to external resources|
|text|text|Text representation of the row|
|data_source|text|Data source (abbreviation of the provider)|





---
### contact_form

Contact form

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|form_url|text|Contact form link|





---
### contact_people

Contact information

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|name_list|text|Contact person (should match that included in email_list column)|
|email_list|text|Contact email for any requests from users about models. If multiple, included as comma separated list|





---
### engraftment_sample_state

PDX Engraftment material state (e.g. fresh or frozen)

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|name|text|Engraftment sample state|





---
### engraftment_sample_type

Description of the type of  material grafted into the mouse. (e.g. tissue fragments, cell suspension)

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|name|text|Engraftment sample type|





---
### engraftment_site

Organ or anatomical site used for the PDX tumour engraftment (e.g. mammary fat pad, Right flank)

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|name|text|Engraftment site|





---
### engraftment_type

PDX Engraftment Type: Orthotopic if the tumour was engrafted at a corresponding anatomical site (e.g. patient tumour of primary site breast was grafted in mouse mammary fat pad). If grafted subcuteanously hererotopic is used

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|name|text|Engraftment type|





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
|rnaseq_coverage|numeric|The ratio between the number of bases of the mapped reads by the number of bases of a reference|
|rnaseq_fpkm|numeric|Gene expression value represented in Fragments per kilo base of transcript per million mapped fragments (FPKM)|
|rnaseq_tpm|numeric|Gene expression value represented in transcript per million (TPM)|
|rnaseq_count|numeric|Read counts of the gene|
|affy_hgea_probe_id|text|Affymetrix probe identifier|
|affy_hgea_expression_value|numeric|Expresion value captured using Affymetrix arrays|
|illumina_hgea_probe_id|text|Illumina probe identifier|
|illumina_hgea_expression_value|numeric|Expresion value captured using Illumina arrays|
|z_score|numeric|Z-score representing the gene expression level|
|external_db_links|json|Links to external resources|





---
### expression_data_table

Expression molecular data (without joins)

  Expression molecular data without joins to other tables.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|molecular_characterization_id|bigint|Reference to the molecular_characterization table|
|hgnc_symbol|text|Gene symbol|
|non_harmonised_symbol|text|Original symbol as reported by the provider|
|rnaseq_coverage|numeric|The ratio between the number of bases of the mapped reads by the number of bases of a reference|
|rnaseq_fpkm|numeric|Gene expression value represented in Fragments per kilo base of transcript per million mapped fragments (FPKM)|
|rnaseq_tpm|numeric|Gene expression value represented in transcript per million (TPM)|
|rnaseq_count|numeric|Read counts of the gene|
|affy_hgea_probe_id|text|Affymetrix probe identifier|
|affy_hgea_expression_value|numeric|Expresion value captured using Affymetrix arrays|
|illumina_hgea_probe_id|text|Illumina probe identifier|
|illumina_hgea_expression_value|numeric|Expresion value captured using Illumina arrays|
|z_score|numeric|Z-score representing the gene expression level|
|external_db_links|json|Links to external resources|
|text|text|Text representation of the row|





---
### host_strain

Host strain information

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|name|text|Host mouse strain name (e.g. NOD-SCID, NSG, etc)|
|nomenclature|text|The full nomenclature form of the host mouse strain name|





---
### immunemarker_data_extended

Immunemarkers molecular data

  Immunemarkers data with the model and sample it comes from.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|model_id|text|Full name of the model used by provider|
|data_source|character varying|Data source of the model (provider abbreviation)|
|source|text|(patient, xenograft, cell)|
|sample_id|text|Sample identifier given by the provider|
|marker_type|text|Type of the immune marker|
|marker_name|text|Name of the immune marker|
|marker_value|text|Value or measurement associated with the immune marker|
|essential_or_additional_details|text|Additional details or notes about the immune marker|





---
### immunemarker_data_table

Immunemarker molecular data

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|molecular_characterization_id|bigint|Reference to the molecular_characterization_ table|
|marker_type|text|Marker type|
|marker_name|text|Name of the immune marker|
|marker_value|text|Value or measurement associated with the immune marker|
|essential_or_additional_details|text|Additional details or notes about the immune marker|





---
### model_information

Model information (without joins)

  Model creation.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|external_model_id|text|Unique identifier of the model. Given by the provider|
|type|text|Model Type|
|data_source|character varying|Abbreviation of the provider. Added explicitly here to help with queries|
|publication_group_id|bigint|Reference to the publication_group table. Corresponds to the publications the model is part of|
|accessibility_group_id|bigint|Reference to the accessibility_group table|
|contact_people_id|bigint|Reference to the contact_people table|
|contact_form_id|bigint|Reference to the contact_form table|
|source_database_id|bigint|Reference to the source_database table|
|license_id|bigint|Reference to the license table|
|external_ids|text|Depmap accession, Cellusaurus accession or other id. Please place in comma separated list|
|supplier_type|text|Model supplier type - commercial, academic, other|
|catalog_number|text|Catalogue number of cell model, if commercial|
|vendor_link|text|Link to purchasable cell model, if commercial|
|rrid|text|Cellosaurus ID|
|parent_id|text|model Id of the model used to generate the model|
|origin_patient_sample_id|text|Unique ID of the patient tumour sample used to generate the model|
|model_relationships|jsonb|-|





---
### model_metadata

Model metadata (joined with other tables)

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
|engraftment_type|text|Orthotopic: if the tumour was engrafted at a corresponding anatomical site. Heterotopic: If grafted subcuteanously|
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
|morphological_features|text|Morphological features of the model|
|snp_analysis|text|Was SNP analysis done on the model?|
|str_analysis|text|Was STR analysis done on the model?|
|tumour_status|text|Gene expression validation of established model|
|model_purity|text|Presence of tumour vs stroma or normal cells|
|comments|text|Comments about the model that cannot be expressed by other fields|





---
### molecular_data_restriction

Internal table to store molecular tables which data cannot be displayed to the user. Configured at provider level.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|data_source|text|Provider with the restriction|
|molecular_data_table|text|Table whose data cannot be showed|





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
|external_db_links|json|JSON column with links to external resources|





---
### mutation_data_table

Mutation measurement data

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|molecular_characterization_id|bigint|Reference to the molecular_characterization_ table|
|hgnc_symbol|text|Gene symbol|
|non_harmonised_symbol|text|Original symbol as reported by the provider|
|amino_acid_change|text|Changes in the amino acid due to the variant|
|chromosome|text|Chromosome where the mutation occurs|
|strand|text|Orientation of the DNA strand where a mutation is located|
|consequence|text|Genomic consequence of this variant, for example: insertion of a codon caused frameshift variation will be considered frameshift variant |
|read_depth|text|Read depth, the number of times each individual base was sequenced|
|allele_frequency|text|Allele frequency, the relative frequency of an allele in a population|
|seq_start_position|text|Location on the genome at which the variant is found|
|ref_allele|text|The base seen in the reference genome|
|alt_allele|text|The base other than the reference allele seen at the locus|
|biotype|text|Biotype of the transcript or regulatory feature eg. protein coding, non coding|
|external_db_links|json|JSON column with links to external resources|
|data_source|text|Data source (abbreviation of the provider)|
|text|text|Text representation of the row|





---
### project_group

Projects

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|name|text|-|





---
### provider_group

Information of data providers

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|name|text|Project name|
|abbreviation|text|Provider abbreviation|
|description|text|A description of the provider|
|provider_type_id|bigint|Reference to the provider type|
|project_group_id|bigint|Reference to the project the provider belongs to|





---
### publication_group

Groups of publications that are associated to one or more models

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|pubmed_ids|text|pubmed IDs separated by commas|





---
### quality_assurance

Cell Sample

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|description|text|Short description of what was compared and what was the result: (e.g. high, good, moderate concordance between xenograft, 'model validated against histological features of same diagnosis' or 'not determined') - It needs to be clear if the model is validated or not.|
|passages_tested|text|Provide a list of all passages where validation was performed. Passage 0 correspond to first engraftment (if this is not the case please define how passages are numbered)|
|validation_technique|text|Any technique used to validate PDX against their original patient tumour, including fingerprinting, histology, immunohistochemistry|
|validation_host_strain_nomenclature|text|Validation host mouse strain, following mouse strain nomenclature from MGI JAX|
|model_id|bigint|Reference to the model_information table|
|morphological_features|text|-|
|snp_analysis|text|-|
|str_analysis|text|-|
|tumour_status|text|-|
|model_purity|text|-|
|comments|text|-|





---
### release_info

Table that shows columns with data per data source / molecular data table

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|name|text|Name of the release|
|date|timestamp without time zone|Date of the release|
|providers|text[]|List of processed providers|





---
### search_facet

Helper table to show filter options

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|facet_section|text|Facet section|
|facet_name|text|Facet name|
|facet_column|text|Facet column|
|facet_options|text[]|List of possible options|
|facet_example|text|Facet example|





---
### search_index

Helper table to show results in a search

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|pdcm_model_id|bigint|Internal id of the model|
|external_model_id|text|Internal of the model given by the provider|
|data_source|text|Datasource (provider abbreviation)|
|project_name|text|Project of the model|
|provider_name|text|Provider name|
|model_type|text|Type of model|
|supplier_type|text|Model supplier type - commercial, academic, other|
|catalog_number|text|Catalogue number of cell model, if commercial|
|vendor_link|text|Link to purchasable cell model, if commercial|
|rrid|text|Cellosaurus ID|
|external_ids|text|Depmap accession, Cellusaurus accession or other id. Please place in comma separated list|
|histology|text|Harmonised patient sample diagnosis|
|search_terms|text[]|All diagnosis related (by ontology relations) to the model|
|cancer_system|text|Cancer system of the model|
|dataset_available|text[]|List of datasets reported for the model (like cna, expression, publications, etc)|
|license_name|text|License name for the model|
|license_url|text|Url of the license|
|primary_site|text|Site of the primary tumor where primary cancer is originating from (may not correspond to the site of the current tissue sample)|
|collection_site|text|Site of collection of the tissue sample (can be different than the primary site if tumour type is metastatic).|
|tumour_type|text|Collected tumour type|
|cancer_grade|text|The implanted tumour grade value|
|cancer_grading_system|text|Grade classification corresponding used to describe the stage, add the version if available|
|cancer_stage|text|Stage of the patient at the time of collection|
|cancer_staging_system|text|Stage classification system used to describe the stage, add the version if available|
|patient_age|text|Patient age at collection|
|patient_age_category|text|Age category at the time of sampling|
|patient_sex|text|Patient sex|
|patient_history|text|Cancer relevant comorbidity or environmental exposure|
|patient_ethnicity|text|Patient Ethnic group. Can be derived from self-assessment or genetic analysis|
|patient_ethnicity_assessment_method|text|Patient Ethnic group assessment method|
|patient_initial_diagnosis|text|Diagnosis of the patient when first diagnosed at age_at_initial_diagnosis - this can be different than the diagnosis at the time of collection which is collected in the sample section|
|patient_age_at_initial_diagnosis|text|This is the age of first diagnostic. Can be prior to the age at which the tissue sample was collected for implant|
|patient_sample_id|text|Patient sample identifier given by the provider|
|patient_sample_collection_date|text|Date of collections. Important for understanding the time relationship between models generated for the same patient|
|patient_sample_collection_event|text|Collection event corresponding to each time a patient was sampled to generate a cancer model, subsequent collection events are incremented by 1|
|patient_sample_collection_method|text|Method of collection of the tissue sample|
|patient_sample_months_since_collection_1|text|The time difference between the 1st collection event and the current one (in months)|
|patient_sample_gene_mutation_status|text|Outcome of mutational status tests for the following genes: BRAF, PIK3CA, PTEN, KRAS|
|patient_sample_virology_status|text|Positive virology status at the time of collection. Any relevant virology information which can influence cancer like EBV, HIV, HPV status|
|patient_sample_sharable|text|Indicates if patient treatment information is available and sharable|
|patient_sample_treatment_naive_at_collection|text|Was the patient treatment naive at the time of collection? This includes the patient being treated at the time of tumour sample collection and if the patient was treated prior to the tumour sample collection.\nThe value will be 'yes' if either treated_at_collection or treated_prior_to_collection are 'yes'|
|patient_sample_treated_at_collection|text|Indicates if the patient was being treated for cancer (radiotherapy, chemotherapy, targeted therapy, hormono-therapy) at the time of collection|
|patient_sample_treated_prior_to_collection|text|Indicates if the patient was previously treated prior to the collection (radiotherapy, chemotherapy, targeted therapy, hormono-therapy)|
|patient_sample_response_to_treatment|text|Patient’s response to treatment.|
|pdx_model_publications|text|Publications that are associated to one or more models (PubMed IDs separated by commas)|
|quality_assurance|json|Quality assurance data|
|xenograft_model_specimens|json|Represents a xenografted mouse that has participated in the line creation and characterisation in some meaningful way. E.g., the specimen provided a tumor that was characterized and used as quality assurance or drug dosing data|
|model_images|json|Images associated with the model|
|markers_with_cna_data|text[]|Marker list in associate CNA data|
|markers_with_mutation_data|text[]|Marker list in associate mutation data|
|markers_with_expression_data|text[]|Marker list in associate expression data|
|markers_with_biomarker_data|text[]|Marker list in associate biomarker data|
|breast_cancer_biomarkers|text[]|List of biomarkers associated to breast cancer|
|msi_status|text[]|MSI status|
|hla_types|text[]|HLA types|
|treatment_list|text[]|Patient treatment data|
|model_treatment_list|text[]|Drug dosing data|
|custom_treatment_type_list|text[]|Treatment types + patient treatment status (Excluding "Not Provided")|
|raw_data_resources|text[]|List of resources (calculated from raw data links) the model links to|
|cancer_annotation_resources|text[]|List of resources (calculated from cancer annotation links) the model links to|
|scores|json|Model characterizations scores|
|model_dataset_type_count|integer|The number of datasets for which data exists|
|paediatric|boolean|Calculated field based on the diagnosis, patient age and project that indicates if the model is paediatric|
|model_relationships|jsonb|-|





---
### source_database

Institution public database

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|database_url|text|Link of the institution public database|


---
### xenograft_model_specimen

Xenograft Model Specimen. Represents a Xenografted mouse that has participated in the line creation and characterisation in some meaningful way.  E.g., the specimen provided a tumor that was characterized and used as quality assurance or drug dosing data

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|passage_number|text|Indicate the passage number of the sample where the PDX sample was harvested (where passage 0 corresponds to first engraftment)|
|engraftment_site_id|bigint|Reference to the engraftment_site table|
|engraftment_type_id|bigint|Reference to the engraftment_type table|
|engraftment_sample_type_id|bigint|Reference to the engraftment_sample_type type|
|engraftment_sample_state_id|bigint|Reference to the engraftment_sample_state table|
|host_strain_id|bigint|Reference to the host_strain table|
|model_id|bigint|Reference to the model_information table|





---
