**NOTE**: this document is generated, do not edit manually.

# Documentation of tables and views in schema `pdcm_admin`

## Tables
### accessibility_group

Define any limitation of access of the model per type of users like academia, industry, academia and industry, or national limitation if needed (e.g. no specific consent for sequencing)

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|europdx_access_modalities|text|If part of EUROPDX consortium fill this in. Designates a model is accessible for transnational access through the EDIReX infrastructure, or only on a collaborative basis (i.e. upon approval of the proposed project by the owner of the model)|
|accessibility|text|Limitation of access: academia, industry, academia and industry|





---
### available_molecular_data_columns

Table that shows columns with data per data source / molecular data table

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|data_source|text|Data source|
|not_empty_cols|text[]|List of columns that have data|
|molecular_characterization_type|text|Type of molecular data table|





---
### biomarker_molecular_data

Biomarker molecular data

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|biomarker|text|Gene symbol|
|biomarker_status|text|Marker status|
|essential_or_additional_marker|text|Essential or additional marker|
|non_harmonised_symbol|text|Original symbol as reported by the provider|
|harmonisation_result|text|Result of the symbol harmonisation process|
|molecular_characterization_id|bigint|Reference to the molecular_characterization_ table|
|data_source|text|Data source (abbreviation of the provider)|
|external_db_links|json|JSON column with links to external resources|





---
### cell_model

Cell model

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
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



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|model_id|model_information|id|fk_cell_model_model|


---
### cell_sample

Cell Sample

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|external_cell_sample_id|text|Unique identifier for the cell sample. Given by the provider|
|model_id|bigint|Reference to the model_information table|
|platform_id|bigint|Reference to the platform table|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|model_id|model_information|id|fk_cell_sample_model|
|platform_id|platform|id|fk_cell_sample_platform|



---
### cna_molecular_data

CNA molecular data

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
|ensembl_gene_id|text|-|
|ncbi_gene_id|text|-|
|non_harmonised_symbol|text|Original symbol as reported by the provider|
|harmonisation_result|text|Result of the symbol harmonisation process|
|molecular_characterization_id|bigint|Reference to the molecular_characterization_ table|
|data_source|text|Data source (abbreviation of the provider)|
|external_db_links|json|JSON column with links to external resources|





---
### contact_form

Contact form

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|form_url|text|Contact form link|





---
### contact_people

Contact information

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name_list|text|Contact person (should match that included in email_list column)|
|email_list|text|Contact email for any requests from users about models. If multiple, included as comma separated list|


---
### edge

Represents the connection between nodes

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|previous_node 🔑|bigint|Reference to the previous node.|
|next_node 🔑|bigint|Reference to the next node.|
|edge_label|text|Label of the relation.|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|previous_node|node|id|fk_edge_node_1|
|next_node|node|id|fk_edge_node_2|


---
### engraftment_sample_state

PDX Engraftment material state (e.g. fresh or frozen)

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|Engraftment sample state|





---
### engraftment_sample_type

Description of the type of  material grafted into the mouse. (e.g. tissue fragments, cell suspension)

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|Engraftment sample type|





---
### engraftment_site

Organ or anatomical site used for the PDX tumour engraftment (e.g. mammary fat pad, Right flank)

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|Engraftment site|





---
### engraftment_type

PDX Engraftment Type: Orthotopic if the tumour was engrafted at a corresponding anatomical site (e.g. patient tumour of primary site breast was grafted in mouse mammary fat pad). If grafted subcuteanously hererotopic is used

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|Engraftment type|





---
### ethnicity

Patient Ethnic group. Can be derived from self-assessment or genetic analysis

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|Ethnicity name|





---
### expression_molecular_data

Expression molecular data

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|hgnc_symbol|text|Gene symbol|
|z_score|numeric|Z-score representing the gene expression level|
|rnaseq_coverage|numeric|The ratio between the number of bases of the mapped reads by the number of bases of a reference|
|rnaseq_fpkm|numeric|Gene expression value represented in Fragments per kilo base of transcript per million mapped fragments (FPKM)|
|rnaseq_tpm|numeric|Gene expression value represented in transcript per million (TPM)|
|rnaseq_count|numeric|Read counts of the gene|
|affy_hgea_probe_id|text|Affymetrix probe identifier|
|affy_hgea_expression_value|numeric|Expresion value captured using Affymetrix arrays|
|illumina_hgea_probe_id|text|Illumina probe identifier|
|illumina_hgea_expression_value|numeric|Expresion value captured using Illumina arrays|
|ensembl_gene_id|text|-|
|ncbi_gene_id|text|-|
|non_harmonised_symbol|text|Original symbol as reported by the provider|
|harmonisation_result|text|Result of the symbol harmonisation process|
|molecular_characterization_id|bigint|Reference to the molecular_characterization_ table|
|data_source|text|Data source (abbreviation of the provider)|
|external_db_links|json|JSON column with links to external resources|





---
### gene_marker

A marker represents a specific location on the _human_ genome that usually corresponds to a gene. The genes are validated based on this https://www.genenames.org/. Table used to harmosisation of gene symbols

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|hgnc_id|text|Internal identifier|
|approved_symbol|text|The official gene symbol approved by the HGNC, which is typically a short form of the gene name.|
|approved_name|text|The full gene name approved by the HGNC|
|previous_symbols|text|This field displays any names that were previously HGNC-approved nomenclature|
|alias_symbols|text|Alternative symbols that have been used to refer to the gene. Aliases may be from literature, from other databases or may be added to represent membership of a gene group.|
|accession_numbers|text|Accession numbers|
|refseq_ids|text|Internal identifier|
|alias_names|text|Alias names|
|ensembl_gene_id|text|Ensembl gene id|
|ncbi_gene_id|text|NCBI gene id|





---
### host_strain

Host strain information

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|Host mouse strain name (e.g. NOD-SCID, NSG, etc)|
|nomenclature|text|The full nomenclature form of the host mouse strain name|





---
### image_study

Information about an image study

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|study_id|text|Accession number for BioImage Archive study|
|title|text|The title for your dataset|
|description|text|Field to describe your dataset. This can be the abstract to an accompanying publication|
|licence|text|The licence under which the data are available|
|contact|text|Contact details for the authors involved in the study|
|sample_organism|text|What is being imaged|
|sample_description|text|High level description of sample|
|sample_preparation_protocol|text|How the sample was prepared for imaging|
|imaging_instrument|text|Description of the instrument used to capture the images|
|image_acquisition_parameters|text|How the images were acquired, including instrument settings/parameters|
|imaging_method|text|What method was used to capture images|





---
### immunemarker_molecular_data

Immunemarker molecular data

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|marker_type|text|Marker type|
|marker_name|text|Name of the immune marker|
|marker_value|text|Value or measurement associated with the immune marker|
|essential_or_additional_details|text|Additional details or notes about the immune marker|
|molecular_characterization_id|bigint|Reference to the molecular_characterization_ table|
|data_source|text|Data source (abbreviation of the provider)|





---
### license

License of a model

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|License name|
|url|text|Url of the license definition|






---
### model_image

Information about an image study

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|model_id|bigint|Unique identifier for all the PDXs derived from the same tissue sample|
|url|text|Link to the model image|
|description|text|Description of the image on what was being imaged|
|sample_type|text|Type of sample being imaged (pdx, patient, organoid, cell line)|
|passage|text|Passage number imaging was performed. Passage 0 correspond to first engraftment|
|magnification|text|Magnification of the mode image|
|staining|text|Staining used for imaging the sample|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|model_id|model_information|id|fk_model_image_model|


---
### model_information

Model creation information

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|external_model_id|text|Unique identifier of the model. Given by the provider|
|type|text|Model type|
|data_source|character varying|Abbreviation of the provider. Added explicitly here to help with queries|
|publication_group_id|bigint|Reference to the publication_group table. Corresponds to the publications the model is part of|
|accessibility_group_id|bigint|Reference to the accessibility_group table|
|contact_people_id|bigint|Reference to the contact_people table|
|contact_form_id|bigint|Reference to the contact_form table|
|source_database_id|bigint|Reference to the source_database table|
|license_id|bigint|Reference to the license table|
|external_ids|text|Depmap accession, Cellusaurus accession or other id. Please place in comma separated list|
|supplier|text|Supplier brief acronym or name followed by a colon and the number or name use to reference the model|
|supplier_type|text|Model supplier type - commercial, academic, other|
|catalog_number|text|Catalogue number of cell model, if commercial|
|vendor_link|text|Link to purchasable cell model, if commercial|
|rrid|text|Cellosaurus ID|
|parent_id|text|model Id of the model used to generate the model|
|origin_patient_sample_id|text|Unique ID of the patient tumour sample used to generate the model|
|model_availability|text|Model availability status, i.e. if the model is still available to purchase.|
|date_submitted|text|Date of submission to the resource|
|other_model_links|json|External ids links and supplier link|
|model_relationships|json|Model relationships|
|has_relations|boolean|Indicates if the model has parent(s) or children|
|knowledge_graph|json|Knowledge graph|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|publication_group_id|publication_group|id|fk_model_publication_group|
|accessibility_group_id|accessibility_group|id|fk_model_accessibility_group|
|contact_people_id|contact_people|id|fk_model_contact_people|
|contact_form_id|contact_form|id|fk_model_contact_form|
|source_database_id|source_database|id|fk_model_source_database|
|license_id|license|id|fk_model_license|


---
### molecular_characterization

Molecular Characterization. Represents the results of applying a specified technology to the biological sample in order to gain insight to the tumor's attributes

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|molecular_characterization_type_id|bigint|Reference to the molecular_characterization_type table|
|platform_id|bigint|Reference to the platform table|
|raw_data_url|text|Identifiers used to build links to external resources with raw data|
|patient_sample_id|bigint|Reference to patient_sample table if the sample is a patient sample|
|xenograft_sample_id|bigint|Reference to xenograft_sample table if the sample is a xenograft sample|
|cell_sample_id|bigint|Reference to cell_sample table if the sample is a cell sample|
|external_db_links|json|JSON column with links to external resources|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|molecular_characterization_type_id|molecular_characterization_type|id|fk_molecular_characterization_mchar_type|
|platform_id|platform|id|fk_molecular_characterization_platform|
|patient_sample_id|patient_sample|id|fk_molecular_characterization_patient_sample|
|xenograft_sample_id|xenograft_sample|id|fk_molecular_characterization_xenograft_sample|
|cell_sample_id|cell_sample|id|fk_molecular_characterization_cell_sample|


---
### molecular_characterization_type

Molecular Characterization Type

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|Molecular characterization type name|





---
### molecular_data_restriction

Internal table to store molecular tables which data cannot be displayed to the user. Configured at provider level.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|data_source|text|Provider with the restriction|
|molecular_data_table|text|Table whose data cannot be showed|





---
### mutation_marker



#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|-|
|biotype|text|-|
|coding_sequence_change|text|-|
|variant_class|text|-|
|codon_change|text|-|
|amino_acid_change|text|-|
|consequence|text|-|
|functional_prediction|text|-|
|seq_start_position|text|-|
|ref_allele|text|-|
|alt_allele|text|-|
|ncbi_transcript_id|text|-|
|ensembl_transcript_id|text|-|
|variation_id|text|-|
|gene_marker_id|bigint|-|
|non_harmonised_symbol|text|-|
|harmonisation_result|text|-|





---
### mutation_measurement_data

Mutation measurement data

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id|bigint|Internal identifier|
|hgnc_symbol|text|Gene symbol|
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
|coding_sequence_change|text|Change in the DNA Sequence|
|variant_class|text|Variation classification eg: SNV, intronic change etc|
|codon_change|text|Change in nucleotides|
|functional_prediction|text|Functional prediction|
|ncbi_transcript_id|text|NCBI transcript id|
|ensembl_transcript_id|text|Ensembl transcript id|
|variation_id|text|Gene symbol|
|ensembl_gene_id|text|-|
|ncbi_gene_id|text|-|
|molecular_characterization_id|bigint|Reference to the molecular_characterization_ table|
|non_harmonised_symbol|text|Original symbol as reported by the provider|
|harmonisation_result|text|Result of the symbol harmonisation process|
|data_source|text|Data source (abbreviation of the provider)|
|external_db_links|json|JSON column with links to external resources|





---
### node

Represents a node in a graph

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|node_type|text|The type of data we are representing: patient sample, model, treatment, etc.|
|node_label|text|Text to display for the node. It will be the external_model_id in the case of models, or external sample id in the case of samples, for example.|
|data_source|text|Data source associated to the node|
|data|json|A JSON column with arbitrary key-value information about the current entity/node|





---
### ontology_term_diagnosis

Ontology terms for diagnosis

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|term_id|text|NCIT ontology term id|
|term_name|text|NCIT ontology term name|
|term_url|text|NCIT ontology term url|
|is_a|text|List of NCIT ontology term ids the term is classified as "is a"|
|ancestors|text|List of NCIT ontology term ids that are ancestors of the term|





---
### ontology_term_regimen

Ontology terms for regimens

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|term_id|text|NCIT ontology term id|
|term_name|text|NCIT ontology term name|
|is_a|text|List of NCIT ontology term ids the term is classified as "is a"|





---
### ontology_term_treatment

Ontology terms for treatment

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|term_id|text|NCIT ontology term id|
|term_name|text|NCIT ontology term name|
|is_a|text|List of NCIT ontology term ids the term is classified as "is a"|





---
### patient

Information about a patient

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|external_patient_id|text|Unique anonymous/de-identified ID for the patient from the provider|
|sex|text|Sex of the patient|
|history|text|Cancer relevant comorbidity or environmental exposure|
|ethnicity_id|bigint|Reference to the ethnicity|
|ethnicity_assessment_method|text|Patient Ethnic group assessment method|
|initial_diagnosis|text|Diagnosis of the patient when first diagnosed at age_at_initial_diagnosis - this can be different than the diagnosis at the time of collection which is collected in the sample section|
|age_at_initial_diagnosis|text|This is the age of first diagnostic. Can be prior to the age at which the tissue sample was collected for implant|
|provider_group_id|bigint|Reference to the provider group the patient is related to|
|age_category|text|Age category at time of sampling|
|smoking_status|text|Patient smoking history|
|alcohol_status|text|Alcohol intake of the patient, self-reported|
|alcohol_frequency|text|The average number of days per week on which the patient consumes alcohol, self-reported|
|family_history_of_cancer|text|If a first-degree relative of the patient has been diagnosed with a cancer of the same or different type|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|provider_group_id|provider_group|id|fk_patient_provider_group|
|ethnicity_id|ethnicity|id|fk_patient_ethnicity|


---
### patient_sample

Tumour type

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|external_patient_sample_id|text|Unique ID of the patient tumour sample used to generate cancer models|
|patient_id|bigint|Reference to the patient table|
|diagnosis|text|Diagnosis at time of collection of the patient tumour used in the cancer model|
|grade|text|The implanted tumour grade value|
|grading_system|text|Grade classification corresponding used to describe the stage, add the version if available|
|stage|text|Stage of the patient at the time of collection|
|staging_system|text|Stage classification system used to describe the stage, add the version if available|
|primary_site_id|bigint|Reference to the tissue table. Site of the primary tumor where primary cancer is originating from (may not correspond to the site of the current tissue sample)|
|collection_site_id|bigint|Reference to the tissue table. Site of collection of the tissue sample (can be different than the primary site if tumour type is metastatic)|
|prior_treatment|text|Was the patient treated for cancer prior to the time of tumour sample collection. This includes any of the following: radiotherapy, chemotherapy, targeted therapy, homorno-therapy|
|tumour_type_id|bigint|Reference to the tumour_type table. Collected tumour type|
|age_in_years_at_collection|text|Patient age at collection|
|collection_event|text|Collection event corresponding to each time a patient was sampled to generate a cancer model, subsequent collection events are incremented by 1|
|collection_date|text|Date of collections. Important for understanding the time relationship between models generated for the same patient|
|collection_method|text|Method of collection of the tissue sample|
|months_since_collection_1|text|The time difference between the 1st collection event and the current one (in months)|
|gene_mutation_status|text|Outcome of mutational status tests for the following genes: BRAF, PIK3CA, PTEN, KRAS|
|treatment_naive_at_collection|text|Was the patient treatment naive at the time of collection? This includes the patient being treated at the time of tumour sample collection and if the patient was treated prior to the tumour sample collection.\nThe value will be 'yes' if either treated_at_collection or treated_prior_to_collection are 'yes'|
|treated_at_collection|text|Was the patient being treated for cancer at the time of tumour sample collection.|
|treated_prior_to_collection|text|Was the patient treated prior to the tumour sample collection.|
|response_to_treatment|text|Patient’s response to treatment.|
|virology_status|text|Positive virology status at the time of collection. Any relevant virology information which can influence cancer like EBV, HIV, HPV status|
|sharable|text|-|
|model_id|bigint|Reference to the model_information table|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|primary_site_id|tissue|id|fk_patient_primary_site|
|collection_site_id|tissue|id|fk_patient_collection_site|
|tumour_type_id|tumour_type|id|fk_patient_sample_tumour_type|
|model_id|model_information|id|fk_patient_sample_model|


---
### platform

Technical platform used to generate molecular characterisation.  E.g., targeted next generation sequencing

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|library_strategy|text|The library strategy species the sequencing technique intended fo this library. Examples: WGS,WGA,WXS, RNA-Seq, Amplicon, Targeted-capture|
|provider_group_id|bigint|Reference to the provider_group table|
|instrument_model|text|The name of the platform technology used to produce the molecular characterisation,  This could be the sequencer or the name of the microarray|
|library_selection|text|The library selection specifies whether any method was used to select for or against, enrich, or screen the material being sequenced. (PCR, cDNA, Hybrid Selection, PolyA, RANDOM)|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|provider_group_id|provider_group|id|fk_platform_provider_group|


---
### project_group

Project. A grouper element for providers

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|Name of the project|





---
### provider_group

Information of data providers

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|Provider name|
|abbreviation|text|Provider abbreviation|
|description|text|A description of the provider|
|provider_type_id|bigint|Reference to the provider type|
|project_group_id|bigint|Reference to the project the provider belongs to|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|provider_type_id|provider_type|id|fk_provider_group_provider_type|
|project_group_id|project_group|id|fk_provider_group_project_group|


---
### provider_type

Provider type. Example: Academic, Industry

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|Provider type name|





---
### publication_group

Groups of publications that are associated to one or more models

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|pubmed_ids|text|pubmed IDs separated by commas|





---
### quality_assurance

Cell Sample

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|description|text|Short description of what was compared and what was the result: (e.g. high, good, moderate concordance between xenograft, 'model validated against histological features of same diagnosis' or 'not determined') - It needs to be clear if the model is validated or not.|
|passages_tested|text|Provide a list of all passages where validation was performed. Passage 0 correspond to first engraftment (if this is not the case please define how passages are numbered)|
|validation_technique|text|Any technique used to validate PDX against their original patient tumour, including fingerprinting, histology, immunohistochemistry|
|validation_host_strain_nomenclature|text|Validation host mouse strain, following mouse strain nomenclature from MGI JAX|
|model_id|bigint|Reference to the model_information table|
|morphological_features|text|Morphological features of the model|
|snp_analysis|text|Was SNP analysis done on the model?|
|str_analysis|text|Was STR analysis done on the model?|
|tumour_status|text|Gene expression validation of established model|
|model_purity|text|Presence of tumour vs stroma or normal cells|
|comments|text|Comments about the model that cannot be expressed by other fields|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|model_id|model_information|id|fk_quality_assurance_model|


---
### regimen_to_ontology

Mapping between treatments and ontology terms

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|regimen_id|bigint|Reference to the treatment table (regimens)|
|ontology_term_id|bigint|Reference to the ontology_term_regimen table|





---
### regimen_to_treatment

Relation between regimen and treatments

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|regimen_ontology_term_id|bigint|Reference to the regimen_ontology_term table|
|treatment_ontology_term_id|bigint|Reference to the treatment_ontology_term table|





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
### response

Response to a treatment

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|Name of the response|





---
### response_classification

Response classification

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|Classification used to define response to treatment|





---
### sample_to_ontology

Mapping between diagnosis in from a sample and ontology term

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|sample_id|bigint|Reference to the patient_sample table|
|ontology_term_id|bigint|Reference to the ontology_term_diagnosis table|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|sample_id|patient_sample|id|fk_sample_to_ontology_patient_sample|
|ontology_term_id|ontology_term_diagnosis|id|fk_sample_to_ontology_ontology_term_diagnosis|


---
### search_facet

Helper table to show filter options

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|index|integer|-|
|facet_section|text|Facet section|
|facet_name|text|Facet name|
|facet_description|text|-|
|facet_column|text|Facet column|
|facet_options|text[]|List of possible options|
|facet_example|text|Facet example|
|any_operator|text|Operator to be used when the search involves several options and the search uses ANY|
|all_operator|text|Operator to be used when the search involves several options and the search uses ALL|
|is_boolean|boolean|Indicates if the filter is to be used on a boolean field|
|facet_type|text|Indicates how to create the element in the UI: check, autocomplete, or multivalued|





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
|supplier|text|Supplier brief acronym or name followed by a colon and the number or name use to reference the model|
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
|patient_id|text|Patient id given by the provider|
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
|patient_sample_treatment_naive_at_collection|text|-|
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
|patient_treatments|text[]|Patient treatments|
|patient_treatments_responses|text[]|List of responses for the patient treatments|
|model_treatments|text[]|Drug dosing data|
|model_treatments_responses|text[]|List of responses for the model treatments|
|custom_treatment_type_list|text[]|Treatment types + patient treatment status (Excluding "Not Provided")|
|raw_data_resources|text[]|List of resources (calculated from raw data links) the model links to|
|cancer_annotation_resources|text[]|List of resources (calculated from cancer annotation links) the model links to|
|model_availability|text|Model availability status, i.e. if the model is still available to purchase.|
|date_submitted|text|Date of submission to the resource|
|scores|json|Model characterizations scores|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|pdcm_model_id|model_information|id|fk_search_index_model|




---
### source_database

Institution public database

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|database_url|text|Link of the institution public database|





---
### tissue

Tissue

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|A human tissue from which a sample was collected|





---
### treatment

Treatment name

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|Treatment name. If `term_id` is not null, `name` corresponds to the ontology term found in the mapping process|
|term_id|text|Id of the ontology term for this treatment|
|types|text[]|List of treatment types associated to the treatment|
|external_db_links|json|JSON column with links to external resources|
|aliases|text[]|List of names for the treatment as we got them from the providers|





---
### treatment_component

The specifics of drug(s) and timing of administering the drugs

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|dose|text|Dose used in the treatment|
|treatment_protocol_id|bigint|Reference to the treatment_protocol table|
|treatment_id|bigint|Reference to the treatment table|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|treatment_protocol_id|treatment_protocol|id|fk_treatment_component_treatment_protocol|
|treatment_id|treatment|id|fk_treatment_component_treatment|


---
### treatment_protocol

The specifics of drug(s) and timing of administering the drugs

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|model_id|bigint|Reference to the model_information_table|
|patient_id|bigint|Reference to the patient table|
|treatment_target|text|Patient or model|
|response_id|bigint|Reference to the response table|
|response_classification_id|bigint|Reference to the response_classification table|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|model_id|model_information|id|fk_treatment_protocol_model|
|patient_id|patient|id|fk_treatment_protocol_patient|
|response_id|response|id|fk_treatment_protocol_response|
|response_classification_id|response_classification|id|fk_treatment_protocol_response_classification|


---
### treatment_to_ontology

Mapping between treatments and ontology terms

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|treatment_id|bigint|Reference to the treatment table|
|ontology_term_id|bigint|Reference to the ontology_term_treatment table|





---
### tumour_type

Tumour type

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|name|text|Collected tumour type|





---
### xenograft_model_specimen

Xenograft Model Specimen. Represents a Xenografted mouse that has participated in the line creation and characterisation in some meaningful way.  E.g., the specimen provided a tumor that was characterized and used as quality assurance or drug dosing data

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|passage_number|text|Indicate the passage number of the sample where the PDX sample was harvested (where passage 0 corresponds to first engraftment)|
|engraftment_site_id|bigint|Reference to the engraftment_site table|
|engraftment_type_id|bigint|Reference to the engraftment_type table|
|engraftment_sample_type_id|bigint|Reference to the engraftment_sample_type type|
|engraftment_sample_state_id|bigint|Reference to the engraftment_sample_state table|
|host_strain_id|bigint|Reference to the host_strain table|
|model_id|bigint|Reference to the model_information table|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|engraftment_site_id|engraftment_site|id|fk_specimen_engraftment_site|
|engraftment_type_id|engraftment_type|id|fk_specimen_engraftment_type|
|engraftment_sample_type_id|engraftment_sample_type|id|fk_specimen_engraftment_sample_type|
|model_id|model_information|id|fk_specimen_model|
|engraftment_sample_state_id|engraftment_sample_state|id|fk_specimen_engraftment_sample_state|
|host_strain_id|host_strain|id|fk_specimen_host_strain|


---
### xenograft_sample

Tumour type

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|id 🔑|bigint|Internal identifier|
|external_xenograft_sample_id|text|Unique identifier for the xenograft sample. Given by the provider. Identifier of the sample from any patient tissue or PDX derived sample from which OMIC data was generated|
|passage|character varying|Indicates the passage number of the sample where the PDX sample was harvested (where passage 0 corresponds to first engraftment)|
|host_strain_id|bigint|Reference to the host_strain table. Host mouse strain name (e.g. NOD-SCID, NSG, etc)- When different mouse strain was used during the PDX line generation|
|model_id|bigint|Reference to the model_information table|
|platform_id|bigint|Internal identifier|



#### Relations
|Column Name|Foreign Table|Foreign Table Primary Key|Foreign Key Name|
|-----|-----|-----|-----|
|model_id|model_information|id|fk_xenograft_sample_model|
|host_strain_id|host_strain|id|fk_xenograft_sample_host_strain|
|platform_id|platform|id|fk_xenograft_sample_platform|


---


## Views
### molecular_characterization_vw

Molecular characterization

  Molecular characterization with model external ids.

#### Columns
|Column Name|Data Type|Comment|
|-----|-----|-----|
|model_id|text|Full name of the model used by provider|
|data_source|character varying|Data source of the model (provider abbreviation)|
|source|text|Sample origin (patient, xenograft, cell, unknown)|
|sample_id|text|Sample external id|
|xenograft_passage|character varying|Unique identifier for the xenograft sample. Given by the provider|
|raw_data_url|text|Identifiers used to build links to external resources with raw data|
|data_type|text|Molecular data type|
|platform_name|text|Platform instrument_model|
|molecular_characterization_id|bigint|Reference to the molecular_characterization_type table|
|external_db_links|json|JSON column with links to external resources|

