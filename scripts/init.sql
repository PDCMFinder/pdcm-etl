CREATE SCHEMA IF NOT EXISTS pdcm_api;

COMMENT ON SCHEMA pdcm_api IS
  'Patient-derived cancer models (PDCMs) are a powerful oncology research platform for studying tumour biology, mechanisms of drug response and resistance and for testing personalised medicine. Distributed nature of repositories for PDCMs (xenografts, organoids and cell lines) and the use of different metadata standards for describing model''s characteristics make it difficult for researchers to identify suitable PDCM models relevant to specific cancer research questions. PDCM Finder aims to solve this problem by providing harmonized and integrated model attributes to support consistent searching across the originating resources';

DROP TABLE IF EXISTS ethnicity CASCADE;

CREATE TABLE ethnicity (
    id BIGINT NOT NULL,
    name TEXT
);

COMMENT ON TABLE ethnicity IS 'Patient Ethnic group. Can be derived from self-assessment or genetic analysis';
COMMENT ON COLUMN ethnicity.id IS 'Internal identifier';
COMMENT ON COLUMN ethnicity.name IS 'Ethnicity name';

DROP TABLE IF EXISTS provider_type CASCADE;

CREATE TABLE provider_type (
    id BIGINT NOT NULL,
    name TEXT
);

COMMENT ON TABLE provider_type IS 'Provider type. Example: Academic, Industry';
COMMENT ON COLUMN provider_type.id IS 'Internal identifier';
COMMENT ON COLUMN provider_type.name IS 'Provider type name';

DROP TABLE IF EXISTS provider_group CASCADE;

CREATE TABLE provider_group (
    id BIGINT NOT NULL,
    name TEXT,
    abbreviation TEXT,
    description TEXT,
    provider_type_id BIGINT,
    project_group_id BIGINT
);

COMMENT ON TABLE provider_group IS 'Information of data providers';
COMMENT ON COLUMN provider_group.id IS 'Internal identifier';
COMMENT ON COLUMN provider_group.name IS 'Provider name';
COMMENT ON COLUMN provider_group.abbreviation IS 'Provider abbreviation';
COMMENT ON COLUMN provider_group.description IS 'A description of the provider';
COMMENT ON COLUMN provider_group.provider_type_id IS 'Reference to the provider type';
COMMENT ON COLUMN provider_group.project_group_id IS 'Reference to the project the provider belongs to';

DROP TABLE IF EXISTS patient CASCADE;

CREATE TABLE patient (
    id BIGINT NOT NULL,
    external_patient_id TEXT NOT NULL,
    sex TEXT NOT NULL,
    history TEXT,
    ethnicity_id BIGINT,
    ethnicity_assessment_method TEXT,
    initial_diagnosis TEXT,
    age_at_initial_diagnosis TEXT,
    provider_group_id BIGINT,
    age_category TEXT,
    smoking_status TEXT,
    alcohol_status TEXT,
    alcohol_frequency TEXT,
    family_history_of_cancer TEXT
);

COMMENT ON TABLE patient IS 'Information about a patient';
COMMENT ON COLUMN patient.id IS 'Internal identifier';
COMMENT ON COLUMN patient.external_patient_id IS 'Unique anonymous/de-identified ID for the patient from the provider';
COMMENT ON COLUMN patient.sex IS 'Sex of the patient';
COMMENT ON COLUMN patient.history IS 'Cancer relevant comorbidity or environmental exposure';
COMMENT ON COLUMN patient.ethnicity_id IS 'Reference to the ethnicity';
COMMENT ON COLUMN patient.ethnicity_assessment_method IS 'Patient Ethnic group assessment method';
COMMENT ON COLUMN patient.initial_diagnosis IS 'Diagnosis of the patient when first diagnosed at age_at_initial_diagnosis - this can be different than the diagnosis at the time of collection which is collected in the sample section';
COMMENT ON COLUMN patient.age_at_initial_diagnosis IS 'This is the age of first diagnostic. Can be prior to the age at which the tissue sample was collected for implant';
COMMENT ON COLUMN patient.provider_group_id IS 'Reference to the provider group the patient is related to';
COMMENT ON COLUMN patient.age_category IS 'Age category at time of sampling';
COMMENT ON COLUMN patient.smoking_status IS 'Patient smoking history';
COMMENT ON COLUMN patient.alcohol_status IS 'Alcohol intake of the patient, self-reported';
COMMENT ON COLUMN patient.alcohol_frequency IS 'The average number of days per week on which the patient consumes alcohol, self-reported';
COMMENT ON COLUMN patient.family_history_of_cancer IS 'If a first-degree relative of the patient has been diagnosed with a cancer of the same or different type';


DROP TABLE IF EXISTS publication_group CASCADE;

CREATE TABLE publication_group (
    id BIGINT NOT NULL,
    pubmed_ids TEXT NOT NULL
);

COMMENT ON TABLE publication_group IS 'Groups of publications that are associated to one or more models';
COMMENT ON COLUMN publication_group.id IS 'Internal identifier';
COMMENT ON COLUMN publication_group.pubmed_ids IS 'pubmed IDs separated by commas';

DROP TABLE IF EXISTS accessibility_group CASCADE;

CREATE TABLE accessibility_group (
    id BIGINT NOT NULL,
    europdx_access_modalities TEXT,
    accessibility TEXT
);

COMMENT ON TABLE accessibility_group IS 'Define any limitation of access of the model per type of users like academia, industry, academia and industry, or national limitation if needed (e.g. no specific consent for sequencing)';
COMMENT ON COLUMN accessibility_group.id IS 'Internal identifier';
COMMENT ON COLUMN accessibility_group.europdx_access_modalities IS 'If part of EUROPDX consortium fill this in. Designates a model is accessible for transnational access through the EDIReX infrastructure, or only on a collaborative basis (i.e. upon approval of the proposed project by the owner of the model)';
COMMENT ON COLUMN accessibility_group.accessibility IS 'Limitation of access: academia, industry, academia and industry';

DROP TABLE IF EXISTS contact_people CASCADE;

CREATE TABLE contact_people (
    id BIGINT NOT NULL,
    name_list TEXT,
    email_list TEXT
);

COMMENT ON TABLE contact_people IS 'Contact information';
COMMENT ON COLUMN contact_people.id IS 'Internal identifier';
COMMENT ON COLUMN contact_people.name_list IS 'Contact person (should match that included in email_list column)';
COMMENT ON COLUMN contact_people.email_list IS 'Contact email for any requests from users about models. If multiple, included as comma separated list';

DROP TABLE IF EXISTS contact_form CASCADE;

CREATE TABLE contact_form (
    id BIGINT NOT NULL,
    form_url TEXT NOT NULL
);

COMMENT ON TABLE contact_form IS 'Contact form';
COMMENT ON COLUMN contact_form.id IS 'Internal identifier';
COMMENT ON COLUMN contact_form.form_url IS 'Contact form link';

DROP TABLE IF EXISTS source_database CASCADE;

CREATE TABLE source_database (
    id BIGINT NOT NULL,
    database_url TEXT NOT NULL
);

COMMENT ON TABLE source_database IS 'Institution public database';
COMMENT ON COLUMN source_database.id IS 'Internal identifier';
COMMENT ON COLUMN source_database.database_url IS 'Link of the institution public database';

DROP TABLE IF EXISTS model_information CASCADE;

CREATE TABLE model_information (
    id BIGINT NOT NULL,
    external_model_id TEXT,
    type TEXT,
    data_source varchar,
    publication_group_id BIGINT,
    accessibility_group_id BIGINT,
    contact_people_id BIGINT,
    contact_form_id BIGINT,
    source_database_id BIGINT,
    license_id BIGINT,
    external_ids TEXT,
    supplier TEXT,
    supplier_type TEXT,
    catalog_number TEXT,
    vendor_link TEXT,
    rrid TEXT,
    parent_id TEXT,
    origin_patient_sample_id TEXT
);

COMMENT ON TABLE model_information IS 'Model creation information';
COMMENT ON COLUMN model_information.id IS 'Internal identifier';
COMMENT ON COLUMN model_information.external_model_id IS 'Unique identifier of the model. Given by the provider';
COMMENT ON COLUMN model_information.type IS 'Model type';
COMMENT ON COLUMN model_information.data_source IS 'Abbreviation of the provider. Added explicitly here to help with queries';
COMMENT ON COLUMN model_information.publication_group_id IS 'Reference to the publication_group table. Corresponds to the publications the model is part of';
COMMENT ON COLUMN model_information.accessibility_group_id IS 'Reference to the accessibility_group table';
COMMENT ON COLUMN model_information.contact_people_id IS 'Reference to the contact_people table';
COMMENT ON COLUMN model_information.contact_form_id IS 'Reference to the contact_form table';
COMMENT ON COLUMN model_information.source_database_id IS 'Reference to the source_database table';
COMMENT ON COLUMN model_information.license_id IS 'Reference to the license table';
COMMENT ON COLUMN model_information.external_ids IS 'Depmap accession, Cellusaurus accession or other id. Please place in comma separated list';
COMMENT ON COLUMN model_information.supplier IS 'Supplier brief acronym or name followed by a colon and the number or name use to reference the model';
COMMENT ON COLUMN model_information.supplier_type IS 'Model supplier type - commercial, academic, other';
COMMENT ON COLUMN model_information.catalog_number IS 'Catalogue number of cell model, if commercial';
COMMENT ON COLUMN model_information.vendor_link IS 'Link to purchasable cell model, if commercial';
COMMENT ON COLUMN model_information.rrid IS 'Cellosaurus ID';
COMMENT ON COLUMN model_information.parent_id IS 'model Id of the model used to generate the model';
COMMENT ON COLUMN model_information.origin_patient_sample_id IS 'Unique ID of the patient tumour sample used to generate the model';

DROP TABLE IF EXISTS license CASCADE;

CREATE TABLE license (
    id BIGINT NOT NULL,
    name TEXT,
    url TEXT
);

COMMENT ON TABLE license IS 'License of a model';
COMMENT ON COLUMN license.id IS 'Internal identifier';
COMMENT ON COLUMN license.name IS 'License name';
COMMENT ON COLUMN license.url IS 'Url of the license definition';

DROP TABLE IF EXISTS cell_model CASCADE;

CREATE TABLE cell_model (
    id BIGINT NOT NULL,
    model_name TEXT,
    model_name_aliases TEXT,
    type TEXT,
    growth_properties TEXT,
    growth_media TEXT,
    media_id TEXT,
    parent_id TEXT,
    origin_patient_sample_id TEXT,
    model_id BIGINT,
    plate_coating TEXT,
    other_plate_coating TEXT,
    passage_number TEXT,
    contaminated TEXT,
    contamination_details TEXT,
    supplements TEXT,
    drug TEXT,
    drug_concentration TEXT
);

COMMENT ON TABLE cell_model IS 'Cell model';
COMMENT ON COLUMN cell_model.id IS 'Internal identifier';
COMMENT ON COLUMN cell_model.model_name IS 'Most common name associate with model. Please use the CCLE name if available';
COMMENT ON COLUMN cell_model.model_name_aliases IS 'model_name_aliases';
COMMENT ON COLUMN cell_model.type IS 'Type of organoid or cell model';
COMMENT ON COLUMN cell_model.growth_properties IS 'Observed growth properties of the related model';
COMMENT ON COLUMN cell_model.growth_media IS 'Base media formulation the model was grown in';
COMMENT ON COLUMN cell_model.media_id IS 'Unique identifier for each media formulation (Catalogue number)';
COMMENT ON COLUMN cell_model.parent_id IS 'model Id of the model used to generate the model';
COMMENT ON COLUMN cell_model.origin_patient_sample_id IS 'Unique ID of the patient tumour sample used to generate the model';
COMMENT ON COLUMN cell_model.model_id IS 'Reference to model_information_table';
COMMENT ON COLUMN cell_model.plate_coating IS 'Coating on plate model was grown in';
COMMENT ON COLUMN cell_model.other_plate_coating IS 'Other coating on plate model was grown in (not mentioned above)';
COMMENT ON COLUMN cell_model.passage_number IS 'Passage number at time of sequencing/screening';
COMMENT ON COLUMN cell_model.contaminated IS 'Is there contamination present in the model';
COMMENT ON COLUMN cell_model.contamination_details IS 'What are the details of the contamination';
COMMENT ON COLUMN cell_model.supplements IS 'Additional supplements the model was grown with';
COMMENT ON COLUMN cell_model.drug IS 'Additional drug/compounds the model was grown with';
COMMENT ON COLUMN cell_model.drug_concentration IS 'Concentration of Additional drug/compounds the model was grown with';

DROP TABLE IF EXISTS cell_sample CASCADE;

CREATE TABLE cell_sample (
    id BIGINT NOT NULL,
    external_cell_sample_id TEXT,
    model_id BIGINT,
    platform_id BIGINT
);

COMMENT ON TABLE cell_sample IS 'Cell Sample';
COMMENT ON COLUMN cell_sample.id IS 'Internal identifier';
COMMENT ON COLUMN cell_sample.external_cell_sample_id IS 'Unique identifier for the cell sample. Given by the provider';
COMMENT ON COLUMN cell_sample.model_id IS 'Reference to the model_information table';
COMMENT ON COLUMN cell_sample.platform_id IS 'Reference to the platform table';

DROP TABLE IF EXISTS quality_assurance CASCADE;

CREATE TABLE quality_assurance (
    id BIGINT NOT NULL,
    description TEXT,
    passages_tested TEXT,
    validation_technique TEXT,
    validation_host_strain_nomenclature TEXT,
    model_id BIGINT NOT NULL,
    morphological_features TEXT,
    SNP_analysis TEXT,
    STR_analysis TEXT,
    tumour_status TEXT,
    model_purity TEXT,
    comments TEXT
);

COMMENT ON TABLE quality_assurance IS 'Cell Sample';
COMMENT ON COLUMN quality_assurance.id IS 'Internal identifier';
COMMENT ON COLUMN quality_assurance.description IS 'Short description of what was compared and what was the result: (e.g. high, good, moderate concordance between xenograft, ''model validated against histological features of same diagnosis'' or ''not determined'') - It needs to be clear if the model is validated or not.';
COMMENT ON COLUMN quality_assurance.passages_tested IS 'Provide a list of all passages where validation was performed. Passage 0 correspond to first engraftment (if this is not the case please define how passages are numbered)';
COMMENT ON COLUMN quality_assurance.validation_technique IS 'Any technique used to validate PDX against their original patient tumour, including fingerprinting, histology, immunohistochemistry';
COMMENT ON COLUMN quality_assurance.validation_host_strain_nomenclature IS 'Validation host mouse strain, following mouse strain nomenclature from MGI JAX';
COMMENT ON COLUMN quality_assurance.model_id IS 'Reference to the model_information table';
COMMENT ON COLUMN quality_assurance.morphological_features IS 'Morphological features of the model';
COMMENT ON COLUMN quality_assurance.SNP_analysis IS 'Was SNP analysis done on the model?';
COMMENT ON COLUMN quality_assurance.STR_analysis IS 'Was STR analysis done on the model?';
COMMENT ON COLUMN quality_assurance.tumour_status IS 'Gene expression validation of established model';
COMMENT ON COLUMN quality_assurance.model_purity IS 'Presence of tumour vs stroma or normal cells';
COMMENT ON COLUMN quality_assurance.comments IS 'Comments about the model that cannot be expressed by other fields';

DROP TABLE IF EXISTS tissue CASCADE;

CREATE TABLE tissue (
    id BIGINT NOT NULL,
    name TEXT
);

COMMENT ON TABLE tissue IS 'Tissue';
COMMENT ON COLUMN tissue.id IS 'Internal identifier';
COMMENT ON COLUMN tissue.name IS 'A human tissue from which a sample was collected';

DROP TABLE IF EXISTS tumour_type CASCADE;

CREATE TABLE tumour_type (
    id BIGINT NOT NULL,
    name TEXT
);

COMMENT ON TABLE tumour_type IS 'Tumour type';
COMMENT ON COLUMN tumour_type.id IS 'Internal identifier';
COMMENT ON COLUMN tumour_type.name IS 'Collected tumour type';

DROP TABLE IF EXISTS patient_sample CASCADE;

CREATE TABLE patient_sample (
    id BIGINT NOT NULL,
    external_patient_sample_id TEXT,
    patient_id BIGINT,
    diagnosis TEXT,
    grade TEXT,
    grading_system TEXT,
    stage TEXT,
    staging_system TEXT,
    primary_site_id BIGINT,
    collection_site_id BIGINT,
    prior_treatment TEXT,
    tumour_type_id BIGINT,
    age_in_years_at_collection TEXT,
    collection_event TEXT,
    collection_date TEXT,
    collection_method TEXT,
    months_since_collection_1 TEXT,
    gene_mutation_status TEXT,
    treatment_naive_at_collection TEXT,
    treated_at_collection TEXT,
    treated_prior_to_collection TEXT,
    response_to_treatment TEXT,
    virology_status TEXT,
    sharable TEXT,
    model_id BIGINT
);

COMMENT ON TABLE patient_sample IS 'Tumour type';
COMMENT ON COLUMN patient_sample.id IS 'Internal identifier';
COMMENT ON COLUMN patient_sample.external_patient_sample_id IS 'Unique ID of the patient tumour sample used to generate cancer models';
COMMENT ON COLUMN patient_sample.patient_id IS 'Reference to the patient table';
COMMENT ON COLUMN patient_sample.diagnosis IS 'Diagnosis at time of collection of the patient tumour used in the cancer model';
COMMENT ON COLUMN patient_sample.grade IS 'The implanted tumour grade value';
COMMENT ON COLUMN patient_sample.grading_system IS 'Grade classification corresponding used to describe the stage, add the version if available';
COMMENT ON COLUMN patient_sample.stage IS 'Stage of the patient at the time of collection';
COMMENT ON COLUMN patient_sample.staging_system IS 'Stage classification system used to describe the stage, add the version if available';
COMMENT ON COLUMN patient_sample.primary_site_id IS 'Reference to the tissue table. Site of the primary tumor where primary cancer is originating from (may not correspond to the site of the current tissue sample)';
COMMENT ON COLUMN patient_sample.collection_site_id IS 'Reference to the tissue table. Site of collection of the tissue sample (can be different than the primary site if tumour type is metastatic)';
COMMENT ON COLUMN patient_sample.prior_treatment IS 'Was the patient treated for cancer prior to the time of tumour sample collection. This includes any of the following: radiotherapy, chemotherapy, targeted therapy, homorno-therapy';
COMMENT ON COLUMN patient_sample.tumour_type_id IS 'Reference to the tumour_type table. Collected tumour type';
COMMENT ON COLUMN patient_sample.age_in_years_at_collection IS 'Patient age at collection';
COMMENT ON COLUMN patient_sample.collection_event IS 'Collection event corresponding to each time a patient was sampled to generate a cancer model, subsequent collection events are incremented by 1';
COMMENT ON COLUMN patient_sample.collection_date IS 'Date of collections. Important for understanding the time relationship between models generated for the same patient';
COMMENT ON COLUMN patient_sample.collection_method IS 'Method of collection of the tissue sample';
COMMENT ON COLUMN patient_sample.months_since_collection_1 IS 'The time difference between the 1st collection event and the current one (in months)';
COMMENT ON COLUMN patient_sample.gene_mutation_status IS 'Outcome of mutational status tests for the following genes: BRAF, PIK3CA, PTEN, KRAS';
COMMENT ON COLUMN patient_sample.treatment_naive_at_collection IS 'Was the patient treatment naive at the time of collection? This includes the patient being treated at the time of tumour sample collection and if the patient was treated prior to the tumour sample collection.\nThe value will be ''yes'' if either treated_at_collection or treated_prior_to_collection are ''yes''';
COMMENT ON COLUMN patient_sample.treated_at_collection IS 'Was the patient being treated for cancer at the time of tumour sample collection.';
COMMENT ON COLUMN patient_sample.treated_prior_to_collection IS 'Was the patient treated prior to the tumour sample collection.';
COMMENT ON COLUMN patient_sample.response_to_treatment IS 'Patient’s response to treatment.';
COMMENT ON COLUMN patient_sample.virology_status IS 'Positive virology status at the time of collection. Any relevant virology information which can influence cancer like EBV, HIV, HPV status';
COMMENT ON COLUMN patient_sample.model_id IS 'Reference to the model_information table';

DROP TABLE IF EXISTS xenograft_sample CASCADE;

CREATE TABLE xenograft_sample (
    id BIGINT NOT NULL,
    external_xenograft_sample_id TEXT,
    passage VARCHAR,
    host_strain_id BIGINT,
    model_id BIGINT,
    platform_id BIGINT

);

COMMENT ON TABLE xenograft_sample IS 'Tumour type';
COMMENT ON COLUMN xenograft_sample.id IS 'Internal identifier';
COMMENT ON COLUMN xenograft_sample.external_xenograft_sample_id IS 'Unique identifier for the xenograft sample. Given by the provider. Identifier of the sample from any patient tissue or PDX derived sample from which OMIC data was generated';
COMMENT ON COLUMN xenograft_sample.passage IS 'Indicates the passage number of the sample where the PDX sample was harvested (where passage 0 corresponds to first engraftment)';
COMMENT ON COLUMN xenograft_sample.host_strain_id IS 'Reference to the host_strain table. Host mouse strain name (e.g. NOD-SCID, NSG, etc)- When different mouse strain was used during the PDX line generation';
COMMENT ON COLUMN xenograft_sample.model_id IS 'Reference to the model_information table';
COMMENT ON COLUMN xenograft_sample.platform_id IS 'Internal identifier';

DROP TABLE IF EXISTS engraftment_site CASCADE;

CREATE TABLE engraftment_site (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

COMMENT ON TABLE engraftment_site IS 'Organ or anatomical site used for the PDX tumour engraftment (e.g. mammary fat pad, Right flank)';
COMMENT ON COLUMN engraftment_site.id IS 'Internal identifier';
COMMENT ON COLUMN engraftment_site.name IS 'Engraftment site';

DROP TABLE IF EXISTS engraftment_type CASCADE;

CREATE TABLE engraftment_type (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

COMMENT ON TABLE engraftment_type IS 'PDX Engraftment Type: Orthotopic if the tumour was engrafted at a corresponding anatomical site (e.g. patient tumour of primary site breast was grafted in mouse mammary fat pad). If grafted subcuteanously hererotopic is used';
COMMENT ON COLUMN engraftment_type.id IS 'Internal identifier';
COMMENT ON COLUMN engraftment_type.name IS 'Engraftment type';

DROP TABLE IF EXISTS engraftment_sample_state CASCADE;

CREATE TABLE engraftment_sample_state (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

COMMENT ON TABLE engraftment_sample_state IS 'PDX Engraftment material state (e.g. fresh or frozen)';
COMMENT ON COLUMN engraftment_sample_state.id IS 'Internal identifier';
COMMENT ON COLUMN engraftment_sample_state.name IS 'Engraftment sample state';

DROP TABLE IF EXISTS engraftment_sample_type CASCADE;

CREATE TABLE engraftment_sample_type (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

COMMENT ON TABLE engraftment_sample_type IS 'Description of the type of  material grafted into the mouse. (e.g. tissue fragments, cell suspension)';
COMMENT ON COLUMN engraftment_sample_type.id IS 'Internal identifier';
COMMENT ON COLUMN engraftment_sample_type.name IS 'Engraftment sample type';

DROP TABLE IF EXISTS host_strain CASCADE;

CREATE TABLE host_strain (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    nomenclature TEXT NOT NULL
);

COMMENT ON TABLE host_strain IS 'Host strain information';
COMMENT ON COLUMN host_strain.id IS 'Internal identifier';
COMMENT ON COLUMN host_strain.name IS 'Host mouse strain name (e.g. NOD-SCID, NSG, etc)';
COMMENT ON COLUMN host_strain.nomenclature IS 'The full nomenclature form of the host mouse strain name';

DROP TABLE IF EXISTS project_group CASCADE;

CREATE TABLE project_group (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

COMMENT ON TABLE project_group IS 'Project. A grouper element for providers';
COMMENT ON COLUMN project_group.id IS 'Internal identifier';
COMMENT ON COLUMN project_group.name IS 'Name of the project';

DROP TABLE IF EXISTS treatment CASCADE;

CREATE TABLE treatment (
    id BIGINT NOT NULL,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    data_source TEXT NOT NULL
);

COMMENT ON TABLE treatment IS 'Treatment name';
COMMENT ON COLUMN treatment.id IS 'Internal identifier';
COMMENT ON COLUMN treatment.name IS 'treatment name, can be surgery, radiotherapy, drug name or drug combination ( (radiotherapy, chemotherapy, targeted therapy, hormone-therapy))';
COMMENT ON COLUMN treatment.data_source IS 'Abbreviation of the provider. Here due to mapping, but might change if treatment names do not change between providers';

DROP TABLE IF EXISTS response CASCADE;

CREATE TABLE response (
    id BIGINT NOT NULL,
    name TEXT
);

COMMENT ON TABLE response IS 'Response to a treatment';
COMMENT ON COLUMN response.id IS 'Internal identifier';
COMMENT ON COLUMN response.name IS 'Name of the response';

DROP TABLE IF EXISTS response_classification CASCADE;

CREATE TABLE response_classification (
    id BIGINT NOT NULL,
    name TEXT
);

COMMENT ON TABLE response_classification IS 'Response classification';
COMMENT ON COLUMN response_classification.id IS 'Internal identifier';
COMMENT ON COLUMN response_classification.name IS 'Classification used to define response to treatment';

DROP TABLE IF EXISTS molecular_characterization_type CASCADE;

CREATE TABLE molecular_characterization_type (
    id BIGINT NOT NULL,
    name TEXT NOT NULL
);

COMMENT ON TABLE molecular_characterization_type IS 'Molecular Characterization Type';
COMMENT ON COLUMN molecular_characterization_type.id IS 'Internal identifier';
COMMENT ON COLUMN molecular_characterization_type.name IS 'Molecular characterization type name';

DROP TABLE IF EXISTS platform CASCADE;

CREATE TABLE platform (
    id BIGINT NOT NULL,
    library_strategy TEXT,
    provider_group_id BIGINT,
    instrument_model TEXT,
    library_selection TEXT
);

COMMENT ON TABLE platform IS 'Technical platform used to generate molecular characterisation.  E.g., targeted next generation sequencing';
COMMENT ON COLUMN platform.id IS 'Internal identifier';
COMMENT ON COLUMN platform.library_strategy IS 'The library strategy species the sequencing technique intended fo this library. Examples: WGS,WGA,WXS, RNA-Seq, Amplicon, Targeted-capture';
COMMENT ON COLUMN platform.provider_group_id IS 'Reference to the provider_group table';
COMMENT ON COLUMN platform.instrument_model IS 'The name of the platform technology used to produce the molecular characterisation,  This could be the sequencer or the name of the microarray';
COMMENT ON COLUMN platform.library_selection IS 'The library selection specifies whether any method was used to select for or against, enrich, or screen the material being sequenced. (PCR, cDNA, Hybrid Selection, PolyA, RANDOM)';

DROP TABLE IF EXISTS molecular_characterization CASCADE;

CREATE TABLE molecular_characterization (
    id BIGINT NOT NULL,
    molecular_characterization_type_id BIGINT NOT NULL,
    platform_id BIGINT NOT NULL,
    raw_data_url TEXT,
    patient_sample_id BIGINT,
    xenograft_sample_id BIGINT,
    cell_sample_id BIGINT,
    external_db_links JSON
);

COMMENT ON TABLE molecular_characterization IS 'Molecular Characterization. Represents the results of applying a specified technology to the biological sample in order to gain insight to the tumor''s attributes';
COMMENT ON COLUMN molecular_characterization.id IS 'Internal identifier';
COMMENT ON COLUMN molecular_characterization.molecular_characterization_type_id IS 'Reference to the molecular_characterization_type table';
COMMENT ON COLUMN molecular_characterization.platform_id IS 'Reference to the platform table';
COMMENT ON COLUMN molecular_characterization.raw_data_url IS 'Identifiers used to build links to external resources with raw data';
COMMENT ON COLUMN molecular_characterization.patient_sample_id IS 'Reference to patient_sample table if the sample is a patient sample';
COMMENT ON COLUMN molecular_characterization.xenograft_sample_id IS 'Reference to xenograft_sample table if the sample is a xenograft sample';
COMMENT ON COLUMN molecular_characterization.cell_sample_id IS 'Reference to cell_sample table if the sample is a cell sample';
COMMENT ON COLUMN molecular_characterization.external_db_links IS 'JSON column with links to external resources';

DROP TABLE IF EXISTS cna_molecular_data CASCADE;

CREATE UNLOGGED TABLE cna_molecular_data (
    id BIGINT NOT NULL,
    hgnc_symbol TEXT,
    chromosome TEXT,
    strand TEXT,
    log10r_cna NUMERIC,
    log2r_cna NUMERIC,
    seq_start_position NUMERIC,
    seq_end_position NUMERIC,
    copy_number_status TEXT,
    gistic_value TEXT,
    picnic_value TEXT,
    ensembl_gene_id TEXT,
    ncbi_gene_id TEXT,
    non_harmonised_symbol TEXT,
    harmonisation_result TEXT,
    molecular_characterization_id BIGINT,
    data_source TEXT,
    external_db_links JSON
);

COMMENT ON TABLE cna_molecular_data IS 'CNA molecular data';
COMMENT ON COLUMN cna_molecular_data.id IS 'Internal identifier';
COMMENT ON COLUMN cna_molecular_data.hgnc_symbol IS 'Gene symbol';
COMMENT ON COLUMN cna_molecular_data.chromosome IS 'Chromosome where the DNA copy occurs';
COMMENT ON COLUMN cna_molecular_data.strand IS 'Orientation of the DNA strand associated with the observed copy number changes, whether it is the positive or negative strand';
COMMENT ON COLUMN cna_molecular_data.log10r_cna IS 'Log10 scaled copy number variation ratio';
COMMENT ON COLUMN cna_molecular_data.log2r_cna IS 'Log2 scaled copy number variation ratio';
COMMENT ON COLUMN cna_molecular_data.seq_start_position IS 'Starting position of a genomic sequence or region that is associated with a copy number alteration';
COMMENT ON COLUMN cna_molecular_data.seq_end_position IS 'Ending position of a genomic sequence or region that is associated with a copy number alteration';
COMMENT ON COLUMN cna_molecular_data.copy_number_status IS 'Details whether there was a gain or loss of function. Categorized into gain, loss';
COMMENT ON COLUMN cna_molecular_data.gistic_value IS 'Score predicted using GISTIC tool for the copy number variation';
COMMENT ON COLUMN cna_molecular_data.picnic_value IS 'Score predicted using PICNIC algorithm for the copy number variation';
COMMENT ON COLUMN cna_molecular_data.non_harmonised_symbol IS 'Original symbol as reported by the provider';
COMMENT ON COLUMN cna_molecular_data.harmonisation_result IS 'Result of the symbol harmonisation process';
COMMENT ON COLUMN cna_molecular_data.molecular_characterization_id IS 'Reference to the molecular_characterization_ table';
COMMENT ON COLUMN cna_molecular_data.data_source IS 'Data source (abbreviation of the provider)';
COMMENT ON COLUMN cna_molecular_data.external_db_links IS 'JSON column with links to external resources';

DROP TABLE IF EXISTS biomarker_molecular_data CASCADE;

CREATE UNLOGGED TABLE biomarker_molecular_data (
    id BIGINT NOT NULL,
    biomarker TEXT,
    biomarker_status TEXT,
    essential_or_additional_marker TEXT,
    non_harmonised_symbol TEXT,
    harmonisation_result TEXT,
    molecular_characterization_id BIGINT,
    data_source TEXT,
    external_db_links JSON
);

COMMENT ON TABLE biomarker_molecular_data IS 'Biomarker molecular data';
COMMENT ON COLUMN biomarker_molecular_data.id IS 'Internal identifier';
COMMENT ON COLUMN biomarker_molecular_data.biomarker IS 'Gene symbol';
COMMENT ON COLUMN biomarker_molecular_data.biomarker_status IS 'Marker status';
COMMENT ON COLUMN biomarker_molecular_data.essential_or_additional_marker IS 'Essential or additional marker';
COMMENT ON COLUMN biomarker_molecular_data.non_harmonised_symbol IS 'Original symbol as reported by the provider';
COMMENT ON COLUMN biomarker_molecular_data.harmonisation_result IS 'Result of the symbol harmonisation process';
COMMENT ON COLUMN biomarker_molecular_data.molecular_characterization_id IS 'Reference to the molecular_characterization_ table';
COMMENT ON COLUMN biomarker_molecular_data.data_source IS 'Data source (abbreviation of the provider)';
COMMENT ON COLUMN biomarker_molecular_data.external_db_links IS 'JSON column with links to external resources';

DROP TABLE IF EXISTS expression_molecular_data CASCADE;

CREATE UNLOGGED TABLE expression_molecular_data (
    id BIGINT NOT NULL,
    hgnc_symbol TEXT,
    z_score NUMERIC,
    rnaseq_coverage NUMERIC,
    rnaseq_fpkm NUMERIC,
    rnaseq_tpm NUMERIC,
    rnaseq_count NUMERIC,
    affy_hgea_probe_id TEXT,
    affy_hgea_expression_value NUMERIC,
    illumina_hgea_probe_id TEXT,
    illumina_hgea_expression_value NUMERIC,
    ensembl_gene_id TEXT,
    ncbi_gene_id TEXT,
    non_harmonised_symbol TEXT,
    harmonisation_result TEXT,
    molecular_characterization_id BIGINT,
    data_source TEXT,
    external_db_links JSON
);

COMMENT ON TABLE expression_molecular_data IS 'Expression molecular data';
COMMENT ON COLUMN expression_molecular_data.id IS 'Internal identifier';
COMMENT ON COLUMN expression_molecular_data.hgnc_symbol IS 'Gene symbol';
COMMENT ON COLUMN expression_molecular_data.z_score IS 'Z-score representing the gene expression level';
COMMENT ON COLUMN expression_molecular_data.rnaseq_coverage IS 'The ratio between the number of bases of the mapped reads by the number of bases of a reference';
COMMENT ON COLUMN expression_molecular_data.rnaseq_fpkm IS 'Gene expression value represented in Fragments per kilo base of transcript per million mapped fragments (FPKM)';
COMMENT ON COLUMN expression_molecular_data.rnaseq_tpm IS 'Gene expression value represented in transcript per million (TPM)';
COMMENT ON COLUMN expression_molecular_data.rnaseq_count IS 'Read counts of the gene';
COMMENT ON COLUMN expression_molecular_data.affy_hgea_probe_id IS 'Affymetrix probe identifier';
COMMENT ON COLUMN expression_molecular_data.affy_hgea_expression_value IS 'Expresion value captured using Affymetrix arrays';
COMMENT ON COLUMN expression_molecular_data.illumina_hgea_probe_id IS 'Illumina probe identifier';
COMMENT ON COLUMN expression_molecular_data.illumina_hgea_expression_value IS 'Expresion value captured using Illumina arrays';
COMMENT ON COLUMN expression_molecular_data.non_harmonised_symbol IS 'Original symbol as reported by the provider';
COMMENT ON COLUMN expression_molecular_data.harmonisation_result IS 'Result of the symbol harmonisation process';
COMMENT ON COLUMN expression_molecular_data.molecular_characterization_id IS 'Reference to the molecular_characterization_ table';
COMMENT ON COLUMN expression_molecular_data.data_source IS 'Data source (abbreviation of the provider)';
COMMENT ON COLUMN expression_molecular_data.external_db_links IS 'JSON column with links to external resources';

DROP TABLE IF EXISTS mutation_measurement_data CASCADE;

CREATE UNLOGGED TABLE mutation_measurement_data (
    id BIGINT NOT NULL,
    hgnc_symbol TEXT,
    amino_acid_change TEXT,
    chromosome TEXT,
    strand TEXT,
    consequence TEXT,
    read_depth TEXT,
    allele_frequency TEXT,
    seq_start_position TEXT,
    ref_allele TEXT,
    alt_allele TEXT,
    biotype TEXT,
    coding_sequence_change TEXT,
    variant_class TEXT,
    codon_change TEXT,
    functional_prediction TEXT,
    ncbi_transcript_id TEXT,
    ensembl_transcript_id TEXT,
    variation_id TEXT,
    ensembl_gene_id TEXT,
    ncbi_gene_id TEXT,
    molecular_characterization_id BIGINT,
    non_harmonised_symbol TEXT,
    harmonisation_result TEXT,
    data_source TEXT,
    external_db_links JSON
);

COMMENT ON TABLE mutation_measurement_data IS 'Mutation measurement data';
COMMENT ON COLUMN mutation_measurement_data.id IS 'Internal identifier';
COMMENT ON COLUMN mutation_measurement_data.hgnc_symbol IS 'Gene symbol';
COMMENT ON COLUMN mutation_measurement_data.amino_acid_change IS 'Changes in the amino acid due to the variant';
COMMENT ON COLUMN mutation_measurement_data.chromosome IS 'Chromosome where the mutation occurs';
COMMENT ON COLUMN mutation_measurement_data.strand IS 'Orientation of the DNA strand where a mutation is located';
COMMENT ON COLUMN mutation_measurement_data.consequence IS 'Genomic consequence of this variant, for example: insertion of a codon caused frameshift variation will be considered frameshift variant ';
COMMENT ON COLUMN mutation_measurement_data.read_depth IS 'Read depth, the number of times each individual base was sequenced';
COMMENT ON COLUMN mutation_measurement_data.allele_frequency IS 'Allele frequency, the relative frequency of an allele in a population';
COMMENT ON COLUMN mutation_measurement_data.seq_start_position IS 'Location on the genome at which the variant is found';
COMMENT ON COLUMN mutation_measurement_data.ref_allele IS 'The base seen in the reference genome';
COMMENT ON COLUMN mutation_measurement_data.alt_allele IS 'The base other than the reference allele seen at the locus';
COMMENT ON COLUMN mutation_measurement_data.biotype IS 'Biotype of the transcript or regulatory feature eg. protein coding, non coding';
COMMENT ON COLUMN mutation_measurement_data.coding_sequence_change IS 'Change in the DNA Sequence';
COMMENT ON COLUMN mutation_measurement_data.variant_class IS 'Variation classification eg: SNV, intronic change etc';
COMMENT ON COLUMN mutation_measurement_data.codon_change IS 'Change in nucleotides';
COMMENT ON COLUMN mutation_measurement_data.functional_prediction IS 'Functional prediction';
COMMENT ON COLUMN mutation_measurement_data.ncbi_transcript_id IS 'NCBI transcript id';
COMMENT ON COLUMN mutation_measurement_data.ensembl_transcript_id IS 'Ensembl transcript id';
COMMENT ON COLUMN mutation_measurement_data.variation_id IS 'Gene symbol';
COMMENT ON COLUMN mutation_measurement_data.non_harmonised_symbol IS 'Original symbol as reported by the provider';
COMMENT ON COLUMN mutation_measurement_data.harmonisation_result IS 'Result of the symbol harmonisation process';
COMMENT ON COLUMN mutation_measurement_data.molecular_characterization_id IS 'Reference to the molecular_characterization_ table';
COMMENT ON COLUMN mutation_measurement_data.data_source IS 'Data source (abbreviation of the provider)';
COMMENT ON COLUMN mutation_measurement_data.external_db_links IS 'JSON column with links to external resources';

DROP TABLE IF EXISTS immunemarker_molecular_data CASCADE;

CREATE UNLOGGED TABLE immunemarker_molecular_data (
    id BIGINT NOT NULL,
    marker_type TEXT,
    marker_name TEXT,
    marker_value TEXT,
    essential_or_additional_details TEXT,
    molecular_characterization_id BIGINT,
    data_source TEXT
);

COMMENT ON TABLE immunemarker_molecular_data IS 'Immunemarker molecular data';
COMMENT ON COLUMN immunemarker_molecular_data.id IS 'Internal identifier';
COMMENT ON COLUMN immunemarker_molecular_data.marker_type IS 'Marker type';
COMMENT ON COLUMN immunemarker_molecular_data.marker_name IS 'Name of the immune marker';
COMMENT ON COLUMN immunemarker_molecular_data.marker_value IS 'Value or measurement associated with the immune marker';
COMMENT ON COLUMN immunemarker_molecular_data.essential_or_additional_details IS 'Additional details or notes about the immune marker';
COMMENT ON COLUMN immunemarker_molecular_data.molecular_characterization_id IS 'Reference to the molecular_characterization_ table';
COMMENT ON COLUMN immunemarker_molecular_data.data_source IS 'Data source (abbreviation of the provider)';

DROP TABLE IF EXISTS xenograft_model_specimen CASCADE;

CREATE TABLE xenograft_model_specimen (
    id BIGINT NOT NULL,
    passage_number TEXT NOT NULL,
    engraftment_site_id BIGINT,
    engraftment_type_id BIGINT,
    engraftment_sample_type_id BIGINT,
    engraftment_sample_state_id BIGINT,
    host_strain_id BIGINT,
    model_id BIGINT
);

COMMENT ON TABLE xenograft_model_specimen IS 'Xenograft Model Specimen. Represents a Xenografted mouse that has participated in the line creation and characterisation in some meaningful way.  E.g., the specimen provided a tumor that was characterized and used as quality assurance or drug dosing data';
COMMENT ON COLUMN xenograft_model_specimen.id IS 'Internal identifier';
COMMENT ON COLUMN xenograft_model_specimen.passage_number IS 'Indicate the passage number of the sample where the PDX sample was harvested (where passage 0 corresponds to first engraftment)';
COMMENT ON COLUMN xenograft_model_specimen.engraftment_site_id IS 'Reference to the engraftment_site table';
COMMENT ON COLUMN xenograft_model_specimen.engraftment_type_id IS 'Reference to the engraftment_type table';
COMMENT ON COLUMN xenograft_model_specimen.engraftment_sample_type_id IS 'Reference to the engraftment_sample_type type';
COMMENT ON COLUMN xenograft_model_specimen.engraftment_sample_state_id IS 'Reference to the engraftment_sample_state table';
COMMENT ON COLUMN xenograft_model_specimen.host_strain_id IS 'Reference to the host_strain table';
COMMENT ON COLUMN xenograft_model_specimen.model_id IS 'Reference to the model_information table';

DROP TABLE IF EXISTS gene_marker CASCADE;

CREATE TABLE gene_marker (
    id BIGINT NOT NULL,
    hgnc_id TEXT NOT NULL,
    approved_symbol TEXT,
    approved_name TEXT,
    previous_symbols TEXT,
    alias_symbols TEXT,
    accession_numbers TEXT,
    refseq_ids TEXT,
    alias_names TEXT,
    ensembl_gene_id TEXT,
    ncbi_gene_id TEXT
);

COMMENT ON TABLE gene_marker IS 'A marker represents a specific location on the _human_ genome that usually corresponds to a gene. The genes are validated based on this https://www.genenames.org/. Table used to harmosisation of gene symbols';
COMMENT ON COLUMN gene_marker.id IS 'Internal identifier';
COMMENT ON COLUMN gene_marker.hgnc_id IS 'Internal identifier';
COMMENT ON COLUMN gene_marker.approved_symbol IS 'The official gene symbol approved by the HGNC, which is typically a short form of the gene name.';
COMMENT ON COLUMN gene_marker.approved_name IS 'The full gene name approved by the HGNC';
COMMENT ON COLUMN gene_marker.previous_symbols IS 'This field displays any names that were previously HGNC-approved nomenclature';
COMMENT ON COLUMN gene_marker.alias_symbols IS 'Alternative symbols that have been used to refer to the gene. Aliases may be from literature, from other databases or may be added to represent membership of a gene group.';
COMMENT ON COLUMN gene_marker.accession_numbers IS 'Accession numbers';
COMMENT ON COLUMN gene_marker.refseq_ids IS 'Internal identifier';
COMMENT ON COLUMN gene_marker.alias_names IS 'Alias names';
COMMENT ON COLUMN gene_marker.ensembl_gene_id IS 'Ensembl gene id';
COMMENT ON COLUMN gene_marker.ncbi_gene_id IS 'NCBI gene id';

DROP TABLE IF EXISTS ontology_term_diagnosis CASCADE;

CREATE TABLE ontology_term_diagnosis(
    id BIGINT NOT NULL,
    term_id TEXT NOT NULL,
    term_name TEXT,
    term_url TEXT,
    is_a TEXT,
    ancestors TEXT
);

COMMENT ON TABLE ontology_term_diagnosis IS 'Ontology terms for diagnosis';
COMMENT ON COLUMN ontology_term_diagnosis.id IS 'Internal identifier';
COMMENT ON COLUMN ontology_term_diagnosis.term_id IS 'NCIT ontology term id';
COMMENT ON COLUMN ontology_term_diagnosis.term_name IS 'NCIT ontology term name';
COMMENT ON COLUMN ontology_term_diagnosis.term_url IS 'NCIT ontology term url';
COMMENT ON COLUMN ontology_term_diagnosis.is_a IS 'List of NCIT ontology term ids the term is classified as "is a"';
COMMENT ON COLUMN ontology_term_diagnosis.ancestors IS 'List of NCIT ontology term ids that are ancestors of the term';

DROP TABLE IF EXISTS ontology_term_treatment CASCADE;

CREATE TABLE ontology_term_treatment(
    id BIGINT NOT NULL,
    term_id TEXT NOT NULL,
    term_name TEXT,
    is_a TEXT
);

COMMENT ON TABLE ontology_term_treatment IS 'Ontology terms for treatment';
COMMENT ON COLUMN ontology_term_treatment.id IS 'Internal identifier';
COMMENT ON COLUMN ontology_term_treatment.term_id IS 'NCIT ontology term id';
COMMENT ON COLUMN ontology_term_treatment.term_name IS 'NCIT ontology term name';
COMMENT ON COLUMN ontology_term_treatment.is_a IS 'List of NCIT ontology term ids the term is classified as "is a"';

DROP TABLE IF EXISTS ontology_term_regimen CASCADE;

CREATE TABLE ontology_term_regimen(
    id BIGINT NOT NULL,
    term_id TEXT NOT NULL,
    term_name TEXT,
    is_a TEXT
);

COMMENT ON TABLE ontology_term_regimen IS 'Ontology terms for regimens';
COMMENT ON COLUMN ontology_term_regimen.id IS 'Internal identifier';
COMMENT ON COLUMN ontology_term_regimen.term_id IS 'NCIT ontology term id';
COMMENT ON COLUMN ontology_term_regimen.term_name IS 'NCIT ontology term name';
COMMENT ON COLUMN ontology_term_regimen.is_a IS 'List of NCIT ontology term ids the term is classified as "is a"';

DROP TABLE IF EXISTS sample_to_ontology CASCADE;

CREATE TABLE sample_to_ontology(
    id BIGINT NOT NULL,
    sample_id BIGINT,
    ontology_term_id BIGINT
);

COMMENT ON TABLE sample_to_ontology IS 'Mapping between diagnosis in from a sample and ontology term';
COMMENT ON COLUMN sample_to_ontology.id IS 'Internal identifier';
COMMENT ON COLUMN sample_to_ontology.sample_id IS 'Reference to the patient_sample table';
COMMENT ON COLUMN sample_to_ontology.ontology_term_id IS 'Reference to the ontology_term_diagnosis table';

DROP TABLE IF EXISTS treatment_to_ontology CASCADE;

CREATE TABLE treatment_to_ontology (
    id BIGINT NOT NULL,
    treatment_id BIGINT,
    ontology_term_id BIGINT
);

COMMENT ON TABLE treatment_to_ontology IS 'Mapping between treatments and ontology terms';
COMMENT ON COLUMN treatment_to_ontology.id IS 'Internal identifier';
COMMENT ON COLUMN treatment_to_ontology.treatment_id IS 'Reference to the treatment table';
COMMENT ON COLUMN treatment_to_ontology.ontology_term_id IS 'Reference to the ontology_term_treatment table';

DROP TABLE IF EXISTS regimen_to_ontology CASCADE;

CREATE TABLE regimen_to_ontology (
    id BIGINT NOT NULL,
    regimen_id BIGINT,
    ontology_term_id BIGINT
);

COMMENT ON TABLE regimen_to_ontology IS 'Mapping between treatments and ontology terms';
COMMENT ON COLUMN regimen_to_ontology.id IS 'Internal identifier';
COMMENT ON COLUMN regimen_to_ontology.regimen_id IS 'Reference to the treatment table (regimens)';
COMMENT ON COLUMN regimen_to_ontology.ontology_term_id IS 'Reference to the ontology_term_regimen table';

DROP TABLE IF EXISTS regimen_to_treatment CASCADE;

CREATE TABLE regimen_to_treatment (
    id BIGINT NOT NULL,
    regimen_ontology_term_id BIGINT,
    treatment_ontology_term_id BIGINT
);

COMMENT ON TABLE regimen_to_treatment IS 'Relation between regimen and treatments';
COMMENT ON COLUMN regimen_to_treatment.id IS 'Internal identifier';
COMMENT ON COLUMN regimen_to_treatment.regimen_ontology_term_id IS 'Reference to the regimen_ontology_term table';
COMMENT ON COLUMN regimen_to_treatment.treatment_ontology_term_id IS 'Reference to the treatment_ontology_term table';

DROP TABLE IF EXISTS treatment_protocol CASCADE;

CREATE TABLE treatment_protocol (
    id BIGINT NOT NULL,
    model_id BIGINT,
    patient_id BIGINT,
    treatment_target TEXT,
    response_id BIGINT,
    response_classification_id BIGINT
);

COMMENT ON TABLE treatment_protocol IS 'The specifics of drug(s) and timing of administering the drugs';
COMMENT ON COLUMN treatment_protocol.id IS 'Internal identifier';
COMMENT ON COLUMN treatment_protocol.model_id IS 'Reference to the model_information_table';
COMMENT ON COLUMN treatment_protocol.patient_id IS 'Reference to the patient table';
COMMENT ON COLUMN treatment_protocol.treatment_target IS 'Patient or model';
COMMENT ON COLUMN treatment_protocol.response_id IS 'Reference to the response table';
COMMENT ON COLUMN treatment_protocol.response_classification_id IS 'Reference to the response_classification table';


DROP TABLE IF EXISTS treatment_component CASCADE;

CREATE TABLE treatment_component (
    id BIGINT NOT NULL,
    dose TEXT,
    treatment_protocol_id BIGINT,
    treatment_id BIGINT

);

COMMENT ON TABLE treatment_component IS 'The specifics of drug(s) and timing of administering the drugs';
COMMENT ON COLUMN treatment_component.id IS 'Internal identifier';
COMMENT ON COLUMN treatment_component.dose IS 'Dose used in the treatment';
COMMENT ON COLUMN treatment_component.treatment_protocol_id IS 'Reference to the treatment_protocol table';
COMMENT ON COLUMN treatment_component.treatment_id IS 'Reference to the treatment table';

DROP TABLE IF EXISTS search_index CASCADE;

CREATE TABLE search_index (
    pdcm_model_id BIGINT NOT NULL,
    external_model_id TEXT NOT NULL,
    data_source TEXT,
    project_name TEXT,
    provider_name TEXT,
    model_type TEXT,
    supplier TEXT,
    supplier_type TEXT,
    catalog_number TEXT,
    vendor_link TEXT,
    rrid TEXT,
    external_ids TEXT,
    histology TEXT,
    search_terms TEXT[],
    cancer_system TEXT,
    dataset_available TEXT[],
    license_name TEXT,
    license_url TEXT,
    primary_site TEXT,
    collection_site TEXT,
    tumour_type TEXT,
    cancer_grade TEXT,
    cancer_grading_system TEXT,
    cancer_stage TEXT,
    cancer_staging_system TEXT,
    patient_age TEXT,
    patient_age_category TEXT,
    patient_sex TEXT,
    patient_history TEXT,
    patient_ethnicity TEXT,
    patient_ethnicity_assessment_method TEXT,
    patient_initial_diagnosis TEXT,
    patient_age_at_initial_diagnosis TEXT,
    patient_sample_id TEXT,
    patient_sample_collection_date TEXT,
    patient_sample_collection_event TEXT,
    patient_sample_collection_method TEXT,
    patient_sample_months_since_collection_1 TEXT,
    patient_sample_gene_mutation_status TEXT,
    patient_sample_virology_status TEXT,
    patient_sample_sharable TEXT,
    patient_sample_treatment_naive_at_collection TEXT,
    patient_sample_treated_at_collection TEXT,
    patient_sample_treated_prior_to_collection TEXT,
    patient_sample_response_to_treatment TEXT,
    pdx_model_publications TEXT,
    quality_assurance JSON,
    xenograft_model_specimens JSON,
    model_images JSON,
    markers_with_cna_data TEXT[],
    markers_with_mutation_data TEXT[],
    markers_with_expression_data TEXT[],
    markers_with_biomarker_data TEXT[],
    breast_cancer_biomarkers TEXT[],
    msi_status TEXT[],
    hla_types TEXT[],
    treatment_list TEXT[],
    model_treatment_list TEXT[],
    custom_treatment_type_list TEXT[],
    raw_data_resources TEXT[],
    cancer_annotation_resources TEXT[],
    scores JSON
);

COMMENT ON TABLE search_index IS 'Helper table to show results in a search';
COMMENT ON COLUMN search_index.pdcm_model_id IS 'Internal id of the model';
COMMENT ON COLUMN search_index.external_model_id IS 'Internal of the model given by the provider';
COMMENT ON COLUMN search_index.data_source IS 'Datasource (provider abbreviation)';
COMMENT ON COLUMN search_index.project_name IS 'Project of the model';
COMMENT ON COLUMN search_index.provider_name IS 'Provider name';
COMMENT ON COLUMN search_index.model_type IS 'Type of model';
COMMENT ON COLUMN search_index.supplier IS 'Supplier brief acronym or name followed by a colon and the number or name use to reference the model';
COMMENT ON COLUMN search_index.supplier_type IS 'Model supplier type - commercial, academic, other';
COMMENT ON COLUMN search_index.catalog_number IS 'Catalogue number of cell model, if commercial';
COMMENT ON COLUMN search_index.vendor_link IS 'Link to purchasable cell model, if commercial';
COMMENT ON COLUMN search_index.rrid IS 'Cellosaurus ID';
COMMENT ON COLUMN search_index.external_ids IS 'Depmap accession, Cellusaurus accession or other id. Please place in comma separated list';
COMMENT ON COLUMN search_index.histology IS 'Harmonised patient sample diagnosis';
COMMENT ON COLUMN search_index.search_terms IS 'All diagnosis related (by ontology relations) to the model';
COMMENT ON COLUMN search_index.cancer_system IS 'Cancer system of the model';
COMMENT ON COLUMN search_index.dataset_available IS 'List of datasets reported for the model (like cna, expression, publications, etc)';
COMMENT ON COLUMN search_index.cancer_system IS 'Cancer system of the model';
COMMENT ON COLUMN search_index.license_name IS 'License name for the model';
COMMENT ON COLUMN search_index.license_url IS 'Url of the license';
COMMENT ON COLUMN search_index.primary_site IS 'Site of the primary tumor where primary cancer is originating from (may not correspond to the site of the current tissue sample)';
COMMENT ON COLUMN search_index.collection_site IS 'Site of collection of the tissue sample (can be different than the primary site if tumour type is metastatic).';
COMMENT ON COLUMN search_index.tumour_type IS 'Collected tumour type';
COMMENT ON COLUMN search_index.cancer_grade IS 'The implanted tumour grade value';
COMMENT ON COLUMN search_index.cancer_grading_system IS 'Grade classification corresponding used to describe the stage, add the version if available';
COMMENT ON COLUMN search_index.cancer_stage IS 'Stage of the patient at the time of collection';
COMMENT ON COLUMN search_index.cancer_staging_system IS 'Stage classification system used to describe the stage, add the version if available';
COMMENT ON COLUMN search_index.patient_age IS 'Patient age at collection';
COMMENT ON COLUMN search_index.patient_age_category IS 'Age category at the time of sampling';
COMMENT ON COLUMN search_index.patient_sex IS 'Patient sex';
COMMENT ON COLUMN search_index.patient_history IS 'Cancer relevant comorbidity or environmental exposure';
COMMENT ON COLUMN search_index.patient_ethnicity IS 'Patient Ethnic group. Can be derived from self-assessment or genetic analysis';
COMMENT ON COLUMN search_index.patient_ethnicity_assessment_method IS 'Patient Ethnic group assessment method';
COMMENT ON COLUMN search_index.patient_initial_diagnosis IS 'Diagnosis of the patient when first diagnosed at age_at_initial_diagnosis - this can be different than the diagnosis at the time of collection which is collected in the sample section';
COMMENT ON COLUMN search_index.patient_age_at_initial_diagnosis IS 'This is the age of first diagnostic. Can be prior to the age at which the tissue sample was collected for implant';
COMMENT ON COLUMN search_index.patient_sample_id IS 'Patient sample identifier given by the provider';
COMMENT ON COLUMN search_index.patient_sample_collection_date IS 'Date of collections. Important for understanding the time relationship between models generated for the same patient';
COMMENT ON COLUMN search_index.patient_sample_collection_event IS 'Collection event corresponding to each time a patient was sampled to generate a cancer model, subsequent collection events are incremented by 1';
COMMENT ON COLUMN search_index.patient_sample_collection_method IS 'Method of collection of the tissue sample';
COMMENT ON COLUMN search_index.patient_sample_months_since_collection_1 IS 'The time difference between the 1st collection event and the current one (in months)';
COMMENT ON COLUMN search_index.patient_sample_gene_mutation_status IS 'Outcome of mutational status tests for the following genes: BRAF, PIK3CA, PTEN, KRAS';
COMMENT ON COLUMN search_index.patient_sample_virology_status IS 'Positive virology status at the time of collection. Any relevant virology information which can influence cancer like EBV, HIV, HPV status';
COMMENT ON COLUMN search_index.patient_sample_sharable IS 'Indicates if patient treatment information is available and sharable';
COMMENT ON COLUMN search_index.patient_sample_treated_at_collection IS 'Indicates if the patient was being treated for cancer (radiotherapy, chemotherapy, targeted therapy, hormono-therapy) at the time of collection';
COMMENT ON COLUMN search_index.patient_sample_treated_prior_to_collection IS 'Indicates if the patient was previously treated prior to the collection (radiotherapy, chemotherapy, targeted therapy, hormono-therapy)';
COMMENT ON COLUMN search_index.patient_sample_response_to_treatment IS 'Patient’s response to treatment.';
COMMENT ON COLUMN search_index.pdx_model_publications IS 'Publications that are associated to one or more models (PubMed IDs separated by commas)';
COMMENT ON COLUMN search_index.quality_assurance IS 'Quality assurance data';
COMMENT ON COLUMN search_index.xenograft_model_specimens IS 'Represents a xenografted mouse that has participated in the line creation and characterisation in some meaningful way. E.g., the specimen provided a tumor that was characterized and used as quality assurance or drug dosing data';
COMMENT ON COLUMN search_index.model_images IS 'Images associated with the model';
COMMENT ON COLUMN search_index.markers_with_cna_data IS 'Marker list in associate CNA data';
COMMENT ON COLUMN search_index.markers_with_mutation_data IS 'Marker list in associate mutation data';
COMMENT ON COLUMN search_index.markers_with_expression_data IS 'Marker list in associate expression data';
COMMENT ON COLUMN search_index.markers_with_biomarker_data IS 'Marker list in associate biomarker data';
COMMENT ON COLUMN search_index.breast_cancer_biomarkers IS 'List of biomarkers associated to breast cancer';
COMMENT ON COLUMN search_index.msi_status IS 'MSI status';
COMMENT ON COLUMN search_index.hla_types IS 'HLA types';
COMMENT ON COLUMN search_index.treatment_list IS 'Patient treatment data';
COMMENT ON COLUMN search_index.model_treatment_list IS 'Drug dosing data';
COMMENT ON COLUMN search_index.custom_treatment_type_list IS 'Treatment types + patient treatment status (Excluding "Not Provided")';
COMMENT ON COLUMN search_index.raw_data_resources IS 'List of resources (calculated from raw data links) the model links to';
COMMENT ON COLUMN search_index.cancer_annotation_resources IS 'List of resources (calculated from cancer annotation links) the model links to';
COMMENT ON COLUMN search_index.scores IS 'Model characterizations scores';


DROP TABLE IF EXISTS search_facet CASCADE;

CREATE TABLE search_facet (
    facet_section TEXT,
    facet_name TEXT,
    facet_column TEXT,
    facet_options TEXT[],
    facet_example TEXT
);

COMMENT ON TABLE search_facet IS 'Helper table to show filter options';
COMMENT ON COLUMN search_facet.facet_section IS 'Facet section';
COMMENT ON COLUMN search_facet.facet_name IS 'Facet name';
COMMENT ON COLUMN search_facet.facet_column IS 'Facet column';
COMMENT ON COLUMN search_facet.facet_options IS 'List of possible options';
COMMENT ON COLUMN search_facet.facet_example IS 'Facet example';

DROP TABLE IF EXISTS molecular_data_restriction CASCADE;

CREATE TABLE molecular_data_restriction (
    data_source TEXT,
    molecular_data_table TEXT
);

COMMENT ON TABLE molecular_data_restriction IS 'Internal table to store molecular tables which data cannot be displayed to the user. Configured at provider level.';
COMMENT ON COLUMN molecular_data_restriction.data_source IS 'Provider with the restriction';
COMMENT ON COLUMN molecular_data_restriction.molecular_data_table IS 'Table whose data cannot be showed';

DROP TABLE IF EXISTS available_molecular_data_columns CASCADE;
CREATE TABLE available_molecular_data_columns (
    data_source TEXT,
    not_empty_cols TEXT[],
    molecular_characterization_type TEXT
);

COMMENT ON TABLE available_molecular_data_columns IS 'Table that shows columns with data per data source / molecular data table';
COMMENT ON COLUMN available_molecular_data_columns.data_source IS 'Data source';
COMMENT ON COLUMN available_molecular_data_columns.not_empty_cols IS 'List of columns that have data';
COMMENT ON COLUMN available_molecular_data_columns.molecular_characterization_type IS 'Type of molecular data table';

DROP TABLE IF EXISTS release_info CASCADE;
CREATE TABLE release_info (
    name TEXT,
    date TIMESTAMP,
    providers TEXT[]
);

COMMENT ON TABLE release_info IS 'Table that shows columns with data per data source / molecular data table';
COMMENT ON COLUMN release_info.name IS 'Name of the release';
COMMENT ON COLUMN release_info.date IS 'Date of the release';
COMMENT ON COLUMN release_info.providers IS 'List of processed providers';

DROP TABLE IF EXISTS image_study CASCADE;
CREATE TABLE image_study (
    id BIGINT NOT NULL,
    study_id TEXT,
    title TEXT,
    description TEXT,
    licence TEXT,
    contact TEXT,
    sample_organism TEXT,
    sample_description TEXT,
    sample_preparation_protocol TEXT,
    imaging_instrument TEXT,
    image_acquisition_parameters TEXT,
    imaging_method TEXT
);

COMMENT ON TABLE image_study IS 'Information about an image study';
COMMENT ON COLUMN image_study.id IS 'Internal identifier';
COMMENT ON COLUMN image_study.study_id IS 'Accession number for BioImage Archive study';
COMMENT ON COLUMN image_study.title IS 'The title for your dataset';
COMMENT ON COLUMN image_study.description IS 'Field to describe your dataset. This can be the abstract to an accompanying publication';
COMMENT ON COLUMN image_study.licence IS 'The licence under which the data are available';
COMMENT ON COLUMN image_study.contact IS 'Contact details for the authors involved in the study';
COMMENT ON COLUMN image_study.sample_organism IS 'What is being imaged';
COMMENT ON COLUMN image_study.sample_description IS 'High level description of sample';
COMMENT ON COLUMN image_study.sample_preparation_protocol IS 'How the sample was prepared for imaging';
COMMENT ON COLUMN image_study.imaging_instrument IS 'Description of the instrument used to capture the images';
COMMENT ON COLUMN image_study.image_acquisition_parameters IS 'How the images were acquired, including instrument settings/parameters';
COMMENT ON COLUMN image_study.imaging_method IS 'What method was used to capture images';

DROP TABLE IF EXISTS model_image CASCADE;
CREATE TABLE model_image (
    id BIGINT NOT NULL,
    model_id BIGINT NOT NULL,
    url TEXT,
    description TEXT,
    sample_type TEXT,
    passage TEXT,
    magnification TEXT,
    staining TEXT
);

COMMENT ON TABLE model_image IS 'Information about an image study';
COMMENT ON COLUMN model_image.id IS 'Internal identifier';
COMMENT ON COLUMN model_image.model_id IS 'Unique identifier for all the PDXs derived from the same tissue sample';
COMMENT ON COLUMN model_image.url IS 'Link to the model image';
COMMENT ON COLUMN model_image.description IS 'Description of the image on what was being imaged';
COMMENT ON COLUMN model_image.sample_type IS 'Type of sample being imaged (pdx, patient, organoid, cell line)';
COMMENT ON COLUMN model_image.passage IS 'Passage number imaging was performed. Passage 0 correspond to first engraftment';
COMMENT ON COLUMN model_image.magnification IS 'Magnification of the mode image';
COMMENT ON COLUMN model_image.staining IS 'Staining used for imaging the sample';

--- PostgreSQL functions

-- Returns a JSON object with all the model parents connected to _model
CREATE OR REPLACE FUNCTION pdcm_api.get_parents_tree(_model varchar)
  RETURNS jsonb
  LANGUAGE sql STABLE PARALLEL SAFE AS
$func$
SELECT jsonb_agg(sub)
FROM  
(
	SELECT 
		r2.external_model_id,
		r2.type,
		pdcm_api.get_parents_tree(r1.parent_id) AS parents
 	FROM model_information r1, model_information r2
 	WHERE _model = r1.external_model_id
 	AND r1.parent_id = r2.external_model_id
   ) sub
$func$;

-- Returns a JSON object with all the models derived from _model
CREATE OR REPLACE FUNCTION pdcm_api.get_children_tree(_model varchar)
  RETURNS jsonb
  LANGUAGE sql STABLE PARALLEL SAFE AS
$func$
SELECT jsonb_agg(sub)
FROM  
(
	SELECT 
		r1.external_model_id,
		r1.type,
		pdcm_api.get_children_tree(r1.external_model_id) AS children
 	FROM model_information r1, model_information r2
 	WHERE _model = r1.parent_id
 	AND r1.parent_id = r2.external_model_id
   ) sub
$func$;
