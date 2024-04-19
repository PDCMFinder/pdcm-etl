# Weights for fields that are common in all types of models
common_weights = {
    "patient_sex": 1,
    "patient_history": 0,
    "patient_ethnicity": 0.5,
    "patient_ethnicity_assessment_method": 0,
    "patient_initial_diagnosis": 0,
    "patient_age_at_initial_diagnosis": 0,
    "patient_sample_id": 1,
    "patient_sample_collection_date": 0,
    "patient_sample_collection_event": 0,
    "patient_sample_months_since_collection_1": 0,
    "patient_age": 1,
    "histology": 1,
    "tumour_type": 1,
    "primary_site": 1,
    "collection_site": 0.5,
    "cancer_stage": 0.5,
    "cancer_staging_system": 0,
    "cancer_grade": 0.5,
    "cancer_grading_system": 0,
    "patient_sample_virology_status": 0,
    "patient_sample_sharable": 0,
    "patient_sample_treated_at_collection": 0.5,
    "patient_sample_treated_prior_to_collection": 0.5,
    "pdx_model_publications": 0,
    "quality_assurance.validation_technique": 1,
    "quality_assurance.description": 1,
    "quality_assurance.passages_tested": 1,
    "quality_assurance.validation_host_strain_nomenclature": 1,
    "quality_assurance.SNP_analysis": 1,
    "quality_assurance.STR_analysis": 1,
    "quality_assurance.comments": 0,
    "supplier": 1,
    "supplier_type": 1

}

# Weights for fields that only apply to PDX models
pdx_only_weights = {
    "xenograft_model_specimens.host_strain_name": 1,
    "xenograft_model_specimens.host_strain_nomenclature": 1,
    "xenograft_model_specimens.engraftment_site": 1,
    "xenograft_model_specimens.engraftment_type": 1,
    "xenograft_model_specimens.engraftment_sample_type": 1,
    "xenograft_model_specimens.engraftment_sample_state": 0.5,
    "xenograft_model_specimens.passage_number": 1
}

# Weights for fields that only apply to In Vitro models
in_vitro_only_weights = {
    "model_name": 1,
    "model_name_aliases": 0.5,
    "growth_properties": 1,
    "growth_media": 1,
    "media_id": 1,
    "plate_coating": 1, 
    "other_plate_coating" :1,
    "passage_number": 1,
    "contaminated": 1,
    "contamination_details": 0.5,
    "supplements": 0.5,
    "drug": 0.5,
    "drug_concentration": 0.5,
    "quality_assurance.morphological_features": 1,
    "quality_assurance.tumour_status": 1,
    "quality_assurance.model_purity": 1,
}

