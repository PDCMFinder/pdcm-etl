patient_treatment_columns = [
    "patient_id",
    "treatment_name",
    "treatment_type",
    "treatment_dose",
    "treatment_starting_date",
    "treatment_duration",
    "treatment_event",
    "elapsed_time",
    "treatment_response",
    "response_classification",
    "model_id",
    "data_source_tmp"]

drug_dosing_columns = [
    "model_id",
    "passage_range",
    "treatment_name",
    "treatment_type",
    "treatment_dose",
    "administration_route",
    "treatment_schedule",
    "treatment_length",
    "treatment_response",
    "response_classification",
    "data_source_tmp"]


ethnicity = [
    {
        "id": "1",
        "name": "Not Hispanic or Latino"
    }
]

diagnosis = [
    {
        "id": "1",
        "name": "Invasive Ductal Carcinoma Not Otherwise Specified"
    }
]

provider_group = [
    {
        "id": "1",
        "name": "TRACE"
    }
]

publication_group = [
    {
        "id": "1",
        "pubmed_id": "PMID: 27626319"
    }
]

accessibility_group = [
    {
        "id": "1",
        "europdx_access_modalities": "Transnational access, test only",
        "accessibility": "industry and academia"
    }
]

contact_people = [
    {
        "id": "1",
        "name_list": "name1,name2",
        "email_list": "email1,email2"
    }
]

contact_form = [
    {
        "id": "1",
        "form_url": "https://www.uzleuven-kuleuven.be/lki/trace/contact"
    }
]

host_strains = [
    {
        "id": "1",
        "name": "NOD SCID GAMMA",
        "host_strain_nomenclature": "NOD.Cg-Prkdcscid Il2rgtm1Sug/JicTac"
    }]

models = [
    {
        "id": "1",
        "external_model_id": "BRC0013PR",
        "data_source": "TRACE",
        "publication_group_id": "1",
        "accessibility_group_id": "1",
        "contact_people_id": "1",
        "contact_form_id": "1",
        "source_database_id": "1"
    },
    {
        "id": "2",
        "external_model_id": "BRC0015PR",
        "data_source": "TRACE",
        "publication_group_id": "1",
        "accessibility_group_id": "1",
        "contact_people_id": "1",
        "contact_form_id": "1",
        "source_database_id": "1"
    }
]


patients = [
    {
        "id": "1",
        "external_patient_id": "PTX-BRC-002",
        "sex": "Female",
        "history": "",
        "ethnicity_id": "1",
        "ethnicity_assessment_method": "1",
        "initial_diagnosis_id": "1",
        "provider_group_id": "1",
        "data_source_tmp": "TRACE"
    },
    {
        "id": "1",
        "external_patient_id": "PTX-BRC-007",
        "sex": "Male",
        "history": "",
        "ethnicity_id": "1",
        "ethnicity_assessment_method": "1",
        "initial_diagnosis_id": "1",
        "provider_group_id": "1",
        "data_source_tmp": "TRACE"
    }
]

responses = [
    {
        "id": "1",
        "name": "complete response"
    },
    {
        "id": "2",
        "name": "stable disease"
    }
]

responses_classification = [
    {
        "id": "1",
        "name": "RECIST 1.1"
    },
    {
        "id": "2",
        "name": "modified RECIST criteria"
    }
]