patient = [
    {
        "id": "1",
        "external_patient_id": "patient_1",
        "sex": 'Female',
        "data_source_tmp": "TRACE"
    }
]

patient_sample = [
    {
        "id": "1",
        "external_patient_sample_id": "patient_sample_1",
        "patient_id": "1",
        "model_id": "1",
        "data_source_tmp": "TRACE"
    },
    {
        "id": "2",
        "external_patient_sample_id": "patient_sample_2",
        "patient_id": "1",
        "model_id": "2",
        "data_source_tmp": "TRACE"
    }
]

model = [
    {
        "id": "1",
        "external_model_id": "model_1",
        "type": "PDX",
        "data_source_tmp": "TRACE",
        "parent_id": None
    },
    {
        "id": "2",
        "external_model_id": "model_2",
        "type": "PDX",
        "data_source_tmp": "TRACE",
        "parent_id": "model_1"
    }
]