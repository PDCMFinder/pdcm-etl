treatment = [
    {
        "id": "1",
        "name": "ontology_t1",
        "term_id": "NCIT:0001",
        "types": ["Immunotherapy"],
        "class": "treatment",
    },
    {
        "id": "2",
        "name": "ontology_t2",
        "term_id": "NCIT:0002",
        "types": ["Immunotherapy"],
        "class": "treatment",
    },
    {
        "id": "3",
        "name": "ontology_r1",
        "term_id": "NCIT:C0010",
        "types": ["Immunotherapy"],
        "class": "regimen",
    },
    {
        "id": "4",
        "name": "ontology_r2",
        "term_id": "NCIT:0011",
        "types": ["Immunotherapy"],
        "class": "regimen",
    },
    {
        "id": "5",
        "name": "ontology_t3(r1)",
        "term_id": "NCIT:0003",
        "types": ["Immunotherapy"],
        "class": "treatment",
    },
    {
        "id": "6",
        "name": "ontology_t4(r1)",
        "term_id": "NCIT:0004",
        "types": ["Immunotherapy"],
        "class": "treatment",
    },
    {
        "id": "7",
        "name": "ontology_t5(r2)",
        "term_id": "NCIT:0005",
        "types": ["Immunotherapy"],
        "class": "treatment",
    },
    {
        "id": "8",
        "name": "ontology_t6(r2)",
        "term_id": "NCIT:0006",
        "types": ["Immunotherapy"],
        "class": "treatment",
    },
    {
        "id": "9",
        "name": "ontology_t7(protocol2)",
        "term_id": "NCIT:0007",
        "types": ["Immunotherapy"],
        "class": "treatment",
    },
]

patient_sample = [{"id": "1", "external_patient_sample_id": "sample1", "model_id": "1"}]

treatment_protocol = [
    {
        "id": "1",
        "model_id": None,
        "patient_id": "1",
        "treatment_target": "patient",
        "response_id": "",
        "response_classification_id": "",
        "data_source_tmp": "TRACE",
    },
    {
        "id": "2",
        "model_id": "1",
        "patient_id": None,
        "treatment_target": "drug dosing",
        "response_id": "",
        "response_classification_id": "",
        "data_source_tmp": "TRACE",
    },
    {
        "id": "3",
        "model_id": None,
        "patient_id": "1",
        "treatment_target": "patient",
        "response_id": "",
        "response_classification_id": "",
        "data_source_tmp": "TRACE",
    },
]

formatted_protocol = [
    {
        "treatment_protocol_id": "1",
        "protocol_model": "1",
        "patient_id": "1",
        "treatment_target": "patient",
        "response_id": "1",
        "response_name": "response1",
        "response_classification_id": "",
        "data_source_tmp": "TRACE",
    },
    {
        "treatment_protocol_id": "2",
        "protocol_model": "1",
        "patient_id": None,
        "treatment_target": "drug dosing",
        "response_id": "2",
        "response_name": "response2",
        "response_classification_id": "",
        "data_source_tmp": "TRACE",
    },
    {
        "treatment_protocol_id": "3",
        "protocol_model": "1",
        "patient_id": "1",
        "treatment_target": "patient",
        "response_id": "3",
        "response_name": "response3",
        "response_classification_id": "",
        "data_source_tmp": "TRACE",
    },
]

treatment_component = [
    {"id": "1", "treatment_id": "1", "dose": "1 mg", "treatment_protocol_id": "1"},
    {"id": "2", "treatment_id": "3", "dose": "2 mg", "treatment_protocol_id": "1"},
    {"id": "3", "treatment_id": "8", "dose": "1 mg", "treatment_protocol_id": "2"},
    {"id": "4", "treatment_id": "7", "dose": "2 mg", "treatment_protocol_id": "2"},
    {"id": "5", "treatment_id": "9", "dose": "2 mg", "treatment_protocol_id": "3"},
    {"id": "6", "treatment_id": "1", "dose": "4 mg", "treatment_protocol_id": "3"},
]

regimen_to_treatment = [
    {"regimen": "ontology_r1", "treatment": "ontology_t3(r1)"},
    {"regimen": "ontology_r1", "treatment": "ontology_t4(r1)"},
    {"regimen": "ontology_r2", "treatment": "ontology_t5(r2)"},
    {"regimen": "ontology_r2", "treatment": "ontology_t6(r2)"},
]
