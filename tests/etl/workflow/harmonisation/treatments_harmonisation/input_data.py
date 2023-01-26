treatment = [
    {
        "id": "1",
        "name": "t1",
        "data_source": "TRACE"
    },
    {
        "id": "10",
        "name": "t1",
        "data_source": "OTHER"
    },
    {
        "id": "2",
        "name": "t2",
        "data_source": "TRACE"
    },
    {
        "id": "3",
        "name": "r1",
        "data_source": "TRACE"
    },
    {
        "id": "4",
        "name": "r2",
        "data_source": "TRACE"
    },
    {
        "id": "5",
        "name": "t3(r1)",
        "data_source": "TRACE"
    },
    {
        "id": "6",
        "name": "t4(r1)",
        "data_source": "TRACE"
    },
    {
        "id": "7",
        "name": "t5(r2)",
        "data_source": "TRACE"
    },
    {
        "id": "8",
        "name": "t6(r2)",
        "data_source": "TRACE"
    },
    {
        "id": "9",
        "name": "t7(protocol2)",
        "data_source": "TRACE"
    }
]

patient_sample = [
    {
        "id": "1",
        "external_patient_sample_id": "sample1",
        "model_id": "1"
    }
]

treatment_protocol = [
    {
        "id": "1",
        "model_id": None,
        "patient_id": "1",
        "treatment_target": "patient",
        "response_id": "",
        "response_classification_id": "",
        "data_source_tmp": "TRACE"
    },
    {
        "id": "2",
        "model_id": "1",
        "patient_id": None,
        "treatment_target": "drug dosing",
        "response_id": "",
        "response_classification_id": "",
        "data_source_tmp": "TRACE"
    },
    {
        "id": "3",
        "model_id": None,
        "patient_id": "1",
        "treatment_target": "patient",
        "response_id": "",
        "response_classification_id": "",
        "data_source_tmp": "TRACE"
    }
]

formatted_protocol = [
    {
        "treatment_protocol_id": "1",
        "protocol_model": "1",
        "patient_id": "1",
        "treatment_target": "patient",
        "response_id": "1",
        "response_name": "r1",
        "response_classification_id": "",
        "data_source_tmp": "TRACE"
    },
    {
        "treatment_protocol_id": "2",
        "protocol_model": "1",
        "patient_id": None,
        "treatment_target": "drug dosing",
        "response_id": "2",
        "response_name": "r2",
        "response_classification_id": "",
        "data_source_tmp": "TRACE"
    },
    {
        "treatment_protocol_id": "3",
        "protocol_model": "1",
        "patient_id": "1",
        "treatment_target": "patient",
        "response_id": "3",
        "response_name": "r3",
        "response_classification_id": "",
        "data_source_tmp": "TRACE"
    }
]

treatment_component = [
    {
        "id": "1",
        "treatment_id": "1",
        "dose": "1 mg",
        "treatment_protocol_id": "1"
    },
    {
        "id": "2",
        "treatment_id": "3",
        "dose": "2 mg",
        "treatment_protocol_id": "1"
    },
    {
        "id": "3",
        "treatment_id": "8",
        "dose": "1 mg",
        "treatment_protocol_id": "2"
    },
    {
        "id": "4",
        "treatment_id": "7",
        "dose": "2 mg",
        "treatment_protocol_id": "2"
    },
    {
        "id": "5",
        "treatment_id": "9",
        "dose": "2 mg",
        "treatment_protocol_id": "3"
    },
    {
        "id": "6",
        "treatment_id": "1",
        "dose": "4 mg",
        "treatment_protocol_id": "3"
    }
]

ontology_term_regimen = [
    {
        "id": "1",
        "term_id": "NCIT:C0010",
        "term_name": "ontology_r1",
    },
    {
        "id": "2",
        "term_id": "NCIT:0011",
        "term_name": "ontology_r2",
    }
]

ontology_term_treatment = [
    {
        "id": "1",
        "term_id": "NCIT:0001",
        "term_name": "ontology_t1",
    },
    {
        "id": "2",
        "term_id": "NCIT:0002",
        "term_name": "ontology_t2"
    },
    {
        "id": "3",
        "term_id": "NCIT:0003",
        "term_name": "ontology_t3(r1)"
    },
    {
        "id": "4",
        "term_id": "NCIT:0004",
        "term_name": "ontology_t4(r1)"
    },
    {
        "id": "5",
        "term_id": "NCIT:0005",
        "term_name": "ontology_t5(r2)"
    },
    {
        "id": "6",
        "term_id": "NCIT:0006",
        "term_name": "ontology_t6(r2)"
    },
    {
        "id": "7",
        "term_id": "NCIT:0007",
        "term_name": "ontology_t7(protocol2)"
    }
]

treatment_to_ontology = [
    {
        "id": "1",
        "treatment_id": "1",
        "ontology_term_id": "1"
    },
    {
        "id": "2",
        "treatment_id": "2",
        "ontology_term_id": "2"
    },
    {
        "id": "3",
        "treatment_id": "5",
        "ontology_term_id": "3"
    },
    {
        "id": "4",
        "treatment_id": "6",
        "ontology_term_id": "4"
    },
    {
        "id": "5",
        "treatment_id": "7",
        "ontology_term_id": "5"
    },
    {
        "id": "6",
        "treatment_id": "8",
        "ontology_term_id": "6"
    },
    {
        "id": "7",
        "treatment_id": "9",
        "ontology_term_id": "7"
    }
]

regimen_to_ontology = [
    {
        "id": "1",
        "regimen_id": "3",
        "ontology_term_id": "1"
    },
    {
        "id": "2",
        "regimen_id": "4",
        "ontology_term_id": "2"
    }
]

regimen_to_treatment = [
    {
        "id": "1",
        "regimen_ontology_term_id": "1",
        "treatment_ontology_term_id": "3"
    },
    {
        "id": "2",
        "regimen_ontology_term_id": "1",
        "treatment_ontology_term_id": "4"
    },
    {
        "id": "3",
        "regimen_ontology_term_id": "2",
        "treatment_ontology_term_id": "5"
    },
    {
        "id": "4",
        "regimen_ontology_term_id": "2",
        "treatment_ontology_term_id": "6"
    }
]
