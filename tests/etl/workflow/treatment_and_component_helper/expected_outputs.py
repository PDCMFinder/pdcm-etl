expected_one_treatment_one_dose = [
    {
        "treatment_protocol_id": "1",
        "treatment_name": "Dactolisib",
        "treatment_type": "treatment",
        "treatment_dose": "40.0mg/kg",
        "data_source_tmp": "trace"
    }
]

expected_several_treatment_several_doses = [
    {
        "treatment_protocol_id": "1",
        "treatment_name": "5-Fluorouracil",
        "treatment_type": "treatment",
        "treatment_dose": "500 mg/m2",
        "data_source_tmp": "trace"
    },
    {
        "treatment_protocol_id": "1",
        "treatment_name": "Epirubicin",
        "treatment_type": "treatment",
        "treatment_dose": "75 mg/m2",
        "data_source_tmp": "trace"
    },
    {
        "treatment_protocol_id": "1",
        "treatment_name": "Cyclophosphamide",
        "treatment_type": "treatment",
        "treatment_dose": "500 mg/m2",
        "data_source_tmp": "trace"
    }
]

expected_one_treatment_several_doses = [
    {
        "treatment_protocol_id": "1",
        "treatment_name": "5-Fluorouracil",
        "treatment_type": "treatment",
        "treatment_dose": "500 mg/m2 + 75 mg/m2 + 500 mg/m2",
        "data_source_tmp": "trace"
    }
]

expected_several_treatments_one_dose = [
    {
        "treatment_protocol_id": "1",
        "treatment_name": "5-Fluorouracil",
        "treatment_type": "treatment",
        "treatment_dose": "500 mg/m2",
        "data_source_tmp": "trace"
    },
    {
        "treatment_protocol_id": "1",
        "treatment_name": "Epirubicin",
        "treatment_type": "treatment",
        "treatment_dose": "500 mg/m2",
        "data_source_tmp": "trace"
    },
    {
        "treatment_protocol_id": "1",
        "treatment_name": "Cyclophosphamide",
        "treatment_type": "treatment",
        "treatment_dose": "500 mg/m2",
        "data_source_tmp": "trace"
    }
]


# Treatment type

expected_one_treatment_one_type = [
    {
        "treatment_protocol_id": "1",
        "treatment_name": "Dactolisib",
        "treatment_type": "treatment",
        "treatment_dose": "40.0mg/kg",
        "data_source_tmp": "trace"
    }
]

expected_several_treatment_several_types = [
    {
        "treatment_protocol_id": "1",
        "treatment_name": "5-Fluorouracil",
        "treatment_type": "treatment1",
        "treatment_dose": "500 mg/m2",
        "data_source_tmp": "trace"
    },
    {
        "treatment_protocol_id": "1",
        "treatment_name": "Epirubicin",
        "treatment_type": "treatment2",
        "treatment_dose": "75 mg/m2",
        "data_source_tmp": "trace"
    },
    {
        "treatment_protocol_id": "1",
        "treatment_name": "Cyclophosphamide",
        "treatment_type": "treatment3",
        "treatment_dose": "500 mg/m2",
        "data_source_tmp": "trace"
    }
]

expected_several_treatments_one_type = [
    {
        "treatment_protocol_id": "1",
        "treatment_name": "5-Fluorouracil",
        "treatment_type": "treatment",
        "treatment_dose": "500 mg/m2",
        "data_source_tmp": "trace"
    },
    {
        "treatment_protocol_id": "1",
        "treatment_name": "Epirubicin",
        "treatment_type": "treatment",
        "treatment_dose": "500 mg/m2",
        "data_source_tmp": "trace"
    },
    {
        "treatment_protocol_id": "1",
        "treatment_name": "Cyclophosphamide",
        "treatment_type": "treatment",
        "treatment_dose": "500 mg/m2",
        "data_source_tmp": "trace"
    }
]
