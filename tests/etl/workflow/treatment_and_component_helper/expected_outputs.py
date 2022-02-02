expected_one_treatment_one_dose = [
    {
        "treatment_protocol_id": "1",
        "model_id": "1",
        "patient_id": None,
        "treatment_name": "Dactolisib",
        "treatment_dose": "40.0mg/kg"
    }
]

expected_several_treatment_several_doses = [
    {
        "treatment_protocol_id": "1",
        "model_id": "1",
        "patient_id": None,
        "treatment_name": "5-Fluorouracil",
        "treatment_dose": "500 mg/m2"
    },
    {
        "treatment_protocol_id": "1",
        "model_id": "1",
        "patient_id": None,
        "treatment_name": "Epirubicin",
        "treatment_dose": "75 mg/m2"
    },
    {
        "treatment_protocol_id": "1",
        "model_id": "1",
        "patient_id": None,
        "treatment_name": "Cyclophosphamide",
        "treatment_dose": "500 mg/m2"
    }
]

expected_one_treatment_several_doses = [
    {
        "treatment_protocol_id": "1",
        "model_id": "1",
        "patient_id": None,
        "treatment_name": "5-Fluorouracil",
        "treatment_dose": "500 mg/m2 + 75 mg/m2 + 500 mg/m2"
    }
]

expected_several_treatments_one_dose = [
    {
        "treatment_protocol_id": "1",
        "model_id": "1",
        "patient_id": None,
        "treatment_name": "5-Fluorouracil",
        "treatment_dose": "500 mg/m2"
    },
    {
        "treatment_protocol_id": "1",
        "model_id": "1",
        "patient_id": None,
        "treatment_name": "Epirubicin",
        "treatment_dose": "500 mg/m2"
    },
    {
        "treatment_protocol_id": "1",
        "model_id": "1",
        "patient_id": None,
        "treatment_name": "Cyclophosphamide",
        "treatment_dose": "500 mg/m2"
    }
]


