treatment_type_helper = [
    {
        "name": "Hormone Therapy",
        "term_name": "Hormone Therapy",
        "term_id": "NCIT:C15445",
        "treatment_types": ["Hormone Therapy"],
        "class": "treatment",
    },
    {
        "name": "Interleukin-2",
        "term_name": "Interleukin-2",
        "term_id": "NCIT:C20507",
        "treatment_types": ["Immunotherapy"],
        "class": "treatment",
    },
    {
        "name": "CKX620",
        "term_name": None,
        "term_id": None,
        "treatment_types": [],
        "class": "treatment",
    },
]

raw_external_resources = [
    {
        "id": 1,
        "name": "ChEMBL (Treatments)",
        "label": "ChEMBL",
        "type": "Treatment",
        "link_building_method": "ChEMBLInlineLink",
        "link_template": "https://www.ebi.ac.uk/chembl/compound_report_card/ChEMBL_ID",
    },
     {
        "id": 1,
        "name": "PubChem (Treatments)",
        "label": "PubChem",
        "type": "Treatment",
        "link_building_method": "PubChemInlineLink",
        "link_template": "https://pubchem.ncbi.nlm.nih.gov/compound/PubChem_ID",
    }
]
