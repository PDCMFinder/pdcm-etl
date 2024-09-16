import pytest
from etl.jobs.transformation.treatment_type_helper_transformer_job import calculate_type


@pytest.mark.parametrize(
    "treatment_name, ancestors, expected",
    [
        ("treatment", [], []),
        ("Hormone therapy", [], ["Hormone therapy"]),
        (
            "Interleukin-2",
            ["Interleukin", "Protein", "Cytokine", "Protein, Organized by Function"],
            ["Immunotherapy"],
        ),
        (
            "Steroidal Aromatase Inhibitor",
            [
                "Antineoplastic Agent",
                "Hormone Antagonist",
                "Antineoplastic Enzyme Inhibitor",
                "Aromatase Inhibitor",
                "Antineoplastic Protein Inhibitor",
                "Hormone Therapy Agent",
                "Antineoplastic Hormonal/Endocrine Agent",
                "Enzyme Inhibitor",
                "Targeted Therapy Agent",
                "Antiestrogen",
                "Signal Transduction Inhibitor",
            ],
            ["Hormone therapy", "Targeted Therapy"],
        ),
        (
            "Akt Inhibitor MK2206",
            [
                "Antineoplastic Agent",
                "Antineoplastic Enzyme Inhibitor",
                "Antineoplastic Protein Inhibitor",
                "Serine/Threonine Kinase Inhibitor",
                "AKT Inhibitor",
                "Enzyme Inhibitor",
                "Signal Transduction Inhibitor",
                "Angiogenesis Inhibitor",
                "Protein Kinase Inhibitor",
            ],
            ["Targeted Therapy"],
        ),
        (
            "Nilotinib",
            [
                "PDGFR-targeting Agent",
                "Antineoplastic Agent",
                "Tyrosine Kinase Inhibitor",
                "Antineoplastic Enzyme Inhibitor",
                "c-KIT Inhibitor",
                "cKIT-targeting Agent",
                "Antineoplastic Protein Inhibitor",
                "Enzyme Inhibitor",
                "Targeted Therapy Agent",
                "Signal Transduction Inhibitor",
                "BCR-ABL Inhibitor",
                "Protein Kinase Inhibitor",
                "PDGFR Inhibitor",
            ],
            ["Targeted Therapy"],
        ),
        (
            "Lenalidomide",
            [
                "Indole Compound",
                "Immunomodulatory Imide Drug",
                "Antineoplastic Agent",
                "Immunotherapeutic Agent",
                "Organic Chemical",
                "Angiogenesis Inhibitor",
                "Ring Compound",
                "Aromatic CompoundsvAntineoplastic Immunomodulating Agent",
            ],
            ["Immunotherapy"],
        ),
        ("Chemotherapy", [], ["Chemotherapy"]),
        (
            "lumpectomy",
            [
                "Breast Cancer Therapeutic Procedure",
                "Breast Conservation Treatment",
                "Cancer Therapeutic Procedure",
            ],
            ["Surgery"],
        ),
        (
            "rucaparib",
            [
                "Poly (ADP-Ribose) Polymerase Inhibitor",
                "Antineoplastic Agent",
                "Enzyme Inhibitor",
                "Targeted Therapy Agent",
            ],
            ["Targeted Therapy"],  # This was classified as Radiation therapy in the db
        ),
        (
            "liposomal doxorubicin",
            [],
            ["Chemotherapy"], # Failing because there is no mapping yet
        ),
        (
            "Plitidepsin",
            [
                "Antineoplastic Agent",
                "Antineoplastic Antibiotic",
                "Protein Synthesis Inhibitor",
                "Depsipeptide Antineoplastic Antibiotic",
                "Cytotoxic Chemotherapeutic Agent",
            ],
            ["Chemotherapy"],
        ),
        (
            "lymphadenectomy",
            [],
            ["Surgery"],  # Not mapped yet
        ),
        (
            "Surgery",
            [],
            ["Surgery"],
        ),
        (
            "Biopsy",
            [],
            ["Surgery"],
        ),
        (
            "anti-hgf monoclonal antibody tak-701",
            [],
            ["Targeted Therapy"],  # Not mapped yet
        ),
        (
            "tivantinib",
            [
                "c-Met Inhibitor",
                "c-Met-targeting Agent",
                "cAntineoplastic Agent",
                "cTyrosine Kinase Inhibitor",
                "cAntineoplastic Enzyme Inhibitor",
                "cAntineoplastic Protein Inhibitor",
                "cEnzyme Inhibitor",
                "cTargeted Therapy Agent",
                "cSignal Transduction Inhibitor",
                "cProtein Kinase Inhibitor",
            ],
            ["Targeted Therapy"],
        ),
        (
            "Radiation Therapy",
            [],
            ["Radiation Therapy"],
        ),
    ],
)

def test_calculate_type_parametrized(treatment_name, ancestors, expected):
    assert calculate_type(treatment_name, ancestors) == expected
