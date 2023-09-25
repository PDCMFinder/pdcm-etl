from etl.workflow.transformer import *


def get_transformation_class_by_entity_name(entity_name):
    return entities[entity_name]


def get_all_transformation_classes():
    transformation_classes = []
    for entity in entities:
        transformation_classes.append(get_transformation_class_by_entity_name(entity))
    return transformation_classes


entities = {
    Constants.ETHNICITY_ENTITY: TransformEthnicity(),
    Constants.PATIENT_ENTITY: TransformPatient(),
    Constants.PROVIDER_TYPE_ENTITY: TransformProviderType(),
    Constants.PROVIDER_GROUP_ENTITY: TransformProviderGroup(),
    Constants.PUBLICATION_GROUP_ENTITY: TransformPublicationGroup(),
    Constants.MODEL_INFORMATION_ENTITY: TransformModel(),
    Constants.LICENSE_ENTITY: TransformLicense(),
    Constants.CELL_MODEL_ENTITY: TransformCellModel(),
    Constants.CELL_SAMPLE_ENTITY: TransformCellSample(),
    Constants.CONTACT_PEOPLE_ENTITY: TransformContactPeople(),
    Constants.CONTACT_FORM_ENTITY: TransformContactForm(),
    Constants.SOURCE_DATABASE_ENTITY: TransformSourceDatabase(),
    Constants.QUALITY_ASSURANCE_ENTITY: TransformQualityAssurance(),
    Constants.TISSUE_ENTITY: TransformTissue(),
    Constants.TUMOUR_TYPE_ENTITY: TransformTumourType(),
    Constants.PATIENT_SAMPLE_ENTITY: TransformPatientSample(),
    Constants.XENOGRAFT_SAMPLE_ENTITY: TransformXenograftSample(),
    Constants.ENGRAFTMENT_SITE_ENTITY: TransformEngraftmentSite(),
    Constants.ENGRAFTMENT_TYPE_ENTITY: TransformEngraftmentType(),
    Constants.ENGRAFTMENT_SAMPLE_STATE_ENTITY: TransformEngraftmentSampleState(),
    Constants.ENGRAFTMENT_SAMPLE_TYPE_ENTITY: TransformEngraftmentSampleType(),
    Constants.ACCESSIBILITY_GROUP_ENTITY: TransformAccessibilityGroup(),
    Constants.HOST_STRAIN_ENTITY: TransformHostStrain(),
    Constants.PROJECT_GROUP_ENTITY: TransformProjectGroup(),
    Constants.TREATMENT_ENTITY: TransformTreatment(),
    Constants.RESPONSE_ENTITY: TransformResponse(),
    Constants.RESPONSE_CLASSIFICATION_ENTITY: TransformResponseClassification(),
    Constants.MOLECULAR_CHARACTERIZATION_TYPE_ENTITY: TransformMolecularCharacterizationType(),
    Constants.MOLECULAR_CHARACTERIZATION_ENTITY: TransformMolecularCharacterization(),
    Constants.PLATFORM_ENTITY: TransformPlatform(),
    Constants.CNA_MOLECULAR_DATA_ENTITY: TransformCnaMolecularData(),
    Constants.CYTOGENETICS_MOLECULAR_DATA_ENTITY: TransformCytogeneticsMolecularData(),
    Constants.EXPRESSION_MOLECULAR_DATA_ENTITY: TransformExpressionMolecularData(),
    Constants.MUTATION_MEASUREMENT_DATA_ENTITY: TransformMutationMeasurementData(),
    Constants.GENE_MARKER_ENTITY: TransformGeneMarker(),
    Constants.IMAGE_STUDY_ENTITY: TransformImageStudy(),
    Constants.MODEL_IMAGE_ENTITY: TransformModelImage(),
    Constants.ONTOLOGY_TERM_DIAGNOSIS_ENTITY: TransformOntologyTermDiagnosis(),
    Constants.ONTOLOGY_TERM_TREATMENT_ENTITY: TransformOntologyTermTreatment(),
    Constants.ONTOLOGY_TERM_REGIMEN_ENTITY: TransformOntologyTermRegimen(),
    Constants.REGIMENT_TO_TREATMENT_ENTITY: TransformRegimenToTreatment(),
    Constants.XENOGRAFT_MODEL_SPECIMEN_ENTITY: TransformXenograftModelSpecimen(),
    Constants.SAMPLE_TO_ONTOLOGY_ENTITY: TransformSampleToOntology(),
    Constants.TREATMENT_PROTOCOL_ENTITY: TransformTreatmentProtocol(),
    Constants.TREATMENT_COMPONENT_ENTITY: TransformTreatmentComponent(),
    Constants.TREATMENT_TO_ONTOLOGY_ENTITY: TransformTreatmentToOntology(),
    Constants.REGIMEN_TO_ONTOLOGY_ENTITY: TransformRegimenToOntology(),
    Constants.SEARCH_INDEX_ENTITY: TransformSearchIndex(),
    Constants.SEARCH_FACET_ENTITY: TransformSearchFacet(),
    Constants.MOLECULAR_DATA_RESTRICTION_ENTITY: TransformMolecularDataRestriction(),
    Constants.AVAILABLE_MOLECULAR_DATA_COLUMNS_ENTITY: TransformAvailableMolecularDataColumns()
}
