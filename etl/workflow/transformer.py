import luigi
from luigi.contrib.spark import SparkSubmitTask

from etl.constants import Constants
from etl.workflow.config import PdcmConfig
from etl.workflow.extractor import ExtractPatient, ExtractSharing, ExtractModel, \
    ExtractModelValidation, ExtractSample, ExtractDrugDosing, ExtractPatientTreatment, \
    ExtractCna, ExtractCytogenetics, ExtractExpression, ExtractMutation, ExtractMolecularMetadataPlatform, \
    ExtractMolecularMetadataSample, ExtractSource, ExtractGeneMarker, ExtractOntology, ExtractMappingDiagnosis, \
    ExtractCellModel, ExtractOntolia, ExtractMappingTreatment


class TransformEntity(luigi.contrib.spark.SparkSubmitTask):
    """
        Creates a dataframe ready with all the information needed for a specific entity.
    """
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()

    """ Luigi tasks that are required for this task to be executed """
    requiredTasks = []

    """ Entity to be processed """
    entity_name = None

    app = 'etl/jobs/transformation/spark_transformation_job.py'

    def requires(self):
        return self.requiredTasks

    def app_options(self):
        spark_input_parameters = [self.entity_name]

        """ The inputs (outputs of the dependencies) will be input parameters for the spark job """
        for dependency_output in self.input():
            spark_input_parameters.append(dependency_output.path)

        """ The last parameter of the spark job is the output directory """
        spark_input_parameters.append(self.output().path)

        return spark_input_parameters

    def output(self):
        return PdcmConfig().get_target("{0}/{1}/{2}".format(
            self.data_dir_out, Constants.TRANSFORMED_DIRECTORY, self.entity_name))


class TransformDiagnosis(TransformEntity):
    requiredTasks = [
        ExtractPatient(),
        ExtractSample()
    ]
    entity_name = Constants.DIAGNOSIS_ENTITY


class TransformEthnicity(TransformEntity):
    requiredTasks = [
        ExtractPatient()
    ]
    entity_name = Constants.ETHNICITY_ENTITY


class TransformProviderType(TransformEntity):
    requiredTasks = [
        ExtractSource()
    ]
    entity_name = Constants.PROVIDER_TYPE_ENTITY


class TransformProjectGroup(TransformEntity):
    requiredTasks = [
        ExtractSource()
    ]
    entity_name = Constants.PROJECT_GROUP_ENTITY


class TransformProviderGroup(TransformEntity):
    requiredTasks = [
        ExtractSource(),
        TransformProviderType(),
        TransformProjectGroup()
    ]
    entity_name = Constants.PROVIDER_GROUP_ENTITY


class TransformPatient(TransformEntity):
    requiredTasks = [
        ExtractPatient(),
        TransformDiagnosis(),
        TransformEthnicity(),
        TransformProviderGroup()
    ]
    entity_name = Constants.PATIENT_ENTITY


class TransformPublicationGroup(TransformEntity):
    requiredTasks = [
        ExtractModel()
    ]
    entity_name = Constants.PUBLICATION_GROUP_ENTITY


class TransformContactPeople(TransformEntity):
    requiredTasks = [
        ExtractSharing()
    ]
    entity_name = Constants.CONTACT_PEOPLE_ENTITY


class TransformContactForm(TransformEntity):
    requiredTasks = [
        ExtractSharing()
    ]
    entity_name = Constants.CONTACT_FORM_ENTITY


class TransformSourceDatabase(TransformEntity):
    requiredTasks = [
        ExtractSharing()
    ]
    entity_name = Constants.SOURCE_DATABASE_ENTITY


class TransformAccessibilityGroup(TransformEntity):
    requiredTasks = [
        ExtractSharing(),
        ExtractSharing()
    ]
    entity_name = Constants.ACCESSIBILITY_GROUP_ENTITY


class TransformModel(TransformEntity):
    requiredTasks = [
        ExtractModel(),
        ExtractCellModel(),
        ExtractSharing(),
        TransformPublicationGroup(),
        TransformAccessibilityGroup(),
        TransformContactPeople(),
        TransformContactForm(),
        TransformSourceDatabase()
    ]
    entity_name = Constants.MODEL_INFORMATION_ENTITY


class TransformCellModel(TransformEntity):
    requiredTasks = [
        ExtractCellModel(),
        TransformModel()
    ]
    entity_name = Constants.CELL_MODEL_ENTITY


class TransformQualityAssurance(TransformEntity):
    requiredTasks = [
        ExtractModelValidation(),
        TransformModel()
    ]
    entity_name = Constants.QUALITY_ASSURANCE_ENTITY


class TransformTissue(TransformEntity):
    requiredTasks = [
        ExtractSample()
    ]
    entity_name = Constants.TISSUE_ENTITY


class TransformTumourType(TransformEntity):
    requiredTasks = [
        ExtractSample()
    ]
    entity_name = Constants.TUMOUR_TYPE_ENTITY


class TransformPatientSample(TransformEntity):
    requiredTasks = [
        ExtractSample(),
        TransformDiagnosis(),
        TransformTissue(),
        TransformTumourType(),
        TransformModel()
    ]
    entity_name = Constants.PATIENT_SAMPLE_ENTITY


class TransformPatientSnapshot(TransformEntity):
    requiredTasks = [
        ExtractSample(),
        TransformPatientSample(),
        TransformPatient()
    ]
    entity_name = Constants.PATIENT_SNAPSHOT_ENTITY


class TransformEngraftmentSite(TransformEntity):
    requiredTasks = [
        ExtractModel()
    ]
    entity_name = Constants.ENGRAFTMENT_SITE_ENTITY


class TransformEngraftmentType(TransformEntity):
    requiredTasks = [
        ExtractModel()
    ]
    entity_name = Constants.ENGRAFTMENT_TYPE_ENTITY


class TransformEngraftmentSampleState(TransformEntity):
    requiredTasks = [
        ExtractModel()
    ]
    entity_name = Constants.ENGRAFTMENT_SAMPLE_STATE_ENTITY


class TransformEngraftmentSampleType(TransformEntity):
    requiredTasks = [
        ExtractModel()
    ]
    entity_name = Constants.ENGRAFTMENT_SAMPLE_TYPE_ENTITY


class TransformHostStrain(TransformEntity):
    requiredTasks = [
        ExtractModel()
    ]
    entity_name = Constants.HOST_STRAIN_ENTITY


class TransformResponse(TransformEntity):
    requiredTasks = [
        ExtractDrugDosing(),
        ExtractPatientTreatment()
    ]
    entity_name = Constants.RESPONSE_ENTITY


class TransformResponseClassification(TransformEntity):
    requiredTasks = [
        ExtractDrugDosing(),
        ExtractPatientTreatment()
    ]
    entity_name = Constants.RESPONSE_CLASSIFICATION_ENTITY


class TransformTreatmentProtocol(TransformEntity):
    requiredTasks = [
        ExtractDrugDosing(),
        ExtractPatientTreatment(),
        TransformModel(),
        TransformPatient(),
        TransformResponse(),
        TransformResponseClassification()
    ]
    entity_name = Constants.TREATMENT_PROTOCOL_ENTITY


class TransformTreatmentAndComponentHelper(TransformEntity):
    requiredTasks = [
        TransformTreatmentProtocol()
    ]
    entity_name = Constants.TREATMENT_AND_COMPONENT_HELPER_ENTITY


class TransformTreatment(TransformEntity):
    requiredTasks = [
        TransformTreatmentAndComponentHelper()
    ]
    entity_name = Constants.TREATMENT_ENTITY


class TransformMolecularCharacterizationType(TransformEntity):
    entity_name = Constants.MOLECULAR_CHARACTERIZATION_TYPE_ENTITY


class TransformPlatform(TransformEntity):
    requiredTasks = [
        ExtractMolecularMetadataPlatform(),
        TransformProviderGroup()
    ]
    entity_name = Constants.PLATFORM_ENTITY


class TransformXenograftSample(TransformEntity):
    requiredTasks = [
        ExtractMolecularMetadataSample(),
        TransformHostStrain(),
        TransformModel(),
        TransformPlatform()
    ]
    entity_name = Constants.XENOGRAFT_SAMPLE_ENTITY


class TransformCellSample(TransformEntity):
    requiredTasks = [
        ExtractMolecularMetadataSample(),
        TransformModel(),
        TransformPlatform()
    ]
    entity_name = Constants.CELL_SAMPLE_ENTITY


class TransformXenograftModelSpecimen(TransformEntity):
    requiredTasks = [
        ExtractModel(),
        TransformEngraftmentSite(),
        TransformEngraftmentType(),
        TransformEngraftmentSampleType(),
        TransformEngraftmentSampleState(),
        TransformHostStrain(),
        TransformModel()
    ]
    entity_name = Constants.XENOGRAFT_MODEL_SPECIMEN_ENTITY


class TransformMolecularCharacterization(TransformEntity):
    requiredTasks = [
        ExtractMolecularMetadataSample(),
        TransformPlatform(),
        TransformPatientSample(),
        TransformXenograftSample(),
        TransformCellSample(),
        TransformMolecularCharacterizationType()
    ]
    entity_name = Constants.MOLECULAR_CHARACTERIZATION_ENTITY


class TransformGeneMarker(TransformEntity):
    requiredTasks = [
        ExtractGeneMarker()
    ]
    entity_name = Constants.GENE_MARKER_ENTITY


class TransformCnaMolecularData(TransformEntity):
    requiredTasks = [
        TransformMolecularCharacterization(),
        ExtractCna(),
        TransformGeneMarker()
    ]
    entity_name = Constants.CNA_MOLECULAR_DATA_ENTITY


class TransformCytogeneticsMolecularData(TransformEntity):
    requiredTasks = [
        ExtractCytogenetics(),
        TransformMolecularCharacterization(),
        TransformGeneMarker()
    ]
    entity_name = Constants.CYTOGENETICS_MOLECULAR_DATA_ENTITY


class TransformExpressionMolecularData(TransformEntity):
    requiredTasks = [
        TransformMolecularCharacterization(),
        ExtractExpression(),
        TransformGeneMarker()
    ]
    entity_name = Constants.EXPRESSION_MOLECULAR_DATA_ENTITY


class TransformMutationMarker(TransformEntity):
    requiredTasks = [
        ExtractMutation(),
        TransformGeneMarker()
    ]
    entity_name = Constants.MUTATION_MARKER_ENTITY


class TransformMutationMeasurementData(TransformEntity):
    requiredTasks = [
        ExtractMutation(),
        TransformMutationMarker(),
        TransformMolecularCharacterization()
    ]
    entity_name = Constants.MUTATION_MEASUREMENT_DATA_ENTITY


class TransformOntologyTermDiagnosis(TransformEntity):
    requiredTasks = [
        ExtractOntology()
    ]
    entity_name = Constants.ONTOLOGY_TERM_DIAGNOSIS_ENTITY


class TransformOntologyTermTreatment(TransformEntity):
    requiredTasks = [
        ExtractOntology()
    ]
    entity_name = Constants.ONTOLOGY_TERM_TREATMENT_ENTITY


class TransformOntologyTermRegimen(TransformEntity):
    requiredTasks = [
        ExtractOntology()
    ]
    entity_name = Constants.ONTOLOGY_TERM_REGIMEN_ENTITY


class TransformRegimenToTreatment(TransformEntity):
    requiredTasks = [
        ExtractOntolia(),
        TransformOntologyTermRegimen(),
        TransformOntologyTermTreatment(),
    ]
    entity_name = Constants.REGIMENT_TO_TREATMENT_ENTITY


class TransformSampleToOntology(TransformEntity):
    requiredTasks = [
        TransformModel(),
        TransformPatientSample(),
        TransformDiagnosis(),
        TransformTumourType(),
        TransformTissue(),
        TransformOntologyTermDiagnosis(),
        ExtractMappingDiagnosis()
    ]
    entity_name = Constants.SAMPLE_TO_ONTOLOGY_ENTITY


class TransformTreatmentToOntology(TransformEntity):
    requiredTasks = [
        TransformTreatment(),
        TransformOntologyTermTreatment(),
        ExtractMappingTreatment()
    ]
    entity_name = Constants.TREATMENT_TO_ONTOLOGY_ENTITY


class TransformRegimenToOntology(TransformEntity):
    requiredTasks = [
        TransformTreatment(),
        TransformOntologyTermRegimen(),
        ExtractMappingTreatment()
    ]
    entity_name = Constants.REGIMEN_TO_ONTOLOGY_ENTITY


class TransformTreatmentComponent(TransformEntity):
    requiredTasks = [
        TransformTreatmentAndComponentHelper(),
        TransformTreatment()
    ]
    entity_name = Constants.TREATMENT_COMPONENT_ENTITY


class TransformTreatmentHarmonisationHelper(TransformEntity):
    requiredTasks = [
        TransformPatientSample(),
        TransformPatientSnapshot(),
        TransformTreatmentProtocol(),
        TransformTreatmentComponent(),
        TransformTreatment(),
        TransformTreatmentToOntology(),
        TransformRegimenToTreatment(),
        TransformRegimenToOntology(),
        TransformOntologyTermTreatment(),
        TransformOntologyTermRegimen()
    ]
    entity_name = Constants.TREATMENT_HARMONISATION_HELPER_ENTITY


class TransformSearchIndex(TransformEntity):
    requiredTasks = [
        TransformModel(),
        TransformMolecularCharacterization(),
        TransformMolecularCharacterizationType(),
        TransformPatientSample(),
        TransformPatientSnapshot(),
        TransformPatient(),
        TransformEthnicity(),
        TransformXenograftSample(),
        TransformCellModel(),
        TransformTumourType(),
        TransformTissue(),
        TransformGeneMarker(),
        TransformMutationMarker(),
        TransformMutationMeasurementData(),
        TransformCnaMolecularData(),
        TransformExpressionMolecularData(),
        TransformCytogeneticsMolecularData(),
        TransformProviderGroup(),
        TransformProjectGroup(),
        TransformSampleToOntology(),
        TransformOntologyTermDiagnosis(),
        TransformTreatmentHarmonisationHelper()
    ]
    entity_name = Constants.SEARCH_INDEX_ENTITY


class TransformSearchFacet(TransformEntity):
    requiredTasks = [TransformSearchIndex()]
    entity_name = Constants.SEARCH_FACET_ENTITY


if __name__ == "__main__":
    luigi.run()
