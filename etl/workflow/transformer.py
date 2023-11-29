import luigi
from luigi.contrib.spark import SparkSubmitTask

from etl.constants import Constants
from etl.workflow.config import PdcmConfig
from etl.workflow.extractor import ExtractImmunemarker, ExtractPatient, ExtractSharing, ExtractModel, \
    ExtractModelValidation, ExtractSample, ExtractDrugDosing, ExtractPatientTreatment, \
    ExtractCna, ExtractBiomarker, ExtractExpression, ExtractMutation, ExtractMolecularMetadataPlatform, \
    ExtractMolecularMetadataSample, ExtractSource, ExtractGeneMarker, ExtractOntology, ExtractMappingDiagnosis, \
    ExtractCellModel, ExtractOntolia, ExtractMappingTreatment, ExtractExternalResources, ExtractDownloadedResourcesData, \
    ExtractModelCharacterizationConf, ExtractImageStudy, ExtractModelImage


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

        # Exceptional case: this particular task needs an additional parameter. If set in the transformation
        # class itself the value cannot be read here. Maybe ther is a better way to do this but for now it works
        if self.entity_name == Constants.MOLECULAR_DATA_RESTRICTION_ENTITY:
            spark_input_parameters.append(self.molecular_data_restrictions)

        """ The last parameter of the spark job is the output directory """
        spark_input_parameters.append(self.output().path)

        return spark_input_parameters

    def output(self):
        return PdcmConfig().get_target("{0}/{1}/{2}".format(
            self.data_dir_out, Constants.TRANSFORMED_DIRECTORY, self.entity_name))


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
        TransformEthnicity(),
        TransformProviderGroup()
    ]
    entity_name = Constants.PATIENT_ENTITY


class TransformPublicationGroup(TransformEntity):
    requiredTasks = [
        ExtractModel(),
        ExtractCellModel()
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


class TransformLicense(TransformEntity):
    requiredTasks = []
    entity_name = Constants.LICENSE_ENTITY


class TransformModel(TransformEntity):
    requiredTasks = [
        ExtractModel(),
        ExtractCellModel(),
        ExtractSharing(),
        TransformPublicationGroup(),
        TransformAccessibilityGroup(),
        TransformContactPeople(),
        TransformContactForm(),
        TransformSourceDatabase(),
        TransformLicense()
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
        TransformPatient(),
        TransformTissue(),
        TransformTumourType(),
        TransformModel()
    ]
    entity_name = Constants.PATIENT_SAMPLE_ENTITY


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
        TransformMolecularCharacterizationType(),
        ExtractExternalResources()
    ]
    entity_name = Constants.MOLECULAR_CHARACTERIZATION_ENTITY


class TransformGeneMarker(TransformEntity):
    requiredTasks = [
        ExtractGeneMarker()
    ]
    entity_name = Constants.GENE_MARKER_ENTITY


class TransformGeneHelper(TransformEntity):
    requiredTasks = [
        ExtractCna(),
        ExtractBiomarker(),
        ExtractExpression(),
        ExtractMutation(),
        TransformGeneMarker()
    ]
    entity_name = Constants.GENE_HELPER_ENTITY


class TransformImageStudy(TransformEntity):
    requiredTasks = [
        ExtractImageStudy()
    ]
    entity_name = Constants.IMAGE_STUDY_ENTITY


class TransformModelImage(TransformEntity):
    requiredTasks = [
        ExtractModelImage(),
        TransformModel()
    ]
    entity_name = Constants.MODEL_IMAGE_ENTITY


class TransformInitialCnaMolecularData(TransformEntity):
    requiredTasks = [
        ExtractCna(),
        TransformMolecularCharacterization(),
    ]
    entity_name = Constants.INITIAL_CNA_MOLECULAR_DATA_ENTITY


class TransformInitialBiomarkerMolecularData(TransformEntity):
    requiredTasks = [
        ExtractBiomarker(),
        TransformMolecularCharacterization(),
    ]
    entity_name = Constants.INITIAL_BIOMARKER_MOLECULAR_DATA_ENTITY


class TransformInitialExpressionMolecularData(TransformEntity):
    requiredTasks = [
        ExtractExpression(),
        TransformMolecularCharacterization(),
    ]
    entity_name = Constants.INITIAL_EXPRESSION_MOLECULAR_DATA_ENTITY


class TransformInitialMutationMolecularData(TransformEntity):
    requiredTasks = [
        ExtractMutation(),
        TransformMolecularCharacterization(),
    ]
    entity_name = Constants.INITIAL_MUTATION_MOLECULAR_DATA_ENTITY


class TransformCnaMolecularData(TransformEntity):
    requiredTasks = [
        TransformInitialCnaMolecularData(),
        ExtractExternalResources(),
        ExtractDownloadedResourcesData(),
        TransformGeneHelper()
    ]
    entity_name = Constants.CNA_MOLECULAR_DATA_ENTITY


class TransformBiomarkerMolecularData(TransformEntity):
    requiredTasks = [
        TransformInitialBiomarkerMolecularData(),
        ExtractExternalResources(),
        ExtractDownloadedResourcesData(),
        TransformGeneHelper()
    ]
    entity_name = Constants.BIOMARKER_MOLECULAR_DATA_ENTITY


class TransformImmunemarkerMolecularData(TransformEntity):
    requiredTasks = [
        ExtractImmunemarker(),
        TransformMolecularCharacterization(),
    ]
    entity_name = Constants.IMMUNEMARKER_MOLECULAR_DATA_ENTITY


class TransformExpressionMolecularData(TransformEntity):
    requiredTasks = [
        TransformInitialExpressionMolecularData(),
        ExtractExternalResources(),
        ExtractDownloadedResourcesData(),
        TransformGeneHelper()
    ]
    entity_name = Constants.EXPRESSION_MOLECULAR_DATA_ENTITY


class TransformMutationMeasurementData(TransformEntity):
    requiredTasks = [
        TransformInitialMutationMolecularData(),
        ExtractExternalResources(),
        ExtractDownloadedResourcesData(),
        TransformGeneHelper()
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
        TransformTreatmentProtocol(),
        TransformTreatmentComponent(),
        TransformTreatment(),
        TransformTreatmentToOntology(),
        TransformRegimenToTreatment(),
        TransformRegimenToOntology(),
        TransformOntologyTermTreatment(),
        TransformOntologyTermRegimen(),
        TransformResponse()
    ]
    entity_name = Constants.TREATMENT_HARMONISATION_HELPER_ENTITY


class TransformSearchIndexPatientSample(TransformEntity):
    requiredTasks = [
        TransformPatientSample(),
        TransformPatient(),
        TransformSampleToOntology(),
        TransformOntologyTermDiagnosis(),
    ]
    entity_name = Constants.SEARCH_INDEX_PATIENT_SAMPLE_ENTITY


class TransformSearchIndexMolecularCharacterization(TransformEntity):
    requiredTasks = [
        TransformMolecularCharacterization(),
        TransformPatientSample(),
        TransformXenograftSample(),
        TransformCellSample()
    ]
    entity_name = Constants.SEARCH_INDEX_MOLECULAR_CHARACTERIZATION_ENTITY


class TransformModelMetadata(TransformEntity):
    requiredTasks = [
        TransformModel(),
        TransformSearchIndexPatientSample(),
        TransformXenograftModelSpecimen(),
        TransformQualityAssurance(),
        TransformModelImage(),
        TransformTreatmentHarmonisationHelper(),
        TransformSearchIndexMolecularCharacterization()
    ]
    entity_name = Constants.MODEL_METADATA


class TransformSearchIndexMolecularData(TransformEntity):
    requiredTasks = [
        TransformModelMetadata(),
        TransformSearchIndexMolecularCharacterization(),
        TransformMutationMeasurementData(),
        TransformCnaMolecularData(),
        TransformExpressionMolecularData(),
        TransformBiomarkerMolecularData(),
        TransformImmunemarkerMolecularData(),
        ExtractExternalResources(),
    ]
    entity_name = Constants.SEARCH_INDEX_MOLECULAR_DATA_ENTITY


class TransformSearchIndex(TransformEntity):
    requiredTasks = [
        TransformSearchIndexMolecularData(),
        ExtractExternalResources(),
        ExtractModelCharacterizationConf()

    ]
    entity_name = Constants.SEARCH_INDEX_ENTITY


class TransformSearchFacet(TransformEntity):
    requiredTasks = [TransformSearchIndex()]
    entity_name = Constants.SEARCH_FACET_ENTITY


class TransformMolecularDataRestriction(TransformEntity):
    requiredTasks = []
    entity_name = Constants.MOLECULAR_DATA_RESTRICTION_ENTITY
    molecular_data_restrictions = luigi.Parameter()


class TransformAvailableMolecularDataColumns(TransformEntity):
    requiredTasks = [
        TransformExpressionMolecularData(),
        TransformCnaMolecularData(),
        TransformBiomarkerMolecularData(),
        TransformMutationMeasurementData()]
    entity_name = Constants.AVAILABLE_MOLECULAR_DATA_COLUMNS_ENTITY


if __name__ == "__main__":
    luigi.run()
