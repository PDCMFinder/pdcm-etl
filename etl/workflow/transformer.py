import luigi
from luigi.contrib.spark import SparkSubmitTask

from etl.constants import Constants
from etl.workflow.extractor import ExtractPatientModuleSpark, ExtractSampleModuleSpark, ExtractSharingModuleSpark, \
    ExtractLoaderModuleSpark, ExtractModelModuleSpark, ExtractSamplePlatformModuleSpark


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
        return luigi.LocalTarget("{0}/{1}/{2}".format(
            self.data_dir_out, Constants.TRANSFORMED_DIRECTORY, self.entity_name))


class TransformDiagnosis(TransformEntity):
    requiredTasks = [
            ExtractPatientModuleSpark(),
            ExtractSampleModuleSpark()
        ]
    entity_name = Constants.DIAGNOSIS_ENTITY


class TransformEthnicity(TransformEntity):
    requiredTasks = [
        ExtractPatientModuleSpark()
    ]
    entity_name = Constants.ETHNICITY_ENTITY


class TransformProviderType(TransformEntity):
    requiredTasks = [
        ExtractSharingModuleSpark()
    ]
    entity_name = Constants.PROVIDER_TYPE_ENTITY


class TransformProviderGroup(TransformEntity):
    requiredTasks = [
        ExtractSharingModuleSpark(),
        ExtractLoaderModuleSpark(),
        TransformProviderType()
    ]
    entity_name = Constants.PROVIDER_GROUP_ENTITY


class TransformPatient(TransformEntity):
    requiredTasks = [
        ExtractPatientModuleSpark(),
        TransformDiagnosis(),
        TransformEthnicity(),
        TransformProviderGroup()
    ]
    entity_name = Constants.PATIENT_ENTITY


class TransformPublicationGroup(TransformEntity):
    requiredTasks = [
        ExtractModelModuleSpark()
    ]
    entity_name = Constants.PUBLICATION_GROUP_ENTITY


class TransformModel(TransformEntity):
    requiredTasks = [
        ExtractModelModuleSpark(),
        TransformPublicationGroup()
    ]
    entity_name = Constants.MODEL_ENTITY


class TransformTissue(TransformEntity):
    requiredTasks = [
        ExtractSampleModuleSpark()
    ]
    entity_name = Constants.TISSUE_ENTITY


class TransformTumourType(TransformEntity):
    requiredTasks = [
        ExtractSampleModuleSpark()
    ]
    entity_name = Constants.TUMOUR_TYPE_ENTITY


class TransformPatientSample(TransformEntity):
    requiredTasks = [
        ExtractSampleModuleSpark(),
        TransformDiagnosis(),
        TransformTissue(),
        TransformTumourType(),
        TransformModel(),
        ExtractSamplePlatformModuleSpark()
    ]
    entity_name = Constants.PATIENT_SAMPLE_ENTITY


class TransformEngraftmentSite(TransformEntity):
    requiredTasks = [
        ExtractModelModuleSpark()
    ]
    entity_name = Constants.ENGRAFTMENT_SITE_ENTITY


class TransformEngraftmentType(TransformEntity):
    requiredTasks = [
        ExtractModelModuleSpark()
    ]
    entity_name = Constants.ENGRAFTMENT_TYPE_ENTITY


class TransformEngraftmentMaterial(TransformEntity):
    requiredTasks = [
        ExtractModelModuleSpark()
    ]
    entity_name = Constants.ENGRAFTMENT_MATERIAL_ENTITY


if __name__ == "__main__":
    luigi.run()
