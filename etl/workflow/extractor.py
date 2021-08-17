import luigi

from etl.constants import Constants
from etl.workflow.spark_reader import get_tasks_to_run, get_tsv_extraction_task_by_module


class Extract(luigi.WrapperTask):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()

    def requires(self):
        tasks = get_tasks_to_run(self.data_dir, self.providers, self.data_dir_out)
        yield tasks


class ExtractModule(luigi.Task):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()
    module_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget("{0}/{1}/{2}".format(self.data_dir_out, Constants.RAW_DIRECTORY, self.module_name))

    def requires(self):
        # return Extract(self.data_dir, self.providers, self.data_dir_out)
        return get_tsv_extraction_task_by_module(
            self.data_dir, self.providers, self.data_dir_out, self.module_name)


class ExtractPatient(ExtractModule):
    module_name = Constants.PATIENT_MODULE


class ExtractSample(ExtractModule):
    module_name = Constants.SAMPLE_MODULE


class ExtractSharing(ExtractModule):
    module_name = Constants.SHARING_MODULE


class ExtractModel(ExtractModule):
    module_name = Constants.MODEL_MODULE


class ExtractModelValidation(ExtractModule):
    module_name = Constants.MODEL_VALIDATION_MODULE


class ExtractSamplePlatform(ExtractModule):
    module_name = Constants.SAMPLE_PLATFORM_MODULE


class ExtractMolecularMetadataSample(ExtractModule):
    module_name = Constants.MOLECULAR_DATA_SAMPLE_MODULE


class ExtractMolecularMetadataPlatform(ExtractModule):
    module_name = Constants.MOLECULAR_DATA_PLATFORM_MODULE


class ExtractMolecularMetadataPlatformWeb(ExtractModule):
    module_name = Constants.MOLECULAR_DATA_PLATFORM_WEB_MODULE


class ExtractDrugDosing(ExtractModule):
    module_name = Constants.DRUG_DOSING_MODULE


class ExtractPatientTreatment(ExtractModule):
    module_name = Constants.PATIENT_TREATMENT_MODULE


class ExtractCna(ExtractModule):
    module_name = Constants.CNA_MODULE


class ExtractCytogenetics(ExtractModule):
    module_name = Constants.CYTOGENETICS_MODULE


class ExtractExpression(ExtractModule):
    module_name = Constants.EXPRESSION_MODULE


class ExtractMutation(ExtractModule):
    module_name = Constants.MUTATION_MODULE


if __name__ == "__main__":
    luigi.run()
