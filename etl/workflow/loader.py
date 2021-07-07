import luigi
from luigi.contrib.spark import SparkSubmitTask

from etl.constants import Constants
from etl.jobs.load.database_manager import copy_all_tsv_to_database
from etl.workflow.transformer import TransformPatient, TransformDiagnosis, TransformEthnicity, TransformProviderType, \
    TransformProviderGroup, TransformModel, TransformPublicationGroup, TransformTissue, TransformTumourType, \
    TransformPatientSample, TransformEngraftmentSite, TransformEngraftmentType, TransformEngraftmentMaterial, \
    TransformPatientSnapshot, TransformQualityAssurance, TransformXenograftSample, TransformEngraftmentSampleState, \
    TransformEngraftmentSampleType, TransformAccessibilityGroup, TransformContactPeople, TransformContactForm, \
    TransformSourceDatabase, TransformHostStrain, TransformProjectGroup, TransformTreatment


class ParquetToTsv(SparkSubmitTask):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()
    name = luigi.Parameter()

    app = 'etl/jobs/util/parquet_to_tsv_converter.py'

    def requires(self):
        if Constants.DIAGNOSIS_ENTITY == self.name:
            return TransformDiagnosis(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.ETHNICITY_ENTITY == self.name:
            return TransformEthnicity(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.PATIENT_ENTITY == self.name:
            return TransformPatient(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.PROVIDER_TYPE_ENTITY == self.name:
            return TransformProviderType(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.PROVIDER_GROUP_ENTITY == self.name:
            return TransformProviderGroup(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.PUBLICATION_GROUP_ENTITY == self.name:
            return TransformPublicationGroup(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.MODEL_ENTITY == self.name:
            return TransformModel(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.CONTACT_PEOPLE_ENTITY == self.name:
            return TransformContactPeople(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.CONTACT_FORM_ENTITY == self.name:
            return TransformContactForm(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.SOURCE_DATABASE_ENTITY == self.name:
            return TransformSourceDatabase(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.QUALITY_ASSURANCE_ENTITY == self.name:
            return TransformQualityAssurance(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.TISSUE_ENTITY == self.name:
            return TransformTissue(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.TUMOUR_TYPE_ENTITY == self.name:
            return TransformTumourType(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.PATIENT_SAMPLE_ENTITY == self.name:
            return TransformPatientSample(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.XENOGRAFT_SAMPLE_ENTITY == self.name:
            return TransformXenograftSample(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.PATIENT_SNAPSHOT_ENTITY == self.name:
            return TransformPatientSnapshot(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.ENGRAFTMENT_SITE_ENTITY == self.name:
            return TransformEngraftmentSite(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.ENGRAFTMENT_TYPE_ENTITY == self.name:
            return TransformEngraftmentType(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.ENGRAFTMENT_MATERIAL_ENTITY == self.name:
            return TransformEngraftmentMaterial(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.ENGRAFTMENT_SAMPLE_STATE_ENTITY == self.name:
            return TransformEngraftmentSampleState(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.ENGRAFTMENT_SAMPLE_TYPE_ENTITY == self.name:
            return TransformEngraftmentSampleType(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.ACCESSIBILITY_GROUP_ENTITY == self.name:
            return TransformAccessibilityGroup(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.HOST_STRAIN_ENTITY == self.name:
            return TransformHostStrain(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.PROJECT_GROUP_ENTITY == self.name:
            return TransformProjectGroup(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.TREATMENT_ENTITY == self.name:
            return TransformTreatment(self.data_dir, self.providers, self.data_dir_out)

    def app_options(self):
        return [
            self.input().path,
            self.output().path
        ]

    def output(self):
        return luigi.LocalTarget("{0}/{1}/{2}".format(self.data_dir_out, Constants.DATABASE_FORMATTED, self.name))


class Load(luigi.Task):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()

    def requires(self):
        return [
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.DIAGNOSIS_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.ETHNICITY_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.PATIENT_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.PROVIDER_TYPE_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.PROVIDER_GROUP_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.PUBLICATION_GROUP_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.MODEL_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.CONTACT_PEOPLE_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.CONTACT_FORM_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.SOURCE_DATABASE_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.QUALITY_ASSURANCE_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.TISSUE_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.TUMOUR_TYPE_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.PATIENT_SAMPLE_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.XENOGRAFT_SAMPLE_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.PATIENT_SNAPSHOT_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.ENGRAFTMENT_SITE_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.ENGRAFTMENT_TYPE_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.ENGRAFTMENT_MATERIAL_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.ENGRAFTMENT_SAMPLE_STATE_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.ENGRAFTMENT_SAMPLE_TYPE_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.ACCESSIBILITY_GROUP_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.HOST_STRAIN_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.PROJECT_GROUP_ENTITY),
            ParquetToTsv(self.data_dir, self.providers, self.data_dir_out, Constants.TREATMENT_ENTITY)
        ]

    def run(self):
        copy_all_tsv_to_database(self.data_dir_out)
        with self.output().open('w') as out_file:
            out_file.write("data loaded")

    def output(self):
        return luigi.LocalTarget("{0}/log.txt".format(self.data_dir_out))


if __name__ == "__main__":
    luigi.run()
