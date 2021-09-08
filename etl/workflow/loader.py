import luigi
from luigi.contrib.spark import SparkSubmitTask
import time

from etl.constants import Constants
from etl.jobs.load.database_manager import copy_entity_to_database, get_database_connection, \
    delete_fks, delete_indexes, create_indexes, create_fks
from etl.workflow.config import PdcmConfig
from etl.workflow.transformer import TransformPatient, TransformDiagnosis, TransformEthnicity, TransformProviderType, \
    TransformProviderGroup, TransformModel, TransformPublicationGroup, TransformTissue, TransformTumourType, \
    TransformPatientSample, TransformEngraftmentSite, TransformEngraftmentType, TransformEngraftmentMaterial, \
    TransformPatientSnapshot, TransformQualityAssurance, TransformXenograftSample, TransformEngraftmentSampleState, \
    TransformEngraftmentSampleType, TransformAccessibilityGroup, TransformContactPeople, TransformContactForm, \
    TransformSourceDatabase, TransformHostStrain, TransformProjectGroup, TransformTreatment, TransformResponse, \
    TransformMolecularCharacterizationType, TransformPlatform, TransformMolecularCharacterization, \
    TransformCnaMolecularData, TransformCytogeneticsMolecularData, TransformExpressionMolecularData, \
    TransformMutationMarker


transform_classes = {
    Constants.DIAGNOSIS_ENTITY: TransformDiagnosis(),
    Constants.ETHNICITY_ENTITY: TransformEthnicity(),
    Constants.PATIENT_ENTITY: TransformPatient(),
    Constants.PROVIDER_TYPE_ENTITY: TransformProviderType(),
    Constants.PROVIDER_GROUP_ENTITY: TransformProviderGroup(),
    Constants.PUBLICATION_GROUP_ENTITY: TransformPublicationGroup(),
    Constants.MODEL_ENTITY: TransformModel(),
    Constants.CONTACT_PEOPLE_ENTITY: TransformContactPeople(),
    Constants.CONTACT_FORM_ENTITY: TransformContactForm(),
    Constants.SOURCE_DATABASE_ENTITY: TransformSourceDatabase(),
    Constants.QUALITY_ASSURANCE_ENTITY: TransformQualityAssurance(),
    Constants.TISSUE_ENTITY: TransformTissue(),
    Constants.TUMOUR_TYPE_ENTITY: TransformTumourType(),
    Constants.PATIENT_SAMPLE_ENTITY: TransformPatientSample(),
    Constants.XENOGRAFT_SAMPLE_ENTITY: TransformXenograftSample(),
    Constants.PATIENT_SNAPSHOT_ENTITY: TransformPatientSnapshot(),
    Constants.ENGRAFTMENT_SITE_ENTITY: TransformEngraftmentSite(),
    Constants.ENGRAFTMENT_TYPE_ENTITY: TransformEngraftmentType(),
    Constants.ENGRAFTMENT_MATERIAL_ENTITY: TransformEngraftmentMaterial(),
    Constants.ENGRAFTMENT_SAMPLE_STATE_ENTITY: TransformEngraftmentSampleState(),
    Constants.ENGRAFTMENT_SAMPLE_TYPE_ENTITY: TransformEngraftmentSampleType(),
    Constants.ACCESSIBILITY_GROUP_ENTITY: TransformAccessibilityGroup(),
    Constants.HOST_STRAIN_ENTITY: TransformHostStrain(),
    Constants.PROJECT_GROUP_ENTITY: TransformProjectGroup(),
    Constants.TREATMENT_ENTITY: TransformTreatment(),
    Constants.RESPONSE_ENTITY: TransformResponse(),
    Constants.MOLECULAR_CHARACTERIZATION_TYPE_ENTITY: TransformMolecularCharacterizationType(),
    Constants.MOLECULAR_CHARACTERIZATION_ENTITY: TransformMolecularCharacterization(),
    Constants.PLATFORM_ENTITY: TransformPlatform(),
    Constants.CNA_MOLECULAR_DATA_ENTITY: TransformCnaMolecularData(),
    Constants.CYTOGENETICS_MOLECULAR_DATA_ENTITY: TransformCytogeneticsMolecularData(),
    Constants.EXPRESSION_MOLECULAR_DATA_ENTITY: TransformExpressionMolecularData(),
    Constants.MUTATION_MARKER_ENTITY: TransformMutationMarker()
}


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

        elif Constants.RESPONSE_ENTITY == self.name:
            return TransformResponse(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.MOLECULAR_CHARACTERIZATION_TYPE_ENTITY == self.name:
            return TransformMolecularCharacterizationType(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.PLATFORM_ENTITY == self.name:
            return TransformPlatform(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.MOLECULAR_CHARACTERIZATION_ENTITY == self.name:
            return TransformMolecularCharacterization(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.CNA_MOLECULAR_DATA_ENTITY == self.name:
            return TransformCnaMolecularData(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.CYTOGENETICS_MOLECULAR_DATA_ENTITY == self.name:
            return TransformCytogeneticsMolecularData(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.EXPRESSION_MOLECULAR_DATA_ENTITY == self.name:
            return TransformExpressionMolecularData(self.data_dir, self.providers, self.data_dir_out)

        elif Constants.MUTATION_MARKER_ENTITY == self.name:
            return TransformMutationMarker(self.data_dir, self.providers, self.data_dir_out)

    def app_options(self):
        return [
            self.input().path,
            self.name,
            self.output().path
        ]

    def output(self):
        return PdcmConfig().get_target("{0}/{1}/{2}".format(self.data_dir_out, Constants.DATABASE_FORMATTED, self.name))


class CopyEntityFromParquetToDb(luigi.Task):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()
    entity_name = luigi.Parameter()

    db_host = luigi.Parameter()
    db_port = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user = luigi.Parameter()
    db_password = luigi.Parameter()

    def requires(self):
        return ParquetToTsv(name=self.entity_name)

    def output(self):
        return PdcmConfig().get_target("{0}/{1}/{2}".format(self.data_dir_out, "database/copied", self.entity_name))

    def run(self):
        start = time.time()
        copy_entity_to_database(
            self.entity_name, self.input().path, self.db_host, self.db_port, self.db_name, self.db_user, self.db_password)
        end = time.time()
        print("Ended {0} in {1} seconds".format(self.entity_name, round(end - start, 4)))
        with self.output().open('w') as outfile:
            outfile.write("Ended in {0} seconds".format(round(end - start, 4)))


def get_all_copying_tasks():
    tasks = []
    for entity_name in transform_classes:
        tasks.append(CopyEntityFromParquetToDb(entity_name=entity_name))
    return tasks


class DeleteFksAndIndexes(luigi.Task):
    data_dir_out = luigi.Parameter()
    db_host = luigi.Parameter()
    db_port = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user = luigi.Parameter()
    db_password = luigi.Parameter()

    def output(self):
        return PdcmConfig().get_target("{0}/{1}/{2}".format(self.data_dir_out, "database", "fks_indexes"))

    def run(self):
        connection = get_database_connection(self.db_host, self.db_port, self.db_name, self.db_user, self.db_password)
        delete_fks(connection)
        delete_indexes(connection)
        with self.output().open('w') as outfile:
            outfile.write("Fks and indexes deleted")
        connection.commit()
        connection.close()


class CreateFksAndIndexes(luigi.Task):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()
    db_host = luigi.Parameter()
    db_port = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user = luigi.Parameter()
    db_password = luigi.Parameter()

    def requires(self):
        return CopyAll(self.data_dir, self.providers, self.data_dir_out)

    def output(self):
        return PdcmConfig().get_target("{0}/{1}/{2}".format(self.data_dir_out, "database", "fks_indexes"))

    def run(self):
        print("db_host:", self.db_host, "db_port", self.db_port)
        connection = get_database_connection(self.db_host, self.db_port, self.db_name, self.db_user, self.db_password)

        create_indexes(connection)
        create_fks(connection)
        with self.output().open('w') as outfile:
            outfile.write("Fks and indexes created")
        connection.commit()
        connection.close()


class CopyAll(luigi.WrapperTask):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()

    def requires(self):
        return DeleteFksAndIndexes()

    def run(self):
        yield get_all_copying_tasks()


if __name__ == "__main__":
    luigi.run()
