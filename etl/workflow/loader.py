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
    TransformMutationMarker, TransformMutationMeasurementData, TransformGeneMarker

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
    Constants.MUTATION_MARKER_ENTITY: TransformMutationMarker(),
    Constants.MUTATION_MEASUREMENT_DATA_ENTITY: TransformMutationMeasurementData(),
    Constants.GENE_MARKER_ENTITY: TransformGeneMarker()
}


class ParquetToCsv(SparkSubmitTask):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()
    name = luigi.Parameter()

    app = 'etl/jobs/util/parquet_to_tsv_converter.py'

    def requires(self):
        return transform_classes[self.name]

    def app_options(self):
        return [
            self.input().path,
            self.name,
            self.output().path
        ]

    def output(self):
        return PdcmConfig().get_target("{0}/{1}/{2}".format(self.data_dir_out, Constants.DATABASE_FORMATTED, self.name))


class CopyEntityFromCsvToDb(luigi.Task):
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
        return ParquetToCsv(name=self.entity_name)

    def output(self):
        return PdcmConfig().get_target("{0}/{1}/{2}".format(self.data_dir_out, "database/copied", self.entity_name))

    def run(self):
        start = time.time()
        copy_entity_to_database(
            self.entity_name, self.input().path, self.db_host, self.db_port, self.db_name, self.db_user,
            self.db_password)
        end = time.time()
        print("Ended {0} in {1} seconds".format(self.entity_name, round(end - start, 4)))
        with self.output().open('w') as outfile:
            outfile.write("Ended in {0} seconds".format(round(end - start, 4)))


def get_all_copying_tasks():
    tasks = []
    for entity_name in transform_classes:
        tasks.append(CopyEntityFromCsvToDb(entity_name=entity_name))
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
        return CopyAll(self.data_dir, self.providers,
                       self.data_dir_out) if PdcmConfig().deploy_mode != "cluster" else ParquetToPg()

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


class ParquetToPg(SparkSubmitTask):
    name = "parquet_to_pg_load_all"
    app = "etl/jobs/load/database_loader.py"
    db_host = luigi.Parameter()
    db_port = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user = luigi.Parameter()
    db_password = luigi.Parameter()
    data_dir_out = luigi.Parameter()
    table_names = list(transform_classes.keys())

    def output(self):
        return PdcmConfig().get_target("{0}/{1}/{2}".format(self.data_dir_out, "database", "parquet_to_pg_load_all"))

    def requires(self):
        return [DeleteFksAndIndexes()] + list(transform_classes.values())

    def app_options(self):
        return [self.db_user, self.db_password, self.db_host, self.db_port, self.db_name,
                "|".join(i.path for i in self.input()[1:]), "|".join(self.table_names), self.output().path]


if __name__ == "__main__":
    luigi.run()
