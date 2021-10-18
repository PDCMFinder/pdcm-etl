import luigi
from luigi.contrib.spark import SparkSubmitTask
import time

from etl.constants import Constants
from etl.entities_registry import get_all_entities_names
from etl.entities_task_index import get_transformation_class_by_entity_name, get_all_transformation_classes
from etl.jobs.load.database_manager import copy_entity_to_database, get_database_connection, \
    delete_fks, delete_indexes, create_indexes, create_fks
from etl.workflow.config import PdcmConfig


class ParquetToCsv(SparkSubmitTask):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()
    name = luigi.Parameter()

    app = 'etl/jobs/util/parquet_to_tsv_converter.py'

    def requires(self):
        return get_transformation_class_by_entity_name(self.name)

    def app_options(self):
        return [
            self.input().path,
            self.name,
            self.output().path
        ]

    def output(self):
        return PdcmConfig().get_target("{0}/{1}/{2}".format(self.data_dir_out, Constants.DATABASE_FORMATTED, self.name))


class ParquetToPg(SparkSubmitTask):
    name = "parquet_to_pg_load_all"
    app = "etl/jobs/load/database_loader.py"
    db_host = luigi.Parameter()
    db_port = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user = luigi.Parameter()
    db_password = luigi.Parameter()
    data_dir_out = luigi.Parameter()
    entity_name = luigi.Parameter()

    def requires(self):
        return get_transformation_class_by_entity_name(self.entity_name)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}/{2}".format(self.data_dir_out, Constants.DATABASE_FORMATTED, self.entity_name))

    def app_options(self):
        return [self.db_user, self.db_password, self.db_host, self.db_port, self.db_name,
                self.entity_name, self.output().path]


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
    for entity_name in get_all_entities_names():
        tasks.append(CopyEntityFromCsvToDb(entity_name=entity_name))
    return tasks


def get_all_copying_cluster_tasks():
    tasks = []
    for entity_name in get_all_entities_names():
        tasks.append(ParquetToPg(entity_name=entity_name))
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
                       self.data_dir_out) if PdcmConfig().deploy_mode != "cluster" else CopyAllCluster(self.data_dir,
                                                                                                       self.providers,
                                                                                                       self.data_dir_out)

    def output(self):
        return PdcmConfig().get_target("{0}/{1}/{2}".format(self.data_dir_out, "database", "fks_indexes"))

    def run(self):
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


class CopyAllCluster(luigi.WrapperTask):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()

    def requires(self):
        return DeleteFksAndIndexes()

    def run(self):
        yield get_all_copying_cluster_tasks()


if __name__ == "__main__":
    luigi.run()
