import luigi
from luigi.contrib.spark import SparkSubmitTask

from etl.constants import Constants
from etl.entities_registry import get_all_entities_names_to_store_db
from etl.entities_task_index import get_transformation_class_by_entity_name
from etl.jobs.load.database_manager import copy_entity_to_database, get_database_connection, \
    create_indexes, create_fks, recreate_tables, create_views
from etl.jobs.util.file_manager import copy_directory
from etl.workflow.config import PdcmConfig
from etl.workflow.reporter import WriteReleaseInfoCsv


class ParquetToCsv(SparkSubmitTask):
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
    env = luigi.Parameter()

    def requires(self):
        return {'parquetToCsvDependency': ParquetToCsv(name=self.entity_name),
                'recreateTablesDependency': RecreateTables()}

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}_{2}/{3}/{4}".format(self.data_dir_out, "database", self.env, "copied", self.entity_name))

    def run(self):
        copy_entity_to_database(
            self.entity_name, self.input()['parquetToCsvDependency'].path, self.db_host, self.db_port, self.db_name,
            self.db_user, self.db_password)

        with self.output().open('w') as outfile:
            outfile.write("Entity {0} copied".format(self.entity_name))


def get_all_copying_tasks():
    tasks = []
    for entity_name in get_all_entities_names_to_store_db():
        tasks.append(CopyEntityFromCsvToDb(entity_name=entity_name))
    return tasks


class RecreateTables(luigi.Task):
    data_dir_out = luigi.Parameter()
    db_host = luigi.Parameter()
    db_port = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user = luigi.Parameter()
    db_password = luigi.Parameter()
    env = luigi.Parameter()

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}_{2}/{3}".format(self.data_dir_out, "database", self.env, "tables_recreated"))

    def run(self):
        connection = get_database_connection(self.db_host, self.db_port, self.db_name, self.db_user, self.db_password)
        recreate_tables(connection)
        with self.output().open('w') as outfile:
            outfile.write("Tables recreated")
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
    env = luigi.Parameter()

    def requires(self):
        return CopyAll(self.data_dir, self.providers, self.data_dir_out)

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}_{2}/{3}".format(self.data_dir_out, "database", self.env, "fks_indexes_created"))

    def run(self):
        connection = get_database_connection(self.db_host, self.db_port, self.db_name, self.db_user, self.db_password)

        create_indexes(connection)
        create_fks(connection)
        with self.output().open('w') as outfile:
            outfile.write("Fks and indexes created")
        connection.commit()
        connection.close()


class CopyAll(luigi.Task):
    data_dir = luigi.Parameter()
    providers = luigi.ListParameter()
    data_dir_out = luigi.Parameter()
    env = luigi.Parameter()

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}_{2}/{3}".format(self.data_dir_out, "database", self.env, "all_entities_copied"))

    def requires(self):
        return get_all_copying_tasks()

    def run(self):
        yield get_all_copying_tasks()

        with self.output().open('w') as outfile:
            outfile.write("All entities copied")


class Cache(luigi.Task):
    cache = luigi.Parameter()
    cache_dir = luigi.Parameter()
    data_dir_out = luigi.Parameter()

    def output(self):
        return PdcmConfig().get_target("{0}/{1}".format(self.data_dir_out, "cache_checks_executed"))

    def run(self):
        use_cache = False
        if self.cache:
            use_cache = "yes" == str(self.cache).lower()
        if use_cache:
            copy_directory(self.cache_dir, self.data_dir_out)
        with self.output().open('w') as outfile:
            outfile.write("use_cache: {0}. folder: {1}".format(use_cache, self.cache_dir))


class CreateViews(luigi.Task):
    db_host = luigi.Parameter()
    db_port = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user = luigi.Parameter()
    db_password = luigi.Parameter()
    data_dir_out = luigi.Parameter()
    env = luigi.Parameter()
    """
        Creates all the views.
    """

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}_{2}/{3}".format(self.data_dir_out, "database", self.env, "views_created"))

    def run(self):
        print("\n\n********** Loading views ***********\n")

        connection = get_database_connection(self.db_host, self.db_port, self.db_name, self.db_user, self.db_password)

        create_views(connection)
        connection.commit()
        connection.close()

        with self.output().open('w') as outfile:
            outfile.write("Views created")

        print("\n********** End Loading views ***********\n")


class LoadPublicDBObjects(luigi.Task):
    """
        Loads all the objects (views and materialized views) that are going to be exposed in the schema created for the api.
    """
    data_dir_out = luigi.Parameter()
    env = luigi.Parameter()

    def requires(self):
        return [CreateFksAndIndexes(), LoadReleaseInfo()]

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}_{2}/{3}".format(self.data_dir_out, "database", self.env, "all_public_DB_objects_loaded"))

    def run(self):
        print("\n\n********** Loading all public DB objects ***********\n")
        # yield [CreateMaterializedViews(), CreateViews()]
        yield [CreateViews()]
        with self.output().open('w') as outfile:
            outfile.write("all public DB objects loaded")

        print("\n********** End Loading all public DB objects ***********\n")


class LoadReleaseInfo(luigi.Task):
    db_host = luigi.Parameter()
    db_port = luigi.Parameter()
    db_name = luigi.Parameter()
    db_user = luigi.Parameter()
    db_password = luigi.Parameter()
    data_dir_out = luigi.Parameter()
    env = luigi.Parameter()
    """
        Write data in release_info.
    """

    def requires(self):
        return WriteReleaseInfoCsv()

    def output(self):
        return PdcmConfig().get_target(
            "{0}/{1}_{2}/{3}".format(self.data_dir_out, "database", self.env, Constants.RELEASE_INFO_ENTITY))

    def run(self):

        copy_entity_to_database(
            Constants.RELEASE_INFO_ENTITY, self.input().path, self.db_host, self.db_port, self.db_name,
            self.db_user, self.db_password)

        with self.output().open('w') as outfile:
            outfile.write("Entity {0} copied".format(Constants.RELEASE_INFO_ENTITY))


if __name__ == "__main__":
    luigi.run()
