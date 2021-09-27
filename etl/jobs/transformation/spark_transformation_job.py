import sys

from etl.entities_registry import get_spark_job_by_entity_name


def main(argv):
    """
    Calls the needed job according to the entity
    :param list argv: the list elements should be:
                    [1]: Entity name
                    [2:]: Rest of parameters (the paths to the parquet files needed for the job).
                    Last parameter is always the output path
    """
    entity_name = argv[1]
    args_without_entity = argv[0:1] + argv[2:]

    job_method = get_spark_job_by_entity_name(entity_name)
    job_method(args_without_entity)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
