import sys
from pyspark.sql import DataFrame, SparkSession
from etl.jobs.extract import biomarker_extractor
from etl.jobs.util.id_assigner import add_id


def main(argv):
    """
    DCC Extractor job runner
    :param list argv: the list elements should be:
                    [1]: Input Path
                    [2]: Output Path
    """
    input_path = argv[1]
    output_path = argv[2]

    spark = SparkSession.builder.getOrCreate()
    markers_df = biomarker_extractor.get_marker_dataframe(input_path)
    markers_df = add_id(markers_df, "id")
    markers_df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))


