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
    markers_list = biomarker_extractor.extract_markers(input_path)
    markers_df = biomarker_extractor.create_marker_dataframe(markers_list)
    markers_df = add_id(markers_df, "id")
    markers_df.write.mode("overwrite").parquet(output_path+"/markers")

    marker_aliases_df = biomarker_extractor.create_marker_aliases_dataframe(markers_list)
    marker_aliases_df = add_id(marker_aliases_df, "id")
    marker_aliases_df.write.mode("overwrite").parquet(output_path + "/aliases")

    marker_prev_symbols_df = biomarker_extractor.create_marker_prev_symbols_dataframe(markers_list)
    marker_prev_symbols_df = add_id(marker_prev_symbols_df, "id")
    marker_prev_symbols_df.write.mode("overwrite").parquet(output_path + "/prev_symbols")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
