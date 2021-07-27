import sys
from pyspark.sql import DataFrame, SparkSession
from etl.jobs.extract import ontology_extractor
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
    treatment_terms_df = ontology_extractor.get_treatment_term_dataframe(input_path)
    treatment_terms_df = add_id(treatment_terms_df, "id")
    treatment_terms_df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
