import sys

from pyspark.sql import DataFrame, SparkSession

from etl.constants import Constants
from etl.jobs.util.id_assigner import add_id
from etl.jobs.util.molecular_characterization_fk_assigner import set_fk_molecular_characterization


def main(argv):
    """
    Creates a parquet file with cna molecular data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw cna molecular data
                    [2]: Parquet file path with transformed molecular characterization data
                    [2]: Output file
    """
    raw_cna_molecular_parquet_path = argv[1]
    molecular_characterization_path = argv[2]

    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    raw_cna_molecular_data_df = spark.read.parquet(raw_cna_molecular_parquet_path)
    molecular_characterization_df = spark.read.parquet(molecular_characterization_path)

    initial_cna_molecular_data_df = transform_initial_cna_molecular_data(
        raw_cna_molecular_data_df,
        molecular_characterization_df)
    initial_cna_molecular_data_df.write.mode("overwrite").parquet(output_path)


def transform_initial_cna_molecular_data(
        raw_cna_molecular_data_df: DataFrame,
        molecular_characterization_df: DataFrame) -> DataFrame:

    cna_df = get_cna_df(raw_cna_molecular_data_df)
    cna_df = set_fk_molecular_characterization(cna_df, 'copy number alteration', molecular_characterization_df)
    cna_df = cna_df.withColumnRenamed(Constants.DATA_SOURCE_COLUMN, "data_source")
    cna_df = add_id(cna_df, "id")
    return cna_df


def get_cna_df(raw_cna_df: DataFrame) -> DataFrame:
    return raw_cna_df.select(
        "sample_id",
        Constants.DATA_SOURCE_COLUMN,
        "seq_start_position",
        "seq_end_position",
        "symbol",
        "chromosome",
        "strand",
        "platform_id",
        "log10r_cna",
        "log2r_cna",
        "copy_number_status",
        "gistic_value",
        "picnic_value",
        "ensembl_gene_id",
        "ncbi_gene_id").drop_duplicates()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
