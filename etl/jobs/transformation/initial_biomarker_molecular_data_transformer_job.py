import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

from etl.constants import Constants
from etl.jobs.util.id_assigner import add_id
from etl.jobs.util.molecular_characterization_fk_assigner import set_fk_molecular_characterization


def main(argv):
    """
    Creates a parquet file with provider type data.
    :param list argv: the list elements should be:
                    [1]: Parquet file path with raw biomarkers data
                    [2]: Parquet file path with raw external resources
                    [3]: Parquet file path with raw external resources' data
                    [4]: Parquet file path with molecular characterization data
                    [5]: Parquet file path with gene markers data
                    [6]: Output file
    """
    raw_biomarkers_parquet_path = argv[1]
    molecular_characterization_parquet_path = argv[2]

    output_path = argv[3]

    spark = SparkSession.builder.getOrCreate()
    raw_biomarkers_df = spark.read.parquet(raw_biomarkers_parquet_path)
    molecular_characterization_df = spark.read.parquet(molecular_characterization_parquet_path)

    initial_biomarkers_molecular_data_df = transform_initial_biomarkers_molecular_data(
        raw_biomarkers_df,
        molecular_characterization_df)

    initial_biomarkers_molecular_data_df.write.mode("overwrite").parquet(output_path)


def transform_initial_biomarkers_molecular_data(
        raw_biomarkers_df: DataFrame,
        molecular_characterization_df: DataFrame) -> DataFrame:
    biomarkers_df = get_biomarkers_df(raw_biomarkers_df)

    # Adding columns that don't exist in the dataset but are convenient to have for the harmonisation process
    biomarkers_df = biomarkers_df.withColumn("ensembl_gene_id", lit(""))
    biomarkers_df = biomarkers_df.withColumn("ncbi_gene_id", lit(""))

    # Rename column to make it easy to apply general transformations
    biomarkers_df = biomarkers_df.withColumnRenamed("biomarker", "symbol")

    biomarkers_df = set_fk_molecular_characterization(biomarkers_df, 'biomarker', molecular_characterization_df)

    biomarkers_df = biomarkers_df.withColumnRenamed(Constants.DATA_SOURCE_COLUMN, "data_source")
    biomarkers_df = add_id(biomarkers_df, "id")
    return biomarkers_df


def get_biomarkers_df(raw_biomarkers_df: DataFrame) -> DataFrame:
    return raw_biomarkers_df.select(
        "sample_id",
        "biomarker_status",
        "biomarker",
        "platform_id",
        "essential_or_additional_marker",
        Constants.DATA_SOURCE_COLUMN).drop_duplicates()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
