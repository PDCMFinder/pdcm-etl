import sys
from pyspark.sql import DataFrame, SparkSession
import csv


def create_marker_dataframe(markers) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    columns = ["hgnc_id", "approved_symbol", "approved_name", "status", "previous_symbols", "alias_symbols", "accession_numbers", "refseq_ids", "alias_names", "ensembl_gene_id", "ncbi_gene_id"]
    df = spark.createDataFrame(data=markers, schema=columns)
    return df


def create_marker_aliases_dataframe(markers) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    aliases = []
    for marker in markers:
        approved_symbol = marker[1]
        alias_list = marker[5].split(",")
        for alias in alias_list:
            symbol_alias_pair = [approved_symbol, alias]
            aliases.append(symbol_alias_pair)

    columns = ["approved_symbol", "alias_symbol"]
    df = spark.createDataFrame(data=aliases, schema=columns)
    return df


def create_marker_prev_symbols_dataframe(markers) -> DataFrame:
    spark = SparkSession.builder.getOrCreate()
    prev_symbols = []
    for marker in markers:
        approved_symbol = marker[1]
        prev_symbol_list = marker[4].split(",")
        for prev_symbol in prev_symbol_list:
            symbol_prev_symbol_pair = [approved_symbol, prev_symbol]
            prev_symbols.append(symbol_prev_symbol_pair)

    columns = ["approved_symbol", "previous_symbol"]
    df = spark.createDataFrame(data=prev_symbol, schema=columns)
    return df


def extract_markers(input_path):
    markers = []
    with open(input_path + "/markers.tsv") as fp:
        tsv_file = csv.reader(fp, delimiter="\t")
        first_row = True
        for line in tsv_file:
            if first_row:
                first_row = False
            else:
                markers.append(line)
    return markers


def main(argv):
    """
    DCC Extractor job runner
    :param list argv: the list elements should be:
                    [1]: Input Path
                    [2]: Output Path
    """
    input_path = argv[1]
    #output_path = argv[2]
    markers = extract_markers(input_path)
    create_marker_aliases_dataframe(markers)


if __name__ == "__main__":
    sys.exit(main(sys.argv))
