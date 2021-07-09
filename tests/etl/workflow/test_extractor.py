from etl.workflow.extractor import *
from tests.data import model_raw_data, molecular_data_raw_data
from tests.util import convert_to_dataframe


def test_get_data_dir_path():
    path = get_data_dir_path("data_dir_folder", "provider_name")
    assert path == "data_dir_folder/data/UPDOG/provider_name"


def test_get_datasource_from_path():
    datasource = get_datasource_from_path("data_dir_folder/data/UPDOG/data_source/")
    assert datasource == "data_source"


def test_select_rows_with_data_with_description_rows(spark_session):
    model_raw_data_df = convert_to_dataframe(spark_session, model_raw_data)
    columns = ["model_id", "host_strain", "host_strain_full"]
    model_raw_data_df = select_rows_with_data(model_raw_data_df, columns)
    assert model_raw_data_df.count() == 2
    assert model_raw_data_df.columns == columns


def test_select_rows_with_data_without_description_rows(spark_session):
    molecular_data_raw_data_df = convert_to_dataframe(spark_session, molecular_data_raw_data)
    columns = ["model_id", "sample_id", "sample_origin", "passage", "host_strain_nomenclature", "chromosome",
               "seq_start_position", "seq_end_position", "symbol", "ucsc_gene_id", "ncbi_gene_id",
               "ensembl_gene_id", "log10r_cna", "log2r_cna", "copy_number_status", "gistic_value",
               "picnic_value", "genome_assembly", "platform"]

    model_raw_data_df = select_rows_with_data(molecular_data_raw_data_df, columns)
    assert model_raw_data_df.count() == 2
    assert model_raw_data_df.columns == columns

