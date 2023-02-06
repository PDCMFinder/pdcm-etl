import csv
import os
from urllib.request import urlopen

import json

import requests

download_folder = "external_db"
tmp_folder = "tmp"


def create_folder_if_not_exists(target_path):
    if not os.path.exists(target_path):
        try:
            os.makedirs(target_path)
        except Exception as e:
            print(e)
            raise


# Fetches a JSON from an url and saves it locally (under {tmp_folder} folder)
def download_json_from_url(url, download_name):
    print(f"starts download: {download_name}")
    response = urlopen(url)
    data_json = json.loads(response.read())

    create_folder_if_not_exists(tmp_folder)

    with open(os.path.join(tmp_folder, download_name), 'w') as f:
        json.dump(data_json, f)

    print(f'File downloaded: {download_name}')


def download_csv_from_url(url, download_name):
    print(f"starts download: {download_name}")
    #  Using verify=False because of certificate issue when accessing data.oncomx.org
    with open(os.path.join(tmp_folder, download_name), 'wb') as f, \
            requests.get(url, stream=True, verify=False) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())

    print(f'File downloaded: {download_name}')


# Read local JSON and extract only the wanted property, keeping only unique entries
def get_unique_entries_local_json(local_json_name, json_node_with_data,  entry_value_property, entry_id_property):
    f = open(os.path.join(tmp_folder, local_json_name))

    # returns JSON object as a dictionary
    json_object = json.load(f)

    unique_values = set()
    #   Get the object that holds the data we are interested on
    data = json_object[json_node_with_data]

    unique_values = set()
    for x in data:
        unique_values.add(str(x[entry_value_property]) + "|" + str(x[entry_id_property]))

    return unique_values


def get_unique_entries_local_csv(local_csv_name, entry_column, entry_id_column):
    unique_values = set()

    with open(os.path.join(tmp_folder, local_csv_name), 'r') as f:
        file = csv.DictReader(f)
        for row in file:
            unique_values.add(row[entry_column] + "|" + row[entry_id_column])
    return unique_values


def write_entries_to_csv(entries, target_path, target_file):
    create_folder_if_not_exists(target_path)
    last_dot_index = target_file.index(".")
    target_file_csv_format = target_file[0:last_dot_index] + "_processed.csv"
    f = csv.writer(open(os.path.join(target_path, target_file_csv_format), "w"))
    f.writerow(["entry", "entry_id"])
    for value in entries:
        f.writerow(value.split("|"))


def download_civic_genes_data():
    url = "https://civicdb.org/api/datatables/genes?count=1000000"
    file_name = "civic_genes.csv"
    json_node_with_data = "result"
    entry_value_property = "name"
    entry_id_property = "name"

    download_json_resource(url, file_name, json_node_with_data, entry_value_property, entry_id_property)


def download_civic_variants_data():
    url = "https://civicdb.org/api/variants?count=10000"
    file_name = "civic_variants.csv"
    json_node_with_data = "records"
    entry_value_property = "name"
    entry_id_property = "id"

    download_json_resource(url, file_name, json_node_with_data, entry_value_property, entry_id_property)


def download_oncomx_genes_data():
    url = "https://data.oncomx.org/ln2wwwdata/reviewed/human_cancer_mutation.csv"
    file_name = "oncomx_genes.csv"
    entry_column = "gene_symbol"
    entry_id_column = "gene_symbol"

    download_csv_resource(url, file_name, entry_column, entry_id_column)


def download_json_resource(url, file_name, json_node_with_data, entry_value_property, entry_id_property):
    # Download the original JSON to process it later
    download_json_from_url(url, file_name)
    entries = get_unique_entries_local_json(file_name, json_node_with_data,  entry_value_property, entry_id_property)
    write_entries_to_csv(entries, download_folder, file_name)


def download_csv_resource(url, file_name, entry_column, entry_id_column):
    # Download the original csv to process it later
    download_csv_from_url(url, file_name)
    entries_from_csv = get_unique_entries_local_csv(file_name, entry_column, entry_id_column)
    write_entries_to_csv(entries_from_csv, download_folder, file_name)


download_civic_genes_data()
download_civic_variants_data()
download_oncomx_genes_data()
