import os
from urllib.request import urlopen

import json

import requests


def create_folder_if_not_exists(target_path):
    if not os.path.exists(target_path):
        try:
            os.makedirs(target_path)
        except Exception as e:
            print(e)
            raise


def download_json_from_url(url, target_path, target_file):
    response = urlopen(url)
    data_json = json.loads(response.read())

    create_folder_if_not_exists(target_path)

    with open(os.path.join(target_path, target_file), 'w') as f:
        json.dump(data_json, f)

    print(f'File downloaded: {target_file}')


def download_csv_from_url(url, target_path, target_file):
    #  Using verify=False because of certificate issue when accessing data.oncomx.org'
    with open(os.path.join(target_path, target_file), 'wb') as f, \
            requests.get(url, stream=True, verify=False) as r:
        for line in r.iter_lines():
            f.write(line + '\n'.encode())

    print(f'File downloaded: {target_file}')


download_folder = "external_db"

civic_genes_url = "https://civicdb.org/api/datatables/genes?count=1000000"
civic_genes_file = "civic_genes.json"

civic_variants_url = "https://civicdb.org/api/variants?count=10000"
civic_variants_file = "civic_variants.json"

oncomx_genes_url = " https://data.oncomx.org/ln2wwwdata/reviewed/human_cancer_mutation.csv"
oncomx_genes_file = "OncoMX_genes.csv"

download_json_from_url(civic_genes_url, download_folder, civic_genes_file)
download_json_from_url(civic_variants_url, download_folder, civic_variants_file)
download_csv_from_url(oncomx_genes_url, download_folder, oncomx_genes_file)
