# PDCM ETL

ETL process for the PDCM project.
The objective of this process is to, starting with a set of files containing data and metadata from providers, end up with a database containing all the information needed to power up the website https://www.pdxfinder.org/.


## Getting started
**Note**: The current code needs java 8. Java 11 won't work well.

After downloading or cloning the repository, go to the project folder and execute:

 - To run a postgres db using docker-compose, in case you don't want to run one yourself:
```sh
docker-compose up -d
```

 - To setup a python environment
```sh
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

Note: I had to run (on Mac)
```sh
env LDFLAGS='-L/usr/local/lib -L/usr/local/opt/openssl/lib -L/usr/local/opt/readline/lib' pip install psycopg2==2.8.6
```
because the installation of psycopg2 was failing.


 - The execution parameters are currently in a configuration file. Edit luigi.cfg and set the respective values for:
	 - data_dir: Directory where the tsv files with the data/metadata are
	 - providers: List of providers to process
	 - data_dir_out: Directory where the results of the process will be
```cfg
[DEFAULT]  
data_dir = {...}/pdxfinder-data  
providers = ["TRACE"]  
data_dir_out = output
```

- Start a local luigi scheduler
 ```sh
luigid --port 8082
```

- Run the process
```sh
PYTHONPATH=. python etl/workflow/main.py --scheduler-host localhost PdcmEtl
```