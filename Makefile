all: default

default: clean devDeps build

luigi.cfg:
	echo "Creating luigi file local"
	cp luigi_template.cfg luigi.cfg
	sh inject.sh local.properties luigi.cfg

luigi-cluster-dev.cfg:
	echo "Creating luigi file cluster (dev)"
	cp luigi_template.cfg luigi-cluster-dev.cfg
	sh inject.sh cluster-dev.properties luigi-cluster-dev.cfg

luigi-cluster-prod.cfg:
	echo "Creating luigi file cluster (prod)"
	cp luigi_template.cfg luigi-cluster-prod.cfg
	sh inject.sh cluster-prod.properties luigi-cluster-prod.cfg

submit-local:
	source venv/bin/activate && PYTHONPATH='.' PYSPARK_PYTHON=python3  time luigi --module etl.workflow.main --scheduler-host localhost PdcmEtl --workers 8

submit-dev: build
	source venv/bin/activate && LUIGI_CONFIG_PATH='luigi-cluster-dev.cfg'  PYTHONPATH='.' TMPDIR='/nfs/production/tudor/pdcm/tmp' PYSPARK_PYTHON=python3 time luigi --module etl.workflow.main --scheduler-host ves-ebi-d9.ebi.ac.uk PdcmEtl --workers 10

submit-prod: build
	source venv/bin/activate && LUIGI_CONFIG_PATH='luigi-cluster-prod.cfg'  PYTHONPATH='.' TMPDIR='/nfs/production/tudor/pdcm/tmp' PYSPARK_PYTHON=python3 time luigi --module etl.workflow.main --scheduler-host ves-ebi-d9.ebi.ac.uk PdcmEtl --workers 10

submit: build
	source venv/bin/activate && LUIGI_CONFIG_PATH='luigi-lsf-dev.cfg'  PYTHONPATH='.' TMPDIR='/nfs/production/tudor/pdcm/tmp' PYSPARK_PYTHON=python3  luigi --module etl.workflow.main --scheduler-host ves-ebi-d9.ebi.ac.uk PdcmEtl --workers 10

rebundle-submit: clean libs build        ##@submit Submit luigi workflows to the cluster
	source venv/bin/activate && LUIGI_CONFIG_PATH='luigi-lsf-dev.cfg'  PYTHONPATH='.' TMPDIR='/nfs/production/tudor/pdcm/tmp'  PYSPARK_PYTHON=python3  python etl/workflow/main.py --scheduler-host ves-ebi-d9.ebi.ac.uk PdcmEtl --workers 8

.venv:          ##@environment Create a venv
	if [ ! -e "venv/bin/activate" ] ; then python3 -m venv --clear venv ; fi
	source venv/bin/activate && pip install --upgrade pip

build: clean-build        ##@deploy Build to the dist package
	mkdir -p ./dist
	zip -r ./dist/etl.zip etl

libs: clean-libs
	cd ./dist && mkdir libs
	source venv/bin/activate && pip install --upgrade pip
	source venv/bin/activate && pip install -U -r requirements.txt -t ./dist/libs
	cd ./dist/libs && zip -r ../libs.zip .
	cd ./dist && rm -rf libs


clean: clean-build clean-libs clean-pyc clean-test           ##@clean Clean all

clean-build:           ##@clean Clean the dist folder
	rm dist/etl.zip || true

clean-libs:
	rm dist/libs.zip || true

clean-pyc:           ##@clean Clean all the python auto generated files
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:           ##@clean Clean the pytest cache
	rm -fr .pytest_cache

dependencies: .venv      ##@dependencies Create a venv and install common and prod dependencies
	source venv/bin/activate && pip install -U -r requirements.txt


devEnv: .venv dependencies
##	source .venv/bin/activate && pip install -U pre-commit
##	source .venv/bin/activate && pre-commit install --install-hooks

test:       ##@best_practices Run pystest against the test folder
	source venv/bin/activate && pytest


 HELP_FUN = \
		 %help; \
		 while(<>) { push @{$$help{$$2 // 'options'}}, [$$1, $$3] if /^(\w+)\s*:.*\#\#(?:@(\w+))?\s(.*)$$/ }; \
		 print "usage: make [target]\n\n"; \
	 for (keys %help) { \
		 print "$$_:\n"; $$sep = " " x (20 - length $$_->[0]); \
		 print "  $$_->[0]$$sep$$_->[1]\n" for @{$$help{$$_}}; \
		 print "\n"; }


help:
	@echo "Run 'make' without a target to clean all, install dev dependencies, test, lint and build the package \n"
	@perl -e '$(HELP_FUN)' $(MAKEFILE_LIST)
