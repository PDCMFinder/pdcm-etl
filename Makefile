all: default

default: clean devDeps build

submit: build
	source venv/bin/activate && LUIGI_CONFIG_PATH='luigi.cfg'  PYTHONPATH='.'  YARN_CONF_DIR=/homes/mi_hadoop/hadoop-yarn-conf/ PYSPARK_PYTHON=python36  luigi --module etl.workflow.main --scheduler-host ves-ebi-d9.ebi.ac.uk PdcmEtl --workers 10

rebundle-submit: clean libs build        ##@submit Submit luigi workflows to the cluster
	source venv/bin/activate && LUIGI_CONFIG_PATH='luigi.cfg'  PYTHONPATH='.' PYSPARK_PYTHON=python36  python etl/workflow/main.py --scheduler-host ves-ebi-d9.ebi.ac.uk PdcmEtl --workers 8

.venv:          ##@environment Create a venv
	if [ ! -e "venv/bin/activate" ] ; then $(PYTHON) -m venv --clear venv ; fi
	source venv/bin/activate && pip install --upgrade pip

build: clean-build        ##@deploy Build to the dist package
	mkdir -p ./dist
	zip -r ./dist/etl.zip etl
	cp etl/entities.yaml ./
	cp etl/sources.yaml ./

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