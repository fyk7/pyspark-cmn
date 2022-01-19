help:
	@echo "clean - remove all build, test, coverage and Python artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-test - remove test and coverage artifacts"
	@echo "test - run tests quickly with the default Python"
	@echo "coverage - check code coverage quickly with the default Python"
	@echo "build - package"

all: default

default: clean dev_deps deps test lint build

venv:
	if [ ! -e "venv/bin" ] ; then python3 -m venv venv ; fi

clean: clean-build clean-pyc #clean-test

clean-build:
	rm -rf dist/

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -rf {} +

# clean-test:
# 	rm -rf .tox/
# 	rm -f .coverage
# 	rm -rf htmlcov/

deps: venv
	source venv/bin/activate && pip install -U -r requirements.txt -t ./src/libs

dev_deps: venv
	source venv/bin/activate && pip install -U -r requirements.txt

# test:
# 	. .venv/bin/activate && pytest

# 依存モジュール(サードパーティ&自作)はzipに固めてdistに配置することでpysparkが参照できるようになる
# emrにデプロイする際はaws cp --recursiveでs3からcodeをfeatchすると良いかも
build: clean
	mkdir ./dist
	cp ./src/main.py ./dist
	cp ./src/config/local.yml ./dist
	cd ./src && zip -x main.py -x \*libs\* -r ../dist/jobs.zip .
	cd ./src/libs && zip -r ../../dist/libs.zip .