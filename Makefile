.PHONY: all
all: help

.PHONY: help
help:
	# -----------------------------------------------------------------------------
	#  
	#  Targets:
	#
	#	help		: show this message
	#	clean		: remove build artifacts
	#	setup		: create the conda environment
	#	build		: build from source
	#	test-publish	: publish build artifacts to test pypi
	#	prod-publish	: publish build artifacts to prod pypi
	#
	# -----------------------------------------------------------------------------

# -----------------------------------------------------------------------------
# TARGETS
# -----------------------------------------------------------------------------

.PHONY: clean
clean:
	rm -rf venv
	rm -rf build
	rm -rf dist
	rm -rf energy_dashboard_library.egg-info

.PHONY: setup
setup:
	-conda env create --file eap-dev.yml
	echo "activate environment with..."
	echo "$ conda activate eap-dev"

.PHONY: build
build: 
	python3 setup.py sdist bdist_wheel


.PHONY: test-publish
test-publish: 
	twine upload --repository testpypi dist/*

.PHONY: prod-publish
prod-publish: 
	twine upload --repository pypi dist/*
