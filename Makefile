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
	#	pub		: publish build artifacts to prod pypi
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
	-conda env create --file builder.yml
	echo "activate environment with..."
	echo "$ conda activate builder"

.PHONY: build
build: 
	python3 setup.py sdist bdist_wheel

.PHONY: pub
pub: clean build
	twine upload --repository pypi dist/*
