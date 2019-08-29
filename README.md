# Energy Dashboard Library (EDL)

This is a collection of common library functions used by the [Energy
Dashboard](https://github.com/energy-analytics-project/energy-dashboard), a
project of the [Energy Analytics
Project](https://github.com/energy-analytics-project).

## Overview

Common utility functions are used by:

* [energy-dashboard-cli](https://github.com/energy-analytics-project/energy-dashboard-client)
* [data feeds](https://github.com/energy-analytics-project/energy-dashboard/data)
* [feed generator](https://github.com/energy-analytics-project/data-source-template)

This is probably only interesting to project maintainers. Tool users will want
to look at
[energy-dashboard-cli](https://github.com/energy-analytics-project/energy-dashboard-client).

## Dependencies

* [conda](https://conda.io/en/latest/)
* [make](https://www.gnu.org/software/make/manual/html_node/Introduction.html)

## Getting Started

```bash
# only required the first time around, creates a conda environment named 'eap-dev'
make setup

# activate the created conda environment
conda activate eap-dev

# build
make build
```

## Publishing

* Ensure that you have a valid account and that your account has permissions to push to this project.
* Ensure that your `~/.pypirc` file is up to date. Note that I'm using auth tokens below:

```text
[distutils]
index-servers=
    pypi
    testpypi

[testpypi]
repository: https://test.pypi.org/legacy/
username: __token__
password: pypi-...elided...

[pypi]
username: __token__
password: pypi-...elided...
```

* Publish to test first

```bash
# publish
make test-publish
```

* If that worked, then publish to prod

```bash
# publish
make prod-publish
```

## Links

* https://geohackweek.github.io/Introductory/01-conda-tutorial/
* https://packaging.python.org/guides/migrating-to-pypi-org/
* https://packaging.python.org/guides/using-testpypi/
* https://packaging.python.org/tutorials/packaging-projects/

## Author
Todd Greenwood-Geer (Enviro Software Solutions, LLC)
