# WMATA_AVL

The WMATA_AVL repository contains the `wmatarawnav` Python package for working with WMATA rawnav data and as well as code specific to the Queue Jump Effectiveness Study. 

## Setup and Execution

First, create a Python environment based on the included requirements.txt or environment.yml files. Instructions on doing so can be found [here](https://stackoverflow.com/questions/48787250/set-up-virtualenv-using-a-requirements-txt-generated-by-conda), among other places.

The `wmatarawnav` code is used in two locations in the folder **analysis**:

* In **queue-jump-analysis**, code in several scripts executes the processing and analysis of rawnav data for the Queue Jump Effectiveness study. These scripts should be run in their numbered sequence. 
* In **exploratory**, code in several Jupyter notebooks illustrates the use of `wmatarawnav` functions. Other material in this folder includes other R and Jupyter notebooks used for quick, ad-hoc analyses of rawnav data and is not intended to be stable or final.

## Other Contents
The contents of the repository are briefly described here:

* **data**: Data used for tests, documentation, and other files small enough and appropriate enough to commit (e.g., segment geojson files).
* **docs**: Placeholder folder for rendered function documentation
* **renv**: R environment used to execute R notebooks.
* **tests**: Test scripts used to ensure Python code functions as expected.

## Development Strategy

The Master branch is intended to remain stable while in-development code remains in branches. Once branches have tests that successfully clear, they are merged to the Master branch. 
