# WMATA_AVL

The WMATA_AVL repository contains the `wmatarawnav` Python (>=3.7) package for working with WMATA rawnav data and as well as code specific to the Queue Jump Effectiveness Study. 

Code for the Queue Jump Effectiveness study exists in several forms:
1. **Code usable for any analysis of rawnav data.** The Python package `wmatarawnav` contains code that can be used for analysis of rawnav data.
2. **Code specific to the Queue Jump Effectiveness study**. The code in the folder analysis/queue-jump-analysis contains project-specific code to relate rawnav data to other sources using functions in the `wmatarawnav` package. 

Documentation of `wmatarawnav` code is provided in **docs/vignettes**, with additional documentation provided in function documentation within the code.

## Setup 

### Running Queue Jump Analysis Code

	1. Create a Python environment based on the included requirements.txt or environment.yml files. Instructions on doing so can be found [here](https://stackoverflow.com/questions/48787250/set-up-virtualenv-using-a-requirements-txt-generated-by-conda), among other places.
	2. Download rawnav data. Typically, this data is stored outside of the repository on a shared drive. Rawnav files can remain zipped, ala "rawnav00001191015.txt.zip". 
	3. Obtain a WMATA schedule database file. The `wmatarawnav` code will extract required pattern and stop information from this database, though specific database drivers are needed to do so. If your computer does not have these drivers, an error message will provide information on where to obtain the required drivers.
	4. Open the scripts in the folder analysis/queue-jump-analysis to make modifications to run on your computer. Paths are defined on a per-user basis, as source data and processed outputs may not be available to the same users. Most scripts also include a set of key parameters that can be modified, such as the routes to analyze or the days of the week to examine. 
	5. Execute the scripts in sequence. 

### Guidance on Modifying the Code for Other Analyses

In addition to the steps above, several additional steps are needed to run the evaluation code on other locations.
	1. Create a new folder within **analysis**. If desired, use the **queue-jump-analysis** folder as a template.
	1. Define evaluation segments as a linestring .geojson file. The only required field in this file is a "seg_name_id" that identifies the name of the segment, ala "sixteenth_u". Note that the directionality of the segment is important for `wmatarawnav` functions. Update the paths within your code to point to this segments file. Generally, segments are defined around a stop, beginning 75 feet downstream from the previous upstream signalized intersection and ending 300 feet after the stop. 
	2. Update the crosswalk table `xwalk_seg_pattern_stop_in` defined in the scripts `03_merge_segments.py` and `04_decompose_travel_time.py`. This crosswalk table defines what patterns should be associated with each segment. Additional guidance is provided within the codebase.
As described in the documentation, the travel time decomposition code is focused on the decomposition on nearside queue jump stops. Moreover, it currently works for a single evaluation stop, as the decomposition focuses on areas around a queue jump stop. Other stops may still be present in the segment, but time associated with these stops will be assigned to t_traffic.

### General Development Guidance
The Master branch is intended to remain stable while in-development code remains in branches. Once branches have tests and all tests successfully pass, they can be merged to the Master branch. 

Scripts in the `wmatarawnav` package are generally named to mirror the Queue Jump Effectiveness Study script that they support. For instance, functions applicable to the `03-merge-segments.py` script are in the package module `merge-segments.py`


## Other Contents
The contents of the repository are briefly described here:

	* **data**: Data used for tests, documentation, and other files small enough and appropriate enough to commit (e.g., segment geojson files).
	* **docs**: Folder for rendered function documentation
	* **renv**: R environment files used to execute R notebooks (see additional context below)
	* **analysis/exploratory**: Notebooks and scripts used to explore the characteristics of rawnav data. In general, these notebooks and scripts will not run without additional modification -- in some cases, they depend on code or outputs that has been superseded, or data that is not stored within the repository. For illustrations of the use of rawnav data, see the docs/vignettes folder.
	* **tests**: Test scripts used to ensure Python code functions as expected. Directions on running tests are included in each test script.
	* top-level files: Readme and environment files


