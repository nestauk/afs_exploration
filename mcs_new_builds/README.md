# MCS new builds exploration

Analysis for MCS, who are interested in identifying new builds within the MCS dataset.
Analysis includes:
* proportion of MCS installations believed to be new builds
* proportion of EPC "new builds with heat pumps" appearing in MCS dataset
* short exploration of EPC "new dwelling" certificates where an earlier certificate for the same property exists

All analysis is in one notebook, `analysis.ipynb`.

## Setup

* Clone this repo: `git clone git@github.com:nestauk/asf_exploration.git`
* Navigate to the main folder: `cd asf_exploration`
* Switch to the correct branch: `git checkout 13_mcs_new_builds`
* Navigate to the project folder: `cd mcs_new_builds`
* Create a conda environment: `conda create -n mcs_new_builds python=3`
* Activate the conda environment: `conda activate mcs_new_builds`
* Install requirements: `pip install -r requirements.txt`
* Install Jupyter: `conda install jupyter ipykernel`
* Create a Jupyter kernel: `ipython kernel install --user --name=mcs_new_builds`
* Open and run the notebook. Note that you will need to change the path in cell 2 to your own local version of EPC data (which should contain a file with path `outputs/EPC/preprocessed_data/{batch}/EPC_GB_preprocessed.csv` - see documentation [here](https://github.com/nestauk/asf_core_data)).