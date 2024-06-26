# Data for visualisations: InstallerSHOW and policy report

Putting together the data to be used in the Flourish story for the installerSHOW and for some visualisations to be featured in the policy report.

Data:
- HP installations data from the MCS Installations database
- LA codes from Flourish
- Household data:
    - (Scotland)[https://www.nrscotland.gov.uk/statistics-and-data/statistics/statistics-by-theme/households/household-estimates/2022] - Households and Dwellings in Scotland, 2022
    - (England and Wales)[https://www.ons.gov.uk/datasets/TS041/editions/2021/versions/3] - census 2021

## Set up 🛠️
Open your terminal and follow the instructions:
1. **Clone this repo:** `git clone git@github.com:nestauk/asf_exploration.git`

2. **Navigate to this exploration's folder:** `cd asf_exploration/installer_show_maps_data`

3. **Create your conda environment:** `conda create --name installer_show python=3.10`

4. **Activate your conda environment:** `conda activate installer_show`

5. **Install package dependencies:** `pip install -r requirements.txt`

6. **Add your conda environment to the notebooks:** `python3 -m ipykernel install --user --name=installer_show`

7. **Install kernel:** `conda install -n installer_show ipykernel --update-deps --force-reinstall`

8. **Transform .py script into jupyter notebook to explore data/pipeline:** `jupytext --to notebook prep_data_for_maps.py`