# InstallerSHOW maps and visualisations

Putting together the data to be used in the Flourish story for the installerSHOW.

## Set up 🛠️
Open your terminal and follow the instructions:
1. **Clone this repo:** `git clone git@github.com:nestauk/asf_exploration.git`

2. **Navigate to this exploration's folder:** `cd asf_exploration/installer_show_maps_data`

3. **Create your conda environment:** `conda create --name installer_show python=3.10`

4. **Activate your conda environment:** `conda activate installer_show`

5. **Install package dependencies:** `pip install -r requirements.txt`

6. **Add your conda environment to the notebooks:** `python3 -m ipykernel install --user --name=installer_show`

7. **Install ipykernel:** `conda install -n installer_show ipykernel --update-deps --force-reinstall`

8. **Transform .py script into jupyter notebook to explore data/pipeline:** `jupytext --to notebook prep_data_for_maps.py`