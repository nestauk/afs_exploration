# Using Great Expectations for asf_core_data and asf_daps data quality checks

## Set up 🛠️
Open your terminal and follow the instructions:
1. **Clone this repo:** `git clone git@github.com:nestauk/asf_exploration.git`

2. **Navigate to this exploration's folder:** `cd asf_exploration/great_expectations/`

3. **Create your conda environment:** `conda create --name great_expectations python=3.10`

4. **Activate your conda environment:** `conda activate great_expectations`

5. **Install package dependencies:** `pip3 install -r requirements.txt`

6. **Add your conda environment to the notebooks:** `python3 -m ipykernel install --user --name=great_expectations`


## Instructions

1. Download newest batch of HPMT data from asf-core-data S3 bucket

2. Run `python asf_exploration/great_expectations/expectations_hpmt_dataset.py` to create expectations. Alternatively run the jupyter notebook.

3. Donwload HPMT data from asf-daps S3 bucket

4. Run `python asf_exploration/great_expectations/validate_hpmt_expectations.py` to validate expectations
