# Script to generate subsets of daps and core outputs for comparison

repo_path = '/home/ubuntu/sky_workdir/asf_exploration/'  # path to asf_exploration folder

import sys
sys.path.append(repo_path)

import pandas as pd
import json
import polars as pl
import os
import s3fs

from compare_core_with_daps_outputs import config


def convert_float(s):
    "Convert floats in strings to int"
    if s[-2:] == ".0":
        s = s[0:-2]
    return s


# Import data
test_daps = pl.read_parquet("s3://asf-daps/testing/processed_dedupl-0.parquet",
                                columns=config["common_cols"])
data_schema = test_daps.schema
test_core = pl.read_csv("s3://asf-daps/testing/q2_2023_EPC_GB_preprocessed_deddupl.csv",
                        columns=config["common_cols"],
                        dtypes=data_schema
                        )


# Remove null values from INSPECTION_DATE which will be used to generate unique ID
test_core = test_core.drop_nulls(subset="INSPECTION_DATE")
test_daps = test_daps.drop_nulls(subset="INSPECTION_DATE")

# Create unique id in daps
test_daps = test_daps.with_columns((pl.col("UPRN").map_elements(convert_float).alias("corrected_uprn")))
test_daps = test_daps.with_columns((pl.col("corrected_uprn").cast(pl.String) + pl.col("INSPECTION_DATE").cast(pl.String)).alias("uprn_date_id"))

# Create unique id in core
test_core = test_core.with_columns((pl.col("UPRN").map_elements(convert_float).alias("corrected_uprn")))
test_core = test_core.with_columns(pl.col("corrected_uprn").str.replace(" unknown", ""))
test_core = test_core.with_columns((pl.col("corrected_uprn").cast(pl.String) + pl.col("INSPECTION_DATE").cast(pl.String)).alias("uprn_date_id"))

# Get unique ids in core and daps
core_uprn_ids = set(test_core.select(pl.col("uprn_date_id").unique()).to_series())
daps_uprn_ids = set(test_daps.select(pl.col("uprn_date_id").unique()).to_series())
id_present_in_both = core_uprn_ids.intersection(daps_uprn_ids)

# Get subset of datasets with common row IDs
daps_subset = test_daps.filter(pl.col("uprn_date_id").is_in(id_present_in_both))
core_subset = test_core.filter(pl.col("uprn_date_id").is_in(id_present_in_both))

# Remove duplicated rows
dupl_ids = set(core_subset.filter(pl.col("uprn_date_id").is_duplicated())["uprn_date_id"])
core_subset = core_subset.filter(~pl.col("uprn_date_id").is_in(dupl_ids))
daps_subset = daps_subset.filter(~pl.col("uprn_date_id").is_in(dupl_ids))

# Save datasets to S3
fs = s3fs.S3FileSystem()

# Core subset
destination = "s3://asf-daps/testing/core_Q2_2023_processed_dedupl.parquet"
with fs.open(destination, mode='wb') as f:
    core_subset.write_parquet(f)

# Daps subset
destination = "s3://asf-daps/testing/daps_Q2_2023_processed_dedupl.parquet"
with fs.open(destination, mode='wb') as f:
    daps_subset.write_parquet(f)
