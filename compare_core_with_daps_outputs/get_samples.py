# Script to generate subsets of daps and core outputs for comparison

repo_path = '/home/ubuntu/sky_workdir/asf_exploration/'  # path to asf_exploration folder

import sys
sys.path.append(repo_path)

import polars as pl
import s3fs
from datetime import datetime
import string
from argparse import ArgumentParser
import pathlib
from tqdm import tqdm

from compare_core_with_daps_outputs import config, schemas


def convert_float(s):
    "Convert floats in strings to int"
    if s[-2:] == ".0":
        s = s[0:-2]
    return s


def create_argparser():
    parser = ArgumentParser()

    parser.add_argument(
        "--file_path",
        help="Path to processed EPC file to use. Can be local file path or S3 URI."
        )
    
    parser.add_argument(
        "--pipeline",
        help="Specify pipeline used to create file specified in `file_path` arg. Either `core` for `asf_core_data` or `daps` for `asf_daps`."
    )

    return parser


if __name__ == "__main__":
    
    pl.enable_string_cache()
    parser = create_argparser()
    args = parser.parse_args()

    today = datetime.today().strftime("%Y%m%d")

    # Import processed dataset
    if pathlib.Path(args.file_path).suffix == ".csv":
        df = pl.read_csv(args.file_path, columns=config["common_cols"], dtypes=schemas.epc_schema)
    elif pathlib.Path(args.file_path).suffix == ".parquet":
        df = pl.read_parquet(args.file_path, columns=config["common_cols"])


    # Correct UPRN and create unique id in daps
    df = df.drop_nulls(subset="INSPECTION_DATE")

    df = df.with_columns((pl.col("UPRN").map_elements(convert_float, return_dtype=pl.String).alias("corrected_uprn")))
    df = df.drop("UPRN")
    df = df.with_columns(pl.col("corrected_uprn").str.replace(" unknown", ""))
    df = df.with_columns((pl.col("corrected_uprn").cast(pl.String) + pl.col("INSPECTION_DATE").cast(pl.String)).alias("uprn_date_id"))

    # Standardise nulls/unknowns and floats
    df = df.with_columns(pl.all().fill_null(""))
    for col in tqdm(df.columns):
        df = df.with_columns(pl.col(col).map_elements(convert_float, return_dtype=pl.String))
        df = df.with_columns(pl.col(col).replace("unknown", ""))

    # Standardise all string cols: remove punctuation and set to uppercase
    for col in tqdm(config["string_cols"]):
        df = df.with_columns(pl.col(col).map_elements(lambda s: s.translate(str.maketrans('', '', string.punctuation)).upper(), return_dtype=pl.String))

    # Save datasets to S3
    fs = s3fs.S3FileSystem()

    # Core subset
    destination = f"s3://asf-daps/testing/{args.pipeline}_Q2_2023_processed_dedupl_cleaned_{today}.parquet"
    with fs.open(destination, mode='wb') as f:
        df.write_parquet(f)
