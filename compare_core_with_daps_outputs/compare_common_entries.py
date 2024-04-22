# %%
# Check EC2 connection
!ec2metadata

# %%
repo_path = '/home/ubuntu/sky_workdir/asf_exploration/'  # path to asf_exploration folder

# %%
import sys
sys.path.append(repo_path)

import polars as pl
pl.enable_string_cache()
import matplotlib.pyplot as plt
from tqdm import tqdm
import string
from datetime import datetime

# %% [markdown]
# ## Import data
# 
# The data imported here was created with `get_samples.py`. 
# 
# Here we will conduct further processing to prepare the datasets for comparison:
# - Filter to rows with unique ID (UPRN + INSPECTION_DATE) which appear in both core and daps outputs.
# - Remove rows with no inspection date/UPRN and rows with duplicate unique IDs.

# %%
test_daps = pl.read_parquet("s3://asf-daps/testing/daps_Q2_2023_processed_dedupl_cleaned_20240422.parquet")
test_core = pl.read_parquet("s3://asf-daps/testing/core_Q2_2023_processed_dedupl_cleaned_20240422.parquet")

# %% [markdown]
# ## Get common rows in both datasets

# %%
# Get unique ids in core and daps
core_uprn_ids = set(test_core.select(pl.col("uprn_date_id").unique()).to_series())
daps_uprn_ids = set(test_daps.select(pl.col("uprn_date_id").unique()).to_series())

id_present_in_both = core_uprn_ids.intersection(daps_uprn_ids)

# %%
# Get subset of datasets with common row IDs
test_daps = test_daps.filter(pl.col("uprn_date_id").is_in(id_present_in_both))
test_core = test_core.filter(pl.col("uprn_date_id").is_in(id_present_in_both))

# %%
# Remove duplicated rows
dupl_ids_core = set(test_core.filter(pl.col("uprn_date_id").is_duplicated())["uprn_date_id"])
dupl_ids_daps = set(test_daps.filter(pl.col("uprn_date_id").is_duplicated())["uprn_date_id"])

test_core = test_core.filter(~pl.col("uprn_date_id").is_in(dupl_ids_core | dupl_ids_daps))
test_daps = test_daps.filter(~pl.col("uprn_date_id").is_in(dupl_ids_core | dupl_ids_daps))

# %% [markdown]
# ## Look for diffs on subsample
# Looking at 100 rows, find all cols with a diff and output them as an example.

# %%
# Get set of unique IDs
ids = test_daps["uprn_date_id"].unique()

n = 100

# %%
daps_diffs_dicts = []
core_diffs_dicts = []

for _, id in tqdm(enumerate(ids[:n])):
    _daps = test_daps.filter(pl.col("uprn_date_id") == id)
    _core = test_core.filter(pl.col("uprn_date_id") == id)

    daps_diffs = _daps[:, [True if b in [None, True] else False for b in (_daps != _core).rows()[0]]].to_dicts()
    core_diffs = _core[:, [True if b in [None, True] else False for b in (_daps != _core).rows()[0]]].to_dicts()

    if len(daps_diffs) > 0:
        daps_diffs[0]["uprn_date_id"] = id
        daps_diffs_dicts.extend(daps_diffs)

    if len(core_diffs) > 0:
        core_diffs[0]["uprn_date_id"] = id
        core_diffs_dicts.extend(core_diffs)

# %%
daps_diffs_df = pl.DataFrame(daps_diffs_dicts)
x = daps_diffs_df.drop("uprn_date_id").count().columns
y = daps_diffs_df.drop("uprn_date_id").count().row(0)

fig, ax = plt.subplots()
ax.bar(x=x, height=y)
plt.xticks(rotation=90)
plt.title(f"Count of rows with differences between core and daps\noutputs by feature (EPC_proc_dedupl Q2 2023), N={n}")
plt.ylabel("Count of rows")
plt.show()

# %% [markdown]
# ## Manual look at some diffs

# %%
core_diffs_df = pl.DataFrame(core_diffs_dicts)

# %%
daps_diffs_dicts

# %%
core_diffs_dicts

# %% [markdown]
# ## Upscale to compare whole dataset for diffs

# %%
# Sort daps and core into the same order
_daps = test_daps.sort("uprn_date_id")

# %%
_core = test_core.sort("uprn_date_id")

# %%
# Get diffs between core and daps, row by row
diffs = (_core != _daps)

# %%
# Count rows with diffs for each feature and calculate % of rows with diffs
diffs_T = diffs.sum().transpose(include_header=True, header_name="feature", column_names=["diffs_count"])
diffs_T = diffs_T.with_columns((pl.col("diffs_count") / len(_daps) * 100).alias("percent"))

# %%
# See rows with most diffs
diffs_T.sort("diffs_count", descending=True)

# %%
# See rows with no diffs
diffs_T.filter(pl.col("diffs_count") == 0)

# %%
# Plot count of diffs per feature
plot_data = diffs_T.filter(pl.col("diffs_count") != 0).sort("diffs_count", descending=True)
x = plot_data["feature"]
y = plot_data["diffs_count"]

fig, ax = plt.subplots(figsize=(14, 5))
ax.bar(x=x, height=y)
plt.yscale("log")
plt.xticks(rotation=90)
plt.title("Count of rows with differences between core and daps\noutputs* by feature (EPC_proc_dedupl Q2 2023)")
plt.xlabel("*for rows with unique ID present in core and daps")
plt.ylabel("Row count (log)")
plt.show()

# %%
# Plot perc of diffs per feature
plot_data = diffs_T.filter(pl.col("percent") != 0).sort("percent", descending=True)
x = plot_data["feature"]
y = plot_data["percent"]

fig, ax = plt.subplots(figsize=(14, 5))
ax.bar(x=x, height=y)
plt.xticks(rotation=90)
plt.title("Percent of rows with differences between core and daps\noutputs* by feature (EPC_proc_dedupl Q2 2023)")
plt.xlabel("*for rows with unique ID present in core and daps")
plt.ylabel("Percent of rows")
plt.show()

# %% [markdown]
# ## Diffs per row

# %%
# Plot distribution of diff count per row
x = diffs.sum_horizontal()

fig, ax = plt.subplots()
ax.hist(x.filter(x > 0), bins=30)
plt.yscale("log")
plt.title(f"Distribution of diffs per row* (log scale), N={len(x.filter(x > 0))}")
plt.xlabel("Count of diffs in row\n\n*for rows with unique ID present in core and daps")
plt.ylabel("Count of rows (log)")
plt.show()

# %%
print(f"{round(x.filter(x == 0).count() / len(_daps) * 100, 2)}% rows have no differences")
print(f"{x.filter(x == 0).count()} rows have no differences.")
print(f"{x.filter(x > 0).count()} rows have at least one difference.")

# %% [markdown]
# ## Compare by col
# 
# Manually see what causes differences in ADDRESS cols.

# %%
# Add ID col to diffs
df = diffs.with_columns(pl.Series(name="uprn_date_id", values=_daps["uprn_date_id"]))

# %%
df.head()

# %%
# Sample daps to 100 rows with diffs in ADDRESS1 col
sample = _daps[["ADDRESS1", "ADDRESS2", "POSTCODE", "uprn_date_id"]].filter(df["ADDRESS1"]).sample(n=100, seed=8)

# %%
sample.sort("uprn_date_id")

# %%
# Filter core dataset to same unique IDs
_core[["ADDRESS1", "ADDRESS2", "POSTCODE", "uprn_date_id"]].filter(pl.col("uprn_date_id").is_in(sample["uprn_date_id"])).sort("uprn_date_id")

# %%



