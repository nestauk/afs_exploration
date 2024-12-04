# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     custom_cell_magics: kql
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.2
#   kernelspec:
#     display_name: mcs_new_builds
#     language: python
#     name: mcs_new_builds
# ---

# %% [markdown]
# ### Imports and setup

# %%
from asf_core_data import get_mcs_installations, load_preprocessed_epc_data

import pandas as pd
import numpy as np

# %%
# change path to your own local version of EPC data
epc_path = "/Users/chris.williamson/Documents/ASF_data"

# %%
# Get installation data with each property's entire EPC history attached
# meaning that each property might be represented more than once
mcs = get_mcs_installations("full")

# %%
# convert date columns to datetime type
mcs["commission_date"] = pd.to_datetime(mcs["commission_date"])
mcs["INSPECTION_DATE"] = pd.to_datetime(mcs["INSPECTION_DATE"])

# %%
# merge installation type columns and filter to domestic installations
mcs["installation_type"] = mcs["installation_type"].fillna(
    mcs["end_user_installation_type"]
)
mcs = mcs.loc[mcs.installation_type == "Domestic"].reset_index(
    drop=True
)

# %% [markdown]
# ### What proportion of domestic records in the MCS database relate to new-build installations?

# %%
# UPRNs of properties that have an EPC labelled as "new dwelling" (not necessarily the first one - see below)
new_uprns = [uprn for uprn in mcs.loc[mcs["TRANSACTION_TYPE"] == "new dwelling"]["UPRN"] if uprn != "unknown"]

# %%
# filter to first records
# records not linked to an EPC are kept
# note: assumption here is that MCS records not matched to EPC are not new builds
# they could still be self-builds (don't need EPC if not selling/renting) but expect not many of these
first_records = (
    mcs
    .sort_values("INSPECTION_DATE")
    .groupby("original_mcs_index")
    .head(1)
    .sort_values("original_mcs_index")
)

# %%
# find number of days between first recorded EPC inspection and HP commission
# could instead use the first EPC that labels the property as a "new dwelling", rather than the first overall - which is best? (see below)
first_records["diff_epc_to_mcs"] = (
    first_records["commission_date"] - first_records["INSPECTION_DATE"]
).dt.days

# assume dwelling was built with HP if:
# - it has an EPC indicating that it is a new dwelling
# - time difference between first EPC inspection and HP installation is less than 1 year
# the time difference is arbitrary but means that HP installation needs to happen either before
# or within 1 year of the EPC inspection - this is unlikely to happen if the property is
# not a new build
first_records["assumed_hp_when_built"] = (
    first_records["UPRN"].isin(new_uprns)
) & (first_records["diff_epc_to_mcs"] < 365)

# %% [markdown]
# Proportion of domestic MCS installations that relate to new builds:

# %%
first_records.assumed_hp_when_built.value_counts(normalize=True)

# %% [markdown]
# Raw numbers:

# %%
first_records.assumed_hp_when_built.value_counts()

# %% [markdown]
# Top 5 installers of domestic new build installations:

# %%
first_records.loc[first_records["assumed_hp_when_built"]]["installer_name"].value_counts().head()

# %% [markdown]
# For these top installers, what proportion of their installations are new builds?

# %%
top_5_installers = list(first_records.loc[first_records["assumed_hp_when_built"]]["installer_name"].value_counts().head().index)

# %%
counts = first_records.loc[first_records["installer_name"].isin(top_5_installers)].groupby(["installer_name", "assumed_hp_when_built"]).size().reset_index().rename(columns={0: "count"})

counts["proportion"] = counts["count"] / counts.groupby("installer_name")["count"].transform("sum")

counts

# %% [markdown]
# Difference in average costs for domestic retrofits and new builds:

# %%
first_records.groupby("assumed_hp_when_built").cost.mean()

# %% [markdown]
# ### What proportion of properties in the EPC database that were built with a HP appear in the MCS database?

# %%
epc = load_preprocessed_epc_data(epc_path, version="preprocessed", usecols=["UPRN", "TRANSACTION_TYPE", "INSPECTION_DATE", "HP_INSTALLED"])

# %%
# filter to records of new builds with a heat pump
new_hp = epc.loc[(epc["TRANSACTION_TYPE"] == "new dwelling") & (epc["HP_INSTALLED"])]

# %%
# replace missing or unknown UPRNs with different codes to avoid them appearing the same in both datasets
new_hp["UPRN"] = new_hp["UPRN"].replace("unknown", "unknown_epc").fillna("unknown_epc").astype("str")
mcs["UPRN"] = mcs["UPRN"].replace("unknown", "unknown_mcs").fillna("unknown_mcs").astype("str")

# %%
new_hp["in_mcs"] = new_hp["UPRN"].isin(mcs["UPRN"])

# %%
new_hp = new_hp.drop_duplicates("UPRN")

# %% [markdown]
# Proportions of EPC new builds that are in the MCS database:

# %%
new_hp["in_mcs"].value_counts(normalize=True)

# %% [markdown]
# Raw numbers:

# %%
new_hp["in_mcs"].value_counts()

# %% [markdown]
# Weird that there are more properties identified here than by starting with the MCS dataset - how can this happen?

# %%
assumed_hp_when_built_uprns = first_records.loc[first_records["assumed_hp_when_built"]]["UPRN"]

weird = new_hp.loc[(new_hp["in_mcs"]) & (~new_hp["UPRN"].isin(assumed_hp_when_built_uprns))]

# %%
mcs.loc[mcs["UPRN"].isin(weird["UPRN"])][["UPRN", "commission_date", "TRANSACTION_TYPE", "INSPECTION_DATE"]].sort_values(["UPRN", "INSPECTION_DATE"]).head(20)

# %%
epc.loc[epc["UPRN"].isin(weird["UPRN"])].sort_values(["UPRN", "INSPECTION_DATE"]).head(20)

# %% [markdown]
# These properties seem to be ones with an EPC certificate that says that the property is a "new dwelling" and has a HP and either:
# * an earlier certificate with inspection date >1 year before the MCS commission date, or
# * an MCS commission date that is >1 year after the EPC inspection date
#
# The former could indicate errors in the EPC dataset (why would a new dwelling have a previous certificate?)
#
# The latter could be properties getting their HP replaced or getting a second HP installed, or errors in the MCS dataset.

# %% [markdown]
# ### Side note: How many "new dwelling" EPCs have an earlier certificate?

# %%
epc["certificate_n"] = epc.sort_values("INSPECTION_DATE").groupby("UPRN").cumcount()

# %%
epc.loc[(epc["UPRN"] != "unknown") & (epc["TRANSACTION_TYPE"] == "new dwelling")]["certificate_n"].value_counts()

# %% [markdown]
# Proportion of "new dwelling" EPCs that have an earlier certificate:

# %%
(epc.loc[(epc["UPRN"] != "unknown") & (epc["TRANSACTION_TYPE"] == "new dwelling")]["certificate_n"] >= 1).value_counts(normalize=True)

# %% [markdown]
# One extreme example:

# %%
epc.loc[epc["certificate_n"] == 273]

# %% [markdown]
# This one property has 300 "new dwelling" certificates:

# %%
epc.loc[(epc["UPRN"] == "384131") & (epc["TRANSACTION_TYPE"] == "new dwelling")]

# %% [markdown]
# Transaction types of first records for properties with a "new dwelling" certificate and an earlier certificate:

# %%
bad_uprns = epc.loc[(epc["UPRN"] != "unknown") & (epc["TRANSACTION_TYPE"] == "new dwelling") & (epc["certificate_n"] >= 1)]["UPRN"]

epc.loc[(epc["UPRN"].isin(bad_uprns)) & (epc["certificate_n"] == 0)]["TRANSACTION_TYPE"].value_counts(normalize=True)

# %% [markdown]
# ## Summary: to report to MCS

# %% [markdown]
# * About 11% of domestic installation records in the MCS dataset (up to Q1 2023) relate to new builds (13,420 records).
#
# * The biggest installer of new build HPs is ... with ... new build installations, which corresponds to ...% of this installer’s total installations. The second and third biggest installers of new build HPs are ... (... installations / ...%) and ... (... installations / ...%).
#
# * The average reported cost of a new build installation is about £840 less than a retrofit (£11,760 compared to £12,600). However note that this could be a result of many other factors such as the date of the installation, physical property characteristics, etc.
#
# * About 83% of properties in the EPC dataset which are labelled as being new dwellings with a heat pump are not found in the MCS dataset (89,485 properties).
#
# * Some "new dwellings with a heat pump" in the EPC dataset appear in the MCS dataset with a commissioning date that is more than 1 year after the EPC inspection date. These could be properties getting their original heat pump replaced or getting a second heat pump installed, but they could also indicate errors in the MCS dataset.
#

# %% [markdown]
#
