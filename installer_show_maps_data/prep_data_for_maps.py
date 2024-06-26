# %% [markdown]
# ## Preparing data to create maps and plots for the installerSHOW/policy plan 
# 
# This notebook is used to prepare the data for 1) the installerSHOW maps and plots data story and 2) policy plan.
# 
# The plots are visualised using Flourish.

# %%
import pandas as pd

# %% [markdown]
# ## 1. Loading MCS installations data

# %%
installations_data = pd.read_csv(
    "s3://asf-core-data/outputs/MCS/mcs_installations_240415.csv"
)

# %% [markdown]
# ## 2. Loading local authority and dealing with missing local authorities

# %%
# information about local authorities from Flourish
flourish_LAs = pd.read_csv(
    "s3://asf-exploration/installer_show_maps_data/inputs/flourish_LAs_and_codes.csv"
)

# %%
# information about local authorities from MCS
mcs_LAs = pd.DataFrame(installations_data["local_authority"].unique()).rename(
    columns={0: "Name"}
)
mcs_LAs = mcs_LAs[~pd.isnull(mcs_LAs["Name"])]

# %%
len(flourish_LAs), len(mcs_LAs)

# %%
# saving MCS local authorities that are not in Flourish to S3
mcs_LAs[~mcs_LAs["Name"].isin(flourish_LAs["Name"])].to_csv(
    "s3://asf-exploration/installer_show_maps_data/inputs/missing_LAs.csv", index=False
)

# %%
# saving Flourish local authorities that are not in MCS
flourish_LAs[~flourish_LAs["Name"].isin(mcs_LAs["Name"])]

# %% [markdown]
# ### Updating local authority information

# %%
# we've manually mapped MCS LAs to the Flourish LA
missing_LAs_update = pd.read_csv(
    "s3://asf-exploration/installer_show_maps_data/inputs/missing_LAs_updated.csv"
)

# %%
missing_LAs_update = missing_LAs_update.set_index("MCS LA").to_dict()["Flourish LA"]

# %%
installations_data["local_authority_updated"] = installations_data[
    "local_authority"
].replace(missing_LAs_update)

# %%


# %% [markdown]
# ## 3. Loading household data per local authority
# 
# Household info for Scotland here:
# 
# Household info for England and Wales here:
# https://www.ons.gov.uk/datasets/TS041/editions/2021/versions/3
# 
# 
# Data was downloaded from the websites and then uploaded to S3. Before uploading to S3 a few LA names were updated to match the names in the MCS data.

# %%
scotland_la_households =  pd.read_excel("s3://asf-exploration/installer_show_maps_data/inputs/scotland_households.xls")
england_wales_la_households = pd.read_csv("s3://asf-exploration/installer_show_maps_data/inputs/england_wales_households.csv")


# %%
scotland_la_households.head()

# %%
england_wales_la_households.head()

# %%
scotland_la_households.columns

# %%
scotland_la_households = scotland_la_households[['Area Name', 'Area code', '2022']].rename(columns={'Area Name': 'LA', 'Area code': 'LA Code', '2022': 'Number of Households'})
england_wales_la_households = england_wales_la_households.rename(columns = {'Lower Tier Local Authorities': 'LA', 'Lower Tier Local Authorities Code':'LA Code', 'Observation':'Number of Households'})
la_households_countries_combined = pd.concat([scotland_la_households, england_wales_la_households])

# %%
la_households_countries_combined

# %% [markdown]
# ## 4. Filtering data
# 
# 
# We're removing any commercial/non-domestic installation.
# 
# We're keeping installations of type unspecified/missing, as we can't be sure they're not domestic installations.

# %%
installations_data["installation_type"].unique()

# %% [markdown]
# Filtering out non-domestic and commercial, as we know that some of the unspecific data is domestic:

# %%
len(installations_data)

# %%
installations_data = installations_data[
    ~installations_data["installation_type"].isin(["Non-Domestic", "Commercial"])
]

# %%
len(installations_data)

# %% [markdown]
# Filtering out installations with missing local authority:

# %%
# % of installations with mising local authority
len(installations_data[pd.isnull(installations_data["local_authority"])]) / len(
    installations_data
) * 100

# %%
installations_data = installations_data[
    ~pd.isnull(installations_data["local_authority"])
]

# %%
len(installations_data)

# %%


# %% [markdown]
# ## 5. Cumulative installations and installers

# %%
cumulative_installations = installations_data.groupby(
    "local_authority_updated", as_index=False
).count()[["local_authority_updated", "commission_date"]]
cumulative_installations.rename(
    columns={
        "local_authority_updated": "Local Authority",
        "commission_date": "Total number of installations",
    },
    inplace=True,
)

# %%
cumulative_installations

# %%
cumulative_installations.to_csv(
    "s3://asf-exploration/installer_show_maps_data/outputs/cumulative_installations.csv"
)

# %%
cumulative_installations[
    "Total number of installations"
].max(), cumulative_installations["Total number of installations"].min()

# %%
cumulative_installers = installations_data.groupby(
    "local_authority_updated", as_index=False
).nunique()[["local_authority_updated", "company_unique_id"]]
cumulative_installers.rename(
    columns={
        "local_authority_updated": "Local Authority",
        "company_unique_id": "Total number of installation companies",
    },
    inplace=True,
)

# %%
cumulative_installers

# %%
cumulative_installers.to_csv(
    "s3://asf-exploration/installer_show_maps_data/outputs/cumulative_installers.csv"
)

# %%
cumulative_installers[
    "Total number of installation companies"
].max(), cumulative_installers["Total number of installation companies"].min()

# %% [markdown]
# ## 6. Installations and installers yearly

# %% [markdown]
# Number of installations per local authority per year:

# %%
installations_per_year = installations_data.groupby(
    ["local_authority_updated", "commission_year"]
).count()[["commission_date"]]
installations_per_year.rename(
    columns={"commission_date": "Number of installations"}, inplace=True
)
installations_per_year

# %%
installations_per_year = installations_per_year.unstack().fillna(0)
installations_per_year = installations_per_year.droplevel(0, axis=1)
installations_per_year["total"] = installations_per_year.sum(axis=1)
installations_per_year

# %% [markdown]
# Number of installers per local authority per year:

# %%
installers_per_year = installations_data.groupby(
    ["local_authority_updated", "commission_year"]
).nunique()[["company_unique_id"]]
installers_per_year.rename(
    columns={"company_unique_id": "Number of installation companies per year"},
    inplace=True,
)
installers_per_year

# %%
installers_per_year = installers_per_year.unstack().fillna(0)
installers_per_year = installers_per_year.droplevel(0, axis=1)
installers_per_year.reset_index(inplace=True)

# %%
installers_per_year = installers_per_year.merge(
    cumulative_installers,
    left_on="local_authority_updated",
    right_on="Local Authority",
    how="left",
)

# %%


# %% [markdown]
# ## 7. Growth rate of installations and installers between 2021 and 2023

# %% [markdown]
# Should we not map when # installations in first year is 0? Or should we say the growth is 0?
# I went with the first one, but happy to change it.

# %%
def growth_rate(final_year, first_year):
    if first_year == 0:
        return None
    return (final_year - first_year) / first_year * 100

# %%
first_year = 2021
final_year = 2023

# %% [markdown]
# Growth rate of installations:

# %%
installations_per_year[
    f"growth_rate_{first_year}_{final_year}"
] = installations_per_year.apply(
    lambda x: growth_rate(x[final_year], x[first_year]), axis=1
)

# %%
installations_per_year = installations_per_year.merge(
    flourish_LAs, left_on="local_authority_updated", right_on="Name", how="left"
)

# %%
installations_per_year["country"] = installations_per_year["Code"].str[0]
installations_per_year["country"] = installations_per_year["country"].map(
    {"E": "England", "W": "Wales", "S": "Scotland", "N": "Northern Ireland"}
)

# %%
installers_per_year[
    f"growth_rate_{first_year}_{final_year}"
] = installers_per_year.apply(
    lambda x: growth_rate(x[final_year], x[first_year]), axis=1
)

# %%
# saving to S3
installations_per_year[
    [
        first_year,
        final_year,
        "total",
        f"growth_rate_{first_year}_{final_year}",
        "country",
        "Name",
    ]
].to_csv(
    f"s3://asf-exploration/installer_show_maps_data/outputs/installations_growth_rate_{first_year}_{final_year}.csv"
)

# %%
# the map includes all the data above
# the scatter plots is based filtering the above csv: 1) only LAs with at least 100 installations in 2023 are included and 2) only LAs with at least 100% growth rate are included

# %% [markdown]
# Growth rate of installation companies:

# %%
installers_per_year = installers_per_year.merge(
    flourish_LAs, left_on="local_authority_updated", right_on="Name", how="left"
)

# %%
installers_per_year["country"] = installers_per_year["Code"].str[0]
installers_per_year["country"] = installers_per_year["country"].map(
    {"E": "England", "W": "Wales", "S": "Scotland", "N": "Northern Ireland"}
)

# %%
installers_per_year[
    f"growth_rate_{first_year}_{final_year}"
] = installers_per_year.apply(
    lambda x: growth_rate(x[final_year], x[first_year]), axis=1
)

# %%
# saving to S3
installers_per_year[
    [
        first_year,
        final_year,
        "Total number of installation companies",
        f"growth_rate_{first_year}_{final_year}",
        "country",
        "Name",
    ]
].to_csv(
    f"s3://asf-exploration/installer_show_maps_data/outputs/installers_growth_rate_{first_year}_{final_year}.csv"
)

# %%


# %% [markdown]
# ## 8. Installations per 1000 households

# %%
installations_per_household = cumulative_installations.merge(la_households_countries_combined, left_on='Local Authority', right_on='LA', how='left')

# %% [markdown]
# ### Checking that the nulls are all from Northern Ireland

# %%
las_with_missing_data = installations_per_household[installations_per_household["LA"].isnull()]["Local Authority"].tolist()

# %%
flourish_LAs[flourish_LAs["Name"].isin(las_with_missing_data)]

# %% [markdown]
# ### Installations per 1000 households:

# %%
installations_per_household = installations_per_household[~installations_per_household["LA"].isnull()]

# %%
installations_per_household["installations per 1000 households"] = installations_per_household["Total number of installations"] / installations_per_household["Number of Households"] * 1000

# %%
installations_per_household.to_csv(
    f"s3://asf-exploration/installer_show_maps_data/outputs/installatios_per_1000households.csv"
)

# %%


# %% [markdown]
# ## 9. Installations per year

# %%
installations_per_year = installations_data.groupby("commission_year").count()[["commission_date"]].rename(columns={"commission_date": "Number of installations"})

# %%
installations_per_year.to_csv(
    f"s3://asf-exploration/installer_show_maps_data/outputs/installatios_per_year.csv"
)

# %% [markdown]
# ## 10. Installations per month

# %%
installations_data["commission_month"] = installations_data["commission_date"].str[:7]

# %%
installations_per_month = installations_data.groupby("commission_month").count()[["commission_date"]].rename(columns={"commission_date": "Number of installations"})

# %%
months = pd.date_range(start=installations_data["commission_date"].min(), end=installations_data["commission_date"].max(), freq='M')
months = [str(month)[:7] for month in months]

# %%
installations_per_month = installations_per_month.reindex(months).fillna(0)

# %%
installations_per_month.to_csv(
    f"s3://asf-exploration/installer_show_maps_data/outputs/installatios_per_month.csv"
)

# %%



