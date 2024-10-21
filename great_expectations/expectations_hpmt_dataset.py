import pandas as pd
import numpy as np
import json
import great_expectations as gx
from utils import save_expectations
import config


def hp_installed_false_expectations(data):
    data.expect_column_distinct_values_to_equal_set(
        column="MCS_AVAILABLE", value_set=[False]
    )["success"]
    data.expect_column_values_to_not_be_null(column="MCS_AVAILABLE")["success"]
    data.expect_column_distinct_values_to_equal_set(
        column="HP_TYPE", value_set=["No HP"]
    )["success"]
    data.expect_column_values_to_not_be_null(column="HP_TYPE")["success"]

    expectation_suite = data.get_expectation_suite()

    return expectation_suite


def hp_installed_missing_expectations(data):
    data.expect_column_values_to_be_null(column="MCS_AVAILABLE")["success"]
    data.expect_column_values_to_be_null(column="EPC_AVAILABLE")["success"]
    data.expect_column_values_to_be_null(column="HP_TYPE")["success"]
    expectation_suite = data.get_expectation_suite()

    return expectation_suite


def hp_data_unique_values(data):
    data.expect_column_distinct_values_to_equal_set(
        column="HP_INSTALLED", value_set=[True]
    )["success"]
    data.expect_column_distinct_values_to_equal_set(
        column="HEATING_SYSTEM", value_set=["heat pump"]
    )["success"]
    data.expect_column_distinct_values_to_equal_set(
        column="HEATING_FUEL", value_set=["electric"]
    )["success"]
    data.expect_column_distinct_values_to_equal_set(
        column="MCS_AVAILABLE", value_set=[True, False]
    )["success"]
    data.expect_column_distinct_values_to_equal_set(
        column="EPC_AVAILABLE", value_set=[True, False]
    )["success"]
    data.expect_column_distinct_values_to_equal_set(
        column="MAINS_GAS_FLAG", value_set=["N", "Y"]
    )["success"]
    data.expect_column_distinct_values_to_equal_set(
        column="SOLAR_WATER_HEATING_FLAG", value_set=[True, False]
    )["success"]
    for var in ["CURRENT_ENERGY_RATING", "POTENTIAL_ENERGY_RATING"]:
        data.expect_column_distinct_values_to_equal_set(
            column=var, value_set=["A", "B", "C", "D", "E", "F", "G"]
        )["success"]

    for var in [
        "WALLS_ENERGY_EFF",
        "ROOF_ENERGY_EFF",
        "FLOOR_ENERGY_EFF",
        "WINDOWS_ENERGY_EFF",
        "MAINHEAT_ENERGY_EFF",
        "MAINHEATC_ENERGY_EFF",
        "HOT_WATER_ENERGY_EFF",
        "LIGHTING_ENERGY_EFF",
    ]:
        data.expect_column_distinct_values_to_equal_set(
            column=var, value_set=["Very Good", "Good", "Average", "Poor", "Very Poor"]
        )["success"]

    data.expect_column_distinct_values_to_equal_set(
        column="CONSTRUCTION_AGE_BAND",
        value_set=[
            "England and Wales: before 1900",
            "Scotland: before 1919",
            "1900-1929",
            "1930-1949",
            "1950-1966",
            "1965-1975",
            "1976-1983",
            "1983-1991",
            "1991-1998",
            "1996-2002",
            "2003-2007",
            "2007 onwards",
        ],
    )["success"]
    data.expect_column_distinct_values_to_equal_set(
        column="GLAZED_AREA", value_set=[1, 2, 3, 4, 5]
    )["success"]
    data.expect_column_distinct_values_to_equal_set(
        column="GLAZED_TYPE",
        value_set=["double glazing", "triple glazing", "single glazing"],
    )["success"]
    data.expect_column_distinct_values_to_equal_set(
        column="TENURE",
        value_set=["rental (private)", "rental (social)", "owner-occupied"],
    )["success"]
    data.expect_column_distinct_values_to_equal_set(
        column="BUILT_FORM",
        value_set=[
            "Semi-Detached",
            "Detached",
            "Mid-Terrace",
            "Enclosed Mid-Terrace",
            "End-Terrace",
            "Enclosed End-Terrace",
        ],
    )["success"]
    data.expect_column_distinct_values_to_equal_set(
        column="PROPERTY_TYPE",
        value_set=["Flat", "House", "Bungalow", "Maisonette", "Park home"],
    )["success"]

    data.expect_column_distinct_values_to_equal_set(
        column="COUNTRY", value_set=["England", "Wales", "Scotland"]
    )["success"]
    data.expect_column_distinct_values_to_equal_set(
        column="HP_TYPE",
        value_set=[
            "Air Source Heat Pump",
            "Ground/Water Source Heat Pump",
            "Undefined or Other Heat Pump Type",
        ],
    )["success"]

    data.expect_column_distinct_values_to_equal_set(
        column="installation_type", value_set=["Domestic"]
    )["success"]

    hp_certifified_cols = [col for col in data.columns if col.endswith("certified")]
    for col in hp_certifified_cols:
        data.expect_column_distinct_values_to_equal_set(
            column=col, value_set=[True, False]
        )["success"]

    data.expect_column_distinct_values_to_equal_set(
        column="design",
        value_set=[
            "Space heat and DHW",
            "Space heat only",
            "Space Heat, DHW and another purpose",
            "DHW only",
            "DHW and another purpose",
            "Space Heat and another purpose",
            "Another purpose only",
        ],
    )["success"]

    return data


def hp_data_range_values(data):
    data.expect_column_values_to_be_between(
        column="NUMBER_HABITABLE_ROOMS", min_value=0, strict_min=False
    )["success"]
    data.expect_column_values_to_be_between(
        column="NUMBER_HEATED_ROOMS", min_value=0, strict_min=False
    )["success"]
    data.expect_column_values_to_be_between(
        column="ENERGY_CONSUMPTION_CURRENT", min_value=0, strict_min=False
    )["success"]
    data.expect_column_values_to_be_between(
        column="TOTAL_FLOOR_AREA", min_value=0, strict_min=False
    )["success"]
    data.expect_column_values_to_be_between(
        column="CURRENT_ENERGY_EFFICIENCY", min_value=0, strict_min=True
    )["success"]
    data.expect_column_values_to_be_between(
        column="PHOTO_SUPPLY",
        min_value=0,
        max_value=100,
        strict_min=False,
        strict_max=False,
    )["success"]
    data.expect_column_values_to_be_between(
        column="capacity", min_value=0, strict_min=False
    )["success"]
    data.expect_column_values_to_be_between(
        column="estimated_annual_generation", min_value=0, strict_min=False
    )["success"]
    data.expect_column_values_to_be_between(
        column="cost", min_value=0, strict_min=True
    )["success"]
    data.expect_column_values_to_be_between(
        column="flow_temp", min_value=0, strict_min=True
    )["success"]
    data.expect_column_values_to_be_between(
        column="scop", min_value=0, strict_min=True
    )["success"]

    return data


def hp_data_no_missings(data):
    for col in [
        "HP_INSTALLED",
        "HEATING_SYSTEM",
        "HEATING_FUEL",
        "HP_TYPE",
        "MCS_AVAILABLE",
        "EPC_AVAILABLE",
    ]:
        data.expect_column_values_to_not_be_null(column=col)["success"]

    return data


def small_percentage_missings(data):
    # expectation is going to fail for asf_core_data COUNTRY's variable, but shouldn't in asf_daps
    for col in ["POSTCODE", "HP_INSTALL_DATE", "COUNTRY"]:
        hp_data.expect_column_values_to_not_be_null(column=col, mostly=0.99)["success"]

    return data


def hp_data_expectations(data):
    data.expect_table_columns_to_match_set(config.hpmt_vars, exact_match=False)[
        "success"
    ]
    data = hp_data_unique_values(data)
    data = hp_data_range_values(data)
    data.expect_column_values_to_be_unique(column="UPRN")["success"]
    data = hp_data_no_missings(data)
    data = small_percentage_missings(data)

    expectation_suite = data.get_expectation_suite()

    return expectation_suite


def mcs_specifc_expectations(data):
    for col in config.most_mcs_columns:
        data.expect_column_values_to_not_be_null(column=col, mostly=0.9)["success"]

    for col in ["flow_temp", "scop"]:
        data.expect_column_values_to_not_be_null(column=col, mostly=0.84)["success"]

    # These are not really "missing values" - they correspond to installation type "Unspecified"
    data.expect_column_values_to_not_be_null(column="installation_type", mostly=0.85)[
        "success"
    ]

    expectation_suite = data.get_expectation_suite()

    return expectation_suite


def max_percent_expectations(data):
    data.expect_column_values_to_be_between(
        column="percentage", max_value=90, strict_max=False
    )["success"]

    expectation_suite = data.get_expectation_suite()

    return expectation_suite


def asf_daps_expectations(data):
    data.expect_table_row_count_to_equal(value=len(hp_data_asf_daps))["success"]

    for col in [
        "UPRN",
        "POSTCODE",
        "HP_INSTALL_DATE",
        "effective_from",
        "effective_to",
    ]:
        data.expect_column_unique_value_count_to_be_between(
            column=col,
            min_value=hp_data_asf_daps[col].nunique(),
            max_value=hp_data_asf_daps[col].nunique(),
        )["success"]

    for col in [
        "TOTAL_FLOOR_AREA",
        "CURRENT_ENERGY_EFFICIENCY",
        "NUMBER_HABITABLE_ROOMS",
        "capacity",
        "estimated_annual_generation",
        "cost",
        "flow_temp",
        "scop",
        "latitude",
        "longitude",
        "LATITUDE",
        "LONGITUDE",
    ]:
        data.expect_column_values_to_be_between(
            column=col,
            min_value=hp_data_asf_daps[col].nunique(),
            max_value=hp_data_asf_daps[col].nunique(),
        )["success"]

    expectation_suite = data.get_expectation_suite()

    return expectation_suite


if __name__ == "__main__":
    # Replace by any other version below
    data = pd.read_csv("merged_epc_mcs_installations_installers_230906.csv")

    # Data where HP_INSTALLED is False
    no_hp_installed = data[data["HP_INSTALLED"] == False]
    no_hp_installed = gx.from_pandas(no_hp_installed)

    no_hp_installed_expectation_suite = hp_installed_false_expectations(no_hp_installed)
    save_expectations(
        no_hp_installed_expectation_suite, "no_hp_installed_expectation_suite"
    )

    # Data where HP_INSTALLED is missing
    just_installers = data[pd.isnull(data["HP_INSTALLED"])]
    just_installers = gx.from_pandas(just_installers)

    del no_hp_installed_expectation_suite
    del no_hp_installed

    just_installers_expectation_suite = hp_installed_missing_expectations(
        just_installers
    )
    save_expectations(
        just_installers_expectation_suite, "just_installers_expectation_suite"
    )

    del just_installers_expectation_suite
    del just_installers

    # Data where HP_INSTALLED is True
    hp_data = data[data["HP_INSTALLED"] == True]
    hp_data = gx.from_pandas(hp_data)
    del data

    hp_data_expectation_suite = hp_data_expectations(hp_data)
    save_expectations(hp_data_expectation_suite, "hp_data_expectation_suite")

    # MCS specific checks
    mcs_hp_data = hp_data[hp_data["MCS_AVAILABLE"]]

    mcs_hp_data_expectation_suite = mcs_specifc_expectations(mcs_hp_data)
    save_expectations(mcs_hp_data_expectation_suite, "mcs_hp_data_expectation_suite")
    del mcs_hp_data_expectation_suite
    del mcs_hp_data

    # MCS and EPC specific checks
    epc_mcs_hp_data = hp_data[hp_data["EPC_AVAILABLE"] & hp_data["MCS_AVAILABLE"]]
    epc_mcs_percentages = pd.DataFrame(
        data=epc_mcs_hp_data.isnull().sum() / len(epc_mcs_hp_data) * 100
    )
    epc_mcs_percentages.columns = ["percentage"]
    epc_mcs_percentages = gx.from_pandas(epc_mcs_percentages)

    mcs_epc_hp_data_expectation_suite = max_percent_expectations(epc_mcs_percentages)

    save_expectations(
        mcs_epc_hp_data_expectation_suite, "mcs_epc_hp_data_expectation_suite"
    )
    del mcs_epc_hp_data_expectation_suite
    del epc_mcs_percentages

    # EPC specific data checks
    epc_hp_data = hp_data[hp_data["EPC_AVAILABLE"]]
    epc_percentages = pd.DataFrame(epc_hp_data.isnull().sum() / len(epc_hp_data) * 100)
    epc_percentages.columns = ["percentage"]
    epc_percentages = gx.from_pandas(epc_percentages)

    epc_hp_data_expectation_suite = max_percent_expectations(epc_percentages)

    save_expectations(epc_hp_data_expectation_suite, "epc_hp_data_expectation_suite")
    del epc_hp_data_expectation_suite
    del epc_percentages

    # Expectations based on batch data - useful for comparisons between
    # asf_core_data and asf_daps data
    hp_data_asf_daps = hp_data.copy()
    del hp_data

    hp_data_asf_daps_expectation_suite = asf_daps_expectations(hp_data_asf_daps)

    save_expectations(
        hp_data_asf_daps_expectation_suite, "hp_data_asf_daps_expectation_suite"
    )

    del hp_data_asf_daps_expectation_suite
    del hp_data_asf_daps
