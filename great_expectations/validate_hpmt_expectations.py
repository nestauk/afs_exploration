import pandas as pd
import great_expectations as gx
from utils import save_expectations


if __name__ == "__main__":
    # replace below with path to new data batch
    new_data_batch = gx.read_csv("path_to_new_data_batch.csv")

    # Data where HP_INSTALLED is False
    no_hp_installed_expectation_suite_validation_results = new_data_batch.validate(
        expectation_suite="no_hp_installed_expectation_suite.json"
    )
    save_expectations(
        no_hp_installed_expectation_suite_validation_results,
        "no_hp_installed_expectation_suite_validation_results",
    )

    # Data where HP_INSTALLED is missing
    just_installers_expectation_suite_validation_results = new_data_batch.validate(
        expectation_suite="just_installers_expectation_suite.json"
    )
    save_expectations(
        just_installers_expectation_suite_validation_results,
        "just_installers_expectation_suite_validation_results",
    )

    # Data where HP_INSTALLED is True
    hp_data_expectation_suite_validation_results = new_data_batch.validate(
        expectation_suite="hp_data_expectation_suite.json"
    )
    save_expectations(
        hp_data_expectation_suite_validation_results,
        "hp_data_expectation_suite_validation_results",
    )

    # MCS specific checks
    mcs_hp_data_expectation_suite_validation_results = new_data_batch.validate(
        expectation_suite="mcs_hp_data_expectation_suite.json"
    )
    save_expectations(
        mcs_hp_data_expectation_suite_validation_results,
        "mcs_hp_data_expectation_suite_validation_results",
    )

    # MCS and EPC specific checks
    mcs_epc_hp_data_expectation_suite_validation_results = new_data_batch.validate(
        expectation_suite="mcs_epc_hp_data_expectation_suite.json"
    )
    save_expectations(
        mcs_epc_hp_data_expectation_suite_validation_results,
        "mcs_epc_hp_data_expectation_suite_validation_results",
    )

    # EPC specific data checks
    epc_hp_data_expectation_suite_validation_results = new_data_batch.validate(
        expectation_suite="epc_hp_data_expectation_suite.json"
    )
    save_expectations(
        epc_hp_data_expectation_suite_validation_results,
        "epc_hp_data_expectation_suite_validation_results",
    )

    # Expectations based on batch data - useful for comparisons between
    # asf_core_data and asf_daps data
    hp_data_asf_daps_expectation_suite_validation_results = new_data_batch.validate(
        expectation_suite="hp_data_asf_daps_expectation_suite.json"
    )
    save_expectations(
        hp_data_asf_daps_expectation_suite_validation_results,
        "hp_data_asf_daps_expectation_suite_validation_results",
    )
