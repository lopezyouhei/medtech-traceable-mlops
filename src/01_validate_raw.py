import sys

import great_expectations as gx
import pandas as pd
from great_expectations.data_context.data_context.ephemeral_data_context import (
    EphemeralDataContext,
)
from pandas.core.frame import DataFrame


def run_raw_validation():
    # load data
    raw_data_path = "data/raw/heart_disease_uci.csv"
    df: DataFrame = pd.read_csv(raw_data_path)

    # setup gx context and create validator
    context: EphemeralDataContext = gx.get_context()

    data_source = context.data_sources.add_pandas(name="heart_source")
    data_asset = data_source.add_dataframe_asset(name="raw_data_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name="batch_def")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    validator = context.get_validator(batch=batch)

    print(f"Validating {raw_data_path}")

    # define raw data expectations
    validator.expect_column_values_to_be_unique(column="id")

    # execute and check results
    results = validator.validate()

    if not results.success:
        print("RAW DATA VALIDATION FAILED!")
        # Print out the specific failures for debugging
        for res in results.results:
            if not res.success:
                print(
                    f"  - Failure in {res.expectation_config.kwargs.get('column')}: {res.result}"
                )
        sys.exit(1)  # Stop the pipeline/DVC
    else:
        print("RAW DATA VALIDATION PASSED!")
        sys.exit(0)


if __name__ == "__main__":
    run_raw_validation()
