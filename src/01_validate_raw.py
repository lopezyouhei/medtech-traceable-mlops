# TODO: use logging for the print statements to make it automation-ready
import logging
import subprocess
import sys
from pathlib import Path

import great_expectations as gx
import pandas as pd
import yaml

from config.data_contract import (
    CATEGORICAL_SETS,
    EXPECTED_BRONZE_TYPES,
    NUMERICAL_BOUNDS,
    FeatureNames,
)
from config.logging_config import LogLevel

logging.basicConfig(level=logging.INFO, format=LogLevel.FORMAT.value)
logger = logging.getLogger(__name__)


def run_raw_validation():
    # load data and enforce expected dtypes
    raw_data_path = Path("data/raw/heart_disease_uci.csv")
    df = pd.read_csv(raw_data_path, dtype=EXPECTED_BRONZE_TYPES)

    # initialize gx context
    gx_path = Path.cwd() / "gx"
    if not gx_path.exists():
        context = gx.get_context(mode="file", project_root_dir=Path.cwd())
    else:
        context = gx.get_context(project_root_dir=Path.cwd())

    # get or create data source pipeline
    try:
        data_source = context.data_sources.get(name="heart_source")
    except Exception:
        data_source = context.data_sources.add_pandas(name="heart_source")

    try:
        data_asset = data_source.get_asset(name="raw_data_asset")
    except Exception:
        data_asset = data_source.add_dataframe_asset(name="raw_data_asset")

    try:
        batch_definition = data_asset.get_batch_definition(name="batch_def")
    except Exception:
        batch_definition = data_asset.add_batch_definition_whole_dataframe(
            name="batch_def"
        )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})
    validator = context.get_validator(batch=batch)

    # define raw data expectations
    ## table properties
    validator.expect_table_row_count_to_equal(value=920)
    validator.expect_table_columns_to_match_set(
        column_set=CATEGORICAL_SETS["BRONZE_COLUMN_NAMES"]
    )
    ## type checks
    for feature, expected_type in EXPECTED_BRONZE_TYPES.items():
        if expected_type in ["Int64", "Float64"]:
            gx_type = expected_type.lower()
        elif expected_type == "boolean":
            gx_type = "bool"
        elif expected_type == "string":
            gx_type = "str"
        else:
            gx_type = expected_type

        validator.expect_column_values_to_be_of_type(
            column=feature.value, type_=gx_type
        )

    ## value checks
    validator.expect_column_values_to_be_unique(column=FeatureNames.ID.value)
    validator.expect_column_values_to_be_between(
        column=FeatureNames.AGE.value,
        min_value=NUMERICAL_BOUNDS[FeatureNames.AGE]["min_value"],
        max_value=NUMERICAL_BOUNDS[FeatureNames.AGE]["max_value"],
        mostly=0.95,
    )
    validator.expect_column_values_to_be_in_set(
        column=FeatureNames.SEX.value, value_set=CATEGORICAL_SETS[FeatureNames.SEX]
    )
    validator.expect_column_values_to_be_in_set(
        column=FeatureNames.DATASET.value,
        value_set=CATEGORICAL_SETS[FeatureNames.DATASET],
    )
    validator.expect_column_values_to_be_in_set(
        column=FeatureNames.CP.value, value_set=CATEGORICAL_SETS[FeatureNames.CP]
    )
    validator.expect_column_values_to_be_between(
        column=FeatureNames.TRESTBPS.value,
        min_value=NUMERICAL_BOUNDS[FeatureNames.TRESTBPS]["min_value"],
        max_value=NUMERICAL_BOUNDS[FeatureNames.TRESTBPS]["max_value"],
        mostly=0.95,
    )
    validator.expect_column_values_to_be_between(
        column=FeatureNames.CHOL.value,
        min_value=NUMERICAL_BOUNDS[FeatureNames.CHOL]["min_value"],
        max_value=NUMERICAL_BOUNDS[FeatureNames.CHOL]["max_value"],
        mostly=0.75,
    )
    validator.expect_column_values_to_be_in_set(
        column=FeatureNames.FBS.value, value_set=CATEGORICAL_SETS[FeatureNames.FBS]
    )
    validator.expect_column_values_to_be_in_set(
        column=FeatureNames.RESTECG.value,
        value_set=CATEGORICAL_SETS[FeatureNames.RESTECG],
    )
    validator.expect_column_values_to_be_between(
        column=FeatureNames.THALCH.value,
        min_value=NUMERICAL_BOUNDS[FeatureNames.THALCH]["min_value"],
        max_value=NUMERICAL_BOUNDS[FeatureNames.THALCH]["max_value"],
        mostly=0.95,
    )
    validator.expect_column_values_to_be_in_set(
        column=FeatureNames.EXANG.value, value_set=CATEGORICAL_SETS[FeatureNames.EXANG]
    )
    validator.expect_column_values_to_be_between(
        column=FeatureNames.OLDPEAK.value,
        min_value=NUMERICAL_BOUNDS[FeatureNames.OLDPEAK]["min_value"],
        max_value=NUMERICAL_BOUNDS[FeatureNames.OLDPEAK]["max_value"],
        mostly=0.95,
    )
    validator.expect_column_values_to_be_in_set(
        column=FeatureNames.SLOPE.value, value_set=CATEGORICAL_SETS[FeatureNames.SLOPE]
    )
    validator.expect_column_values_to_be_in_set(
        column=FeatureNames.CA.value, value_set=CATEGORICAL_SETS[FeatureNames.CA]
    )
    validator.expect_column_values_to_be_in_set(
        column=FeatureNames.THAL.value, value_set=CATEGORICAL_SETS[FeatureNames.THAL]
    )
    validator.expect_column_values_to_be_in_set(
        column=FeatureNames.NUM.value, value_set=CATEGORICAL_SETS[FeatureNames.NUM]
    )

    # save suite
    suite = validator.expectation_suite
    suite.name = "raw_data_suite"

    # # TODO: REMOVE AFTER TESTING PHASE
    # try:
    #     context.suites.delete(suite.name)
    # except Exception:
    #     pass
    # context.suites.add(suite)

    # TODO: UNCOMMENT FOR PRODUCTION AND CREATE AUTHORING NOTEBOOK
    try:
        context.suites.get(suite.name)
    except Exception:
        print("Suite not found! Did you run the authoring notebook first?")
        sys.exit(1)

    # get or create validation definition
    val_def_name = "raw_data_val_def"
    try:
        val_def = context.validation_definitions.get(val_def_name)
    except Exception:
        val_def = gx.ValidationDefinition(
            name=val_def_name,
            data=batch_definition,
            suite=suite,
        )
        context.validation_definitions.add(val_def)

    # get or create checkpoint
    checkpoint_name = "raw_data_checkpoint"
    try:
        checkpoint = context.checkpoints.get(checkpoint_name)
    except Exception:
        checkpoint = gx.Checkpoint(
            name=checkpoint_name,
            validation_definitions=[val_def],
        )
        context.checkpoints.add(checkpoint)

    # get dvc and git hash
    dvc_file_path = raw_data_path.with_name(raw_data_path.name + ".dvc")
    try:
        with open(dvc_file_path, "r") as f:
            dvc_metadata = yaml.safe_load(f)
            dvc_hash = dvc_metadata["outs"][0].get("md5")[:8]
    except FileNotFoundError:
        print(f"Warning: DVC file not found at {dvc_file_path}. Using a default hash.")
        dvc_hash = f"untracked_{raw_data_path.name}"

    git_hash = (
        subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
        .decode("ascii")
        .strip()
    )

    # add git and dvc hash for data traceability
    run_id = gx.RunIdentifier(run_name=f"dvc_{dvc_hash}_git_{git_hash}")

    # run and report
    results = checkpoint.run(batch_parameters={"dataframe": df}, run_id=run_id)
    context.build_data_docs()
    docs_url = context.get_docs_sites_urls()[0]["site_url"]
    print(f"View report at: {docs_url}")

    if not results.success:
        print("RAW DATA VALIDATION FAILED!")
        # print out the specific failures for debugging
        for res in results.results:
            if not res.success:
                print(
                    f"  - Failure in {res.expectation_config.kwargs.get('column')}: {res.result}"
                )
        sys.exit(1)  # stop the pipeline/DVC
    else:
        print("RAW DATA VALIDATION PASSED!")
        sys.exit(0)


if __name__ == "__main__":
    run_raw_validation()
