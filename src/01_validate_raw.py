import subprocess
import sys
from pathlib import Path

import great_expectations as gx
import pandas as pd
import yaml


def run_raw_validation():
    # load data
    raw_data_path = Path("data/raw/heart_disease_uci.csv")
    df = pd.read_csv(raw_data_path)

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
        column_set=[
            "id",
            "age",
            "dataset",
            "sex",
            "cp",
            "trestbps",
            "chol",
            "fbs",
            "restecg",
            "thalch",
            "exang",
            "oldpeak",
            "slope",
            "ca",
            "thal",
            "num",
        ]
    )
    ## type checks
    validator.expect_column_values_to_be_of_type(column="id", type_="int64")
    validator.expect_column_values_to_be_of_type(column="age", type_="int64")
    validator.expect_column_values_to_be_of_type(column="sex", type_="object")
    validator.expect_column_values_to_be_of_type(column="dataset", type_="object")
    validator.expect_column_values_to_be_of_type(column="cp", type_="object")
    validator.expect_column_values_to_be_of_type(column="trestbps", type_="float64")
    validator.expect_column_values_to_be_of_type(column="chol", type_="float64")
    validator.expect_column_values_to_be_of_type(column="fbs", type_="object")
    validator.expect_column_values_to_be_of_type(column="restecg", type_="object")
    validator.expect_column_values_to_be_of_type(column="thalch", type_="float64")
    validator.expect_column_values_to_be_of_type(column="exang", type_="object")
    validator.expect_column_values_to_be_of_type(column="oldpeak", type_="float64")
    validator.expect_column_values_to_be_of_type(column="slope", type_="object")
    validator.expect_column_values_to_be_of_type(column="ca", type_="float64")
    validator.expect_column_values_to_be_of_type(column="thal", type_="object")
    validator.expect_column_values_to_be_of_type(column="num", type_="int64")

    ## value checks
    validator.expect_column_values_to_be_unique(column="id")
    validator.expect_column_values_to_be_between(
        column="age", min_value=18, max_value=120, mostly=0.95
    )
    validator.expect_column_values_to_be_in_set(
        column="sex", value_set=["Male", "Female"]
    )
    validator.expect_column_values_to_be_in_set(
        column="dataset",
        value_set=["Cleveland", "Hungary", "Switzerland", "VA Long Beach"],
    )
    validator.expect_column_values_to_be_in_set(
        column="cp",
        value_set=["typical angina", "asymptomatic", "non-anginal", "atypical angina"],
    )
    validator.expect_column_values_to_be_between(
        column="trestbps", min_value=80, max_value=200, mostly=0.95
    )
    validator.expect_column_values_to_be_between(
        column="chol", min_value=100, max_value=600, mostly=0.75
    )
    validator.expect_column_values_to_be_in_set(column="fbs", value_set=[True, False])
    validator.expect_column_values_to_be_in_set(
        column="restecg", value_set=["lv hypertrophy", "normal", "st-t abnormality"]
    )
    validator.expect_column_values_to_be_between(
        column="thalch", min_value=60, max_value=220, mostly=0.95
    )
    validator.expect_column_values_to_be_in_set(column="exang", value_set=[True, False])
    validator.expect_column_values_to_be_between(
        column="oldpeak", min_value=-3, max_value=10, mostly=0.95
    )
    validator.expect_column_values_to_be_in_set(
        column="slope", value_set=["downsloping", "flat", "upsloping"]
    )
    validator.expect_column_values_to_be_in_set(
        column="ca", value_set=[0.0, 1.0, 2.0, 3.0]
    )
    validator.expect_column_values_to_be_in_set(
        column="thal", value_set=["fixed defect", "normal", "reversable defect"]
    )
    validator.expect_column_values_to_be_in_set(column="num", value_set=[0, 1, 2, 3, 4])

    # save suite
    suite = validator.expectation_suite
    suite.name = "raw_data_suite"

    # TODO: REMOVE AFTER TESTING PHASE
    try:
        context.suites.delete(suite.name)
    except Exception:
        pass
    context.suites.add(suite)

    # TODO: UNCOMMENT FOR PRODUCTION AND CREATE AUTHORING NOTEBOOK
    #  try:
    #     context.suites.add(suite)
    # except Exception:
    #     print("Suite not found! Did you run the authoring notebook first?")
    #     sys.exit(1)

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
