import sys
from pathlib import Path

import great_expectations as gx
import pandas as pd


def run_raw_validation():
    # load data
    raw_data_path = "data/raw/heart_disease_uci.csv"
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
    validator.expect_column_values_to_be_unique(column="id")
    # TODO: define other rules

    # save suite
    suite = validator.expectation_suite
    suite.name = "raw_data_suite"
    try:
        context.suites.add(suite)
    except Exception:
        pass  # if it already exists on disk

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

    # run and report
    results = checkpoint.run(batch_parameters={"dataframe": df})
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
