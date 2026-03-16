import logging
from pathlib import Path

import pandas as pd
import yaml

from config.data_contract import (
    CATEGORICAL_SETS,
    EXPECTED_SILVER_TYPES,
    NUMERICAL_BOUNDS,
    FeatureNames,
)
from config.logging_config import LogLevel

logging.basicConfig(level=logging.INFO, format=LogLevel.FORMAT.value)
logger = logging.getLogger(__name__)


def run_clean_raw():
    logger.info("Starting raw data cleaning.")

    raw_data_path = Path("data/raw/heart_disease_uci.csv")
    cleaned_data_path = Path("data/cleaned/heart_disease_uci_cleaned.csv")

    cleaned_data_path.parent.mkdir(parents=True, exist_ok=True)

    # load raw data in reference to dvc hash
    dvc_file_path = raw_data_path.with_name(raw_data_path.name + ".dvc")
    try:
        with open(dvc_file_path, "r") as f:
            dvc_metadata = yaml.safe_load(f)
            dvc_hash = dvc_metadata["outs"][0].get("md5")[:8]
    except FileNotFoundError:
        logger.warning(f"DVC file not found at {dvc_file_path}. Using a default hash.")
        dvc_hash = f"untracked_{raw_data_path.name}"
    logger.info(f"Loading raw data from {raw_data_path} with hash {dvc_hash}.")
    df = pd.read_csv(raw_data_path, dtype=EXPECTED_SILVER_TYPES)

    # clean data according to data contract rules
    logger.info("Cleaning data according to data contract rules.")
    logger.info("Keeping only columns defined in the silver column names set.")
    df = df[CATEGORICAL_SETS["SILVER_COLUMN_NAMES"]]
    logger.info("Convert 'chol' values of 0 to NaN.")
    df[FeatureNames.CHOL.value] = df[FeatureNames.CHOL.value].replace(0, pd.NA)
    logger.info(
        "Drop rows corresponding to numerical values outside of defined bounds."
    )
    for feature, bounds in NUMERICAL_BOUNDS.items():
        min_val = bounds["min_value"]
        max_val = bounds["max_value"]

        # keeps rows that are within bounds or are missing (NaN)
        mask = ((df[feature.value] >= min_val) & (df[feature.value] <= max_val)) | df[
            feature.value
        ].isna()
        df = df[mask]

    logger.info(f"Binazing {FeatureNames.NUM.value} column.")
    df[FeatureNames.NUM.value] = (df[FeatureNames.NUM.value] > 0).astype("Int64")

    logger.info(f"Saving cleaned data to {cleaned_data_path}.")
    df.to_csv(cleaned_data_path, index=False)
    logger.info("Finished raw data cleaning.")


if __name__ == "__main__":
    run_clean_raw()
