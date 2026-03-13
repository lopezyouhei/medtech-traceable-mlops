import logging
from pathlib import Path

import pandas as pd
import yaml

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_preprocess():
    logger.info("Starting data preprocessing.")

    raw_data_path = Path("data/raw/heart_disease_uci.csv")
    processed_dir = Path("data/processed")
    models_dir = Path("models")

    processed_dir.mkdir(parents=True, exist_ok=True)
    models_dir.mkdir(parents=True, exist_ok=True)

    # load raw data in reference to dvc hash
    # TODO: create separate function for this, it's already a code duplicate
    dvc_file_path = raw_data_path.with_name(raw_data_path.name + ".dvc")
    try:
        with open(dvc_file_path, "r") as f:
            dvc_metadata = yaml.safe_load(f)
            dvc_hash = dvc_metadata["outs"][0].get("md5")[:8]
    except FileNotFoundError:
        print(f"Warning: DVC file not found at {dvc_file_path}. Using a default hash.")
        dvc_hash = f"untracked_{raw_data_path.name}"
    logger.info(f"Loading raw data from {raw_data_path} with hash {dvc_hash}.")
    df = pd.read_csv(raw_data_path)
