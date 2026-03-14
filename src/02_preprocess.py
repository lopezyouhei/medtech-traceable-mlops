import logging
from pathlib import Path

import joblib
import pandas as pd
import yaml
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

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

    # data preparation for AI model
    logger.info("Binarizing the target variable.")
    df["target"] = df["num"].apply(lambda x: 1 if x > 0 else 0)

    # drop original target and columns which won't be used for predictive purposes
    X = df.drop(columns=["id", "dataset", "num", "target"])
    y = df["target"]

    # define feature groups based on Great Expectations rules
    categorical_features = ["sex", "cp", "restecg", "exang", "slope", "thal", "ca"]
    numerical_features = ["age", "trestbps", "chol", "thalch", "oldpeak"]

    # instantiate preprocess pipeline
    logger.info("Creating preprocessing pipeline.")
    numeric_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
            ("scaler", StandardScaler()),
        ]
    )
    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )
    preprocessor = ColumnTransformer(
        transformers=[
            ("num", numeric_transformer, numerical_features),
            ("cat", categorical_transformer, categorical_features),
        ]
    )

    # define train and test split
    logger.info("Splitting data into train and test sets.")
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # preprocess data
    logger.info("Fitting preprocessor and transforming data.")
    X_train_processed = preprocessor.fit_transform(
        X_train
    )  # calculates all baseline statistics and it transforms
    X_test_processed = preprocessor.transform(
        X_test
    )  # apply historical statistics and transform

    cat_feature_names = preprocessor.named_transformers_["cat"][
        "onehot"
    ].get_feature_names_out(categorical_features)
    all_feature_names = numerical_features + list(cat_feature_names)

    train_df = pd.DataFrame(X_train_processed, columns=all_feature_names)
    train_df["target"] = y_train.values
    test_df = pd.DataFrame(X_test_processed, columns=all_feature_names)
    test_df["target"] = y_test.values

    # save artefacts
    logger.info("Saving processed datasets and preprocessor artifact.")
    train_df.to_csv(processed_dir / "train.csv", index=False)
    test_df.to_csv(processed_dir / "test.csv", index=False)

    joblib.dump(preprocessor, models_dir / "preprocessor.joblib")

    logger.info(
        f"Preprocessing complete. Artifacts save to {processed_dir} and {models_dir}"
    )


if __name__ == "__main__":
    run_preprocess()
