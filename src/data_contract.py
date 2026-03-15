from enum import Enum


class FeatureNames(str, Enum):
    ID = "id"
    AGE = "age"
    DATASET = "dataset"
    SEX = "sex"
    CP = "cp"
    TRESTBPS = "trestbps"
    CHOL = "chol"
    FBS = "fbs"
    RESTECG = "restecg"
    THALCH = "thalch"
    EXANG = "exang"
    OLDPEAK = "oldpeak"
    SLOPE = "slope"
    CA = "ca"
    THAL = "thal"
    NUM = "num"


class DatasetCategories(str, Enum):
    CLEVELAND = "Cleveland"
    HUNGARY = "Hungary"
    SWITZERLAND = "Switzerland"
    VA_LONG_BEACH = "VA Long Beach"


class SexCategories(str, Enum):
    MALE = "Male"
    FEMALE = "Female"


class CPCategories(str, Enum):
    TYPICAL_ANGINA = "typical angina"
    ASYMPTOMATIC = "asymptomatic"
    NON_AGINAL = "non-anginal"
    ATYPICAL_ANGINA = "atypical angina"


class FBSCategories(bool, Enum):
    TRUE = True
    FALSE = False


class RestECGCategories(str, Enum):
    LV_HYPERTROPHY = "lv hypertrophy"
    NORMAL = "normal"
    STT_ABNORMALITY = "st-t abnormality"


class ExangCategories(bool, Enum):
    TRUE = True
    FALSE = False


class SlopeCategories(str, Enum):
    DOWNSLOPING = "downsloping"
    FLAT = "flat"
    UPSLOPING = "upsloping"


class CACategories(float, Enum):
    VESSELS_0 = 0.0
    VESSELS_1 = 1.0
    VESSELS_2 = 2.0
    VESSELS_3 = 3.0


class ThalCategories(str, Enum):
    FIXED_DEFECT = "fixed defect"
    NORMAL = "normal"
    REVERSABLE_DEFECT = "reversable defect"


class NumCategories(int, Enum):
    DIAGNOSIS_0 = 0
    DIAGNOSIS_1 = 1
    DIAGNOSIS_2 = 2
    DIAGNOSIS_3 = 3
    DIAGNOSIS_4 = 4


NUMERICAL_BOUNDS = {
    FeatureNames.AGE: {"min_value": 18, "max_value": 120},
    FeatureNames.TRESTBPS: {"min_value": 80, "max_value": 200},
    FeatureNames.CHOL: {"min_value": 100, "max_value": 600},
    FeatureNames.THALCH: {"min_value": 60, "max_value": 220},
    FeatureNames.OLDPEAK: {"min_value": -3, "max_value": 10},
}

CATEGORICAL_SETS = {
    FeatureNames.SEX: [e.value for e in SexCategories],
    FeatureNames.DATASET: [e.value for e in DatasetCategories],
    FeatureNames.CP: [e.value for e in CPCategories],
    FeatureNames.FBS: [e.value for e in FBSCategories],
    FeatureNames.RESTECG: [e.value for e in RestECGCategories],
    FeatureNames.EXANG: [e.value for e in ExangCategories],
    FeatureNames.SLOPE: [e.value for e in SlopeCategories],
    FeatureNames.CA: [e.value for e in CACategories],
    FeatureNames.THAL: [e.value for e in ThalCategories],
    FeatureNames.NUM: [e.value for e in NumCategories],
}
