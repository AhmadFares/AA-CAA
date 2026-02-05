# core/data_loading.py

import os
import glob
import pandas as pd
from helpers.test_cases import TestCases


# Single instance of TestCases (contains all URs and T tables)
TEST_CASES = TestCases()

# Where your pre-generated sources will be stored
GENERATED_SPLITS_FOLDER = "data/generated_splits"


def load_ur(ur_id):
    """
    Return (T, UR) dataframes from TestCases.Example: 20 -> (T, UR)
    """
    try:
        return TEST_CASES.cases[ur_id]
    except KeyError:
        raise ValueError(f"UR id {ur_id} not found in TestCases.cases")


def load_source_split(ur_id, split_name):
    """
    Load all CSV source files for a given UR and split name.
    
    Expected folder structure:
    data/generated_splits/UR20/random/
        src_1.csv
        src_2.csv
        ...
    """
    
    folder = os.path.join(GENERATED_SPLITS_FOLDER, f"UR{ur_id}", split_name)
    
    if not os.path.isdir(folder):
        raise ValueError(f"Split folder not found: {folder}")
    
    files = sorted(glob.glob(os.path.join(folder, "*.csv")))
    
    if len(files) == 0:
        raise ValueError(f"No CSV sources found in: {folder}")
    
    return [pd.read_csv(f) for f in files]
