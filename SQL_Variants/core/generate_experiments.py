# core/generate_experiments.py

import itertools
from config.test_config import GENERAL_CONFIG

def generate_all_experiment_configs():
    for ur, split, src_num, theta in itertools.product(
            GENERAL_CONFIG["URs"],
            GENERAL_CONFIG["source_splits"],
            GENERAL_CONFIG["source_numbers"],
            GENERAL_CONFIG["thetas"]):
        
        yield {
            "UR": ur,
            "source_split": split,
            "source_number": src_num,
            "theta": theta
        }
