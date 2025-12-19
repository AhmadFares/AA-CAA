# config/test_config.py

GENERAL_CONFIG = {
    # identifiers only — no dataframes here
    "URs": [30],
    "source_splits": [
        "random",
        "low_penalty",
        "high_penalty",
        "low_coverage",
        "skewed",
    ],
    # example values (you will adjust them)
    "source_numbers": [2, 4, 6, 8],
    "thetas": [0.7, 0.8, 0.9],
    # 6 different methods you will implement
    "methods": ["method1", "method2", "method3", "method4", "method5", "method6"],
    # root folder for results
    "results_root": "results",
}
