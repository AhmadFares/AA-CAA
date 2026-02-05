# config/test_config.py

GENERAL_CONFIG = {
    # identifiers only — no dataframes here
    "URs": [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40],
    "modes": ["tvd-exi", "tvd-uni"],
    "source_splits": [
        "candidates",
        "random",
        "low_penalty",
        "high_penalty",
        "low_coverage",
        "skewed",
    ],
    # example values (you will adjust them)
    "source_numbers": [5, 20],
    "thetas": [0.7, 0.8, 1],
    # 6 different methods you will implement
    "methods": ["method1", "method2", "method3", "method4", "method5", "method6"],
    # root folder for results
    "results_root": "results",
    "seeds": [42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56],  
}
