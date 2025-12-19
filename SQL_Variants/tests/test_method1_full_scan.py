
from SQL_Variants.core.duckdb_connection import get_connection, register_table
from SQL_Variants.methods.Method1_Full_Scan import Full_Scan

# Your existing helpers
from helpers.test_cases import TestCases
from helpers.Source_Constructors import SourceConstructor


def test_method1_full_scan():

    print("\n===== Loading Test Case =====")
    test_cases = TestCases()
    Sources, UR_df = test_cases.get_case(21)   # <-- Use any case number you want

    print(f"Number of sources in case: {len(Sources)}")
    print("UR columns:", list(UR_df.columns))

    # -------------------------------
    # 1. Construct sources
    # -------------------------------
    constructor = SourceConstructor(Sources, UR_df, seed=42)
    source_list = constructor.high_penalty_sources()    # or low_coverage_sources(), etc

    print(f"Constructed {len(source_list)} split sources.")

    # -------------------------------
    # 2. Create DuckDB & register tables
    # -------------------------------
    con = get_connection()

    table_names = []
    for i, df in enumerate(source_list):
        tname = f"S{i+1}"
        register_table(con, df, tname)
        table_names.append(tname)

    print("Registered tables in DB:", table_names)

    # -------------------------------
    # 3. Run Method 1 (Full Scan)
    # -------------------------------
    T = Full_Scan(con, UR_df, table_names)

    print("\n===== METHOD 1 FULL SCAN RESULT =====\n")
    print(T)

    print("\nCoverage & Penalty:")
    from SQL_Variants.core.utils import (
        compute_overall_coverage_dict,
        compute_overall_penalty_dict,
        ur_df_to_dict
    )

    UR = ur_df_to_dict(UR_df)

    cov, covs = compute_overall_coverage_dict(T, UR)
    pen, pens = compute_overall_penalty_dict(T, UR)

    print("Coverage:", cov, covs)
    print("Penalty:", pen, pens)

    print("\n===== END OF TEST =====\n")

if __name__ == "__main__":
    test_method1_full_scan()
