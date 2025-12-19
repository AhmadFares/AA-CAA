import pandas as pd
import sqlite3
import os
from helpers.id_utils import detect_id_column

BASE_DIR = os.path.dirname(os.path.dirname(__file__))


class TestCases:
    """
    This class contains test cases for the Coverage-Guided Row Selection algorithm.
    Each test case is defined as a tuple (T, UR) where:
      - T is the initial table (a pandas DataFrame).
      - UR is the User Request table (a pandas DataFrame) that specifies the required values for each column.
    """

    def __init__(self):
        self.cases = {}
        self.load_fixed_mathe_case()  # Load MATHE case
        self.load_fixed_movielens_case()  # Load fixed MovieLens case

    def load_lisa_sheets(self):
        """
        Load the Lisa_Sheets table from the SQLite database and store it as T.
        """
        db_path = "data/Lisa_Tabular_Data.db"  # Database path
        conn = sqlite3.connect(db_path)
        query = "SELECT * FROM Lisa_Sheets;"
        T = pd.read_sql_query(query, conn)
        conn.close()

        # 🔹 Define User Requests (UR):
        """
        UR1: Answer in the Beggining of the table
        UR2: Answer in the End of Table
        UR3: Answer Distributed in the Table
        UR4: No Answers
        UR5: Partial Answers
        UR6: Multiple Answers Applicable -> To test importance of Penalty_Optimization
        UR7: Intermediate Answers -> To test importance of Optimize_Selection
        """
        user_requests = {
            1: {
                "Keyword1": [
                    "venous approaches",
                    "removal venous",
                    "gestational hypertension",
                    "pre eclampsia",
                    "pregnancy methods",
                ],
                "Keyword2": [
                    "peripheral venous",
                    "pregnancy hypertension",
                    "haemorrhage",
                    "lupus",
                ],
            },
            2: {
                "Keyword1": ["mri lumbar", "sacroiliac tests", "spinal causes"],
                "Keyword2": [
                    "spine mri",
                    "spondylodiscitis pott",
                    "severe undernutrition",
                    "pain spinal",
                ],
            },
            3: {
                "Keyword1": [
                    "venous approaches",
                    "sacroiliac tests",
                    "pre eclampsia",
                    "mri lumbar",
                    "tumour stomach",
                    "splenomegaly enlarged",
                    "preventive cerclage",
                    "rachis cervical",
                ],
                "Keyword2": [
                    "hyperplasia parathyroid",
                    "oedematous syndrome",
                    "schizophrenia following",
                ],
            },
            4: {
                "Keyword1": ["aaaaa", "bbb", "cccc"],
                "Keyword2": ["dddd", "eeee", "ffff"],
            },
            5: {
                "Keyword1": ["venous approaches", "aaaaaa", "removal venous"],
                "Keyword2": [
                    "bbbbbbb",
                    "oedematous syndrome",
                    "hyperplasia parathyroid",
                ],
            },
            6: {
                # ID-FARES-Test||||||||||venous approaches|approach venous||
                "Keyword1": ["venous approaches"],
                "Keyword2": ["approach venous"],
            },
            7: {
                "Keyword1": [
                    "cerebral mri",
                    "limb trauma",
                    "trendelebourg lameness",
                    "complications pregnancy",
                ],
                "Keyword2": [
                    "stroke mri",
                    "saluting trendelebourg",
                    "maternal complications",
                    "complications nerve",
                ],
            },
        }

        # 🔹 Convert all User Requests to properly formatted DataFrames
        for case_number, ur_data in user_requests.items():
            self.cases[case_number] = (T, self.create_flexible_dataframe(ur_data))

        # 🔹 Add additional test cases (without Lisa_Sheets)
        self.cases[10] = self.create_penalty_opt_case()
        self.cases[11] = self.create_optimized_selection_case()

    def create_flexible_dataframe(self, data_dict):
        """
        Convert a dictionary to a pandas DataFrame, handling columns with different lengths.
        Drops NaN values to prevent bugs during matching and statistics.
        """
        return pd.DataFrame.from_dict(
            {
                key: pd.Series(value, dtype=object).dropna()
                for key, value in data_dict.items()
            }
        )

    def load_fixed_mathe_case(self, csv_path=None):
        """
        Load MATHE and use a fixed User Request (UR) across 3 columns.
        """

        if csv_path is None:
            csv_path = os.path.join(BASE_DIR, "data/MATHE/output_table.csv")

        mathe_df = pd.read_csv(csv_path, delimiter=";")
        id_col = detect_id_column(mathe_df)

        UR_Deep_1 = {
            "keyword_name": [
                "Two variables",
                "Orthogonality",
                "Three points rule",
                "Mean",
            ],
            "topic_name": [
                "Linear Algebra",
                "Probability",
                "Optimization",
                "Discrete Mathematics",
            ],
            "subtopic_name": [
                "Linear Transformations",
                "Vector Spaces",
                "Algebraic expressions, Equations, and Inequalities",
                "Triple Integration",
            ],
        }
        UR_Deep_2 = {
            "keyword_name": [
                "Matrix of a linear transformation",
                "Triangles",
                "Event",
                "Roots of a function",
            ],
            "topic_name": [
                "Real Functions of Several Variables",
                "Optimization",
                "Real Functions of a Single Variable",
                "Graph Theory",
            ],
            "subtopic_name": [
                "Double Integration",
                "Triple Integration",
                "Derivatives",
                "Domain, Image and Graphics",
            ],
        }

        UR_Shallow_1 = {
            "keyword_name": ["Cauchy problem"],
            "topic_name": ["Integration", "Discrete Mathematics"],
            "subtopic_name": ["Recursivity"],
            "question_id": [80],
            "id_lect": [2162],
            "answer1": ["The system has no solution."],
            "keyword_id": [139],
        }

        UR_Shallow_2 = {
            "newLevel": [2],
            "algorithmLevel": [2],
            "checked": [1.0],
            "keyword_id": [41.0],
            "keyword_name": ["Continuity"],
            "topic_name": ["Discrete Mathematics"],
            "subtopic_name": ["Limits, Continuity, Domain and Image"],
        }

        UR = self.create_flexible_dataframe(UR_Deep_1)
        base_cols = list(UR_Deep_1.keys()) + [id_col]
        T = mathe_df[base_cols].copy()

        self.cases[20] = (T, UR)  # UR_Deep_1

        UR = self.create_flexible_dataframe(UR_Deep_2)
        base_cols = list(UR_Deep_2.keys()) + [id_col]
        T = mathe_df[base_cols].copy()
        print(f"[load_fixed_mathe_case] Loaded T with columns: {list(T.columns)}")
        self.cases[21] = (T, UR)  # UR_Deep_2

        UR = self.create_flexible_dataframe(UR_Shallow_1)
        base_cols = list(UR_Shallow_1.keys()) + [id_col]
        T = mathe_df[base_cols].copy()

        self.cases[22] = (T, UR)  # UR_Shallow_1

        UR = self.create_flexible_dataframe(UR_Shallow_2)
        base_cols = list(UR_Shallow_2.keys()) + [id_col]
        T = mathe_df[base_cols].copy()

        self.cases[23] = (T, UR)  # UR_Shallow_2

        UR_Singleton = {"student_id": [1484]}
        UR = self.create_flexible_dataframe(UR_Singleton)
        base_cols = list(UR_Singleton.keys()) + [id_col]
        T = mathe_df[base_cols].copy()

        self.cases[29] = (T, UR)

        UR_Sparce = {
            "keyword_name": [
                "Classification of geometrical figures",
                "Volume",
                "Third order",
                "Open surface",
            ],
            "duration": [
                56.0,
                47.0,
            ],
        }
        UR = self.create_flexible_dataframe(UR_Sparce)
        base_cols = list(UR_Sparce.keys()) + [id_col]
        T = mathe_df[base_cols].copy()
        self.cases[30] = (T, UR)

    def load_fixed_movielens_case(self, csv_path=None):
        """
        Load MovieLens and use a fixed User Request (UR) for Occupation, Zip-code, Title.
        """
        import pandas as pd

        if csv_path is None:
            csv_path = os.path.join(BASE_DIR, "data/Movie_Lens/movielens-200k.csv")
        df = pd.read_csv(csv_path)
        df["synthetic_id"] = df["UserID"].astype(str) + "_" + df["MovieID"].astype(str)
        id_col = "synthetic_id"

        UR_Deep_ML = {
            "Occupation": [7, 13, 0, 1],
            "Zip-code": ["11793", "67042", "77459", "97124"],
            "Title": [
                "Swingers (1996)",
                "Very Brady Sequel, A (1996)",
                "Meatballs 4 (1992)",
                "Fiendish Plot of Dr. Fu Manchu, The (1980)",
            ],
        }

        UR_Shallow_ML = {
            "Occupation": [13],
            "Zip-code": ["62702"],
            "Title": ["Raise the Red Lantern (1991)"],
            "Genres": ["Drama|Film-Noir"],
            "Rating": [2],
            "Gender": ["M"],
            "Age": [25],
        }

        UR = self.create_flexible_dataframe(UR_Deep_ML)
        base_cols = list(UR_Deep_ML.keys()) + [id_col]
        T = df[base_cols].copy()

        self.cases[25] = (T, UR)

        UR = self.create_flexible_dataframe(UR_Shallow_ML)
        base_cols = list(UR_Shallow_ML.keys()) + [id_col]
        T = df[base_cols].copy()

        self.cases[26] = (T, UR)

    def get_case(self, case_number):
        """
        Return the tuple (T, UR) for the specified case number.
        Defaults to case 1 if the given case is not found.
        """
        return self.cases.get(case_number, self.cases[case_number])
