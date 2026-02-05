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
       # self.load_fixed_mathe_case()  # Load MATHE case
        self.load_fixed_movielens_case()  # Load fixed MovieLens case
        self.load_fixed_tus_case()

   
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

   
   
    # def load_fixed_mathe_case(self, csv_path=None):
    #     """
    #     Load MATHE and use a fixed User Request (UR) across 3 columns.
    #     """

    #     if csv_path is None:
    #         csv_path = os.path.join(BASE_DIR, "data/MATHE/output_table.csv")

    #     mathe_df = pd.read_csv(csv_path, delimiter=";")
    #     id_col = detect_id_column(mathe_df)

    #     UR_Deep_1 = {
    #         "keyword_name": [
    #             "Two variables",
    #             "Orthogonality",
    #             "Three points rule",
    #             "Mean",
    #         ],
    #         "topic_name": [
    #             "Linear Algebra",
    #             "Probability",
    #             "Optimization",
    #             "Discrete Mathematics",
    #         ],
    #         "subtopic_name": [
    #             "Linear Transformations",
    #             "Vector Spaces",
    #             "Algebraic expressions, Equations, and Inequalities",
    #             "Triple Integration",
    #         ],
    #     }
    #     UR_Deep_2 = {
    #         "keyword_name": [
    #             "Matrix of a linear transformation",
    #             "Triangles",
    #             "Event",
    #             "Roots of a function",
    #         ],
    #         "topic_name": [
    #             "Real Functions of Several Variables",
    #             "Optimization",
    #             "Real Functions of a Single Variable",
    #             "Graph Theory",
    #         ],
    #         "subtopic_name": [
    #             "Double Integration",
    #             "Triple Integration",
    #             "Derivatives",
    #             "Domain, Image and Graphics",
    #         ],
    #     }

    #     UR_Shallow_1 = {
    #         "keyword_name": ["Cauchy problem"],
    #         "topic_name": ["Integration", "Discrete Mathematics"],
    #         "subtopic_name": ["Recursivity"],
    #         "question_id": [80],
    #         "id_lect": [2162],
    #         "answer1": ["The system has no solution."],
    #         "keyword_id": [139],
    #     }

    #     UR_Shallow_2 = {
    #         "newLevel": [2],
    #         "algorithmLevel": [2],
    #         "checked": [1.0],
    #         "keyword_id": [41.0],
    #         "keyword_name": ["Continuity"],
    #         "topic_name": ["Discrete Mathematics"],
    #         "subtopic_name": ["Limits, Continuity, Domain and Image"],
    #     }

    #     UR = self.create_flexible_dataframe(UR_Deep_1)
    #     base_cols = list(UR_Deep_1.keys()) + [id_col]
    #     T = mathe_df[base_cols].copy()

    #     # self.cases[20] = (T, UR)  # UR_Deep_1

    #     UR = self.create_flexible_dataframe(UR_Deep_2)
    #     base_cols = list(UR_Deep_2.keys()) + [id_col]
    #     T = mathe_df[base_cols].copy()
    #     print(f"[load_fixed_mathe_case] Loaded T with columns: {list(T.columns)}")
    #     self.cases[21] = (T, UR)  # UR_Deep_2

    #     UR = self.create_flexible_dataframe(UR_Shallow_1)
    #     base_cols = list(UR_Shallow_1.keys()) + [id_col]
    #     T = mathe_df[base_cols].copy()

    #     self.cases[22] = (T, UR)  # UR_Shallow_1

    #     UR = self.create_flexible_dataframe(UR_Shallow_2)
    #     base_cols = list(UR_Shallow_2.keys()) + [id_col]
    #     T = mathe_df[base_cols].copy()

    #     self.cases[23] = (T, UR)  # UR_Shallow_2

    #     UR_Singleton = {"student_id": [1484]}
    #     UR = self.create_flexible_dataframe(UR_Singleton)
    #     base_cols = list(UR_Singleton.keys()) + [id_col]
    #     T = mathe_df[base_cols].copy()

    #     self.cases[29] = (T, UR)

    #     UR_Sparce = {
    #         "keyword_name": [
    #             "Classification of geometrical figures",
    #             "Volume",
    #             "Third order",
    #             "Open surface",
    #         ],
    #         "duration": [
    #             56.0,
    #             47.0,
    #         ],
    #     }
    #     UR = self.create_flexible_dataframe(UR_Sparce)
    #     base_cols = list(UR_Sparce.keys()) + [id_col]
    #     T = mathe_df[base_cols].copy()
    #     self.cases[30] = (T, UR)

   

    def load_fixed_movielens_case(self, csv_path=None):
        
        """
        MovieLens: hardcoded URs (you paste them below), then auto-register into self.cases.

        - Keeps your UR variable names (deep_rich_1, shallow_sparse_5, etc.)
        - Automatically creates (T, UR_df) and stores them in self.cases with IDs starting at 1
        - Also stores mapping id -> name in self.case_names
        """
        import os
        import pandas as pd

        # if csv_path is None:
        #     csv_path = os.path.join(BASE_DIR, "data/Movie_Lens/movielens-1m-full.csv")

        # df = pd.read_csv(csv_path)
        # df["synthetic_id"] = df["UserID"].astype(str) + "_" + df["MovieID"].astype(str)
        # id_col = "synthetic_id"

        deep_rich_1 = {
                "Age": [
                    35,
                    25,
                    18,
                ],
                "Occupation": [
                    7,
                    0,
                    4,
                ],
        }

        deep_sparse_1 = {
                "Zip-code": [
                    "85210",
                    "27510",
                    "06880",
                    "76707",
                ],
                "Age": [
                    1,
                    56,
                    45,
                    50,
                ],
        }

        shallow_rich_1 = {
                "Genres": [
                    "Comedy|Romance",
                ],
                "Title": [
                    "Star Wars: Episode IV - A New Hope (1977)",
                ],
                "Age": [
                    35,
                ],
                "Zip-code": [
                    "98103",
                ],
                "Occupation": [
                    0,
                ],
        }

        shallow_sparse_1 = {
                "Age": [
                    56,
                    45,
                ],
                "Genres": [
                    "Romance|Western",
                    "Action|Crime|Thriller",
                ],
                "Zip-code": [
                    "98632",
                ],
                "Title": [
                    "Whole Nine Yards, The (2000)",
                    "Ballad of Narayama, The (Narayama Bushiko) (1982)",
                ],
                "Occupation": [
                    9,
                    19,
                ],
        }

        deep_rich_2 = {
                "Title": [
                    "Star Wars: Episode IV - A New Hope (1977)",
                    "American Beauty (1999)",
                    "Star Wars: Episode V - The Empire Strikes Back (1980)",
                ],
                "Occupation": [
                    7,
                    0,
                    4,
                ],
        }

        deep_sparse_2 = {
                "Age": [
                    50,
                    1,
                    56,
                ],
                "Genres": [
                    "Drama|Film-Noir|Thriller",
                    "Action|Drama|Mystery",
                    "Crime|Horror",
                    "Action|Comedy|Sci-Fi|War",
                    "Children's|Comedy",
                ],
        }

        shallow_rich_2 = {
                "Occupation": [
                    7,
                    4,
                ],
                "Zip-code": [
                    "94110",
                ],
                "Genres": [
                    "Comedy",
                ],
                "Age": [
                    35,
                ],
        }

        shallow_sparse_2 = {
                "Genres": [
                    "Documentary|War",
                    "Drama|Film-Noir",
                ],
                "Occupation": [
                    5,
                    13,
                ],
                "Zip-code": [
                    "95420",
                ],
                "Title": [
                    "Anna Karenina (1997)",
                    "Paradine Case, The (1947)",
                ],
                "Age": [
                    50,
                    1,
                ],
        }

        deep_rich_3 = {
                "Age": [
                    35,
                    18,
                    25,
                    56,
                    50,
                ],
        }

        deep_sparse_3 = {
                "Title": [
                    "High Noon (1952)",
                    "Man of the House (1995)",
                    "One True Thing (1998)",
                    "Permanent Midnight (1998)",
                ],
                "Zip-code": [
                    "60126",
                    "48170",
                    "75605",
                    "33313",
                    "48360",
                    "06459",
                ],
                "Genres": [
                    "Drama|War",
                    "Action|Adventure|Fantasy|Sci-Fi",
                    "Film-Noir|Thriller",
                    "Animation|Children's|Musical",
                    "Action|Comedy|War",
                    "Action|Drama",
                ],
        }

        shallow_rich_3 = {
                "Zip-code": [
                    "94110",
                    "60640",
                ],
                "Age": [
                    25,
                ],
                "Occupation": [
                    0,
                ],
                "Genres": [
                    "Drama",
                ],
                "Title": [
                    "Star Wars: Episode V - The Empire Strikes Back (1980)",
                    "Star Wars: Episode IV - A New Hope (1977)",
                ],
        }

        shallow_sparse_3 = {
                "Age": [
                    56,
                ],
                "Zip-code": [
                    "55317",
                ],
                "Occupation": [
                    15,
                ],
                "Genres": [
                    "Adventure|Drama",
                ],
                "Title": [
                    "Mrs. Brown (Her Majesty, Mrs. Brown) (1997)",
                    "Big Trouble in Little China (1986)",
                ],
        }

        deep_rich_4 = {
                "Occupation": [
                    7,
                    0,
                    4,
                ],
                 "Zip-code": [
                    "60640",
                    "94110",
                    "98103",
                ],
        }

        deep_sparse_4 = {
                "Title": [
                    "American Pie (1999)",
                    "Return of Martin Guerre, The (Retour de Martin Guerre, Le) (1982)",
                    "Trial and Error (1997)",
                ],
        }

        shallow_rich_4 = {
                "Title": [
                    "American Beauty (1999)",
                ],
                "Genres": [
                    "Comedy",
                    "Comedy|Romance",
                ],
                "Occupation": [
                    0,
                    4,
                ],
                "Zip-code": [
                    "60640",
                ],
                "Age": [
                    18,
                    35,
                ],
        }

        shallow_sparse_4 = {
                "Age": [
                    50,
                ],
                "Occupation": [
                    18,
                ],
                "Zip-code": [
                    "80010",
                    "11355",
                ],
                "Genres": [
                    "Horror",
                    "Action|Children's|Fantasy",
                ],
        }

        deep_rich_5 = {
                "Occupation": [
                    0,
                    4,
                    7,
                ],
                "Age": [
                    25,
                    35,
                    18,
                ],
                "Zip-code": [
                    "60640",
                    "94110",
                    "98103",
                ],
        }

        deep_sparse_5 = {
                "Genres": [
                    "Mystery|Sci-Fi|Thriller",
                    "Film-Noir|Sci-Fi",
                    "Drama|Mystery|Romance",
                    "Film-Noir|Thriller",
                    "Sci-Fi",
                    "Comedy|Romance|Thriller",
                ],
                "Zip-code": [
                    "60126",
                    "48170",
                    "75605",
                    "33313",
                    "48360",
                    "06459",
                ],
        }

        shallow_rich_5 = {
                "Genres": [
                    "Comedy",
                ],
                "Age": [
                    35,
                ],
                "Title": [
                    "American Beauty (1999)",
                    "Star Wars: Episode V - The Empire Strikes Back (1980)",
                ],
                "Occupation": [
                    7,
                ],
                "Zip-code": [
                    "60640",
                ],
        }

        shallow_sparse_5 = {
                "Title": [
                    "Wrong Trousers, The (1993)",
                    "Operation Dumbo Drop (1995)",
                ],
                "Occupation": [
                    1,
                    15,
                ],
                "Genres": [
                    "Action|Comedy|Crime|Horror|Thriller",
                ],
                "Age": [
                    56,
                    50,
                ],
                "Zip-code": [
                    "50111",
                    "01746",
                ],
        }



        # ============================================================
        # AUTO-ASSIGN (no manual repetition)
        # ============================================================

        # Collect only variables that match your naming scheme and are dicts
        hardcoded = {
            name: val
            for name, val in locals().items()
            if isinstance(val, dict)
            and (
                name.startswith("deep_rich_") or name.startswith("deep_sparse_")
                or name.startswith("shallow_rich_") or name.startswith("shallow_sparse_")
            )
        }

        # Stable order: sort by (seed number, then variant)
        def _sort_key(nm):
            # nm like "deep_sparse_3"
            parts = nm.split("_")
            seed = int(parts[-1])
            variant = "_".join(parts[:-1])  # deep_sparse
            variant_order = {
                "deep_rich": 0,
                "deep_sparse": 1,
                "shallow_rich": 2,
                "shallow_sparse": 3,
            }
            return (seed, variant_order.get(variant, 99), nm)

        self.cases = {}
        self.case_names = {}

        case_id = 1
        for name in sorted(hardcoded.keys(), key=_sort_key):
            ur_dict = hardcoded[name]

            UR_df = self.create_flexible_dataframe(ur_dict)
            # base_cols = list(ur_dict.keys()) + [id_col]
            # T = df[base_cols].copy()

            self.cases[case_id] = (UR_df)
            self.case_names[case_id] = name
            case_id += 1

    
    def load_fixed_tus_case(self):
        UR21 = {
            "Branch name": ['NGM Americas', 'OGM Asia Pacific'],
            "Fund centre name": ['Brazil'],
            "Organisation name": ['International Development Research Centre'],
            "Organisation sub-class": ['NGO'],
            "Sector name": ['Fishery education/training', 'Democratic participation and civil society'],
        }

        UR22 = {
            "Branch name": [ 'MFM Global Issues and Development'],
            "Fund centre name": ['Democratic Republic of Congo', 'Inter American', 'Philippines'],
            "Organisation sub-class": ['UNITED NATIONS', 'CONSULTING SERVICES', 'SPECIALIZED INSTITUTE'],
        }

        UR23 = {
            "Organisation name": ['Information not available', 'Colleges and Institutes Canada'],
            "Organisation sub-class": ['UMBRELLA ORGANIZATION'],
            "Sector name": ['Water resources conservation (including data collection)'],
        }   
        UR24 = {
        "COLLISION_TYPE": ['REAR-END, ONE STANDING, OTHER MOVING'],
        }

        UR25 = {
            "COLLISION_TYPE": ['SIDE, ONE STANDING, OTHER MOVING', 'REAR-END, ONE STANDING, OTHER MOVING', 'SAME TRAIN, ONE MOVING, OTHER STANDING', 'SIDE, BOTH MOVING'],
        }

        UR26 = {
            "AWD_TYPE": ['FLASHING LIGHT SIGNALS AND BELL'],
            "COLLISION_TYPE": ['SAME TRAIN, BOTH MOVING'],
            "OCC_DATE": ['2006-08-14'],
            "SUBD_NAME": ['BLACKFOOT', 'BROOKS'],
            "XING_TYPE": ['PUBLIC AUTOMATED'],
        }

        UR27 = {
            "AWD_TYPE": ['FLS AND B WITH GATES'],
            "COLLISION_TYPE": ['SAME TRAIN, ONE MOVING, OTHER STANDING'],
        }

        UR28 = {
            "OCC_DATE": ['2008-10-09'],
            "SUBD_NAME": ['SPRAGUE'],
        }

        UR29 = {
            "U_2": ['Active - FLBG', 'Protection', 'Active - FLB', 'Passive'],
            "U_20": [4500, 'Vehicles Daily', 8200, 320],
        }

        UR30 = {
            "U_2": ['Active - FLB', 'Passive', 'Active - FLBG'],
            "U_20": ["1270", "50", "25"],
        }

        UR31 = {
            "U_2": ['Active - FLB', 'Passive', 'Active - FLBG'],
            "U_20": [1140, 10],
        }

        UR32 = {
            "CreationDateTime": ['2014-02-13T00:00:00', '2007-11-01T00:00:00', '2014-09-19T00:00:00', '2010-10-13T00:00:00', '2013-05-16T00:00:00'],
            "LocalityName": ['Abram', 'Weaste', 'Davyhulme', 'Uppermill', 'Chorlton'],        }

        UR33 = {
            "Bearing": ['S'],
            "CreationDateTime": ['2007-11-01T00:00:00'],
            "LocalityName": ['Bardsley'],
            "Notes": ['INDICATOR AMENDED'],
            "Street": ['ASHTON RD'],
        }

        UR34 = {
            "LocalityName": ['Ashton Upon Mersey', 'Charlestown', 'Middleton', 'Atherton', 'Clifton Green'],
            "Notes": ['INDICATOR AMENDED', 'STOP LOCATION AMENDED', 'HNR LOCATION AMENDMENT', 'NAME AMENDED', 'indicator amended'],
            "Street": ['ASHTON LANE', 'AUCKLAND DR', 'BUS STATION', 'LOVERS LANE', 'RAKE LN'],
        }

        UR35 = {
            "Lic_No": ['PG0000273'],
            "Reg_No": ['PG0000273/51'],
            "Service Number": ['53A PM/SUN'],
        }

        UR36 = {
            "Lic_No": ['PG0000421', 'PG0004444', 'PG0005006', 'PG0005052', 'PG0005316'],
            "Reg_No": ['PG0000421/298', 'PG0004444/1', 'PG0005006/1', 'PG0005052/1', 'PG0005316/1'],
            "Service Number": ['X18'],
        }

        UR37 = {
            "Lic_No": ['PG0006630', 'PG0006682', 'PG0006703', 'PG0006865', 'PG0006900'],
            "Reg_No": ['PG0006630/109', 'PG0006682/1', 'PG0006703/1', 'PG0006865/1', 'PG0006900/1'],
        }

        UR38 = {
            "Auth_Description": ['Cardiff County Council'],
            "Lic_No": ['PG0000273'],
            "Service_Type_Other_Details": ['SERVICE REVISED TIMETABLE'],
            "TAO Covered BY Area": ['Wales'],

        }

        UR39 = {
            "Auth_Description": ['City & County of Swansea'],
            "Lic_No": ['PG0000421'],
            "Service_Type_Other_Details": ['Vary route/timetable'],

        }

        UR40 = {
            "Auth_Description": ['Torfaen Council'],
            "Lic_No": ['PG0006630'],
            "Service_Type_Other_Details": ['Monday to Saturday Service. (TT changes in Pontypool and evening from Cwmbran)'],
            "TAO Covered BY Area": ['Wales'],
        }
        hardcoded = {
            name: val
            for name, val in locals().items()
            if isinstance(val, dict)
            and name.startswith("UR")
            and name[2:].isdigit()
        }

        self.case_names = getattr(self, "case_names", {})

        # Register using the numeric suffix as the case id (UR21 -> 21)
        for name in sorted(hardcoded.keys(), key=lambda nm: int(nm[2:])):
            cid = int(name[2:])
            ur_dict = hardcoded[name]

            # (recommended) drop empty list keys to avoid empty columns
            ur_dict = {k: v for k, v in ur_dict.items() if isinstance(v, list) and len(v) > 0}

            UR_df = self.create_flexible_dataframe(ur_dict)

            self.cases[cid] = UR_df
            self.case_names[cid] = f"tus_{cid}"
            
        

        

    def get_case(self, case_number):
        """
        Return the tuple (T, UR) for the specified case number.
        Defaults to case 1 if the given case is not found.
        """
        return self.cases[case_number]
