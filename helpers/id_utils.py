def detect_id_column(df):
    candidates = [
        "Identifiant",
        "id_assessment",
        "synthetic_id",
    ]
    for col in candidates:
        if col in df.columns:
            return col
    return None
