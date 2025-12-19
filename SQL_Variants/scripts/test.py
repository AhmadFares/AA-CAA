import pandas as pd

# Load CSV
df = pd.read_csv(
    "/Users/ahmaddfaress/Desktop/TVD/data/experiment_results/Experiment1.csv"
)

# Save as Excel
df.to_excel("output.xlsx", index=False)
