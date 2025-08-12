#!/usr/bin/env python3

try:
    import pandas as pd
except ImportError:
    print("Pandas not available, trying to install...")
    import subprocess
    import sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas"])
    import pandas as pd

# Read the CSV file
df = pd.read_csv('dev_data/combined_results.csv')

print("Original data shape:", df.shape)
print("Original data columns:", df.columns.tolist())

# Create false_positives.csv
# Filter for equivalent=False AND res=correct
false_positives = df[(df['equivalent'] == "False") & (df['res'] == 'correct')]
print(f"False positives shape: {false_positives.shape}")
false_positives.to_csv('dev_data/false_positives.csv', index=False)
print("Saved false_positives.csv")

# Create false_negatives.csv  
# Filter for equivalent=True AND res=incorrect
false_negatives = df[(df['equivalent'] == "True") & (df['res'] == 'incorrect')]
print(f"False negatives shape: {false_negatives.shape}")
false_negatives.to_csv('dev_data/false_negatives.csv', index=False)
print("Saved false_negatives.csv")

print("Done!") 