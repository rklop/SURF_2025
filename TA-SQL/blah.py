import pandas as pd

# Load the CSV file
file_path = '/mnt/data/alpha_results_with_timeout.csv'
df = pd.read_csv(file_path)

# Remove entries where 'equivalent' == False and 'res' == True
filtered_df = df[~((df['equivalent'] == False) & (df['res'] == True))]

# Calculate numerical breakdown of 'res'
res_counts = filtered_df['res'].value_counts()
res_percentages = filtered_df['res'].value_counts(normalize=True) * 100

res_counts, res_percentages