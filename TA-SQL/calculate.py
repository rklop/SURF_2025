import pandas as pd
import argparse

def calculate_breakdown(file_path) -> tuple:

    df_omni_new = pd.read_csv(file_path)

    # Identify all false positives
    false_positives_omni_new = df_omni_new[(df_omni_new['equivalent'] == 'False') & (df_omni_new['res'] == 'correct')]

    # Get all question_ids for false positives
    false_positive_ids_omni_new = false_positives_omni_new['question_id'].unique()

    # Remove any entry with those question_ids
    filtered_df_omni_new = df_omni_new[~df_omni_new['question_id'].isin(false_positive_ids_omni_new)]

    # Calculate breakdown of 'res'
    res_counts_omni_new = filtered_df_omni_new['res'].value_counts()
    res_percentages_omni_new = filtered_df_omni_new['res'].value_counts(normalize=True) * 100

    # Counts for easy calculations
    original_count_omni_new = len(df_omni_new)
    removed_count_omni_new = original_count_omni_new - len(filtered_df_omni_new)
    remaining_count_omni_new = len(filtered_df_omni_new)

    # Output
    print(f"Original count: {original_count_omni_new}")
    print(f"Removed count: {removed_count_omni_new}")
    print(f"Remaining count: {remaining_count_omni_new}")
    print(f"False positive count: {len(false_positive_ids_omni_new)}")
    print(f"Res counts: {res_counts_omni_new}")
    final_ex_score = res_counts_omni_new["correct"] / original_count_omni_new
    print(f"Final EX Score: {final_ex_score:.4%}")

if __name__ == "__main__":

    ap = argparse.ArgumentParser(
        description="Calculate breakdown of results from a CSV file."
    )
    ap.add_argument("--input", required=True, help="Path to CSV file.")
    args = ap.parse_args()

    calculate_breakdown(args.input)

