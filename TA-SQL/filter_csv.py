#!/usr/bin/env python3

import csv
import os

def filter_csv():
    input_file = './dev_data/RESULTS_better.csv'
    output_file = './dev_data/RESULTS_better_filtered.csv'
    
    # Create backup of original file
    backup_file = './dev_data/RESULTS_better_backup.csv'
    if not os.path.exists(backup_file):
        print(f"Creating backup of original file as {backup_file}")
        os.system(f'cp "{input_file}" "{backup_file}"')
    
    kept_rows = 0
    removed_rows = 0
    
    print(f"Filtering {input_file} to keep only rows where question_id <= 1533")
    
    with open(input_file, 'r', newline='', encoding='utf-8') as infile:
        reader = csv.DictReader(infile)
        
        # Get the fieldnames from the original file
        fieldnames = reader.fieldnames
        
        with open(output_file, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)
            writer.writeheader()
            
            for row in reader:
                try:
                    question_id = int(row['question_id'])
                    if question_id <= 1533:
                        writer.writerow(row)
                        kept_rows += 1
                    else:
                        removed_rows += 1
                except (ValueError, KeyError) as e:
                    print(f"Warning: Could not parse question_id in row: {e}")
                    # Keep rows where we can't parse question_id (like header or malformed rows)
                    writer.writerow(row)
                    kept_rows += 1
    
    print(f"Filtering complete!")
    print(f"Kept {kept_rows} rows")
    print(f"Removed {removed_rows} rows")
    print(f"Output saved to {output_file}")
    
    # Replace original file with filtered version
    os.system(f'mv "{output_file}" "{input_file}"')
    print(f"Replaced original file with filtered version")

if __name__ == "__main__":
    filter_csv() 