import sys
sys.path.append('/Users/rklop/text2sql model/TA-SQL/verieql')
sys.path.append('/Users/rklop/text2sql model/TA-SQL/verieql/z3py_libs')

import re
from run import parser, main
from verieql.verieql import verify_sql_equivalence
from join_csv import join_csv_files
import json
import csv
from src.modules import TASL, TALOG
from tqdm import tqdm
import traceback
import subprocess
from join_csv import join_csv_files

# I give it a .json path containing each question, a .json path containing the table definitions, and a .json path containing the constraints.
# It will then run the TA-SQL modes on each question, and then verify the SQL equivalence of the generated SQL and the gold SQL.
# It will then save the results to a .csv file.

def clean_up(generated_sql):
    
    def transform_backtick_content(match):
        """Transform the content inside backticks to a clean identifier."""
        content = match.group(1)  # Get content between backticks
        
        # Convert to uppercase
        content = content.upper()
        
        # Replace spaces with underscores
        content = content.replace(' ', '_')
        
        # Handle hyphens based on context:
        # - If it's between numbers (like "5-17"), replace with underscore ("5_17")
        # - If it's between letters and numbers (like "K-12"), remove the hyphen ("K12")
        content = re.sub(r'(\d+)-(\d+)', r'\1_\2', content)  # Numbers with hyphen -> underscore
        content = re.sub(r'([A-Z]+)-(\d+)', r'\1\2', content)  # Letters-number with hyphen -> remove hyphen
        
        # Remove non-alphanumeric characters except underscores
        # This removes parentheses, percent signs, slashes, etc.
        content = re.sub(r'[^A-Z0-9_]', '', content)
        
        # Clean up multiple consecutive underscores
        content = re.sub(r'_+', '_', content)
        
        # Remove leading/trailing underscores
        content = content.strip('_')
        
        return content
    
    # Find all backtick-quoted substrings and replace them
    # Pattern matches `anything inside backticks`
    pattern = r'`([^`]+)`'
    
    # Replace each backtick-quoted substring with its cleaned version
    generated_sql = re.sub(pattern, transform_backtick_content, generated_sql)
    
    return generated_sql

if __name__ == '__main__':
    
    opt = parser()
    #output_dic = main(opt)

    output_dic = json.load(open('./outputs/temp_test.json'))

    csv_path = './temp_data/temp_databases/verification_results.csv'
    csv_headers = ['bound_size', 'question_id', 'equivalent', 'counterexample', 'time_cost', 'generated_sql', 'gold_sql']

    '''
    cmd = [
        'python', 'evaluation/evaluation_ex.py',
        '--predicted_sql_path', './dev_data/dev.json',
        '--ground_truth_path', './dev_data/dev.sql',
        '--db_root_path', './dev_data/dev_databases/',
        '--diff_json_path', './dev_data/dev_sqlite.jsonl',
        '--output_log_path', './dev_data/output.log',
        '--sql_dialect', 'SQLite'
    ]

    result = subprocess.run(cmd, capture_output = True, text = True)
    print("DONE!")

    '''

    with open(csv_path, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
        writer.writeheader()
        
        for question_idx in tqdm(output_dic.keys(), total = len(output_dic)):  

            #print(f"Running veriEQL on question {question_idx}" + "*"*100)
            
            try:
                generated_sql = output_dic[question_idx]  

                # gold_sql_path = './temp_data/temp_databases/' + opt.mode + '.json'
                gold_sql_path = './temp_data/temp_databases/temp.json'
                
                with open(gold_sql_path, 'r') as f:
                    data = json.load(f)
                
                i = int(question_idx) 
                #print(f"I: {i}") 
                gold_sql = data[i]['SQL']
                gold_sql = gold_sql.upper()
                # print(f"GOLD_SQL: {gold_sql}")

                question_id = data[i]['question_id']
                #print(f"QUESTION_ID: {question_id}")
                database_id = data[i]['db_id']
                #print(f"DATABASE_ID: {database_id}")

                stopper = '\t'
                generated_sql = generated_sql.split(stopper)[0]
                generated_sql = ' '.join(generated_sql.split())
                generated_sql = generated_sql.upper()
                # print(f"GENERATED_SQL: {generated_sql}")

                generated_sql = clean_up(generated_sql)
                gold_sql = clean_up(gold_sql)
                
                #print(f"GENERATED_SQL: {generated_sql}")

                schema_path = './temp_data/temp_databases/table_definitions.json'
                with open(schema_path, 'r') as f:
                    schema = json.load(f)

                tables_definitions = schema[str(database_id)]
                #print(f"TABLES_DEFINITIONS: {tables_definitions}")
                
                constraints_path = './dev_data/dev_constraints.json'
                with open(constraints_path, 'r') as f:
                    constraints = json.load(f)

                integrity_constraints = constraints[str(database_id)][0]
                #print(f"INTEGRITY_CONSTRAINTS: {integrity_constraints}")

                for bound_size in range(1, 4):

                    print(f"Testing question {question_idx} with bound_size = {bound_size}")

                    config = {'generate_code': True, 'timer': True, 'show_counterexample': True}
                    verification_result = verify_sql_equivalence(generated_sql, gold_sql, tables_definitions, bound_size, integrity_constraints, **config)
                
                    #print(f"VERIFICATION_RESULT for bound_size {bound_size}: {verification_result}")

                    csv_row = {
                        'bound_size': bound_size,
                        'question_id': question_id,
                        'equivalent': verification_result['equivalent'],
                        'counterexample': str(verification_result['counterexample']) if verification_result['counterexample'] else '',
                        'time_cost': verification_result['time_cost'] if verification_result['time_cost'] else '',
                        'generated_sql': generated_sql,
                        'gold_sql': gold_sql
                    }

                    writer.writerow(csv_row)
                    csvfile.flush() 
                    
                    #print(f"Processed question {question_idx}: {question_id} with bound_size {bound_size}")
                
            except Exception as e:

                #print(f"Error processing question {question_idx}: {type(e).__name__}: {str(e)}")
                #print(f"Full traceback:")
                #traceback.print_exc()

                csv_row = {
                    'bound_size': bound_size,
                    'question_id': question_id if 'question_id' in locals() else question_idx,
                    'equivalent': 'ERROR',
                    'counterexample': f"{type(e).__name__}: {str(e)}",
                    'time_cost': '',
                    'generated_sql': generated_sql if 'generated_sql' in locals() else '',
                    'gold_sql': gold_sql if 'gold_sql' in locals() else ''
                }

                writer.writerow(csv_row)
                csvfile.flush()
    
    print(f"all results saved to {csv_path}")

    #join_csv_files()

    print("Done!")
