import sys
sys.path.append('/Users/rklop/text2sql model/TA-SQL/verieql')
sys.path.append('/Users/rklop/text2sql model/TA-SQL/verieql/z3py_libs')
import os

from run import parser, main
from verieql.verieql import verify_sql_equivalence
from join_csv import join_csv_files
import json
import csv
from src.modules import TASL, TALOG
import tqdm
import traceback
import subprocess
from join_csv import join_csv_files

if __name__ == '__main__':

    task_id = int(os.environ.get('SLURM_ARRAY_TASK_ID'))
        
    opt = parser()
    output_dic = json.load(open('./outputs/temp_test.json'))

    question_idx = int(task_id // 5) 
    bound_size = int(task_id % 5) + 1

    print(f"Running question {question_idx} with bound {bound_size}")

    csv_path = './outputs/' + str(question_idx) + '_' + str(bound_size) + '.csv'
    csv_headers = ['bound_size', 'question_id', 'equivalent', 'counterexample', 'time_cost', 'generated_sql', 'gold_sql']

    with open(csv_path, 'w', newline='') as csvfile:

        writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
        writer.writeheader()
                    
        try:
            generated_sql = output_dic[str(question_idx)]  
            gold_sql_path = './temp_data/temp_databases/' + opt.mode + '.json'

            with open(gold_sql_path, 'r') as f:
                data = json.load(f)
            
            i = int(question_idx) 

            gold_sql = data[i]['SQL']
            gold_sql = gold_sql.upper()
            print(f"GOLD_SQL: {gold_sql}")

            question_id = data[i]['question_id']
            database_id = data[i]['db_id']

            stopper = '\t'
            generated_sql = generated_sql.split(stopper)[0]
            generated_sql = ' '.join(generated_sql.split())
            generated_sql = generated_sql.upper()
            print(f"GENERATED_SQL: {generated_sql}")

            schema_path = './temp_data/temp_databases/table_definitions.json'
            with open(schema_path, 'r') as f:
                schema = json.load(f)

            tables_definitions = schema[str(database_id)]  
            
            constraints_path = './temp_data/temp_databases/constraints.json'
            with open(constraints_path, 'r') as f:
                constraints = json.load(f)

            integrity_constraints = constraints[str(database_id)][0]

            config = {'generate_code': True, 'timer': True, 'show_counterexample': True}
            verification_result = verify_sql_equivalence(generated_sql, gold_sql, tables_definitions, bound_size, integrity_constraints, **config)
        
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
                        
        except Exception as e:

            traceback.print_exc()

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





    