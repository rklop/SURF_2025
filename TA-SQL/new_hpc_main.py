import sys
sys.path.append('/Users/rklop/text2sql model/TA-SQL/verieql')
sys.path.append('/Users/rklop/text2sql model/TA-SQL/verieql/z3py_libs')
import os

import re
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
import argparse

def clean_up(generated_sql):

    frpm_replacements = {
        "'FREE MEAL COUNT (K-12)'": "FREE_MEAL_COUNT_K12",
        "'FREE MEAL COUNT (AGES 5-17)'": "FREE_MEAL_COUNT_AGES_5_17",
        "'ENROLLMENT (K-12)'": "ENROLLMENT_K12",
        "'ENROLLMENT (AGES 5-17)'": "ENROLLMENT_AGES_5_17",
        "'PERCENT (%) ELIGIBLE FREE (K-12)'": "PERCENT_ELIGIBLE_FREE_K12",
        "'PERCENT (%) ELIGIBLE FREE (AGES 5-17)'": "PERCENT_ELIGIBLE_FREE_AGES_5_17",
        "'COUNT (K-12)'": "COUNT_K12",
        "'COUNT (AGES 5-17)'": "COUNT_AGES_5_17",
        "'SCHOOL NAME'": "SCHOOL_NAME",
        "'SCHOOL TYPE'": "SCHOOL_TYPE",
        "'DISTRICT NAME'": "DISTRICT_NAME",
        "'DISTRICT TYPE'": "DISTRICT_TYPE",
        "'EDUCATIONAL OPTION TYPE'": "EDUCATIONAL_OPTION_TYPE",
        "'NSLP PROVISION STATUS'": "NSLP_PROVISION_STATUS",
        "'CHARTER SCHOOL YN'": "CHARTER_SCHOOL_YN",
        "'CHARTER SCHOOL NUMBER'": "CHARTER_SCHOOL_NUMBER",
        "'CHARTER FUNDING TYPE'": "CHARTER_FUNDING_TYPE",
        "'IRC'": "IRC",
        "'LOW GRADE'": "LOW_GRADE",
        "'HIGH GRADE'": "HIGH_GRADE",
        "'ACADEMIC YEAR'": "ACADEMIC_YEAR",
        "'COUNTY CODE'": "COUNTY_CODE",
        "'DISTRICT CODE'": "DISTRICT_CODE",
        "'SCHOOL CODE'": "SCHOOL_CODE",
        "'COUNTY NAME'": "COUNTY_NAME",
        "'CHARTER'": "CHARTER",
        "'FUNDING TYPE'": "FUNDING_TYPE",
        "'CALPADS FALL1 CERTIFICATION STATUS'": "CALPADS_FALL1_CERTIFICATION_STATUS"
    }

    pattern = re.compile('|'.join(map(re.escape, frpm_replacements.keys())))
    generated_sql = pattern.sub(lambda match: frpm_replacements[match.group(0)], generated_sql)

    return generated_sql

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--question', type=int, required=True)
    parser.add_argument('--bound', type=int, required=True)
    args = parser.parse_args()

    question_idx = args.question
    bound_size = args.bound
    
    output_dic = json.load(open('./outputs/temp_test.json'))

    print(f"Running question {question_idx} with bound {bound_size}")

    csv_path = './outputs/' + str(question_idx) + '_' + str(bound_size) + '.csv'
    csv_headers = ['bound_size', 'question_id', 'equivalent', 'counterexample', 'time_cost', 'generated_sql', 'gold_sql']

    with open(csv_path, 'w', newline='') as csvfile:

        writer = csv.DictWriter(csvfile, fieldnames=csv_headers)
        writer.writeheader()
                    
        try:
            generated_sql = output_dic[str(question_idx)]  
            gold_sql_path = './temp_data/temp_databases/temp.json'

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

            # if the generated sql contains anything from the frqm table, replace the columns
            if 'FRPM' in generated_sql:
                generated_sql = clean_up(generated_sql)

            schema_path = './temp_data/temp_databases/table_definitions.json'
            with open(schema_path, 'r') as f:
                schema = json.load(f)

            print(f"DEBUG: database_id = '{database_id}'")
            print(f"DEBUG: type of schema = {type(schema)}")
            print(f"DEBUG: schema keys = {list(schema.keys())}")
            print(f"DEBUG: database_id in schema = {database_id in schema}")
            
            tables_definitions = schema[str(database_id)]
            
            print(f"DEBUG: type of tables_definitions = {type(tables_definitions)}")
            print(f"DEBUG: tables_definitions = {tables_definitions}")
            
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