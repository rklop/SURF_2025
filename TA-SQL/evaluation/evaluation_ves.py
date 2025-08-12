import os
import pdb
import sys
import json
import argparse
import sqlite3
import multiprocessing as mp
from func_timeout import func_timeout, FunctionTimedOut

def result_callback(result):
    exec_result.append(result)


def execute_sql(sql, db_path):
    # Connect to the database
    conn = sqlite3.connect(db_path)
    # Create a cursor object
    cursor = conn.cursor()
    cursor.execute(sql)
    return cursor.fetchall()

def iterated_execute_sql(predicted_sql,ground_truth,db_path,iterate_num):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(predicted_sql)
    predicted_res = cursor.fetchall()
    cursor.execute(ground_truth)
    ground_truth_res = cursor.fetchall()
    ex = 0  # Binary EX flag: 1 if results match, 0 if not
    if set(predicted_res) == set(ground_truth_res):
        ex = 1  # Results match
    return ex



def execute_model(predicted_sql,ground_truth, db_place, idx, meta_time_out):
    try:
        ex = func_timeout(meta_time_out, iterated_execute_sql,
                                  args=(predicted_sql, ground_truth, db_place, 1))
    except KeyboardInterrupt:
        sys.exit(0)
    except FunctionTimedOut:
        ex = 0
    except Exception as e:
        ex = 0  # possibly len(query) > 512 or not executable
    result = {'sql_idx': idx, 'ex': ex}
    return result


def package_sqls(sql_path, db_root_path, mode='gpt', data_mode='dev'):
    clean_sqls = []
    db_path_list = []
    if mode == 'gpt':
        sql_data = json.load(open(sql_path + 'predict_' + data_mode + '.json', 'r'))
        for idx, sql_str in sql_data.items():
            if type(sql_str) == str:
                sql, db_name = sql_str.split('\t----- bird -----\t')
                # sql = 'SELECT ' + sql
            else:
                sql, db_name = " ", "financial"
            clean_sqls.append(sql)
            db_path_list.append(db_root_path + db_name + '/' + db_name + '.sqlite')

    elif mode == 'gt':
        sqls = open(sql_path + data_mode + '_gold.sql')
        sql_txt = sqls.readlines()
        for idx, sql_str in enumerate(sql_txt):
            sql, db_name = sql_str.strip().split('\t')
            clean_sqls.append(sql)
            db_path_list.append(db_root_path + db_name + '/' + db_name + '.sqlite')

    return clean_sqls, db_path_list

def run_sqls_parallel(sqls, db_places, num_cpus=1, meta_time_out=30.0):
    pool = mp.Pool(processes=num_cpus)
    for i,sql_pair in enumerate(sqls):
        predicted_sql, ground_truth = sql_pair
        # print(f"{i}_th sql: {predicted_sql}")
        pool.apply_async(execute_model, args=(predicted_sql, ground_truth, db_places[i], i, meta_time_out), callback=result_callback)
    pool.close()
    pool.join()

def sort_results(list_of_dicts):
  return sorted(list_of_dicts, key=lambda x: x['sql_idx'])

def compute_ex(exec_results):
    """Compute Execution Accuracy (EX) - percentage of queries with correct results"""
    num_queries = len(exec_results)
    correct_queries = sum([result['ex'] for result in exec_results])
    ex_accuracy = (correct_queries / num_queries) * 100
    return ex_accuracy

def compute_ex_by_diff(exec_results,diff_json_path):
    num_queries = len(exec_results)
    
    # If no difficulty file is provided, just compute overall EX
    if not diff_json_path or diff_json_path == '':
        all_ex = compute_ex(exec_results)
        return all_ex, all_ex, all_ex, all_ex, [num_queries, num_queries, num_queries, num_queries]
    
    contents = load_json(diff_json_path)
    simple_results, moderate_results, challenging_results = [], [], []
    for i,content in enumerate(contents):
        if content['difficulty'] == 'simple':
            simple_results.append(exec_results[i])
        if content['difficulty'] == 'moderate':
            moderate_results.append(exec_results[i])
        if content['difficulty'] == 'challenging':
            challenging_results.append(exec_results[i])
    simple_ex = compute_ex(simple_results) if simple_results else 0
    moderate_ex = compute_ex(moderate_results) if moderate_results else 0
    challenging_ex = compute_ex(challenging_results) if challenging_results else 0
    all_ex = compute_ex(exec_results)
    count_lists = [len(simple_results), len(moderate_results), len(challenging_results), num_queries]
    return simple_ex, moderate_ex, challenging_ex, all_ex, count_lists

def load_json(dir):
    with open(dir, 'r') as j:
        contents = json.loads(j.read())
    return contents

def print_data(score_lists,count_lists):
    # levels = ['simple', 'moderate', 'challenging', 'total']
    # print("{:20} {:20} {:20} {:20} {:20}".format("", *levels))
    # print("{:20} {:<20} {:<20} {:<20} {:<20}".format('count', *count_lists))

    print('=========================================    EX   ========================================')
    print("{:20} {:<20.2f} {:<20.2f} {:<20.2f} {:<20.2f}".format('ex', *score_lists))

if __name__ == '__main__':
    args_parser = argparse.ArgumentParser()
    args_parser.add_argument('--predicted_sql_path', type=str, required=True, default='')
    args_parser.add_argument('--ground_truth_path', type=str, required=True, default='')
    args_parser.add_argument('--data_mode', type=str, required=True, default='dev')
    args_parser.add_argument('--db_root_path', type=str, required=True, default='')
    args_parser.add_argument('--num_cpus', type=int, default=1)
    args_parser.add_argument('--meta_time_out', type=float, default=30.0)
    args_parser.add_argument('--mode_gt', type=str, default='gt')
    args_parser.add_argument('--mode_predict', type=str, default='gpt')
    args_parser.add_argument('--diff_json_path',type=str,default='')
    args = args_parser.parse_args()
    exec_result = []
    
    pred_queries, db_paths = package_sqls(args.predicted_sql_path, args.db_root_path, mode=args.mode_predict,
                                          data_mode=args.data_mode)
    # generate gt sqls:
    gt_queries, db_paths_gt = package_sqls(args.ground_truth_path, args.db_root_path, mode='gt',
                                           data_mode=args.data_mode)

    query_pairs = list(zip(pred_queries, gt_queries))
    run_sqls_parallel(query_pairs, db_places=db_paths, num_cpus=args.num_cpus, meta_time_out=args.meta_time_out)
    exec_result = sort_results(exec_result)
    # print('start calculate')
    simple_ex, moderate_ex, challenging_ex, ex, count_lists = \
        compute_ex_by_diff(exec_result, args.diff_json_path)
    score_lists = [simple_ex, moderate_ex, challenging_ex, ex]
    print_data(score_lists, count_lists)
    print('===========================================================================================')
    print("Finished evaluation")


