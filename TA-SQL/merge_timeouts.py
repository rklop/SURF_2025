#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Append timed-out rows (question_id, bound_size) into a base results CSV, and fill in:
# - res: from a results CSV (columns: question_id,res)
# - gold_sql: from either a dev.json (list of dicts) or a dev.sql (one SQL per line; index == question_id)
# - generated_sql: from either a JSON mapping {qid_str: sql} or dev_results.json with stopper formatting
#
# Example usage:
#   python merge_timeouts.py --base-csv alpha_combined_results.csv --timed-log alpha_fails --gold dev.sql --gen alpha_sql.json --res alpha_final_results.csv
#   python merge_timeouts.py --base-csv RESULTS_better.csv --timed-log timed_out_2 --gold dev.json --gen dev_results.json --res dev_ex_results.csv --out RESULTS_better_with_timeouts.csv

import argparse
import json
import sys
from datetime import datetime
from typing import Dict, Tuple, List, Optional

import pandas as pd
import re

STOPPER = "\t"


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def format_sql(s: Optional[str]) -> str:
    """Trim at first tab, collapse whitespace, uppercase."""
    if not s:
        return ""
    s = s.split(STOPPER)[0]
    s = " ".join(s.split())
    return s.upper()


def parse_timed_pairs(text: str) -> List[Tuple[int, int]]:
    """Extract (question_id, bound_size) from lines like .../csvs/163_5.csv or .../csvs2/163_5.csv ..."""
    pairs: List[Tuple[int,int]] = []
    for m in re.finditer(r"/csvs2?/(\d+)_(\d+)\.csv", text):
        qid = int(m.group(1))
        bound = int(m.group(2))
        pairs.append((qid, bound))
    return sorted(set(pairs))


def load_gold_map(path: str) -> Dict[int, str]:
    """Load gold SQL mapping from dev.json (list of dicts with question_id, SQL) or dev.sql (one SQL per line)."""
    gold_map: Dict[int, str] = {}
    lower = path.lower()

    if lower.endswith(".json"):
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            for ex in data:
                try:
                    qid = int(ex.get("question_id"))
                    sql = ex.get("SQL", "") or ""
                    gold_map[qid] = format_sql(sql)
                except Exception as e:
                    eprint(f"[gold_map] Skipping record: {e}")
        elif isinstance(data, dict):
            for k, v in data.items():
                try:
                    qid = int(k)
                    gold_map[qid] = format_sql(v or "")
                except Exception as e:
                    eprint(f"[gold_map] Skipping key {k}: {e}")
        else:
            raise ValueError("Unsupported JSON format for gold map.")
    elif lower.endswith(".sql"):
        with open(path, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
        for idx, line in enumerate(lines):
            gold_map[idx] = format_sql(line)
    else:
        raise ValueError("Unsupported gold file type. Use .json or .sql")
    return gold_map


def load_gen_map(path: str) -> Dict[int, str]:
    """Load generated SQL mapping from a JSON file ({qid_str: sql} or list with question_id, sql)."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    gen_map: Dict[int, str] = {}
    if isinstance(data, dict):
        for k, v in data.items():
            try:
                qid = int(k)
                gen_map[qid] = format_sql(v or "")
            except Exception as e:
                eprint(f"[gen_map] Skipping key {k}: {e}")
    elif isinstance(data, list):
        for ex in data:
            try:
                qid = int(ex.get("question_id"))
                sql = ex.get("sql") or ex.get("generated_sql") or ""
                gen_map[qid] = format_sql(sql)
            except Exception as e:
                eprint(f"[gen_map] Skipping record: {e}")
    else:
        raise ValueError("Unsupported JSON format for generated SQL map.")
    return gen_map


def load_res_map(path: str) -> Dict[int, str]:
    """Load res mapping from a CSV with columns: question_id,res"""
    df = pd.read_csv(path)
    if "question_id" not in df.columns or "res" not in df.columns:
        raise ValueError("res CSV must have columns: question_id,res")
    res_map: Dict[int, str] = {}
    for row in df.itertuples(index=False):
        try:
            qid = int(getattr(row, "question_id"))
            res = getattr(row, "res")
            res_map[qid] = res
        except Exception as e:
            eprint(f"[res_map] Skipping row: {e}")
    return res_map


def ensure_columns(df: pd.DataFrame, required: List[str]) -> pd.DataFrame:
    for c in required:
        if c not in df.columns:
            df[c] = None
    return df


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Append timed-out rows to a results CSV with res and SQLs.")
    parser.add_argument("--base-csv", required=True, help="Path to the base results CSV to update.")
    parser.add_argument("--timed-log", required=True, help="Path to file containing timed-out lines (/csvs/<qid>_<bound>.csv).")
    parser.add_argument("--gold", required=True, help="Path to gold SQL source (dev.json or dev.sql).")
    parser.add_argument("--gen", required=True, help="Path to generated SQL JSON (e.g., alpha_sql.json or dev_results.json).")
    parser.add_argument("--res", required=True, help="Path to CSV mapping question_id,res.")
    parser.add_argument("--out", default=None, help="Optional output CSV path. If omitted, overwrites base CSV.")
    parser.add_argument("--no-backup", action="store_true", help="Do not create a backup of the original base CSV.")
    args = parser.parse_args()

    base = pd.read_csv(args.base_csv)
    with open(args.timed_log, "r", encoding="utf-8") as f:
        timed_text = f.read()

    pairs = parse_timed_pairs(timed_text)
    print(f"Found {len(pairs)} timed-out (question_id, bound_size) pairs.")

    gold_map = load_gold_map(args.gold)
    print(f"Loaded {len(gold_map)} gold SQL entries from {args.gold}.")

    gen_map = load_gen_map(args.gen)
    print(f"Loaded {len(gen_map)} generated SQL entries from {args.gen}.")

    res_map = load_res_map(args.res)
    print(f"Loaded {len(res_map)} res entries from {args.res}.")

    required_cols = ["bound_size", "question_id", "equivalent", "counterexample",
                     "time_cost", "generated_sql", "gold_sql", "res"]
    base = ensure_columns(base, required_cols)

    # existing keys
    try:
        existing = set(zip(base["question_id"].astype(int), base["bound_size"].astype(int)))
    except Exception:
        base["question_id"] = base["question_id"].fillna(-1).astype(int)
        base["bound_size"] = base["bound_size"].fillna(-1).astype(int)
        existing = set(zip(base["question_id"], base["bound_size"]))

    rows_to_add = []
    for qid, bound in pairs:
        if (qid, bound) in existing:
            continue
        rows_to_add.append({
            "bound_size": bound,
            "question_id": qid,
            "equivalent": "TIMEOUT",
            "counterexample": "Timed out; no result CSV produced",
            "time_cost": None,
            "generated_sql": gen_map.get(qid, ""),
            "gold_sql": gold_map.get(qid, ""),
            "res": res_map.get(qid, None),
        })

    print(f"New rows to add: {len(rows_to_add)}")

    updated = pd.concat([base, pd.DataFrame(rows_to_add, columns=required_cols)], ignore_index=True)
    updated.sort_values(["question_id", "bound_size"], inplace=True)

    out_path = args.out or args.base_csv
    if not args.no_backup and args.out is None:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = f"{args.base_csv}.backup_{ts}.csv"
        base.to_csv(backup_path, index=False)
        print(f"Backup saved to: {backup_path}")

    updated.to_csv(out_path, index=False)
    print(f"Updated file saved to: {out_path}")
    print("Done.")


if __name__ == "__main__":
    main()
