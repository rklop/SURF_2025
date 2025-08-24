#!/usr/bin/env python3
"""
Reads a input csv where the counterexample column contains the full VeriEQL counterexample block:

    <CREATE/INSERT...>
    -- ----------sql1------------
    <query 1>
    -- ----------sql2------------
    <query 2>

For each row:
  - parse the block into setup_sql, sql1, sql2
  - run filters on setup_sql to clean up the database
  - load setup_sql into a fresh in-memory SQLite DB
  - run sql1 and sql2
  - compare normalized results 
  - write a summary CSV with inlined results

Usage:
  python verify_verieql_cases.py 
      --input alpha_results_with_timeout_FP.csv 
      --out results.csv 
      --order_sensitive # if true, consider order-sensitive equality, default is order-insensitive.
      --list # if true # list the incorrect attacks
      --update_original # if true, update the original CSV with the results


"""

import argparse
import csv
import json
import sqlite3
import sys
from typing import Any, Dict, List, Tuple
import re
import pandas as pd
import os
from calculate import calculate_breakdown
from pathlib import Path
from collections import Counter

ORDER_BY_RE = re.compile(r'\border\s+by\b', flags=re.IGNORECASE)

def has_order_by(sql: str) -> bool:
    return bool(ORDER_BY_RE.search(sql))

RESERVED = {"ORDER"}

def generate_false_positives(input_path: str):

    df = pd.read_csv(input_path)
    mask  = (df["res"] == "correct") & (df["equivalent"] == "False")
    df_fp = df[mask]

    df_fp_ids = df_fp["question_id"].unique()

    df_filtered = df[df["question_id"].isin(df_fp_ids)]

    false_positives_path_filtered = Path(input_path).with_name("false_positives.csv")

    df_filtered.to_csv(false_positives_path_filtered, index=False)

    temporary_path = Path(input_path).with_name("temp.csv")

    df_fp.to_csv(temporary_path, index=False)

    return temporary_path

def quote_hyphenated_identifiers(sql: str) -> str:

    """
    Quote bare identifiers that contain a hyphen (e.g., T-BIL → "T-BIL").
    Skips over string literals ('...'), quoted identifiers ("..."), and comments (--, /* ... */).
    Not a full SQL parser, but robust enough for VeriEQL counterexamples.
    """

    out = []
    i, n = 0, len(sql)

    in_single = False   # inside '...'
    in_double = False   # inside "..."
    in_line = False     # inside -- ... \n
    in_block = False    # inside /* ... */

    while i < n:
        ch = sql[i]

        if in_line:
            out.append(ch)
            if ch == '\n':
                in_line = False
            i += 1
            continue
        if in_block:
            out.append(ch)
            if ch == '*' and i + 1 < n and sql[i+1] == '/':
                out.append('/')
                i += 2
                in_block = False
            else:
                i += 1
            continue
        if in_single:
            out.append(ch)
            if ch == "'" and i + 1 < n and sql[i+1] == "'":  # escaped ''
                out.append("'")
                i += 2
            elif ch == "'":
                i += 1
                in_single = False
            else:
                i += 1
            continue
        if in_double:
            out.append(ch)
            if ch == '"' and i + 1 < n and sql[i+1] == '"':  # escaped ""
                out.append('"')
                i += 2
            elif ch == '"':
                i += 1
                in_double = False
            else:
                i += 1
            continue

        # Entering comment/string?
        if ch == '-' and i + 1 < n and sql[i+1] == '-':
            out.append(ch); out.append(sql[i+1])
            i += 2
            in_line = True
            continue
        if ch == '/' and i + 1 < n and sql[i+1] == '*':
            out.append(ch); out.append(sql[i+1])
            i += 2
            in_block = True
            continue
        if ch == "'":
            out.append(ch); i += 1; in_single = True; continue
        if ch == '"':
            out.append(ch); i += 1; in_double = True; continue

        if ch.isalpha() or ch == '_':
            j = i + 1
            while j < n and (sql[j].isalnum() or sql[j] in ['_', '-']):
                j += 1
            token = sql[i:j]
            if '-' in token:
                out.append('"'); out.append(token); out.append('"')
            else:
                out.append(token)
            i = j
            continue

        out.append(ch)
        i += 1

    return ''.join(out)

def fix_unescaped_apostrophes(sql: str) -> str:

    """
    Inside single-quoted string literals, turn lone apostrophes (')
    into doubled quotes ('') unless they are already escaped (''').
    This is a heuristic fixer for data like ANCESTOR'S CHOSEN.
    """

    out = []
    i = 0
    n = len(sql)
    in_single = False
    while i < n:
        ch = sql[i]
        if not in_single:
            if ch == "'":
                in_single = True
                out.append(ch)
                i += 1
            else:
                out.append(ch)
                i += 1
        else:
            # we're inside a '...'
            if ch == "'":
                # if next char is also ', it's an escaped quote -> keep as-is and skip both
                if i + 1 < n and sql[i+1] == "'":
                    out.append("''")
                    i += 2
                else:

                    if i + 1 < n and sql[i+1].isalpha():
                        out.append("''")  # escape it
                        i += 1
                    else:
                        # treat as terminator
                        out.append("'")
                        i += 1
                        in_single = False
            else:
                out.append(ch)
                i += 1
    return "".join(out)

def quote_reserved_columns(sql: str) -> str:

    def _repl(m):

        return f'"{m.group(0)}"'
    
    for word in RESERVED:
        if word.upper() == "ORDER":
            # quote ORDER unless it's ORDER BY (ignore case)
            sql = re.sub(r'(?<!")\bORDER\b(?!")(?!(\s+BY\b))', _repl, sql, flags=re.IGNORECASE)
        else:
            sql = re.sub(rf'(?<!")\b{word}\b(?!")', _repl, sql, flags=re.IGNORECASE)

    return sql

def normalize_cell(x: Any) -> Any:

    """Make SQLite cell values comparable."""

    if x is None or isinstance(x, (int, float, str)):
        return x
    try:
        if isinstance(x, (bytes, bytearray)):
            return x.decode("utf-8")
    except Exception:
        pass
    return str(x)

def normalize_result(cursor, rows: List[Tuple]) -> Dict[str, Any]:

    cols = [d[0] for d in (cursor.description or [])]
    norm_rows = [tuple(normalize_cell(v) for v in r) for r in rows]

    return {"columns": cols, "rows": norm_rows}

def results_equal(res1: Dict[str, Any], res2: Dict[str, Any], *, order_sensitive: bool) -> bool:

    if res1["columns"] != res2["columns"]:
        return False
    
    if order_sensitive:
        return res1["rows"] == res2["rows"]
    
    else:

        def as_bag(rows):
            return Counter(json.dumps(r, ensure_ascii=False, sort_keys=True) for r in rows) # dict containing comparable JSON strings as rows
        
        return as_bag(res1["rows"]) == as_bag(res2["rows"])

def maybe_scalar(res: Dict[str, Any]) -> Any:

    if len(res.get("rows", [])) == 1 and len(res.get("columns", [])) == 1:
        return res["rows"][0][0]
    
    return None

def run_sqlite_case(setup_sql: str, q1: str, q2: str, *, order_sensitive: bool) -> Dict[str, Any]:

    out: Dict[str, Any] = {

        # equal will now be a string: "True" | "False" | "Error"

        "equal": "Error",
        "q1_ok": False, "q2_ok": False,
        "q1_error": "", "q2_error": "",
        "q1_columns": None, "q2_columns": None,
        "q1_rowcount": None, "q2_rowcount": None,
        "q1_rows_sample": None, "q2_rows_sample": None,
        "q1_scalar": None, "q2_scalar": None,
        "_q1_full": None, "_q2_full": None,
        "generated_sql": q1, "gold_sql": q2,
        "order_sensitive": order_sensitive,
        "generated_sql_has_order_by": has_order_by(q1),
        "gold_sql_has_order_by": has_order_by(q2)
    }

    con = sqlite3.connect(":memory:")

    try:
        con.executescript("""
            PRAGMA foreign_keys = OFF;
            PRAGMA journal_mode = OFF;
            PRAGMA synchronous = OFF;
            PRAGMA temp_store = MEMORY;
            PRAGMA cache_size = -100000;
        """)

        setup_sql = fix_unescaped_apostrophes(setup_sql)
        setup_sql = quote_hyphenated_identifiers(setup_sql)
        setup_sql = quote_reserved_columns(setup_sql)
        con.executescript(setup_sql)

    except Exception as e:
        out["q1_error"] = f"setup_error: {e}"
        con.close()
        return out  # equal stays "Error"

    # generated_sql
    try:
        cur1 = con.execute(q1)
        rows1 = cur1.fetchall()
        res1 = normalize_result(cur1, rows1)
        out["q1_ok"] = True
        out["q1_columns"] = res1["columns"]
        out["q1_rowcount"] = len(res1["rows"])
        out["q1_rows_sample"] = res1["rows"][:10]
        out["q1_scalar"] = maybe_scalar(res1)
        out["_q1_full"] = res1
        out["generated_sql"] = q1
    except Exception as e:
        out["q1_error"] = str(e)

    # gold_sql
    try:
        cur2 = con.execute(q2)
        rows2 = cur2.fetchall()
        res2 = normalize_result(cur2, rows2)
        out["q2_ok"] = True
        out["q2_columns"] = res2["columns"]
        out["q2_rowcount"] = len(res2["rows"])
        out["q2_rows_sample"] = res2["rows"][:10]
        out["q2_scalar"] = maybe_scalar(res2)
        out["_q2_full"] = res2
        out["gold_sql"] = q2
    except Exception as e:
        out["q2_error"] = str(e)

    # Only compute equality if both queries succeeded

    if out["q1_ok"] and out["q2_ok"]:
        out["equal"] = "True" if results_equal(res1, res2, order_sensitive=order_sensitive) else "False"
    else:
        out["equal"] = "Error"

    con.close()
    return out

SQL1_MARK = "-- ----------sql1------------"
SQL2_MARK = "-- ----------sql2------------"

def strip_leading_comment_lines(s: str) -> str:

    """Drop leading lines that start with '--' inside a query segment."""

    lines = s.strip().splitlines()
    out = []
    started = False
    for ln in lines:
        t = ln.strip()
        if not started and t.startswith("--"):
            continue
        started = True
        out.append(ln)
    return "\n".join(out).strip()

def parse_block_text(block: str) -> List[Dict[str, str]]:

    """
    Parse ONE string that may contain one or more counterexample blocks.
    A block must contain both markers. Returns a list of {setup_sql, sql1, sql2}.
    """

    chunks = [b.strip() for b in block.split("~~~~~~~~~~~") if b.strip()]
    if not chunks:
        chunks = [block.strip()]

    out: List[Dict[str, str]] = []
    for b in chunks:
        if SQL1_MARK not in b or SQL2_MARK not in b:
            # skip malformed sub-blocks
            continue
        pre, after_sql1 = b.split(SQL1_MARK, 1)
        sql1_part, sql2_part = after_sql1.split(SQL2_MARK, 1)
        setup_sql = pre.strip()
        sql1 = strip_leading_comment_lines(sql1_part)
        sql2 = strip_leading_comment_lines(sql2_part)

        # print(f"sql1: {sql1}")
        # print(f"sql2: {sql2}")

        if setup_sql and sql1 and sql2:
            out.append({"setup_sql": setup_sql, "sql1": sql1, "sql2": sql2})
    return out

def read_cases_from_block_csv(path: str) -> List[Dict[str, str]]:

    block_col = "counterexample"
    id_col = "question_id"

    """
    Read rows from a CSV that has:
      - one column 'block_col' holding the entire counterexample text
      - optional 'id_col' used for case_id (else row index)
    Returns a list of {case_id, setup_sql, sql1, sql2}.
    """

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    cases: List[Dict[str, str]] = []
    with p.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Normalize fieldnames (lower-case) for robust access
        fns = [fn.lower() for fn in (reader.fieldnames or [])]
        if block_col.lower() not in fns:
            raise ValueError(
                f"CSV must contain a '{block_col}' column; found: {fns}"
            )

        for i, row in enumerate(reader, 1):
            bound_size = row.get("bound_size")
            # print(f"bound_size: {bound_size}")

            row_lower = {k.lower(): v for k, v in row.items()}
            raw_block = (row_lower.get(block_col.lower()) or "").strip()
            if not raw_block:
                continue

            blocks = parse_block_text(raw_block)
            if not blocks:
                # no valid markers found in this row
                continue

            base_id = (row_lower.get(id_col.lower()) or f"row_{i}")
            if len(blocks) == 1:
                b = blocks[0]
                cases.append({
                    "bound_size": bound_size,
                    "question_id": str(base_id),
                    "setup_sql": b["setup_sql"],
                    "sql1": b["sql1"],
                    "sql2": b["sql2"],
                })
            else:
                # rare, but support multiple blocks inside one cell
                for j, b in enumerate(blocks, 1):
                    cases.append({
                        "bound_size": bound_size,
                        "question_id": f"{base_id}_blk{j}",
                        "setup_sql": b["setup_sql"],
                        "sql1": b["sql1"],
                        "sql2": b["sql2"],
                    })
    return cases

def write_results(out_path: str, results: List[Dict[str, Any]]) -> None:
    """
    Write a summary CSV. Inline full rows for small outputs (<= inline_max_rows).
    """

    inline_max_rows = 100

    fieldnames = [
        "bound_size",
        "question_id", "equal",
        "generated_sql_error", "gold_sql_error",
        # "order_sensitive",
        "generated_sql_columns", "gold_sql_columns",
        #"q1_rowcount", "q2_rowcount",
        #"q1_rows_sample", "q2_rows_sample",
        #"q1_fingerprint", "q2_fingerprint",
        "generated_sql_results", "gold_sql_results",
        "generated_sql_scalar", "gold_sql_scalar",
        "generated_sql", "gold_sql",
        #"q1_json", "q2_json",
        "generated_sql_ok", "gold_sql_ok"
    ]

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for r in results:
            
            row = {
                "bound_size": r.get("bound_size"),
                "question_id": r.get("question_id"),
                "equal": r.get("equal"),
                # "order_sensitive": r.get("order_sensitive"),
                "generated_sql_error": r.get("q1_error"),
                "gold_sql_error": r.get("q2_error"),
                "generated_sql_scalar": r.get("q1_scalar"),
                "gold_sql_scalar": r.get("q2_scalar"),
                "generated_sql_ok": r.get("q1_ok"),
                "gold_sql_ok": r.get("q2_ok"),
                "generated_sql": r.get("generated_sql"),
                "gold_sql": r.get("gold_sql"),
                "generated_sql_columns": r.get("q1_columns"),
                "gold_sql_columns": r.get("q2_columns")
            }

            for side in (("generated_sql", "q1"), ("gold_sql", "q2")):
                full = r.get(f"_{side[1]}_full")
                if full and inline_max_rows and len(full["rows"]) <= inline_max_rows:
                    row[f"{side[0]}_results"] = json.dumps(full["rows"], ensure_ascii=False)
                else:
                    row[f"{side[0]}_results"] = ""

            w.writerow(row)

def main():

    ap = argparse.ArgumentParser(
        description="Verify VeriEQL counterexamples from a CSV column containing the full block."
    )
    ap.add_argument("--input", required=True, help="Path to CSV with a 'counterexample' column.")
    ap.add_argument("--out", default="verieql_verification_results.csv", help="Output CSV path.")
    ap.add_argument("--order_sensitive", action="store_true",
                    help="If true, consider order-sensitive equality, default is order-insensitive.")
    ap.add_argument("--list", action="store_true",
                    help="If true, list the incorrect attacks.")
    ap.add_argument("--update_original", action="store_true",
                    help="If true, update the original CSV with the results.")
    args = ap.parse_args()

    false_positives = generate_false_positives(args.input)

    cases = read_cases_from_block_csv(
        false_positives
    )

    if not cases:
        print("No valid cases found (are the sql1/sql2 markers present in the 'counterexample' text?).", file=sys.stderr)
        sys.exit(2)

    results: List[Dict[str, Any]] = []

    for c in cases:
        res = run_sqlite_case(c["setup_sql"], c["sql1"], c["sql2"], order_sensitive = args.order_sensitive)
        res["question_id"] = c["question_id"]
        res["bound_size"] = c["bound_size"]
        res["generated_sql"] = c["sql1"]
        res["gold_sql"] = c["sql2"]
        results.append(res)

    write_results(
        args.out,
        results
    )

    df = pd.read_csv(args.out)
    incorrect_attacks = df[df["equal"] == True]
    print(f"Amount of incorrect attacks: {len(incorrect_attacks)}")

    if args.list:
        for i, attack in incorrect_attacks.iterrows():
            print(f"Incorrect attack: Question ID: {attack['question_id']}, Bound: {attack['bound_size']}")

    df_final = pd.read_csv(args.input)
    
    for i, attack in incorrect_attacks.iterrows():

        mask = (
            (df_final["question_id"] == attack["question_id"]) &
            (df_final["bound_size"] == attack["bound_size"])
        )

        df_final.loc[mask, "equivalent"] = "Failed Attack"
    
    final_path = Path(args.input).with_name("ATTACK_UPDATED.csv")
    df_final.to_csv(final_path, index=False)

    calculate_breakdown(final_path)

    if not args.update_original:
        os.remove(final_path)

    # delete the temp file
    os.remove(false_positives)

    print(f"Wrote {len(results)} results to {args.out}")

if __name__ == "__main__":
    main()
