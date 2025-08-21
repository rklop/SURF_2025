#!/usr/bin/env python3
"""
verify_verieql_from_block_csv.py
--------------------------------
Reads a CSV where ONE column (default: 'counterexample') contains the full VeriEQL
counterexample block:

    <CREATE/INSERT...>
    -- ----------sql1------------
    <query 1>
    -- ----------sql2------------
    <query 2>

For each row:
  - parse the block into setup_sql, sql1, sql2
  - load setup_sql into a fresh in-memory SQLite DB
  - run sql1 and sql2
  - compare normalized results (order-insensitive)
  - write a summary CSV (with inlined full results when small)

Usage:
  python verify_verieql_from_block_csv.py \
      --input alpha_results_with_timeout_FP.csv \
      --out results.csv \
      --block-col counterexample \
      --id-col question_id
"""

import argparse
import csv
import json
import sqlite3
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple
import re

# ------------------------------
# Helpers: normalize and compare
# ------------------------------

RESERVED = {"ORDER"}

def quote_reserved_columns(sql: str) -> str:
    """
    Quote reserved words when they are used as bare table names in DDL/DML.
    This is a pragmatic sanitizer; it won't fully parse SQL but handles common patterns.
    """

    def _repl(m):
        word = m.group(0)
        return f'"{word}"'

    for word in RESERVED:
        pattern = rf'(?<!")\b{word}\b(?!")'
        sql = re.sub(pattern, _repl, sql)
    
    return sql

def normalize_cell(x: Any) -> Any:
    """Make SQLite cell values JSON-serializable and comparable."""
    if x is None or isinstance(x, (int, float, str)):
        return x
    try:
        if isinstance(x, (bytes, bytearray)):
            return x.decode("utf-8")
    except Exception:
        pass
    return str(x)

def normalize_result(cursor, rows: List[Tuple]) -> Dict[str, Any]:
    cols = [d[0] for d in cursor.description] if cursor.description else []
    norm_rows = [tuple(normalize_cell(v) for v in r) for r in rows]

    # Use JSON string as the sort key to avoid type-comparison errors
    norm_rows_sorted = sorted(
        norm_rows,
        key=lambda r: json.dumps(r, sort_keys=True, ensure_ascii=False)
    )
    return {"columns": cols, "rows": norm_rows_sorted}

def result_fingerprint(res: Dict[str, Any]) -> str:
    """Stable SHA-256 over columns+rows."""
    import hashlib
    payload = json.dumps(res, sort_keys=True, ensure_ascii=False)
    return "__sha256__:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()

def maybe_scalar(res: Dict[str, Any]) -> Any:
    """If result is 1×1, return that scalar (handy for COUNT(*))."""
    if len(res.get("rows", [])) == 1 and len(res.get("columns", [])) == 1:
        return res["rows"][0][0]
    return None

# ------------------------------
# Core executor
# ------------------------------

def run_sqlite_case(setup_sql: str, q1: str, q2: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        # equal will now be a string: "True" | "False" | "Error"
        "equal": "Error",
        "q1_ok": False, "q2_ok": False,
        "q1_error": "", "q2_error": "",
        "q1_columns": None, "q2_columns": None,
        "q1_rowcount": None, "q2_rowcount": None,
        "q1_rows_sample": None, "q2_rows_sample": None,
        "q1_fingerprint": "", "q2_fingerprint": "",
        "q1_scalar": None, "q2_scalar": None,
        "_q1_full": None, "_q2_full": None,
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
        setup_sql = quote_reserved_columns(setup_sql)
        con.executescript(setup_sql)
    except Exception as e:
        out["q1_error"] = f"setup_error: {e}"
        con.close()
        return out  # equal stays "Error"

    # q1
    try:
        cur1 = con.execute(q1)
        rows1 = cur1.fetchall()
        res1 = normalize_result(cur1, rows1)
        out["q1_ok"] = True
        out["q1_columns"] = res1["columns"]
        out["q1_rowcount"] = len(res1["rows"])
        out["q1_rows_sample"] = res1["rows"][:10]
        out["q1_fingerprint"] = result_fingerprint(res1)
        out["q1_scalar"] = maybe_scalar(res1)
        out["_q1_full"] = res1
    except Exception as e:
        out["q1_error"] = str(e)

    # q2
    try:
        cur2 = con.execute(q2)
        rows2 = cur2.fetchall()
        res2 = normalize_result(cur2, rows2)
        out["q2_ok"] = True
        out["q2_columns"] = res2["columns"]
        out["q2_rowcount"] = len(res2["rows"])
        out["q2_rows_sample"] = res2["rows"][:10]
        out["q2_fingerprint"] = result_fingerprint(res2)
        out["q2_scalar"] = maybe_scalar(res2)
        out["_q2_full"] = res2
    except Exception as e:
        out["q2_error"] = str(e)

    # Only compute equality if both queries succeeded
    if out["q1_ok"] and out["q2_ok"]:
        out["equal"] = "True" if (out["q1_fingerprint"] == out["q2_fingerprint"]) else "False"
    else:
        out["equal"] = "Error"

    con.close()
    return out

# ------------------------------
# Block parsing (your exact format)
# ------------------------------

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
    # You might have multiple blocks concatenated with '~~~~~~~~~~~', so split on that
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
        if setup_sql and sql1 and sql2:
            out.append({"setup_sql": setup_sql, "sql1": sql1, "sql2": sql2})
    return out

# ------------------------------
# CSV reader (single block column)
# ------------------------------

def read_cases_from_block_csv(path: str, *, block_col: str, id_col: str) -> List[Dict[str, str]]:
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
            print(f"bound_size: {bound_size}")

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
                        "question_id": f"{base_id}_blk{j}",
                        "setup_sql": b["setup_sql"],
                        "sql1": b["sql1"],
                        "sql2": b["sql2"],
                    })
    return cases

# ------------------------------
# Writer
# ------------------------------

def write_results(out_path: str, results: List[Dict[str, Any]],
                  *, inline_max_rows: int = 100, dump_json_dir: str = "") -> None:
    """
    Write a summary CSV. Inline full rows for small outputs (<= inline_max_rows).
    Optionally dump full results to JSON files on disk.
    """
    fieldnames = [
        "bound_size",
        "question_id", "equal",
        "generated_sql_error", "gold_sql_error",
        #"q1_columns", "q2_columns",
        #"q1_rowcount", "q2_rowcount",
        #"q1_rows_sample", "q2_rows_sample",
        #"q1_fingerprint", "q2_fingerprint",
        "generated_sql_results", "gold_sql_results",
        "generated_sql_scalar", "gold_sql_scalar",
        "generated_sql", "gold_sql",
        #"q1_json", "q2_json",
        "generated_sql_ok", "gold_sql_ok",
        "generated_sql", "gold_sql"
    ]

    dump_dir = Path(dump_json_dir) if dump_json_dir else None

    if dump_dir:
        dump_dir.mkdir(parents=True, exist_ok=True)

    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for r in results:
            
            row = {
                "bound_size": r.get("bound_size"),
                "question_id": r.get("question_id"),
                "equal": r.get("equal"),
                "generated_sql_error": r.get("q1_error"),
                "gold_sql_error": r.get("q2_error"),
                "generated_sql_scalar": r.get("q1_scalar"),
                "gold_sql_scalar": r.get("q2_scalar"),
                "generated_sql_ok": r.get("q1_ok"),
                "gold_sql_ok": r.get("q2_ok"),
                "generated_sql": r.get("sql1"),
                "gold_sql": r.get("sql2")
            }

            # row: Dict[str, Any] = {k: r.get(k) for k in fieldnames}

            ''' 
            # Make lists JSON-friendly inside CSV cells
            for k in ["q1_columns", "q2_columns", "q1_rows_sample", "q2_rows_sample"]:
                if row.get(k) is not None and not isinstance(row[k], str):
                    row[k] = json.dumps(row[k], ensure_ascii=False)
            '''

            # Inline full rows when small enough
            for side in (("generated_sql", "q1"), ("gold_sql", "q2")):
                full = r.get(f"_{side[1]}_full")
                if full and inline_max_rows and len(full["rows"]) <= inline_max_rows:
                    row[f"{side[0]}_results"] = json.dumps(full["rows"], ensure_ascii=False)
                else:
                    row[f"{side[0]}_results"] = ""

            '''
            # Optional: dump full results to per-case JSONs
            if dump_dir:
                for side in ("q1", "q2"):
                    full = r.get(f"_{side}_full")
                    if full:
                        out_file = dump_dir / f"{r.get('question_id','question')}_{side}.json"
                        with out_file.open("w", encoding="utf-8") as jf:
                            json.dump(full, jf, ensure_ascii=False, indent=2)
                        row[f"{side}_json"] = str(out_file)
                    else:
                        row[f"{side}_json"] = ""
            '''

            w.writerow(row)

# ------------------------------
# CLI
# ------------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Verify VeriEQL counterexamples from a CSV column containing the full block."
    )
    ap.add_argument("--input", required=True, help="Path to CSV with a 'counterexample' column.")
    ap.add_argument("--out", default="verieql_verification_results.csv", help="Output CSV path.")
    ap.add_argument("--block-col", default="counterexample",
                    help="CSV column name that holds the full block text (default: counterexample).")
    ap.add_argument("--id-col", default="question_id",
                    help="CSV column name to use for case_id if present (default: question_id).")
    ap.add_argument("--inline-max-rows", type=int, default=100,
                    help="Inline full result rows in CSV if rowcount <= N (0 disables). Default: 100.")
    ap.add_argument("--dump-json-dir", type=str, default="",
                    help="If set, dump full q1/q2 results as JSON files into this directory.")
    args = ap.parse_args()

    cases = read_cases_from_block_csv(
        args.input,
        block_col=args.block_col,
        id_col=args.id_col,
    )
    if not cases:
        print("No valid cases found (are the sql1/sql2 markers present in the 'counterexample' text?).", file=sys.stderr)
        sys.exit(2)

    results: List[Dict[str, Any]] = []
    for c in cases:
        res = run_sqlite_case(c["setup_sql"], c["sql1"], c["sql2"])
        res["question_id"] = c["question_id"]
        res["bound_size"] = c["bound_size"]
        results.append(res)

    write_results(
        args.out,
        results,
        inline_max_rows=args.inline_max_rows,
        dump_json_dir=args.dump_json_dir,
    )
    print(f"Wrote {len(results)} results to {args.out}")
    if args.dump_json_dir:
        print(f"Full outputs dumped to: {args.dump_json_dir}")

if __name__ == "__main__":
    main()
