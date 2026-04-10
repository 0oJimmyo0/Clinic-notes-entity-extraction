#!/usr/bin/env python3
"""
RQ1 Step 2: Aggregate note-derived entities per visit.

Input:
- extracted_treatment_data_episode_cleaned.csv (span-level output from Stage 2)

Output:
- rq1_note_entities_by_visit.csv
  Columns:
    person_id, visit_id, n_spans, n_notes,
    conditions, drugs, measurements, procedures,
    treatment_actions, discontinuation_reasons

Each list column is JSON-encoded (lowercased, deduplicated).
"""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Iterable, List

import pandas as pd


def _parse_json_list(x) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        vals = x
    else:
        s = str(x).strip()
        if s == "" or s.lower() in {"nan", "none"}:
            return []
        try:
            vals = ast.literal_eval(s)
        except Exception:
            try:
                vals = json.loads(s)
            except Exception:
                return []
    if not isinstance(vals, list):
        return []
    out = []
    for v in vals:
        t = str(v).strip().lower()
        if t:
            out.append(t)
    return out


def _dedupe_sorted(values: Iterable[str]) -> List[str]:
    return sorted({str(v).strip().lower() for v in values if str(v).strip()})


def aggregate_step2(
    extracted_df: pd.DataFrame,
    high_certainty_only: bool = False,
) -> pd.DataFrame:
    df = extracted_df.copy()

    req_cols = {"person_id", "visit_id"}
    missing = req_cols.difference(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Optional filter for cleaner signal
    if high_certainty_only and "certainty" in df.columns:
        df = df[df["certainty"].astype(str).str.lower() == "high"].copy()

    for col in ("conditions", "drugs", "measurements", "procedures"):
        if col in df.columns:
            df[col] = df[col].apply(_parse_json_list)
        else:
            df[col] = [[] for _ in range(len(df))]

    if "treatment_action" not in df.columns:
        df["treatment_action"] = "none"
    if "discontinuation_reason" not in df.columns:
        df["discontinuation_reason"] = "none"

    # group per person+visit
    grouped = df.groupby(["person_id", "visit_id"], dropna=False, sort=False)
    out_rows = []
    for (pid, vid), g in grouped:
        conds = _dedupe_sorted(v for row in g["conditions"] for v in row)
        drugs = _dedupe_sorted(v for row in g["drugs"] for v in row)
        meas = _dedupe_sorted(v for row in g["measurements"] for v in row)
        procs = _dedupe_sorted(v for row in g["procedures"] for v in row)

        actions = _dedupe_sorted(
            a for a in g["treatment_action"].astype(str).str.lower().tolist() if a and a != "none"
        )
        reasons = _dedupe_sorted(
            r for r in g["discontinuation_reason"].astype(str).str.lower().tolist() if r and r != "none"
        )

        n_notes = g["note_id"].nunique() if "note_id" in g.columns else 0
        out_rows.append(
            {
                "person_id": pid,
                "visit_id": vid,
                "n_spans": int(len(g)),
                "n_notes": int(n_notes),
                "conditions": json.dumps(conds),
                "drugs": json.dumps(drugs),
                "measurements": json.dumps(meas),
                "procedures": json.dumps(procs),
                "treatment_actions": json.dumps(actions),
                "discontinuation_reasons": json.dumps(reasons),
            }
        )
    return pd.DataFrame(out_rows)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RQ1 Step 2 note-entity aggregation.")
    p.add_argument(
        "--input-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/stage2/extracted_treatment_data_episode_cleaned.csv",
        help="Stage-2 extracted span-level CSV.",
    )
    p.add_argument(
        "--output-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/rq1_note_entities_by_visit.csv",
        help="Visit-level aggregated note-entity CSV.",
    )
    p.add_argument(
        "--high-certainty-only",
        action="store_true",
        help="Use only rows with certainty=='high'.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    in_csv = (root / args.input_csv).resolve()
    out_csv = (root / args.output_csv).resolve()

    if not in_csv.exists():
        raise FileNotFoundError(f"Input not found: {in_csv}")

    print(f"Loading: {in_csv}")
    df = pd.read_csv(in_csv)
    print(f"Input rows: {len(df):,}")

    out = aggregate_step2(df, high_certainty_only=args.high_certainty_only)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_csv, index=False)

    print(f"Saved: {out_csv}")
    print(f"Visit-level rows: {len(out):,}")
    print(
        "Coverage (visits with >=1 note entity): "
        f"{int((out[['conditions','drugs','measurements','procedures']]!='[]').any(axis=1).sum()):,}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
