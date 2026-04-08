#!/usr/bin/env python3
"""
RQ1 Step 3b: Build visit timeline file for temporal windows (Phase 2).

Input:
- visit_occurrence file/folder (.csv/.parquet), or any table containing:
    person_id, visit_id/visit_occurrence_id, visit_start_date-like column

Output:
- episode_extraction_results/rq1_visit_timeline.csv
  Columns: person_id, visit_id, visit_start_date
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd


PERSON_CANDIDATES = ["person_id"]
VISIT_CANDIDATES = ["visit_id", "visit_occurrence_id"]
DATE_CANDIDATES = [
    "visit_start_date",
    "visit_start_datetime",
    "start_date",
    "start_datetime",
]


def pick_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in cols:
            return cols[c.lower()]
    return None


def normalize_id(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace({"": None, "nan": None, "none": None, "None": None})

    def _clean(v):
        if v is None:
            return None
        x = str(v).strip()
        m = re.match(r"^-?\d+\.0+$", x)
        if m:
            return x.split(".")[0]
        return x

    return s.map(_clean)


def _read_any(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported file type: {path}")


def _resolve_input_files(path: Path) -> List[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted(list(path.glob("*.parquet")) + list(path.glob("*.csv")))
    return []


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RQ1 Step 3b visit timeline builder")
    p.add_argument(
        "--visits-path",
        required=True,
        help="Visit_occurrence file or folder (.csv/.parquet).",
    )
    p.add_argument(
        "--output-csv",
        default="episode_extraction_results/rq1/rq1_visit_timeline.csv",
        help="Output timeline CSV for Step 4 k-window analysis.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]

    visits_path = (root / args.visits_path).resolve()
    out_path = (root / args.output_csv).resolve()

    files = _resolve_input_files(visits_path)
    if not files:
        raise FileNotFoundError(f"visits input not found or empty: {visits_path}")

    parts = []
    for f in files:
        df = _read_any(f)
        pcol = pick_col(df, PERSON_CANDIDATES)
        vcol = pick_col(df, VISIT_CANDIDATES)
        dcol = pick_col(df, DATE_CANDIDATES)
        if pcol is None or vcol is None or dcol is None:
            raise ValueError(
                f"Cannot detect required columns in {f}. "
                f"Need person_id, visit_id/visit_occurrence_id, visit_start_date-like column. "
                f"Columns={list(df.columns)}"
            )
        part = df[[pcol, vcol, dcol]].copy()
        part.columns = ["person_id", "visit_id", "visit_start_date"]
        parts.append(part)

    out = pd.concat(parts, ignore_index=True)
    out["person_id"] = normalize_id(out["person_id"])
    out["visit_id"] = normalize_id(out["visit_id"])
    out["visit_start_date"] = pd.to_datetime(out["visit_start_date"], errors="coerce")
    out = out.dropna(subset=["person_id", "visit_id", "visit_start_date"])
    out = out.drop_duplicates(subset=["person_id", "visit_id"], keep="first")
    out = out.sort_values(["person_id", "visit_start_date", "visit_id"]).reset_index(drop=True)
    out["visit_start_date"] = out["visit_start_date"].dt.date.astype(str)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(out_path, index=False)

    print(f"Saved timeline: {out_path}")
    print(f"Rows: {len(out):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

