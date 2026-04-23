#!/usr/bin/env python3
"""
Compute strict Path B oracle candidate recall from top-k candidate lists.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_drug_linking import normalize_drug_text


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Compute strict Path B oracle recall@k.")
    p.add_argument(
        "--strict-slice-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/normalization_eval/rq1_strict_pathb_diagnostic_slice.csv",
    )
    p.add_argument(
        "--output-json",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/normalization_eval/rq1_pathb_oracle_recall_summary.json",
    )
    return p.parse_args()


def _safe_rate(num: int, den: int) -> float:
    return float(num) / float(den) if den else 0.0


def _parse_topk(text: str) -> List[str]:
    try:
        arr = json.loads(str(text))
    except Exception:
        return []
    if not isinstance(arr, list):
        return []

    out: List[str] = []
    for item in arr:
        if not isinstance(item, dict):
            continue
        label = item.get("canonical_label") or item.get("label") or ""
        norm = normalize_drug_text(str(label))
        if norm:
            out.append(norm)
    return out


def _is_short_or_abbrev(raw: str) -> bool:
    t = str(raw).strip()
    if not t:
        return False
    tn = normalize_drug_text(t)
    toks = [x for x in re.split(r"\s+", tn) if x]
    compact = re.sub(r"[^A-Za-z0-9]", "", t)
    if compact and len(compact) <= 4:
        return True
    if len(toks) == 1 and len(toks[0]) <= 5:
        return True
    if re.fullmatch(r"[A-Z]{2,6}", t):
        return True
    return False


def _oracle_flags(topk: List[str], gold: str, cutoffs: List[int]) -> Dict[str, bool]:
    out: Dict[str, bool] = {}
    if not gold:
        for k in cutoffs:
            out[f"hit_at_{k}"] = False
        return out

    for k in cutoffs:
        out[f"hit_at_{k}"] = gold in topk[:k]
    return out


def _group_recall(df: pd.DataFrame, by: str, cutoffs: List[int]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    s = df[by].fillna("").astype(str).str.strip().replace({"": "<EMPTY>"})
    tmp = df.copy()
    tmp[by] = s

    for value, sub in tmp.groupby(by, dropna=False):
        row: Dict[str, object] = {"group_value": str(value), "n": int(len(sub))}
        for k in cutoffs:
            row[f"oracle_recall_at_{k}"] = round(float(sub[f"hit_at_{k}"].mean()) if len(sub) else 0.0, 6)
        rows.append(row)
    rows.sort(key=lambda x: x["n"], reverse=True)
    return rows


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    slice_path = (root / args.strict_slice_csv).resolve()
    out_path = (root / args.output_json).resolve()

    if not slice_path.exists():
        raise FileNotFoundError(f"Missing strict slice CSV: {slice_path}")

    df = pd.read_csv(slice_path).fillna("")
    required = ["adjudicated_canonical_label", "pathb_top_k_candidates_json", "raw_mention_text"]
    miss = [c for c in required if c not in df.columns]
    if miss:
        raise ValueError(f"Strict slice missing required columns: {miss}")

    df["gold_norm"] = df["adjudicated_canonical_label"].map(normalize_drug_text)
    df["topk_labels"] = df["pathb_top_k_candidates_json"].map(_parse_topk)
    df["topk_len"] = df["topk_labels"].map(len)
    df["short_or_abbrev_flag"] = df["raw_mention_text"].map(_is_short_or_abbrev)

    k_max = int(df["topk_len"].max()) if len(df) else 0
    k_max = max(k_max, 1)
    cutoffs = [1, 3, 5, k_max]
    cutoffs_unique = []
    for k in cutoffs:
        if k not in cutoffs_unique:
            cutoffs_unique.append(k)
    cutoffs = cutoffs_unique

    flags = df.apply(lambda r: _oracle_flags(r["topk_labels"], r["gold_norm"], cutoffs), axis=1)
    flags_df = pd.DataFrame(flags.tolist())
    for c in flags_df.columns:
        df[c] = flags_df[c].astype(bool)

    metrics = {
        "strict_leftover_n": int(len(df)),
        "oracle_recall_at_1": round(float(df["hit_at_1"].mean()) if len(df) else 0.0, 6),
        "oracle_recall_at_3": round(float(df["hit_at_3"].mean()) if "hit_at_3" in df.columns and len(df) else 0.0, 6),
        "oracle_recall_at_5": round(float(df["hit_at_5"].mean()) if "hit_at_5" in df.columns and len(df) else 0.0, 6),
        "oracle_k_value": int(k_max),
        f"oracle_recall_at_{k_max}": round(float(df[f"hit_at_{k_max}"].mean()) if len(df) else 0.0, 6),
    }

    payload = {
        "inputs": {
            "strict_slice_csv": str(slice_path),
        },
        "metrics": metrics,
        "stratified_oracle_recall": {
            "by_mention_status": _group_recall(df, "mention_status", cutoffs),
            "by_current_error_bucket": _group_recall(df, "current_error_bucket", cutoffs),
            "by_short_or_abbrev_flag": _group_recall(df, "short_or_abbrev_flag", cutoffs),
        },
    }

    out_path.parent.mkdir(parents=True, exist_ok=True)
    write_run_summary(out_path, payload)
    print(f"Saved Path B oracle recall summary: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
