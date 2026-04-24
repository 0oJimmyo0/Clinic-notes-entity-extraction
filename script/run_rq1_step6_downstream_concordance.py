#!/usr/bin/env python3
"""
Step 6 downstream concordance: compare adjudicated comparable note drugs to structured EHR.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_drug_linking import normalize_drug_text
from rq1_concordance_utils import (
    build_windowed_ehr,
    compute_domain_similarity,
    infer_timeline_from_visit_id,
    normalize_id,
    parse_list_cell,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run downstream concordance on adjudicated comparable note drugs.")
    p.add_argument(
        "--downstream-comparable-mentions-csv",
        default="episode_extraction_results/rq1/adjudicated/rq1_downstream_comparable_mentions.csv",
        help="Comparable adjudicated mention rows from run_join_adjudication_labels.py",
    )
    p.add_argument(
        "--ehr-csv",
        default="episode_extraction_results/rq1/rq1_ehr_entities_by_visit.csv",
        help="Visit-level structured EHR entities CSV",
    )
    p.add_argument(
        "--timeline-csv",
        default="episode_extraction_results/rq1/rq1_visit_timeline.csv",
        help="Optional visit timeline CSV (required when window size > 0 unless using fallback).",
    )
    p.add_argument(
        "--timeline-fallback",
        choices=["error", "infer_visit_id"],
        default="error",
        help="When window size > 0 and timeline is missing: stop or infer order by visit_id.",
    )
    p.add_argument(
        "--windows",
        default="0",
        help='Comma-separated window sizes, e.g. "0" or "0,1,2".',
    )
    p.add_argument(
        "--method-label",
        default="downstream_adjudicated_comparable",
        help="Method label saved to concordance outputs.",
    )
    p.add_argument(
        "--normalization-detailed-csv",
        default="",
        help=(
            "Optional normalization detail CSV from run_rq1_step5_normalization_eval.py. "
            "When provided, writes concordance ablations for raw/baseline/path_a/path_b_layered."
        ),
    )
    p.add_argument(
        "--output-dir",
        default="episode_extraction_results/rq1/downstream_concordance",
        help="Output directory.",
    )
    return p.parse_args()


def _build_note_visit_from_mentions(df: pd.DataFrame, value_col: str, method_label: str) -> pd.DataFrame:
    work = df.copy()
    work[value_col] = work[value_col].astype(str).str.strip().str.lower()
    work = work[work[value_col] != ""].copy()
    if work.empty:
        return pd.DataFrame(columns=["person_id", "visit_id", "drugs", "method_label"])

    agg = (
        work.groupby(["person_id", "visit_id"], as_index=False)[value_col]
        .agg(lambda x: sorted(set(v for v in x if v)))
        .rename(columns={value_col: "drugs"})
    )
    agg["method_label"] = method_label
    return agg


def _compute_method_ablation(
    mentions: pd.DataFrame,
    norm_detail: pd.DataFrame,
    ehr: pd.DataFrame,
    windows: list[int],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    # Prefer deterministic join by adjudication_unit_id.
    if "adjudication_unit_id" in mentions.columns and "adjudication_unit_id" in norm_detail.columns:
        join = mentions.merge(norm_detail, on="adjudication_unit_id", how="inner", suffixes=("", "_norm"))
    else:
        key_cols = [c for c in ["person_id", "visit_id", "note_id"] if c in mentions.columns and c in norm_detail.columns]
        if "raw_mention_text" in mentions.columns and "raw_mention_text" in norm_detail.columns:
            key_cols.append("raw_mention_text")
        join = mentions.merge(norm_detail, on=key_cols, how="inner", suffixes=("", "_norm")) if key_cols else pd.DataFrame()

    if join.empty:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    join["person_id"] = normalize_id(join["person_id"])
    join["visit_id"] = normalize_id(join["visit_id"])

    if "raw_mention_text" not in join.columns:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    join["raw_norm"] = join["raw_mention_text"].map(normalize_drug_text)
    join["baseline_norm"] = (
        join["baseline_prediction"].astype(str).str.strip().str.lower()
        if "baseline_prediction" in join.columns
        else ""
    )
    join["patha_norm"] = (
        join["patha_prediction"].astype(str).str.strip().str.lower()
        if "patha_prediction" in join.columns
        else ""
    )
    join["pathb_norm"] = (
        join["pathb_prediction"].astype(str).str.strip().str.lower()
        if "pathb_prediction" in join.columns
        else ""
    )
    join["pathb_accepted"] = join["pathb_accepted"].astype(bool) if "pathb_accepted" in join.columns else False
    join["pathb_layered"] = join.apply(
        lambda r: r["pathb_norm"] if r["pathb_accepted"] and str(r["pathb_norm"]).strip() else str(r["patha_norm"]).strip(),
        axis=1,
    )

    method_specs = [
        ("raw_pre_normalization", "raw_norm"),
        ("baseline", "baseline_norm"),
        ("path_a", "patha_norm"),
        ("path_b_layered", "pathb_layered"),
    ]

    all_summary = []
    all_pairs = []
    status_rows = []

    for method_label, col in method_specs:
        note_visit = _build_note_visit_from_mentions(join, col, method_label=method_label)
        if note_visit.empty:
            continue

        for k in windows:
            # Method ablation currently reported for k=0 only; keep API compatible if future windowing is added.
            if k != 0:
                continue
            summary_k, pairs_k = compute_domain_similarity(
                note_df=note_visit[["person_id", "visit_id", "drugs"]],
                ehr_df=ehr,
                domain="drugs",
                window_k=k,
                method_label=method_label,
            )
            all_summary.append(summary_k)
            all_pairs.append(pairs_k)

        if "mention_status" in join.columns:
            for status, sub in join.groupby("mention_status", dropna=False):
                note_visit_s = _build_note_visit_from_mentions(sub, col, method_label=method_label)
                if note_visit_s.empty:
                    continue
                summary_s, _ = compute_domain_similarity(
                    note_df=note_visit_s[["person_id", "visit_id", "drugs"]],
                    ehr_df=ehr,
                    domain="drugs",
                    window_k=0,
                    method_label=method_label,
                )
                if len(summary_s):
                    row = summary_s.iloc[0].to_dict()
                    row["mention_status"] = str(status)
                    status_rows.append(row)

    return (
        pd.concat(all_summary, ignore_index=True) if all_summary else pd.DataFrame(),
        pd.concat(all_pairs, ignore_index=True) if all_pairs else pd.DataFrame(),
        pd.DataFrame(status_rows),
    )


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    mentions_path = (root / args.downstream_comparable_mentions_csv).resolve()
    ehr_path = (root / args.ehr_csv).resolve()
    timeline_path = (root / args.timeline_csv).resolve()
    norm_detail_path = (root / args.normalization_detailed_csv).resolve() if args.normalization_detailed_csv else None
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not mentions_path.exists():
        raise FileNotFoundError(f"Missing downstream comparable mentions CSV: {mentions_path}")
    if not ehr_path.exists():
        raise FileNotFoundError(f"Missing EHR CSV: {ehr_path}")

    mentions = pd.read_csv(mentions_path).fillna("")
    windows = sorted({int(x.strip()) for x in args.windows.split(",") if x.strip() != ""})
    if any(k < 0 for k in windows):
        raise ValueError("window sizes must be >= 0")

    required = {"person_id", "visit_id", "adjudicated_canonical_label"}
    missing = required - set(mentions.columns)
    if missing:
        raise ValueError(f"Comparable mentions CSV missing columns: {sorted(missing)}")

    note_visit = (
        mentions[mentions["adjudicated_canonical_label"].astype(str).str.strip() != ""]
        .groupby(["person_id", "visit_id"])["adjudicated_canonical_label"]
        .agg(lambda x: sorted(set(str(v).strip().lower() for v in x if str(v).strip())))
        .reset_index(name="drugs")
    )
    note_visit["person_id"] = normalize_id(note_visit["person_id"])
    note_visit["visit_id"] = normalize_id(note_visit["visit_id"])

    ehr = pd.read_csv(ehr_path).fillna("")
    if "person_id" not in ehr.columns or "visit_id" not in ehr.columns:
        raise ValueError("EHR CSV must include person_id and visit_id columns")
    ehr["person_id"] = normalize_id(ehr["person_id"])
    ehr["visit_id"] = normalize_id(ehr["visit_id"])
    if "drugs" not in ehr.columns:
        ehr["drugs"] = "[]"
    ehr["drugs"] = ehr["drugs"].apply(parse_list_cell)

    note_path = out_dir / "rq1_downstream_note_entities_by_visit.csv"
    summary_path = out_dir / "rq1_step6_downstream_summary.json"
    summary_csv = out_dir / "rq1_similarity_summary.csv"
    pairs_csv = out_dir / "rq1_similarity_pairs.csv"
    ablation_summary_csv = out_dir / "rq1_similarity_summary_method_ablation.csv"
    ablation_pairs_csv = out_dir / "rq1_similarity_pairs_method_ablation.csv"
    ablation_status_csv = out_dir / "rq1_similarity_status_stratified_method_ablation.csv"

    note_write = note_visit.copy()
    note_write["drugs"] = note_write["drugs"].map(lambda xs: json.dumps(xs, ensure_ascii=False))
    note_write.to_csv(note_path, index=False)

    timeline_df = None
    if max(windows) > 0:
        if timeline_path.exists():
            timeline_df = pd.read_csv(timeline_path)
            need_cols = {"person_id", "visit_id", "visit_start_date"}
            if not need_cols.issubset(set(timeline_df.columns)):
                raise ValueError(f"timeline-csv must include {sorted(need_cols)}")
        elif args.timeline_fallback == "infer_visit_id":
            timeline_df = infer_timeline_from_visit_id(note_df=note_visit, ehr_df=ehr)
        else:
            raise FileNotFoundError(
                "k>0 requested but timeline file missing. Provide --timeline-csv with "
                "person_id, visit_id, visit_start_date or set --timeline-fallback infer_visit_id."
            )

    all_summary = []
    all_pairs = []
    for k in windows:
        if k == 0:
            ehr_k = ehr.copy()
        else:
            ehr_k = build_windowed_ehr(ehr_df=ehr, timeline_df=timeline_df, k=k, domains=["drugs"])

        summary_k, pairs_k = compute_domain_similarity(
            note_df=note_visit,
            ehr_df=ehr_k,
            domain="drugs",
            window_k=k,
            method_label=str(args.method_label),
        )
        all_summary.append(summary_k)
        all_pairs.append(pairs_k)

    summary_df = pd.concat(all_summary, ignore_index=True) if all_summary else pd.DataFrame()
    pairs_df = pd.concat(all_pairs, ignore_index=True) if all_pairs else pd.DataFrame()
    summary_df.to_csv(summary_csv, index=False)
    pairs_df.to_csv(pairs_csv, index=False)

    ablation_summary_df = pd.DataFrame()
    ablation_pairs_df = pd.DataFrame()
    ablation_status_df = pd.DataFrame()
    if norm_detail_path is not None and norm_detail_path.exists():
        norm_detail = pd.read_csv(norm_detail_path).fillna("")
        ablation_summary_df, ablation_pairs_df, ablation_status_df = _compute_method_ablation(
            mentions=mentions,
            norm_detail=norm_detail,
            ehr=ehr,
            windows=windows,
        )
        if len(ablation_summary_df):
            ablation_summary_df.to_csv(ablation_summary_csv, index=False)
        if len(ablation_pairs_df):
            ablation_pairs_df.to_csv(ablation_pairs_csv, index=False)
        if len(ablation_status_df):
            ablation_status_df.to_csv(ablation_status_csv, index=False)

    write_run_summary(
        summary_path,
        {
            "inputs": {
                "downstream_comparable_mentions_csv": str(mentions_path),
                "ehr_csv": str(ehr_path),
                "timeline_csv": str(timeline_path) if timeline_path.exists() else None,
                "normalization_detailed_csv": str(norm_detail_path) if norm_detail_path and norm_detail_path.exists() else None,
                "windows": windows,
            },
            "counts": {
                "comparable_mentions": int(len(mentions)),
                "note_visits": int(len(note_visit)),
                "aligned_visit_pairs": int(len(pairs_df)),
                "method_ablation_summary_rows": int(len(ablation_summary_df)),
                "method_ablation_pair_rows": int(len(ablation_pairs_df)),
            },
            "outputs": {
                "note_visit_csv": str(note_path),
                "summary_csv": str(summary_csv),
                "pairs_csv": str(pairs_csv),
                "ablation_summary_csv": str(ablation_summary_csv) if len(ablation_summary_df) else None,
                "ablation_pairs_csv": str(ablation_pairs_csv) if len(ablation_pairs_df) else None,
                "ablation_status_csv": str(ablation_status_csv) if len(ablation_status_df) else None,
            },
        },
    )

    print(f"Saved downstream concordance note-visit file: {note_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
