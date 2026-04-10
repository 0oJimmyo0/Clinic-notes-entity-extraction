#!/usr/bin/env python3
"""
Pre-adjudication engineering dry-run for baseline, Path A, and Path B.

This script is for execution validation and diagnostics only.
It does not compute truth-performance metrics.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from rq1_adjudication_utils import parse_list_cell, write_run_summary
from rq1_drug_linking import (
    PathBConfig,
    build_canonical_drug_universe,
    canonicalize_drug,
    link_mention_to_canonical_vocab,
    load_alias_entries,
    load_alias_map,
    load_calibration_config,
    normalize_drug_text,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run pre-adjudication dry-run diagnostics for baseline/Path A/Path B.")
    p.add_argument(
        "--note-csv",
        default="episode_extraction_results/rq1/rq1_note_entities_by_visit.csv",
        help="Visit-level note entities CSV (must include person_id, visit_id, drugs column).",
    )
    p.add_argument(
        "--drugs-col",
        default="drugs",
        help="Drug list column in note CSV.",
    )
    p.add_argument(
        "--subset-csv",
        default="",
        help="Optional subset CSV to filter visits (e.g., adjudication subset manifest).",
    )
    p.add_argument(
        "--subset-person-col",
        default="person_id",
        help="Person id column name in subset CSV.",
    )
    p.add_argument(
        "--subset-visit-col",
        default="visit_occurrence_id",
        help="Visit id column name in subset CSV.",
    )
    p.add_argument(
        "--max-visits",
        type=int,
        default=0,
        help="Optional deterministic cap on number of visits after filtering (0 = no cap).",
    )
    p.add_argument(
        "--alias-artifact",
        default="lexicons/rq1_drug_aliases.csv",
        help="Path A alias artifact CSV/JSON.",
    )
    p.add_argument(
        "--canonical-vocab-path",
        default="lexicons/rq1_drug_canonical_vocab.csv",
        help="Canonical vocabulary for Path B candidate universe.",
    )
    p.add_argument("--pathb-top-k", type=int, default=20)
    p.add_argument("--pathb-min-score", type=float, default=0.45)
    p.add_argument("--pathb-min-margin", type=float, default=0.05)
    p.add_argument("--pathb-min-mention-len", type=int, default=4)
    p.add_argument("--pathb-min-score-short-mention", type=float, default=0.80)
    p.add_argument("--pathb-min-calibrated-confidence", type=float, default=0.90)
    p.add_argument(
        "--pathb-calibration-json",
        default="",
        help="Optional calibration JSON for Path B confidence.",
    )
    p.add_argument(
        "--output-dir",
        default="episode_extraction_results/rq1/pre_adjudication_dryrun",
        help="Output directory.",
    )
    return p.parse_args()


def _margin_from_topk(text: str) -> float:
    try:
        vals = json.loads(text)
    except Exception:
        return 0.0
    if not isinstance(vals, list) or not vals:
        return 0.0
    top1 = float(vals[0].get("score", 0.0))
    top2 = float(vals[1].get("score", 0.0)) if len(vals) > 1 else 0.0
    return top1 - top2


def _pct(num: int, den: int) -> float:
    return float(num) / float(den) if den else 0.0


def _series_quantiles(s: pd.Series) -> Dict[str, float]:
    if s is None or len(s) == 0:
        return {"min": 0.0, "p25": 0.0, "p50": 0.0, "p75": 0.0, "p90": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    vals = s.astype(float)
    return {
        "min": float(vals.min()),
        "p25": float(vals.quantile(0.25)),
        "p50": float(vals.quantile(0.50)),
        "p75": float(vals.quantile(0.75)),
        "p90": float(vals.quantile(0.90)),
        "p95": float(vals.quantile(0.95)),
        "p99": float(vals.quantile(0.99)),
        "max": float(vals.max()),
    }


def _token_overlap_ratio(a: str, b: str) -> float:
    sa = set(str(a).split())
    sb = set(str(b).split())
    if not sa or not sb:
        return 0.0
    return float(len(sa & sb)) / float(len(sa | sb))


def _build_mentions(note_df: pd.DataFrame, drugs_col: str) -> pd.DataFrame:
    rows: List[Dict[str, str]] = []
    for r in note_df.itertuples(index=False):
        pid = str(getattr(r, "person_id", "")).strip()
        vid = str(getattr(r, "visit_id", "")).strip()
        for raw in parse_list_cell(getattr(r, drugs_col, [])):
            rows.append(
                {
                    "person_id": pid,
                    "visit_id": vid,
                    "raw_mention_text": str(raw).strip(),
                }
            )
    out = pd.DataFrame(rows)
    if len(out):
        out["raw_mention_text"] = out["raw_mention_text"].astype(str).str.strip()
        out = out[out["raw_mention_text"] != ""].copy()
    return out


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]

    note_path = (root / args.note_csv).resolve()
    subset_path = (root / args.subset_csv).resolve() if args.subset_csv else None
    alias_path = (root / args.alias_artifact).resolve()
    vocab_path = (root / args.canonical_vocab_path).resolve() if args.canonical_vocab_path else None
    calib_path = (root / args.pathb_calibration_json).resolve() if args.pathb_calibration_json else None
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not note_path.exists():
        raise FileNotFoundError(f"Missing note CSV: {note_path}")

    note_df = pd.read_csv(note_path).fillna("")
    required_cols = {"person_id", "visit_id", args.drugs_col}
    missing = required_cols - set(note_df.columns)
    if missing:
        raise ValueError(f"Note CSV missing required columns: {sorted(missing)}")

    note_df["person_id"] = note_df["person_id"].astype(str).str.strip()
    note_df["visit_id"] = note_df["visit_id"].astype(str).str.strip()

    n_input_visits = int(len(note_df))

    if subset_path is not None and subset_path.exists():
        subset_df = pd.read_csv(subset_path).fillna("")
        if args.subset_person_col not in subset_df.columns or args.subset_visit_col not in subset_df.columns:
            raise ValueError(
                f"Subset CSV missing required columns: {args.subset_person_col}, {args.subset_visit_col}"
            )
        subset_df[args.subset_person_col] = subset_df[args.subset_person_col].astype(str).str.strip()
        subset_df[args.subset_visit_col] = subset_df[args.subset_visit_col].astype(str).str.strip()
        keys = set(zip(subset_df[args.subset_person_col], subset_df[args.subset_visit_col]))
        note_df = note_df[note_df.apply(lambda r: (r["person_id"], r["visit_id"]) in keys, axis=1)].copy()

    if int(args.max_visits) > 0 and len(note_df):
        keys_df = note_df[["person_id", "visit_id"]].drop_duplicates().sort_values(["person_id", "visit_id"]).head(int(args.max_visits))
        keep = set(zip(keys_df["person_id"], keys_df["visit_id"]))
        note_df = note_df[note_df.apply(lambda r: (r["person_id"], r["visit_id"]) in keep, axis=1)].copy()

    n_filtered_visits = int(len(note_df))

    note_df["drug_list_len"] = note_df[args.drugs_col].map(lambda x: len(parse_list_cell(x)))
    visits_with_drugs = int((note_df["drug_list_len"] > 0).sum()) if len(note_df) else 0

    mention_df = _build_mentions(note_df, args.drugs_col)
    if not len(mention_df):
        raise ValueError("No mention rows found after filtering; cannot run dry-run diagnostics.")

    mention_df["mention_norm_baseline"] = mention_df["raw_mention_text"].map(normalize_drug_text)

    alias_map = load_alias_map(alias_path) if alias_path.exists() else {}
    alias_entries = load_alias_entries(alias_path) if alias_path.exists() else []

    mention_df["patha_term"] = mention_df["raw_mention_text"].map(lambda x: canonicalize_drug(x, alias_map))
    mention_df["patha_alias_hit"] = mention_df.apply(
        lambda r: bool(r["mention_norm_baseline"]) and (r["mention_norm_baseline"] in alias_map) and (r["patha_term"] != r["mention_norm_baseline"]),
        axis=1,
    )

    calibration = load_calibration_config(calib_path) if calib_path and calib_path.exists() else None
    cfg = PathBConfig(
        top_k=int(args.pathb_top_k),
        min_score=float(args.pathb_min_score),
        min_margin=float(args.pathb_min_margin),
        min_mention_len=int(args.pathb_min_mention_len),
        min_score_short_mention=float(args.pathb_min_score_short_mention),
        min_calibrated_confidence=float(args.pathb_min_calibrated_confidence),
        calibration=calibration,
    )
    universe = build_canonical_drug_universe(
        alias_map=alias_map,
        canonical_vocab_path=vocab_path if vocab_path and vocab_path.exists() else None,
    )

    decisions: List[Dict] = []
    for row in mention_df.itertuples(index=False):
        d = link_mention_to_canonical_vocab(
            raw_mention=str(getattr(row, "raw_mention_text", "") or ""),
            alias_map=alias_map,
            candidate_universe=universe,
            pathb_config=cfg,
            mention_metadata={},
        )
        top_k_json = json.dumps(d.get("top_k_candidates", []), ensure_ascii=False)
        decisions.append(
            {
                "stage": str(d.get("stage", "")),
                "pathb_accepted": bool(d.get("accepted", False)),
                "prediction": str(d.get("prediction", "") or ""),
                "pathb_score": float(d.get("score", 0.0) or 0.0),
                "pathb_calibrated_confidence": float(d.get("calibrated_confidence", 0.0) or 0.0),
                "pathb_reason_codes_json": json.dumps(d.get("reason_codes", []), ensure_ascii=False),
                "pathb_top_k_candidates_json": top_k_json,
                "pathb_margin": _margin_from_topk(top_k_json),
            }
        )

    dec_df = pd.DataFrame(decisions)
    detail = pd.concat([mention_df.reset_index(drop=True), dec_df.reset_index(drop=True)], axis=1)

    detail["pathb_passed"] = detail["stage"] == "path_b_canonical_transparent"
    detail["pathb_abstained"] = detail["pathb_passed"] & (~detail["pathb_accepted"])
    detail["baseline_nonempty"] = detail["mention_norm_baseline"].astype(str).str.strip() != ""
    detail["patha_nonempty"] = detail["patha_term"].astype(str).str.strip() != ""
    detail["patha_exact_vocab_resolved"] = detail["stage"] == "path_a_exact_vocab"

    reason_counter = Counter()
    for txt in detail.loc[detail["pathb_abstained"], "pathb_reason_codes_json"].astype(str):
        try:
            vals = json.loads(txt)
        except Exception:
            vals = []
        if not isinstance(vals, list):
            vals = []
        if not vals:
            reason_counter["<missing_reason_code>"] += 1
        for v in vals:
            reason_counter[str(v)] += 1

    reject_reason_df = pd.DataFrame(
        [{"reason_code": k, "count": int(v)} for k, v in reason_counter.most_common()]
    )

    unresolved_df = (
        detail[detail["pathb_abstained"]]
        .groupby(["raw_mention_text", "mention_norm_baseline"], as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values("count", ascending=False)
        .head(200)
    )

    accepted_df = (
        detail[
            detail["pathb_passed"]
            & detail["pathb_accepted"]
            & (detail["prediction"].astype(str).str.strip() != "")
        ]
        .groupby("prediction", as_index=False)
        .size()
        .rename(columns={"size": "count", "prediction": "canonical_label"})
        .sort_values("count", ascending=False)
        .head(200)
    )

    alias_hits_df = (
        detail[detail["patha_alias_hit"]]
        .groupby(["mention_norm_baseline", "patha_term"], as_index=False)
        .size()
        .rename(columns={"size": "count", "mention_norm_baseline": "alias_norm", "patha_term": "canonical_norm"})
        .sort_values("count", ascending=False)
    )

    canonical_out_df = (
        detail.groupby("patha_term", as_index=False)
        .size()
        .rename(columns={"size": "count", "patha_term": "canonical_output"})
        .sort_values("count", ascending=False)
        .head(500)
    )

    # Alias artifact quality checks.
    alias_df = pd.DataFrame(alias_entries)
    suspicious_alias_rows: List[Dict[str, object]] = []
    if len(alias_df):
        use = alias_df.copy()
        use["include_flag"] = use["include_flag"].astype(str).str.strip().str.lower()
        use = use[use["include_flag"].isin({"yes", "true", "1"})].copy()
        use["alias_norm"] = use["alias_normalized"].astype(str).map(normalize_drug_text)
        use["canonical_norm"] = use["canonical_label"].astype(str).map(normalize_drug_text)

        # Non one-to-one alias conflicts.
        conflict = (
            use.groupby("alias_norm", as_index=False)["canonical_norm"]
            .nunique()
            .rename(columns={"canonical_norm": "n_canonical"})
        )
        conflict = conflict[conflict["n_canonical"] > 1].copy()
        for row in conflict.itertuples(index=False):
            cn = sorted(use[use["alias_norm"] == row.alias_norm]["canonical_norm"].dropna().unique().tolist())
            suspicious_alias_rows.append(
                {
                    "alias_norm": row.alias_norm,
                    "canonical_norm": "|".join(cn),
                    "issue": "non_one_to_one_alias",
                }
            )

        # Heuristic suspicious patterns.
        for row in use.itertuples(index=False):
            alias_norm = str(getattr(row, "alias_norm", "") or "")
            canonical_norm = str(getattr(row, "canonical_norm", "") or "")
            if not alias_norm or not canonical_norm:
                continue
            issue = ""
            if len(alias_norm) <= 3:
                issue = "very_short_alias"
            elif _token_overlap_ratio(alias_norm, canonical_norm) == 0.0:
                issue = "zero_token_overlap"
            elif str(getattr(row, "confidence", "") or "").strip().lower() in {"low", "uncertain"}:
                issue = "low_confidence_mapping"
            if issue:
                suspicious_alias_rows.append(
                    {
                        "alias_norm": alias_norm,
                        "canonical_norm": canonical_norm,
                        "issue": issue,
                    }
                )

    suspicious_alias_df = pd.DataFrame(suspicious_alias_rows).drop_duplicates()

    # Conflicts and duplicates in dry-run outputs.
    duplicate_key_count = int(
        detail.duplicated(subset=["person_id", "visit_id", "raw_mention_text"], keep=False).sum()
    )
    conflict_pred_df = (
        detail.groupby("mention_norm_baseline", as_index=False)["prediction"]
        .nunique()
        .rename(columns={"prediction": "n_unique_predictions"})
    )
    conflict_pred_df = conflict_pred_df[conflict_pred_df["n_unique_predictions"] > 1].copy()

    # Suspicious accepted Path B links by lexical heuristics.
    accepted_pathb = detail[detail["pathb_passed"] & detail["pathb_accepted"]].copy()
    accepted_pathb["lexical_overlap"] = accepted_pathb.apply(
        lambda r: _token_overlap_ratio(r["mention_norm_baseline"], r["prediction"]), axis=1
    )
    suspicious_accept = accepted_pathb[
        (accepted_pathb["lexical_overlap"] == 0.0)
        | (accepted_pathb["mention_norm_baseline"].astype(str).str.len() <= 4)
        | (accepted_pathb["pathb_margin"] < 0.08)
    ].copy()
    suspicious_accept = suspicious_accept[
        [
            "raw_mention_text",
            "mention_norm_baseline",
            "patha_term",
            "prediction",
            "pathb_score",
            "pathb_calibrated_confidence",
            "pathb_margin",
            "pathb_top_k_candidates_json",
            "lexical_overlap",
        ]
    ].head(200)

    reason_code_coverage = int(
        detail.loc[detail["pathb_abstained"], "pathb_reason_codes_json"]
        .astype(str)
        .map(lambda x: isinstance(json.loads(x), list) and len(json.loads(x)) > 0 if x.startswith("[") else False)
        .sum()
    ) if int(detail["pathb_abstained"].sum()) else 0

    transitions = [
        {"stage": "input_visits", "count": n_input_visits},
        {"stage": "visits_after_subset_and_cap", "count": n_filtered_visits},
        {"stage": "visits_with_nonempty_drugs", "count": visits_with_drugs},
        {"stage": "raw_mentions_entering_normalization", "count": int(len(detail))},
        {"stage": "baseline_nonempty", "count": int(detail["baseline_nonempty"].sum())},
        {"stage": "patha_nonempty", "count": int(detail["patha_nonempty"].sum())},
        {"stage": "patha_exact_vocab_resolved", "count": int(detail["patha_exact_vocab_resolved"].sum())},
        {"stage": "pathb_passed", "count": int(detail["pathb_passed"].sum())},
        {"stage": "pathb_accepted", "count": int((detail["pathb_passed"] & detail["pathb_accepted"]).sum())},
        {"stage": "pathb_abstained", "count": int(detail["pathb_abstained"].sum())},
    ]
    transition_df = pd.DataFrame(transitions)

    # Write artifacts.
    detail_path = out_dir / "rq1_preadj_method_diagnostics_detailed.csv"
    transition_path = out_dir / "rq1_preadj_transition_counts.csv"
    reject_reason_path = out_dir / "rq1_preadj_pathb_rejection_reasons.csv"
    unresolved_path = out_dir / "rq1_preadj_top_unresolved_raw_mentions.csv"
    accepted_path = out_dir / "rq1_preadj_top_accepted_canonical_labels.csv"
    alias_hits_path = out_dir / "rq1_preadj_patha_alias_hits.csv"
    canonical_out_path = out_dir / "rq1_preadj_patha_canonical_outputs.csv"
    suspicious_alias_path = out_dir / "rq1_preadj_patha_suspicious_alias_mappings.csv"
    conflict_pred_path = out_dir / "rq1_preadj_prediction_conflicts.csv"
    suspicious_accept_path = out_dir / "rq1_preadj_pathb_suspicious_acceptances.csv"
    summary_path = out_dir / "rq1_preadj_dryrun_summary.json"

    detail.to_csv(detail_path, index=False)
    transition_df.to_csv(transition_path, index=False)
    reject_reason_df.to_csv(reject_reason_path, index=False)
    unresolved_df.to_csv(unresolved_path, index=False)
    accepted_df.to_csv(accepted_path, index=False)
    alias_hits_df.to_csv(alias_hits_path, index=False)
    canonical_out_df.to_csv(canonical_out_path, index=False)
    suspicious_alias_df.to_csv(suspicious_alias_path, index=False)
    conflict_pred_df.to_csv(conflict_pred_path, index=False)
    suspicious_accept.to_csv(suspicious_accept_path, index=False)

    summary = {
        "inputs": {
            "note_csv": str(note_path),
            "subset_csv": str(subset_path) if subset_path and subset_path.exists() else None,
            "alias_artifact": str(alias_path) if alias_path.exists() else None,
            "canonical_vocab_path": str(vocab_path) if vocab_path and vocab_path.exists() else None,
            "pathb_calibration_json": str(calib_path) if calib_path and calib_path.exists() else None,
        },
        "counts": {
            "raw_mentions_entering_normalization": int(len(detail)),
            "baseline_nonempty_n": int(detail["baseline_nonempty"].sum()),
            "baseline_nonempty_rate": _pct(int(detail["baseline_nonempty"].sum()), int(len(detail))),
            "patha_nonempty_n": int(detail["patha_nonempty"].sum()),
            "patha_nonempty_rate": _pct(int(detail["patha_nonempty"].sum()), int(len(detail))),
            "patha_exact_vocab_resolved_n": int(detail["patha_exact_vocab_resolved"].sum()),
            "patha_exact_vocab_resolved_rate": _pct(int(detail["patha_exact_vocab_resolved"].sum()), int(len(detail))),
            "patha_alias_hit_n": int(detail["patha_alias_hit"].sum()),
            "patha_alias_hit_rate": _pct(int(detail["patha_alias_hit"].sum()), int(len(detail))),
            "pathb_passed_n": int(detail["pathb_passed"].sum()),
            "pathb_passed_rate": _pct(int(detail["pathb_passed"].sum()), int(len(detail))),
            "pathb_accept_n": int((detail["pathb_passed"] & detail["pathb_accepted"]).sum()),
            "pathb_accept_rate_within_passed": _pct(
                int((detail["pathb_passed"] & detail["pathb_accepted"]).sum()),
                int(detail["pathb_passed"].sum()),
            ),
            "pathb_abstain_n": int(detail["pathb_abstained"].sum()),
            "pathb_abstain_rate_within_passed": _pct(int(detail["pathb_abstained"].sum()), int(detail["pathb_passed"].sum())),
            "reason_code_coverage_on_abstains": _pct(reason_code_coverage, int(detail["pathb_abstained"].sum())),
            "duplicate_input_rows_n": duplicate_key_count,
            "prediction_conflict_norm_terms_n": int(len(conflict_pred_df)),
        },
        "pathb_distributions": {
            "score_quantiles_passed": _series_quantiles(detail.loc[detail["pathb_passed"], "pathb_score"]),
            "confidence_quantiles_passed": _series_quantiles(detail.loc[detail["pathb_passed"], "pathb_calibrated_confidence"]),
            "margin_quantiles_passed": _series_quantiles(detail.loc[detail["pathb_passed"], "pathb_margin"]),
        },
        "outputs": {
            "detailed_csv": str(detail_path),
            "transition_counts_csv": str(transition_path),
            "pathb_rejection_reasons_csv": str(reject_reason_path),
            "top_unresolved_mentions_csv": str(unresolved_path),
            "top_accepted_labels_csv": str(accepted_path),
            "patha_alias_hits_csv": str(alias_hits_path),
            "patha_canonical_outputs_csv": str(canonical_out_path),
            "patha_suspicious_alias_mappings_csv": str(suspicious_alias_path),
            "prediction_conflicts_csv": str(conflict_pred_path),
            "pathb_suspicious_acceptances_csv": str(suspicious_accept_path),
        },
    }

    write_run_summary(summary_path, summary)
    print(f"Saved pre-adjudication dry-run summary: {summary_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
