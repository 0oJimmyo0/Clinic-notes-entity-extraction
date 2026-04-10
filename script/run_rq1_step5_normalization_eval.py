#!/usr/bin/env python3
"""
Evaluate baseline, Path A, and Path B against adjudicated canonical drug labels.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_drug_linking import (
    PathBConfig,
    build_canonical_drug_universe,
    link_mention_to_canonical_vocab,
    load_alias_map,
    normalize_drug_text,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Evaluate normalization/linking against adjudicated canonical labels.")
    p.add_argument(
        "--adjudicated-mentions-csv",
        default="episode_extraction_results/rq1/adjudicated/rq1_adjudicated_mentions.csv",
        help="Adjudicated mentions output from run_join_adjudication_labels.py",
    )
    p.add_argument(
        "--alias-artifact",
        default="resources/lexicons/rq1_drug_aliases.csv",
        help="Path A alias CSV/JSON.",
    )
    p.add_argument(
        "--canonical-vocab-path",
        default="resources/lexicons/rq1_drug_canonical_vocab.csv",
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
        help="Optional calibration JSON produced by run_rq1_step4b_calibrate_pathb.py",
    )
    p.add_argument(
        "--output-dir",
        default="episode_extraction_results/rq1/normalization_eval",
        help="Output directory.",
    )
    return p.parse_args()


def _error_bucket(row: Dict[str, object]) -> str:
    gold = str(row.get("gold_canonical", "") or "")
    baseline = str(row.get("baseline_prediction", "") or "")
    patha = str(row.get("patha_prediction", "") or "")
    pathb = str(row.get("pathb_prediction", "") or "")
    accepted = bool(row.get("pathb_accepted"))
    reasons = set(row.get("pathb_reason_codes", []))

    if patha != gold and pathb == gold and accepted:
        return "pathb_recovered_leftover"
    if baseline != gold and patha == gold:
        return "alias_miss"
    if patha == gold and pathb == gold:
        return "patha_solved"
    if patha != gold and not accepted and "no_candidates" in reasons:
        return "vocabulary_miss"
    if patha != gold and not accepted and {"mention_too_short", "ambiguous_short_term"} & reasons:
        return "ambiguous_abbreviation"
    if patha != gold and not accepted:
        return "pathb_abstained"
    if accepted and pathb and pathb != gold:
        return "false_link"
    if baseline == gold:
        return "baseline_exact"
    return "tokenization_or_cleanup_miss"


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    adjud_path = (root / args.adjudicated_mentions_csv).resolve()
    alias_path = (root / args.alias_artifact).resolve()
    vocab_path = (root / args.canonical_vocab_path).resolve() if args.canonical_vocab_path else None
    calibration_path = (root / args.pathb_calibration_json).resolve() if args.pathb_calibration_json else None
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not adjud_path.exists():
        raise FileNotFoundError(f"Missing adjudicated mentions CSV: {adjud_path}")

    df = pd.read_csv(adjud_path).fillna("")
    required = {"raw_mention_text", "adjudicated_canonical_label", "mention_status"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Adjudicated mentions missing columns: {sorted(missing)}")
    df = df[df["adjudicated_canonical_label"].astype(str).str.strip() != ""].copy()

    alias_map = load_alias_map(alias_path) if alias_path.exists() else {}
    calibration = None
    if calibration_path and calibration_path.exists():
        calibration = json.loads(calibration_path.read_text(encoding="utf-8"))
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
        adjudicated_labels_path=adjud_path,
    )

    rows: List[Dict] = []
    for row in df.itertuples(index=False):
        raw = str(getattr(row, "raw_mention_text", "") or "")
        gold = normalize_drug_text(getattr(row, "adjudicated_canonical_label", "") or "")
        baseline_pred = normalize_drug_text(raw)
        patha_pred = normalize_drug_text(alias_map.get(baseline_pred, baseline_pred))
        pathb_decision = link_mention_to_canonical_vocab(
            raw_mention=raw,
            alias_map=alias_map,
            candidate_universe=universe,
            pathb_config=cfg,
            mention_metadata={"mention_status": str(getattr(row, "mention_status", "") or "").strip().lower()},
        )
        pathb_pred = normalize_drug_text(pathb_decision["prediction"])

        out = {
            "adjudication_unit_id": getattr(row, "adjudication_unit_id", ""),
            "person_id": getattr(row, "person_id", ""),
            "visit_id": getattr(row, "visit_id", ""),
            "note_id": getattr(row, "note_id", ""),
            "raw_mention_text": raw,
            "gold_canonical": gold,
            "mention_status": getattr(row, "mention_status", ""),
            "compare_to_structured_ehr": getattr(row, "compare_to_structured_ehr", ""),
            "baseline_prediction": baseline_pred,
            "baseline_correct": baseline_pred == gold,
            "patha_prediction": patha_pred,
            "patha_correct": patha_pred == gold,
            "pathb_prediction": pathb_pred,
            "pathb_correct": pathb_pred == gold if pathb_pred else False,
            "pathb_accepted": bool(pathb_decision["accepted"]),
            "pathb_score": round(float(pathb_decision["score"]), 6),
            "pathb_calibrated_confidence": round(float(pathb_decision["calibrated_confidence"]), 6),
            "pathb_reason_codes_json": json.dumps(pathb_decision["reason_codes"]),
            "pathb_top_k_candidates_json": json.dumps(pathb_decision["top_k_candidates"]),
            "pathb_stage": pathb_decision["stage"],
            "patha_exact_vocab_hit": bool(pathb_decision["patha_exact_vocab_hit"]),
            "patha_term": pathb_decision["patha_term"],
            "pathb_reason_codes": pathb_decision["reason_codes"],
        }
        out["error_bucket"] = _error_bucket(out)
        rows.append(out)

    detail = pd.DataFrame(rows)
    detail["pathb_margin"] = 0.0
    if len(detail):
        def _margin_from_json(text: str) -> float:
            try:
                vals = json.loads(text)
            except Exception:
                return 0.0
            if not isinstance(vals, list) or not vals:
                return 0.0
            top1 = float(vals[0].get("score", 0.0))
            top2 = float(vals[1].get("score", 0.0)) if len(vals) > 1 else 0.0
            return top1 - top2
        detail["pathb_margin"] = detail["pathb_top_k_candidates_json"].map(_margin_from_json)

    n = len(detail)
    baseline_acc = float(detail["baseline_correct"].mean()) if n else 0.0
    patha_acc = float(detail["patha_correct"].mean()) if n else 0.0
    pathb_acc = float(detail["pathb_correct"].mean()) if n else 0.0
    accepted = detail[detail["pathb_accepted"]].copy()
    accepted_precision = float(accepted["pathb_correct"].mean()) if len(accepted) else 0.0
    abstention_rate = float((~detail["pathb_accepted"]).mean()) if n else 0.0

    summary = {
        "n_mentions": int(n),
        "baseline_accuracy": round(baseline_acc, 6),
        "path_a_accuracy": round(patha_acc, 6),
        "path_b_accuracy": round(pathb_acc, 6),
        "delta_path_a_vs_baseline": round(patha_acc - baseline_acc, 6),
        "delta_path_b_vs_path_a": round(pathb_acc - patha_acc, 6),
        "path_b_abstention_rate": round(abstention_rate, 6),
        "path_b_accepted_link_precision": round(accepted_precision, 6),
        "path_b_accepted_n": int(len(accepted)),
    }

    bucket_df = (
        detail.groupby("error_bucket", as_index=False)
        .size()
        .rename(columns={"size": "count"})
        .sort_values("count", ascending=False)
    )
    detail_out = detail.drop(columns=["pathb_reason_codes"])
    detail_path = out_dir / "rq1_normalization_eval_detailed.csv"
    summary_path = out_dir / "rq1_normalization_eval_summary.json"
    bucket_path = out_dir / "rq1_normalization_error_buckets.csv"
    pathb_audit_path = out_dir / "rq1_pathb_candidate_audit.csv"

    detail_out.to_csv(detail_path, index=False)
    bucket_df.to_csv(bucket_path, index=False)
    detail_out[
        [
            "adjudication_unit_id",
            "raw_mention_text",
            "gold_canonical",
            "patha_term",
            "pathb_prediction",
            "pathb_accepted",
            "pathb_score",
            "pathb_margin",
            "pathb_calibrated_confidence",
            "pathb_reason_codes_json",
            "pathb_top_k_candidates_json",
            "error_bucket",
        ]
    ].to_csv(pathb_audit_path, index=False)

    write_run_summary(
        summary_path,
        {
            "inputs": {
                "adjudicated_mentions_csv": str(adjud_path),
                "alias_artifact": str(alias_path),
                "canonical_vocab_path": str(vocab_path) if vocab_path and vocab_path.exists() else None,
                "pathb_calibration_json": str(calibration_path) if calibration_path and calibration_path.exists() else None,
            },
            "metrics": summary,
            "outputs": {
                "detailed_csv": str(detail_path),
                "error_buckets_csv": str(bucket_path),
                "pathb_candidate_audit_csv": str(pathb_audit_path),
            },
        },
    )

    print(f"Saved normalization evaluation detail: {detail_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
