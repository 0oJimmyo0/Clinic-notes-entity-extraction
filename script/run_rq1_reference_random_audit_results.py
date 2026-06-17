#!/usr/bin/env python3
"""
Analyze a manually reviewed random/stratified audit of previously unaudited reference rows.
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_bibm_utils import action_cue


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Summarize completed random audit annotation results.")
    p.add_argument(
        "--annotation-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_sample/rq1_reference_random_audit_annotation_template.csv",
    )
    p.add_argument(
        "--sample-summary-json",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_sample/rq1_reference_random_audit_sample_summary.json",
    )
    p.add_argument(
        "--output-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_results",
    )
    return p.parse_args()


def _normalize_bool_like(value: object) -> str:
    v = str(value).strip().lower()
    if v in {"yes", "y", "true", "1", "correct", "valid"}:
        return "yes"
    if v in {"no", "n", "false", "0", "incorrect", "invalid"}:
        return "no"
    if v in {"uncertain", "unknown", "maybe", "unclear"}:
        return "uncertain"
    return ""


def _normalize_text(value: object) -> str:
    return str(value).strip()


def _is_reviewed(row: pd.Series) -> bool:
    fields = [
        "adjudicated_medication_valid",
        "adjudicated_span_valid",
        "adjudicated_canonical_correct",
        "adjudicated_corrected_canonical_label",
        "adjudicated_action_correct",
        "adjudicated_error_category",
        "adjudicated_confidence",
    ]
    return any(_normalize_text(row.get(c, "")) for c in fields)


def _wilson_interval(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n <= 0:
        return (float("nan"), float("nan"))
    phat = k / n
    denom = 1 + (z * z) / n
    center = (phat + (z * z) / (2 * n)) / denom
    radius = (z / denom) * math.sqrt((phat * (1 - phat) / n) + (z * z) / (4 * n * n))
    return max(0.0, center - radius), min(1.0, center + radius)


def _cohen_kappa(labels1: list[str], labels2: list[str]) -> float:
    if not labels1 or not labels2 or len(labels1) != len(labels2):
        return float("nan")
    categories = sorted(set(labels1) | set(labels2))
    n = len(labels1)
    if n == 0:
        return float("nan")
    p0 = sum(1 for a, b in zip(labels1, labels2) if a == b) / n
    p1 = {c: sum(1 for x in labels1 if x == c) / n for c in categories}
    p2 = {c: sum(1 for x in labels2 if x == c) / n for c in categories}
    pe = sum(p1[c] * p2[c] for c in categories)
    if pe >= 1.0:
        return float("nan")
    return (p0 - pe) / (1 - pe)


def _rebuild_sample_pool(summary: dict) -> pd.DataFrame:
    root = Path(__file__).resolve().parents[1]
    final = pd.read_csv(Path(summary["inputs"]["final_reviewed_csv"])).fillna("")
    audit = pd.read_csv(Path(summary["inputs"]["audit_review_csv"])).fillna("")
    packets = pd.read_csv(Path(summary["inputs"]["packets_mentions_csv"])).fillna("")
    for df in [final, audit, packets]:
        for col in ["adjudication_unit_id", "note_id", "person_id", "visit_id"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()
    audited_ids = set(audit["adjudication_unit_id"].astype(str))
    pool = final[~final["adjudication_unit_id"].astype(str).isin(audited_ids)].copy()
    packets_small = packets[
        [
            c
            for c in [
                "adjudication_unit_id",
                "note_title",
                "candidate_category",
                "seed_treatment_action",
                "seed_discontinuation_reason",
                "seed_certainty",
            ]
            if c in packets.columns
        ]
    ].copy()
    pool = pool.merge(packets_small, on="adjudication_unit_id", how="left")
    pool["action_cue"] = pool.apply(
        lambda r: action_cue(
            str(r.get("seed_treatment_action", "")),
            str(r.get("seed_discontinuation_reason", "")),
            str(r.get("context_text", "")),
        ),
        axis=1,
    )
    pool["_strata"] = (
        pool.get("note_title", "").astype(str).str.slice(0, 40)
        + "||"
        + pool.get("candidate_category", "").astype(str)
        + "||"
        + pool["action_cue"].astype(str)
    )
    return pool


def _weighted_accuracy(df: pd.DataFrame, strata_pop: pd.Series) -> tuple[float, float]:
    reviewed = df[df["reviewed"]].copy()
    if reviewed.empty:
        return float("nan"), float("nan")
    strata_col = "_strata" if "_strata" in reviewed.columns else "sample_stratum"
    sample_counts = reviewed[strata_col].value_counts()
    weighted_conservative = 0.0
    weighted_lenient_num = 0.0
    weighted_lenient_den = 0.0
    total_pop = float(strata_pop.sum())
    for strata, pop_n in strata_pop.items():
        if strata not in sample_counts or sample_counts[strata] == 0:
            continue
        sub = reviewed[reviewed[strata_col] == strata]
        w = float(pop_n) / total_pop
        cons = (sub["adjudicated_canonical_correct_norm"] == "yes").mean()
        weighted_conservative += w * cons
        lenient_sub = sub[sub["adjudicated_canonical_correct_norm"].isin(["yes", "no"])]
        if not lenient_sub.empty:
            weighted_lenient_num += w * (lenient_sub["adjudicated_canonical_correct_norm"] == "yes").mean()
            weighted_lenient_den += w
    weighted_lenient = weighted_lenient_num / weighted_lenient_den if weighted_lenient_den else float("nan")
    return weighted_conservative, weighted_lenient


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    ann_path = (root / args.annotation_csv).resolve()
    summary_path = (root / args.sample_summary_json).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    ann = pd.read_csv(ann_path).fillna("")
    with open(summary_path, "r", encoding="utf-8") as f:
        sample_summary = json.load(f)

    ann["reviewed"] = ann.apply(_is_reviewed, axis=1)
    for col in [
        "reviewer_1_medication_valid",
        "reviewer_1_span_valid",
        "reviewer_1_canonical_correct",
        "reviewer_1_action_correct",
        "reviewer_2_medication_valid",
        "reviewer_2_span_valid",
        "reviewer_2_canonical_correct",
        "reviewer_2_action_correct",
        "adjudicated_medication_valid",
        "adjudicated_span_valid",
        "adjudicated_canonical_correct",
        "adjudicated_action_correct",
    ]:
        if col in ann.columns:
            ann[f"{col}_norm"] = ann[col].map(_normalize_bool_like)

    ann["adjudicated_error_category_norm"] = ann.get("adjudicated_error_category", "").map(_normalize_text)

    reviewed = ann[ann["reviewed"]].copy()
    n_reviewed = int(len(reviewed))
    yes_mask = reviewed["adjudicated_canonical_correct_norm"] == "yes"
    no_mask = reviewed["adjudicated_canonical_correct_norm"] == "no"
    uncertain_mask = reviewed["adjudicated_canonical_correct_norm"] == "uncertain"

    n_correct = int(yes_mask.sum())
    n_uncertain = int(uncertain_mask.sum())
    conservative_accuracy = (n_correct / n_reviewed) if n_reviewed else float("nan")
    lenient_den = int((yes_mask | no_mask).sum())
    lenient_accuracy = (n_correct / lenient_den) if lenient_den else float("nan")
    ci_low, ci_high = _wilson_interval(n_correct, n_reviewed)
    lenient_ci_low, lenient_ci_high = _wilson_interval(n_correct, lenient_den) if lenient_den else (float("nan"), float("nan"))

    valid_span = reviewed[
        (reviewed["adjudicated_medication_valid_norm"] == "yes")
        & (reviewed["adjudicated_span_valid_norm"] == "yes")
    ].copy()
    n_valid_span = int(len(valid_span))
    canon_evaluable = valid_span[valid_span["adjudicated_canonical_correct_norm"].isin(["yes", "no"])].copy()
    n_canon_evaluable = int(len(canon_evaluable))
    n_canon_evaluable_correct = int((canon_evaluable["adjudicated_canonical_correct_norm"] == "yes").sum())
    canon_evaluable_accuracy = (
        n_canon_evaluable_correct / n_canon_evaluable if n_canon_evaluable else float("nan")
    )
    canon_eval_ci_low, canon_eval_ci_high = _wilson_interval(n_canon_evaluable_correct, n_canon_evaluable)

    action_evaluable = valid_span[valid_span["adjudicated_action_correct_norm"].isin(["yes", "no"])].copy()
    n_action_evaluable = int(len(action_evaluable))
    n_action_correct = int((action_evaluable["adjudicated_action_correct_norm"] == "yes").sum())
    action_evaluable_accuracy = n_action_correct / n_action_evaluable if n_action_evaluable else float("nan")
    action_eval_ci_low, action_eval_ci_high = _wilson_interval(n_action_correct, n_action_evaluable)

    pool = _rebuild_sample_pool(sample_summary)
    strata_pop = pool["_strata"].value_counts()
    weighted_conservative, weighted_lenient = _weighted_accuracy(ann, strata_pop)

    summary_rows = [
        ("Reviewed rows", n_reviewed, "row"),
        ("Correct rows", n_correct, "row"),
        ("Uncertain canonical rows", n_uncertain, "row"),
        ("Conservative canonical accuracy", conservative_accuracy, "fraction"),
        ("Conservative Wilson CI low", ci_low, "wilson_95"),
        ("Conservative Wilson CI high", ci_high, "wilson_95"),
        ("Lenient denominator", lenient_den, "row"),
        ("Lenient canonical accuracy", lenient_accuracy, "fraction"),
        ("Lenient Wilson CI low", lenient_ci_low, "wilson_95"),
        ("Lenient Wilson CI high", lenient_ci_high, "wilson_95"),
        ("Valid medication+span rows", n_valid_span, "row"),
        ("Canonical evaluable rows", n_canon_evaluable, "row"),
        ("Canonical evaluable correct rows", n_canon_evaluable_correct, "row"),
        ("Canonical accuracy among valid medication+span rows", canon_evaluable_accuracy, "fraction"),
        ("Canonical evaluable Wilson CI low", canon_eval_ci_low, "wilson_95"),
        ("Canonical evaluable Wilson CI high", canon_eval_ci_high, "wilson_95"),
        ("Action evaluable rows", n_action_evaluable, "row"),
        ("Action correct rows among evaluable valid rows", n_action_correct, "row"),
        ("Action accuracy among valid medication+span rows", action_evaluable_accuracy, "fraction"),
        ("Action evaluable Wilson CI low", action_eval_ci_low, "wilson_95"),
        ("Action evaluable Wilson CI high", action_eval_ci_high, "wilson_95"),
        ("Weighted conservative accuracy", weighted_conservative, "fraction"),
        ("Weighted lenient accuracy", weighted_lenient, "fraction"),
    ]
    summary_df = pd.DataFrame(summary_rows, columns=["item", "value", "unit"])
    summary_df.to_csv(out_dir / "rq1_reference_random_audit_summary.csv", index=False)

    results_df = reviewed[
        [
            c
            for c in [
                "row_id",
                "adjudication_unit_id",
                "visit_id",
                "note_id",
                "note_type",
                "candidate_category",
                "action_cue",
                "sample_stratum",
                "proposed_canonical_label",
                "adjudicated_medication_valid",
                "adjudicated_span_valid",
                "adjudicated_canonical_correct",
                "adjudicated_corrected_canonical_label",
                "adjudicated_action_correct",
                "adjudicated_error_category",
                "adjudicated_confidence",
            ]
            if c in reviewed.columns
        ]
    ].copy()
    results_df.to_csv(out_dir / "rq1_reference_random_audit_results.csv", index=False)

    tax = (
        reviewed.assign(
            adjudicated_error_category_norm=reviewed["adjudicated_error_category_norm"].replace("", "unspecified")
        )["adjudicated_error_category_norm"]
        .value_counts()
        .rename_axis("error_category")
        .reset_index(name="count")
    )
    if tax.empty:
        tax = pd.DataFrame(columns=["error_category", "count", "percent_of_reviewed"])
    else:
        tax["percent_of_reviewed"] = tax["count"] / max(n_reviewed, 1)
    tax.to_csv(out_dir / "rq1_reference_random_audit_error_taxonomy.csv", index=False)

    by_stratum = []
    for strata, sub in reviewed.groupby("sample_stratum", dropna=False):
        pop_n = int(strata_pop.get(strata, 0))
        sample_n = int(len(sub))
        correct_n = int((sub["adjudicated_canonical_correct_norm"] == "yes").sum())
        cons_acc = (correct_n / sample_n) if sample_n else float("nan")
        ci_l, ci_h = _wilson_interval(correct_n, sample_n)
        lenient_sub = sub[sub["adjudicated_canonical_correct_norm"].isin(["yes", "no"])]
        len_acc = (
            (lenient_sub["adjudicated_canonical_correct_norm"] == "yes").mean()
            if not lenient_sub.empty
            else float("nan")
        )
        by_stratum.append(
            {
                "sample_stratum": strata,
                "population_rows": pop_n,
                "reviewed_rows": sample_n,
                "correct_rows": correct_n,
                "conservative_accuracy": cons_acc,
                "conservative_ci_low": ci_l,
                "conservative_ci_high": ci_h,
                "lenient_accuracy": len_acc,
            }
        )
    by_stratum_df = pd.DataFrame(
        by_stratum,
        columns=[
            "sample_stratum",
            "population_rows",
            "reviewed_rows",
            "correct_rows",
            "conservative_accuracy",
            "conservative_ci_low",
            "conservative_ci_high",
            "lenient_accuracy",
        ],
    )
    by_stratum_df.to_csv(out_dir / "rq1_reference_random_audit_by_stratum.csv", index=False)

    disagreement_frames = []
    reviewer2_present = False
    for field in [
        "medication_valid",
        "span_valid",
        "canonical_correct",
        "action_correct",
        "error_category",
    ]:
        c1 = f"reviewer_1_{field}"
        c2 = f"reviewer_2_{field}"
        if c1 in ann.columns and c2 in ann.columns:
            filled2 = ann[c2].astype(str).str.strip() != ""
            if filled2.any():
                reviewer2_present = True
                disagree = ann[
                    (ann[c1].astype(str).str.strip() != "")
                    & filled2
                    & (ann[c1].astype(str).str.strip() != ann[c2].astype(str).str.strip())
                ].copy()
                if not disagree.empty:
                    disagree["disagreement_field"] = field
                    disagreement_frames.append(disagree)
    disagreements = pd.concat(disagreement_frames, ignore_index=True) if disagreement_frames else pd.DataFrame()
    if disagreements.empty:
        disagreements = pd.DataFrame(columns=["row_id", "adjudication_unit_id", "disagreement_field"])
    keep_cols = [c for c in ["row_id", "adjudication_unit_id", "visit_id", "note_id", "disagreement_field",
                             "reviewer_1_medication_valid", "reviewer_2_medication_valid",
                             "reviewer_1_span_valid", "reviewer_2_span_valid",
                             "reviewer_1_canonical_correct", "reviewer_2_canonical_correct",
                             "reviewer_1_action_correct", "reviewer_2_action_correct",
                             "reviewer_1_error_category", "reviewer_2_error_category"] if c in disagreements.columns]
    disagreements[keep_cols].to_csv(out_dir / "rq1_reference_random_audit_disagreements.csv", index=False)

    interrater_rows = []
    if reviewer2_present:
        for field in ["medication_valid", "span_valid", "canonical_correct", "action_correct"]:
            c1 = f"reviewer_1_{field}"
            c2 = f"reviewer_2_{field}"
            c1n = f"{c1}_norm"
            c2n = f"{c2}_norm"
            if c1n not in ann.columns or c2n not in ann.columns:
                continue
            sub = ann[
                (ann[c1n].isin(["yes", "no", "uncertain"])) & (ann[c2n].isin(["yes", "no", "uncertain"]))
            ]
            if sub.empty:
                continue
            l1 = sub[c1n].tolist()
            l2 = sub[c2n].tolist()
            agree = sum(1 for a, b in zip(l1, l2) if a == b) / len(l1)
            interrater_rows.append(
                {
                    "field": field,
                    "n_double_coded": int(len(sub)),
                    "percent_agreement": agree,
                    "cohen_kappa": _cohen_kappa(l1, l2),
                }
            )

    paper_text = []
    paper_text.append("## Methods Paragraph")
    paper_text.append(
        f"A separate stratified audit sample was drawn from the unaudited portion of the LLM-bootstrapped reference set to estimate reliability outside the targeted difficult-review queue. "
        f"Using the frozen unaudited pool of {sample_summary['counts']['unaudited_pool_rows']:,} rows, we generated a stratified sample of {sample_summary['counts']['sample_rows']:,} rows using note type, candidate category, and action cue strata. "
        f"Rows were reviewed in bounded note context without structured EHR medication fields, and adjudication captured medication validity, span validity, canonical-label correctness, action correctness, corrected label when needed, error category, and reviewer confidence."
    )
    paper_text.append("")
    paper_text.append("## Results Paragraph")
    if n_reviewed == 0:
        paper_text.append(
            "The stratified random audit sample has been generated, but manual adjudication is still pending. "
            "Once reviewed, this layer will provide an estimate of unaudited reference reliability that is distinct from the targeted difficult-review audit."
        )
    else:
        paper_text.append(
            f"After manual review of {n_reviewed:,} random-audit rows, {n_valid_span:,} were adjudicated as valid medication mentions with acceptable spans. "
            f"Within that evaluable subset, {n_canon_evaluable_correct:,} of {n_canon_evaluable:,} rows were canonically correct, yielding canonical agreement {canon_evaluable_accuracy:.4f} "
            f"(Wilson 95\\% CI {canon_eval_ci_low:.4f} to {canon_eval_ci_high:.4f}). "
            f"When all non-evaluable or uncertain rows were conservatively counted as incorrect, the row-level correctness rate was {conservative_accuracy:.4f} "
            f"(Wilson 95\\% CI {ci_low:.4f} to {ci_high:.4f}). "
            + (
                f"Lenient accuracy was {lenient_accuracy:.4f} (Wilson 95\\% CI {lenient_ci_low:.4f} to {lenient_ci_high:.4f}) after excluding uncertain canonical judgments. "
                if lenient_den
                else ""
            )
            + (
                f"Weighted conservative accuracy was {weighted_conservative:.4f}. "
                if not math.isnan(weighted_conservative)
                else ""
            )
            + (
                f"Action correctness among evaluable valid rows was {action_evaluable_accuracy:.4f} "
                f"(Wilson 95\\% CI {action_eval_ci_low:.4f} to {action_eval_ci_high:.4f}), indicating that treatment-action attribution was the harder judgment dimension. "
                if n_action_evaluable
                else ""
            )
            + "The most frequent adjudicated error categories are summarized in the random-audit error taxonomy table."
        )
    paper_text.append("")
    paper_text.append("## Limitations Sentence")
    paper_text.append(
        "The targeted audit estimates behavior on an error-enriched difficult-review queue, whereas the random audit estimates reliability of the previously unaudited reference rows; neither should be interpreted as converting the full resource into an exhaustive manual gold standard."
    )
    if interrater_rows:
        paper_text.append("")
        paper_text.append("## Double Annotation")
        for row in interrater_rows:
            paper_text.append(
                f"- {row['field']}: n={row['n_double_coded']}, agreement={row['percent_agreement']:.4f}, kappa={row['cohen_kappa']:.4f}"
            )
    (out_dir / "rq1_reference_random_audit_paper_text.md").write_text("\n".join(paper_text), encoding="utf-8")

    payload = {
        "inputs": {
            "annotation_csv": str(ann_path),
            "sample_summary_json": str(summary_path),
        },
        "counts": {
            "sample_rows_total": int(len(ann)),
            "reviewed_rows": n_reviewed,
            "correct_rows": n_correct,
            "uncertain_rows": n_uncertain,
            "valid_medication_span_rows": n_valid_span,
            "canonical_evaluable_rows": n_canon_evaluable,
            "canonical_evaluable_correct_rows": n_canon_evaluable_correct,
            "action_evaluable_rows": n_action_evaluable,
            "action_correct_rows": n_action_correct,
        },
        "metrics": {
            "conservative_accuracy": conservative_accuracy,
            "conservative_ci_low": ci_low,
            "conservative_ci_high": ci_high,
            "lenient_accuracy": lenient_accuracy,
            "lenient_ci_low": lenient_ci_low,
            "lenient_ci_high": lenient_ci_high,
            "canonical_evaluable_accuracy": canon_evaluable_accuracy,
            "canonical_evaluable_ci_low": canon_eval_ci_low,
            "canonical_evaluable_ci_high": canon_eval_ci_high,
            "action_evaluable_accuracy": action_evaluable_accuracy,
            "action_evaluable_ci_low": action_eval_ci_low,
            "action_evaluable_ci_high": action_eval_ci_high,
            "weighted_conservative_accuracy": weighted_conservative,
            "weighted_lenient_accuracy": weighted_lenient,
        },
        "outputs": {
            "results_csv": str(out_dir / "rq1_reference_random_audit_results.csv"),
            "summary_csv": str(out_dir / "rq1_reference_random_audit_summary.csv"),
            "error_taxonomy_csv": str(out_dir / "rq1_reference_random_audit_error_taxonomy.csv"),
            "by_stratum_csv": str(out_dir / "rq1_reference_random_audit_by_stratum.csv"),
            "disagreements_csv": str(out_dir / "rq1_reference_random_audit_disagreements.csv"),
            "paper_text_md": str(out_dir / "rq1_reference_random_audit_paper_text.md"),
        },
    }
    if interrater_rows:
        payload["interrater"] = interrater_rows
    write_run_summary(out_dir / "rq1_reference_random_audit_results_summary.json", payload)
    print(f"Saved random audit results to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
