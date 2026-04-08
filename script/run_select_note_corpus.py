#!/usr/bin/env python3
"""
Define visit-level cohorts for treatment-context drug evaluation.

This refactors old note-level random capping into a paper-aligned cohort definer.

Core outputs:
- full_visit_eligible_manifest.csv
- evaluation_visit_manifest.csv            (downstream cohort)
- adjudication_subset_manifest.csv         (visit-level sample)
- evaluation_note_manifest.csv             (note traceability for downstream cohort)
- adjudication_note_manifest.csv           (note traceability for adjudication subset)
- cohort_justification_summary.json

Example:
  python resources/script/run_select_note_corpus.py \
    --notes-dir episode_notes \
    --glob 'episode_notes_chunk*.parquet' \
    --candidate-csv episode_extraction_results/archive_candidates/all_candidates_combined.csv \
    --structured-ehr-csv episode_extraction_results/rq1/rq1_ehr_entities_by_visit.csv \
    --output-dir episode_notes/manifests \
    --sampling-mode stratified \
    --stratify-by note_type,visit_count_bin,candidate_count_bin \
    --max-visits-per-patient-for-adjudication 3 \
    --max-adjudication-visits 5000
"""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Define visit-level corpora for downstream concordance and adjudication")
    ap.add_argument("--notes-dir", type=Path, default=Path("episode_notes"))
    ap.add_argument("--glob", default="episode_notes_chunk*.parquet")
    ap.add_argument(
        "--candidate-csv",
        type=Path,
        default=Path("episode_extraction_results/archive_candidates/all_candidates_combined.csv"),
        help="Stage-1 candidate span file",
    )
    ap.add_argument(
        "--structured-ehr-csv",
        type=Path,
        default=Path("episode_extraction_results/rq1/rq1_ehr_entities_by_visit.csv"),
        help="Visit-level structured entities file",
    )
    ap.add_argument("--output-dir", type=Path, default=Path("episode_notes/manifests"))

    # Inclusion/exclusion controls
    ap.add_argument("--min-note-chars", type=int, default=20)
    ap.add_argument(
        "--allowed-note-types",
        default="",
        help="Comma-separated note_title values to include; empty means all",
    )
    ap.add_argument("--exclude-template-only", action="store_true")
    ap.add_argument(
        "--require-structured-drugs-for-downstream",
        action="store_true",
        help="If set, evaluation_visit_manifest requires non-empty structured drugs",
    )

    # Adjudication subset controls
    ap.add_argument("--max-visits-per-patient-for-adjudication", type=int, default=3)
    ap.add_argument("--max-adjudication-visits", type=int, default=5000)
    ap.add_argument("--sampling-mode", choices=["all", "stratified"], default="stratified")
    ap.add_argument(
        "--stratify-by",
        default="note_type,visit_count_bin,candidate_count_bin",
        help="Comma-separated strata keys from: note_type,service,visit_count_bin,candidate_count_bin",
    )
    ap.add_argument(
        "--require-candidates-for-adjudication",
        action="store_true",
        help="If set, adjudication subset only uses visits with >=1 treatment-context candidate span",
    )

    ap.add_argument("--seed", type=int, default=42)
    return ap.parse_args()


def _normalize_note_type(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().replace({"": "<UNKNOWN_NOTE_TYPE>"})


def _is_template_like(text: str) -> bool:
    t = str(text).strip().lower()
    if not t:
        return True
    if len(t) < 40:
        return True
    markers = [
        "template",
        "autopopulated",
        "auto populated",
        "boilerplate",
        "smartphrase",
        "dotphrase",
    ]
    if any(m in t for m in markers):
        return True
    return False


def _parse_list_cell(x) -> list[str]:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return []
    if isinstance(x, list):
        vals = x
    else:
        s = str(x).strip()
        if not s or s.lower() in {"none", "nan", "[]"}:
            return []
        try:
            vals = ast.literal_eval(s)
        except Exception:
            return []
    if not isinstance(vals, list):
        return []
    out = [str(v).strip().lower() for v in vals if str(v).strip()]
    return sorted(set(out))


def _visit_count_bin(x: int) -> str:
    if x <= 1:
        return "01"
    if x <= 3:
        return "02-03"
    if x <= 6:
        return "04-06"
    if x <= 12:
        return "07-12"
    return "13+"


def _candidate_count_bin(x: int) -> str:
    if x <= 0:
        return "00"
    if x == 1:
        return "01"
    if x <= 3:
        return "02-03"
    if x <= 7:
        return "04-07"
    return "08+"


def _proportional_sample(df: pd.DataFrame, strata_col: str, n: int, seed: int) -> pd.DataFrame:
    if len(df) <= n:
        return df.copy()

    rng = np.random.default_rng(seed)
    counts = df[strata_col].value_counts(dropna=False)
    alloc = (counts / counts.sum() * n).astype(int)
    alloc[alloc == 0] = 1

    while alloc.sum() > n:
        for k in alloc.sort_values(ascending=False).index:
            if alloc.sum() <= n:
                break
            if alloc[k] > 1:
                alloc[k] -= 1

    while alloc.sum() < n:
        for k in counts.sort_values(ascending=False).index:
            if alloc.sum() >= n:
                break
            if alloc[k] < counts[k]:
                alloc[k] += 1

    out = []
    for k, take in alloc.items():
        part = df[df[strata_col] == k]
        if len(part) <= take:
            out.append(part)
        else:
            out.append(part.sample(n=take, random_state=int(rng.integers(0, 1_000_000))))
    sampled = pd.concat(out, ignore_index=True)
    if len(sampled) > n:
        sampled = sampled.sample(n=n, random_state=seed)
    return sampled


def _collect_note_ids(values: Iterable) -> str:
    ids = sorted({str(v) for v in values if pd.notna(v)})
    return json.dumps(ids)


def main() -> None:
    args = parse_args()

    files = sorted(args.notes_dir.glob(args.glob))
    if not files:
        raise SystemExit(f"No files matched: {args.notes_dir / args.glob}")

    needed = ["person_id", "visit_occurrence_id", "note_id", "note_text", "note_title"]
    frames = []
    for p in files:
        df = pd.read_parquet(p)
        miss = [c for c in needed if c not in df.columns and c != "note_title"]
        if miss:
            raise SystemExit(f"Missing columns {miss} in {p}")
        keep_cols = [c for c in ["person_id", "visit_occurrence_id", "note_id", "note_text", "note_title", "note_date", "note_datetime"] if c in df.columns]
        frames.append(df[keep_cols])

    all_notes = pd.concat(frames, ignore_index=True)
    all_notes["person_id"] = pd.to_numeric(all_notes["person_id"], errors="coerce").astype("Int64")
    all_notes["visit_occurrence_id"] = pd.to_numeric(all_notes["visit_occurrence_id"], errors="coerce").astype("Int64")
    all_notes["note_text"] = all_notes["note_text"].fillna("").astype(str)
    all_notes["note_len"] = all_notes["note_text"].str.len()
    if "note_title" not in all_notes.columns:
        all_notes["note_title"] = "<UNKNOWN_NOTE_TYPE>"
    all_notes["note_title_norm"] = _normalize_note_type(all_notes["note_title"])

    summary = {
        "notes_input": int(len(all_notes)),
        "notes_after_valid_ids": 0,
        "notes_after_non_empty": 0,
        "notes_after_min_chars": 0,
        "notes_after_note_type_filter": 0,
        "notes_after_template_filter": 0,
        "notes_after_dedup_note_id": 0,
        "notes_after_dedup_text_within_visit": 0,
    }

    work = all_notes.dropna(subset=["person_id", "visit_occurrence_id", "note_id"]).copy()
    summary["notes_after_valid_ids"] = int(len(work))

    work = work[work["note_text"].str.strip() != ""].copy()
    summary["notes_after_non_empty"] = int(len(work))

    work = work[work["note_len"] >= args.min_note_chars].copy()
    summary["notes_after_min_chars"] = int(len(work))

    if args.allowed_note_types.strip():
        allowed = {x.strip().lower() for x in args.allowed_note_types.split(",") if x.strip()}
        work = work[work["note_title_norm"].str.lower().isin(allowed)].copy()
    summary["notes_after_note_type_filter"] = int(len(work))

    if args.exclude_template_only:
        work = work[~work["note_text"].map(_is_template_like)].copy()
    summary["notes_after_template_filter"] = int(len(work))

    # Deduplicate by note_id first, then by exact text within visit.
    sort_cols = [c for c in ["note_datetime", "note_date"] if c in work.columns]
    if sort_cols:
        work = work.sort_values(sort_cols)
    work = work.drop_duplicates(subset=["note_id"], keep="last").copy()
    summary["notes_after_dedup_note_id"] = int(len(work))

    work["note_text_norm"] = work["note_text"].str.strip().str.lower().str.replace(r"\s+", " ", regex=True)
    work = work.drop_duplicates(subset=["person_id", "visit_occurrence_id", "note_text_norm"], keep="first").copy()
    summary["notes_after_dedup_text_within_visit"] = int(len(work))

    # Candidate span coverage by visit.
    cand = None
    if args.candidate_csv.exists():
        cand = pd.read_csv(args.candidate_csv)
        cand["person_id"] = pd.to_numeric(cand.get("person_id"), errors="coerce").astype("Int64")
        cand["visit_occurrence_id"] = pd.to_numeric(cand.get("visit_id"), errors="coerce").astype("Int64")
        cand = cand.dropna(subset=["person_id", "visit_occurrence_id"])
        cand_visit = (
            cand.groupby(["person_id", "visit_occurrence_id"], as_index=False)
            .agg(
                candidate_span_count=("span_text", "count"),
                candidate_note_count=("note_id", pd.Series.nunique),
                candidate_categories=("category", lambda x: json.dumps(sorted({str(v) for v in x if pd.notna(v)}))),
            )
        )
    else:
        cand_visit = pd.DataFrame(columns=["person_id", "visit_occurrence_id", "candidate_span_count", "candidate_note_count", "candidate_categories"])

    # Structured EHR drug presence by visit.
    ehr_visit = pd.DataFrame(columns=["person_id", "visit_occurrence_id", "has_structured_drug_data"])
    if args.structured_ehr_csv.exists():
        ehr = pd.read_csv(args.structured_ehr_csv)
        ehr["person_id"] = pd.to_numeric(ehr.get("person_id"), errors="coerce").astype("Int64")
        ehr["visit_occurrence_id"] = pd.to_numeric(ehr.get("visit_id"), errors="coerce").astype("Int64")
        if "drugs" in ehr.columns:
            ehr["has_structured_drug_data"] = ehr["drugs"].map(lambda x: len(_parse_list_cell(x)) > 0)
        else:
            ehr["has_structured_drug_data"] = False
        ehr_visit = ehr[["person_id", "visit_occurrence_id", "has_structured_drug_data"]].drop_duplicates()

    # Aggregate notes to visit level.
    visit_df = (
        work.groupby(["person_id", "visit_occurrence_id"], as_index=False)
        .agg(
            eligible_note_count=("note_id", pd.Series.nunique),
            note_ids_json=("note_id", _collect_note_ids),
            note_type_mode=("note_title_norm", lambda x: x.mode().iat[0] if len(x.mode()) else "<UNKNOWN_NOTE_TYPE>"),
            median_note_len=("note_len", "median"),
            max_note_len=("note_len", "max"),
        )
    )

    visit_df = visit_df.merge(cand_visit, on=["person_id", "visit_occurrence_id"], how="left")
    visit_df["candidate_span_count"] = visit_df["candidate_span_count"].fillna(0).astype(int)
    visit_df["candidate_note_count"] = visit_df["candidate_note_count"].fillna(0).astype(int)
    visit_df["candidate_categories"] = visit_df["candidate_categories"].fillna("[]")
    visit_df["has_candidate_span"] = visit_df["candidate_span_count"] > 0

    visit_df = visit_df.merge(ehr_visit, on=["person_id", "visit_occurrence_id"], how="left")
    visit_df["has_structured_drug_data"] = visit_df["has_structured_drug_data"].fillna(False)

    # Patient-level visit frequency bins.
    pt_visit_counts = visit_df.groupby("person_id")["visit_occurrence_id"].nunique().rename("patient_visit_count")
    visit_df = visit_df.merge(pt_visit_counts, on="person_id", how="left")
    visit_df["visit_count_bin"] = visit_df["patient_visit_count"].map(_visit_count_bin)
    visit_df["candidate_count_bin"] = visit_df["candidate_span_count"].map(_candidate_count_bin)

    if "service" in work.columns:
        service_mode = (
            work.groupby(["person_id", "visit_occurrence_id"]) ["service"]
            .agg(lambda x: x.mode().iat[0] if len(x.mode()) else "<UNKNOWN_SERVICE>")
            .reset_index()
            .rename(columns={"service": "service"})
        )
        visit_df = visit_df.merge(service_mode, on=["person_id", "visit_occurrence_id"], how="left")
    else:
        visit_df["service"] = "<UNKNOWN_SERVICE>"

    full_visit_eligible = visit_df.copy()

    # Downstream cohort (visit-level).
    if args.require_structured_drugs_for_downstream:
        evaluation_visit = full_visit_eligible[full_visit_eligible["has_structured_drug_data"]].copy()
    else:
        evaluation_visit = full_visit_eligible.copy()

    # Adjudication candidate pool.
    adjud_pool = full_visit_eligible.copy()
    if args.require_candidates_for_adjudication:
        adjud_pool = adjud_pool[adjud_pool["has_candidate_span"]].copy()

    rng = np.random.default_rng(args.seed)
    adjud_pool["_rand"] = rng.random(len(adjud_pool))
    adjud_pool = adjud_pool.sort_values(["person_id", "_rand"])
    adjud_pool["_pt_rank"] = adjud_pool.groupby("person_id").cumcount() + 1
    if args.max_visits_per_patient_for_adjudication > 0:
        adjud_pool = adjud_pool[
            adjud_pool["_pt_rank"] <= args.max_visits_per_patient_for_adjudication
        ].copy()

    # Optional stratified/sample cap for adjudication subset.
    max_adj = max(args.max_adjudication_visits, 0)
    if max_adj > 0 and len(adjud_pool) > max_adj:
        if args.sampling_mode == "all":
            adjud_subset = adjud_pool.sample(n=max_adj, random_state=args.seed)
        else:
            allowed_strata = {"note_type", "service", "visit_count_bin", "candidate_count_bin"}
            keys = [k.strip() for k in args.stratify_by.split(",") if k.strip()]
            keys = [k for k in keys if k in allowed_strata]
            if not keys:
                keys = ["note_type", "candidate_count_bin"]
            key_map = {
                "note_type": "note_type_mode",
                "service": "service",
                "visit_count_bin": "visit_count_bin",
                "candidate_count_bin": "candidate_count_bin",
            }
            strata_cols = [key_map[k] for k in keys]
            adjud_pool["_strata"] = adjud_pool[strata_cols].astype(str).agg("|".join, axis=1)
            adjud_subset = _proportional_sample(adjud_pool, "_strata", max_adj, args.seed)
    else:
        adjud_subset = adjud_pool.copy()

    # Note-level traceability manifests.
    eval_visits = set(zip(evaluation_visit["person_id"], evaluation_visit["visit_occurrence_id"]))
    adjud_visits = set(zip(adjud_subset["person_id"], adjud_subset["visit_occurrence_id"]))

    eval_note_manifest = work[
        work.apply(lambda r: (r["person_id"], r["visit_occurrence_id"]) in eval_visits, axis=1)
    ][["person_id", "visit_occurrence_id", "note_id", "note_title_norm", "note_len"]].copy()

    adjud_note_manifest = work[
        work.apply(lambda r: (r["person_id"], r["visit_occurrence_id"]) in adjud_visits, axis=1)
    ][["person_id", "visit_occurrence_id", "note_id", "note_title_norm", "note_len"]].copy()

    # Write outputs.
    args.output_dir.mkdir(parents=True, exist_ok=True)
    full_visit_path = args.output_dir / "full_visit_eligible_manifest.csv"
    eval_visit_path = args.output_dir / "evaluation_visit_manifest.csv"
    adjud_visit_path = args.output_dir / "adjudication_subset_manifest.csv"
    eval_note_path = args.output_dir / "evaluation_note_manifest.csv"
    adjud_note_path = args.output_dir / "adjudication_note_manifest.csv"
    summary_path = args.output_dir / "cohort_justification_summary.json"

    full_visit_eligible.to_csv(full_visit_path, index=False)
    evaluation_visit.to_csv(eval_visit_path, index=False)
    adjud_subset.to_csv(adjud_visit_path, index=False)
    eval_note_manifest.to_csv(eval_note_path, index=False)
    adjud_note_manifest.to_csv(adjud_note_path, index=False)

    summary.update(
        {
            "full_eligible_visits": int(len(full_visit_eligible)),
            "full_eligible_patients": int(full_visit_eligible["person_id"].nunique()),
            "visits_with_candidate_span": int(full_visit_eligible["has_candidate_span"].sum()),
            "visits_with_structured_drugs": int(full_visit_eligible["has_structured_drug_data"].sum()),
            "evaluation_visits": int(len(evaluation_visit)),
            "evaluation_patients": int(evaluation_visit["person_id"].nunique()) if len(evaluation_visit) else 0,
            "adjudication_visits": int(len(adjud_subset)),
            "adjudication_patients": int(adjud_subset["person_id"].nunique()) if len(adjud_subset) else 0,
            "sampling_mode": args.sampling_mode,
            "stratify_by": args.stratify_by,
            "require_candidates_for_adjudication": bool(args.require_candidates_for_adjudication),
            "require_structured_drugs_for_downstream": bool(args.require_structured_drugs_for_downstream),
            "output_files": {
                "full_visit_eligible_manifest": str(full_visit_path),
                "evaluation_visit_manifest": str(eval_visit_path),
                "adjudication_subset_manifest": str(adjud_visit_path),
                "evaluation_note_manifest": str(eval_note_path),
                "adjudication_note_manifest": str(adjud_note_path),
            },
        }
    )

    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("full_eligible_visits", len(full_visit_eligible))
    print("evaluation_visits", len(evaluation_visit))
    print("adjudication_visits", len(adjud_subset))
    print("output_dir", args.output_dir)
    print("summary", summary_path)


if __name__ == "__main__":
    main()
