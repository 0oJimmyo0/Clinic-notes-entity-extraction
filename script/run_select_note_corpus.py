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
- full_visit_eligible_notes.parquet        (optional note payload export)
- evaluation_cohort_notes.parquet          (optional note payload export)
- adjudication_subset_notes.parquet        (optional note payload export)
- cohort_justification_summary.json

Example:
  python script/run_select_note_corpus.py \
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


DEFAULT_CLINIC_NOTE_TYPES = [
    "Progress Notes",
    "Assessment & Plan Note",
    "Patient Instructions",
    "H&P",
    "H&P (View-Only)",
    "Research Coordinator Notes",
    "Consults",
    "Discharge Instructions",
]


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Define visit-level corpora for downstream concordance and adjudication")
    ap.add_argument("--notes-dir", type=Path, default=Path("../episode_notes/subcohort_clinic_like_20k_30k"))
    ap.add_argument("--glob", default="notes.parquet")
    ap.add_argument(
        "--note-text-col-candidates",
        default="note_text_full,full_note_text,note_text,text",
        help="Comma-separated note text columns to try in order.",
    )
    ap.add_argument(
        "--candidate-csv",
        type=Path,
        default=Path("../episode_extraction_results/clinic_like_20k_30k/candidates/all_candidates_combined.csv"),
        help="Stage-1 candidate span file",
    )
    ap.add_argument(
        "--structured-ehr-csv",
        type=Path,
        default=Path("../episode_extraction_results/clinic_like_20k_30k/rq1/rq1_ehr_entities_by_visit.csv"),
        help="Visit-level structured entities file",
    )
    ap.add_argument("--output-dir", type=Path, default=Path("../episode_notes/manifests_clinic_like_20k_30k"))
    ap.add_argument(
        "--write-cohort-note-parquet",
        action="store_true",
        help="Write cohort note parquet files under output-dir.",
    )
    ap.add_argument(
        "--no-write-cohort-note-parquet",
        action="store_true",
        help="Disable writing cohort note parquet files.",
    )

    # Inclusion/exclusion controls
    ap.add_argument("--min-note-chars", type=int, default=20)
    ap.add_argument(
        "--allowed-note-types",
        default=",".join(DEFAULT_CLINIC_NOTE_TYPES),
        help=(
            "Comma-separated note_title values to include. "
            "Defaults to clinic-like note titles for the active main pipeline."
        ),
    )
    ap.add_argument(
        "--include-non-clinic-notes",
        action="store_true",
        help="Optional legacy mode: disable note-type filtering and include all note types.",
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


def _resolve_note_text_col(df: pd.DataFrame, candidates: list[str]) -> str:
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(f"No note text column found. Tried: {candidates}")


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
    note_text_col_candidates = [x.strip() for x in str(args.note_text_col_candidates).split(",") if x.strip()]
    write_note_parquet = bool(not args.no_write_cohort_note_parquet)

    files = sorted(args.notes_dir.glob(args.glob))
    if not files:
        raise SystemExit(f"No files matched: {args.notes_dir / args.glob}")

    needed = ["person_id", "visit_occurrence_id", "note_id", "note_title"]
    frames = []
    used_note_text_cols: list[str] = []
    for p in files:
        df = pd.read_parquet(p)
        miss = [c for c in needed if c not in df.columns and c != "note_title"]
        if miss:
            raise SystemExit(f"Missing columns {miss} in {p}")
        note_text_col = _resolve_note_text_col(df, note_text_col_candidates)
        used_note_text_cols.append(note_text_col)
        keep_cols = [
            c
            for c in ["person_id", "visit_occurrence_id", "note_id", "note_title", "note_date", "note_datetime"]
            if c in df.columns
        ]
        chunk = df[keep_cols].copy()
        chunk["note_text"] = df[note_text_col]
        chunk["source_note_text_col"] = note_text_col
        frames.append(chunk)

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
        "note_text_col_candidates": note_text_col_candidates,
        "used_note_text_cols": sorted(set(used_note_text_cols)),
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

    clinic_types = {x.strip().lower() for x in args.allowed_note_types.split(",") if x.strip()}
    if (not args.include_non_clinic_notes) and clinic_types:
        work = work[work["note_title_norm"].str.lower().isin(clinic_types)].copy()
    summary["notes_after_note_type_filter"] = int(len(work))

    work["is_clinic_like_note"] = work["note_title_norm"].str.lower().isin(clinic_types) if clinic_types else False

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

    if len(work):
        note_len = work["note_len"]
        max_note_len = int(note_len.max())
        pct_eq_max = float((note_len == max_note_len).mean() * 100.0)
        summary["max_note_len_after_dedup"] = max_note_len
        summary["pct_note_len_eq_max_after_dedup"] = pct_eq_max
        summary["pct_note_len_eq_2000_after_dedup"] = float((note_len == 2000).mean() * 100.0)
        summary["pct_note_len_ge_1800_after_dedup"] = float((note_len >= 1800).mean() * 100.0)
        summary["suspected_hard_cap_2000"] = bool(max_note_len == 2000 and pct_eq_max >= 10.0)
    else:
        summary["max_note_len_after_dedup"] = 0
        summary["pct_note_len_eq_max_after_dedup"] = 0.0
        summary["pct_note_len_eq_2000_after_dedup"] = 0.0
        summary["pct_note_len_ge_1800_after_dedup"] = 0.0
        summary["suspected_hard_cap_2000"] = False

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
            is_clinic_like_visit=("is_clinic_like_note", "max"),
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

    # Note-level traceability manifests (merge-based for speed).
    eval_visit_keys = evaluation_visit[["person_id", "visit_occurrence_id"]].drop_duplicates()
    adjud_visit_keys = adjud_subset[["person_id", "visit_occurrence_id"]].drop_duplicates()

    eval_notes = work.merge(eval_visit_keys, on=["person_id", "visit_occurrence_id"], how="inner")
    adjud_notes = work.merge(adjud_visit_keys, on=["person_id", "visit_occurrence_id"], how="inner")

    eval_note_manifest = eval_notes[
        ["person_id", "visit_occurrence_id", "note_id", "note_title_norm", "note_len", "is_clinic_like_note"]
    ].copy()
    adjud_note_manifest = adjud_notes[
        ["person_id", "visit_occurrence_id", "note_id", "note_title_norm", "note_len", "is_clinic_like_note"]
    ].copy()

    # Write outputs.
    args.output_dir.mkdir(parents=True, exist_ok=True)
    full_visit_path = args.output_dir / "full_visit_eligible_manifest.csv"
    eval_visit_path = args.output_dir / "evaluation_visit_manifest.csv"
    adjud_visit_path = args.output_dir / "adjudication_subset_manifest.csv"
    eval_note_path = args.output_dir / "evaluation_note_manifest.csv"
    adjud_note_path = args.output_dir / "adjudication_note_manifest.csv"
    full_notes_parquet_path = args.output_dir / "full_visit_eligible_notes.parquet"
    eval_notes_parquet_path = args.output_dir / "evaluation_cohort_notes.parquet"
    adjud_notes_parquet_path = args.output_dir / "adjudication_subset_notes.parquet"
    summary_path = args.output_dir / "cohort_justification_summary.json"

    full_visit_eligible.to_csv(full_visit_path, index=False)
    evaluation_visit.to_csv(eval_visit_path, index=False)
    adjud_subset.to_csv(adjud_visit_path, index=False)
    eval_note_manifest.to_csv(eval_note_path, index=False)
    adjud_note_manifest.to_csv(adjud_note_path, index=False)

    if write_note_parquet:
        base_cols = [
            c
            for c in [
                "person_id",
                "visit_occurrence_id",
                "note_id",
                "note_date",
                "note_datetime",
                "note_title",
                "note_title_norm",
                "note_len",
                "note_text",
                "source_note_text_col",
                "is_clinic_like_note",
            ]
            if c in work.columns
        ]
        work[base_cols].drop_duplicates(subset=["note_id"]).to_parquet(full_notes_parquet_path, index=False)
        eval_notes[base_cols].drop_duplicates(subset=["note_id"]).to_parquet(eval_notes_parquet_path, index=False)
        adjud_notes[base_cols].drop_duplicates(subset=["note_id"]).to_parquet(adjud_notes_parquet_path, index=False)

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
            "write_cohort_note_parquet": write_note_parquet,
            "require_candidates_for_adjudication": bool(args.require_candidates_for_adjudication),
            "require_structured_drugs_for_downstream": bool(args.require_structured_drugs_for_downstream),
            "clinic_note_only_main_pipeline": bool(not args.include_non_clinic_notes),
            "clinic_note_types": sorted(clinic_types),
            "full_eligible_clinic_like_notes": int(work["is_clinic_like_note"].sum()),
            "full_eligible_non_clinic_like_notes": int((~work["is_clinic_like_note"]).sum()),
            "output_files": {
                "full_visit_eligible_manifest": str(full_visit_path),
                "evaluation_visit_manifest": str(eval_visit_path),
                "adjudication_subset_manifest": str(adjud_visit_path),
                "evaluation_note_manifest": str(eval_note_path),
                "adjudication_note_manifest": str(adjud_note_path),
                "full_visit_eligible_notes_parquet": str(full_notes_parquet_path) if write_note_parquet else None,
                "evaluation_cohort_notes_parquet": str(eval_notes_parquet_path) if write_note_parquet else None,
                "adjudication_subset_notes_parquet": str(adjud_notes_parquet_path) if write_note_parquet else None,
            },
        }
    )

    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print("full_eligible_visits", len(full_visit_eligible))
    print("evaluation_visits", len(evaluation_visit))
    print("adjudication_visits", len(adjud_subset))
    print("evaluation_notes", len(eval_notes))
    print("adjudication_notes", len(adjud_notes))
    if summary.get("suspected_hard_cap_2000"):
        print("WARNING: suspected hard 2000-char note cap detected in selected note text column.")
    print("output_dir", args.output_dir)
    print("summary", summary_path)


if __name__ == "__main__":
    main()
