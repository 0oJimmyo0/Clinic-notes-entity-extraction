#!/usr/bin/env python3
"""
Legacy downstream concordance utility: compute note-vs-structured similarity metrics by visit.

Inputs:
- note visit-level file from Step 2:
    episode_extraction_results/rq1_note_entities_by_visit.csv
- structured visit-level file from Step 3:
    episode_extraction_results/rq1_ehr_entities_by_visit.csv

Optional:
- visit timeline file with columns:
    person_id, visit_id, visit_start_date
  to support k-window expansion (k=1,2,3).

Outputs:
- rq1_similarity_summary.csv       (table-ready domain/window metrics)
- rq1_similarity_pairs.csv         (visit-level pair metrics, for plotting/debug)
"""

from __future__ import annotations

import argparse
import ast
import json
import re
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

import pandas as pd
from rq1_drug_linking import (
    PathBConfig,
    build_canonical_drug_universe,
    canonicalize_drug,
    load_abbreviation_map,
    load_alias_map,
    load_calibration_config,
    resolve_note_drugs_hybrid,
    summarize_link_diagnostics,
)


DOMAINS = ["conditions", "drugs", "measurements", "procedures"]


def normalize_id(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.replace({"": None, "nan": None, "none": None, "None": None})
    return s


def parse_list_cell(x) -> List[str]:
    if x is None:
        return []
    if isinstance(x, list):
        vals = x
    else:
        s = str(x).strip()
        if s == "" or s.lower() in {"none", "nan"}:
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
    return sorted(set(out))


GENERIC_NOISE_TOKENS = {
    "diagnosis",
    "history",
    "present",
    "illness",
    "surgeon",
    "allergies",
    "problem",
    "medications",
    "medication",
    "note",
    "other",
    "status",
    "patient",
    "location",
    "agreement",
    "assessment",
    "pain",
}

MEAS_PROC_EXACT_NOISE = {
    "diagnosis",
    "history",
    "history of present illness",
    "problem list",
    "medications",
    "medication management",
    "pain assessment",
    "assessment",
    "allergies",
    "location",
    "status",
    "patient",
    "other",
}

MEAS_PROC_CONTAINS_PATTERNS = [
    r"\bhistory of present illness\b",
    r"\bproblem list\b",
    r"\bmedication management\b",
    r"\bpain assessment\b",
    r"\breview of systems\b",
    r"\bchief complaint\b",
]


def normalize_term(term: str, domain: str) -> str:
    t = str(term).strip().lower()
    if not t:
        return ""

    # Strip parenthetical text and normalize separators.
    t = re.sub(r"\([^)]*\)", " ", t)
    t = t.replace("/", " ").replace("_", " ").replace("-", " ")

    # Domain-aware cleanup.
    if domain == "drugs":
        # Remove dosage/strength and route/form noise.
        t = re.sub(r"\b\d+(\.\d+)?\s*(mg|mcg|g|ml|l|unit|units|%)\b", " ", t)
        t = re.sub(r"\b(po|iv|im|sc|subq|subcutaneous|intravenous|oral|topical|pf)\b", " ", t)
        t = re.sub(
            r"\b(tablet|tabs?|capsule|caps|solution|syrup|injection|injectable|ointment|spray|suspension|patch|cream|drop|drops)\b",
            " ",
            t,
        )
        # Remove common billing/code-like tokens and institutional artifacts.
        t = re.sub(r"\bj\d{4,6}\b", " ", t)  # HCPCS-like J-codes
        t = re.sub(r"\bndc[:\s-]*\d+\b", " ", t)
        t = re.sub(r"\b(builder|carrier fluid|irrigation|vumc|o r)\b", " ", t)

    # Remove punctuation except alnum+space.
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return ""

    # Drop code-like standalone entries from structured side patterns.
    # ICD-ish: c06.0 -> c06 0 after punctuation cleanup; J-codes: j0690.
    if re.fullmatch(r"[a-z]\d{2,6}", t):
        return ""
    if re.fullmatch(r"\d{3,8}", t):
        return ""

    # Remove pure generic noise tokens.
    if t in GENERIC_NOISE_TOKENS:
        return ""

    # Stronger note-side denoising for measurements/procedures.
    if domain in {"measurements", "procedures"}:
        if t in MEAS_PROC_EXACT_NOISE:
            return ""
        for pat in MEAS_PROC_CONTAINS_PATTERNS:
            if re.search(pat, t):
                return ""

    # Additional drug cleanup: keep core lexical chunk (often ingredient/brand).
    if domain == "drugs":
        toks = [x for x in t.split() if len(x) >= 3 and x not in GENERIC_NOISE_TOKENS]
        if not toks:
            return ""
        # Keep up to first 3 informative tokens to avoid long route/form strings.
        t = " ".join(toks[:3])

    return t


def preprocess_terms(terms: Sequence[str], domain: str) -> List[str]:
    out = []
    for x in terms:
        t = normalize_term(x, domain)
        if t:
            out.append(t)
    return sorted(set(out))


def relaxed_match(a: str, b: str) -> bool:
    if not a or not b:
        return False
    if a == b:
        return True
    if len(a) >= 4 and a in b:
        return True
    if len(b) >= 4 and b in a:
        return True
    ta = [t for t in a.split() if len(t) >= 3 and t not in GENERIC_NOISE_TOKENS]
    tb = [t for t in b.split() if len(t) >= 3 and t not in GENERIC_NOISE_TOKENS]
    if not ta or not tb:
        return False
    inter = set(ta) & set(tb)
    if not inter:
        return False
    return (len(inter) / min(len(set(ta)), len(set(tb)))) >= 0.5


def containment_note_in_ehr_relaxed(note_terms: Sequence[str], ehr_terms: Sequence[str]) -> Tuple[float, int]:
    sn, se = list(note_terms), list(ehr_terms)
    if not sn:
        return 1.0, 0
    if not se:
        return 0.0, 0
    matched = 0
    for n in sn:
        if any(relaxed_match(n, e) for e in se):
            matched += 1
    return matched / len(sn), int(matched > 0)


def jaccard(a: Sequence[str], b: Sequence[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def containment_note_in_ehr(note_terms: Sequence[str], ehr_terms: Sequence[str]) -> float:
    sn, se = set(note_terms), set(ehr_terms)
    if not sn:
        return 1.0
    if not se:
        return 0.0
    return len(sn & se) / len(sn)


def build_windowed_ehr(
    ehr_df: pd.DataFrame,
    timeline_df: pd.DataFrame,
    k: int,
) -> pd.DataFrame:
    """
    Build per-visit EHR entity unions across +/-k visits for each person.
    """
    if k == 0:
        return ehr_df.copy()

    t = timeline_df.copy()
    t["person_id"] = normalize_id(t["person_id"])
    t["visit_id"] = normalize_id(t["visit_id"])
    t["visit_start_date"] = pd.to_datetime(t["visit_start_date"], errors="coerce")
    t = t.dropna(subset=["person_id", "visit_id", "visit_start_date"])
    t = t.sort_values(["person_id", "visit_start_date", "visit_id"]).reset_index(drop=True)
    t["visit_rank"] = t.groupby("person_id").cumcount()

    e = ehr_df.copy()
    e["person_id"] = normalize_id(e["person_id"])
    e["visit_id"] = normalize_id(e["visit_id"])
    e = e.merge(t[["person_id", "visit_id", "visit_rank"]], on=["person_id", "visit_id"], how="left")
    e = e.dropna(subset=["visit_rank"]).copy()
    e["visit_rank"] = e["visit_rank"].astype(int)

    # map person -> rank -> row index
    by_person = {}
    for idx, row in e.reset_index().iterrows():
        by_person.setdefault(row["person_id"], {})[row["visit_rank"]] = int(row["index"])

    out_rows = []
    for _, row in e.iterrows():
        pid = row["person_id"]
        r = int(row["visit_rank"])
        ranks = range(r - k, r + k + 1)

        merged_domains = {d: set() for d in DOMAINS}
        rank_map = by_person.get(pid, {})
        for rr in ranks:
            idx = rank_map.get(rr)
            if idx is None:
                continue
            src = e.loc[idx]
            for d in DOMAINS:
                merged_domains[d].update(src[d])

        out_rows.append(
            {
                "person_id": pid,
                "visit_id": row["visit_id"],
                **{d: sorted(merged_domains[d]) for d in DOMAINS},
            }
        )

    return pd.DataFrame(out_rows)


def infer_timeline_from_visit_id(note_df: pd.DataFrame, ehr_df: pd.DataFrame) -> pd.DataFrame:
    """
    Fallback timeline when explicit visit_start_date is unavailable.
    Ranks visits by numeric visit_id (or lexical visit_id if non-numeric) within person.
    """
    keys = pd.concat(
        [
            note_df[["person_id", "visit_id"]].copy(),
            ehr_df[["person_id", "visit_id"]].copy(),
        ],
        ignore_index=True,
    ).drop_duplicates()
    keys["visit_id_num"] = pd.to_numeric(keys["visit_id"], errors="coerce")
    # Numeric first (when parseable), lexical tie-breaker for deterministic ordering.
    keys = keys.sort_values(
        ["person_id", "visit_id_num", "visit_id"],
        na_position="last",
    ).reset_index(drop=True)
    # Create pseudo date using rank so existing window logic can be reused.
    keys["visit_rank"] = keys.groupby("person_id").cumcount()
    keys["visit_start_date"] = pd.Timestamp("2000-01-01") + pd.to_timedelta(keys["visit_rank"], unit="D")
    return keys[["person_id", "visit_id", "visit_start_date"]].copy()


def compute_metrics(
    note_df: pd.DataFrame,
    ehr_df: pd.DataFrame,
    window_k: int,
    method_label: str,
    drug_normalizer: str,
    drug_linker: str,
    drug_linker_threshold: float,
    drug_alias_map: Dict[str, str],
    drug_candidate_universe,
    pathb_config,
) -> Tuple[pd.DataFrame, pd.DataFrame, List[dict]]:
    merged = note_df.merge(ehr_df, on=["person_id", "visit_id"], how="inner", suffixes=("_note", "_ehr"))

    pair_rows = []
    link_diag_rows: List[dict] = []
    for _, row in merged.iterrows():
        base = {
            "person_id": row["person_id"],
            "visit_id": row["visit_id"],
            "window_k": window_k,
            "method_label": method_label,
        }
        for d in DOMAINS:
            n_raw = row[f"{d}_note"]
            e_raw = row[f"{d}_ehr"]
            n = preprocess_terms(n_raw, d)
            e = preprocess_terms(e_raw, d)
            if d == "drugs" and drug_normalizer == "v2":
                n = sorted({canonicalize_drug(x, drug_alias_map) for x in n if canonicalize_drug(x, drug_alias_map)})
                e = sorted({canonicalize_drug(x, drug_alias_map) for x in e if canonicalize_drug(x, drug_alias_map)})
                if drug_linker == "embedding_cpu":
                    n, diag = resolve_note_drugs_hybrid(
                        note_terms=n,
                        ehr_terms=e,
                        alias_map=drug_alias_map,
                        use_embedding=True,
                        threshold=drug_linker_threshold,
                        pathb_mode="embedding_cpu",
                    )
                    link_diag_rows.append(
                        {
                            "person_id": row["person_id"],
                            "visit_id": row["visit_id"],
                            "window_k": window_k,
                            "method_label": method_label,
                            "domain": "drugs",
                            "diagnostics_json": json.dumps(diag, ensure_ascii=False),
                            **summarize_link_diagnostics([diag]),
                        }
                    )
                elif drug_linker == "canonical_transparent":
                    n, diag = resolve_note_drugs_hybrid(
                        note_terms=n,
                        ehr_terms=e,
                        alias_map=drug_alias_map,
                        use_embedding=False,
                        threshold=drug_linker_threshold,
                        pathb_mode="canonical_transparent",
                        candidate_universe=drug_candidate_universe,
                        pathb_config=pathb_config,
                    )
                    link_diag_rows.append(
                        {
                            "person_id": row["person_id"],
                            "visit_id": row["visit_id"],
                            "window_k": window_k,
                            "method_label": method_label,
                            "domain": "drugs",
                            "diagnostics_json": json.dumps(diag, ensure_ascii=False),
                            **summarize_link_diagnostics([diag]),
                        }
                    )
            jac = jaccard(n, e)
            cont = containment_note_in_ehr(n, e)
            cont_relaxed, has_relaxed = containment_note_in_ehr_relaxed(n, e)
            base[f"{d}_jaccard"] = jac
            base[f"{d}_containment"] = cont
            base[f"{d}_containment_relaxed"] = cont_relaxed
            base[f"{d}_note_n"] = len(n)
            base[f"{d}_ehr_n"] = len(e)
            base[f"{d}_has_overlap"] = int(len(set(n) & set(e)) > 0)
            base[f"{d}_has_overlap_relaxed"] = has_relaxed
        pair_rows.append(base)

    pair_df = pd.DataFrame(pair_rows)

    summary_rows = []
    for d in DOMAINS:
        sub = pair_df
        summary_rows.append(
            {
                "window_k": window_k,
                "domain": d,
                "method_label": method_label,
                "n_pairs": int(len(sub)),
                "mean_jaccard": float(sub[f"{d}_jaccard"].mean()) if len(sub) else 0.0,
                "mean_containment_note_in_ehr": float(sub[f"{d}_containment"].mean()) if len(sub) else 0.0,
                "mean_containment_note_in_ehr_relaxed": float(sub[f"{d}_containment_relaxed"].mean()) if len(sub) else 0.0,
                "overlap_rate": float(sub[f"{d}_has_overlap"].mean()) if len(sub) else 0.0,
                "overlap_rate_relaxed": float(sub[f"{d}_has_overlap_relaxed"].mean()) if len(sub) else 0.0,
                "mean_note_terms": float(sub[f"{d}_note_n"].mean()) if len(sub) else 0.0,
                "mean_ehr_terms": float(sub[f"{d}_ehr_n"].mean()) if len(sub) else 0.0,
            }
        )
    return pd.DataFrame(summary_rows), pair_df, link_diag_rows


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RQ1 Step 4 similarity metrics")
    p.add_argument(
        "--note-csv",
        default="episode_extraction_results/rq1/rq1_note_entities_by_visit.csv",
        help="Step 2 note visit-level entities CSV",
    )
    p.add_argument(
        "--ehr-csv",
        default="episode_extraction_results/rq1/rq1_ehr_entities_by_visit.csv",
        help="Step 3 structured EHR visit-level entities CSV",
    )
    p.add_argument(
        "--timeline-csv",
        default="episode_extraction_results/rq1/rq1_visit_timeline.csv",
        help="Optional visit timeline CSV with person_id, visit_id, visit_start_date",
    )
    p.add_argument(
        "--timeline-fallback",
        choices=["error", "infer_visit_id"],
        default="error",
        help=(
            "Behavior when k>0 and --timeline-csv is unavailable. "
            "'error' stops; 'infer_visit_id' builds a pseudo timeline by visit_id order."
        ),
    )
    p.add_argument(
        "--windows",
        default="0",
        help='Comma-separated window sizes, e.g. "0" or "0,1,2,3"',
    )
    p.add_argument(
        "--output-summary-csv",
        default="episode_extraction_results/rq1/rq1_similarity_summary.csv",
        help="Summary metrics output CSV",
    )
    p.add_argument(
        "--output-pairs-csv",
        default="episode_extraction_results/rq1/rq1_similarity_pairs.csv",
        help="Visit-level metrics output CSV",
    )
    p.add_argument(
        "--method-label",
        default="baseline",
        help="Method label saved to outputs (e.g., baseline, path_a, path_ab).",
    )
    p.add_argument(
        "--drug-normalizer",
        choices=["baseline", "v2"],
        default="baseline",
        help="Drug normalization strategy.",
    )
    p.add_argument(
        "--drug-linker",
        choices=["none", "embedding_cpu", "canonical_transparent"],
        default="none",
        help="Optional second-stage linker for unresolved drug mentions.",
    )
    p.add_argument(
        "--drug-linker-threshold",
        type=float,
        default=0.85,
        help="Legacy threshold for embedding_cpu mode (and fallback min-score seed).",
    )
    p.add_argument(
        "--drug-canonical-vocab-path",
        default="",
        help=(
            "Optional canonical candidate vocabulary file (CSV/JSON). "
            "Expected fields include canonical_label and optional synonyms/aliases."
        ),
    )
    p.add_argument(
        "--drug-adjudicated-labels-csv",
        default="",
        help="Optional adjudicated note reference CSV for canonical labels.",
    )
    p.add_argument(
        "--drug-linker-top-k",
        type=int,
        default=20,
        help="Top-k candidates retrieved/scored per unresolved mention in canonical_transparent mode.",
    )
    p.add_argument(
        "--drug-linker-min-score",
        type=float,
        default=0.45,
        help="Minimum raw feature score to accept a Path B link.",
    )
    p.add_argument(
        "--drug-linker-min-margin",
        type=float,
        default=0.05,
        help="Minimum top1-top2 score margin to accept a Path B link.",
    )
    p.add_argument(
        "--drug-linker-min-mention-len",
        type=int,
        default=4,
        help="Reject Path B linking when normalized mention length is below this threshold.",
    )
    p.add_argument(
        "--drug-linker-min-score-short-mention",
        type=float,
        default=0.80,
        help="Higher minimum score for short/ambiguous mentions.",
    )
    p.add_argument(
        "--drug-linker-min-calibrated-confidence",
        type=float,
        default=0.90,
        help="Accept only if calibrated confidence meets this target precision.",
    )
    p.add_argument(
        "--drug-linker-calibration-json",
        default="",
        help="Optional calibration JSON (identity/platt/isotonic_bins).",
    )
    p.add_argument(
        "--drug-abbreviation-json",
        default="",
        help="Optional abbreviation expansion JSON (e.g., tax -> paclitaxel).",
    )
    p.add_argument(
        "--drug-aliases-json",
        default="resources/lexicons/rq1_drug_aliases.csv",
        help="Alias artifact used by deterministic drug normalization. Supports CSV or JSON.",
    )
    p.add_argument(
        "--output-link-diagnostics-csv",
        default="episode_extraction_results/rq1/diagnostics/rq1_drug_link_diagnostics.csv",
        help="Optional diagnostics CSV for drug linker decisions.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]

    note_path = (root / args.note_csv).resolve()
    ehr_path = (root / args.ehr_csv).resolve()
    timeline_path = (root / args.timeline_csv).resolve() if args.timeline_csv else None
    out_summary = (root / args.output_summary_csv).resolve()
    out_pairs = (root / args.output_pairs_csv).resolve()
    alias_path = (root / args.drug_aliases_json).resolve()

    if not note_path.exists():
        raise FileNotFoundError(f"Note file not found: {note_path}")
    if not ehr_path.exists():
        raise FileNotFoundError(f"EHR file not found: {ehr_path}")

    windows = sorted({int(x.strip()) for x in args.windows.split(",") if x.strip() != ""})
    if any(k < 0 for k in windows):
        raise ValueError("window sizes must be >= 0")

    note = pd.read_csv(note_path)
    ehr = pd.read_csv(ehr_path)

    for df in (note, ehr):
        if "person_id" not in df.columns or "visit_id" not in df.columns:
            raise ValueError("Both note/ehr files must have person_id and visit_id")
        df["person_id"] = normalize_id(df["person_id"])
        df["visit_id"] = normalize_id(df["visit_id"])

    for d in DOMAINS:
        if d not in note.columns:
            note[d] = "[]"
        if d not in ehr.columns:
            ehr[d] = "[]"
        note[d] = note[d].apply(parse_list_cell)
        ehr[d] = ehr[d].apply(parse_list_cell)

    timeline_df = None
    if max(windows) > 0:
        if timeline_path is not None and timeline_path.exists():
            timeline_df = pd.read_csv(timeline_path)
            need_cols = {"person_id", "visit_id", "visit_start_date"}
            if not need_cols.issubset(set(timeline_df.columns)):
                raise ValueError(f"timeline-csv must include {sorted(need_cols)}")
            print(f"Using explicit timeline: {timeline_path}")
        elif args.timeline_fallback == "infer_visit_id":
            timeline_df = infer_timeline_from_visit_id(note_df=note, ehr_df=ehr)
            print(
                "Timeline missing; using fallback infer_visit_id "
                "(visit_id ordering within person)."
            )
        else:
            raise FileNotFoundError(
                "k>0 requested but timeline file missing. Provide --timeline-csv with "
                "person_id, visit_id, visit_start_date or set --timeline-fallback infer_visit_id."
            )

    all_summary = []
    all_pairs = []
    all_link_diags: List[dict] = []
    drug_alias_map = load_alias_map(alias_path) if args.drug_normalizer == "v2" else {}

    drug_candidate_universe = None
    pathb_config = None
    if args.drug_linker == "canonical_transparent":
        vocab_path = (root / args.drug_canonical_vocab_path).resolve() if args.drug_canonical_vocab_path else None
        adjud_path = (
            (root / args.drug_adjudicated_labels_csv).resolve() if args.drug_adjudicated_labels_csv else None
        )
        calib_path = (root / args.drug_linker_calibration_json).resolve() if args.drug_linker_calibration_json else None
        abbr_path = (root / args.drug_abbreviation_json).resolve() if args.drug_abbreviation_json else None

        calibration_cfg = load_calibration_config(calib_path)
        abbreviation_map = load_abbreviation_map(abbr_path)
        pathb_config = PathBConfig(
            top_k=max(int(args.drug_linker_top_k), 1),
            min_score=float(args.drug_linker_min_score),
            min_margin=float(args.drug_linker_min_margin),
            min_mention_len=max(int(args.drug_linker_min_mention_len), 1),
            min_score_short_mention=float(args.drug_linker_min_score_short_mention),
            min_calibrated_confidence=float(args.drug_linker_min_calibrated_confidence),
            calibration=calibration_cfg,
            abbreviation_map=abbreviation_map,
        )
        drug_candidate_universe = build_canonical_drug_universe(
            alias_map=drug_alias_map,
            canonical_vocab_path=vocab_path,
            adjudicated_labels_path=adjud_path,
        )
        print(
            "Path B canonical universe loaded: "
            f"{len(drug_candidate_universe.candidates):,} candidates"
        )

    for k in windows:
        if k == 0:
            ehr_k = ehr.copy()
        else:
            ehr_k = build_windowed_ehr(ehr_df=ehr, timeline_df=timeline_df, k=k)
            # ensure parsed list type
            for d in DOMAINS:
                if d not in ehr_k.columns:
                    ehr_k[d] = [[] for _ in range(len(ehr_k))]
                else:
                    ehr_k[d] = ehr_k[d].apply(lambda x: x if isinstance(x, list) else parse_list_cell(x))

        summary_k, pairs_k, diags_k = compute_metrics(
            note_df=note,
            ehr_df=ehr_k,
            window_k=k,
            method_label=args.method_label,
            drug_normalizer=args.drug_normalizer,
            drug_linker=args.drug_linker,
            drug_linker_threshold=args.drug_linker_threshold,
            drug_alias_map=drug_alias_map,
            drug_candidate_universe=drug_candidate_universe,
            pathb_config=pathb_config,
        )
        all_summary.append(summary_k)
        all_pairs.append(pairs_k)
        all_link_diags.extend(diags_k)
        print(f"window k={k}: pair rows={len(pairs_k):,}")

    summary = pd.concat(all_summary, ignore_index=True) if all_summary else pd.DataFrame()
    pairs = pd.concat(all_pairs, ignore_index=True) if all_pairs else pd.DataFrame()

    out_summary.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(out_summary, index=False)
    pairs.to_csv(out_pairs, index=False)
    if args.output_link_diagnostics_csv:
        out_diag = (root / args.output_link_diagnostics_csv).resolve()
        out_diag.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(all_link_diags).to_csv(out_diag, index=False)
        print(f"Saved link diagnostics: {out_diag}")

    print(f"Saved summary: {out_summary}")
    print(f"Saved pairs:   {out_pairs}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
