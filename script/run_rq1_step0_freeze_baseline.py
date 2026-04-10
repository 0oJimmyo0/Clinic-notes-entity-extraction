#!/usr/bin/env python3
"""
RQ1 Step 0: Freeze baseline outputs and create reproducible error-bucket sample.

Creates:
- snapshot copies of summary/pairs
- unmatched drug sample for manual review
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Dict, List, Tuple


def _parse_list(x: str) -> List[str]:
    if not x:
        return []
    try:
        v = json.loads(x)
        if isinstance(v, list):
            return [str(t).strip().lower() for t in v if str(t).strip()]
    except Exception:
        pass
    return []


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        return list(csv.DictReader(f))


def _resolve_method_files(root: Path, method_label: str, summary_csv: str, pairs_csv: str) -> Tuple[Path, Path, str]:
    if summary_csv and pairs_csv:
        return (root / summary_csv).resolve(), (root / pairs_csv).resolve(), method_label if method_label != "auto" else "custom"

    rq1_dir = root / "episode_extraction_results" / "rq1"
    candidates = {
        "baseline": (
            rq1_dir / "rq1_similarity_summary_baseline.csv",
            rq1_dir / "rq1_similarity_pairs_baseline.csv",
        ),
        "path_a": (
            rq1_dir / "rq1_similarity_summary_patha.csv",
            rq1_dir / "rq1_similarity_pairs_patha.csv",
        ),
        "path_ab": (
            rq1_dir / "rq1_similarity_summary_pathab.csv",
            rq1_dir / "rq1_similarity_pairs_pathab.csv",
        ),
        "raw": (
            rq1_dir / "rq1_similarity_summary.csv",
            rq1_dir / "rq1_similarity_pairs.csv",
        ),
    }
    order = ["baseline", "path_a", "path_ab", "raw"] if method_label == "auto" else [method_label]
    for m in order:
        s, p = candidates[m]
        if s.exists() and p.exists():
            return s.resolve(), p.resolve(), m
    tried = [str(candidates[m][0]) + " + " + str(candidates[m][1]) for m in order]
    raise FileNotFoundError(
        "Cannot resolve summary/pairs inputs. "
        f"Checked: {tried}. "
        "Provide --summary-csv and --pairs-csv explicitly if needed."
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Freeze RQ1 baseline and build error bucket.")
    p.add_argument(
        "--method-label",
        choices=["auto", "baseline", "path_a", "path_ab", "raw"],
        default="auto",
        help="Which method run to freeze. 'auto' picks the first available in baseline/path_a/path_ab/raw.",
    )
    p.add_argument(
        "--summary-csv",
        default="",
        help="Optional explicit summary CSV from Step 4.",
    )
    p.add_argument(
        "--pairs-csv",
        default="",
        help="Optional explicit pairs CSV from Step 4.",
    )
    p.add_argument(
        "--note-csv",
        default="episode_extraction_results/rq1/rq1_note_entities_by_visit.csv",
        help="Step 2 note entities by visit CSV.",
    )
    p.add_argument(
        "--ehr-csv",
        default="episode_extraction_results/rq1/rq1_ehr_entities_by_visit.csv",
        help="Step 3 structured entities by visit CSV.",
    )
    p.add_argument(
        "--snapshot-dir",
        default="episode_extraction_results/rq1/snapshots/rq1_baseline_snapshot",
        help="Output directory for baseline snapshot files.",
    )
    p.add_argument(
        "--error-bucket-csv",
        default="episode_extraction_results/rq1/diagnostics/rq1_error_bucket_drugs_unmatched.csv",
        help="Output CSV path for unmatched drug pair sample.",
    )
    p.add_argument(
        "--error-bucket-n",
        type=int,
        default=500,
        help="Maximum number of unmatched drug rows to export.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    summary_path, pairs_path, resolved_method = _resolve_method_files(
        root=root,
        method_label=args.method_label,
        summary_csv=args.summary_csv,
        pairs_csv=args.pairs_csv,
    )
    note_path = (root / args.note_csv).resolve()
    ehr_path = (root / args.ehr_csv).resolve()
    snap_dir = (root / args.snapshot_dir).resolve()
    error_out = (root / args.error_bucket_csv).resolve()

    for p in [summary_path, pairs_path, note_path, ehr_path]:
        if not p.exists():
            raise FileNotFoundError(f"Missing required input: {p}")

    snap_dir.mkdir(parents=True, exist_ok=True)
    (snap_dir / f"rq1_similarity_summary.{resolved_method}.csv").write_text(
        summary_path.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    (snap_dir / f"rq1_similarity_pairs.{resolved_method}.csv").write_text(
        pairs_path.read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    note_rows = _read_csv_rows(note_path)
    ehr_rows = _read_csv_rows(ehr_path)
    pair_rows = _read_csv_rows(pairs_path)

    note_map: Dict[Tuple[str, str], Dict[str, str]] = {(r["person_id"], r["visit_id"]): r for r in note_rows}
    ehr_map: Dict[Tuple[str, str], Dict[str, str]] = {(r["person_id"], r["visit_id"]): r for r in ehr_rows}

    out_rows: List[Dict[str, str]] = []
    for r in pair_rows:
        if str(r.get("window_k", "")) != "0":
            continue
        if float(r.get("drugs_note_n", "0") or 0) <= 0:
            continue
        if int(float(r.get("drugs_has_overlap_relaxed", "0") or 0)) == 1:
            continue
        key = (r["person_id"], r["visit_id"])
        note = note_map.get(key, {})
        ehr = ehr_map.get(key, {})
        out_rows.append(
            {
                "method_label": resolved_method,
                "person_id": r["person_id"],
                "visit_id": r["visit_id"],
                "window_k": r.get("window_k", ""),
                "drugs_note_n": r.get("drugs_note_n", ""),
                "drugs_ehr_n": r.get("drugs_ehr_n", ""),
                "drugs_containment_relaxed": r.get("drugs_containment_relaxed", ""),
                "note_drugs_json": json.dumps(_parse_list(note.get("drugs", "")), ensure_ascii=False),
                "ehr_drugs_json": json.dumps(_parse_list(ehr.get("drugs", "")), ensure_ascii=False),
            }
        )

    out_rows = out_rows[: max(args.error_bucket_n, 0)]
    error_out.parent.mkdir(parents=True, exist_ok=True)
    with error_out.open("w", encoding="utf-8", newline="") as f:
        if out_rows:
            w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
            w.writeheader()
            w.writerows(out_rows)
        else:
            w = csv.writer(f)
            w.writerow(
                [
                    "person_id",
                    "method_label",
                    "visit_id",
                    "window_k",
                    "drugs_note_n",
                    "drugs_ehr_n",
                    "drugs_containment_relaxed",
                    "note_drugs_json",
                    "ehr_drugs_json",
                ]
            )

    print(f"Saved snapshot dir: {snap_dir}")
    print(f"Resolved method: {resolved_method}")
    print(f"Using summary: {summary_path}")
    print(f"Using pairs:   {pairs_path}")
    print(f"Saved error bucket: {error_out} ({len(out_rows):,} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

