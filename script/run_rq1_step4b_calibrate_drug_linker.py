#!/usr/bin/env python3
"""
RQ1 Step 4b: Calibrate drug linker threshold and guardrails.

Given multiple Step 4 outputs (baseline/path_a/path_ab), compute:
- delta on drug non-empty overlap / relaxed containment
- confidence diagnostics from linker scores
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from statistics import mean
from typing import Dict, List


def _read_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        return list(csv.DictReader(f))


def _drug_nonempty_metrics(pairs_rows: List[Dict[str, str]]) -> Dict[str, float]:
    k0 = [r for r in pairs_rows if str(r.get("window_k", "")) == "0"]
    sub = [r for r in k0 if float(r.get("drugs_note_n", "0") or 0) > 0]
    if not sub:
        return {
            "n_pairs_note_nonempty": 0.0,
            "containment_relaxed_pct": 0.0,
            "overlap_relaxed_pct": 0.0,
        }
    cont = [float(r.get("drugs_containment_relaxed", "0") or 0) for r in sub]
    ov = [float(r.get("drugs_has_overlap_relaxed", "0") or 0) for r in sub]
    return {
        "n_pairs_note_nonempty": float(len(sub)),
        "containment_relaxed_pct": 100.0 * mean(cont),
        "overlap_relaxed_pct": 100.0 * mean(ov),
    }


def _diag_guardrails(diag_rows: List[Dict[str, str]], low_conf_threshold: float) -> Dict[str, float]:
    if not diag_rows:
        return {"n_rows": 0.0, "accept_rate": 0.0, "low_conf_accept_rate": 0.0, "mean_accept_conf": 0.0}
    accepted_scores: List[float] = []
    for row in diag_rows:
        try:
            diag = json.loads(row.get("diagnostics_json", "{}"))
        except Exception:
            continue
        if not isinstance(diag, dict):
            continue
        for info in diag.values():
            if isinstance(info, dict) and bool(info.get("accepted")):
                try:
                    accepted_scores.append(
                        float(info.get("calibrated_confidence", info.get("score", 0.0)))
                    )
                except Exception:
                    pass
    if not accepted_scores:
        return {
            "n_rows": float(len(diag_rows)),
            "accept_rate": 0.0,
            "low_conf_accept_rate": 0.0,
            "mean_accept_conf": 0.0,
        }
    low = sum(1 for s in accepted_scores if s < low_conf_threshold)
    return {
        "n_rows": float(len(diag_rows)),
        "accept_rate": float(len(accepted_scores)) / max(len(diag_rows), 1),
        "low_conf_accept_rate": float(low) / max(len(accepted_scores), 1),
        "mean_accept_conf": float(mean(accepted_scores)),
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Calibrate RQ1 drug linker guardrails.")
    p.add_argument("--baseline-pairs-csv", required=True, help="Step4 baseline pairs csv.")
    p.add_argument("--patha-pairs-csv", required=True, help="Step4 path_a pairs csv.")
    p.add_argument("--pathab-pairs-csv", required=True, help="Step4 path_ab pairs csv.")
    p.add_argument(
        "--pathab-diag-csv",
        default="",
        help="Optional path_ab linker diagnostics CSV (from Step4 --output-link-diagnostics-csv).",
    )
    p.add_argument(
        "--low-confidence-threshold",
        type=float,
        default=0.90,
        help="Accepted links below this score are counted as low-confidence.",
    )
    p.add_argument(
        "--output-json",
        default="episode_extraction_results/rq1/diagnostics/rq1_drug_linker_calibration.json",
        help="Output calibration summary JSON.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    baseline_rows = _read_rows((root / args.baseline_pairs_csv).resolve())
    patha_rows = _read_rows((root / args.patha_pairs_csv).resolve())
    pathab_rows = _read_rows((root / args.pathab_pairs_csv).resolve())

    baseline = _drug_nonempty_metrics(baseline_rows)
    patha = _drug_nonempty_metrics(patha_rows)
    pathab = _drug_nonempty_metrics(pathab_rows)
    out = {
        "baseline": baseline,
        "path_a": patha,
        "path_ab": pathab,
        "delta_path_a_vs_baseline": {
            "containment_relaxed_pct": round(patha["containment_relaxed_pct"] - baseline["containment_relaxed_pct"], 4),
            "overlap_relaxed_pct": round(patha["overlap_relaxed_pct"] - baseline["overlap_relaxed_pct"], 4),
        },
        "delta_path_ab_vs_path_a": {
            "containment_relaxed_pct": round(pathab["containment_relaxed_pct"] - patha["containment_relaxed_pct"], 4),
            "overlap_relaxed_pct": round(pathab["overlap_relaxed_pct"] - patha["overlap_relaxed_pct"], 4),
        },
    }

    if args.pathab_diag_csv:
        diag_rows = _read_rows((root / args.pathab_diag_csv).resolve())
        out["guardrails"] = _diag_guardrails(diag_rows, low_conf_threshold=args.low_confidence_threshold)

    out_path = (root / args.output_json).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(f"Saved calibration summary: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

