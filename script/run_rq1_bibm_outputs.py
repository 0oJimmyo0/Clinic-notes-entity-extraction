#!/usr/bin/env python3
"""
Consolidate BIBM-facing tables and figures from existing and new RQ1 analyses.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Sequence

import pandas as pd

from rq1_adjudication_utils import write_run_summary


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build consolidated BIBM paper outputs.")
    p.add_argument(
        "--patha-paper-outputs-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_patha",
    )
    p.add_argument(
        "--reference-audit-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/reference_audit",
    )
    p.add_argument(
        "--note-only-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/note_only_evidence",
    )
    p.add_argument(
        "--semantic-concordance-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/semantic_concordance",
    )
    p.add_argument(
        "--temporal-mismatch-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/temporal_mismatch_ladder",
    )
    p.add_argument(
        "--random-audit-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/reference_random_audit_results",
    )
    p.add_argument(
        "--external-coverage-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/external_coverage_sanity_check",
    )
    p.add_argument(
        "--patha-v1-v2-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/patha_v1_v2_sensitivity",
    )
    p.add_argument(
        "--output-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_bibm",
    )
    return p.parse_args()


def _to_markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows._\n"
    try:
        return df.to_markdown(index=False) + "\n"
    except Exception:
        cols = list(df.columns)
        header = "| " + " | ".join(cols) + " |"
        sep = "| " + " | ".join(["---"] * len(cols)) + " |"
        body = ["| " + " | ".join(str(row[c]) for c in cols) + " |" for _, row in df.iterrows()]
        return "\n".join([header, sep, *body]) + "\n"


def _write_table(df: pd.DataFrame, out_dir: Path, stem: str) -> None:
    df.to_csv(out_dir / f"{stem}.csv", index=False)
    (out_dir / f"{stem}.md").write_text(_to_markdown_table(df), encoding="utf-8")


def _load_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path).fillna("")


def _svg_text(x: float, y: float, text: str, size: int = 12, weight: str = "normal") -> str:
    safe = str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
    return f'<text x="{x:.1f}" y="{y:.1f}" font-family="Arial, Helvetica, sans-serif" font-size="{size}" font-weight="{weight}">{safe}</text>'


def _write_bar_svg(labels: Sequence[str], values: Sequence[float], title: str, out_path: Path, value_fmt: str) -> None:
    width = 920
    row_h = 34
    top = 68
    left_label = 30
    left_bar = 320
    bar_w = 520
    height = top + row_h * len(labels) + 30
    vmax = max(max(values) if values else 1.0, 1e-9)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect x="0" y="0" width="100%" height="100%" fill="white"/>',
        _svg_text(24, 36, title, size=18, weight="bold"),
    ]
    for i, (label, value) in enumerate(zip(labels, values)):
        y = top + i * row_h
        w = bar_w * (float(value) / vmax)
        parts.append(_svg_text(left_label, y + 21, label))
        parts.append(f'<rect x="{left_bar}" y="{y+6}" width="{bar_w}" height="16" fill="#eef2f7" rx="3" ry="3"/>')
        parts.append(f'<rect x="{left_bar}" y="{y+6}" width="{w:.1f}" height="16" fill="#2a6fbb" rx="3" ry="3"/>')
        parts.append(_svg_text(left_bar + bar_w + 12, y + 20, value_fmt.format(value), size=11))
    parts.append("</svg>")
    out_path.write_text("\n".join(parts), encoding="utf-8")


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]

    patha_dir = (root / args.patha_paper_outputs_dir).resolve()
    audit_dir = (root / args.reference_audit_dir).resolve()
    note_only_dir = (root / args.note_only_dir).resolve()
    concord_dir = (root / args.semantic_concordance_dir).resolve()
    temporal_dir = (root / args.temporal_mismatch_dir).resolve()
    random_audit_dir = (root / args.random_audit_dir).resolve()
    ext_dir = (root / args.external_coverage_dir).resolve()
    patha_v1_v2_dir = (root / args.patha_v1_v2_dir).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    cohort = _load_csv(patha_dir / "rq1_table_cohort_adjudication_results_compact.csv")
    norm = _load_csv(patha_dir / "rq1_table_normalization_ladder_patha_focus.csv")
    residual = _load_csv(patha_dir / "rq1_table_patha_failure_taxonomy.csv")
    audit_summary = _load_csv(audit_dir / "rq1_reference_audit_summary.csv")
    audit_error = _load_csv(audit_dir / "rq1_reference_audit_error_taxonomy.csv")
    random_audit_summary = _load_csv(random_audit_dir / "rq1_reference_random_audit_summary.csv")
    random_audit_error = _load_csv(random_audit_dir / "rq1_reference_random_audit_error_taxonomy.csv")
    note_only_action = _load_csv(note_only_dir / "rq1_note_only_by_action.csv")
    note_only_note = _load_csv(note_only_dir / "rq1_note_only_by_note_type.csv")
    note_only_class = _load_csv(note_only_dir / "rq1_note_only_by_drug_class.csv")
    note_only_summary = _load_csv(note_only_dir / "rq1_note_only_evidence_summary.csv")
    note_only_ladder = _load_csv(note_only_dir / "rq1_note_only_semantic_mismatch_ladder_summary.csv")
    semantic = _load_csv(concord_dir / "rq1_semantic_concordance_summary.csv")
    temporal_collapsed = _load_csv(temporal_dir / "rq1_temporal_mismatch_ladder_collapsed_summary.csv")
    temporal_internal = _load_csv(temporal_dir / "rq1_temporal_mismatch_ladder_internal_summary.csv")
    external = _load_csv(ext_dir / "rq1_external_coverage_summary.csv")
    patha_v1_v2_compact = _load_csv(patha_v1_v2_dir / "rq1_patha_v1_v2_compact_summary.csv")
    patha_v1_v2_review = _load_csv(patha_v1_v2_dir / "rq1_patha_v2_review_resolution_compact.csv")

    if len(cohort) and len(audit_summary):
        cohort_audit = cohort.copy()
        for _, row in audit_summary.iterrows():
            cohort_audit = pd.concat(
                [
                    cohort_audit,
                    pd.DataFrame(
                        [{"item": row.get("item", ""), "value": row.get("value", ""), "notes": row.get("unit", "")}]
                    ),
                ],
                ignore_index=True,
            )
    else:
        cohort_audit = cohort

    tables = {
        "rq1_bibm_table_cohort_reference_audit_summary": cohort_audit,
        "rq1_bibm_table_llm_audit_error_taxonomy": audit_error,
        "rq1_bibm_table_normalization_ladder": norm,
        "rq1_bibm_table_residual_patha_taxonomy": residual,
        "rq1_bibm_table_semantic_concordance": semantic,
        "rq1_bibm_table_note_only_summary": note_only_summary,
        "rq1_bibm_table_note_only_semantic_mismatch_ladder": note_only_ladder,
        "rq1_bibm_table_temporal_mismatch_ladder": temporal_collapsed,
        "rq1_bibm_table_note_only_by_action": note_only_action,
        "rq1_bibm_table_note_only_by_note_type": note_only_note,
        "rq1_bibm_table_note_only_by_drug_class": note_only_class,
    }
    if len(random_audit_summary):
        tables["rq1_bibm_table_random_audit_summary"] = random_audit_summary
    if len(random_audit_error):
        tables["rq1_bibm_table_random_audit_error_taxonomy"] = random_audit_error
    if len(external):
        tables["rq1_bibm_table_external_coverage"] = external
    if len(patha_v1_v2_compact):
        tables["rq1_bibm_table_patha_v1_v2_compact"] = patha_v1_v2_compact
    if len(patha_v1_v2_review):
        tables["rq1_bibm_table_patha_v2_review_resolution"] = patha_v1_v2_review

    for stem, df in tables.items():
        if len(df):
            _write_table(df, out_dir, stem)

    if len(norm) and "stage" in norm.columns and "accuracy" in norm.columns:
        labels = norm["stage"].astype(str).tolist()
        values = pd.to_numeric(norm["accuracy"], errors="coerce").fillna(0.0).tolist()
        _write_bar_svg(labels, values, "Deterministic Normalization Ladder", out_dir / "rq1_bibm_fig_normalization_ladder.svg", "{:.3f}")

    if len(note_only_action) and "action_cue" in note_only_action.columns and "note_only_exact_rows" in note_only_action.columns:
        labels = note_only_action["action_cue"].astype(str).tolist()[:8]
        values = pd.to_numeric(note_only_action["note_only_exact_rows"], errors="coerce").fillna(0.0).tolist()[:8]
        _write_bar_svg(labels, values, "Note-Only Medication Evidence by Action Cue", out_dir / "rq1_bibm_fig_note_only_by_action.svg", "{:.0f}")

    if len(semantic) and "concordance_level" in semantic.columns and "mean_jaccard" in semantic.columns:
        labels = semantic["concordance_level"].astype(str).tolist()
        values = pd.to_numeric(semantic["mean_jaccard"], errors="coerce").fillna(0.0).tolist()
        _write_bar_svg(labels, values, "Semantic Note-to-EHR Concordance", out_dir / "rq1_bibm_fig_semantic_concordance.svg", "{:.3f}")

    if len(temporal_collapsed) and "collapsed_bucket" in temporal_collapsed.columns and "mention_rows" in temporal_collapsed.columns:
        labels = temporal_collapsed["collapsed_bucket"].astype(str).tolist()
        values = pd.to_numeric(temporal_collapsed["mention_rows"], errors="coerce").fillna(0.0).tolist()
        _write_bar_svg(labels, values, "Semantic + Temporal Mismatch Ladder", out_dir / "rq1_bibm_fig_temporal_mismatch_ladder.svg", "{:.0f}")

    workflow_mermaid = """flowchart LR
    A[Clinic-note cohort] --> B[Candidate extraction]
    B --> C[Note and mention packets]
    C --> D[LLM bootstrap labels]
    D --> E[Targeted human audit]
    E --> F[Random reliability audit]
    F --> G[Note-grounded reference set]
    G --> H[Deterministic normalization ladder]
    G --> I[Semantic + temporal mismatch ladder]
    G --> J[Semantic note-to-EHR concordance]
    H --> K[BIBM paper tables and figures]
    I --> K
    J --> K
"""
    (out_dir / "rq1_bibm_workflow_mermaid.md").write_text(workflow_mermaid, encoding="utf-8")

    outputs = sorted(str(p.name) for p in out_dir.iterdir() if p.is_file())
    write_run_summary(
        out_dir / "rq1_bibm_outputs_summary.json",
        {
            "inputs": {
                "patha_paper_outputs_dir": str(patha_dir),
                "reference_audit_dir": str(audit_dir),
                "note_only_dir": str(note_only_dir),
                "semantic_concordance_dir": str(concord_dir),
                "temporal_mismatch_dir": str(temporal_dir),
                "random_audit_dir": str(random_audit_dir),
                "external_coverage_dir": str(ext_dir),
                "patha_v1_v2_dir": str(patha_v1_v2_dir),
            },
            "outputs": outputs,
        },
    )

    print(f"Saved consolidated BIBM outputs to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
