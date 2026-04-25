#!/usr/bin/env python3
"""
Generate paper-ready enrichment tables and figures for the clinic_like_20k_30k RQ1 paper.

Focus:
- extraction robustness by slice
- polished workflow / normalization / note-density visuals
- frequent unresolved mentions after Path A
- audit/reference construction grounding
- deterministic stage-contribution summaries
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path
from typing import List, Sequence

import pandas as pd


ACCENT = "#1f5aa6"
ACCENT_2 = "#4c8eda"
ACCENT_3 = "#8fb7e8"
TEXT = "#1f2937"
MUTED = "#6b7280"
GRID = "#d7deea"
BG_BAR = "#eef2f7"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate enriched paper outputs for RQ1.")
    p.add_argument(
        "--slice-metrics-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/note_truth_eval/rq1_step4_note_truth_slice_metrics.csv",
    )
    p.add_argument(
        "--top-unresolved-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs/rq1_table_top_unresolved_after_patha.csv",
    )
    p.add_argument(
        "--note-density-conditioned-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_patha/rq1_table_note_med_density_conditioned_ge1.csv",
    )
    p.add_argument(
        "--normalization-ladder-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_patha/rq1_table_normalization_ladder_patha_focus.csv",
    )
    p.add_argument(
        "--cohort-grounding-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_patha/rq1_table_cohort_adjudication_results_compact.csv",
    )
    p.add_argument(
        "--note-density-stats-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_patha/rq1_table_note_med_density_stats.csv",
    )
    p.add_argument(
        "--failure-taxonomy-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_patha/rq1_table_patha_failure_taxonomy.csv",
    )
    p.add_argument(
        "--extraction-performance-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_patha/rq1_table_extraction_performance_mention_level.csv",
    )
    p.add_argument(
        "--normalization-eval-detailed-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/normalization_eval/rq1_normalization_eval_detailed.csv",
    )
    p.add_argument(
        "--adjudication-patch-summary-json",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/reviewed_adjudication_patch_summary_strict_final.json",
    )
    p.add_argument(
        "--output-dir",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_enriched",
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
        body = []
        for _, row in df.iterrows():
            body.append("| " + " | ".join(str(row[c]) for c in cols) + " |")
        return "\n".join([header, sep, *body]) + "\n"


def _write_table(df: pd.DataFrame, out_dir: Path, stem: str) -> None:
    df.to_csv(out_dir / f"{stem}.csv", index=False)
    (out_dir / f"{stem}.md").write_text(_to_markdown_table(df), encoding="utf-8")


def _svg_text(x: float, y: float, text: str, size: int = 12, weight: str = "normal", fill: str = TEXT) -> str:
    safe = (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    return (
        f'<text x="{x:.1f}" y="{y:.1f}" font-family="Arial, Helvetica, sans-serif" '
        f'font-size="{size}" font-weight="{weight}" fill="{fill}">{safe}</text>'
    )


def _write_svg(path: Path, parts: Sequence[str]) -> None:
    path.write_text("\n".join(parts), encoding="utf-8")


def _convert_svg_to_png(svg_path: Path) -> Path | None:
    png_path = svg_path.with_suffix(".png")
    try:
        subprocess.run(
            ["sips", "-s", "format", "png", str(svg_path), "--out", str(png_path)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        pass
    if png_path.exists():
        return png_path

    try:
        subprocess.run(
            ["qlmanage", "-t", "-s", "1600", "-o", str(svg_path.parent), str(svg_path)],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except Exception:
        return None
    ql_path = svg_path.parent / f"{svg_path.name}.png"
    if ql_path.exists():
        ql_path.replace(png_path)
    return png_path if png_path.exists() else None


def _normalize_note_length_label(value: str) -> str:
    mapping = {
        "lt_250": "<250",
        "250_749": "250-749",
        "750_1499": "750-1499",
        "ge_1500": ">=1500",
    }
    return mapping.get(value, value)


def _normalize_density_label(value: str) -> str:
    mapping = {
        "1": "1",
        "2_3": "2-3",
        "4_7": "4-7",
        "8_plus": "8+",
    }
    return mapping.get(value, value)


def _normalize_title_label(value: str) -> str:
    mapping = {
        "Assessment & Plan Note": "A&P Note",
        "Patient Instructions": "Pt Instructions",
        "Progress Notes": "Progress Notes",
        "H&P": "H&P",
        "Consults": "Consults",
    }
    return mapping.get(value, value)


def _build_slice_summary(slice_df: pd.DataFrame) -> pd.DataFrame:
    keep_titles = [
        "Progress Notes",
        "Assessment & Plan Note",
        "Patient Instructions",
        "H&P",
        "Consults",
    ]
    pieces: List[pd.DataFrame] = []

    cand = slice_df[slice_df["slice_name"] == "candidate_density_bin"].copy()
    cand = cand[cand["slice_value"].astype(str) != ""].copy()
    cand["slice_family"] = "Candidate density"
    cand["slice_value"] = cand["slice_value"].map(_normalize_density_label)
    pieces.append(cand)

    note_len = slice_df[slice_df["slice_name"] == "note_length_bin"].copy()
    note_len = note_len[note_len["slice_value"].astype(str) != ""].copy()
    note_len["slice_family"] = "Note length"
    note_len["slice_value"] = note_len["slice_value"].map(_normalize_note_length_label)
    pieces.append(note_len)

    titles = slice_df[slice_df["slice_name"] == "note_title_norm"].copy()
    titles = titles[titles["slice_value"].isin(keep_titles)].copy()
    titles["slice_family"] = "Note title"
    titles["slice_value"] = titles["slice_value"].map(_normalize_title_label)
    pieces.append(titles)

    out = pd.concat(pieces, ignore_index=True)
    out = out[["slice_family", "slice_value", "tp", "fp", "fn", "precision", "recall", "f1"]].copy()
    for c in ["precision", "recall", "f1"]:
        out[c] = out[c].map(lambda x: round(float(x), 4))
    return out


def _likely_follow_up(term: str) -> str:
    t = str(term).strip().lower()
    if re.search(r"(?:/|\+|\bwith\b|\band\b|\bplus\b|\bxr\b|\ber\b|\bir\b|\btablet\b|\bcapsule\b|\bpatch\b)", t):
        return "Formulation/combo review"
    if re.search(r"\b(calcium|sodium|glucose|iron|creatinine|collagen|lactate)\b", t):
        return "Substance/non-med review"
    return "Alias/vocabulary review"


def _build_top_unresolved(top_df: pd.DataFrame, n: int = 12) -> pd.DataFrame:
    out = top_df.head(n).copy()
    out["likely_follow_up"] = out["raw_mention_text"].map(_likely_follow_up)
    out = out.rename(columns={"raw_mention_text": "raw_mention", "count": "mention_count"})
    return out


def _lookup_compact_count(cohort_df: pd.DataFrame, item_name: str) -> int:
    row = cohort_df.loc[cohort_df["item"] == item_name, "value"]
    if row.empty:
        raise KeyError(f"Missing cohort item: {item_name}")
    return int(row.iloc[0])


def _lookup_metric(stats_df: pd.DataFrame, metric: str) -> float:
    row = stats_df.loc[stats_df["metric"] == metric, "value"]
    if row.empty:
        raise KeyError(f"Missing metric: {metric}")
    return float(row.iloc[0])


def _build_audit_reference_summary(
    cohort_df: pd.DataFrame, stats_df: pd.DataFrame, patch_summary: dict
) -> pd.DataFrame:
    counts = patch_summary["counts"]
    reference_rows = _lookup_compact_count(
        cohort_df, "Total adjudicated mention rows used in normalization"
    )
    audited_rows = int(counts["n_keep"])
    unaudited_rows = int(reference_rows - audited_rows)
    audit_pct = 100.0 * audited_rows / max(reference_rows, 1)

    rows = [
        ("Eligible visits", _lookup_compact_count(cohort_df, "Full eligible visits"), "visit"),
        ("Evaluation visits", _lookup_compact_count(cohort_df, "Evaluation visits"), "visit"),
        ("Adjudication visits", _lookup_compact_count(cohort_df, "Adjudication visits"), "visit"),
        ("Manifest notes", int(_lookup_metric(stats_df, "all_manifest_notes_n")), "note"),
        ("Packet notes", _lookup_compact_count(cohort_df, "Packet notes"), "note"),
        ("Packet mentions", _lookup_compact_count(cohort_df, "Packet mentions"), "mention"),
        ("Reference mention rows", reference_rows, "mention"),
        ("Human-audited rows", audited_rows, "mention"),
        ("Unaudited rows", unaudited_rows, "mention"),
        ("Human-audit percentage", round(audit_pct, 2), "% of reference mention rows"),
    ]
    return pd.DataFrame(rows, columns=["item", "value", "unit_or_denominator"])


def _norm_text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def _build_stage_contribution_summary(norm_df: pd.DataFrame) -> pd.DataFrame:
    raw = norm_df["raw_mention_text"].map(_norm_text)
    gold = norm_df["gold_canonical"].map(_norm_text)
    lex = norm_df["patha_a1_norm"].map(_norm_text)
    alias = norm_df["patha_a3_alias"].map(_norm_text)
    final = norm_df["patha_prediction"].map(_norm_text)

    baseline = raw.eq(gold)
    lexical_only = lex.eq(gold) & ~baseline
    alias_only = alias.eq(gold) & ~lex.eq(gold)
    decomposition_only = final.eq(gold) & ~alias.eq(gold)
    unresolved = ~final.eq(gold)

    stages = [
        ("Solved by surface exact baseline", int(baseline.sum())),
        ("Solved by lexical cleanup only", int(lexical_only.sum())),
        ("Solved by curated alias map only", int(alias_only.sum())),
        ("Solved by safe decomposition only", int(decomposition_only.sum())),
        ("Unresolved after full Path A", int(unresolved.sum())),
    ]
    total = len(norm_df)
    out = pd.DataFrame(stages, columns=["stage_contribution", "mention_count"])
    out["percent_of_reference_rows"] = out["mention_count"].map(
        lambda x: round(100.0 * float(x) / max(total, 1), 2)
    )
    return out


def _write_workflow_figure(out_dir: Path, stem: str) -> None:
    width = 1180
    height = 270
    boxes = [
        (30, 108, 180, 82, "Clinic notes"),
        (255, 108, 200, 82, "Candidate extraction\nand mention packets"),
        (500, 108, 200, 82, "LLM bootstrap\n+ targeted human audit"),
        (745, 108, 185, 82, "Deterministic\nnormalization ladder"),
        (965, 108, 185, 82, "Note-grounded\nprimary evaluation"),
    ]
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect x="0" y="0" width="100%" height="100%" fill="white"/>',
        _svg_text(30, 38, "Paper workflow: note-grounded evaluation and deterministic normalization", 22, "bold"),
        _svg_text(30, 62, "Structured EHR concordance is retained only as a secondary downstream comparison.", 12, "normal", MUTED),
    ]
    for x, y, w, h, label in boxes:
        parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="10" ry="10" fill="#f7f9fc" stroke="{ACCENT}" stroke-width="2"/>')
        lines = label.split("\n")
        for i, line in enumerate(lines):
            parts.append(_svg_text(x + 16, y + 33 + 20 * i, line, 15, "bold" if i == 0 else "normal"))
    arrows = [(210, 149, 255, 149), (455, 149, 500, 149), (700, 149, 745, 149), (930, 149, 965, 149)]
    for x1, y1, x2, y2 in arrows:
        parts.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="{MUTED}" stroke-width="2.5"/>')
        parts.append(f'<polygon points="{x2},{y2} {x2-10},{y2-6} {x2-10},{y2+6}" fill="{MUTED}"/>')
    parts.append("</svg>")
    svg_path = out_dir / f"{stem}.svg"
    _write_svg(svg_path, parts)
    _convert_svg_to_png(svg_path)


def _write_norm_ladder_figure(ladder_df: pd.DataFrame, out_dir: Path, stem: str) -> None:
    labels = ladder_df["stage"].astype(str).tolist()
    accs = ladder_df["accuracy"].astype(float).tolist()
    deltas = ladder_df["delta_vs_previous"].astype(float).tolist()
    width = 980
    height = 330
    left = 70
    top = 80
    plot_w = 820
    plot_h = 150
    y_base = top + plot_h
    x_step = plot_w / max(len(labels) - 1, 1)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect x="0" y="0" width="100%" height="100%" fill="white"/>',
        _svg_text(40, 35, "Deterministic normalization ladder", 22, "bold"),
        _svg_text(40, 58, "Accuracy is reported on 27,752 reference mention rows.", 12, "normal", MUTED),
    ]
    for tick in range(0, 6):
        val = 0.70 + tick * 0.04
        y = y_base - ((val - 0.70) / 0.20) * plot_h
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(_svg_text(20, y + 4, f"{val:.2f}", 11, "normal", MUTED))

    pts = []
    for i, acc in enumerate(accs):
        x = left + i * x_step
        y = y_base - ((acc - 0.70) / 0.20) * plot_h
        pts.append((x, y))
    for (x1, y1), (x2, y2) in zip(pts[:-1], pts[1:]):
        parts.append(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{ACCENT}" stroke-width="3"/>')
    for i, ((x, y), label, acc, delta) in enumerate(zip(pts, labels, accs, deltas)):
        parts.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="5.5" fill="{ACCENT}"/>')
        parts.append(_svg_text(x - 30, y - 12, f"{acc:.4f}", 11, "bold"))
        if i > 0:
            parts.append(_svg_text(x - 28, y + 28, f"+{delta:.4f}", 11, "normal", ACCENT_2))
        short = label.replace("+ ", "")
        parts.append(_svg_text(x - 48, y_base + 28, short, 11, "normal"))
    parts.append("</svg>")
    svg_path = out_dir / f"{stem}.svg"
    _write_svg(svg_path, parts)
    _convert_svg_to_png(svg_path)


def _write_note_density_figure(density_df: pd.DataFrame, out_dir: Path, stem: str) -> None:
    labels = density_df["mention_density_bin_conditioned_on_ge1"].astype(str).tolist()
    values = density_df["percent_of_notes_with_ge1_mentions"].astype(float).tolist()
    width = 900
    height = 340
    left = 80
    top = 70
    plot_w = 760
    plot_h = 180
    bar_gap = 26
    bar_w = (plot_w - bar_gap * (len(labels) - 1)) / len(labels)
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect x="0" y="0" width="100%" height="100%" fill="white"/>',
        _svg_text(30, 35, "Medication-positive note density", 22, "bold"),
        _svg_text(30, 58, "Distribution conditioned on notes with at least one reference medication mention (n = 7,352).", 12, "normal", MUTED),
    ]
    for tick in range(0, 6):
        val = tick * 5
        y = top + plot_h - (val / 25.0) * plot_h
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left+plot_w}" y2="{y:.1f}" stroke="{GRID}" stroke-width="1"/>')
        parts.append(_svg_text(38, y + 4, f"{val}", 11, "normal", MUTED))
    for i, (lab, val) in enumerate(zip(labels, values)):
        x = left + i * (bar_w + bar_gap)
        h = (val / 25.0) * plot_h
        y = top + plot_h - h
        parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{h:.1f}" fill="{ACCENT}" rx="4" ry="4"/>')
        parts.append(_svg_text(x + bar_w / 2 - 12, y - 8, f"{val:.2f}%", 11, "bold"))
        parts.append(_svg_text(x + bar_w / 2 - 10, top + plot_h + 28, lab, 12))
    parts.append("</svg>")
    svg_path = out_dir / f"{stem}.svg"
    _write_svg(svg_path, parts)
    _convert_svg_to_png(svg_path)


def _write_slice_robustness_figure(slice_df: pd.DataFrame, out_dir: Path, stem: str) -> None:
    families = [
        ("Candidate density", slice_df[slice_df["slice_family"] == "Candidate density"].copy()),
        ("Note length", slice_df[slice_df["slice_family"] == "Note length"].copy()),
        ("Note title", slice_df[slice_df["slice_family"] == "Note title"].copy()),
    ]
    width = 1080
    height = 640
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect x="0" y="0" width="100%" height="100%" fill="white"/>',
        _svg_text(30, 34, "Extraction robustness by slice", 22, "bold"),
        _svg_text(30, 56, "Precision, recall, and F1 vary meaningfully with candidate density, note length, and note title.", 12, "normal", MUTED),
    ]
    legend_y = 82
    legend = [("Precision", ACCENT), ("Recall", ACCENT_2), ("F1", ACCENT_3)]
    lx = 30
    for label, color in legend:
        parts.append(f'<circle cx="{lx}" cy="{legend_y}" r="5" fill="{color}"/>')
        parts.append(_svg_text(lx + 12, legend_y + 4, label, 11))
        lx += 95

    panel_left = 160
    panel_w = 850
    panel_h = 145
    panel_gap = 35
    y0 = 120
    for panel_idx, (title, sub) in enumerate(families):
        py = y0 + panel_idx * (panel_h + panel_gap)
        parts.append(_svg_text(30, py + 18, title, 15, "bold"))
        # axis
        parts.append(f'<line x1="{panel_left}" y1="{py+panel_h}" x2="{panel_left+panel_w}" y2="{py+panel_h}" stroke="{TEXT}" stroke-width="1.5"/>')
        for tick in range(0, 6):
            val = tick * 0.2
            x = panel_left + val * panel_w
            parts.append(f'<line x1="{x:.1f}" y1="{py+8}" x2="{x:.1f}" y2="{py+panel_h}" stroke="{GRID}" stroke-width="1"/>')
            parts.append(_svg_text(x - 8, py + panel_h + 18, f"{val:.1f}", 10, "normal", MUTED))
        row_gap = 24
        base_y = py + 38
        for i, row in enumerate(sub.itertuples(index=False)):
            y = base_y + i * row_gap
            parts.append(_svg_text(30, y + 4, row.slice_value, 11))
            for val, color, dy in [(row.precision, ACCENT, -6), (row.recall, ACCENT_2, 0), (row.f1, ACCENT_3, 6)]:
                x = panel_left + float(val) * panel_w
                parts.append(f'<circle cx="{x:.1f}" cy="{y+dy:.1f}" r="4.6" fill="{color}"/>')
    parts.append("</svg>")
    svg_path = out_dir / f"{stem}.svg"
    _write_svg(svg_path, parts)
    _convert_svg_to_png(svg_path)


def _write_stage_contribution_figure(
    stage_df: pd.DataFrame, failure_df: pd.DataFrame, out_dir: Path, stem: str
) -> None:
    width = 1080
    height = 360
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect x="0" y="0" width="100%" height="100%" fill="white"/>',
        _svg_text(30, 34, "Path A gain localization and residual failure concentration", 22, "bold"),
        _svg_text(
            30,
            58,
            "Left: stage-contribution counts on 27,752 reference rows. Right: residual failures are concentrated in missing aliases.",
            12,
            "normal",
            MUTED,
        ),
    ]

    stage_map = {
        "Solved by lexical cleanup only": ACCENT_3,
        "Solved by curated alias map only": ACCENT_2,
        "Solved by safe decomposition only": "#9ca3af",
        "Unresolved after full Path A": "#cbd5e1",
    }
    stage_subset = stage_df[
        stage_df["stage_contribution"].isin(stage_map.keys())
    ].copy()
    left = 40
    top = 100
    total_w = 440
    bar_h = 46
    current_x = left
    total_ref = 27752
    for row in stage_subset.itertuples(index=False):
        width_px = total_w * (row.mention_count / total_ref)
        width_px = max(width_px, 6 if row.mention_count > 0 else 0)
        color = stage_map[row.stage_contribution]
        parts.append(
            f'<rect x="{current_x:.1f}" y="{top}" width="{width_px:.1f}" height="{bar_h}" fill="{color}" rx="4" ry="4"/>'
        )
        short = row.stage_contribution.replace("Solved by ", "").replace(" only", "")
        if width_px > 42:
            parts.append(_svg_text(current_x + 8, top + 20, short, 11, "bold", "white"))
        parts.append(_svg_text(current_x, top + 72, f'{int(row.mention_count):,} ({row.percent_of_reference_rows:.2f}%)', 11))
        current_x += width_px

    parts.append(_svg_text(left, top - 16, "Stage contributions beyond the surface-exact baseline", 13, "bold"))
    parts.append(_svg_text(left, top + 102, "Surface-exact baseline already solves 20,019 rows (72.14%).", 11, "normal", MUTED))

    fail_subset = failure_df[
        failure_df["failure_category"].isin(
            ["missing alias", "lab/substance/non-medication", "combination/formulation mismatch", "ambiguous abbreviation"]
        )
    ].copy()
    fail_subset["display"] = fail_subset["failure_category"].replace(
        {
            "missing alias": "Missing alias",
            "lab/substance/non-medication": "Lab/substance/non-med",
            "combination/formulation mismatch": "Combo/formulation",
            "ambiguous abbreviation": "Ambiguous abbreviation",
        }
    )
    right_x = 600
    right_top = 105
    max_bar = 390
    max_pct = max(float(v) for v in fail_subset["percent_of_patha_failures"]) or 1.0
    parts.append(_svg_text(right_x, right_top - 22, "Residual failure concentration", 13, "bold"))
    for i, row in enumerate(fail_subset.itertuples(index=False)):
        y = right_top + i * 48
        pct = float(row.percent_of_patha_failures)
        bw = max_bar * (pct / max_pct)
        color = ACCENT if i == 0 else BG_BAR
        stroke = ACCENT if i == 0 else GRID
        parts.append(_svg_text(right_x, y + 14, row.display, 11))
        parts.append(
            f'<rect x="{right_x+170}" y="{y}" width="{max_bar}" height="24" fill="white" stroke="{GRID}" stroke-width="1" rx="4" ry="4"/>'
        )
        parts.append(
            f'<rect x="{right_x+170}" y="{y}" width="{bw:.1f}" height="24" fill="{color}" stroke="{stroke}" stroke-width="1" rx="4" ry="4"/>'
        )
        parts.append(_svg_text(right_x + 170 + bw + 8, y + 16, f"{pct:.2f}%", 11, "bold"))
    parts.append("</svg>")
    svg_path = out_dir / f"{stem}.svg"
    _write_svg(svg_path, parts)
    _convert_svg_to_png(svg_path)


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    slice_df = pd.read_csv((root / args.slice_metrics_csv).resolve()).fillna("")
    top_unresolved = pd.read_csv((root / args.top_unresolved_csv).resolve()).fillna("")
    density_df = pd.read_csv((root / args.note_density_conditioned_csv).resolve()).fillna("")
    ladder_df = pd.read_csv((root / args.normalization_ladder_csv).resolve()).fillna("")
    cohort_df = pd.read_csv((root / args.cohort_grounding_csv).resolve()).fillna("")
    stats_df = pd.read_csv((root / args.note_density_stats_csv).resolve()).fillna("")
    failure_df = pd.read_csv((root / args.failure_taxonomy_csv).resolve()).fillna("")
    extraction_df = pd.read_csv((root / args.extraction_performance_csv).resolve()).fillna("")
    norm_eval_df = pd.read_csv((root / args.normalization_eval_detailed_csv).resolve()).fillna("")
    patch_summary = json.loads((root / args.adjudication_patch_summary_json).resolve().read_text(encoding="utf-8"))

    slice_summary = _build_slice_summary(slice_df)
    _write_table(slice_summary, out_dir, "rq1_table_extraction_slice_summary")

    unresolved_summary = _build_top_unresolved(top_unresolved, n=12)
    _write_table(unresolved_summary, out_dir, "rq1_table_top_unresolved_mentions")

    audit_summary = _build_audit_reference_summary(cohort_df, stats_df, patch_summary)
    _write_table(audit_summary, out_dir, "rq1_table_audit_reference_summary")

    stage_summary = _build_stage_contribution_summary(norm_eval_df)
    _write_table(stage_summary, out_dir, "rq1_table_stage_contribution_patha")

    _write_workflow_figure(out_dir, "rq1_fig_workflow_note_grounded")
    _write_norm_ladder_figure(ladder_df, out_dir, "rq1_fig_normalization_ladder_note_grounded")
    _write_note_density_figure(density_df, out_dir, "rq1_fig_note_density_conditioned_note_grounded")
    _write_slice_robustness_figure(slice_summary, out_dir, "rq1_fig_extraction_slice_robustness")
    _write_stage_contribution_figure(stage_summary, failure_df, out_dir, "rq1_fig_stage_contribution_and_failures")

    summary = {
        "inputs": {
            "slice_metrics_csv": str((root / args.slice_metrics_csv).resolve()),
            "top_unresolved_csv": str((root / args.top_unresolved_csv).resolve()),
            "note_density_conditioned_csv": str((root / args.note_density_conditioned_csv).resolve()),
            "normalization_ladder_csv": str((root / args.normalization_ladder_csv).resolve()),
            "cohort_grounding_csv": str((root / args.cohort_grounding_csv).resolve()),
            "note_density_stats_csv": str((root / args.note_density_stats_csv).resolve()),
            "failure_taxonomy_csv": str((root / args.failure_taxonomy_csv).resolve()),
            "extraction_performance_csv": str((root / args.extraction_performance_csv).resolve()),
            "normalization_eval_detailed_csv": str((root / args.normalization_eval_detailed_csv).resolve()),
            "adjudication_patch_summary_json": str((root / args.adjudication_patch_summary_json).resolve()),
        },
        "frozen_run_counts": {
            "reference_rows": int(audit_summary.loc[audit_summary["item"] == "Reference mention rows", "value"].iloc[0]),
            "human_audited_rows": int(audit_summary.loc[audit_summary["item"] == "Human-audited rows", "value"].iloc[0]),
            "human_audit_percentage": float(audit_summary.loc[audit_summary["item"] == "Human-audit percentage", "value"].iloc[0]),
            "surface_exact_rows": int(stage_summary.loc[stage_summary["stage_contribution"] == "Solved by surface exact baseline", "mention_count"].iloc[0]),
            "lexical_only_rows": int(stage_summary.loc[stage_summary["stage_contribution"] == "Solved by lexical cleanup only", "mention_count"].iloc[0]),
            "alias_only_rows": int(stage_summary.loc[stage_summary["stage_contribution"] == "Solved by curated alias map only", "mention_count"].iloc[0]),
            "decomposition_only_rows": int(stage_summary.loc[stage_summary["stage_contribution"] == "Solved by safe decomposition only", "mention_count"].iloc[0]),
            "unresolved_rows": int(stage_summary.loc[stage_summary["stage_contribution"] == "Unresolved after full Path A", "mention_count"].iloc[0]),
            "extraction_precision": float(extraction_df.loc[0, "precision"]),
            "extraction_recall": float(extraction_df.loc[0, "recall"]),
            "extraction_f1": float(extraction_df.loc[0, "f1"]),
        },
        "outputs": sorted(p.name for p in out_dir.iterdir() if p.is_file()),
    }
    (out_dir / "rq1_paper_enrichment_bundle_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(f"Saved enrichment bundle: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
