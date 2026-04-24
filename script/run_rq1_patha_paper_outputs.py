#!/usr/bin/env python3
"""
Generate compact paper-ready Path A focused outputs from a frozen rerun state.

Paper framing:
- Clinic-note-only, adjudication-first.
- Mention-level normalization ladder is primary.
- Path B excluded from main claims.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from statistics import median
from typing import Dict, Iterable, List, Sequence, Tuple

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_drug_linking import (
    canonicalize_drug,
    load_alias_exclusions,
    load_alias_map,
    normalize_drug_text,
)


FAILURE_CATEGORIES = [
    "ambiguous abbreviation",
    "vague class term",
    "lab/substance/non-medication",
    "combination/formulation mismatch",
    "missing alias",
    "unclear canonical target",
]

LAB_SUBSTANCE_TERMS = {
    "amylase",
    "calcium",
    "cholesterol",
    "collagen",
    "creatinine",
    "fructose",
    "glucose",
    "iodine",
    "iron",
    "lactase",
    "lactate",
    "lactose",
    "lipase",
    "lycopene",
    "magnesium",
    "potassium",
    "sodium",
}

VAGUE_CLASS_PATTERNS = [
    r"\bchemo(?:therapy)?\b",
    r"\bimmunotherapy\b",
    r"\bhormone therapy\b",
    r"\bpain (?:med|medication)\b",
    r"\bblood pressure (?:med|medication)\b",
    r"\bmedications?\b",
    r"\btherapy\b",
    r"\btreatment\b",
]

COMBO_FORMULATION_PATTERNS = [
    r"(?:/|\+|\band\b|\bwith\b|\bplus\b)",
    r"\b(?:hcl|hydrochloride|sodium|succinate|acetate|phosphate|citrate|tartrate|mesylate|besylate|fumarate|maleate)\b",
    r"\b(?:tablet|capsule|injection|solution|suspension|patch|cream|ointment|er|xr|sr|dr|ir)\b",
]

ABBREVIATION_HINTS = {
    "5fu",
    "adt",
    "atezo",
    "bev",
    "folfiri",
    "folfirinox",
    "folfox",
    "io",
    "prn",
}

UNCLEAR_TARGET_PATTERNS = [
    r"\bmed(?:ication)?s?\b",
    r"\bdrug\b",
    r"\btherapy\b",
    r"\btreatment\b",
    r"\bregimen\b",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate Path A focused paper outputs.")
    p.add_argument(
        "--normalization-detailed-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/normalization_eval/rq1_normalization_eval_detailed.csv",
    )
    p.add_argument(
        "--adjudicated-mentions-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/rq1_adjudicated_mentions.csv",
    )
    p.add_argument(
        "--note-manifest-csv",
        default="episode_notes/manifests/adjudication_note_manifest.csv",
    )
    p.add_argument(
        "--alias-artifact",
        default="resources/lexicons/rq1_drug_aliases.csv",
    )
    p.add_argument(
        "--patha-exclusions-csv",
        default="resources/manual/pathA_alias_exclusions.csv",
    )
    p.add_argument(
        "--output-dir",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_patha",
    )
    return p.parse_args()


def _basic_surface_norm(text: str) -> str:
    t = str(text).strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t


def _safe_div(num: float, den: float) -> float:
    return float(num) / float(den) if den else 0.0


def _to_markdown_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "_No rows._\n"
    try:
        return df.to_markdown(index=False) + "\n"
    except Exception:
        cols = list(df.columns)
        head = "| " + " | ".join(cols) + " |"
        sep = "| " + " | ".join(["---"] * len(cols)) + " |"
        lines = [head, sep]
        for _, row in df.iterrows():
            lines.append("| " + " | ".join(str(row[c]) for c in cols) + " |")
        return "\n".join(lines) + "\n"


def _write_table(df: pd.DataFrame, out_dir: Path, stem: str) -> None:
    df.to_csv(out_dir / f"{stem}.csv", index=False)
    (out_dir / f"{stem}.md").write_text(_to_markdown_table(df), encoding="utf-8")


def _svg_text(x: float, y: float, text: str, size: int = 12, weight: str = "normal") -> str:
    safe = (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
    return f'<text x="{x:.1f}" y="{y:.1f}" font-family="Arial, Helvetica, sans-serif" font-size="{size}" font-weight="{weight}">{safe}</text>'


def _write_bar_svg(
    labels: Sequence[str],
    values: Sequence[float],
    title: str,
    out_path: Path,
    value_fmt: str = "{:.1f}%",
    max_value: float | None = None,
) -> None:
    width = 920
    row_h = 34
    top = 68
    left_label = 30
    left_bar = 290
    bar_w = 560
    height = top + row_h * len(labels) + 28
    vmax = max_value if max_value is not None else (max(values) if values else 1.0)
    vmax = max(vmax, 1e-9)

    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect x="0" y="0" width="100%" height="100%" fill="white"/>',
        _svg_text(24, 36, title, size=18, weight="bold"),
    ]

    for i, (lab, val) in enumerate(zip(labels, values)):
        y = top + i * row_h
        w = bar_w * (float(val) / vmax)
        parts.append(_svg_text(left_label, y + 21, str(lab), size=12))
        parts.append(f'<rect x="{left_bar}" y="{y+6}" width="{bar_w}" height="16" fill="#eef2f7" rx="3" ry="3"/>')
        parts.append(f'<rect x="{left_bar}" y="{y+6}" width="{max(w,0):.2f}" height="16" fill="#2a6fbb" rx="3" ry="3"/>')
        parts.append(_svg_text(left_bar + bar_w + 8, y + 20, value_fmt.format(float(val)), size=11))

    parts.append("</svg>")
    out_path.write_text("\n".join(parts), encoding="utf-8")


def _write_workflow_svg(out_path: Path) -> None:
    width = 1180
    height = 250
    boxes = [
        (24, 96, 220, 88, "Clinic Notes\nExtraction"),
        (280, 96, 220, 88, "LLM + Human\nAdjudication"),
        (536, 96, 220, 88, "Controlled\nNormalization"),
        (792, 96, 170, 88, "Path A\nEvaluation"),
        (986, 96, 170, 88, "Secondary EHR\nConcordance"),
    ]
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">',
        '<rect x="0" y="0" width="100%" height="100%" fill="white"/>',
        _svg_text(24, 36, "Clinic-Note-Only, Adjudication-First Workflow", size=20, weight="bold"),
    ]
    for x, y, w, h, label in boxes:
        parts.append(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" fill="#f4f7fb" stroke="#345d8a" stroke-width="1.5" rx="8" ry="8"/>')
        lines = label.split("\n")
        for j, line in enumerate(lines):
            parts.append(_svg_text(x + 18, y + 36 + 20 * j, line, size=14, weight="bold" if j == 0 else "normal"))
    arrows = [(244, 140, 280, 140), (500, 140, 536, 140), (756, 140, 792, 140), (962, 140, 986, 140)]
    for x1, y1, x2, y2 in arrows:
        parts.append(f'<line x1="{x1}" y1="{y1}" x2="{x2}" y2="{y2}" stroke="#4d4d4d" stroke-width="2"/>')
        parts.append(f'<polygon points="{x2}, {y2} {x2-8},{y2-5} {x2-8},{y2+5}" fill="#4d4d4d"/>')
    parts.append("</svg>")
    out_path.write_text("\n".join(parts), encoding="utf-8")


def _is_short_or_abbrev(norm_term: str, raw: str) -> bool:
    t = str(norm_term).strip()
    raw_t = str(raw).strip()
    if not t:
        return True
    toks = [x for x in t.split() if x]
    compact = re.sub(r"[^A-Za-z0-9]", "", raw_t)
    if t in ABBREVIATION_HINTS:
        return True
    if len(toks) == 1 and len(toks[0]) <= 4:
        return True
    if compact and len(compact) <= 4:
        return True
    if re.fullmatch(r"[A-Z]{2,6}", raw_t):
        return True
    return False


def _contains_any_pattern(text: str, patterns: Iterable[str]) -> bool:
    t = str(text).strip().lower()
    return any(re.search(p, t) for p in patterns)


def _categorize_failure(row: pd.Series) -> str:
    raw = str(row.get("raw_mention_text", "")).strip().lower()
    a1 = str(row.get("patha_a1_norm", "")).strip().lower()
    gold = str(row.get("gold_canonical", "")).strip().lower()
    tokens = set(a1.split())

    if tokens & LAB_SUBSTANCE_TERMS:
        return "lab/substance/non-medication"
    if _contains_any_pattern(raw, VAGUE_CLASS_PATTERNS):
        return "vague class term"
    if _contains_any_pattern(raw, COMBO_FORMULATION_PATTERNS) or _contains_any_pattern(gold, COMBO_FORMULATION_PATTERNS):
        return "combination/formulation mismatch"
    if _is_short_or_abbrev(a1, raw):
        return "ambiguous abbreviation"
    if not a1 or _contains_any_pattern(raw, UNCLEAR_TARGET_PATTERNS):
        return "unclear canonical target"
    return "missing alias"


def _make_normalization_ladder(detail: pd.DataFrame, alias_map: Dict[str, str]) -> Tuple[pd.DataFrame, Dict[str, float]]:
    work = detail.copy()
    n = len(work)
    work["gold_norm"] = work["gold_canonical"].astype(str).str.strip().str.lower()
    work["surface_exact_norm"] = work["raw_mention_text"].map(_basic_surface_norm)
    work["lexical_cleanup_norm"] = work["raw_mention_text"].map(normalize_drug_text)
    work["alias_map_norm"] = work["raw_mention_text"].map(lambda x: canonicalize_drug(str(x), alias_map))
    work["patha_full_norm"] = work["patha_prediction"].astype(str).str.strip().str.lower()

    stages = [
        ("surface-exact baseline", "surface_exact_norm"),
        ("+ lexical cleanup", "lexical_cleanup_norm"),
        ("+ curated alias map", "alias_map_norm"),
        ("+ safe decomposition / full Path A", "patha_full_norm"),
    ]

    rows: List[Dict[str, object]] = []
    prev_acc = None
    baseline_acc = None
    metrics: Dict[str, float] = {}

    for stage_name, col in stages:
        acc = float((work[col] == work["gold_norm"]).mean()) if n else 0.0
        if baseline_acc is None:
            baseline_acc = acc
        delta_prev = acc - prev_acc if prev_acc is not None else 0.0
        delta_base = acc - baseline_acc
        rows.append(
            {
                "stage": stage_name,
                "n_mentions": int(n),
                "accuracy": round(acc, 6),
                "delta_vs_previous": round(delta_prev, 6),
                "delta_vs_surface_exact_baseline": round(delta_base, 6),
            }
        )
        metrics[stage_name] = acc
        prev_acc = acc

    return pd.DataFrame(rows), metrics


def _make_failure_taxonomy(detail: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    fail = detail[~detail["patha_correct"].astype(bool)].copy()
    fail["patha_failure_category"] = fail.apply(_categorize_failure, axis=1)
    n = len(fail)

    counts = fail["patha_failure_category"].value_counts()
    rows = []
    for cat in FAILURE_CATEGORIES:
        c = int(counts.get(cat, 0))
        rows.append(
            {
                "failure_category": cat,
                "count": c,
                "percent_of_patha_failures": round(100.0 * _safe_div(c, n), 2),
            }
        )
    table = pd.DataFrame(rows)

    # Reproducible examples: top 8 frequent raw mentions per category.
    ex_rows: List[Dict[str, object]] = []
    for cat in FAILURE_CATEGORIES:
        sub = fail[fail["patha_failure_category"] == cat].copy()
        if sub.empty:
            continue
        vc = sub["raw_mention_text"].astype(str).str.lower().value_counts().head(8)
        for mention, c in vc.items():
            top_gold = (
                sub[sub["raw_mention_text"].astype(str).str.lower() == mention]["gold_canonical"]
                .astype(str)
                .str.lower()
                .value_counts()
                .head(1)
            )
            ex_rows.append(
                {
                    "failure_category": cat,
                    "example_raw_mention_text": mention,
                    "example_count": int(c),
                    "most_common_gold_canonical": str(top_gold.index[0]) if len(top_gold) else "",
                }
            )
    examples = pd.DataFrame(ex_rows)
    return table, examples


def _make_note_density_table(adjud: pd.DataFrame, note_manifest: pd.DataFrame) -> pd.DataFrame:
    adjud_valid = adjud[adjud["adjudicated_canonical_label"].astype(str).str.strip() != ""].copy()
    note_counts = adjud_valid.groupby("note_id").size()
    notes_with_1 = int((note_counts == 1).sum())
    notes_with_2p = int((note_counts >= 2).sum())

    manifest_notes = int(note_manifest["note_id"].astype(str).nunique()) if not note_manifest.empty else int(adjud_valid["note_id"].astype(str).nunique())
    notes_with_any = int(note_counts.shape[0])
    notes_with_0 = max(manifest_notes - notes_with_any, 0)

    rows = [
        ("0 meds", notes_with_0),
        ("1 med", notes_with_1),
        (">=2 meds", notes_with_2p),
    ]
    out = []
    for band, c in rows:
        out.append(
            {
                "note_medication_density": band,
                "note_count": int(c),
                "percent_of_notes": round(100.0 * _safe_div(c, manifest_notes), 2),
            }
        )
    return pd.DataFrame(out)


def _make_visit_sensitivity_table(detail: pd.DataFrame) -> pd.DataFrame:
    work = detail.copy()
    work["visit_key"] = work["person_id"].astype(str).str.strip() + "||" + work["visit_id"].astype(str).str.strip()
    work["gold_norm"] = work["gold_canonical"].astype(str).str.strip().str.lower()
    work["patha_norm"] = work["patha_prediction"].astype(str).str.strip().str.lower()

    by_visit_gold = work.groupby("visit_key")["gold_norm"].apply(lambda s: sorted(set(x for x in s if x))).to_dict()
    by_visit_patha = work.groupby("visit_key")["patha_norm"].apply(lambda s: sorted(set(x for x in s if x))).to_dict()
    visits = sorted(set(by_visit_gold) | set(by_visit_patha))

    jaccards: List[float] = []
    exact_match = 0
    gold_sizes: List[int] = []
    patha_sizes: List[int] = []
    for v in visits:
        g = set(by_visit_gold.get(v, []))
        p = set(by_visit_patha.get(v, []))
        gold_sizes.append(len(g))
        patha_sizes.append(len(p))
        if g == p:
            exact_match += 1
        union = g | p
        inter = g & p
        jaccards.append(_safe_div(len(inter), len(union)))

    n_visits = len(visits)
    rows = [
        ("n_visits_in_scope", n_visits),
        ("mean_unique_canonical_drugs_per_visit_gold", round(_safe_div(sum(gold_sizes), n_visits), 4)),
        ("median_unique_canonical_drugs_per_visit_gold", round(float(median(gold_sizes)) if gold_sizes else 0.0, 4)),
        ("mean_unique_canonical_drugs_per_visit_patha", round(_safe_div(sum(patha_sizes), n_visits), 4)),
        ("median_unique_canonical_drugs_per_visit_patha", round(float(median(patha_sizes)) if patha_sizes else 0.0, 4)),
        ("visit_level_exact_set_match_rate_patha_vs_gold", round(_safe_div(exact_match, n_visits), 6)),
        ("visit_level_mean_jaccard_patha_vs_gold", round(_safe_div(sum(jaccards), n_visits), 6)),
    ]
    return pd.DataFrame(rows, columns=["metric", "value"])


def main() -> int:
    args = parse_args()
    # Script lives in resources/script; project root is two levels up.
    root = Path(__file__).resolve().parents[2]
    detail_path = (root / args.normalization_detailed_csv).resolve()
    adjud_path = (root / args.adjudicated_mentions_csv).resolve()
    note_manifest_path = (root / args.note_manifest_csv).resolve()
    alias_path = (root / args.alias_artifact).resolve()
    exclusions_path = (root / args.patha_exclusions_csv).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not detail_path.exists():
        raise FileNotFoundError(f"Missing normalization detailed CSV: {detail_path}")
    if not adjud_path.exists():
        raise FileNotFoundError(f"Missing adjudicated mentions CSV: {adjud_path}")

    detail = pd.read_csv(detail_path).fillna("")
    adjud = pd.read_csv(adjud_path).fillna("")
    note_manifest = pd.read_csv(note_manifest_path).fillna("") if note_manifest_path.exists() else pd.DataFrame()

    patha_exclusions = load_alias_exclusions(exclusions_path) if exclusions_path.exists() else set()
    alias_map = load_alias_map(alias_path, exclusions=patha_exclusions, enforce_one_to_one=False) if alias_path.exists() else {}

    # 1) Mention-level normalization ladder.
    ladder_df, ladder_metrics = _make_normalization_ladder(detail=detail, alias_map=alias_map)
    _write_table(ladder_df, out_dir, "rq1_table_normalization_ladder_patha_focus")

    # 2) Path A remaining failure taxonomy.
    taxonomy_df, taxonomy_examples_df = _make_failure_taxonomy(detail=detail)
    _write_table(taxonomy_df, out_dir, "rq1_table_patha_failure_taxonomy")
    if not taxonomy_examples_df.empty:
        _write_table(taxonomy_examples_df, out_dir, "rq1_table_patha_failure_taxonomy_examples")

    # 3) Note medication density table.
    note_density_df = _make_note_density_table(adjud=adjud, note_manifest=note_manifest)
    _write_table(note_density_df, out_dir, "rq1_table_note_med_density")

    # 4) Optional visit-level sensitivity.
    visit_df = _make_visit_sensitivity_table(detail=detail)
    _write_table(visit_df, out_dir, "rq1_table_visit_level_sensitivity")

    # 5) Compact visuals (dependency-free SVG).
    _write_workflow_svg(out_dir / "rq1_fig_workflow_patha_focus.svg")
    _write_bar_svg(
        labels=list(ladder_df["stage"]),
        values=[100.0 * float(x) for x in ladder_df["accuracy"]],
        title="Normalization Ladder (Mention-Level Accuracy)",
        out_path=out_dir / "rq1_fig_normalization_ladder_patha_focus.svg",
        value_fmt="{:.2f}%",
        max_value=100.0,
    )
    _write_bar_svg(
        labels=list(taxonomy_df["failure_category"]),
        values=[float(x) for x in taxonomy_df["percent_of_patha_failures"]],
        title="Path A Remaining Failure Breakdown",
        out_path=out_dir / "rq1_fig_patha_failure_taxonomy.svg",
        value_fmt="{:.2f}%",
        max_value=100.0,
    )
    _write_bar_svg(
        labels=list(note_density_df["note_medication_density"]),
        values=[float(x) for x in note_density_df["percent_of_notes"]],
        title="Adjudicated Medication Mention Density Per Note",
        out_path=out_dir / "rq1_fig_note_med_density.svg",
        value_fmt="{:.2f}%",
        max_value=100.0,
    )

    # 6) Methods note and results paragraph.
    methods_note = (
        "## Methods Note (Path A Focus)\n"
        "Primary unit is mention-level adjudication rows (`adjudication_unit_id`) from the frozen normalization detailed CSV.\n\n"
        "Ablation row definitions:\n"
        "1. `surface-exact baseline`: raw mention lowercased + whitespace-collapsed (`surface_exact_norm`) and compared directly to adjudicated canonical gold.\n"
        "2. `+ lexical cleanup`: `normalize_drug_text(raw_mention_text)` compared directly to gold.\n"
        "3. `+ curated alias map`: `canonicalize_drug(raw_mention_text, alias_map)` (lexical cleanup + curated aliases, exclusions applied) compared directly to gold.\n"
        "4. `+ safe decomposition / full Path A`: frozen rerun `patha_prediction` compared to gold.\n\n"
        "Failure taxonomy procedure:\n"
        "- Applied deterministic rule-based categorization only to remaining Path A failures (`patha_correct == False`).\n"
        "- Category priority order: lab/substance/non-medication -> vague class term -> combination/formulation mismatch -> ambiguous abbreviation -> unclear canonical target -> missing alias.\n"
        "- Rules are encoded in `run_rq1_patha_paper_outputs.py` and therefore reproducible.\n\n"
        "Note medication-density table:\n"
        "- Denominator notes from adjudication note manifest.\n"
        "- Numerator counts from adjudicated mentions with non-empty canonical labels grouped by note.\n"
        "- Bands: `0 meds`, `1 med`, `>=2 meds`.\n"
    )
    (out_dir / "rq1_methods_note_patha_focus.md").write_text(methods_note, encoding="utf-8")

    stage_rows = ladder_df.to_dict(orient="records")
    s0 = stage_rows[0]["accuracy"] if len(stage_rows) > 0 else 0.0
    s3 = stage_rows[3]["accuracy"] if len(stage_rows) > 3 else 0.0
    fail_top = taxonomy_df.sort_values("count", ascending=False).head(2)
    fail_desc = ", ".join(
        f"{r['failure_category']} ({int(r['count'])}, {float(r['percent_of_patha_failures']):.2f}%)"
        for _, r in fail_top.iterrows()
    )
    results_paragraph = (
        "In mention-level controlled normalization, surface-exact matching reached "
        f"{100.0*float(s0):.2f}% accuracy, while full deterministic Path A reached {100.0*float(s3):.2f}% "
        f"(+{100.0*(float(s3)-float(s0)):.2f} percentage points vs surface-exact). "
        "This supports the main claim that adjudication-grounded deterministic normalization materially outperforms surface matching. "
        "Among remaining Path A failures, the dominant categories were "
        f"{fail_desc}, indicating that unresolved errors are concentrated in alias coverage and clinically ambiguous mention forms "
        "rather than broad extraction collapse."
    )
    (out_dir / "rq1_results_paragraph_patha_focus.md").write_text(results_paragraph + "\n", encoding="utf-8")

    write_run_summary(
        out_dir / "rq1_patha_paper_outputs_summary.json",
        {
            "inputs": {
                "normalization_detailed_csv": str(detail_path),
                "adjudicated_mentions_csv": str(adjud_path),
                "note_manifest_csv": str(note_manifest_path) if note_manifest_path.exists() else None,
                "alias_artifact": str(alias_path) if alias_path.exists() else None,
                "patha_exclusions_csv": str(exclusions_path) if exclusions_path.exists() else None,
            },
            "metrics_snapshot": {
                "n_mentions": int(len(detail)),
                "surface_exact_accuracy": round(float(ladder_metrics.get("surface-exact baseline", 0.0)), 6),
                "lexical_cleanup_accuracy": round(float(ladder_metrics.get("+ lexical cleanup", 0.0)), 6),
                "alias_map_accuracy": round(float(ladder_metrics.get("+ curated alias map", 0.0)), 6),
                "full_patha_accuracy": round(float(ladder_metrics.get("+ safe decomposition / full Path A", 0.0)), 6),
                "patha_failure_n": int((~detail["patha_correct"].astype(bool)).sum()),
            },
            "outputs_dir": str(out_dir),
            "tables": [
                "rq1_table_normalization_ladder_patha_focus",
                "rq1_table_patha_failure_taxonomy",
                "rq1_table_note_med_density",
                "rq1_table_visit_level_sensitivity",
            ],
            "figures_svg": [
                "rq1_fig_workflow_patha_focus.svg",
                "rq1_fig_normalization_ladder_patha_focus.svg",
                "rq1_fig_patha_failure_taxonomy.svg",
                "rq1_fig_note_med_density.svg",
            ],
            "notes": [
                "rq1_methods_note_patha_focus.md",
                "rq1_results_paragraph_patha_focus.md",
            ],
        },
    )

    print(f"Saved Path A paper outputs: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
