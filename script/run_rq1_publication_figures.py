#!/usr/bin/env python3
"""
Generate publication-style paper figures for the clinic-note medication normalization paper.

Figure 2: extraction robustness by slice
Figure 3: deterministic normalization ladder with stage contributions
Figure 4: residual failure concentration with top unresolved terms
Figure 5: medication-density distribution among medication-positive notes
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


PALETTE = {
    "precision": "#1f77b4",
    "recall": "#ff7f0e",
    "f1": "#2ca02c",
    "accent": "#1f5aa6",
    "accent_light": "#8fb7e8",
    "neutral": "#6b7280",
    "grid": "#d9dee7",
    "missing_alias": "#cc4c3b",
    "other": "#9aa5b1",
}


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate publication-style figures for the RQ1 paper.")
    p.add_argument(
        "--slice-metrics-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/note_truth_eval/rq1_step4_note_truth_slice_metrics.csv",
    )
    p.add_argument(
        "--normalization-ladder-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_patha/rq1_table_normalization_ladder_patha_focus.csv",
    )
    p.add_argument(
        "--stage-contribution-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_enriched/rq1_table_stage_contribution_patha.csv",
    )
    p.add_argument(
        "--failure-taxonomy-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_patha/rq1_table_patha_failure_taxonomy.csv",
    )
    p.add_argument(
        "--top-unresolved-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_enriched/rq1_table_top_unresolved_mentions.csv",
    )
    p.add_argument(
        "--note-density-all-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_patha/rq1_table_note_med_density.csv",
    )
    p.add_argument(
        "--note-density-conditioned-csv",
        default="episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_patha/rq1_table_note_med_density_conditioned_ge1.csv",
    )
    p.add_argument(
        "--output-dir",
        default="paper/IEEE-conference-template-062824/figures",
    )
    return p.parse_args()


def _style_axes(ax: plt.Axes) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.spines["left"].set_color("#c7cdd8")
    ax.spines["bottom"].set_color("#c7cdd8")
    ax.grid(axis="x", color=PALETTE["grid"], linewidth=0.8)
    ax.set_axisbelow(True)


def _normalize_slice_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out = out[out["slice_name"].isin(["candidate_density_bin", "note_length_bin", "note_title_norm"])].copy()
    out = out[out["slice_value"].astype(str) != ""].copy()
    out = out[out["slice_value"].astype(str).str.lower() != "nan"].copy()
    out["slice_family"] = out["slice_name"].replace(
        {
            "candidate_density_bin": "Candidate density",
            "note_length_bin": "Note length",
            "note_title_norm": "Note title",
        }
    )
    out["slice_value"] = out["slice_value"].replace(
        {
            "1": "1",
            "2_3": "2--3",
            "4_7": "4--7",
            "8_plus": "8+",
            "lt_250": "<250",
            "250_749": "250--749",
            "750_1499": "750--1499",
            "ge_1500": ">=1500",
            "Assessment & Plan Note": "A&P Note",
            "Patient Instructions": "Patient Instructions",
            "Progress Notes": "Progress Notes",
            "H&P": "H&P",
            "Consults": "Consults",
        }
    )
    return out


def make_figure2(slice_df: pd.DataFrame, output_dir: Path) -> None:
    df = _normalize_slice_df(slice_df)
    family_order = {
        "Candidate density": ["1", "2--3", "4--7", "8+"],
        "Note length": ["<250", "250--749", "750--1499", ">=1500"],
        "Note title": ["A&P Note", "Patient Instructions", "Progress Notes", "H&P", "Consults"],
    }

    fig, axes = plt.subplots(3, 1, figsize=(8.3, 9.2), constrained_layout=True)

    for ax, family in zip(axes, family_order):
        sub = df[df["slice_family"] == family].copy()
        sub["slice_value"] = pd.Categorical(sub["slice_value"], family_order[family], ordered=True)
        sub = sub.sort_values("slice_value")
        y = range(len(sub))

        ax.scatter(sub["precision"], y, s=52, color=PALETTE["precision"], label="Precision", zorder=3)
        ax.scatter(sub["recall"], y, s=52, color=PALETTE["recall"], label="Recall", zorder=3)
        ax.scatter(sub["f1"], y, s=52, color=PALETTE["f1"], label="F1", zorder=3)

        for col, color in [("precision", PALETTE["precision"]), ("recall", PALETTE["recall"]), ("f1", PALETTE["f1"])]:
            ax.plot(sub[col], y, color=color, linewidth=1.4, alpha=0.8)

        ax.set_yticks(list(y))
        ax.set_yticklabels(sub["slice_value"], fontsize=10)
        ax.set_xlim(0.2, 1.02)
        ax.set_ylabel(family, fontsize=11, fontweight="bold")
        _style_axes(ax)

        if family == "Candidate density":
            hi = sub.loc[sub["recall"].idxmax()]
            lo = sub.loc[sub["precision"].idxmax()]
            ax.annotate("highest recall", xy=(hi["recall"], list(sub.index).index(hi.name)),
                        xytext=(hi["recall"] + 0.05, list(sub.index).index(hi.name) + 0.18),
                        fontsize=9, color=PALETTE["neutral"])
            ax.annotate("highest precision", xy=(lo["precision"], list(sub.index).index(lo.name)),
                        xytext=(lo["precision"] + 0.05, list(sub.index).index(lo.name) - 0.25),
                        fontsize=9, color=PALETTE["neutral"])

    axes[0].legend(loc="lower right", ncol=3, frameon=False, fontsize=10)
    axes[-1].set_xlabel("Metric value", fontsize=11)
    fig.savefig(output_dir / "rq1_fig_extraction_slice_robustness.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def make_figure3(ladder_df: pd.DataFrame, stage_df: pd.DataFrame, output_dir: Path) -> None:
    ladder = ladder_df.copy()
    ladder["stage_short"] = [
        "Surface exact",
        "Lexical cleanup",
        "Curated alias",
        "Full Path A",
    ]
    stage_map = {
        "Solved by lexical cleanup only": "Lexical only",
        "Solved by curated alias map only": "Alias only",
        "Solved by safe decomposition only": "Decomposition only",
        "Unresolved after full Path A": "Unresolved",
    }
    stage = stage_df[stage_df["stage_contribution"].isin(stage_map)].copy()
    stage["display"] = stage["stage_contribution"].map(stage_map)

    fig = plt.figure(figsize=(8.3, 4.8), constrained_layout=True)
    gs = fig.add_gridspec(1, 2, width_ratios=[1.25, 1.0])
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])

    x = list(range(len(ladder)))
    acc = ladder["accuracy"].astype(float).tolist()
    ax1.plot(x, acc, marker="o", markersize=7, linewidth=2.4, color=PALETTE["accent"])
    ax1.set_xticks(x)
    ax1.set_xticklabels(ladder["stage_short"], rotation=20, ha="right", fontsize=10)
    ax1.set_ylim(0.68, 0.88)
    ax1.set_ylabel("Mention-level normalization accuracy", fontsize=11)
    _style_axes(ax1)
    for i, (xi, yi) in enumerate(zip(x, acc)):
        ax1.text(xi, yi + 0.006, f"{yi:.4f}", ha="center", va="bottom", fontsize=9, fontweight="bold")
        if i > 0:
            delta = float(ladder.loc[i, "delta_vs_previous"])
            ax1.text(xi - 0.06, yi - 0.02, f"+{delta:.4f}", fontsize=9, color=PALETTE["neutral"])

    colors = [PALETTE["accent_light"], PALETTE["accent"], "#c8d1dc", "#d7dde6"]
    ax2.barh(stage["display"], stage["percent_of_reference_rows"], color=colors, edgecolor="none", height=0.62)
    ax2.invert_yaxis()
    ax2.set_xlabel("% of 27,752 reference rows", fontsize=11)
    _style_axes(ax2)

    for row in stage.itertuples(index=False):
        ax2.text(float(row.percent_of_reference_rows) + 0.6, row.display,
                 f"{int(row.mention_count):,}", va="center", fontsize=9)

    fig.savefig(output_dir / "rq1_fig_normalization_ladder_note_grounded.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def make_figure4(failure_df: pd.DataFrame, unresolved_df: pd.DataFrame, output_dir: Path) -> None:
    fail = failure_df.copy()
    fail = fail[fail["count"].astype(int) > 0].copy()
    fail["display"] = fail["failure_category"].replace(
        {
            "missing alias": "Missing alias",
            "lab/substance/non-medication": "Lab/substance/non-med",
            "combination/formulation mismatch": "Combo/formulation",
            "ambiguous abbreviation": "Ambiguous abbreviation",
        }
    )
    fail = fail.sort_values("percent_of_patha_failures", ascending=True)

    unresolved = unresolved_df.head(10).copy().sort_values("mention_count", ascending=True)

    fig = plt.figure(figsize=(8.3, 5.2), constrained_layout=True)
    gs = fig.add_gridspec(1, 2, width_ratios=[0.95, 1.15])
    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])

    colors = [PALETTE["other"]] * len(fail)
    if "Missing alias" in fail["display"].values:
        colors[list(fail["display"]).index("Missing alias")] = PALETTE["missing_alias"]
    ax1.barh(fail["display"], fail["percent_of_patha_failures"], color=colors, edgecolor="none", height=0.62)
    ax1.set_xlabel("% of unresolved Path A rows", fontsize=11)
    _style_axes(ax1)
    for row in fail.itertuples(index=False):
        ax1.text(float(row.percent_of_patha_failures) + 1.0, row.display,
                 f"{float(row.percent_of_patha_failures):.2f}%", va="center", fontsize=9)

    ax2.barh(unresolved["raw_mention"], unresolved["mention_count"], color=PALETTE["accent"], edgecolor="none", height=0.62)
    ax2.set_xlabel("Unresolved mention rows", fontsize=11)
    _style_axes(ax2)
    for row in unresolved.itertuples(index=False):
        ax2.text(int(row.mention_count) + 8, row.raw_mention, f"{int(row.mention_count)}", va="center", fontsize=9)

    fig.savefig(output_dir / "rq1_fig_stage_contribution_and_failures.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def make_figure5(note_density_all_df: pd.DataFrame, note_density_df: pd.DataFrame, output_dir: Path) -> None:
    all_df = note_density_all_df.copy()
    all_order = ["0 meds", "1 med", ">=2 meds"]
    all_df["note_medication_density"] = pd.Categorical(
        all_df["note_medication_density"], all_order, ordered=True
    )
    all_df = all_df.sort_values("note_medication_density")

    df = note_density_df.copy()
    order = ["1", "2", "3", "4", ">=5"]
    df["mention_density_bin_conditioned_on_ge1"] = pd.Categorical(
        df["mention_density_bin_conditioned_on_ge1"], order, ordered=True
    )
    df = df.sort_values("mention_density_bin_conditioned_on_ge1")

    fig = plt.figure(figsize=(3.35, 3.95))
    gs = fig.add_gridspec(2, 1, height_ratios=[0.72, 1.38])
    ax0 = fig.add_subplot(gs[0, 0])
    ax1 = fig.add_subplot(gs[1, 0])
    fig.subplots_adjust(left=0.23, right=0.97, top=0.94, bottom=0.13, hspace=0.56)

    left = 0.0
    segment_colors = ["#d7dde6", PALETTE["accent_light"], PALETTE["accent"]]
    for row, color in zip(all_df.itertuples(index=False), segment_colors):
        width = float(row.percent_of_all_manifest_notes)
        ax0.barh([0], [width], left=left, color=color, edgecolor="white", height=0.44)
        label = f"{row.note_medication_density}\n{width:.1f}%"
        if width >= 15:
            ax0.text(left + width / 2, 0, label, ha="center", va="center", fontsize=7.1)
        else:
            ax0.annotate(
                label,
                xy=(left + width / 2, 0.14),
                xytext=(left + width / 2, 0.31),
                ha="center",
                va="bottom",
                fontsize=6.9,
                arrowprops=dict(
                    arrowstyle="-",
                    color="#7b8794",
                    linewidth=0.7,
                    shrinkA=0,
                    shrinkB=0,
                ),
            )
        left += width

    ax0.set_xlim(0, 100)
    ax0.set_yticks([0])
    ax0.set_yticklabels(["All notes"], fontsize=7.8)
    ax0.set_xlabel("Share of all manifest notes (%)", fontsize=8.3)
    ax0.text(
        0,
        -0.54,
        f"Medication-positive notes: 32.7% (n={int(all_df.loc[all_df['note_medication_density'] != '0 meds', 'note_count'].sum()):,})",
        fontsize=6.9,
        ha="left",
        va="top",
        color=PALETTE["neutral"],
    )
    ax0.spines["top"].set_visible(False)
    ax0.spines["right"].set_visible(False)
    ax0.spines["left"].set_visible(False)
    ax0.spines["bottom"].set_color("#c7cdd8")
    ax0.grid(axis="x", color=PALETTE["grid"], linewidth=0.8)
    ax0.set_axisbelow(True)

    bars = ax1.barh(
        df["mention_density_bin_conditioned_on_ge1"],
        df["percent_of_notes_with_ge1_mentions"],
        color=[PALETTE["accent"]] * 4 + [PALETTE["accent_light"]],
        edgecolor="none",
        height=0.56,
    )
    ax1.set_xlabel("Share of medication-positive notes (%)", fontsize=8.3)
    ax1.set_ylabel("Mentions per note", fontsize=8.3)
    ax1.set_xlim(0, 33.5)
    _style_axes(ax1)
    ax1.tick_params(axis="both", labelsize=8.3)
    ax1.invert_yaxis()

    for bar, pct, count in zip(
        bars, df["percent_of_notes_with_ge1_mentions"], df["note_count"]
    ):
        ax1.text(
            bar.get_width() + 0.45,
            bar.get_y() + bar.get_height() / 2,
            f"{pct:.1f}% (n={int(count):,})",
            ha="left",
            va="center",
            fontsize=7.0,
        )

    fig.savefig(output_dir / "rq1_fig_note_density_conditioned.png", dpi=300, bbox_inches="tight")
    plt.close(fig)


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    output_dir = (root / args.output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    slice_df = pd.read_csv((root / args.slice_metrics_csv).resolve())
    ladder_df = pd.read_csv((root / args.normalization_ladder_csv).resolve())
    stage_df = pd.read_csv((root / args.stage_contribution_csv).resolve())
    failure_df = pd.read_csv((root / args.failure_taxonomy_csv).resolve())
    unresolved_df = pd.read_csv((root / args.top_unresolved_csv).resolve())
    note_density_all_df = pd.read_csv((root / args.note_density_all_csv).resolve())
    note_density_df = pd.read_csv((root / args.note_density_conditioned_csv).resolve())

    make_figure2(slice_df, output_dir)
    make_figure3(ladder_df, stage_df, output_dir)
    make_figure4(failure_df, unresolved_df, output_dir)
    make_figure5(note_density_all_df, note_density_df, output_dir)
    print(f"Saved publication figures to {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
