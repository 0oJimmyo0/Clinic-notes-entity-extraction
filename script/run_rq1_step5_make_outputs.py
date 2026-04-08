#!/usr/bin/env python3
"""
RQ1 Step 5: Build poster-ready tables + figures from Step 4 outputs.

Required input:
- rq1_similarity_summary.csv
- rq1_similarity_pairs.csv

Optional input (sensitivity):
- rq1_similarity_summary_high_certainty.csv
- rq1_similarity_pairs_high_certainty.csv

Outputs:
- rq1_table_main.csv / .md
- rq1_table_strict_nonempty.csv / .md
- rq1_table_domain_reliability.csv / .md
- rq1_table_high_certainty.csv / .md (if high-certainty inputs provided)
- rq1_bar_containment_all_pairs.png
- rq1_bar_containment_nonempty.png
- rq1_bar_note_coverage.png
- rq1_bar_domain_reliability.png
- rq1_table_drug_ablation.csv / .md
- rq1_bar_drug_ablation_delta.png
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RQ1 Step 5 poster outputs")
    p.add_argument(
        "--summary-csv",
        default="episode_extraction_results/rq1/rq1_similarity_summary.csv",
        help="Step 4 summary CSV (raw).",
    )
    p.add_argument(
        "--summary-high-csv",
        default="",
        help="Optional Step 4 summary CSV for high-certainty run.",
    )
    p.add_argument(
        "--pairs-csv",
        default="episode_extraction_results/rq1/rq1_similarity_pairs.csv",
        help="Step 4 pairs CSV (required for strict non-empty analysis).",
    )
    p.add_argument(
        "--pairs-high-csv",
        default="",
        help="Optional Step 4 high-certainty pairs CSV.",
    )
    p.add_argument(
        "--output-dir",
        default="episode_extraction_results/rq1",
        help="Output directory for table and figure.",
    )
    p.add_argument(
        "--summary-patha-csv",
        default="",
        help="Optional Step 4 summary CSV for Path A method.",
    )
    p.add_argument(
        "--pairs-patha-csv",
        default="",
        help="Optional Step 4 pairs CSV for Path A method.",
    )
    p.add_argument(
        "--summary-pathab-csv",
        default="",
        help="Optional Step 4 summary CSV for Path A+B method.",
    )
    p.add_argument(
        "--pairs-pathab-csv",
        default="",
        help="Optional Step 4 pairs CSV for Path A+B method.",
    )
    return p.parse_args()


def to_percent(x: float) -> float:
    return round(float(x) * 100.0, 2)


def domain_ordered(df: pd.DataFrame) -> pd.DataFrame:
    order = ["conditions", "drugs", "measurements", "procedures"]
    out = df.copy()
    out["domain"] = pd.Categorical(out["domain"], categories=order, ordered=True)
    return out.sort_values("domain")


def build_nonempty_table(pairs_df: pd.DataFrame, mode: str) -> pd.DataFrame:
    """
    Strict view: compute containment/jaccard only for visits where note has >=1 term
    in that domain.
    """
    domains = ["conditions", "drugs", "measurements", "procedures"]
    rows = []
    n_pairs_total = len(pairs_df)
    for d in domains:
        note_n = f"{d}_note_n"
        cont = f"{d}_containment"
        cont_rel = f"{d}_containment_relaxed"
        jac = f"{d}_jaccard"
        ov = f"{d}_has_overlap"
        ov_rel = f"{d}_has_overlap_relaxed"
        mask = pairs_df[note_n] > 0
        sub = pairs_df[mask]
        rows.append(
            {
                "mode": mode,
                "domain": d,
                "n_pairs_total": n_pairs_total,
                "n_pairs_note_nonempty": int(len(sub)),
                "note_nonempty_coverage_pct": to_percent(len(sub) / n_pairs_total if n_pairs_total else 0),
                "containment_nonempty_pct": to_percent(sub[cont].mean() if len(sub) else 0),
                "containment_nonempty_relaxed_pct": to_percent(sub[cont_rel].mean() if len(sub) else 0),
                "jaccard_nonempty_pct": to_percent(sub[jac].mean() if len(sub) else 0),
                "overlap_nonempty_pct": to_percent(sub[ov].mean() if len(sub) else 0),
                "overlap_nonempty_relaxed_pct": to_percent(sub[ov_rel].mean() if len(sub) else 0),
            }
        )
    return pd.DataFrame(rows)


def build_nonempty_by_window_table(pairs_df: pd.DataFrame, mode: str) -> pd.DataFrame:
    domains = ["conditions", "drugs", "measurements", "procedures"]
    rows = []
    for k in sorted(pairs_df["window_k"].dropna().unique().tolist()):
        subk = pairs_df[pairs_df["window_k"] == k].copy()
        n_pairs_total = len(subk)
        for d in domains:
            note_n = f"{d}_note_n"
            cont = f"{d}_containment"
            cont_rel = f"{d}_containment_relaxed"
            jac = f"{d}_jaccard"
            ov = f"{d}_has_overlap"
            ov_rel = f"{d}_has_overlap_relaxed"
            mask = subk[note_n] > 0
            sub = subk[mask]
            rows.append(
                {
                    "mode": mode,
                    "window_k": int(k),
                    "domain": d,
                    "n_pairs_total": n_pairs_total,
                    "n_pairs_note_nonempty": int(len(sub)),
                    "note_nonempty_coverage_pct": to_percent(len(sub) / n_pairs_total if n_pairs_total else 0),
                    "containment_nonempty_pct": to_percent(sub[cont].mean() if len(sub) else 0),
                    "containment_nonempty_relaxed_pct": to_percent(sub[cont_rel].mean() if len(sub) else 0),
                    "jaccard_nonempty_pct": to_percent(sub[jac].mean() if len(sub) else 0),
                    "overlap_nonempty_pct": to_percent(sub[ov].mean() if len(sub) else 0),
                    "overlap_nonempty_relaxed_pct": to_percent(sub[ov_rel].mean() if len(sub) else 0),
                }
            )
    return pd.DataFrame(rows)


def build_window_sensitivity_table(summary_df: pd.DataFrame, mode: str) -> pd.DataFrame:
    out = summary_df.copy()
    out["mode"] = mode
    out["containment_pct"] = out["mean_containment_note_in_ehr"].map(to_percent)
    out["containment_relaxed_pct"] = out["mean_containment_note_in_ehr_relaxed"].map(to_percent)
    out["overlap_rate_pct"] = out["overlap_rate"].map(to_percent)
    out["overlap_rate_relaxed_pct"] = out["overlap_rate_relaxed"].map(to_percent)
    out["jaccard_pct"] = out["mean_jaccard"].map(to_percent)
    return out[
        [
            "mode",
            "window_k",
            "domain",
            "n_pairs",
            "containment_pct",
            "containment_relaxed_pct",
            "overlap_rate_pct",
            "overlap_rate_relaxed_pct",
            "jaccard_pct",
            "mean_note_terms",
            "mean_ehr_terms",
        ]
    ].copy()


def classify_reliability(score_pct: float, coverage_pct: float) -> str:
    if coverage_pct < 10:
        return "insufficient_coverage"
    if score_pct >= 60:
        return "strong"
    if score_pct >= 35:
        return "moderate"
    if score_pct >= 20:
        return "weak"
    return "very_weak"


def domain_interpretation(domain: str, tier: str) -> str:
    if tier == "insufficient_coverage":
        return "Very low note coverage; interpret domain metrics cautiously."
    if tier in {"strong", "moderate"} and domain in {"conditions", "drugs"}:
        return "Likely meaningful after harmonization; suitable for primary reporting."
    if tier in {"strong", "moderate"} and domain in {"measurements", "procedures"}:
        return "Improved but still sensitive to extraction/ontology granularity."
    if domain in {"measurements", "procedures"}:
        return "Requires stricter curation; keep as supportive evidence."
    return "Usable for trend analysis but still limited for strict concordance."


def build_domain_reliability_table(nonempty_window_df: pd.DataFrame, mode: str) -> pd.DataFrame:
    domains = ["conditions", "drugs", "measurements", "procedures"]
    sub = nonempty_window_df[nonempty_window_df["mode"] == mode].copy()
    rows = []
    for d in domains:
        dom = sub[sub["domain"] == d].sort_values("window_k")
        if dom.empty:
            continue
        k0_row = dom[dom["window_k"] == 0]
        k0_score = float(k0_row["containment_nonempty_relaxed_pct"].iloc[0]) if len(k0_row) else 0.0
        k0_cov = float(k0_row["note_nonempty_coverage_pct"].iloc[0]) if len(k0_row) else 0.0

        best_idx = dom["containment_nonempty_relaxed_pct"].idxmax()
        best_row = dom.loc[best_idx]
        best_k = int(best_row["window_k"])
        best_score = float(best_row["containment_nonempty_relaxed_pct"])
        delta = round(best_score - k0_score, 2)
        tier = classify_reliability(best_score, k0_cov)
        interp = domain_interpretation(d, tier)

        rows.append(
            {
                "mode": mode,
                "domain": d,
                "k0_relaxed_nonempty_pct": round(k0_score, 2),
                "best_k": best_k,
                "best_relaxed_nonempty_pct": round(best_score, 2),
                "delta_from_k0_pct": delta,
                "k0_note_nonempty_coverage_pct": round(k0_cov, 2),
                "reliability_tier": tier,
                "interpretation": interp,
            }
        )
    return pd.DataFrame(rows)


def build_drug_ablation_table(
    baseline_pairs: pd.DataFrame,
    patha_pairs: pd.DataFrame | None,
    pathab_pairs: pd.DataFrame | None,
) -> pd.DataFrame:
    def _drug_metrics(df: pd.DataFrame, label: str) -> dict:
        sub = df[df["window_k"] == 0].copy()
        sub = sub[sub["drugs_note_n"] > 0].copy()
        if len(sub) == 0:
            return {
                "method": label,
                "n_pairs_note_nonempty": 0,
                "drug_containment_relaxed_pct": 0.0,
                "drug_overlap_relaxed_pct": 0.0,
                "drug_jaccard_pct": 0.0,
            }
        return {
            "method": label,
            "n_pairs_note_nonempty": int(len(sub)),
            "drug_containment_relaxed_pct": to_percent(sub["drugs_containment_relaxed"].mean()),
            "drug_overlap_relaxed_pct": to_percent(sub["drugs_has_overlap_relaxed"].mean()),
            "drug_jaccard_pct": to_percent(sub["drugs_jaccard"].mean()),
        }

    rows = [_drug_metrics(baseline_pairs, "baseline")]
    if patha_pairs is not None:
        rows.append(_drug_metrics(patha_pairs, "path_a"))
    if pathab_pairs is not None:
        rows.append(_drug_metrics(pathab_pairs, "path_ab"))
    out = pd.DataFrame(rows)
    base_cont = float(out[out["method"] == "baseline"]["drug_containment_relaxed_pct"].iloc[0]) if len(out) else 0.0
    base_ov = float(out[out["method"] == "baseline"]["drug_overlap_relaxed_pct"].iloc[0]) if len(out) else 0.0
    out["delta_containment_vs_baseline_pct"] = out["drug_containment_relaxed_pct"] - base_cont
    out["delta_overlap_vs_baseline_pct"] = out["drug_overlap_relaxed_pct"] - base_ov
    return out


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]

    summary_path = (root / args.summary_csv).resolve()
    pairs_path = (root / args.pairs_csv).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    if not summary_path.exists():
        raise FileNotFoundError(f"Missing summary file: {summary_path}")
    if not pairs_path.exists():
        raise FileNotFoundError(f"Missing pairs file: {pairs_path}")

    raw_all_windows = pd.read_csv(summary_path)
    raw = raw_all_windows[raw_all_windows["window_k"] == 0].copy()
    raw["mode"] = "raw"
    raw_pairs_all = pd.read_csv(pairs_path)
    raw_pairs = raw_pairs_all[raw_pairs_all["window_k"] == 0].copy()

    frames = [raw]
    window_frames = [build_window_sensitivity_table(raw_all_windows, mode="raw")]
    strict_frames = [build_nonempty_table(raw_pairs, mode="raw")]
    nonempty_window_frames = [build_nonempty_by_window_table(raw_pairs_all, mode="raw")]
    if args.summary_high_csv:
        high_path = (root / args.summary_high_csv).resolve()
        high_pairs_path = (root / args.pairs_high_csv).resolve() if args.pairs_high_csv else None
        if high_path.exists() and high_pairs_path and high_pairs_path.exists():
            high = pd.read_csv(high_path)
            high_all_windows = high.copy()
            high = high_all_windows[high_all_windows["window_k"] == 0].copy()
            high["mode"] = "high_certainty"
            frames.append(high)
            window_frames.append(build_window_sensitivity_table(high_all_windows, mode="high_certainty"))
            hp = pd.read_csv(high_pairs_path)
            hp_all = hp.copy()
            hp = hp[hp["window_k"] == 0].copy()
            strict_frames.append(build_nonempty_table(hp, mode="high_certainty"))
            nonempty_window_frames.append(build_nonempty_by_window_table(hp_all, mode="high_certainty"))

    # Optional method variants for ablation (Path A / Path A+B)
    patha_pairs = None
    pathab_pairs = None
    if args.summary_patha_csv and args.pairs_patha_csv:
        patha_summary_path = (root / args.summary_patha_csv).resolve()
        patha_pairs_path = (root / args.pairs_patha_csv).resolve()
        if patha_summary_path.exists() and patha_pairs_path.exists():
            pa = pd.read_csv(patha_summary_path)
            pa["mode"] = "path_a"
            frames.append(pa[pa["window_k"] == 0].copy())
            window_frames.append(build_window_sensitivity_table(pa, mode="path_a"))
            patha_pairs = pd.read_csv(patha_pairs_path)
            strict_frames.append(build_nonempty_table(patha_pairs[patha_pairs["window_k"] == 0].copy(), mode="path_a"))
            nonempty_window_frames.append(build_nonempty_by_window_table(patha_pairs, mode="path_a"))
    if args.summary_pathab_csv and args.pairs_pathab_csv:
        pathab_summary_path = (root / args.summary_pathab_csv).resolve()
        pathab_pairs_path = (root / args.pairs_pathab_csv).resolve()
        if pathab_summary_path.exists() and pathab_pairs_path.exists():
            pb = pd.read_csv(pathab_summary_path)
            pb["mode"] = "path_ab"
            frames.append(pb[pb["window_k"] == 0].copy())
            window_frames.append(build_window_sensitivity_table(pb, mode="path_ab"))
            pathab_pairs = pd.read_csv(pathab_pairs_path)
            strict_frames.append(build_nonempty_table(pathab_pairs[pathab_pairs["window_k"] == 0].copy(), mode="path_ab"))
            nonempty_window_frames.append(build_nonempty_by_window_table(pathab_pairs, mode="path_ab"))

    full = pd.concat(frames, ignore_index=True)
    windows_full = pd.concat(window_frames, ignore_index=True)
    strict_full = pd.concat(strict_frames, ignore_index=True)
    nonempty_window_full = pd.concat(nonempty_window_frames, ignore_index=True)

    # Main poster table (all pairs)
    table = full[
        [
            "mode",
            "domain",
            "n_pairs",
            "mean_containment_note_in_ehr",
            "mean_containment_note_in_ehr_relaxed",
            "mean_jaccard",
            "overlap_rate",
            "overlap_rate_relaxed",
            "mean_note_terms",
            "mean_ehr_terms",
        ]
    ].copy()
    table["containment_pct"] = table["mean_containment_note_in_ehr"].map(to_percent)
    table["containment_relaxed_pct"] = table["mean_containment_note_in_ehr_relaxed"].map(to_percent)
    table["jaccard_pct"] = table["mean_jaccard"].map(to_percent)
    table["overlap_rate_pct"] = table["overlap_rate"].map(to_percent)
    table["overlap_rate_relaxed_pct"] = table["overlap_rate_relaxed"].map(to_percent)

    table_out = table[
        [
            "mode",
            "domain",
            "n_pairs",
            "containment_pct",
            "containment_relaxed_pct",
            "jaccard_pct",
            "overlap_rate_pct",
            "overlap_rate_relaxed_pct",
            "mean_note_terms",
            "mean_ehr_terms",
        ]
    ].sort_values(["mode", "domain"])

    csv_path = out_dir / "rq1_table_main.csv"
    md_path = out_dir / "rq1_table_main.md"
    table_out.to_csv(csv_path, index=False)
    md_path.write_text(table_out.to_markdown(index=False), encoding="utf-8")

    # Strict non-empty table
    strict_out = strict_full.sort_values(["mode", "domain"]).copy()
    strict_csv = out_dir / "rq1_table_strict_nonempty.csv"
    strict_md = out_dir / "rq1_table_strict_nonempty.md"
    strict_out.to_csv(strict_csv, index=False)
    strict_md.write_text(strict_out.to_markdown(index=False), encoding="utf-8")

    # Window sensitivity table (Phase 2)
    windows_out = windows_full.sort_values(["mode", "domain", "window_k"]).copy()
    windows_csv = out_dir / "rq1_table_window_sensitivity.csv"
    windows_md = out_dir / "rq1_table_window_sensitivity.md"
    windows_out.to_csv(windows_csv, index=False)
    windows_md.write_text(windows_out.to_markdown(index=False), encoding="utf-8")

    # Phase 3: domain-specific reliability interpretation table
    rel_frames = [build_domain_reliability_table(nonempty_window_full, mode="raw")]
    if "high_certainty" in nonempty_window_full["mode"].astype(str).unique():
        rel_frames.append(build_domain_reliability_table(nonempty_window_full, mode="high_certainty"))
    reliability_out = pd.concat(rel_frames, ignore_index=True) if rel_frames else pd.DataFrame()
    reliability_out = reliability_out.sort_values(["mode", "domain"]).reset_index(drop=True)
    rel_csv = out_dir / "rq1_table_domain_reliability.csv"
    rel_md = out_dir / "rq1_table_domain_reliability.md"
    reliability_out.to_csv(rel_csv, index=False)
    rel_md.write_text(reliability_out.to_markdown(index=False), encoding="utf-8")

    # Drug ablation outputs: baseline vs Path A vs Path A+B
    drug_ablation = build_drug_ablation_table(
        baseline_pairs=raw_pairs_all,
        patha_pairs=patha_pairs,
        pathab_pairs=pathab_pairs,
    )
    drug_ablation_csv = out_dir / "rq1_table_drug_ablation.csv"
    drug_ablation_md = out_dir / "rq1_table_drug_ablation.md"
    drug_ablation.to_csv(drug_ablation_csv, index=False)
    drug_ablation_md.write_text(drug_ablation.to_markdown(index=False), encoding="utf-8")

    # Optional high-certainty-only table view
    high_only = table_out[table_out["mode"] == "high_certainty"].copy()
    if len(high_only):
        high_csv = out_dir / "rq1_table_high_certainty.csv"
        high_md = out_dir / "rq1_table_high_certainty.md"
        high_only.to_csv(high_csv, index=False)
        high_md.write_text(high_only.to_markdown(index=False), encoding="utf-8")

    # Figure 1 (relabelled): containment across all visit pairs
    fig_all = domain_ordered(table_out[table_out["mode"] == "raw"].copy())

    plt.figure(figsize=(8, 4.5))
    bars = plt.bar(fig_all["domain"].astype(str), fig_all["containment_pct"])
    plt.ylabel("Containment (%)")
    plt.xlabel("Domain")
    plt.title("RQ1 k=0: Note-in-EHR containment (ALL pairs; empty-note visits counted as 100%)")
    plt.ylim(0, 100)
    for b, v in zip(bars, fig_all["containment_pct"]):
        plt.text(b.get_x() + b.get_width() / 2, min(v + 1.5, 99), f"{v:.1f}%", ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    fig_all_path = out_dir / "rq1_bar_containment_all_pairs.png"
    plt.savefig(fig_all_path, dpi=180)
    plt.close()

    # Figure 2: strict containment on non-empty note visits only
    fig_strict = domain_ordered(strict_out[strict_out["mode"] == "raw"].copy())
    plt.figure(figsize=(8, 4.5))
    bars = plt.bar(fig_strict["domain"].astype(str), fig_strict["containment_nonempty_relaxed_pct"])
    plt.ylabel("Containment (%)")
    plt.xlabel("Domain")
    plt.title("RQ1 k=0: Strict RELAXED containment (NON-EMPTY note visits only)")
    plt.ylim(0, 100)
    for b, v in zip(bars, fig_strict["containment_nonempty_relaxed_pct"]):
        plt.text(b.get_x() + b.get_width() / 2, min(v + 1.5, 99), f"{v:.1f}%", ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    fig_strict_path = out_dir / "rq1_bar_containment_nonempty.png"
    plt.savefig(fig_strict_path, dpi=180)
    plt.close()

    # Figure 3: coverage panel (% visits where note has >=1 term)
    plt.figure(figsize=(8, 4.5))
    bars = plt.bar(fig_strict["domain"].astype(str), fig_strict["note_nonempty_coverage_pct"])
    plt.ylabel("Coverage (%)")
    plt.xlabel("Domain")
    plt.title("RQ1 k=0: Note non-empty coverage by domain")
    plt.ylim(0, 100)
    for b, v in zip(bars, fig_strict["note_nonempty_coverage_pct"]):
        plt.text(b.get_x() + b.get_width() / 2, min(v + 1.5, 99), f"{v:.1f}%", ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    fig_cov_path = out_dir / "rq1_bar_note_coverage.png"
    plt.savefig(fig_cov_path, dpi=180)
    plt.close()

    # Figure 4 (Phase 2): relaxed containment vs temporal window (raw mode)
    win_plot = windows_out[windows_out["mode"] == "raw"].copy()
    if len(win_plot):
        plt.figure(figsize=(8.5, 5.0))
        for d in ["conditions", "drugs", "measurements", "procedures"]:
            sub = win_plot[win_plot["domain"] == d].sort_values("window_k")
            if len(sub):
                plt.plot(
                    sub["window_k"],
                    sub["containment_relaxed_pct"],
                    marker="o",
                    label=d,
                )
        plt.xlabel("Temporal window k (visit rank +/-k)")
        plt.ylabel("Relaxed containment (%)")
        plt.title("RQ1 temporal tolerance: relaxed containment by window size")
        plt.ylim(0, 100)
        plt.xticks(sorted(win_plot["window_k"].unique()))
        plt.legend()
        plt.tight_layout()
        fig_win_path = out_dir / "rq1_line_window_containment_relaxed.png"
        plt.savefig(fig_win_path, dpi=180)
        plt.close()
    else:
        fig_win_path = out_dir / "rq1_line_window_containment_relaxed.png"

    # Figure 5 (Phase 3): best relaxed non-empty containment by domain
    fig_rel_path = out_dir / "rq1_bar_domain_reliability.png"
    rel_plot = reliability_out[reliability_out["mode"] == "raw"].copy()
    if len(rel_plot):
        rel_plot = domain_ordered(rel_plot)
        plt.figure(figsize=(8.2, 4.8))
        bars = plt.bar(rel_plot["domain"].astype(str), rel_plot["best_relaxed_nonempty_pct"])
        plt.ylabel("Best relaxed containment (%)")
        plt.xlabel("Domain")
        plt.title("RQ1 domain reliability (best k; non-empty note visits)")
        plt.ylim(0, 100)
        for b, v in zip(bars, rel_plot["best_relaxed_nonempty_pct"]):
            plt.text(b.get_x() + b.get_width() / 2, min(v + 1.5, 99), f"{v:.1f}%", ha="center", va="bottom", fontsize=9)
        plt.tight_layout()
        plt.savefig(fig_rel_path, dpi=180)
        plt.close()

    # Figure 6: drug ablation delta vs baseline
    fig_ablation_path = out_dir / "rq1_bar_drug_ablation_delta.png"
    abl = drug_ablation[drug_ablation["method"] != "baseline"].copy()
    if len(abl):
        plt.figure(figsize=(7.4, 4.3))
        bars = plt.bar(abl["method"], abl["delta_containment_vs_baseline_pct"])
        plt.ylabel("Delta containment (%)")
        plt.xlabel("Method")
        plt.title("Drug relaxed containment gain vs baseline (k=0, non-empty note visits)")
        ymin = min(-1.0, float(abl["delta_containment_vs_baseline_pct"].min()) - 1.0)
        ymax = max(1.0, float(abl["delta_containment_vs_baseline_pct"].max()) + 1.0)
        plt.ylim(ymin, ymax)
        for b, v in zip(bars, abl["delta_containment_vs_baseline_pct"]):
            plt.text(
                b.get_x() + b.get_width() / 2,
                v + (0.2 if v >= 0 else -0.3),
                f"{v:.2f}",
                ha="center",
                va="bottom" if v >= 0 else "top",
                fontsize=9,
            )
        plt.tight_layout()
        plt.savefig(fig_ablation_path, dpi=180)
        plt.close()

    print(f"Saved table CSV: {csv_path}")
    print(f"Saved table MD:  {md_path}")
    print(f"Saved strict CSV: {strict_csv}")
    print(f"Saved strict MD:  {strict_md}")
    print(f"Saved windows CSV: {windows_csv}")
    print(f"Saved windows MD:  {windows_md}")
    print(f"Saved reliability CSV: {rel_csv}")
    print(f"Saved reliability MD:  {rel_md}")
    print(f"Saved drug ablation CSV: {drug_ablation_csv}")
    print(f"Saved drug ablation MD:  {drug_ablation_md}")
    print(f"Saved figure:     {fig_all_path}")
    print(f"Saved figure:     {fig_strict_path}")
    print(f"Saved figure:     {fig_cov_path}")
    print(f"Saved figure:     {fig_win_path}")
    print(f"Saved figure:     {fig_rel_path}")
    print(f"Saved figure:     {fig_ablation_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

