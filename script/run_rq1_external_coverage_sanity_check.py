#!/usr/bin/env python3
"""
Optional lightweight sanity check against off-the-shelf biomedical NLP outputs.

This is intentionally not a leaderboard script. It summarizes whether generic
tool outputs cover adjudicated treatment-context medication mentions.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, List

import pandas as pd

from rq1_adjudication_utils import write_run_summary
from rq1_drug_linking import normalize_drug_text


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Summarize external tool mention coverage against adjudicated mentions.")
    p.add_argument(
        "--adjudicated-mentions-csv",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/adjudicated/rq1_adjudicated_mentions.csv",
    )
    p.add_argument(
        "--tool-output-csvs",
        default="",
        help=(
            "Comma-separated comparator CSVs. Each CSV should contain note_id and either "
            "mention_text/predicted_text/raw_mention_text."
        ),
    )
    p.add_argument(
        "--output-dir",
        default="../episode_extraction_results/clinic_like_20k_30k/rq1/external_coverage_sanity_check",
    )
    return p.parse_args()


def _normalize(df: pd.DataFrame, col: str) -> None:
    if col in df.columns:
        df[col] = df[col].fillna("").astype(str).str.strip()


def _pick_text_col(df: pd.DataFrame) -> str:
    for col in ["mention_text", "predicted_text", "raw_mention_text", "text"]:
        if col in df.columns:
            return col
    raise ValueError("Comparator CSV must include one of: mention_text, predicted_text, raw_mention_text, text")


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[1]
    adjud_path = (root / args.adjudicated_mentions_csv).resolve()
    out_dir = (root / args.output_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    adjud = pd.read_csv(adjud_path).fillna("")
    for col in ["adjudication_unit_id", "note_id", "raw_mention_text", "context_text"]:
        _normalize(adjud, col)
    adjud = adjud[adjud["raw_mention_text"].astype(str).str.strip() != ""].copy()
    adjud["gold_norm"] = adjud["raw_mention_text"].map(normalize_drug_text)
    adjud = adjud[adjud["gold_norm"] != ""].copy()

    comparator_paths = [(root / x.strip()).resolve() for x in str(args.tool_output_csvs).split(",") if x.strip()]
    if not comparator_paths:
        summary = {
            "inputs": {"adjudicated_mentions_csv": str(adjud_path)},
            "counts": {"gold_mentions": int(len(adjud)), "comparator_tools": 0},
            "outputs": {},
            "note": "No comparator CSVs provided. Script created only a placeholder summary.",
        }
        write_run_summary(out_dir / "rq1_external_coverage_summary.json", summary)
        print("No comparator CSVs provided; wrote placeholder summary only.")
        return 0

    all_summary: List[Dict[str, object]] = []
    all_examples: List[pd.DataFrame] = []
    for tool_path in comparator_paths:
        tool = pd.read_csv(tool_path).fillna("")
        _normalize(tool, "note_id")
        text_col = _pick_text_col(tool)
        tool["tool_norm"] = tool[text_col].map(normalize_drug_text)
        tool = tool[tool["tool_norm"] != ""].copy()
        tool_name = tool["tool_name"].iloc[0] if "tool_name" in tool.columns and len(tool) else tool_path.stem

        matched = adjud.merge(
            tool[["note_id", "tool_norm"]].drop_duplicates(),
            left_on=["note_id", "gold_norm"],
            right_on=["note_id", "tool_norm"],
            how="left",
            indicator=True,
        )
        matched["covered"] = matched["_merge"].eq("both")

        covered_n = int(matched["covered"].sum())
        total = int(len(matched))
        all_summary.append(
            {
                "tool_name": tool_name,
                "gold_mentions": total,
                "covered_mentions": covered_n,
                "coverage_rate": round(covered_n / total if total else 0.0, 6),
                "unique_tool_mentions": int(tool[["note_id", "tool_norm"]].drop_duplicates().shape[0]),
            }
        )

        missed = matched[~matched["covered"]][
            [
                "adjudication_unit_id",
                "note_id",
                "raw_mention_text",
                "gold_norm",
                "context_text",
            ]
        ].copy()
        missed.insert(0, "tool_name", tool_name)
        missed["context_text"] = missed["context_text"].astype(str).str.slice(0, 300)
        all_examples.append(missed.head(200))

    summary_df = pd.DataFrame(all_summary).sort_values("coverage_rate", ascending=False)
    examples_df = pd.concat(all_examples, ignore_index=True) if all_examples else pd.DataFrame()

    outputs = {
        "summary_csv": out_dir / "rq1_external_coverage_summary.csv",
        "missed_examples_csv": out_dir / "rq1_external_missed_context_mentions.csv",
        "summary_json": out_dir / "rq1_external_coverage_summary.json",
    }
    summary_df.to_csv(outputs["summary_csv"], index=False)
    examples_df.to_csv(outputs["missed_examples_csv"], index=False)
    write_run_summary(
        outputs["summary_json"],
        {
            "inputs": {
                "adjudicated_mentions_csv": str(adjud_path),
                "tool_output_csvs": [str(p) for p in comparator_paths],
            },
            "counts": {
                "gold_mentions": int(len(adjud)),
                "comparator_tools": int(len(comparator_paths)),
            },
            "outputs": {k: str(v) for k, v in outputs.items()},
        },
    )

    print(f"Saved external coverage sanity check outputs to: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
