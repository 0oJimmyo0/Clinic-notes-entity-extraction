#!/usr/bin/env python3
"""
Shared utilities for adjudication-first RQ1 evaluation scripts.
"""

from __future__ import annotations

import ast
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import pandas as pd


STATUS_VALUES = [
    "active_current",
    "newly_started_or_prescribed",
    "planned_or_considering",
    "discontinued_or_stopped",
    "held_or_paused",
    "historical_prior",
    "reference_only_or_discussion_only",
    "unclear",
]

COMPARE_VALUES = ["yes", "no", "uncertain"]

GROUPED_STATUS_MAP = {
    "active_current": "current_or_intended",
    "newly_started_or_prescribed": "current_or_intended",
    "planned_or_considering": "current_or_intended",
    "discontinued_or_stopped": "not_current",
    "held_or_paused": "not_current",
    "historical_prior": "not_current",
    "reference_only_or_discussion_only": "not_current",
    "unclear": "unclear",
}


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
        t = str(v).strip()
        if t:
            out.append(t)
    return out


def normalize_join_text(text: str) -> str:
    return " ".join(str(text or "").strip().lower().split())


def stable_id(*parts: object, prefix: str = "") -> str:
    payload = "||".join(str(p) for p in parts)
    digest = hashlib.sha1(payload.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}{digest}" if prefix else digest


def build_span_local_id(row: Dict[str, object]) -> str:
    return stable_id(
        row.get("person_id", ""),
        row.get("visit_id", row.get("visit_occurrence_id", "")),
        row.get("note_id", ""),
        row.get("category", ""),
        row.get("match_text", ""),
        row.get("original_position", ""),
        row.get("span_text", ""),
        prefix="span_",
    )


def grouped_status(status: str) -> str:
    return GROUPED_STATUS_MAP.get(str(status or "").strip().lower(), "unclear")


def note_length_bin(x: float) -> str:
    try:
        v = float(x)
    except Exception:
        return "unknown"
    if v < 250:
        return "lt_250"
    if v < 750:
        return "250_749"
    if v < 1500:
        return "750_1499"
    return "ge_1500"


def candidate_density_bin(x: float) -> str:
    try:
        v = float(x)
    except Exception:
        return "unknown"
    if v <= 0:
        return "0"
    if v == 1:
        return "1"
    if v <= 3:
        return "2_3"
    if v <= 7:
        return "4_7"
    return "8_plus"


def write_run_summary(path: Path, payload: Dict) -> None:
    out = dict(payload)
    out["generated_at_utc"] = datetime.now(timezone.utc).isoformat()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(out, indent=2), encoding="utf-8")


def load_note_subset(
    note_ids: Sequence[str],
    notes_parquet: Path | None = None,
    notes_dir: Path | None = None,
    glob_pattern: str = "episode_notes_chunk*.parquet",
    note_text_col_candidates: Sequence[str] = ("note_text_full", "full_note_text", "note_text", "text"),
) -> pd.DataFrame:
    wanted = {str(x).strip() for x in note_ids if str(x).strip()}
    if not wanted:
        return pd.DataFrame(columns=["person_id", "visit_occurrence_id", "note_id", "note_title", "note_date", "note_datetime", "note_text"])

    def _resolve_note_text_col(df: pd.DataFrame) -> str:
        for c in note_text_col_candidates:
            if c in df.columns:
                return c
        raise ValueError(f"No note text column found. Tried: {list(note_text_col_candidates)}")

    frames: List[pd.DataFrame] = []
    if notes_parquet is not None and notes_parquet.exists():
        df = pd.read_parquet(notes_parquet)
        if "note_id" not in df.columns:
            raise ValueError(f"notes parquet missing note_id: {notes_parquet}")
        df = df[df["note_id"].astype(str).isin(wanted)].copy()
        if len(df):
            if "note_text" not in df.columns:
                note_col = _resolve_note_text_col(df)
                df["note_text"] = df[note_col]
            frames.append(df)
    elif notes_dir is not None:
        files = sorted(notes_dir.glob(glob_pattern))
        if not files:
            raise FileNotFoundError(f"No note chunks matched: {notes_dir / glob_pattern}")
        for p in files:
            df = pd.read_parquet(p)
            if "note_id" not in df.columns:
                continue
            hit = df[df["note_id"].astype(str).isin(wanted)].copy()
            if hit.empty:
                continue
            note_col = _resolve_note_text_col(hit)
            keep_cols = [
                c
                for c in ["TaskIDNumber", "person_id", "visit_occurrence_id", "note_id", "note_title", "note_date", "note_datetime"]
                if c in hit.columns
            ]
            chunk = hit[keep_cols].copy()
            chunk["note_text"] = hit[note_col]
            frames.append(chunk)
    else:
        raise ValueError("Either notes_parquet or notes_dir must be provided.")

    if not frames:
        return pd.DataFrame(columns=["person_id", "visit_occurrence_id", "note_id", "note_title", "note_date", "note_datetime", "note_text"])

    out = pd.concat(frames, ignore_index=True)
    out["note_id"] = out["note_id"].astype(str)
    out = out.drop_duplicates(subset=["note_id"], keep="last").copy()
    return out


def csv_rows(path: Path) -> List[Dict[str, str]]:
    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        return list(csv.DictReader(f))
