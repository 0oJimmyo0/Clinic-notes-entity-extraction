#!/usr/bin/env python3
"""
Overnight Stage-1 runner: extract candidate spans from episode note chunks.

Key features:
- Sequential chunk processing (chunk000 -> chunk019 or all available chunks)
- Resume-friendly: skips finished chunks unless --force
- Per-chunk CSV output: <chunk_name>_candidates.csv
- Optional combined output at end
- Lightweight state file with elapsed time and failures

Example:
  nohup python "script/run_candidates_overnight.py" \
    --chunk-dir "episode_notes" \
    --output-dir "episode_extraction_results" \
    --chunk-prefix "episode_notes_chunk" \
    --combine \
    > "logs/candidates_overnight.log" 2>&1 &
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


# ------------------------------
# Lexicon loading
# ------------------------------


def _normalize_term(term: str) -> str:
    return re.sub(r"\s+", " ", str(term).strip().lower())


def _dedupe_terms(terms: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for t in terms:
        x = _normalize_term(t)
        if not x or x in seen:
            continue
        seen.add(x)
        out.append(x)
    return out


def _load_term_list(path: Path) -> List[str]:
    suffix = path.suffix.lower()
    terms: List[str] = []

    if suffix == ".txt":
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            x = _normalize_term(line)
            if x:
                terms.append(x)
        return _dedupe_terms(terms)

    if suffix != ".csv":
        return []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames:
            cols = {c.lower(): c for c in reader.fieldnames}
            if "term" in cols:
                term_col = cols["term"]
                for row in reader:
                    x = _normalize_term(row.get(term_col, ""))
                    if x:
                        terms.append(x)
                return _dedupe_terms(terms)

    # Fallback for single-column CSV without header
    with path.open("r", encoding="utf-8", newline="") as f:
        reader2 = csv.reader(f)
        for row in reader2:
            if row:
                x = _normalize_term(row[0])
                if x:
                    terms.append(x)
    return _dedupe_terms(terms)


def discover_lexicons(lexicon_dir: Path) -> Dict:
    cfg = {
        "candidate_patterns": {
            "treatment_actions": {},
            "discontinuation_reasons": {},
            "treatment_context": {},
        },
        "ehr_entities": {},
    }

    files = sorted(lexicon_dir.glob("*.csv")) + sorted(lexicon_dir.glob("*.txt"))
    for f in files:
        stem = f.stem
        if stem.startswith("candidate_treatment_actions__"):
            cfg["candidate_patterns"]["treatment_actions"][stem[29:]] = _load_term_list(f)
        elif stem.startswith("candidate_discontinuation_reasons__"):
            cfg["candidate_patterns"]["discontinuation_reasons"][stem[35:]] = _load_term_list(f)
        elif stem.startswith("candidate_treatment_context__"):
            cfg["candidate_patterns"]["treatment_context"][stem[29:]] = _load_term_list(f)
        elif stem.startswith("ehr_entities__"):
            cfg["ehr_entities"][stem[14:]] = _load_term_list(f)

    # Minimal fallbacks
    for cat, fb in [
        ("start", ["start"]),
        ("stop", ["stop"]),
        ("hold", ["hold"]),
        ("dose_change", ["dose reduced"]),
    ]:
        cfg["candidate_patterns"]["treatment_actions"].setdefault(cat, fb)
    for cat, fb in [("toxicity", ["toxicity"]), ("progression", ["progression"])]:
        cfg["candidate_patterns"]["discontinuation_reasons"].setdefault(cat, fb)
    cfg["candidate_patterns"]["treatment_context"].setdefault("regimen", ["regimen"])

    return cfg


def _build_phrase_pattern(terms: Iterable[str]) -> Optional[re.Pattern]:
    escaped = sorted({re.escape(t) for t in terms if t}, key=len, reverse=True)
    if not escaped:
        return None
    return re.compile(rf"(?<!\w)(?:{'|'.join(escaped)})(?!\w)", re.IGNORECASE)


def build_candidate_patterns(lexicon_cfg: Dict) -> List[Tuple[re.Pattern, str]]:
    out: List[Tuple[re.Pattern, str]] = []
    action_dict = lexicon_cfg["candidate_patterns"]["treatment_actions"]
    for key in ("start", "stop", "hold"):
        p = _build_phrase_pattern(action_dict.get(key, []))
        if p:
            out.append((p, "treatment_action"))
    p = _build_phrase_pattern(action_dict.get("dose_change", []))
    if p:
        out.append((p, "dose_change"))

    for reason, terms in lexicon_cfg["candidate_patterns"]["discontinuation_reasons"].items():
        p = _build_phrase_pattern(terms)
        if p:
            out.append((p, reason))
    for ctx, terms in lexicon_cfg["candidate_patterns"]["treatment_context"].items():
        p = _build_phrase_pattern(terms)
        if p:
            out.append((p, ctx))

    return out


# ------------------------------
# Candidate extraction
# ------------------------------


def sentence_bounds(text: str) -> List[Tuple[int, int]]:
    """
    Lightweight sentence splitter with offsets.
    Good enough for candidate windows and much faster than full NLP in stage 1.
    """
    bounds: List[Tuple[int, int]] = []
    start = 0
    for m in re.finditer(r"[.!?]\s+|\n+", text):
        end = m.end()
        if end > start:
            bounds.append((start, end))
        start = end
    if start < len(text):
        bounds.append((start, len(text)))
    return bounds if bounds else [(0, len(text))]


def extract_candidate_spans(
    note_text: str,
    candidate_patterns: List[Tuple[re.Pattern, str]],
    target_drug: Optional[str] = None,
) -> List[Dict]:
    if not isinstance(note_text, str) or len(note_text.strip()) < 20:
        return []

    patterns = list(candidate_patterns)
    if target_drug:
        p = re.compile(rf"(?<!\w){re.escape(target_drug.lower())}(?!\w)", re.IGNORECASE)
        patterns.append((p, "target_drug_mention"))

    all_matches = []
    for pattern, category in patterns:
        for match in pattern.finditer(note_text):
            all_matches.append(
                {
                    "start": match.start(),
                    "end": match.end(),
                    "match_text": note_text[match.start() : match.end()],
                    "category": category,
                }
            )

    all_matches.sort(key=lambda x: (x["start"], -x["end"]))

    # Keep left-most non-overlapping match window
    filtered = []
    for m in all_matches:
        if not filtered or m["start"] >= filtered[-1]["end"]:
            filtered.append(m)

    sent = sentence_bounds(note_text)
    out = []
    for m in filtered:
        start_pos = m["start"]
        end_pos = m["end"]

        sent_idx = None
        for i, (s0, s1) in enumerate(sent):
            if s0 <= start_pos < s1:
                sent_idx = i
                break

        if sent_idx is not None:
            span_start, span_end = sent[sent_idx]
        else:
            span_start = max(0, start_pos - 220)
            span_end = min(len(note_text), end_pos + 220)

        span_text = re.sub(r"\s+", " ", note_text[span_start:span_end].strip())
        if len(span_text) <= 15:
            continue

        out.append(
            {
                "span_text": span_text,
                "category": m["category"],
                "match_text": m["match_text"],
                "span_start": span_start,
                "span_end": span_end,
                "original_position": start_pos,
            }
        )

    # Deduplicate by span_text + category
    seen = set()
    unique_out = []
    for x in out:
        key = (x["span_text"].strip(), x["category"])
        if key not in seen:
            seen.add(key)
            unique_out.append(x)
    return unique_out


def extract_candidates_from_note(
    note_text: str,
    note_date: str,
    note_id: str,
    visit_id: str,
    note_title: str,
    candidate_patterns: List[Tuple[re.Pattern, str]],
    target_drug: Optional[str] = None,
) -> List[Dict]:
    cands = extract_candidate_spans(
        note_text=note_text,
        candidate_patterns=candidate_patterns,
        target_drug=target_drug,
    )
    for c in cands:
        c["note_date"] = note_date
        if note_id:
            c["note_id"] = note_id
        if visit_id:
            c["visit_id"] = visit_id
        if note_title:
            c["note_title"] = note_title
    return cands


def process_chunk(
    chunk_file: Path,
    candidate_patterns: List[Tuple[re.Pattern, str]],
    max_candidates_per_patient: int = 40,
    note_text_col_candidates: Optional[List[str]] = None,
    target_drugs_dict: Optional[Dict] = None,
) -> pd.DataFrame:
    t0 = time.time()
    print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Processing {chunk_file.name}")
    df = pd.read_parquet(chunk_file)
    note_text_col_candidates = note_text_col_candidates or ["note_text"]

    note_text_col = None
    for c in note_text_col_candidates:
        if c in df.columns:
            note_text_col = c
            break
    if note_text_col is None:
        raise ValueError(
            f"No note text column found in {chunk_file.name}; tried {note_text_col_candidates}"
        )

    df["person_id"] = pd.to_numeric(df["person_id"], errors="coerce")
    df["note_text_scan"] = df[note_text_col].fillna("").astype(str)
    df = df[df["person_id"].notna() & (df["note_text_scan"].str.strip() != "")]
    if df.empty:
        print("  -> empty chunk after filtering")
        return pd.DataFrame()

    note_len = df["note_text_scan"].str.len()
    max_len = int(note_len.max()) if len(note_len) else 0
    p50 = float(note_len.quantile(0.5)) if len(note_len) else 0.0
    p90 = float(note_len.quantile(0.9)) if len(note_len) else 0.0
    pct_eq_max = float((note_len == max_len).mean() * 100.0) if len(note_len) else 0.0
    print(
        f"  Note text column: {note_text_col} | len p50={p50:.0f} p90={p90:.0f} max={max_len} "
        f"| pct_len==max={pct_eq_max:.1f}%"
    )
    if max_len == 2000 and pct_eq_max >= 10.0:
        print(
            "  WARNING: strong 2000-char hard-cap signature detected; full-note scanning may be limited by upstream truncation."
        )

    grouped = df.groupby("person_id", sort=False)
    print(f"  Notes: {len(df):,} | Patients: {grouped.ngroups:,}")

    all_candidates: List[Dict] = []
    for idx, (pid, patient_notes) in enumerate(grouped, start=1):
        if idx % 25 == 0:
            elapsed = time.time() - t0
            rate = idx / elapsed if elapsed > 0 else 0.0
            print(f"    patient {idx}/{grouped.ngroups} ({rate:.2f} patients/sec)")

        target_drug = target_drugs_dict.get(pid) if target_drugs_dict else None
        patient_candidates: List[Dict] = []

        for row in patient_notes.itertuples(index=False):
            note_text = getattr(row, "note_text_scan", "")
            note_date = getattr(row, "note_date", "")
            note_id = (
                getattr(row, "note_id", None)
                or getattr(row, "note_occurrence_id", None)
                or f"note_{pid}_{note_date}"
            )
            visit_id = getattr(row, "visit_id", None) or getattr(row, "visit_occurrence_id", None)
            note_title = getattr(row, "note_title", "")

            cands = extract_candidates_from_note(
                note_text=note_text,
                note_date=note_date,
                note_id=note_id,
                visit_id=visit_id,
                note_title=note_title,
                candidate_patterns=candidate_patterns,
                target_drug=target_drug,
            )
            patient_candidates.extend(cands)

        if len(patient_candidates) > max_candidates_per_patient:
            patient_candidates = patient_candidates[:max_candidates_per_patient]

        for c in patient_candidates:
            c["person_id"] = pid
            c["target_drug"] = target_drug
            all_candidates.append(c)

    out = pd.DataFrame(all_candidates) if all_candidates else pd.DataFrame()
    elapsed = time.time() - t0
    print(
        f"  -> candidates: {len(out):,} | elapsed: {elapsed/60:.1f} min | "
        f"speed: {len(df)/max(elapsed,1):.1f} notes/sec"
    )
    return out


# ------------------------------
# Runner
# ------------------------------


@dataclass
class RunState:
    completed: Dict[str, Dict]
    failed: Dict[str, str]


def load_state(path: Path) -> RunState:
    if not path.exists():
        return RunState(completed={}, failed={})
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return RunState(completed={}, failed={})
    return RunState(
        completed=payload.get("completed", {}),
        failed=payload.get("failed", {}),
    )


def save_state(path: Path, state: RunState) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(
            {"completed": state.completed, "failed": state.failed},
            indent=2,
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )
    tmp.replace(path)


def resolve_chunks(chunk_dir: Path, chunk_prefix: str, chunk_names: Optional[List[str]]) -> List[Path]:
    if chunk_names:
        paths = [chunk_dir / f"{name}.parquet" for name in chunk_names]
    else:
        paths = sorted(chunk_dir.glob(f"{chunk_prefix}*.parquet"))
    # sort by chunk number if available
    def _key(p: Path):
        m = re.search(r"(\d+)(?=\.parquet$)", p.name)
        return int(m.group(1)) if m else 10**9

    return sorted([p for p in paths if p.exists()], key=_key)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Overnight chunk candidate extraction runner.")
    parser.add_argument("--chunk-dir", default="episode_notes", help="Directory containing chunk parquet files.")
    parser.add_argument(
        "--chunk-prefix",
        default="episode_notes_chunk",
        help="Chunk file prefix before numeric id.",
    )
    parser.add_argument(
        "--chunk-names",
        nargs="*",
        default=None,
        help='Optional explicit chunk names without extension (e.g., "episode_notes_chunk000").',
    )
    parser.add_argument("--lexicon-dir", default="lexicons", help="Lexicon directory.")
    parser.add_argument("--output-dir", default="episode_extraction_results", help="Output directory.")
    parser.add_argument(
        "--max-candidates-per-patient",
        type=int,
        default=40,
        help="Per-patient candidate cap for speed/memory.",
    )
    parser.add_argument(
        "--note-text-col-candidates",
        default="note_text_full,full_note_text,note_text,text",
        help="Comma-separated note text columns to try in order.",
    )
    parser.add_argument("--force", action="store_true", help="Reprocess chunks even if output exists.")
    parser.add_argument("--combine", action="store_true", help="Write all_candidates_combined.csv at end.")
    parser.add_argument(
        "--state-file",
        default="episode_extraction_results/candidate_run_state.json",
        help="Path for run-state JSON file.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    note_text_cols = [x.strip() for x in str(args.note_text_col_candidates).split(",") if x.strip()]

    project_root = Path(__file__).resolve().parents[1]
    chunk_dir = (project_root / args.chunk_dir).resolve()
    lexicon_dir = (project_root / args.lexicon_dir).resolve()
    output_dir = (project_root / args.output_dir).resolve()
    state_path = (project_root / args.state_file).resolve()

    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "logs").mkdir(parents=True, exist_ok=True)

    print("=" * 88)
    print("OVERNIGHT CANDIDATE EXTRACTION RUNNER")
    print("=" * 88)
    print(f"Project root: {project_root}")
    print(f"Chunk dir:    {chunk_dir}")
    print(f"Lexicon dir:  {lexicon_dir}")
    print(f"Output dir:   {output_dir}")
    print(f"State file:   {state_path}")
    print(f"Force mode:   {args.force}")
    print(f"Combine end:  {args.combine}")
    print(f"Note text col candidates: {note_text_cols}")
    print("=" * 88)

    chunks = resolve_chunks(
        chunk_dir=chunk_dir,
        chunk_prefix=args.chunk_prefix,
        chunk_names=args.chunk_names,
    )
    if not chunks:
        print("No chunk files found. Check --chunk-dir / --chunk-prefix / --chunk-names.")
        return 1

    print(f"Chunks resolved: {len(chunks)}")
    print("First 5:", [p.name for p in chunks[:5]])

    lex_cfg = discover_lexicons(lexicon_dir)
    cand_patterns = build_candidate_patterns(lex_cfg)
    print(f"Candidate regex groups: {len(cand_patterns)}")

    state = load_state(state_path)
    all_outputs: List[Path] = []

    run_t0 = time.time()
    for i, chunk_path in enumerate(chunks, start=1):
        chunk_name = chunk_path.stem
        out_csv = output_dir / f"{chunk_name}_candidates.csv"
        all_outputs.append(out_csv)

        if not args.force and out_csv.exists():
            print(f"\n[{i}/{len(chunks)}] Skip existing: {out_csv.name}")
            continue

        try:
            df_out = process_chunk(
                chunk_file=chunk_path,
                candidate_patterns=cand_patterns,
                max_candidates_per_patient=args.max_candidates_per_patient,
                note_text_col_candidates=note_text_cols,
            )
            tmp = out_csv.with_suffix(".csv.tmp")
            df_out.to_csv(tmp, index=False)
            tmp.replace(out_csv)

            state.completed[chunk_name] = {
                "rows": int(len(df_out)),
                "saved_to": str(out_csv),
                "ts": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            if chunk_name in state.failed:
                del state.failed[chunk_name]
            save_state(state_path, state)
            print(f"[{i}/{len(chunks)}] Saved {out_csv.name} ({len(df_out):,} rows)")
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            state.failed[chunk_name] = msg
            save_state(state_path, state)
            print(f"[{i}/{len(chunks)}] FAILED {chunk_name}: {msg}")

    if args.combine:
        existing = [p for p in all_outputs if p.exists()]
        if existing:
            parts = [pd.read_csv(p) for p in existing]
            combined = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
            combined_path = output_dir / "all_candidates_combined.csv"
            tmp = combined_path.with_suffix(".csv.tmp")
            combined.to_csv(tmp, index=False)
            tmp.replace(combined_path)
            print(f"\nCombined output: {combined_path} ({len(combined):,} rows from {len(existing)} chunks)")
        else:
            print("\nNo chunk outputs found to combine.")

    total_elapsed = (time.time() - run_t0) / 60.0
    print("\n" + "=" * 88)
    print(f"Run finished in {total_elapsed:.1f} min")
    print(f"Completed chunks: {len(state.completed)}")
    print(f"Failed chunks:    {len(state.failed)}")
    if state.failed:
        print("Failures:")
        for k, v in state.failed.items():
            print(f"  - {k}: {v}")
    print("=" * 88)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
