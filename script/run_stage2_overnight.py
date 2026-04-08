#!/usr/bin/env python3
"""
Overnight Stage-2 runner: medspaCy extraction on combined candidate spans.

What it does:
- Loads `all_candidates_combined.csv` (or another candidates CSV)
- Builds medspaCy TargetRules from lexicons under resources/lexicons
- Runs extraction in batches with nlp.pipe
- Writes checkpointed output CSV and state JSON for resume

Example:
  nohup python3 "resources/script/run_stage2_overnight.py" \
    --input-csv "episode_extraction_results/all_candidates_combined.csv" \
    --output-csv "episode_extraction_results/extracted_treatment_data_episode_cleaned.csv" \
    --state-file "episode_extraction_results/stage2_run_state.json" \
    --batch-size 128 \
    > "logs/stage2_overnight.log" 2>&1 &
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List

import pandas as pd


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
                tc = cols["term"]
                for row in reader:
                    x = _normalize_term(row.get(tc, ""))
                    if x:
                        terms.append(x)
                return _dedupe_terms(terms)

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


ALLOWED_SHORT_ENTITY_TERMS = {
    "ct", "mri", "pet", "psa", "hiv", "bmi", "ecg", "ekg", "a1c",
    "cbc", "cmp", "bun", "ast", "alt", "wbc", "hgb", "hct", "plt",
    "bp", "hr", "rr", "spo2", "fev1", "fev",
}

NOISY_SINGLE_TOKEN_TERMS = {
    "date", "time", "day", "name", "report", "position", "form", "location",
    "perform", "performed", "authorized", "chief", "complaint", "pain", "side",
    "ic", "pe", "ph", "ga", "cry", "tin", "water", "driving",
}


def _is_mention_safe_term(term: str, entity_type: str) -> bool:
    t = str(term).strip().lower()
    if not t or len(t) > 120:
        return False
    if not re.search(r"[a-z0-9]", t):
        return False
    if " " not in t and "-" not in t and "/" not in t:
        if len(t) <= 2 and t not in ALLOWED_SHORT_ENTITY_TERMS:
            return False
        if len(t) == 3 and t.isalpha() and t not in ALLOWED_SHORT_ENTITY_TERMS:
            return False
        if t in NOISY_SINGLE_TOKEN_TERMS:
            return False
    if entity_type == "measurements":
        if t in NOISY_SINGLE_TOKEN_TERMS:
            return False
        if len(t) <= 3 and t not in ALLOWED_SHORT_ENTITY_TERMS:
            return False
    return True


def setup_nlp(lex_cfg: Dict):
    import medspacy
    from medspacy.ner import TargetRule
    from medspacy.context import ConTextRule

    os.environ["LOGURU_LEVEL"] = "WARNING"
    for name in ["PyRuSH", "PyRuSH.PyRuSHSentencizer", "pyrush", "medspacy"]:
        logging.getLogger(name).setLevel(logging.WARNING)
    try:
        from loguru import logger as _loguru_logger
        _loguru_logger.remove()
        _loguru_logger.add(sys.stderr, level="WARNING")
    except Exception:
        pass

    nlp = medspacy.load()
    target_matcher = nlp.get_pipe("medspacy_target_matcher")
    context = nlp.get_pipe("medspacy_context")

    action_map = {"start": "TREATMENT_START", "stop": "TREATMENT_STOP", "hold": "TREATMENT_HOLD", "dose_change": "DOSE_CHANGE"}
    rule_pairs = set()

    for action, terms in lex_cfg["candidate_patterns"]["treatment_actions"].items():
        label = action_map.get(action)
        if not label:
            continue
        for term in terms:
            rule_pairs.add((term, label))

    for reason, terms in lex_cfg["candidate_patterns"]["discontinuation_reasons"].items():
        specific = f"DISCONT_REASON_{reason.upper()}"
        for term in terms:
            rule_pairs.add((term, specific))
            rule_pairs.add((term, "DISCONT_REASON"))

    ehr = lex_cfg.get("ehr_entities", {})
    for term in [t for t in ehr.get("conditions", []) if _is_mention_safe_term(t, "conditions")]:
        rule_pairs.add((term, "CONDITION_TERM"))
    for term in [t for t in ehr.get("drugs", []) if _is_mention_safe_term(t, "drugs")]:
        rule_pairs.add((term, "DRUG_TERM"))
    for term in [t for t in ehr.get("measurements", []) if _is_mention_safe_term(t, "measurements")]:
        rule_pairs.add((term, "MEASUREMENT_TERM"))
    for term in [t for t in ehr.get("procedures", []) if _is_mention_safe_term(t, "procedures")]:
        rule_pairs.add((term, "PROCEDURE_TERM"))

    target_matcher.add([TargetRule(literal=t, category=l) for t, l in sorted(rule_pairs)])
    context.add(
        [
            ConTextRule("not", "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("no", "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("denies", "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("without", "NEGATED_EXISTENCE", direction="FORWARD"),
            ConTextRule("possible", "POSSIBLE_EXISTENCE", direction="FORWARD"),
            ConTextRule("maybe", "POSSIBLE_EXISTENCE", direction="FORWARD"),
            ConTextRule("possibly", "POSSIBLE_EXISTENCE", direction="FORWARD"),
        ]
    )
    print(f"Loaded medspaCy with {len(rule_pairs):,} target rules")
    return nlp, list(lex_cfg["candidate_patterns"]["discontinuation_reasons"].keys())


def extract_with_medspacy(span_text: str, doc, reason_keys: List[str]) -> Dict:
    text_lower = span_text.lower()
    result = {
        "treatment_action": "none",
        "discontinuation_reason": "none",
        "certainty": "medium",
        "conditions": [],
        "drugs": [],
        "measurements": [],
        "procedures": [],
    }

    labels = {ent.label_ for ent in doc.ents}
    if "TREATMENT_START" in labels:
        result["treatment_action"] = "start"
    elif "TREATMENT_STOP" in labels:
        result["treatment_action"] = "stop"
    elif "TREATMENT_HOLD" in labels:
        result["treatment_action"] = "hold"
    elif "DOSE_CHANGE" in labels:
        result["treatment_action"] = "dose_change"
    else:
        verb_map = {
            "start": ["started", "initiated", "began", "prescribed", "continue", "continued", "resume", "resumed"],
            "stop": ["stopped", "discontinued", "ceased", "d/c", "dc'd"],
            "hold": ["hold", "held", "on hold", "paused"],
            "dose_change": ["increased", "decreased", "reduced", "adjusted"],
        }
        for a, words in verb_map.items():
            if any(w in text_lower for w in words):
                result["treatment_action"] = a
                break

    for reason in reason_keys:
        if f"DISCONT_REASON_{reason.upper()}" in labels:
            result["discontinuation_reason"] = reason
            break

    def _ctx(ent, attr: str) -> bool:
        u = getattr(ent, "_", None)
        return getattr(u, attr, False) if u else False

    has_neg = any(_ctx(e, "is_negated") for e in doc.ents)
    has_unc = any(_ctx(e, "is_uncertain") for e in doc.ents)
    if has_neg or has_unc:
        result["certainty"] = "low"
    elif result["treatment_action"] != "none" or result["discontinuation_reason"] != "none":
        result["certainty"] = "high"
    else:
        result["certainty"] = "low"

    label_map = {
        "conditions": "CONDITION_TERM",
        "drugs": "DRUG_TERM",
        "measurements": "MEASUREMENT_TERM",
        "procedures": "PROCEDURE_TERM",
    }
    for col, lbl in label_map.items():
        vals = {ent.text.strip().lower() for ent in doc.ents if ent.label_ == lbl}
        result[col] = sorted(v for v in vals if v)

    return result


@dataclass
class Stage2State:
    processed_rows: int
    total_rows: int
    updated_at: str


def load_state(path: Path) -> Stage2State:
    if not path.exists():
        return Stage2State(processed_rows=0, total_rows=0, updated_at="")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return Stage2State(
            processed_rows=int(payload.get("processed_rows", 0)),
            total_rows=int(payload.get("total_rows", 0)),
            updated_at=str(payload.get("updated_at", "")),
        )
    except Exception:
        return Stage2State(processed_rows=0, total_rows=0, updated_at="")


def save_state(path: Path, state: Stage2State) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(
            {
                "processed_rows": state.processed_rows,
                "total_rows": state.total_rows,
                "updated_at": state.updated_at,
            },
            indent=2,
            ensure_ascii=True,
        ),
        encoding="utf-8",
    )
    tmp.replace(path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Overnight Stage-2 medspaCy runner.")
    parser.add_argument(
        "--input-csv",
        default="episode_extraction_results/all_candidates_combined.csv",
        help="Input candidates CSV.",
    )
    parser.add_argument(
        "--output-csv",
        default="episode_extraction_results/extracted_treatment_data_episode_cleaned.csv",
        help="Output extracted CSV.",
    )
    parser.add_argument(
        "--state-file",
        default="episode_extraction_results/stage2_run_state.json",
        help="Progress state file.",
    )
    parser.add_argument(
        "--lexicon-dir",
        default="resources/lexicons",
        help="Lexicon directory.",
    )
    parser.add_argument("--batch-size", type=int, default=128, help="nlp.pipe batch size.")
    parser.add_argument(
        "--save-every-batches",
        type=int,
        default=5,
        help="Checkpoint output every N batches.",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=0,
        help="Optional cap for testing (0 means all rows).",
    )
    parser.add_argument("--force", action="store_true", help="Ignore resume and re-run from 0.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]
    in_csv = (root / args.input_csv).resolve()
    out_csv = (root / args.output_csv).resolve()
    state_file = (root / args.state_file).resolve()
    lex_dir = (root / args.lexicon_dir).resolve()

    print("=" * 88)
    print("OVERNIGHT STAGE-2 EXTRACTION RUNNER")
    print("=" * 88)
    print(f"Input CSV:   {in_csv}")
    print(f"Output CSV:  {out_csv}")
    print(f"State file:  {state_file}")
    print(f"Lexicon dir: {lex_dir}")
    print(f"Batch size:  {args.batch_size}")
    print(f"Force:       {args.force}")
    if args.max_rows:
        print(f"Max rows:    {args.max_rows}")
    print("=" * 88)

    if not in_csv.exists():
        print("Input CSV not found.")
        return 1

    df = pd.read_csv(in_csv)
    if args.max_rows > 0:
        df = df.head(args.max_rows).copy()
    total = len(df)
    if total == 0:
        print("Input CSV has 0 rows.")
        return 1

    lex_cfg = discover_lexicons(lex_dir)
    nlp, reason_keys = setup_nlp(lex_cfg)

    state = Stage2State(processed_rows=0, total_rows=total, updated_at="")
    if not args.force:
        state = load_state(state_file)
        if state.total_rows not in (0, total):
            print(
                f"State total_rows={state.total_rows} differs from current total={total}. "
                "Starting from 0 to avoid mismatch."
            )
            state = Stage2State(processed_rows=0, total_rows=total, updated_at="")
        else:
            state.total_rows = total
    else:
        state = Stage2State(processed_rows=0, total_rows=total, updated_at="")

    start_idx = 0 if args.force else max(0, min(state.processed_rows, total))
    print(f"Resume from row index: {start_idx}/{total}")

    # If resume and output exists, load previous rows
    prev_out = None
    if start_idx > 0 and out_csv.exists() and not args.force:
        prev_out = pd.read_csv(out_csv)
        print(f"Loaded existing output rows: {len(prev_out):,}")
    else:
        prev_out = pd.DataFrame()

    work_df = df.iloc[start_idx:].copy()
    if work_df.empty:
        print("Nothing to process. Stage-2 already complete.")
        return 0

    rows = work_df.to_dict("records")
    span_texts = [str(r.get("span_text", "") or "") for r in rows]

    # Build unique text list for pending portion only
    unique_texts = []
    seen = set()
    for txt in span_texts:
        if txt and txt not in seen:
            seen.add(txt)
            unique_texts.append(txt)

    print(f"Pending rows: {len(rows):,} | Unique pending span_text: {len(unique_texts):,}")

    t0 = time.time()
    cache: Dict[str, Dict] = {}
    total_batches = (len(unique_texts) + args.batch_size - 1) // args.batch_size
    produced = []

    for b in range(total_batches):
        s = b * args.batch_size
        e = min((b + 1) * args.batch_size, len(unique_texts))
        batch_texts = unique_texts[s:e]
        for txt, doc in zip(batch_texts, nlp.pipe(batch_texts, batch_size=args.batch_size)):
            cache[txt] = extract_with_medspacy(txt, doc=doc, reason_keys=reason_keys)

        if (b + 1) % max(args.save_every_batches, 1) == 0 or b + 1 == total_batches:
            elapsed = time.time() - t0
            done_u = e
            rate = done_u / elapsed if elapsed > 0 else 0.0
            remaining_u = len(unique_texts) - done_u
            eta_min = (remaining_u / rate) / 60.0 if rate > 0 else float("inf")
            print(
                f"Unique text batch {b+1}/{total_batches} | "
                f"done={done_u:,}/{len(unique_texts):,} | rate={rate:.1f}/s | ETA={eta_min:.1f} min"
            )

    # Assemble row-level output
    for row, txt in zip(rows, span_texts):
        x = cache.get(
            txt,
            {
                "treatment_action": "none",
                "discontinuation_reason": "none",
                "certainty": "low",
                "conditions": [],
                "drugs": [],
                "measurements": [],
                "procedures": [],
            },
        )
        produced.append(
            {
                "person_id": row.get("person_id"),
                "visit_id": row.get("visit_id"),
                "note_id": row.get("note_id"),
                "note_date": row.get("note_date"),
                "note_title": row.get("note_title"),
                "category": row.get("category"),
                "treatment_action": x.get("treatment_action"),
                "discontinuation_reason": x.get("discontinuation_reason"),
                "certainty": x.get("certainty"),
                "conditions": json.dumps(x.get("conditions", [])),
                "drugs": json.dumps(x.get("drugs", [])),
                "measurements": json.dumps(x.get("measurements", [])),
                "procedures": json.dumps(x.get("procedures", [])),
                "target_drug": row.get("target_drug"),
                "span_text": txt,
            }
        )

    new_out = pd.DataFrame(produced)
    final_out = pd.concat([prev_out, new_out], ignore_index=True) if len(prev_out) else new_out
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_csv.with_suffix(".csv.tmp")
    final_out.to_csv(tmp, index=False)
    tmp.replace(out_csv)

    elapsed = time.time() - t0
    state = Stage2State(
        processed_rows=len(final_out),
        total_rows=total,
        updated_at=time.strftime("%Y-%m-%d %H:%M:%S"),
    )
    save_state(state_file, state)

    print("\n" + "=" * 88)
    print(f"Stage-2 completed rows: {len(final_out):,}/{total:,}")
    print(f"Saved output: {out_csv}")
    print(f"Saved state:  {state_file}")
    print(f"Elapsed:      {elapsed/60:.1f} min")
    print("=" * 88)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
