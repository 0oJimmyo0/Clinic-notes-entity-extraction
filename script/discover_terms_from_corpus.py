#!/usr/bin/env python3
"""
Formal term discovery from a text corpus using seed terms.

Reads seed terms from a CSV (column "term"), scans a directory of .txt files,
and extracts n-grams that co-occur with seeds in the same sentence. Ranks
candidates by frequency and co-occurrence for review or merging into lexicons.

Usage:
  python discover_terms_from_corpus.py --seeds PATH --corpus-dir PATH --output PATH [options]

Example:
  python script/discover_terms_from_corpus.py \\
    --seeds lexicons/candidate_treatment_actions__stop.csv \\
    --corpus-dir data/notes_txt \\
    --output lexicons/candidate_treatment_actions__stop_expanded.csv
"""

from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from collections import Counter

DEFAULT_MIN_FREQ = 2
DEFAULT_TOP_K = 200
DEFAULT_MAX_NGRAM = 2


def _norm(t: str) -> str:
    return re.sub(r"\s+", " ", t.strip()).lower()


def load_seeds(path: Path) -> set[str]:
    terms = set()
    with path.open("r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "term" not in reader.fieldnames:
            for row in reader:
                if row:
                    v = list(row.values())[0]
                    if v and _norm(v):
                        terms.add(_norm(v))
            return terms
        for row in reader:
            v = row.get("term", "").strip()
            if v:
                terms.add(_norm(v))
    return terms


def sentence_iter(corpus_dir: Path) -> list[str]:
    """Collect sentences (sentence-split or lines) from .txt files under corpus_dir."""
    sentences = []
    for f in corpus_dir.rglob("*.txt"):
        try:
            text = f.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            continue
        file_sents = []
        for line in text.splitlines():
            line = line.strip()
            if not line or len(line) < 10:
                continue
            for sent in re.split(r"[.!?]\s+", line):
                sent = sent.strip()
                if len(sent) >= 10:
                    file_sents.append(sent)
        if not file_sents:
            for line in text.splitlines():
                s = line.strip()
                if len(s) >= 10:
                    file_sents.append(s)
        sentences.extend(file_sents)
    return sentences


def extract_ngrams(sentence: str, max_n: int) -> list[str]:
    """Return unigrams and bigrams (normalized) from sentence."""
    words = re.findall(r"[a-z0-9']+", _norm(sentence))
    out = []
    for n in range(1, min(max_n, len(words)) + 1):
        for i in range(len(words) - n + 1):
            ngram = " ".join(words[i : i + n])
            if len(ngram) >= 2:
                out.append(ngram)
    return out


def run(
    seeds_path: Path,
    corpus_dir: Path,
    output_path: Path,
    *,
    min_freq: int = DEFAULT_MIN_FREQ,
    top_k: int = DEFAULT_TOP_K,
    max_ngram: int = DEFAULT_MAX_NGRAM,
) -> None:
    seeds = load_seeds(seeds_path)
    if not seeds:
        raise SystemExit("No seed terms loaded. Ensure CSV has a 'term' column or a single column of terms.")
    sentences = sentence_iter(corpus_dir)
    if not sentences:
        raise SystemExit("No sentences found under corpus-dir.")

    cooccur: Counter[str] = Counter()
    for sent in sentences:
        if not any(s in sent.lower() for s in seeds):
            continue
        for ngram in extract_ngrams(sent, max_ngram):
            if ngram not in seeds:
                cooccur[ngram] += 1

    candidates = [(t, c) for t, c in cooccur.most_common(top_k * 2) if c >= min_freq][:top_k]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["term", "cooccurrence_count"])
        for t, c in candidates:
            w.writerow([t, c])
    print(f"Wrote {len(candidates)} candidates to {output_path} (min_freq={min_freq}, top_k={top_k})")


def main() -> int:
    ap = argparse.ArgumentParser(description="Discover candidate terms from corpus using seed co-occurrence")
    ap.add_argument("--seeds", type=Path, required=True, help="CSV with 'term' column (seed terms)")
    ap.add_argument("--corpus-dir", type=Path, required=True, help="Directory of .txt files (notes)")
    ap.add_argument("--output", type=Path, required=True, help="Output CSV: term, cooccurrence_count")
    ap.add_argument("--min-freq", type=int, default=DEFAULT_MIN_FREQ, help="Minimum co-occurrence count")
    ap.add_argument("--top-k", type=int, default=DEFAULT_TOP_K, help="Max number of candidates to output")
    ap.add_argument("--max-ngram", type=int, default=DEFAULT_MAX_NGRAM, help="Max n-gram size (1 or 2)")
    args = ap.parse_args()
    run(
        args.seeds,
        args.corpus_dir,
        args.output,
        min_freq=args.min_freq,
        top_k=args.top_k,
        max_ngram=args.max_ngram,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
