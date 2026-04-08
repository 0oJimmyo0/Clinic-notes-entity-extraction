#!/usr/bin/env python3
"""
RQ1 drug linking utilities (Path A + Path B).

Path A: deterministic normalization + alias canonicalization
Path B: CPU embedding-style linker using char-ngram TF-IDF cosine similarity
        (dependency-free fallback for local CPU environments).
"""

from __future__ import annotations

import json
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Sequence, Tuple


GENERIC_NOISE_TOKENS = {
    "diagnosis",
    "history",
    "present",
    "illness",
    "surgeon",
    "allergies",
    "problem",
    "medications",
    "medication",
    "note",
    "other",
    "status",
    "patient",
    "location",
    "agreement",
    "assessment",
    "pain",
}


def normalize_drug_text(term: str) -> str:
    t = str(term).strip().lower()
    if not t:
        return ""
    t = re.sub(r"\([^)]*\)", " ", t)
    t = t.replace("/", " ").replace("_", " ").replace("-", " ")
    t = re.sub(r"\b\d+(\.\d+)?\s*(mg|mcg|g|ml|l|unit|units|%)\b", " ", t)
    t = re.sub(r"\b(po|iv|im|sc|subq|subcutaneous|intravenous|oral|topical|pf)\b", " ", t)
    t = re.sub(
        r"\b(tablet|tabs?|capsule|caps|solution|syrup|injection|injectable|ointment|spray|suspension|patch|cream|drop|drops)\b",
        " ",
        t,
    )
    t = re.sub(r"\bj\d{4,6}\b", " ", t)
    t = re.sub(r"\bndc[:\s-]*\d+\b", " ", t)
    t = re.sub(r"\b(builder|carrier fluid|irrigation|vumc|o r)\b", " ", t)
    t = re.sub(r"[^a-z0-9\s]", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    if not t:
        return ""
    toks = [x for x in t.split() if len(x) >= 3 and x not in GENERIC_NOISE_TOKENS]
    if not toks:
        return ""
    return " ".join(toks[:4])


def load_alias_map(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    raw = json.loads(path.read_text(encoding="utf-8"))
    out = {}
    for k, v in raw.items():
        nk = normalize_drug_text(k)
        nv = normalize_drug_text(v)
        if nk and nv:
            out[nk] = nv
    return out


def canonicalize_drug(term: str, alias_map: Dict[str, str]) -> str:
    n = normalize_drug_text(term)
    if not n:
        return ""
    return alias_map.get(n, n)


def _char_ngrams(text: str, n_min: int = 3, n_max: int = 4) -> List[str]:
    s = f" {text} "
    out: List[str] = []
    for n in range(n_min, n_max + 1):
        if len(s) < n:
            continue
        for i in range(len(s) - n + 1):
            out.append(s[i : i + n])
    return out


class CharNgramLinker:
    def __init__(self, concepts: Sequence[str]):
        clean = sorted({c for c in concepts if c})
        self.concepts = clean
        self.doc_tf = [Counter(_char_ngrams(c)) for c in self.concepts]
        df = Counter()
        for tf in self.doc_tf:
            for g in tf:
                df[g] += 1
        n_docs = max(len(self.doc_tf), 1)
        self.idf = {g: math.log((1 + n_docs) / (1 + d)) + 1.0 for g, d in df.items()}
        self.doc_vec = [self._to_tfidf(tf) for tf in self.doc_tf]
        self.doc_norm = [self._norm(v) for v in self.doc_vec]

    def _to_tfidf(self, tf: Counter) -> Dict[str, float]:
        return {g: float(c) * self.idf.get(g, 1.0) for g, c in tf.items()}

    @staticmethod
    def _norm(vec: Dict[str, float]) -> float:
        return math.sqrt(sum(v * v for v in vec.values()))

    @staticmethod
    def _dot(a: Dict[str, float], b: Dict[str, float]) -> float:
        if len(a) > len(b):
            a, b = b, a
        return sum(v * b.get(k, 0.0) for k, v in a.items())

    def best(self, query: str) -> Tuple[str, float]:
        tf = Counter(_char_ngrams(query))
        qv = self._to_tfidf(tf)
        qn = self._norm(qv)
        if qn == 0:
            return "", 0.0
        best_c = ""
        best_s = 0.0
        for c, dv, dn in zip(self.concepts, self.doc_vec, self.doc_norm):
            if dn == 0:
                continue
            sim = self._dot(qv, dv) / (qn * dn)
            if sim > best_s:
                best_s = sim
                best_c = c
        return best_c, float(best_s)


def resolve_note_drugs_hybrid(
    note_terms: Sequence[str],
    ehr_terms: Sequence[str],
    alias_map: Dict[str, str],
    use_embedding: bool,
    threshold: float,
) -> Tuple[List[str], Dict[str, dict]]:
    """
    Returns:
      mapped_terms: normalized note terms where accepted links are replaced by linked concepts
      diagnostics: per-note-term decision details
    """
    note_c = sorted({canonicalize_drug(t, alias_map) for t in note_terms if canonicalize_drug(t, alias_map)})
    ehr_c = sorted({canonicalize_drug(t, alias_map) for t in ehr_terms if canonicalize_drug(t, alias_map)})
    ehr_set = set(ehr_c)
    diagnostics: Dict[str, dict] = {}

    if not note_c:
        return [], diagnostics

    # Path A exact canonical match first.
    mapped = []
    unresolved = []
    for t in note_c:
        if t in ehr_set:
            mapped.append(t)
            diagnostics[t] = {"stage": "path_a", "linked_to": t, "score": 1.0, "accepted": True}
        else:
            unresolved.append(t)

    # Path B embedding-style matcher for unresolved terms.
    if use_embedding and unresolved and ehr_c:
        linker = CharNgramLinker(ehr_c)
        for t in unresolved:
            cand, score = linker.best(t)
            accept = bool(cand) and score >= threshold
            if accept:
                mapped.append(cand)
            else:
                mapped.append(t)
            diagnostics[t] = {
                "stage": "path_b_embedding_cpu",
                "linked_to": cand,
                "score": round(float(score), 6),
                "accepted": accept,
            }
    else:
        for t in unresolved:
            mapped.append(t)
            diagnostics[t] = {
                "stage": "path_a_unresolved",
                "linked_to": "",
                "score": 0.0,
                "accepted": False,
            }

    return sorted(set(mapped)), diagnostics


def summarize_link_diagnostics(diags: Sequence[Dict[str, dict]]) -> Dict[str, float]:
    agg = defaultdict(int)
    for row in diags:
        for info in row.values():
            stage = str(info.get("stage", "unknown"))
            agg[f"n_{stage}"] += 1
            if info.get("accepted"):
                agg["n_accepted"] += 1
            agg["n_total"] += 1
    out = {k: float(v) for k, v in agg.items()}
    total = out.get("n_total", 0.0)
    out["accept_rate"] = (out.get("n_accepted", 0.0) / total) if total else 0.0
    return out

