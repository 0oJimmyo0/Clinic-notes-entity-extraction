#!/usr/bin/env python3
"""
RQ1 drug normalization and linking utilities.

Primary paper-facing behavior:
- Path A: deterministic normalization + alias canonicalization
- Path B: abstaining linker over a canonical drug vocabulary

Legacy downstream concordance behavior is retained for backward compatibility:
- same-visit note/EHR overlap helpers
- embedding_cpu char-ngram linker
"""

from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple


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


def load_alias_entries(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []

    suffix = path.suffix.lower()
    rows: List[Dict[str, str]] = []

    if suffix == ".csv":
        with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                alias_raw = str(row.get("alias_raw", "")).strip()
                alias_normalized = str(row.get("alias_normalized", "")).strip()
                canonical_label = str(
                    row.get("canonical_label")
                    or row.get("canonical")
                    or row.get("canonical_norm")
                    or ""
                ).strip()
                include_flag = str(row.get("include_flag", "yes")).strip().lower()
                if not alias_raw or not canonical_label:
                    continue
                rows.append(
                    {
                        "alias_raw": alias_raw,
                        "alias_normalized": alias_normalized or normalize_drug_text(alias_raw),
                        "canonical_label": canonical_label,
                        "mapping_type": str(row.get("mapping_type", "")).strip(),
                        "confidence": str(row.get("confidence", "")).strip().lower(),
                        "include_flag": include_flag or "yes",
                        "notes": str(row.get("notes", "")).strip(),
                        "source_reference": str(row.get("source_reference", "")).strip(),
                    }
                )
        return rows

    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return rows

    for k, v in raw.items():
        if isinstance(v, dict):
            rows.append(
                {
                    "alias_raw": str(k).strip(),
                    "alias_normalized": normalize_drug_text(str(k)),
                    "canonical_label": str(v.get("canonical", "")).strip(),
                    "mapping_type": str(v.get("type", "")).strip(),
                    "confidence": str(v.get("confidence", "")).strip().lower(),
                    "include_flag": "yes",
                    "notes": str(v.get("notes", "")).strip(),
                    "source_reference": str(v.get("source_reference", "")).strip(),
                }
            )
        else:
            rows.append(
                {
                    "alias_raw": str(k).strip(),
                    "alias_normalized": normalize_drug_text(str(k)),
                    "canonical_label": str(v).strip(),
                    "mapping_type": "legacy_flat_json",
                    "confidence": "high",
                    "include_flag": "yes",
                    "notes": "",
                    "source_reference": "legacy_json",
                }
            )
    return rows


def load_alias_exclusions(path: Optional[Path]) -> Set[str]:
    """
    Load deterministic Path A exclusion terms (ambiguous abbreviations, regimen shorthands).
    Accepts CSV with one of: term, alias, alias_raw, alias_normalized.
    """
    out: Set[str] = set()
    if path is None or not path.exists():
        return out
    if path.suffix.lower() != ".csv":
        return out

    with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
        reader = csv.DictReader(f)
        cols = {str(c).strip().lower(): c for c in (reader.fieldnames or [])}
        col = cols.get("term") or cols.get("alias") or cols.get("alias_raw") or cols.get("alias_normalized")
        if col is None:
            return out
        for row in reader:
            n = normalize_drug_text(str(row.get(col, "") or ""))
            if n:
                out.add(n)
    return out


def find_alias_conflicts(
    alias_entries: Sequence[Dict[str, str]],
    exclusions: Optional[Set[str]] = None,
) -> List[Dict[str, str]]:
    """
    Detect aliases mapping to more than one canonical label among active include_flag rows.
    """
    exclusions = exclusions or set()
    bucket: Dict[str, Set[str]] = defaultdict(set)

    for row in alias_entries:
        include_flag = str(row.get("include_flag", "yes")).strip().lower()
        if include_flag not in {"yes", "true", "1"}:
            continue
        alias_norm = normalize_drug_text(row.get("alias_normalized") or row.get("alias_raw") or "")
        canonical_norm = normalize_drug_text(row.get("canonical_label") or "")
        if not alias_norm or not canonical_norm:
            continue
        if alias_norm in exclusions:
            continue
        bucket[alias_norm].add(canonical_norm)

    conflicts: List[Dict[str, str]] = []
    for alias_norm, canonical_set in sorted(bucket.items()):
        if len(canonical_set) <= 1:
            continue
        conflicts.append(
            {
                "alias_norm": alias_norm,
                "canonical_norms": "|".join(sorted(canonical_set)),
            }
        )
    return conflicts


def load_alias_map(
    path: Path,
    exclusions: Optional[Set[str]] = None,
    enforce_one_to_one: bool = False,
) -> Dict[str, str]:
    if not path.exists():
        return {}
    exclusions = exclusions or set()
    entries = load_alias_entries(path)
    conflicts = find_alias_conflicts(entries, exclusions=exclusions)
    if conflicts and enforce_one_to_one:
        raise ValueError(
            "Alias artifact has non one-to-one mappings: "
            + "; ".join(f"{x['alias_norm']}->{x['canonical_norms']}" for x in conflicts)
        )

    out = {}
    for row in entries:
        if str(row.get("include_flag", "yes")).strip().lower() not in {"yes", "true", "1"}:
            continue
        nk = normalize_drug_text(row.get("alias_normalized") or row.get("alias_raw") or "")
        nv = normalize_drug_text(row.get("canonical_label") or "")
        if nk in exclusions:
            continue
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

    def topk(self, query: str, k: int = 20) -> List[Tuple[str, float]]:
        tf = Counter(_char_ngrams(query))
        qv = self._to_tfidf(tf)
        qn = self._norm(qv)
        if qn == 0:
            return []
        scored: List[Tuple[str, float]] = []
        for c, dv, dn in zip(self.concepts, self.doc_vec, self.doc_norm):
            if dn == 0:
                continue
            sim = self._dot(qv, dv) / (qn * dn)
            scored.append((c, float(sim)))
        scored.sort(key=lambda x: x[1], reverse=True)
        return scored[: max(int(k), 0)]


@dataclass
class PathBConfig:
    top_k: int = 20
    min_score: float = 0.45
    min_margin: float = 0.05
    min_mention_len: int = 4
    min_score_short_mention: float = 0.80
    min_calibrated_confidence: float = 0.90
    short_mention_len: int = 5
    calibration: Optional[dict] = None
    abbreviation_map: Dict[str, str] = field(default_factory=dict)
    out_of_scope_statuses: Set[str] = field(
        default_factory=lambda: {
            "unclear",
            "reference_only",
            "historical_prior",
            "planned_considering",
            "discussion_only",
        }
    )
    score_weights: Dict[str, float] = field(
        default_factory=lambda: {
            "exact_norm_eq": 0.28,
            "exact_token_set_eq": 0.10,
            "token_jaccard": 0.14,
            "containment_ratio": 0.14,
            "edit_similarity": 0.10,
            "longest_token_overlap": 0.06,
            "first_informative_token_match": 0.05,
            "alias_hit": 0.05,
            "brand_generic_relation": 0.03,
            "ingredient_overlap": 0.03,
            "abbreviation_expansion_hit": 0.02,
        }
    )


@dataclass
class CanonicalCandidate:
    canonical_label: str
    canonical_norm: str
    synonyms_norm: Set[str]
    alias_norms: Set[str]
    ingredient_tokens: Set[str]
    is_combo: bool


class CanonicalDrugUniverse:
    def __init__(self, candidates: Sequence[CanonicalCandidate]):
        self.candidates = sorted(candidates, key=lambda c: c.canonical_norm)
        self.by_norm: Dict[str, CanonicalCandidate] = {c.canonical_norm: c for c in self.candidates}
        self.synonym_to_canonical: Dict[str, str] = {}
        self.token_index: Dict[str, Set[str]] = defaultdict(set)
        self.first_token_index: Dict[str, Set[str]] = defaultdict(set)

        for c in self.candidates:
            syns = set(c.synonyms_norm) | {c.canonical_norm}
            for s in syns:
                # Keep first seen mapping; candidates are deterministic sorted.
                self.synonym_to_canonical.setdefault(s, c.canonical_norm)
                toks = [t for t in s.split() if len(t) >= 2]
                for tok in toks:
                    self.token_index[tok].add(c.canonical_norm)
                if toks:
                    self.first_token_index[toks[0]].add(c.canonical_norm)

        self._char_linker = CharNgramLinker([c.canonical_norm for c in self.candidates])

    def topk_char(self, mention_norm: str, k: int) -> List[Tuple[str, float]]:
        return self._char_linker.topk(mention_norm, k=k)


def _tokenize_norm(text: str) -> List[str]:
    return [t for t in str(text).split() if t and t not in GENERIC_NOISE_TOKENS]


def _safe_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def load_calibration_config(path: Optional[Path]) -> Optional[dict]:
    if path is None or not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def load_abbreviation_map(path: Optional[Path]) -> Dict[str, str]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in payload.items():
        nk = normalize_drug_text(str(k))
        nv = normalize_drug_text(str(v))
        if nk and nv:
            out[nk] = nv
    return out


def _read_canonical_vocab_rows(path: Path) -> List[Tuple[str, List[str]]]:
    rows: List[Tuple[str, List[str]]] = []
    if not path.exists():
        return rows

    if path.suffix.lower() == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, dict):
            for k, v in payload.items():
                syns = []
                if isinstance(v, list):
                    syns = [str(x) for x in v]
                elif isinstance(v, str):
                    syns = [v]
                rows.append((str(k), syns))
            return rows
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, str):
                    rows.append((item, []))
                    continue
                if not isinstance(item, dict):
                    continue
                canonical = (
                    item.get("canonical_label")
                    or item.get("canonical")
                    or item.get("label")
                    or item.get("name")
                    or ""
                )
                if not canonical:
                    continue
                syn = item.get("synonyms") or item.get("aliases") or []
                if isinstance(syn, str):
                    syn = [x.strip() for x in re.split(r"[|;,]", syn) if x.strip()]
                if not isinstance(syn, list):
                    syn = []
                rows.append((str(canonical), [str(x) for x in syn]))
            return rows
        return rows

    import pandas as pd

    df = pd.read_csv(path)
    lc = {c.lower(): c for c in df.columns}
    canonical_col = (
        lc.get("canonical_label")
        or lc.get("canonical")
        or lc.get("label")
        or lc.get("name")
        or lc.get("concept_name")
    )
    if canonical_col is None:
        return rows

    synonyms_col = lc.get("synonyms") or lc.get("aliases") or lc.get("alias")
    for _, r in df.iterrows():
        canonical = str(r.get(canonical_col, "")).strip()
        if not canonical or canonical.lower() in {"nan", "none"}:
            continue
        syns: List[str] = []
        if synonyms_col is not None:
            raw = r.get(synonyms_col, "")
            if isinstance(raw, str):
                syns = [x.strip() for x in re.split(r"[|;]", raw) if x.strip()]
        rows.append((canonical, syns))
    return rows


def _read_adjudicated_labels(path: Optional[Path]) -> List[str]:
    if path is None or not path.exists():
        return []
    import pandas as pd

    df = pd.read_csv(path)
    lower = {c.lower(): c for c in df.columns}
    col = (
        lower.get("adjudicated_drug_label")
        or lower.get("canonical_label")
        or lower.get("drug_label")
        or lower.get("label")
        or lower.get("drug")
    )
    if col is None:
        return []
    vals = df[col].dropna().astype(str).tolist()
    return [v for v in vals if v.strip()]


def _ingredient_tokens_from_norm(s: str) -> Set[str]:
    # Preserve transparent ingredient-level overlap for combo products.
    parts = [x.strip() for x in re.split(r"\b(?:and|with|plus)\b", s) if x.strip()]
    toks: Set[str] = set()
    for p in parts:
        toks.update([t for t in _tokenize_norm(p) if len(t) >= 3])
    return toks


def build_canonical_drug_universe(
    alias_map: Dict[str, str],
    canonical_vocab_path: Optional[Path] = None,
    adjudicated_labels_path: Optional[Path] = None,
) -> CanonicalDrugUniverse:
    bucket: Dict[str, Dict[str, Set[str]]] = {}

    def _add(canonical_raw: str, synonyms: Iterable[str], alias_raw: Optional[str] = None) -> None:
        cn = normalize_drug_text(canonical_raw)
        if not cn:
            return
        b = bucket.setdefault(cn, {"syn": set(), "alias": set()})
        b["syn"].add(cn)
        for s in synonyms:
            sn = normalize_drug_text(s)
            if sn:
                b["syn"].add(sn)
        if alias_raw:
            an = normalize_drug_text(alias_raw)
            if an:
                b["alias"].add(an)

    # Alias map is always part of candidate universe.
    for raw_alias, raw_canonical in alias_map.items():
        _add(raw_canonical, [raw_alias, raw_canonical], alias_raw=raw_alias)

    # Optional canonical vocabulary table.
    if canonical_vocab_path is not None and canonical_vocab_path.exists():
        for canonical, syns in _read_canonical_vocab_rows(canonical_vocab_path):
            _add(canonical, [canonical, *syns])

    # Optional adjudicated canonical labels.
    for lbl in _read_adjudicated_labels(adjudicated_labels_path):
        _add(lbl, [lbl])

    # Fallback if alias map is unexpectedly empty.
    if not bucket:
        for t in ["paclitaxel", "docetaxel", "pembrolizumab", "nivolumab"]:
            _add(t, [t])

    candidates: List[CanonicalCandidate] = []
    for cn, values in bucket.items():
        syns = set(values["syn"]) | {cn}
        alias_norms = set(values["alias"])
        is_combo = bool(re.search(r"\b(?:and|with|plus)\b", cn)) or (len(cn.split()) >= 4 and len(_ingredient_tokens_from_norm(cn)) >= 2)
        candidates.append(
            CanonicalCandidate(
                canonical_label=cn,
                canonical_norm=cn,
                synonyms_norm=syns,
                alias_norms=alias_norms,
                ingredient_tokens=_ingredient_tokens_from_norm(cn),
                is_combo=is_combo,
            )
        )

    return CanonicalDrugUniverse(candidates)


def _token_jaccard(a: Sequence[str], b: Sequence[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def _longest_token_overlap_ratio(mention_tokens: Sequence[str], inter: Set[str]) -> float:
    if not mention_tokens or not inter:
        return 0.0
    denom = max(len(t) for t in mention_tokens)
    if denom <= 0:
        return 0.0
    num = max(len(t) for t in inter)
    return num / denom


def _feature_vector(
    mention_norm: str,
    candidate: CanonicalCandidate,
    abbreviation_expanded_norm: str,
) -> Dict[str, float]:
    mention_tokens = _tokenize_norm(mention_norm)
    cand_tokens = _tokenize_norm(candidate.canonical_norm)
    inter = set(mention_tokens) & set(cand_tokens)

    exact_norm_eq = float(mention_norm == candidate.canonical_norm or mention_norm in candidate.synonyms_norm)
    exact_token_set_eq = float(set(mention_tokens) == set(cand_tokens) and bool(mention_tokens))
    token_jacc = _token_jaccard(mention_tokens, cand_tokens)
    containment = float(len(inter) / max(len(set(mention_tokens)), 1))
    edit_sim = SequenceMatcher(None, mention_norm, candidate.canonical_norm).ratio()
    longest_overlap = _longest_token_overlap_ratio(mention_tokens, inter)
    first_token_match = float(bool(mention_tokens and cand_tokens and mention_tokens[0] == cand_tokens[0]))
    alias_hit = float(mention_norm in candidate.alias_norms)
    brand_generic = float(alias_hit and mention_norm != candidate.canonical_norm)
    ingredient_overlap = float(bool(inter & candidate.ingredient_tokens))
    combo_partial = float(candidate.is_combo and 0 < len(inter) < max(len(set(cand_tokens)), 1))
    abbr_hit = float(bool(abbreviation_expanded_norm and abbreviation_expanded_norm in candidate.synonyms_norm))

    return {
        "exact_norm_eq": exact_norm_eq,
        "exact_token_set_eq": exact_token_set_eq,
        "token_jaccard": token_jacc,
        "containment_ratio": containment,
        "edit_similarity": float(edit_sim),
        "longest_token_overlap": float(longest_overlap),
        "first_informative_token_match": first_token_match,
        "alias_hit": alias_hit,
        "brand_generic_relation": brand_generic,
        "ingredient_overlap": ingredient_overlap,
        "combo_partial_overlap_flag": combo_partial,
        "abbreviation_expansion_hit": abbr_hit,
    }


def _weighted_score(features: Dict[str, float], cfg: PathBConfig) -> float:
    num = 0.0
    den = 0.0
    for k, w in cfg.score_weights.items():
        den += abs(w)
        num += w * float(features.get(k, 0.0))
    if den <= 0:
        return 0.0
    return max(0.0, min(1.0, num / den))


def _calibrate_confidence(raw_score: float, calibration: Optional[dict]) -> float:
    s = max(0.0, min(1.0, float(raw_score)))
    if not calibration:
        return s

    kind = str(calibration.get("type", "identity")).strip().lower()
    if kind in {"", "identity"}:
        return s

    if kind == "platt":
        a = _safe_float(calibration.get("a", 1.0), 1.0)
        b = _safe_float(calibration.get("b", 0.0), 0.0)
        z = max(-60.0, min(60.0, a * s + b))
        return 1.0 / (1.0 + math.exp(-z))

    if kind in {"isotonic_bins", "score_bins"}:
        bins = calibration.get("bins", [])
        if isinstance(bins, list):
            for item in bins:
                if not isinstance(item, dict):
                    continue
                lo = _safe_float(item.get("score_min", 0.0), 0.0)
                hi = _safe_float(item.get("score_max", 1.0), 1.0)
                if lo <= s <= hi:
                    return max(0.0, min(1.0, _safe_float(item.get("precision", s), s)))

    return s


def _retrieve_topk_candidates(
    mention_norm: str,
    universe: CanonicalDrugUniverse,
    cfg: PathBConfig,
) -> List[str]:
    out: Set[str] = set()

    exact = universe.synonym_to_canonical.get(mention_norm)
    if exact:
        out.add(exact)

    mention_tokens = _tokenize_norm(mention_norm)
    if mention_tokens:
        for t in mention_tokens:
            out.update(universe.token_index.get(t, set()))
        out.update(universe.first_token_index.get(mention_tokens[0], set()))

    if mention_norm in cfg.abbreviation_map:
        expanded = cfg.abbreviation_map[mention_norm]
        exact_expanded = universe.synonym_to_canonical.get(expanded)
        if exact_expanded:
            out.add(exact_expanded)

    # Backstop lexical retrieval over full candidate space.
    for c, _ in universe.topk_char(mention_norm, k=max(cfg.top_k * 2, 20)):
        out.add(c)

    # Rank retrieval candidates quickly before expensive feature scoring.
    ranked = []
    for cn in out:
        cand = universe.by_norm.get(cn)
        if cand is None:
            continue
        tok_overlap = _token_jaccard(_tokenize_norm(mention_norm), _tokenize_norm(cand.canonical_norm))
        ed = SequenceMatcher(None, mention_norm, cand.canonical_norm).ratio()
        r = 0.65 * tok_overlap + 0.35 * ed
        ranked.append((cn, r))

    ranked.sort(key=lambda x: x[1], reverse=True)
    return [cn for cn, _ in ranked[: max(int(cfg.top_k), 1)]]


def _link_unresolved_mention(
    mention_norm: str,
    universe: CanonicalDrugUniverse,
    cfg: PathBConfig,
    mention_meta: Optional[dict] = None,
) -> Dict:
    mention_meta = mention_meta or {}
    candidates = _retrieve_topk_candidates(mention_norm, universe, cfg)
    if not candidates:
        return {
            "best_candidate": "",
            "score": 0.0,
            "calibrated_confidence": 0.0,
            "accepted": False,
            "reason_codes": ["no_candidates"],
            "top_k_candidates": [],
            "feature_values": {},
        }

    abbr_expanded = cfg.abbreviation_map.get(mention_norm, "")
    scored = []
    for cn in candidates:
        cand = universe.by_norm.get(cn)
        if cand is None:
            continue
        fv = _feature_vector(mention_norm, cand, abbreviation_expanded_norm=abbr_expanded)
        score = _weighted_score(fv, cfg)
        conf = _calibrate_confidence(score, cfg.calibration)
        scored.append(
            {
                "canonical_label": cand.canonical_norm,
                "score": round(float(score), 6),
                "calibrated_confidence": round(float(conf), 6),
                "feature_values": {k: round(float(v), 6) for k, v in fv.items()},
            }
        )

    if not scored:
        return {
            "best_candidate": "",
            "score": 0.0,
            "calibrated_confidence": 0.0,
            "accepted": False,
            "reason_codes": ["no_scored_candidates"],
            "top_k_candidates": [],
            "feature_values": {},
        }

    scored.sort(key=lambda x: (x["score"], x["calibrated_confidence"]), reverse=True)
    best = scored[0]
    second = scored[1] if len(scored) > 1 else None
    margin = float(best["score"] - second["score"]) if second else float(best["score"])

    reasons: List[str] = []
    mention_tokens = _tokenize_norm(mention_norm)

    if len(mention_norm) < cfg.min_mention_len:
        reasons.append("mention_too_short")
    if not mention_tokens or mention_norm in GENERIC_NOISE_TOKENS:
        reasons.append("mention_noisy_or_empty")
    if best["score"] < cfg.min_score:
        reasons.append("score_below_min")
    if len(mention_tokens) <= 1 and best["score"] < cfg.min_score_short_mention:
        reasons.append("ambiguous_short_term")
    if margin < cfg.min_margin:
        reasons.append("low_margin_top1_top2")
    if best["calibrated_confidence"] < cfg.min_calibrated_confidence:
        reasons.append("confidence_below_target_precision")
    if best["feature_values"].get("combo_partial_overlap_flag", 0.0) > 0.5:
        reasons.append("combo_single_agent_mismatch")

    mention_status = str(mention_meta.get("mention_status", "")).strip().lower()
    if mention_status and mention_status in cfg.out_of_scope_statuses:
        reasons.append("out_of_scope_mention_status")

    accepted = len(reasons) == 0

    return {
        "best_candidate": str(best["canonical_label"]),
        "score": float(best["score"]),
        "calibrated_confidence": float(best["calibrated_confidence"]),
        "accepted": bool(accepted),
        "reason_codes": reasons,
        "top_k_candidates": scored,
        "feature_values": best["feature_values"],
    }


def link_mention_to_canonical_vocab(
    raw_mention: str,
    alias_map: Dict[str, str],
    candidate_universe: Optional[CanonicalDrugUniverse] = None,
    pathb_config: Optional[PathBConfig] = None,
    mention_metadata: Optional[dict] = None,
) -> Dict:
    """
    Public entry point for Path A + Path B over a canonical drug vocabulary.

    Returns a dictionary containing:
    - raw_mention
    - mention_norm
    - patha_term
    - patha_exact_vocab_hit
    - prediction
    - accepted
    - score
    - calibrated_confidence
    - reason_codes
    - top_k_candidates
    """
    mention_norm = normalize_drug_text(raw_mention)
    if not mention_norm:
        return {
            "raw_mention": str(raw_mention),
            "mention_norm": "",
            "patha_term": "",
            "patha_exact_vocab_hit": False,
            "prediction": "",
            "accepted": False,
            "score": 0.0,
            "calibrated_confidence": 0.0,
            "reason_codes": ["empty_after_normalization"],
            "top_k_candidates": [],
            "feature_values": {},
            "stage": "path_a_unresolved",
        }

    universe = candidate_universe or build_canonical_drug_universe(alias_map=alias_map)
    cfg = pathb_config or PathBConfig()
    patha_term = canonicalize_drug(mention_norm, alias_map)
    exact = universe.synonym_to_canonical.get(patha_term, "")
    if exact:
        return {
            "raw_mention": str(raw_mention),
            "mention_norm": mention_norm,
            "patha_term": patha_term,
            "patha_exact_vocab_hit": True,
            "prediction": exact,
            "accepted": True,
            "score": 1.0,
            "calibrated_confidence": 1.0,
            "reason_codes": ["deterministic_exact_match"],
            "top_k_candidates": [],
            "feature_values": {},
            "stage": "path_a_exact_vocab",
        }

    decision = _link_unresolved_mention(
        mention_norm=patha_term,
        universe=universe,
        cfg=cfg,
        mention_meta=mention_metadata,
    )
    prediction = decision["best_candidate"] if decision["accepted"] else ""
    return {
        "raw_mention": str(raw_mention),
        "mention_norm": mention_norm,
        "patha_term": patha_term,
        "patha_exact_vocab_hit": False,
        "prediction": prediction,
        "accepted": bool(decision["accepted"]),
        "score": float(decision["score"]),
        "calibrated_confidence": float(decision["calibrated_confidence"]),
        "reason_codes": list(decision["reason_codes"]),
        "top_k_candidates": decision["top_k_candidates"],
        "feature_values": decision["feature_values"],
        "stage": "path_b_canonical_transparent",
    }


def resolve_note_drugs_hybrid(
    note_terms: Sequence[str],
    ehr_terms: Sequence[str],
    alias_map: Dict[str, str],
    use_embedding: bool,
    threshold: float,
    pathb_mode: str = "embedding_cpu",
    candidate_universe: Optional[CanonicalDrugUniverse] = None,
    pathb_config: Optional[PathBConfig] = None,
    mention_metadata: Optional[Dict[str, dict]] = None,
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

    mapped = []
    unresolved = []

    # Path B transparent canonical-universe linker.
    if pathb_mode == "canonical_transparent":
        universe = candidate_universe or build_canonical_drug_universe(alias_map=alias_map)
        cfg = pathb_config or PathBConfig(min_score=float(threshold))

        # Path A for canonical mode: deterministic exact match to canonical vocabulary.
        for t in note_c:
            exact = universe.synonym_to_canonical.get(t, "")
            if exact:
                mapped.append(exact)
                diagnostics[t] = {
                    "stage": "path_a_exact_vocab",
                    "mention_patha_input": t,
                    "linked_to": exact,
                    "score": 1.0,
                    "calibrated_confidence": 1.0,
                    "accepted": True,
                    "reason_codes": ["deterministic_exact_match"],
                    "top_k_candidates": [],
                    "feature_values": {},
                }
            else:
                unresolved.append(t)

        for t in unresolved:
            decision = _link_unresolved_mention(
                mention_norm=t,
                universe=universe,
                cfg=cfg,
                mention_meta=(mention_metadata or {}).get(t, {}),
            )
            if decision["accepted"] and decision["best_candidate"]:
                mapped.append(decision["best_candidate"])
            else:
                mapped.append(t)
            diagnostics[t] = {
                "stage": "path_b_canonical_transparent",
                "mention_patha_input": t,
                "linked_to": decision["best_candidate"],
                "score": round(float(decision["score"]), 6),
                "calibrated_confidence": round(float(decision["calibrated_confidence"]), 6),
                "accepted": bool(decision["accepted"]),
                "reason_codes": list(decision["reason_codes"]),
                "top_k_candidates": decision["top_k_candidates"],
                "feature_values": decision["feature_values"],
            }

        return sorted(set(mapped)), diagnostics

    # Path A exact canonical match first (legacy EHR-overlap framing).
    for t in note_c:
        if t in ehr_set:
            mapped.append(t)
            diagnostics[t] = {"stage": "path_a", "linked_to": t, "score": 1.0, "accepted": True}
        else:
            unresolved.append(t)

    # Path B legacy embedding-style matcher for unresolved terms.
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
