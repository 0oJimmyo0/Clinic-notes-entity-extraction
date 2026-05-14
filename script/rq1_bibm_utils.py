#!/usr/bin/env python3
"""
Shared helpers for BIBM-focused RQ1 paper analyses.
"""

from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Dict, Iterable, Sequence, Set

from rq1_adjudication_utils import parse_list_cell
from rq1_drug_linking import load_alias_entries, normalize_drug_text


def safe_div(num: float, den: float) -> float:
    return float(num) / float(den) if den else 0.0


def wilson_ci(successes: int, total: int, z: float = 1.96) -> tuple[float, float]:
    if total <= 0:
        return 0.0, 0.0
    p = safe_div(successes, total)
    denom = 1.0 + (z**2) / total
    center = (p + (z**2) / (2 * total)) / denom
    spread = z * math.sqrt((p * (1 - p) / total) + ((z**2) / (4 * (total**2)))) / denom
    return max(0.0, center - spread), min(1.0, center + spread)


def load_brand_generic_map(alias_paths: Sequence[Path]) -> Dict[str, str]:
    """
    Build a normalized mapping suitable for ingredient/generic-level collapsing.

    For the current project, we use curated alias artifacts as an RxNorm-style proxy:
    brand/local shorthand -> ingredient/generic when known, identity otherwise.
    """
    out: Dict[str, str] = {}
    for path in alias_paths:
        if not path.exists():
            continue
        for row in load_alias_entries(path):
            alias_norm = normalize_drug_text(row.get("alias_normalized") or row.get("alias_raw") or "")
            canonical_norm = normalize_drug_text(row.get("canonical_label") or "")
            include_flag = str(row.get("include_flag", "yes")).strip().lower()
            if include_flag not in {"yes", "true", "1"}:
                continue
            if alias_norm and canonical_norm:
                out[alias_norm] = canonical_norm
    return out


def ingredient_level_label(term: str, brand_generic_map: Dict[str, str]) -> str:
    norm = normalize_drug_text(term)
    if not norm:
        return ""
    return brand_generic_map.get(norm, norm)


def action_cue(
    seed_treatment_action: str = "",
    seed_discontinuation_reason: str = "",
    context_text: str = "",
) -> str:
    text = " ".join(
        x.strip().lower()
        for x in [seed_treatment_action, seed_discontinuation_reason, context_text]
        if str(x).strip()
    )
    if not text:
        return "other_or_unclear"

    cue_rules = [
        ("start", [r"\b(start|started|initiat|prescrib|begin|new)\b"]),
        ("stop", [r"\b(stop|stopp|discontinu|d c\b|dc\b|off)\b"]),
        ("hold", [r"\b(hold|held|pause|paused)\b"]),
        ("changed", [r"\b(change|changed|switch|switched|taper|reduc|increase|dose)\b"]),
        ("continue", [r"\b(continue|continued|remain on|stay on|keep taking)\b"]),
        ("discussed", [r"\b(discuss|consider|review|counsel|question|plan)\b"]),
    ]
    for label, patterns in cue_rules:
        if any(re.search(pat, text) for pat in patterns):
            return label
    return "other_or_unclear"


_CATEGORY_RULES: list[tuple[str, list[str]]] = [
    (
        "endocrine_hormonal",
        [
            r"\bletrozole\b",
            r"\banastrozole\b",
            r"\bexemestane\b",
            r"\btamoxifen\b",
            r"\bfulvestrant\b",
            r"\bgoserelin\b",
            r"\bleuprolide\b",
            r"\babiraterone\b",
            r"\benzalutamide\b",
            r"\bapalutamide\b",
            r"\bsynthroid\b",
            r"\blevothyroxine\b",
        ],
    ),
    (
        "cytotoxic_chemotherapy",
        [
            r"\bcarboplatin\b",
            r"\bcisplatin\b",
            r"\bdoxorubicin\b",
            r"\bdoxil\b",
            r"\bpaclitaxel\b",
            r"\bdocetaxel\b",
            r"\bcyclophosphamide\b",
            r"\bgemcitabine\b",
            r"\bcapecitabine\b",
            r"\btemozolomide\b",
            r"\boxaliplatin\b",
            r"\bfluorouracil\b",
        ],
    ),
    (
        "targeted_or_oral_oncology",
        [
            r"\bpalbociclib\b",
            r"\bribociclib\b",
            r"\babemaciclib\b",
            r"\balpelisib\b",
            r"\bolaparib\b",
            r"\btalazoparib\b",
            r"\beverolimus\b",
            r"\bibrutinib\b",
            r"\bimbruvica\b",
            r"\btucatinib\b",
            r"\bneratinib\b",
            r"\blapatinib\b",
        ],
    ),
    (
        "immunotherapy_biologic",
        [
            r"\bpembrolizumab\b",
            r"\bnivolumab\b",
            r"\bipilimumab\b",
            r"\batezolizumab\b",
            r"\btrastuzumab\b",
            r"\bpertuzumab\b",
            r"\bado trastuzumab emtansine\b",
            r"\bbevacizumab\b",
            r"\bdenosumab\b",
        ],
    ),
    (
        "supportive_gi_acid",
        [
            r"\branditidine\b",
            r"\bfamotidine\b",
            r"\bomeprazole\b",
            r"\bpantoprazole\b",
            r"\besomeprazole\b",
            r"\bondansetron\b",
            r"\bpromethazine\b",
        ],
    ),
    (
        "analgesic_neurologic_msk",
        [
            r"\boxycodone\b",
            r"\bgabapentin\b",
            r"\bacetaminophen\b",
            r"\bmethocarbamol\b",
            r"\btramadol\b",
            r"\bhydrocodone\b",
        ],
    ),
    (
        "steroid_antiinflammatory",
        [
            r"\bdexamethasone\b",
            r"\bprednisone\b",
            r"\bmethylprednisolone\b",
            r"\bprednisolone\b",
        ],
    ),
    (
        "cardiometabolic",
        [
            r"\baspirin\b",
            r"\bmetformin\b",
            r"\batorvastatin\b",
            r"\btenormin\b",
            r"\batenolol\b",
            r"\blisinopril\b",
        ],
    ),
]


def broad_drug_category(term: str) -> str:
    norm = normalize_drug_text(term)
    if not norm:
        return "unknown"
    for label, patterns in _CATEGORY_RULES:
        if any(re.search(pat, norm) for pat in patterns):
            return label
    return "other_medication"


def list_from_json_cell(cell) -> list[str]:
    return [normalize_drug_text(x) for x in parse_list_cell(cell) if normalize_drug_text(x)]


def set_metrics(left: Iterable[str], right: Iterable[str]) -> Dict[str, float | int]:
    left_set: Set[str] = {str(x).strip() for x in left if str(x).strip()}
    right_set: Set[str] = {str(x).strip() for x in right if str(x).strip()}
    inter = left_set & right_set
    union = left_set | right_set
    return {
        "left_n": len(left_set),
        "right_n": len(right_set),
        "intersection_n": len(inter),
        "union_n": len(union),
        "exact_set_match": int(left_set == right_set),
        "overlap_any": int(bool(inter)),
        "jaccard": safe_div(len(inter), len(union)),
        "left_in_right_containment": safe_div(len(inter), len(left_set)),
        "right_in_left_containment": safe_div(len(inter), len(right_set)),
    }
