from __future__ import annotations

import ast
import json
import re
from typing import List, Sequence, Tuple

import pandas as pd

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

MEAS_PROC_EXACT_NOISE = {
    "diagnosis",
    "history",
    "history of present illness",
    "problem list",
    "medications",
    "medication management",
    "pain assessment",
    "assessment",
    "allergies",
    "location",
    "status",
    "patient",
    "other",
}

MEAS_PROC_CONTAINS_PATTERNS = [
    r"\bhistory of present illness\b",
    r"\bproblem list\b",
    r"\bmedication management\b",
    r"\bpain assessment\b",
    r"\breview of systems\b",
    r"\bchief complaint\b",
]


def normalize_id(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    return s.replace({"": None, "nan": None, "none": None, "None": None})


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
        t = str(v).strip().lower()
        if t:
            out.append(t)
    return sorted(set(out))


def normalize_term(term: str, domain: str) -> str:
    t = str(term).strip().lower()
    if not t:
        return ""

    t = re.sub(r"\([^)]*\)", " ", t)
    t = t.replace("/", " ").replace("_", " ").replace("-", " ")

    if domain == "drugs":
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

    if re.fullmatch(r"[a-z]\d{2,6}", t):
        return ""
    if re.fullmatch(r"\d{3,8}", t):
        return ""
    if t in GENERIC_NOISE_TOKENS:
        return ""

    if domain in {"measurements", "procedures"}:
        if t in MEAS_PROC_EXACT_NOISE:
            return ""
        for pat in MEAS_PROC_CONTAINS_PATTERNS:
            if re.search(pat, t):
                return ""

    if domain == "drugs":
        toks = [x for x in t.split() if len(x) >= 3 and x not in GENERIC_NOISE_TOKENS]
        if not toks:
            return ""
        t = " ".join(toks[:3])

    return t


def preprocess_terms(terms: Sequence[str], domain: str) -> List[str]:
    out = []
    for x in terms:
        t = normalize_term(x, domain)
        if t:
            out.append(t)
    return sorted(set(out))


def relaxed_match(a: str, b: str) -> bool:
    if not a or not b:
        return False
    if a == b:
        return True
    if len(a) >= 4 and a in b:
        return True
    if len(b) >= 4 and b in a:
        return True
    ta = [t for t in a.split() if len(t) >= 3 and t not in GENERIC_NOISE_TOKENS]
    tb = [t for t in b.split() if len(t) >= 3 and t not in GENERIC_NOISE_TOKENS]
    if not ta or not tb:
        return False
    inter = set(ta) & set(tb)
    if not inter:
        return False
    return (len(inter) / min(len(set(ta)), len(set(tb)))) >= 0.5


def jaccard(a: Sequence[str], b: Sequence[str]) -> float:
    sa, sb = set(a), set(b)
    if not sa and not sb:
        return 1.0
    if not sa or not sb:
        return 0.0
    return len(sa & sb) / len(sa | sb)


def containment_note_in_ehr(note_terms: Sequence[str], ehr_terms: Sequence[str]) -> float:
    sn, se = set(note_terms), set(ehr_terms)
    if not sn:
        return 1.0
    if not se:
        return 0.0
    return len(sn & se) / len(sn)


def containment_note_in_ehr_relaxed(note_terms: Sequence[str], ehr_terms: Sequence[str]) -> Tuple[float, int]:
    sn, se = list(note_terms), list(ehr_terms)
    if not sn:
        return 1.0, 0
    if not se:
        return 0.0, 0
    matched = 0
    for n in sn:
        if any(relaxed_match(n, e) for e in se):
            matched += 1
    return matched / len(sn), int(matched > 0)


def infer_timeline_from_visit_id(note_df: pd.DataFrame, ehr_df: pd.DataFrame) -> pd.DataFrame:
    keys = pd.concat(
        [
            note_df[["person_id", "visit_id"]].copy(),
            ehr_df[["person_id", "visit_id"]].copy(),
        ],
        ignore_index=True,
    ).drop_duplicates()
    keys["visit_id_num"] = pd.to_numeric(keys["visit_id"], errors="coerce")
    keys = keys.sort_values(["person_id", "visit_id_num", "visit_id"], na_position="last").reset_index(drop=True)
    keys["visit_rank"] = keys.groupby("person_id").cumcount()
    keys["visit_start_date"] = pd.Timestamp("2000-01-01") + pd.to_timedelta(keys["visit_rank"], unit="D")
    return keys[["person_id", "visit_id", "visit_start_date"]].copy()


def build_windowed_ehr(ehr_df: pd.DataFrame, timeline_df: pd.DataFrame, k: int, domains: Sequence[str]) -> pd.DataFrame:
    if k == 0:
        return ehr_df.copy()

    t = timeline_df.copy()
    t["person_id"] = normalize_id(t["person_id"])
    t["visit_id"] = normalize_id(t["visit_id"])
    t["visit_start_date"] = pd.to_datetime(t["visit_start_date"], errors="coerce")
    t = t.dropna(subset=["person_id", "visit_id", "visit_start_date"])
    t = t.sort_values(["person_id", "visit_start_date", "visit_id"]).reset_index(drop=True)
    t["visit_rank"] = t.groupby("person_id").cumcount()

    e = ehr_df.copy()
    e["person_id"] = normalize_id(e["person_id"])
    e["visit_id"] = normalize_id(e["visit_id"])
    e = e.merge(t[["person_id", "visit_id", "visit_rank"]], on=["person_id", "visit_id"], how="left")
    e = e.dropna(subset=["visit_rank"]).copy()
    e["visit_rank"] = e["visit_rank"].astype(int)

    by_person = {}
    for _, row in e.reset_index().iterrows():
        by_person.setdefault(row["person_id"], {})[row["visit_rank"]] = int(row["index"])

    out_rows = []
    for _, row in e.iterrows():
        pid = row["person_id"]
        r = int(row["visit_rank"])
        ranks = range(r - k, r + k + 1)

        merged_domains = {d: set() for d in domains}
        rank_map = by_person.get(pid, {})
        for rr in ranks:
            idx = rank_map.get(rr)
            if idx is None:
                continue
            src = e.loc[idx]
            for d in domains:
                merged_domains[d].update(src[d])

        out_rows.append(
            {
                "person_id": pid,
                "visit_id": row["visit_id"],
                **{d: sorted(merged_domains[d]) for d in domains},
            }
        )

    return pd.DataFrame(out_rows)


def compute_domain_similarity(
    note_df: pd.DataFrame,
    ehr_df: pd.DataFrame,
    *,
    domain: str,
    window_k: int,
    method_label: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    merged = note_df.merge(ehr_df, on=["person_id", "visit_id"], how="inner", suffixes=("_note", "_ehr"))

    pair_rows = []
    for _, row in merged.iterrows():
        note_terms = preprocess_terms(row[f"{domain}_note"], domain)
        ehr_terms = preprocess_terms(row[f"{domain}_ehr"], domain)
        cont_relaxed, has_relaxed = containment_note_in_ehr_relaxed(note_terms, ehr_terms)

        pair_rows.append(
            {
                "person_id": row["person_id"],
                "visit_id": row["visit_id"],
                "window_k": int(window_k),
                "method_label": method_label,
                f"{domain}_jaccard": jaccard(note_terms, ehr_terms),
                f"{domain}_containment": containment_note_in_ehr(note_terms, ehr_terms),
                f"{domain}_containment_relaxed": cont_relaxed,
                f"{domain}_note_n": len(note_terms),
                f"{domain}_ehr_n": len(ehr_terms),
                f"{domain}_has_overlap": int(len(set(note_terms) & set(ehr_terms)) > 0),
                f"{domain}_has_overlap_relaxed": int(has_relaxed),
            }
        )

    pair_df = pd.DataFrame(pair_rows)
    summary_df = pd.DataFrame(
        [
            {
                "window_k": int(window_k),
                "domain": domain,
                "method_label": method_label,
                "n_pairs": int(len(pair_df)),
                "mean_jaccard": float(pair_df[f"{domain}_jaccard"].mean()) if len(pair_df) else 0.0,
                "mean_containment_note_in_ehr": float(pair_df[f"{domain}_containment"].mean()) if len(pair_df) else 0.0,
                "mean_containment_note_in_ehr_relaxed": float(pair_df[f"{domain}_containment_relaxed"].mean()) if len(pair_df) else 0.0,
                "overlap_rate": float(pair_df[f"{domain}_has_overlap"].mean()) if len(pair_df) else 0.0,
                "overlap_rate_relaxed": float(pair_df[f"{domain}_has_overlap_relaxed"].mean()) if len(pair_df) else 0.0,
                "mean_note_terms": float(pair_df[f"{domain}_note_n"].mean()) if len(pair_df) else 0.0,
                "mean_ehr_terms": float(pair_df[f"{domain}_ehr_n"].mean()) if len(pair_df) else 0.0,
            }
        ]
    )
    return summary_df, pair_df
