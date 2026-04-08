#!/usr/bin/env python3
"""
RQ1 Step 3: Build structured EHR visit-level entity table.

Input per domain can be either:
1) a single file (.csv or .parquet), OR
2) a folder containing chunk files (.csv/.parquet)

Each input needs at least:
  - person_id
  - visit_id (or visit_occurrence_id)
  - concept_name (or domain-specific concept name column)

Output:
  episode_extraction_results/rq1_ehr_entities_by_visit.csv
with columns:
  person_id, visit_id, conditions, drugs, measurements, procedures
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd


VISIT_CANDIDATES = ["visit_id", "visit_occurrence_id"]
PERSON_CANDIDATES = ["person_id"]
CONCEPT_CANDIDATES = [
    # Preferred human-readable concept names
    "concept_name",
    "condition_concept_name",
    "drug_concept_name",
    "measurement_concept_name",
    "procedure_concept_name",
    # Common OMOP source text fallbacks (still human-readable in many datasets)
    "condition_source_value",
    "drug_source_value",
    "measurement_source_value",
    "procedure_source_value",
    "source_value",
    # Last-resort IDs (least ideal for semantic similarity, but keeps pipeline moving)
    "condition_concept_id",
    "drug_concept_id",
    "measurement_concept_id",
    "procedure_concept_id",
]


def pick_col(df: pd.DataFrame, candidates: Iterable[str]) -> Optional[str]:
    cols = {c.lower(): c for c in df.columns}
    for c in candidates:
        if c.lower() in cols:
            return cols[c.lower()]
    return None


def normalize_entities(series: pd.Series, is_id_col: bool = False) -> pd.Series:
    s = series.astype(str).str.strip().str.lower()
    if is_id_col:
        # Keep IDs explicit so they are not confused with free text.
        s = "concept_id:" + s
    return (
        s
        .replace({"": None, "nan": None, "none": None})
    )


def load_icd10cm_map(icd_codes_path: Path) -> dict:
    """
    Load ICD-10-CM code -> description map from lines like:
      C060 Malignant neoplasm of cheek mucosa
      C06.0 Malignant neoplasm of cheek mucosa
    """
    mapping = {}
    if not icd_codes_path.exists():
        return mapping
    with icd_codes_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            m = re.match(r"^([A-Z0-9\.]+)\s+(.+)$", line)
            if not m:
                continue
            code = m.group(1).strip().upper()
            desc = m.group(2).strip().lower()
            if code and desc:
                mapping[code] = desc
                mapping[code.replace(".", "")] = desc
    return mapping


def load_icd10pcs_map(icd10pcs_order_path: Path) -> dict:
    """
    Load ICD-10-PCS code -> long title map from order file lines like:
      00002 0016070 1 ...  Bypass Cerebral Ventricle to Nasopharynx ...
    """
    mapping = {}
    if not icd10pcs_order_path.exists():
        return mapping
    with icd10pcs_order_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.rstrip("\n")
            if not line.strip():
                continue
            m = re.match(r"^\S+\s+([0-9A-HJ-NP-Z]{7})\s+\S+\s+.+?\s{2,}(.+)$", line)
            if not m:
                continue
            code = m.group(1).strip().upper()
            desc = m.group(2).strip().lower()
            if code and desc:
                mapping[code] = desc
    return mapping


def map_condition_source_to_desc(val: str, icd_map: dict) -> str:
    """
    Map condition_source_value to ICD description when value looks like ICD code.
    Falls back to original value if no mapping found.
    """
    s = str(val).strip()
    if not s:
        return s
    # Try first token and exact full value as candidate code.
    cands = [s.split()[0], s]
    for c in cands:
        code = c.strip().upper()
        desc = icd_map.get(code) or icd_map.get(code.replace(".", ""))
        if desc:
            return desc
    return s.lower()


def _extract_code_candidates(val: str) -> List[str]:
    s = str(val).strip().upper()
    if not s:
        return []
    toks = re.split(r"[\s,;|/]+", s)
    cands = [s] + toks
    out = []
    seen = set()
    for c in cands:
        c = c.strip().strip(".")
        if not c:
            continue
        c = c.replace(".", "")
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


def map_procedure_source_to_desc(val: str, icd10pcs_map: dict) -> str:
    s = str(val).strip()
    if not s:
        return s
    for c in _extract_code_candidates(s):
        if re.fullmatch(r"[0-9A-HJ-NP-Z]{7}", c):
            desc = icd10pcs_map.get(c)
            if desc:
                return desc
    return s.lower()


def normalize_drug_term(val: str) -> str:
    s = str(val).strip().lower()
    if not s:
        return s
    s = re.sub(r"\([^)]*\)", " ", s)
    s = s.replace("/", " ").replace("_", " ").replace("-", " ")
    s = re.sub(r"\b\d+(\.\d+)?\s*(mg|mcg|g|ml|l|unit|units|%)\b", " ", s)
    s = re.sub(r"\b(po|iv|im|sc|subq|subcutaneous|intravenous|oral|topical|pf)\b", " ", s)
    s = re.sub(
        r"\b(tablet|tabs?|capsule|caps|solution|syrup|injection|injectable|ointment|spray|suspension|patch|cream|drop|drops)\b",
        " ",
        s,
    )
    s = re.sub(r"\bj\d{4,6}\b", " ", s)  # HCPCS-like J-codes
    s = re.sub(r"\bndc[:\s-]*\d+\b", " ", s)
    s = re.sub(r"\b(builder|carrier fluid|irrigation|vumc|o r)\b", " ", s)
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""
    toks = [x for x in s.split() if len(x) >= 3]
    if not toks:
        return ""
    return " ".join(toks[:3])


def normalize_id(series: pd.Series) -> pd.Series:
    """
    Normalize merge keys across mixed dtypes (int/float/object).
    Converts to integer-like strings when possible; otherwise stripped string.
    """
    s = series.astype(str).str.strip()
    s = s.replace({"": None, "nan": None, "none": None, "None": None})

    def _clean(v):
        if v is None:
            return None
        x = str(v).strip()
        # normalize float-looking integers like "123.0"
        m = re.match(r"^-?\d+\.0+$", x)
        if m:
            return x.split(".")[0]
        return x

    return s.map(_clean)


def _read_any(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported file type: {path}")


def _resolve_input_files(path: Path) -> List[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        files = sorted(list(path.glob("*.parquet")) + list(path.glob("*.csv")))
        return files
    return []


def load_domain(
    path: Path,
    domain_name: str,
    icd_map: Optional[dict] = None,
    icd10pcs_map: Optional[dict] = None,
) -> pd.DataFrame:
    files = _resolve_input_files(path)
    if not files:
        raise FileNotFoundError(f"{domain_name} input not found or empty: {path}")

    parts = []
    pcol = vcol = ccol = None
    is_id_col = False
    for i, f in enumerate(files, start=1):
        df = _read_any(f)
        if i == 1:
            pcol = pick_col(df, PERSON_CANDIDATES)
            vcol = pick_col(df, VISIT_CANDIDATES)
            ccol = pick_col(df, CONCEPT_CANDIDATES)
            if pcol is None or vcol is None or ccol is None:
                raise ValueError(
                    f"{domain_name}: cannot find required columns in {f}. "
                    f"Need person_id, visit_id/visit_occurrence_id, concept_name-like column. "
                    f"Columns={list(df.columns)}"
                )
            is_id_col = ccol.lower().endswith("_concept_id")
            print(
                f"{domain_name}: detected columns -> person_id={pcol}, visit_id={vcol}, concept={ccol}"
            )
        if pcol not in df.columns or vcol not in df.columns or ccol not in df.columns:
            # Try re-detect for this chunk in case schema differs slightly
            p_i = pick_col(df, PERSON_CANDIDATES)
            v_i = pick_col(df, VISIT_CANDIDATES)
            c_i = pick_col(df, CONCEPT_CANDIDATES)
            if p_i is None or v_i is None or c_i is None:
                raise ValueError(
                    f"{domain_name}: required columns missing in {f}; columns={list(df.columns)}"
                )
            use_cols = [p_i, v_i, c_i]
        else:
            use_cols = [pcol, vcol, ccol]
        part = df[use_cols].copy()
        part.columns = ["person_id", "visit_id", "concept_name"]
        parts.append(part)

    df = pd.concat(parts, ignore_index=True)
    print(f"{domain_name}: loaded {len(df):,} rows from {len(files)} file(s)")
    pcol = pick_col(df, PERSON_CANDIDATES)
    vcol = pick_col(df, VISIT_CANDIDATES)
    ccol = pick_col(df, CONCEPT_CANDIDATES)
    if pcol is None or vcol is None or ccol is None:
        raise ValueError(
            f"{domain_name}: cannot find required columns. "
            f"Need person_id, visit_id/visit_occurrence_id, concept_name-like column. "
            f"Columns={list(df.columns)}"
        )

    out = df[[pcol, vcol, ccol]].copy()
    out.columns = ["person_id", "visit_id", "concept_name"]
    out["person_id"] = normalize_id(out["person_id"])
    out["visit_id"] = normalize_id(out["visit_id"])
    out = out[out["person_id"].notna() & out["visit_id"].notna()]
    ccol_l = ccol.lower() if ccol else ""
    if domain_name == "conditions" and icd_map and ccol_l in {"condition_source_value", "source_value"}:
        out["concept_name"] = out["concept_name"].apply(lambda x: map_condition_source_to_desc(x, icd_map))
    if domain_name == "procedures" and icd10pcs_map and ccol_l in {"procedure_source_value", "source_value"}:
        out["concept_name"] = out["concept_name"].apply(lambda x: map_procedure_source_to_desc(x, icd10pcs_map))
    if domain_name == "drugs" and not is_id_col:
        out["concept_name"] = out["concept_name"].apply(normalize_drug_term)

    out["concept_name"] = normalize_entities(out["concept_name"], is_id_col=is_id_col)
    out = out[out["concept_name"].notna()]
    if is_id_col:
        print(
            f"{domain_name}: WARNING using concept_id fallback for entities. "
            "For semantic similarity in RQ1, concept_name/source_value is preferred."
        )
    return out


def aggregate_domain(df: pd.DataFrame, out_col: str) -> pd.DataFrame:
    agg = (
        df.groupby(["person_id", "visit_id"], dropna=False)["concept_name"]
        .agg(lambda s: json.dumps(sorted(set(s.tolist()))))
        .reset_index()
    )
    agg = agg.rename(columns={"concept_name": out_col})
    return agg


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="RQ1 Step 3 structured EHR visit-level builder")
    p.add_argument("--conditions-path", required=True, help="Conditions file or folder (.csv/.parquet)")
    p.add_argument("--drugs-path", required=True, help="Drugs file or folder (.csv/.parquet)")
    p.add_argument("--measurements-path", required=True, help="Measurements file or folder (.csv/.parquet)")
    p.add_argument("--procedures-path", required=True, help="Procedures file or folder (.csv/.parquet)")
    p.add_argument(
        "--output-csv",
        default="episode_extraction_results/rq1/rq1_ehr_entities_by_visit.csv",
        help="Output visit-level structured CSV",
    )
    p.add_argument(
        "--restrict-to-note-visits-csv",
        default="",
        help="Optional note-visit file (e.g., episode_extraction_results/rq1/rq1_note_entities_by_visit.csv) to restrict rows",
    )
    p.add_argument(
        "--icd-codes-path",
        default="resources/raw/ICD-10-CM/Code Descriptions/icd10cm_codes_2026.txt",
        help="ICD-10-CM codes file for condition_source_value -> description mapping.",
    )
    p.add_argument(
        "--icd10pcs-order-path",
        default="resources/raw/ICD-10-PCS/zip-file-4-2026-icd-10-pcs-order-file-long-and-abbreviated-titles/icd10pcs_order_2026.txt",
        help="ICD-10-PCS order file for procedure_source_value -> long title mapping.",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    root = Path(__file__).resolve().parents[2]

    cond_path = (root / args.conditions_path).resolve()
    drug_path = (root / args.drugs_path).resolve()
    meas_path = (root / args.measurements_path).resolve()
    proc_path = (root / args.procedures_path).resolve()
    out_path = (root / args.output_csv).resolve()
    icd_path = (root / args.icd_codes_path).resolve()
    icd10pcs_path = (root / args.icd10pcs_order_path).resolve()
    icd_map = load_icd10cm_map(icd_path)
    icd10pcs_map = load_icd10pcs_map(icd10pcs_path)
    if icd_map:
        print(f"Loaded ICD map entries: {len(icd_map):,} from {icd_path}")
    else:
        print(f"ICD map unavailable (no file or empty): {icd_path}")
    if icd10pcs_map:
        print(f"Loaded ICD-10-PCS map entries: {len(icd10pcs_map):,} from {icd10pcs_path}")
    else:
        print(f"ICD-10-PCS map unavailable (no file or empty): {icd10pcs_path}")

    print("Loading structured domain files...")
    cond = load_domain(cond_path, "conditions", icd_map=icd_map, icd10pcs_map=icd10pcs_map)
    drug = load_domain(drug_path, "drugs", icd_map=icd_map, icd10pcs_map=icd10pcs_map)
    meas = load_domain(meas_path, "measurements", icd_map=icd_map, icd10pcs_map=icd10pcs_map)
    proc = load_domain(proc_path, "procedures", icd_map=icd_map, icd10pcs_map=icd10pcs_map)

    print(
        f"Rows loaded: conditions={len(cond):,}, drugs={len(drug):,}, "
        f"measurements={len(meas):,}, procedures={len(proc):,}"
    )

    cagg = aggregate_domain(cond, "conditions")
    dagg = aggregate_domain(drug, "drugs")
    magg = aggregate_domain(meas, "measurements")
    pagg = aggregate_domain(proc, "procedures")

    # outer merge across all domains
    merged = cagg.merge(dagg, on=["person_id", "visit_id"], how="outer")
    merged = merged.merge(magg, on=["person_id", "visit_id"], how="outer")
    merged = merged.merge(pagg, on=["person_id", "visit_id"], how="outer")

    for col in ["conditions", "drugs", "measurements", "procedures"]:
        if col not in merged.columns:
            merged[col] = "[]"
        merged[col] = merged[col].fillna("[]")

    if args.restrict_to_note_visits_csv:
        note_path = (root / args.restrict_to_note_visits_csv).resolve()
        note_df = pd.read_csv(note_path)
        if "person_id" not in note_df.columns or "visit_id" not in note_df.columns:
            raise ValueError("restrict file must contain person_id and visit_id")
        keys = note_df[["person_id", "visit_id"]].copy()
        keys["person_id"] = normalize_id(keys["person_id"])
        keys["visit_id"] = normalize_id(keys["visit_id"])
        keys = keys.dropna().drop_duplicates()
        merged["person_id"] = normalize_id(merged["person_id"])
        merged["visit_id"] = normalize_id(merged["visit_id"])
        before = len(merged)
        merged = merged.merge(keys, on=["person_id", "visit_id"], how="inner")
        print(f"Restricted to note visits: {before:,} -> {len(merged):,}")

    merged = merged.sort_values(["person_id", "visit_id"]).reset_index(drop=True)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(out_path, index=False)
    print(f"Saved: {out_path}")
    print(f"Visit-level structured rows: {len(merged):,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

