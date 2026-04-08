#!/usr/bin/env python3
"""
Process raw vocabulary files in resources/raw into medspaCy-ready lexicons.

Raw sources can be zips or extracted folders; this script auto-detects layout
and parses each format into a single "term" column CSV under resources/lexicons/.

Usage:
  python resources/script/process_raw_vocabularies.py [--raw-dir PATH] [--lexicon-dir PATH]

Default:
  --raw-dir      resources/raw
  --lexicon-dir  resources/lexicons
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path
from typing import Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_RAW = PROJECT_ROOT / "resources" / "raw"
DEFAULT_LEXICON = PROJECT_ROOT / "resources" / "lexicons"


def _norm(t: str) -> str:
    return re.sub(r"\s+", " ", t.strip()).lower()


def _dedupe(terms: Iterable[str]) -> list[str]:
    seen = set()
    out = []
    for x in terms:
        v = _norm(x)
        if not v or v in seen:
            continue
        seen.add(v)
        out.append(v)
    return sorted(out)


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
    t = _norm(term)
    if not t:
        return False
    if len(t) > 120:
        return False
    if not re.search(r"[a-z0-9]", t):
        return False

    # Drop very short/ambiguous single-token items unless whitelisted clinical abbreviations.
    if " " not in t and "-" not in t and "/" not in t:
        if len(t) <= 2 and t not in ALLOWED_SHORT_ENTITY_TERMS:
            return False
        if len(t) == 3 and t.isalpha() and t not in ALLOWED_SHORT_ENTITY_TERMS:
            return False
        if t in NOISY_SINGLE_TOKEN_TERMS:
            return False

    # Additional conservative filters for measurements where short abbreviations are very noisy.
    if entity_type == "measurements":
        if t in NOISY_SINGLE_TOKEN_TERMS:
            return False
        if len(t) <= 3 and t not in ALLOWED_SHORT_ENTITY_TERMS:
            return False

    return True


def _filter_entity_terms_for_mentions(terms: Iterable[str], entity_type: str) -> list[str]:
    return _dedupe(t for t in terms if _is_mention_safe_term(t, entity_type))


def _write_lexicon(path: Path, terms: list[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["term"])
        for t in terms:
            w.writerow([t])
    return len(terms)


# ---------- RxNorm (Prescribable) ----------
# Format: directory RxNorm_full_prescribe_* or similar, containing rrf/RXNCONSO.RRF
# RRF = pipe-delimited; columns: ... 11=SAB, 12=TTY, 13=CODE, 14=STR
def _find_rxnorm_rrf(raw_dir: Path) -> Path | None:
    for d in raw_dir.iterdir():
        if not d.is_dir():
            continue
        if "rxnorm" in d.name.lower() and "prescribe" in d.name.lower():
            rrf = d / "rrf" / "RXNCONSO.RRF"
            if rrf.exists():
                return rrf
    return None


def extract_rxnorm_drugs(rrf_path: Path) -> list[str]:
    terms = []
    want_ttys = {"IN", "PIN", "SCD", "SBD", "BN", "MIN"}
    with rrf_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            cols = line.strip().split("|")
            if len(cols) < 15:
                continue
            sab, tty, str_val = cols[11], cols[12], cols[14]
            if sab == "RXNORM" and tty in want_ttys and str_val:
                terms.append(str_val)
    return _dedupe(terms)


# ---------- ICD-10-CM (Conditions) ----------
# Format: directory ICD-10-CM (or similar), containing Code Descriptions/icd10cm_codes_2026.txt
# Line format: "A000    Cholera due to Vibrio cholerae 01, biovar cholerae"
def _find_icd10cm_codes(raw_dir: Path) -> Path | None:
    for d in raw_dir.iterdir():
        if not d.is_dir():
            continue
        if "icd" in d.name.lower() and "cm" in d.name.lower() and "pcs" not in d.name.lower():
            for sub in ["Code Descriptions", "Code Descriptions"]:
                p = d / sub / "icd10cm_codes_2026.txt"
                if p.exists():
                    return p
            for f in d.rglob("icd10cm_codes*.txt"):
                return f
    return None


def extract_icd10cm_conditions(txt_path: Path) -> list[str]:
    terms = []
    with txt_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("Codes") or line.startswith(" "):
                continue
            # Code is first token (alphanumeric + dot); rest is description
            m = re.match(r"^[A-Z0-9\.]+\s+(.+)$", line)
            if m:
                terms.append(m.group(1))
    return _dedupe(terms)


# ---------- LOINC (Measurements) ----------
# Format: directory Loinc_* containing LoincTable/Loinc.csv (or LoincTableCore/LoincTableCore.csv)
# CSV columns: LONG_COMMON_NAME, COMPONENT, SHORTNAME, CONSUMER_NAME
def _find_loinc_table(raw_dir: Path) -> Path | None:
    for d in raw_dir.iterdir():
        if not d.is_dir():
            continue
        if "loinc" in d.name.lower():
            for name in ["LoincTable/Loinc.csv", "LoincTableCore/LoincTableCore.csv"]:
                p = d / name
                if p.exists():
                    return p
    return None


def extract_loinc_measurements(csv_path: Path) -> list[str]:
    terms = []
    with csv_path.open("r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Mention-safe preference: use human-facing names, avoid SHORTNAME/COMPONENT
            # which contain many terse technical fragments in narrative notes.
            for key in ("LONG_COMMON_NAME", "CONSUMER_NAME", "DisplayName"):
                if key in row and row[key]:
                    terms.append(row[key])
    return _dedupe(terms)


# ---------- ICD-10-PCS (Procedures) ----------
# Format: directory ICD-10-PCS, under zip-file-4-*/icd10pcs_order_2026.txt
# Line: "00002 0016070 1 Bypass Cereb Vent...  Bypass Cerebral Ventricle..."
# Last segment (after multiple spaces) is the long title.
def _find_icd10pcs_order(raw_dir: Path) -> Path | None:
    for d in raw_dir.iterdir():
        if not d.is_dir():
            continue
        if "icd" in d.name.lower() and "pcs" in d.name.lower():
            for f in d.rglob("icd10pcs_order*.txt"):
                return f
    return None


def extract_icd10pcs_procedures(txt_path: Path) -> list[str]:
    terms = []
    with txt_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            # Last column is long title (after sequence of 2+ spaces)
            parts = re.split(r"\s{2,}", line)
            if len(parts) >= 2:
                terms.append(parts[-1])
    return _dedupe(terms)


# ---------- NCI Thesaurus (Treatment context / progression / improvement) ----------
# Format: directory NCIt (or similar) containing Thesaurus.txt
# Tab-delimited; column index 3 (4th column) = "Preferred Name|SYNONYM" - split by | for terms
def _find_ncit_thesaurus(raw_dir: Path) -> Path | None:
    for d in raw_dir.iterdir():
        if not d.is_dir():
            continue
        if "ncit" in d.name.lower() or d.name == "NCIt":
            p = d / "Thesaurus.txt"
            if p.exists():
                return p
    return None


def extract_ncit_terms(txt_path: Path) -> tuple[list[str], list[str], list[str]]:
    context, progression, improvement = [], [], []
    with txt_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            cols = line.strip().split("\t")
            if len(cols) < 4:
                continue
            name_syn = cols[3]
            for part in name_syn.split("|"):
                t = _norm(part)
                if not t or len(t) < 3:
                    continue
                lower = t
                if any(k in lower for k in ("regimen", "protocol", "chemotherapy", "immunotherapy", "cycle", "combination therapy")):
                    context.append(t)
                if any(k in lower for k in ("progression", "progressive disease", "refractory", "resistant", "failed therapy")):
                    progression.append(t)
                if any(k in lower for k in ("remission", "response", "improved", "stable disease", "partial response", "complete response")):
                    improvement.append(t)
    return _dedupe(context), _dedupe(progression), _dedupe(improvement)


# ---------- NCI Thesaurus: treatment actions & other discontinuation reasons (CDISC-embedded) ----------
# NCIt includes CDISC SDTM terminology; column 8 = code systems (e.g. "Reason for Non-Completion", "Reason for Treatment Interruption").
# We collect terms from those concepts and map by keyword to our action/reason buckets for wider coverage.
def extract_ncit_actions_and_reasons(txt_path: Path) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Extract treatment-action and other-discontinuation-reason terms from NCIt (CDISC terminologies)."""
    actions: dict[str, list[str]] = {
        "start": [],
        "stop": [],
        "hold": [],
        "dose_change": [],
    }
    reasons: dict[str, list[str]] = {
        "cost": [],
        "logistics": [],
        "patient_preference": [],
        "completion": [],
    }
    reason_non_completion = "reason for non-completion"
    reason_interruption = "reason for treatment interruption"
    reason_for_treatment = "reason for treatment"
    subject_discontinuation = "study subject discontinuation"
    dose_limiting = "dose limiting"
    with txt_path.open("r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            cols = line.strip().split("\t")
            if len(cols) < 9:
                continue
            name_syn = cols[3]
            code_systems = (cols[8] or "").lower()
            terms = []
            for part in name_syn.split("|"):
                t = _norm(part)
                if not t or len(t) < 2:
                    continue
                terms.append(t)
            if not terms:
                continue
            for t in terms:
                lower = t
                if reason_non_completion in code_systems or subject_discontinuation in code_systems:
                    if any(k in lower for k in ("financial", "cost", "insurance", "afford", "burden", "expense", "financial strain", "financial difficulty")):
                        reasons["cost"].append(t)
                    if any(k in lower for k in ("logistical", "logistics", "availability", "access", "unable to obtain", "pharmacy", "lost to follow", "lost to follow-up", "relocation", "lost")):
                        reasons["logistics"].append(t)
                    if any(k in lower for k in ("withdrawal of consent", "withdrew", "consent withdrawn", "refused", "declined", "patient preference", "subject withdrawal", "withdrawal by subject", "parent or guardian", "withdrawal", "withdrawn", "non-compliance", "opted out")):
                        reasons["patient_preference"].append(t)
                    if any(k in lower for k in ("treatment completed", "completed the study", "completed protocol", "completed as prescribed", "completion", "completed", "finished")):
                        reasons["completion"].append(t)
                    # Study Subject Discontinuation concepts also give stop-action synonyms (e.g. "discontinuation", "discontinue")
                    if any(k in lower for k in ("discontinu", "withdraw", "stop", "cease")):
                        actions["stop"].append(t)
                if reason_interruption in code_systems:
                    if any(k in lower for k in ("hold", "pause", "suspend", "interruption", "temporarily")):
                        actions["hold"].append(t)
                    if any(k in lower for k in ("dose reduc", "dose increas", "dose adjust", "forgot to take", "missed dose", "dose modif")):
                        actions["dose_change"].append(t)
                if reason_for_treatment in code_systems:
                    if any(k in lower for k in ("start", "initiate", "begin", "resume", "continue")):
                        actions["start"].append(t)
                    if any(k in lower for k in ("stop", "discontinu", "cease", "withdraw")):
                        actions["stop"].append(t)
                if dose_limiting in code_systems or "dose modification" in code_systems:
                    actions["dose_change"].append(t)
    for k in actions:
        actions[k] = _dedupe(actions[k])
    for k in reasons:
        reasons[k] = _dedupe(reasons[k])
    return actions, reasons


# ---------- CDISC SDTM Terminology Excel (optional) ----------
# If user downloads SDTM Terminology.xls from NCI EVS (evs.nci.nih.gov/ftp1/CDISC/SDTM/),
# we can parse codelist sheets for "Reason for Treatment Discontinuation" / "Reason for Non-Completion" etc.
def _find_sdtm_excel(raw_dir: Path) -> Path | None:
    for f in raw_dir.rglob("*.xls"):
        if "sdtm" in f.name.lower() and "terminology" in f.name.lower():
            return f
    for f in raw_dir.rglob("*.xlsx"):
        if "sdtm" in f.name.lower() and "terminology" in f.name.lower():
            return f
    return None


def extract_sdtm_reasons_and_actions(excel_path: Path) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Parse SDTM Terminology Excel for discontinuation reason and treatment action codelists."""
    try:
        import pandas as pd
    except ImportError:
        return ({}, {})
    actions: dict[str, list[str]] = {"start": [], "stop": [], "hold": [], "dose_change": []}
    reasons: dict[str, list[str]] = {"cost": [], "logistics": [], "patient_preference": [], "completion": []}
    try:
        xl = pd.ExcelFile(excel_path)
        for sheet in xl.sheet_names:
            sh_lower = sheet.lower()
            # Sheets of interest: contain "discontinuation", "non-completion", "interruption", "reason for treatment"
            if "discontinuation" not in sh_lower and "non-completion" not in sh_lower and "interruption" not in sh_lower and "reason for treatment" not in sh_lower:
                continue
            df = pd.read_excel(excel_path, sheet_name=sheet)
            if df.empty or len(df.columns) == 0:
                continue
            # Find column with submission values or terms (CDISC often uses "CDISC Submission Value", "Term", "Preferred Term")
            term_col = None
            for c in df.columns:
                cc = str(c).lower()
                if "submission value" in cc or ("term" in cc and "code" not in cc) or "preferred term" in cc or "cdisc" in cc and "value" in cc:
                    term_col = c
                    break
            if term_col is None:
                term_col = df.columns[0]
            for v in df[term_col].dropna().astype(str):
                t = _norm(v)
                if not t or len(t) < 2:
                    continue
                # Map by keyword to buckets (same as NCIt)
                if any(k in t for k in ("financial", "cost", "insurance", "afford", "burden")):
                    reasons["cost"].append(t)
                if any(k in t for k in ("logistical", "availability", "access", "pharmacy", "lost to follow")):
                    reasons["logistics"].append(t)
                if any(k in t for k in ("withdrawal", "consent", "refused", "declined", "patient")):
                    reasons["patient_preference"].append(t)
                if any(k in t for k in ("completed", "completion", "finished")):
                    reasons["completion"].append(t)
                if any(k in t for k in ("hold", "pause", "suspend", "interruption")):
                    actions["hold"].append(t)
                if any(k in t for k in ("dose reduc", "dose increas", "dose adjust", "dose modif")):
                    actions["dose_change"].append(t)
    except Exception:
        pass
    for k in actions:
        actions[k] = _dedupe(actions[k])
    for k in reasons:
        reasons[k] = _dedupe(reasons[k])
    return actions, reasons


# ---------- CTCAE (Toxicity / adverse events) ----------
# Format: any .xlsx in raw_dir with "ctcae" in filename
# Sheets/columns: look for "Term", "Adverse Event", "Name", "MedDRA Term", etc.
def _find_ctcae_xlsx(raw_dir: Path) -> Path | None:
    for f in raw_dir.glob("*.xlsx"):
        if "ctcae" in f.name.lower():
            return f
    return None


def extract_ctcae_toxicity(xlsx_path: Path) -> list[str]:
    try:
        import pandas as pd
    except ImportError:
        return []
    terms = []
    try:
        xl = pd.ExcelFile(xlsx_path)
        for sheet in xl.sheet_names:
            df = pd.read_excel(xlsx_path, sheet_name=sheet)
            if df.empty or len(df.columns) == 0:
                continue
            # Prefer columns whose names look like term/adverse event
            for c in df.columns:
                c_lower = str(c).lower()
                if any(k in c_lower for k in ("term", "adverse", "name", "meddra", "ae ", "llt", "preferred")):
                    terms.extend([str(v).strip() for v in df[c].dropna().astype(str).tolist() if str(v).strip() and len(str(v)) > 2])
            # If no matching column, use first column (many CTCAE sheets have term in col 0)
            if not terms and len(df.columns) >= 1:
                first = df.iloc[:, 0]
                terms.extend([str(v).strip() for v in first.dropna().astype(str).tolist() if str(v).strip() and len(str(v)) > 2])
    except Exception:
        pass
    return _dedupe(terms)


# ---------- Fallback defaults (when raw file missing or empty) ----------
def default_drugs(): return ["prednisone", "dexamethasone", "paclitaxel", "carboplatin", "cisplatin"]
def default_conditions(): return ["hypertension", "diabetes", "heart failure", "copd", "infection"]
def default_measurements(): return ["blood pressure", "heart rate", "creatinine", "hemoglobin", "glucose", "bmi"]
def default_procedures(): return ["biopsy", "resection", "radiation", "chemotherapy", "port placement", "transfusion"]
def default_toxicity(): return ["toxicity", "adverse event", "side effect", "intolerance", "not tolerated"]
def default_progression(): return ["progression", "progressive disease", "refractory", "resistant"]
def default_improvement(): return ["remission", "response", "improved", "resolved", "stable disease", "partial response"]
def default_context(): return ["regimen", "protocol", "chemotherapy", "immunotherapy", "combination therapy"]

# Defaults for treatment actions and other reasons (used when no external source or to ensure minimum coverage)
def default_actions() -> dict[str, list[str]]:
    return {
        "start": ["start", "started", "initiate", "initiated", "begin", "began", "resume", "resumed", "continue", "continued"],
        "stop": ["stop", "stopped", "discontinue", "discontinued", "cease", "ceased", "d/c", "dc'd"],
        "hold": ["hold", "held", "on hold", "paused", "suspended"],
        "dose_change": ["dose increased", "dose decreased", "dose reduced", "dose adjusted", "increased dose", "decreased dose"],
    }


def default_other_reasons() -> dict[str, list[str]]:
    return {
        "cost": ["cost", "expensive", "insurance", "cannot afford"],
        "logistics": ["availability", "access", "unable to obtain", "pharmacy"],
        "patient_preference": ["patient preference", "declined", "refused", "opted"],
        "completion": ["completed treatment", "completed course", "finished"],
    }


def run(raw_dir: Path, lexicon_dir: Path) -> dict[str, int]:
    raw_dir = raw_dir.resolve()
    lexicon_dir = lexicon_dir.resolve()
    lexicon_dir.mkdir(parents=True, exist_ok=True)
    counts = {}
    source = {}  # "raw" | "builtin" for user visibility

    # ---- Drugs (RxNorm) ----
    rrf = _find_rxnorm_rrf(raw_dir)
    drugs = extract_rxnorm_drugs(rrf) if rrf else []
    drugs = _filter_entity_terms_for_mentions(drugs, "drugs")
    if not drugs:
        drugs = default_drugs()
        drugs = _filter_entity_terms_for_mentions(drugs, "drugs")
        source["ehr_entities__drugs.csv"] = "builtin"
    else:
        source["ehr_entities__drugs.csv"] = "raw"
    counts["ehr_entities__drugs.csv"] = _write_lexicon(lexicon_dir / "ehr_entities__drugs.csv", drugs)

    # ---- Conditions (ICD-10-CM) ----
    icd_cm = _find_icd10cm_codes(raw_dir)
    conditions = extract_icd10cm_conditions(icd_cm) if icd_cm else []
    conditions = _filter_entity_terms_for_mentions(conditions, "conditions")
    if not conditions:
        conditions = default_conditions()
        conditions = _filter_entity_terms_for_mentions(conditions, "conditions")
        source["ehr_entities__conditions.csv"] = "builtin"
    else:
        source["ehr_entities__conditions.csv"] = "raw"
    counts["ehr_entities__conditions.csv"] = _write_lexicon(lexicon_dir / "ehr_entities__conditions.csv", conditions)

    # ---- Measurements (LOINC) ----
    loinc_csv = _find_loinc_table(raw_dir)
    measurements = extract_loinc_measurements(loinc_csv) if loinc_csv else []
    measurements = _filter_entity_terms_for_mentions(measurements, "measurements")
    if not measurements:
        measurements = default_measurements()
        measurements = _filter_entity_terms_for_mentions(measurements, "measurements")
        source["ehr_entities__measurements.csv"] = "builtin"
    else:
        source["ehr_entities__measurements.csv"] = "raw"
    counts["ehr_entities__measurements.csv"] = _write_lexicon(lexicon_dir / "ehr_entities__measurements.csv", measurements)

    # ---- Procedures (ICD-10-PCS) ----
    pcs_order = _find_icd10pcs_order(raw_dir)
    procedures = extract_icd10pcs_procedures(pcs_order) if pcs_order else []
    procedures = _filter_entity_terms_for_mentions(procedures, "procedures")
    if not procedures:
        procedures = default_procedures()
        procedures = _filter_entity_terms_for_mentions(procedures, "procedures")
        source["ehr_entities__procedures.csv"] = "builtin"
    else:
        source["ehr_entities__procedures.csv"] = "raw"
    counts["ehr_entities__procedures.csv"] = _write_lexicon(lexicon_dir / "ehr_entities__procedures.csv", procedures)

    # ---- NCI Thesaurus (context, progression, improvement) ----
    ncit = _find_ncit_thesaurus(raw_dir)
    ctx, prog, improv = extract_ncit_terms(ncit) if ncit else ([], [], [])
    if not ctx:
        ctx = default_context()
    if not prog:
        prog = default_progression()
    if not improv:
        improv = default_improvement()
    source["candidate_treatment_context__regimen.csv"] = "raw" if ncit else "builtin"
    source["candidate_discontinuation_reasons__progression.csv"] = "raw" if ncit else "builtin"
    source["candidate_discontinuation_reasons__improvement.csv"] = "raw" if ncit else "builtin"
    counts["candidate_treatment_context__regimen.csv"] = _write_lexicon(lexicon_dir / "candidate_treatment_context__regimen.csv", ctx)
    counts["candidate_discontinuation_reasons__progression.csv"] = _write_lexicon(lexicon_dir / "candidate_discontinuation_reasons__progression.csv", prog)
    counts["candidate_discontinuation_reasons__improvement.csv"] = _write_lexicon(lexicon_dir / "candidate_discontinuation_reasons__improvement.csv", improv)

    # ---- CTCAE (toxicity) ----
    ctcae = _find_ctcae_xlsx(raw_dir)
    tox = extract_ctcae_toxicity(ctcae) if ctcae else []
    if not tox:
        tox = default_toxicity()
        source["candidate_discontinuation_reasons__toxicity.csv"] = "builtin"
    else:
        source["candidate_discontinuation_reasons__toxicity.csv"] = "raw"
    counts["candidate_discontinuation_reasons__toxicity.csv"] = _write_lexicon(lexicon_dir / "candidate_discontinuation_reasons__toxicity.csv", tox)

    # ---- Treatment context: cycle, response (use NCI + defaults) ----
    source["candidate_treatment_context__cycle.csv"] = "builtin"  # no external source for cycle phrases
    source["candidate_treatment_context__response.csv"] = "raw" if ncit else "builtin"
    counts["candidate_treatment_context__cycle.csv"] = _write_lexicon(
        lexicon_dir / "candidate_treatment_context__cycle.csv",
        ["cycle 1", "cycle 2", "cycle 3", "day 1 of cycle", "day 8 of cycle", "cycle number"],
    )
    response_terms = list(dict.fromkeys(improv + prog)) if ncit else default_improvement() + default_progression()
    counts["candidate_treatment_context__response.csv"] = _write_lexicon(lexicon_dir / "candidate_treatment_context__response.csv", response_terms)

    # ---- Treatment actions: NCIt (CDISC) when available, else builtin ----
    ncit_actions, ncit_reasons = (extract_ncit_actions_and_reasons(ncit) if ncit else ({}, {}))
    builtin_actions = default_actions()
    actions_merged = {}
    for action in ("start", "stop", "hold", "dose_change"):
        combined = list(dict.fromkeys((ncit_actions.get(action) or []) + builtin_actions[action]))
        actions_merged[action] = _dedupe(combined)
    any_action_from_raw = any(ncit_actions.get(a) for a in ("start", "stop", "hold", "dose_change"))
    # Optional: CDISC SDTM Terminology Excel (adds more terms if present)
    sdtm_excel = _find_sdtm_excel(raw_dir)
    sdtm_actions, sdtm_reasons = ({}, {})
    if sdtm_excel:
        sdtm_actions, sdtm_reasons = extract_sdtm_reasons_and_actions(sdtm_excel)
        for action in ("start", "stop", "hold", "dose_change"):
            actions_merged[action] = _dedupe(actions_merged[action] + (sdtm_actions.get(action) or []))
        any_action_from_raw = any_action_from_raw or any(sdtm_actions.get(a) for a in ("start", "stop", "hold", "dose_change"))
    for action, terms in actions_merged.items():
        name = f"candidate_treatment_actions__{action}.csv"
        source[name] = "raw" if (ncit and any_action_from_raw) else "builtin"
        counts[name] = _write_lexicon(lexicon_dir / name, terms)

    # ---- Other discontinuation reasons: NCIt (CDISC) when available, else builtin ----
    builtin_reasons = default_other_reasons()
    reasons_merged = {}
    for label in ("cost", "logistics", "patient_preference", "completion"):
        combined = list(dict.fromkeys((ncit_reasons.get(label) or []) + builtin_reasons[label]))
        reasons_merged[label] = _dedupe(combined)
    any_reason_from_raw = any(ncit_reasons.get(r) for r in ("cost", "logistics", "patient_preference", "completion"))
    if sdtm_excel and sdtm_reasons:
        for label in ("cost", "logistics", "patient_preference", "completion"):
            reasons_merged[label] = _dedupe(reasons_merged[label] + (sdtm_reasons.get(label) or []))
        any_reason_from_raw = any_reason_from_raw or any(sdtm_reasons.get(r) for r in ("cost", "logistics", "patient_preference", "completion"))
    for label, terms in reasons_merged.items():
        name = f"candidate_discontinuation_reasons__{label}.csv"
        source[name] = "raw" if (ncit and any_reason_from_raw) else "builtin"
        counts[name] = _write_lexicon(lexicon_dir / name, terms)

    return counts, source


def main() -> int:
    ap = argparse.ArgumentParser(description="Process raw vocabularies into medspaCy lexicons")
    ap.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW, help="Raw vocabulary root (e.g. resources/raw)")
    ap.add_argument("--lexicon-dir", type=Path, default=DEFAULT_LEXICON, help="Output lexicon directory")
    args = ap.parse_args()
    counts, source = run(args.raw_dir, args.lexicon_dir)
    print(f"Lexicons written to: {args.lexicon_dir}")
    print(f"{'File':<50} {'Terms':>8}  Source")
    print("-" * 62)
    for k in sorted(counts.keys()):
        src = source.get(k, "?")
        print(f"  {k:<48} {counts[k]:>8}  {src}")
    print()
    print("Source: raw = from downloaded vocabulary; builtin = script default (no external file or fallback)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
