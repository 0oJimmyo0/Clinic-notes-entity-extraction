#!/usr/bin/env python3
"""
Downloader + builder for public clinical vocabularies.

Outputs lexicon files expected by data_brief.ipynb under:
  lexicons/

Design:
- Tries fully automated downloads where possible.
- Falls back to clear "manual download required" instructions when a source
  needs login/acceptance (for example LOINC downloads).
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable
from urllib.error import URLError, HTTPError
from urllib.request import urlopen, urlretrieve


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "resources" / "terminologies" / "raw"
LEXICON_DIR = PROJECT_ROOT / "resources" / "lexicons"
# If this dir exists with NCIt, the build step will delegate to process_raw_vocabularies (Option A).
PROCESSOR_RAW_DIR = PROJECT_ROOT / "resources" / "raw"


def _slug(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip().lower())


def _dedupe(items: Iterable[str]) -> list[str]:
    seen = set()
    out = []
    for item in items:
        val = _slug(item)
        if not val or val in seen:
            continue
        seen.add(val)
        out.append(val)
    return out


def _write_terms_csv(path: Path, terms: Iterable[str]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    cleaned = _dedupe(terms)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["term"])
        for t in cleaned:
            writer.writerow([t])
    return len(cleaned)


def _safe_fetch_text(url: str) -> str:
    with urlopen(url, timeout=60) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def _download(url: str, out_file: Path) -> bool:
    out_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        urlretrieve(url, out_file)
        return True
    except (URLError, HTTPError):
        return False


def resolve_rxnorm_prescribe_zip() -> str | None:
    page = _safe_fetch_text(
        "https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html"
    )
    # Typical pattern: .../RxNorm_full_prescribe_YYYYMMDD.zip
    m = re.search(
        r"https://download\.nlm\.nih\.gov/umls/kss/rxnorm/RxNorm_full_prescribe_[0-9]{8}\.zip",
        page,
    )
    return m.group(0) if m else None


def resolve_ncit_flat_zip() -> str | None:
    page = _safe_fetch_text("https://evs.nci.nih.gov/evs-download/thesaurus-downloads")
    m = re.search(
        r"https://evs\.nci\.nih\.gov/ftp1/NCI_Thesaurus/Thesaurus_[0-9]{2}\.[0-9]{2}[a-z]?\.FLAT\.zip",
        page,
        flags=re.IGNORECASE,
    )
    return m.group(0) if m else None


def resolve_icd10cm_zip(year: int) -> str | None:
    # CDC directory listing with downloadable zip files.
    base = f"https://ftp.cdc.gov/pub/health_statistics/nchs/publications/ICD10CM/{year}/"
    try:
        page = _safe_fetch_text(base)
    except Exception:
        return None
    m = re.search(
        rf"icd10cm-Code(?:\s|%20)Descriptions-{year}\.zip",
        page,
        flags=re.IGNORECASE,
    )
    if not m:
        return None
    fname = m.group(0).replace(" ", "%20")
    return base + fname


@dataclass
class DownloadItem:
    key: str
    resolver: Callable[[], str | None] | None
    manual_url: str
    output_name: str
    notes: str = ""


DOWNLOAD_ITEMS = [
    DownloadItem(
        key="rxnorm_prescribe",
        resolver=resolve_rxnorm_prescribe_zip,
        manual_url="https://www.nlm.nih.gov/research/umls/rxnorm/docs/prescribe.html",
        output_name="rxnorm_prescribe.zip",
        notes="Public RxNorm Prescribable Content (no paid license).",
    ),
    DownloadItem(
        key="ncit_flat",
        resolver=resolve_ncit_flat_zip,
        manual_url="https://evs.nci.nih.gov/evs-download/thesaurus-downloads",
        output_name="ncit_flat.zip",
        notes="NCI Thesaurus FLAT file (tab-delimited inside zip).",
    ),
    DownloadItem(
        key="icd10cm",
        resolver=lambda: resolve_icd10cm_zip(2026),
        manual_url="https://www.cdc.gov/nchs/icd/icd-10-cm/files.html",
        output_name="icd10cm.zip",
        notes="ICD-10-CM code descriptions.",
    ),
    DownloadItem(
        key="loinc",
        resolver=None,
        manual_url="https://loinc.org/downloads/",
        output_name="loinc.zip",
        notes="Requires free account/login. Place downloaded zip at resources/terminologies/raw/loinc.zip",
    ),
    DownloadItem(
        key="icd10pcs",
        resolver=None,
        manual_url="https://www.cms.gov/medicare/coding-billing/icd-10-codes",
        output_name="icd10pcs.zip",
        notes="Download ICD-10-PCS tables zip manually.",
    ),
    DownloadItem(
        key="ctcae",
        resolver=None,
        manual_url="https://dctd.cancer.gov/research/ctep-trials/for-sites/adverse-events",
        output_name="ctcae.xlsx",
        notes="Download CTCAE v5 spreadsheet manually.",
    ),
]


def cmd_download(_: argparse.Namespace) -> int:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    manifest = []
    print(f"[info] Raw directory: {RAW_DIR}")
    for item in DOWNLOAD_ITEMS:
        out_file = RAW_DIR / item.output_name
        status = "manual_required"
        resolved_url = None
        if item.resolver:
            try:
                resolved_url = item.resolver()
            except Exception:
                resolved_url = None
            if resolved_url:
                ok = _download(resolved_url, out_file)
                status = "downloaded" if ok else "auto_failed"
        manifest.append(
            {
                "key": item.key,
                "status": status,
                "resolved_url": resolved_url,
                "manual_url": item.manual_url,
                "output_file": str(out_file),
                "notes": item.notes,
            }
        )
        print(f"[{item.key}] {status} -> {out_file.name}")
        if status != "downloaded":
            print(f"  manual: {item.manual_url}")
    manifest_path = RAW_DIR / "download_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[info] Wrote manifest: {manifest_path}")
    return 0


def _iter_zip_members(zip_path: Path, pattern: str) -> Iterable[tuple[str, bytes]]:
    regex = re.compile(pattern, flags=re.IGNORECASE)
    with zipfile.ZipFile(zip_path) as zf:
        for name in zf.namelist():
            if regex.search(name):
                yield name, zf.read(name)


def build_drugs_from_rxnorm() -> list[str]:
    zip_path = RAW_DIR / "rxnorm_prescribe.zip"
    if not zip_path.exists():
        return []
    terms = []
    for name, data in _iter_zip_members(zip_path, r"RXNCONSO\.RRF$"):
        for line in data.decode("utf-8", errors="ignore").splitlines():
            cols = line.split("|")
            if len(cols) < 15:
                continue
            sab = cols[11]
            tty = cols[12]
            term = cols[14]
            # Keep RxNorm names, prioritize clinically useful term types.
            if sab == "RXNORM" and tty in {"IN", "PIN", "SCD", "SBD", "BN", "MIN"}:
                terms.append(term)
    return _dedupe(terms)


def build_conditions_from_icd10cm() -> list[str]:
    zip_path = RAW_DIR / "icd10cm.zip"
    if not zip_path.exists():
        return []
    terms = []
    for _, data in _iter_zip_members(zip_path, r"(code|tabular|description).*\.txt$"):
        for line in data.decode("utf-8", errors="ignore").splitlines():
            # Typical line format starts with code then description.
            m = re.match(r"^[A-Z0-9\.]+\s+(.+)$", line.strip())
            if m:
                terms.append(m.group(1))
    return _dedupe(terms)


def build_measurements_from_loinc() -> list[str]:
    zip_path = RAW_DIR / "loinc.zip"
    if not zip_path.exists():
        return []
    terms = []
    # Typical LOINC bundle has LoincTable/Loinc.csv
    for _, data in _iter_zip_members(zip_path, r"Loinc(?:Table)?/?Loinc.*\.csv$"):
        text = data.decode("utf-8", errors="ignore")
        reader = csv.DictReader(text.splitlines())
        for row in reader:
            for key in ("LONG_COMMON_NAME", "SHORTNAME", "COMPONENT"):
                if key in row and row[key]:
                    terms.append(row[key])
    return _dedupe(terms)


def build_procedures_from_icd10pcs() -> list[str]:
    zip_path = RAW_DIR / "icd10pcs.zip"
    if not zip_path.exists():
        return []
    terms = []
    for _, data in _iter_zip_members(zip_path, r"(pcs|code|table|order).*\.(txt|csv)$"):
        text = data.decode("utf-8", errors="ignore")
        for line in text.splitlines():
            # heuristic extraction of trailing description.
            m = re.match(r"^[A-Z0-9\.]+\s+(.+)$", line.strip())
            if m:
                terms.append(m.group(1))
    return _dedupe(terms)


def build_ncit_terms() -> tuple[list[str], list[str], list[str]]:
    """
    Returns:
      (context_terms, progression_terms, improvement_terms)
    """
    zip_path = RAW_DIR / "ncit_flat.zip"
    if not zip_path.exists():
        return [], [], []
    context_terms, progression_terms, improvement_terms = [], [], []
    for _, data in _iter_zip_members(zip_path, r"\.txt$"):
        for line in data.decode("utf-8", errors="ignore").splitlines():
            ll = line.lower()
            if any(k in ll for k in ("regimen", "protocol", "chemotherapy", "cycle")):
                context_terms.append(line)
            if any(k in ll for k in ("progression", "progressive disease", "refractory", "resistant")):
                progression_terms.append(line)
            if any(k in ll for k in ("remission", "response", "improved", "stable disease")):
                improvement_terms.append(line)
    return _dedupe(context_terms), _dedupe(progression_terms), _dedupe(improvement_terms)


def build_toxicity_from_ctcae() -> list[str]:
    xlsx_path = RAW_DIR / "ctcae.xlsx"
    if not xlsx_path.exists():
        return []
    try:
        import pandas as pd
    except Exception:
        print("[warn] pandas not available; skipping ctcae.xlsx parsing")
        return []
    terms = []
    try:
        df = pd.read_excel(xlsx_path)
        for c in df.columns:
            if "term" in c.lower() or "adverse" in c.lower():
                terms.extend([str(v) for v in df[c].dropna().tolist()])
    except Exception:
        return []
    return _dedupe(terms)


def default_action_terms() -> dict[str, list[str]]:
    return {
        "start": [
            "start",
            "started",
            "initiate",
            "initiated",
            "begin",
            "began",
            "commence",
            "commenced",
            "resume",
            "resumed",
            "continue",
            "continued",
        ],
        "stop": [
            "stop",
            "stopped",
            "discontinue",
            "discontinued",
            "cease",
            "ceased",
            "off therapy",
            "d/c",
        ],
        "hold": ["hold", "held", "on hold", "paused", "suspended"],
        "dose_change": [
            "dose increased",
            "dose decreased",
            "dose reduced",
            "dose adjusted",
            "increased dose",
            "decreased dose",
            "reduced dose",
            "adjusted dose",
        ],
    }


def cmd_build(_: argparse.Namespace) -> int:
    # If Option A layout exists (raw with NCIt), use the processor so treatment
    # actions and other reasons come from NCIt/CDISC instead of built-in lists only.
    ncit_path = PROCESSOR_RAW_DIR / "NCIt" / "Thesaurus.txt"
    if PROCESSOR_RAW_DIR.exists() and ncit_path.exists():
        try:
            from process_raw_vocabularies import run as processor_run
        except ImportError:
            import importlib.util
            spec = importlib.util.spec_from_file_location(
                "process_raw_vocabularies",
                Path(__file__).parent / "process_raw_vocabularies.py",
            )
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            processor_run = mod.run
        counts, source = processor_run(PROCESSOR_RAW_DIR, LEXICON_DIR)
        LEXICON_DIR.mkdir(parents=True, exist_ok=True)
        print(f"[info] Used process_raw_vocabularies.py (Option A) with {PROCESSOR_RAW_DIR}")
        print(f"[info] Lexicons written to: {LEXICON_DIR}")
        for k, v in sorted(counts.items()):
            src = source.get(k, "?")
            print(f"  {k}: {v} terms  ({src})")
        summary = {
            "raw_dir": str(PROCESSOR_RAW_DIR),
            "lexicon_dir": str(LEXICON_DIR),
            "counts": counts,
            "source": source,
            "notes": "Built via process_raw_vocabularies (Option A layout present).",
        }
        (LEXICON_DIR / "build_summary.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8"
        )
        return 0

    # Option B: build from zips in resources/terminologies/raw
    LEXICON_DIR.mkdir(parents=True, exist_ok=True)

    drugs = build_drugs_from_rxnorm()
    conditions = build_conditions_from_icd10cm()
    measurements = build_measurements_from_loinc()
    procedures = build_procedures_from_icd10pcs()
    ctx_terms, prog_terms, improv_terms = build_ncit_terms()
    tox_terms = build_toxicity_from_ctcae()

    # EHR entity columns
    counts = {}
    counts["ehr_entities__drugs.csv"] = _write_terms_csv(
        LEXICON_DIR / "ehr_entities__drugs.csv", drugs
    )
    counts["ehr_entities__conditions.csv"] = _write_terms_csv(
        LEXICON_DIR / "ehr_entities__conditions.csv", conditions
    )
    counts["ehr_entities__measurements.csv"] = _write_terms_csv(
        LEXICON_DIR / "ehr_entities__measurements.csv", measurements
    )
    counts["ehr_entities__procedures.csv"] = _write_terms_csv(
        LEXICON_DIR / "ehr_entities__procedures.csv", procedures
    )

    # Candidate lexicons (dictionary-driven)
    for action, terms in default_action_terms().items():
        fname = f"candidate_treatment_actions__{action}.csv"
        counts[fname] = _write_terms_csv(LEXICON_DIR / fname, terms)

    # Discontinuation reasons
    counts["candidate_discontinuation_reasons__toxicity.csv"] = _write_terms_csv(
        LEXICON_DIR / "candidate_discontinuation_reasons__toxicity.csv",
        tox_terms
        or [
            "toxicity",
            "adverse event",
            "side effect",
            "intolerance",
            "not tolerated",
        ],
    )
    counts["candidate_discontinuation_reasons__progression.csv"] = _write_terms_csv(
        LEXICON_DIR / "candidate_discontinuation_reasons__progression.csv",
        prog_terms or ["progression", "progressive disease", "refractory", "resistant"],
    )
    counts["candidate_discontinuation_reasons__improvement.csv"] = _write_terms_csv(
        LEXICON_DIR / "candidate_discontinuation_reasons__improvement.csv",
        improv_terms or ["response", "remission", "improved", "resolved"],
    )
    # Non-clinical reasons are generally not in major public medical ontologies.
    for label, defaults in {
        "cost": ["cost", "expensive", "insurance", "cannot afford"],
        "logistics": ["availability", "access", "unable to obtain", "pharmacy delay"],
        "patient_preference": ["patient preference", "declined", "refused", "opted"],
        "completion": ["completed treatment", "completed course", "finished"],
    }.items():
        fname = f"candidate_discontinuation_reasons__{label}.csv"
        counts[fname] = _write_terms_csv(LEXICON_DIR / fname, defaults)

    # Treatment context
    counts["candidate_treatment_context__regimen.csv"] = _write_terms_csv(
        LEXICON_DIR / "candidate_treatment_context__regimen.csv",
        ctx_terms or ["regimen", "protocol", "chemotherapy", "immunotherapy"],
    )
    counts["candidate_treatment_context__cycle.csv"] = _write_terms_csv(
        LEXICON_DIR / "candidate_treatment_context__cycle.csv",
        ["cycle 1", "cycle 2", "cycle 3", "day 1 of cycle", "cycle number"],
    )
    counts["candidate_treatment_context__response.csv"] = _write_terms_csv(
        LEXICON_DIR / "candidate_treatment_context__response.csv",
        improv_terms + prog_terms or ["stable disease", "progressive disease", "partial response"],
    )

    print(f"[info] Wrote lexicons to: {LEXICON_DIR}")
    for k, v in sorted(counts.items()):
        print(f"  {k}: {v} terms")

    summary = {
        "raw_dir": str(RAW_DIR),
        "lexicon_dir": str(LEXICON_DIR),
        "counts": counts,
        "notes": [
            "If counts are very low, check missing raw files in resources/terminologies/raw/",
            "Run: python script/build_public_lexicons.py download",
            "Then manually place login-required files (LOINC, optional CTCAE/ICD10PCS), and run build.",
        ],
    }
    (LEXICON_DIR / "build_summary.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Public vocabulary downloader + lexicon builder")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("download", help="Download what can be auto-downloaded; write manifest for manual steps")
    sub.add_parser("build", help="Build lexicon CSV files from downloaded raw vocab files")
    return p


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.cmd == "download":
        return cmd_download(args)
    if args.cmd == "build":
        return cmd_build(args)
    parser.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
