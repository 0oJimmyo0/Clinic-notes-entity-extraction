# Public Vocabulary Downloader/Builder

Two ways to get medspaCy-ready lexicons under `resources/lexicons/`:

---

## Option A: Process your existing raw files (recommended if you already downloaded)

If you have put raw vocabularies under **`resources/raw`** (extracted zips or folders with different formats), run:

```bash
python resources/script/process_raw_vocabularies.py --raw-dir resources/raw --lexicon-dir resources/lexicons
```

This script **auto-detects** layout and parses each source format into a single `term` column CSV. It supports:

- **RxNorm** (directory with `rrf/RXNCONSO.RRF`) → drugs  
- **ICD-10-CM** (e.g. `Code Descriptions/icd10cm_codes_2026.txt`) → conditions  
- **LOINC** (e.g. `Loinc_2.81/LoincTable/Loinc.csv`) → measurements  
- **ICD-10-PCS** (e.g. `icd10pcs_order_2026.txt`) → procedures  
- **NCI Thesaurus** (`NCIt/Thesaurus.txt`) → treatment context, progression, improvement  
- **CTCAE** (any `.xlsx` with "ctcae" in the name) → toxicity  

See **FORMATS.md** in this folder for exact file layouts and column usage.

---

## Option B: Download + build (automated download where possible)

From project root:

```bash
python resources/script/build_public_lexicons.py download
python resources/script/build_public_lexicons.py build
```

- Raw files go to `resources/terminologies/raw/` by default.  
- **Build step:** If `resources/raw` exists and contains `NCIt/Thesaurus.txt`, `build` runs **Option A** (process_raw_vocabularies) so you get NCIt/CDISC-derived treatment actions and other reasons. Otherwise build uses the downloaded zips in `resources/terminologies/raw/` and built-in lists for actions/reasons.
- Lexicons are always written to `resources/lexicons/`.

Use Option A when you already have raw files in `resources/raw` and want to support varied/extracted layouts.

---

## Output lexicon files (used by the notebook)

- `ehr_entities__conditions.csv`
- `ehr_entities__drugs.csv`
- `ehr_entities__measurements.csv`
- `ehr_entities__procedures.csv`
- `candidate_treatment_actions__start.csv`
- `candidate_treatment_actions__stop.csv`
- `candidate_treatment_actions__hold.csv`
- `candidate_treatment_actions__dose_change.csv`
- `candidate_discontinuation_reasons__toxicity.csv`
- `candidate_discontinuation_reasons__progression.csv`
- `candidate_discontinuation_reasons__improvement.csv`
- `candidate_discontinuation_reasons__cost.csv`
- `candidate_discontinuation_reasons__logistics.csv`
- `candidate_discontinuation_reasons__patient_preference.csv`
- `candidate_discontinuation_reasons__completion.csv`
- `candidate_treatment_context__regimen.csv`
- `candidate_treatment_context__cycle.csv`
- `candidate_treatment_context__response.csv`

All are CSV with header `term` and one term per row.

---

## Notes

- For **CTCAE** Excel parsing, `pandas` and `openpyxl` must be installed; otherwise a small default toxicity list is used.
- If a raw source is missing or unreadable, the processor uses built-in default term lists so the pipeline still runs.

---

## Next step after lexicons are built

Run **data_brief.ipynb**: load the lexicons from `resources/lexicons/` (CSV with column `term`), run NLP and dictionary matching to detect entities, treatment actions, and discontinuation reasons in your free text.
