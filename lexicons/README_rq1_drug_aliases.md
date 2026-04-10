# RQ1 drug alias map

**Primary file:** `rq1_drug_aliases.csv`  
**Compatibility file:** `rq1_drug_aliases.json`

**Purpose:** Curated Path A alias resource for deterministic, high-precision drug normalization. The CSV is the paper-facing artifact because it records mapping type, confidence, include/exclude status, notes, and source reference. The JSON is retained for backward compatibility.

**Origin and reference:**  
This file was **created for this project** as part of the A+B entity linking rollout (planning alias and freeze scripts). It is a **hand-curated** list of oncology brand/short names to canonical (generic) names, built to support Path A drug normalization in Step 4. Entries were chosen to cover common oncology drugs (hormonals, chemotherapy, immunotherapy, supportive care) likely to appear in notes and structured EHR. There is no automated derivation from an external database.

**“Extended using the error bucket”** means: when you run Step 0 (`run_rq1_step0_freeze_baseline.py`), it writes an **error-bucket CSV** (e.g. `rq1_error_bucket_drugs_unmatched.csv`) containing a sample of **unmatched** cases—visits where note-derived drug terms did not match any EHR term after Path A (and optionally Path B). By **manually reviewing** that CSV, you can spot recurring note terms (e.g. brand names or variants) that should map to a known EHR concept; you then **add those mappings by hand** to `rq1_drug_aliases.json` so future runs can match them. So the map is extended **by you** when you find new pairs in the error bucket—it is not updated automatically by a script.

For verification of brand–generic pairs, standard references include FDA Orange Book, RxNorm (NLM), or Drugs@FDA; for a more comprehensive or reproducible map, consider deriving or supplementing from RxNorm with a consistent citation.

**CSV columns:**
- `alias_raw`
- `alias_normalized`
- `canonical_label`
- `mapping_type`
- `confidence`
- `include_flag`
- `notes`
- `source_reference`

Only rows with `include_flag=yes` should be used in active Path A.
