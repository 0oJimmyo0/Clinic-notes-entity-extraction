# RQ1 drug alias map

**File:** `rq1_drug_aliases.json`

**Purpose:** Maps brand or shorthand drug names to a canonical (usually generic) name for Path A drug normalization in RQ1 similarity (Step 4). Both note-derived and EHR-derived drug terms are normalized and looked up; matching is by exact equality of canonical form.

**Origin and reference:**  
This file was **created for this project** as part of the A+B entity linking rollout (planning alias and freeze scripts). It is a **hand-curated** list of oncology brand/short names to canonical (generic) names, built to support Path A drug normalization in Step 4. Entries were chosen to cover common oncology drugs (hormonals, chemotherapy, immunotherapy, supportive care) likely to appear in notes and structured EHR. There is no automated derivation from an external database.

**“Extended using the error bucket”** means: when you run Step 0 (`run_rq1_step0_freeze_baseline.py`), it writes an **error-bucket CSV** (e.g. `rq1_error_bucket_drugs_unmatched.csv`) containing a sample of **unmatched** cases—visits where note-derived drug terms did not match any EHR term after Path A (and optionally Path B). By **manually reviewing** that CSV, you can spot recurring note terms (e.g. brand names or variants) that should map to a known EHR concept; you then **add those mappings by hand** to `rq1_drug_aliases.json` so future runs can match them. So the map is extended **by you** when you find new pairs in the error bucket—it is not updated automatically by a script.

For verification of brand–generic pairs, standard references include FDA Orange Book, RxNorm (NLM), or Drugs@FDA; for a more comprehensive or reproducible map, consider deriving or supplementing from RxNorm with a consistent citation.

**Format:** JSON object: keys = normalized brand/short name (lowercase), values = canonical generic (or preferred) name.
