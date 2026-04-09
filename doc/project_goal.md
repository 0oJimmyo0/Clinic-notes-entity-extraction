PROJECT REFINEMENT PLAN

GOAL
Refactor the current repo from an EHR-overlap-first pipeline into a note-grounded evaluation pipeline for treatment-context drug extraction and normalization.

WHAT IS ALREADY WORKING
- Stage 1 candidate extraction from treatment-action / discontinuation-reason / treatment-context lexicons
- Stage 2 medspaCy extraction on candidate spans
- visit-level aggregation
- baseline and Path A style note-vs-EHR comparison
- old Path B char-ngram linker
- basic linker threshold diagnostics

WHAT IS NOT YET ALIGNED WITH THE NEW PROJECT
- current corpus selector is note-level random capped selection, not a clinically justified visit-level evaluation cohort
- current Step 4 uses EHR overlap as the main evaluation target
- current Path B links unresolved note drugs to EHR drug terms from the same visit
- current calibration is not based on adjudicated correct/incorrect labels
- no code yet for evaluation against LLM+human adjudicated note reference

TOP PRIORITY NEXT STEP
Freeze the evaluation corpus definition before changing the linker.

TASK 1 — BUILD A CORPUS PROFILING SCRIPT
Create a new script:
script/run_profile_note_corpus.py

Input:
- chunked note parquet files

Output:
- cohort_summary.json
- patient_level_summary.csv
- visit_level_summary.csv
- note_level_summary.csv
- basic plots or CSVs for:
  - visits per patient
  - notes per patient
  - note length distribution
  - note type distribution
  - service / clinic distribution if available
  - candidate span count per visit
  - extracted drug count per visit (if stage1/stage2 outputs available)

Purpose:
- understand the true distribution before defining inclusion/exclusion rules
- identify duplicates, empty/template notes, extreme-utilization patients, and sparse visits

TASK 2 — REPLACE THE CURRENT RANDOM NOTE SELECTOR WITH A VISIT-LEVEL CORPUS DEFINER
Refactor or replace:
script/run_select_note_corpus.py

New behavior:
- define eligibility at the visit level, not note level
- include a visit if:
  - it has a valid patient id and visit id
  - it has at least one eligible clinic note
  - note text is non-empty
  - the note contains at least one treatment-context candidate span OR is eligible for mentor adjudication
- deduplicate note versions if necessary
- keep a visit manifest, not just a note manifest
- retain note_id(s) per visit for traceability

Add options:
- --min-note-chars
- --allowed-note-types
- --exclude-template-only
- --max-visits-per-patient-for-adjudication
- --sampling-mode {all,stratified}
- --stratify-by {note_type,service,visit_count_bin,candidate_count_bin}

Outputs:
- evaluation_visit_manifest.csv
- adjudication_subset_manifest.csv
- cohort_justification_summary.json

IMPORTANT:
Do not exclude high-visit patients by default.
Instead:
- keep all eligible visits in the full downstream concordance cohort
- for the adjudication subset, cap visits per patient (for example 1–3) so a few frequent-utilization patients do not dominate manual review

TASK 3 — FREEZE TWO DIFFERENT CORPORA
Create two explicit corpus definitions:

A. full downstream cohort
Purpose:
- downstream note-to-EHR concordance after adjudication filters
Rule:
- all eligible visits

B. adjudication/evaluation subset
Purpose:
- mentor LLM+human extraction reference set
Rule:
- visit-level stratified sample
- cap visits per patient
- enrich for difficult cases, including:
  - multiple drug mentions
  - discontinuation / hold / start language
  - long notes
  - unresolved Path A cases
  - ambiguous or noisy mentions

TASK 4 — DEFINE THE INTERFACE TO THE MENTOR’S ADJUDICATED OUTPUT
Before rewriting evaluation scripts, define the exact schema expected from mentor output.

Required columns for adjudicated reference:
- person_id
- visit_id
- note_id
- span_id or adjudication_unit_id
- raw_mention_text
- adjudicated_drug_label
- mention_status
- compare_to_structured_ehr (yes/no)
- adjudication_confidence or review_flag (optional)

Possible mention_status values:
- active_current
- newly_started
- discontinued_stopped
- held_paused
- planned_considering
- historical_prior
- reference_only
- unclear

TASK 5 — SPLIT EVALUATION INTO THREE DISTINCT LAYERS
Create new scripts rather than forcing everything into current Step 4.

A. extraction evaluation
script/run_eval_extraction_against_adjudicated.py
Compare current Stage 2 extracted note mentions against adjudicated note mentions.
Metrics:
- precision
- recall
- F1
- optional status accuracy if labels align

B. normalization/linking evaluation
script/run_eval_normalization_against_adjudicated.py
Compare:
- baseline normalization
- Path A
- revised Path B
against adjudicated canonical drug labels.
Metrics:
- canonical match accuracy
- delta baseline->A
- delta A->B
- unresolved rate
- error buckets

C. downstream concordance
script/run_eval_note_to_ehr_concordance.py
Only include adjudicated note mentions with compare_to_structured_ehr = yes.
Metrics:
- same-visit containment
- relaxed overlap
- Jaccard
- coverage
- optional windowed sensitivity

TASK 6 — REDEFINE BASELINE, PATH A, AND PATH B
Baseline:
- current extraction pipeline + minimal drug normalization only
- lowercase
- whitespace cleanup
- punctuation cleanup
- possibly dosage/route stripping only if you want that in baseline, but keep it conservative

Path A:
- deterministic high-precision normalization
- current alias map plus refinements
- brand-to-generic mapping
- oncology shorthand expansion
- dosage/route/form removal
- J-code / NDC cleanup
- ingredient / combo-drug normalization if needed
- should be versioned and auditable

Path B:
- replace current char-ngram matcher over visit-level EHR terms
- new Path B should link unresolved Path A mentions to a canonical drug vocabulary, not to EHR terms from the same visit
- candidate universe should come from:
  - RxNorm / UMLS term table
  - alias vocabulary
  - adjudicated canonical drug labels if helpful
- recommended design:
  1. lexical candidate generation (BM25, TF-IDF, or token retrieval)
  2. constrained scoring with transparent features:
     - exact token overlap
     - normalized edit similarity
     - containment
     - alias hit
     - first-token/ingredient overlap
  3. hard guards:
     - reject too-short/noisy terms
     - reject low-margin matches
     - reject ties or near-ties
  4. calibration on adjudicated sample
  5. accept only above target precision threshold

IMPORTANT:
Do not call the revised Path B “semantic” unless the actual scoring uses meaningful embeddings.
Otherwise call it “ontology-constrained candidate retrieval and calibrated linking.”

TASK 7 — REWORK THE CURRENT CALIBRATION SCRIPT
Current Step 4b only summarizes overlap deltas and low-confidence acceptance.
Create a true calibration script that uses adjudicated labels.

New outputs should include:
- precision by score band
- recall by score band if possible
- selected operating point
- false-link rate
- accepted/rejected counts
- utility-risk curve

TASK 8 — ADD ERROR ANALYSIS OUTPUTS
Need one error CSV per stage:

A. extraction errors
- missed mention
- hallucinated mention
- wrong span
- wrong status

B. normalization/linking errors
- alias miss
- brand/generic miss
- abbreviation miss
- wrong candidate accepted
- correct candidate rejected

C. downstream note-vs-EHR discordance
- note mention is historical
- note mention is discussion-only
- note mention is planned
- note mention is current but absent from structured EHR
- structured EHR has drug absent from note

TASK 9 — FREEZE THE FIRST PAPER SCOPE
Primary analyses:
- drug domain only
- treatment-context notes only
- adjudicated note reference for extraction and normalization
- structured EHR as secondary downstream concordance target

Do not expand yet to:
- full-note extraction
- all domains
- deep-learning baseline/path C
- external portability unless very low friction