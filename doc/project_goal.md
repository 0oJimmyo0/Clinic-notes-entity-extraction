PROJECT GOAL

Build a paper around a layered evaluation design for treatment-context medication extraction:

1. First, create an adjudicated note-based reference set using LLM extraction + human review.
2. Then evaluate our existing extraction pipeline against that adjudicated note reference.
3. Then evaluate normalization/linking (baseline, Path A, Path B) against adjudicated canonical drug labels.
4. Only after that, compare adjudicated note medications to structured EHR medications as a downstream concordance analysis.

The key paper contribution is NOT “LLM extraction” by itself.
The key contribution is:
- separating extraction truth from structured-EHR concordance
- showing how much deterministic normalization helps
- showing whether Path B adds useful recovery or mainly adds risk
- doing this in a narrow, reproducible treatment-context drug setting


IMMEDIATE NEXT STEP PLAN

STEP 1 — LOCK THE PAPER IDENTITY
Freeze the paper scope as:
- domain: drugs only for primary analyses
- note scope: treatment-context spans only
- primary truth source: adjudicated note reference set
- structured EHR role: downstream reference/concordance target, not extraction gold standard

Do NOT let the project drift into:
- full-note medication extraction
- all-domain extraction
- generic LLM extraction benchmark
- structured EHR as ground truth


STEP 2 — DEFINE THE ANNOTATION / ADJUDICATION SCHEMA
Create a medication mention schema for note review with:
- raw span text
- canonical drug label
- mention status
- whether this mention should be considered comparable to structured EHR

Minimum status labels:
- active/current
- newly prescribed/started
- held/stopped/discontinued
- planned/considering
- historical/prior exposure
- reference-only/comparison/discussion-only
- unclear

Add one binary field:
- compare_to_structured_ehr = yes/no

Rule:
Only statuses that truly represent current or intended patient medication use should usually be marked yes.


STEP 3 — BUILD A PILOT ADJUDICATION SET
Select a pilot subset of notes/visits for manual review.
Run the LLM on those notes first, then have humans review/edit.
Goal of pilot:
- test whether the schema is workable
- identify ambiguous categories
- estimate annotation burden
- refine prompt + review instructions before scaling

Outputs:
- pilot adjudication guideline
- refined label definitions
- examples of hard cases


STEP 4 — CREATE THE ADJUDICATED NOTE REFERENCE SET
After pilot refinement, build the main adjudicated subset.
This should be an evaluation subset, not necessarily the full corpus.

Need:
- clear reviewer instructions
- disagreement resolution protocol
- inter-rater agreement on a subset
- final adjudicated table per medication mention

This adjudicated table should become the main reference for extraction evaluation and normalization evaluation.


STEP 5 — EVALUATE THE CURRENT EXTRACTION PIPELINE AGAINST THE ADJUDICATED NOTE REFERENCE
Evaluate the existing pipeline at the mention level:
- precision
- recall
- F1

Also evaluate status labeling if feasible:
- exact status accuracy or grouped-status accuracy

Important:
Do NOT use structured EHR to judge whether extraction was correct.
Use the adjudicated note reference only.


STEP 6 — EVALUATE NORMALIZATION / LINKING AGAINST ADJUDICATED CANONICAL DRUG LABELS
For all extracted/adjudicated drug mentions, compare:
- baseline normalization
- Path A deterministic normalization
- Path B calibrated ontology-constrained linking

Primary outputs:
- canonical drug match accuracy
- gain baseline -> A
- gain A -> B
- calibration curve / confidence bands for B
- error buckets for A and B

Important:
This stage is about whether extracted mentions are normalized correctly, not yet about structured EHR agreement.


STEP 7 — RUN DOWNSTREAM NOTE-TO-EHR CONCORDANCE ONLY ON IN-SCOPE NOTE MEDICATIONS
Restrict downstream concordance to adjudicated note mentions with compare_to_structured_ehr = yes.

Likely include:
- active/current
- newly prescribed/started
- possibly some planned meds if policy is clear

Likely exclude or analyze separately:
- historical
- discussion-only
- comparison/reference-only
- discontinued/held
- unclear

Then compute visit-level concordance metrics:
- relaxed containment
- relaxed overlap
- Jaccard
- coverage

Compare:
- baseline
- Path A
- Path B


STEP 8 — ADD CALIBRATION + RISK ANALYSIS FOR PATH B
For Path B:
- do threshold sweep
- estimate precision by score band
- create utility-risk curve
- quantify false-link rate from adjudicated sample

Decision rule:
Path B is valuable only if it improves downstream matching at acceptable precision/risk.
A small-gain or boundary result is still acceptable if it shows deterministic saturation.


STEP 9 — DO COHORT EXPLORATION BEFORE FINALIZING THE MAIN ANALYSIS SET
Explore and summarize:
- visits per patient
- notes per patient
- note length
- note type / service
- number of extracted medication mentions per visit
- number of adjudicated in-scope medication mentions per visit

Do NOT automatically discard patients just because they have many visits.
Instead, assess whether they dominate the analysis and then use sensitivity analyses.


STEP 10 — RUN PATIENT-AWARE SENSITIVITY ANALYSES
Main analysis:
- all eligible visits

Sensitivity analyses:
- one random eligible visit per patient
- capped number of visits per patient
- optional exclusion of top 1% high-visit patients only as robustness check
- patient-clustered bootstrap or patient-level resampling

Goal:
show conclusions are not driven by a few frequent-visit patients.


STEP 11 — FREEZE THE PAPER OUTPUTS
Create the minimum final outputs:

Table 1:
Cohort and adjudication subset description

Table 2:
Mention-level extraction results against adjudicated note reference

Table 3:
Normalization/linking results (baseline, A, B) against adjudicated canonical labels

Table 4:
Downstream note-to-EHR concordance for in-scope note medications

Figure 1:
Workflow diagram (LLM+human adjudication -> extraction eval -> normalization/linking -> structured concordance)

Figure 2:
Visits-per-patient and note-level corpus distribution plots

Figure 3:
Path A / Path B gain and calibration-risk plots

Figure 4:
Layered error analysis
- extraction errors
- normalization/linking errors
- note-vs-EHR discordance reasons


STEP 12 — WRITE CLAIMS WE CAN SUPPORT
Allowed claims:
- LLM+human adjudication provides a note-grounded reference set
- extraction accuracy should be judged from the note, not structured EHR alone
- deterministic normalization recovers part of the canonical matching gap
- Path B may add value or may reveal a practical saturation boundary
- structured EHR mismatch often reflects documentation/context differences, not necessarily extraction error

Avoid claims like:
- structured EHR is gold standard
- full-note medication truth
- all-domain generalization
- LLM system is state of the art