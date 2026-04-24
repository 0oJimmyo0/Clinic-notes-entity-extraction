# Clinic-Note Medication Normalization (Paper README)

This README is the working outline for the 8-page conference paper.

## Paper Frame (Frozen)
Study type:
- clinic-note-only
- adjudication-first
- treatment-context medication normalization

Main claims:
1. Note-grounded adjudication is required; structured EHR is not extraction truth.
2. Surface-exact normalization is insufficient.
3. Deterministic, auditable Path A materially improves canonical medication mapping.
4. Remaining Path A errors show where aliases, ambiguity, and context still limit rule-based normalization.

Out of scope for the main paper:
- SOTA extraction benchmarking
- structured-EHR-as-gold extraction framing
- Path B as a primary result (Path B can be mentioned briefly as exploratory/negative).

## 8-Page Paper Structure
1. Introduction
   - Why note-grounded truth is needed in clinic notes.
   - Why deterministic normalization matters after extraction.

2. Related Work
   - Clinical NLP extraction and normalization.
   - Note vs structured-EHR mismatch literature.

3. Data and Cohort Design
   - Clinic-note-only cohort definitions.
   - Adjudication subset construction and rationale.

4. Methods
   - Candidate extraction and mention packets.
   - LLM-assisted human adjudication protocol.
   - Controlled normalization ladder:
     - surface-exact baseline
     - + lexical cleanup
     - + curated alias map
     - + safe decomposition / full Path A

5. Results
   - Compact cohort/adjudication/results grounding table (dataset scale + evaluation units).
   - Mention-level extraction performance table (TP/FP/FN, precision/recall/F1).
   - Mention-level normalization ladder (primary table).
   - Remaining Path A failure taxonomy.
   - Note medication-density table.
   - Optional visit-level sensitivity (secondary).

6. Discussion
   - Why deterministic gains are large and auditable.
   - Why remaining errors point to alias coverage/context limits.
   - Note-vs-EHR concordance as secondary analysis.

7. Limitations and Future Work
   - Adjudication and cohort limitations.
   - Targeted alias coverage expansions.

8. Conclusion
   - Clinic-note-only, adjudication-first normalization takeaway.

## Workflow for This Project
Primary workflow (end-to-end):
1. Cohort and note preparation
   - `script/run_select_note_corpus.py`
   - `script/run_attach_full_note_text.py`
2. Candidate extraction
   - `script/run_candidates_overnight.py`
   - `script/run_stage2_overnight.py`
3. Adjudication packet construction
   - `script/run_build_adjudication_packets.py`
4. Adjudication join and truth tables
   - `script/run_join_adjudication_labels.py`
5. Primary extraction evaluation
   - `script/run_rq1_step4_note_truth_eval.py`
6. Primary normalization evaluation (baseline vs Path A)
   - `script/run_rq1_step5_normalization_eval.py`
7. Secondary downstream concordance (optional/secondary in paper)
   - `script/run_rq1_step6_downstream_concordance.py`
8. Paper output assembly
   - `script/run_rq1_step5_make_outputs.py`
   - `script/run_rq1_patha_paper_outputs.py` (Path A-focused paper bundle)

## Critical Script Functions (Paper-Relevant)
- `script/run_join_adjudication_labels.py`
  - Joins reviewed adjudication labels to mention packets.
  - Produces adjudicated mention truth and extraction-vs-truth alignment tables.
- `script/run_rq1_step4_note_truth_eval.py`
  - Mention-level extraction TP/FP/FN vs adjudicated note truth.
- `script/run_rq1_step5_normalization_eval.py`
  - Mention-level normalization outputs with stage columns:
    - `patha_a1_norm`
    - `patha_a2_exact_vocab`
    - `patha_a3_alias`
    - `patha_a4_decomposed`
    - `patha_prediction`
- `script/run_rq1_patha_paper_outputs.py`
  - Builds the paper tables/figures for the Path A-focused framing.
  - Exports cohort/adjudication/results grounding table, extraction performance table, normalization ladder, Path A failure taxonomy, note density tables, and compact visuals.

## Frozen Key Results to Include (Current Rerun)
Source cohort/run:
- `episode_extraction_results/clinic_like_20k_30k/rq1`

Compact cohort/adjudication/results grounding counts:
- full eligible visits: `95,078`
- evaluation visits: `10,828`
- adjudication visits: `5,000`
- packet notes: `10,543`
- packet mentions: `31,282`
- total adjudicated mention rows used in normalization: `27,552`
- notes with 0 / 1 / >=2 adjudicated meds: `21,378 / 2,159 / 5,168`

Mention-level extraction performance (note-grounded adjudication reference):
- TP: `19,044`
- FP: `13,096`
- FN: `8,508`
- precision: `0.592533`
- recall: `0.691202`
- F1: `0.638075`

Mention-level normalization ladder (`n_mentions = 27,552`):
- surface-exact baseline: `0.809016`
- + lexical cleanup: `0.871334` (`+0.062319` vs previous)
- + curated alias map: `0.936484` (`+0.065150` vs previous)
- + safe decomposition / full Path A: `0.936484` (`+0.000000` vs previous)
- full Path A delta vs surface-exact: `+0.127468` (12.75 points)
- explicit ablation interpretation: lexical cleanup + curated aliases account for measurable gain; safe deterministic decomposition adds no measurable lift in this cohort.

Remaining Path A failures (`n=1,750`):
- missing alias: `1,694` (96.80%)
- combination/formulation mismatch: `30` (1.71%)
- lab/substance/non-medication: `22` (1.26%)
- ambiguous abbreviation: `4` (0.23%)

Note medication-density (manifest denominator):
- 0 meds: `21,378` (74.47%)
- 1 med: `2,159` (7.52%)
- >=2 meds: `5,168` (18.00%)

Companion note-density (conditioned on notes with >=1 adjudicated medication mention; `n=7,327`):
- 1 mention: `2,159` (29.47%)
- 2 mentions: `1,620` (22.11%)
- 3 mentions: `1,070` (14.60%)
- 4 mentions: `682` (9.31%)
- >=5 mentions: `1,796` (24.51%)

Important interpretation note:
- The note-density denominator is the adjudication note manifest (`28,705` notes), while normalization accuracy is computed only on adjudicated mention rows (`27,552` mention-level rows).
- Therefore, “0 meds” in this table means “no adjudicated medication mention captured in current adjudication tables for that note,” not automatically “the note is unusable” or “the note truly has no medication content.”

## Paper-Ready Artifacts
Path A focused outputs:
- `episode_extraction_results/clinic_like_20k_30k/rq1/paper_outputs_patha/`

Key files:
- `rq1_table_cohort_adjudication_results_compact.csv`
- `rq1_table_extraction_performance_mention_level.csv`
- `rq1_table_normalization_ladder_patha_focus.csv`
- `rq1_table_patha_failure_taxonomy.csv`
- `rq1_table_note_med_density.csv`
- `rq1_table_note_med_density_detailed.csv`
- `rq1_table_note_med_density_conditioned_ge1.csv`
- `rq1_table_note_med_density_stats.csv`
- `rq1_table_visit_level_sensitivity.csv`
- `rq1_fig_workflow_patha_focus.svg`
- `rq1_fig_normalization_ladder_patha_focus.svg`
- `rq1_fig_patha_failure_taxonomy.svg`
- `rq1_fig_note_med_density.svg`
- `rq1_fig_note_med_density_conditioned_ge1.svg`
- `rq1_methods_note_patha_focus.md`
- `rq1_results_paragraph_patha_focus.md`

## Scope Guardrails (Do Not Drift)
- Keep adjudicated note-grounded truth as the primary reference.
- Keep mention-level normalization as the primary endpoint.
- Keep downstream note-vs-EHR concordance as secondary.
- Do not restart Path B tuning for this paper version.
