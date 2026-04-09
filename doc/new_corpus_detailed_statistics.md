# New Corpus Detailed Statistics and Step 1-4 Freeze

Date: 2026-04-08

This file is now the frozen Step 1-4 methods/status memo using the latest regenerated cohort artifacts.

Primary source artifacts used in this update:
- episode_notes/manifests/cohort_justification_summary.json
- episode_notes/manifests/new_corpus_detailed_statistics_current.json
- episode_notes/manifests/three_cohort_detailed_stats_current.json
- episode_notes/manifests/raw_chunk_text_cap_audit.json
- episode_notes/manifests/step1_4_method_stats.json
- episode_notes/manifests/capped_note_boundary_sample.csv
- episode_extraction_results/rq1/rq1_table_drug_ablation.md
- episode_extraction_results/rq1/step4_refine_metrics.json
- episode_extraction_results/rq1/snapshots/rq1_baseline_snapshot/

## Current Regenerated Corpus Snapshot

### Full eligible cohort
- Visits: 95,078
- Patients: 2,106
- Visits with candidate span: 21,693 (22.82%)
- Visits with structured drugs: 10,828 (11.39%)
- Notes: 276,260
- Notes with length == 2000: 39.82%
- Notes with length >= 1800: 41.41%
- Multi-note visits: 50,267 / 95,078 (52.87%)

### Downstream evaluation cohort
- Visits: 10,828
- Patients: 2,041
- Visits with candidate span: 10,828 (100.00%)
- Visits with structured drugs: 10,828 (100.00%)
- Notes: 64,165
- Notes with length == 2000: 43.37%
- Notes with length >= 1800: 45.08%
- Multi-note visits: 7,313 / 10,828 (67.54%)

### Adjudication subset
- Visits: 5,000
- Patients: 2,051
- Visits with candidate span: 5,000 (100.00%)
- Visits with structured drugs: 2,651 (53.02%)
- Notes: 28,705
- Notes with length == 2000: 44.80%
- Notes with length >= 1800: 46.56%
- Multi-note visits: 3,338 / 5,000 (66.76%)

## Detailed Statistics for the Three Cohorts (Current Run)

### Full eligible cohort (detailed)

Visit-level detail:
- Visits: 95,078
- Patients: 2,106
- Visits per patient quantiles (p1/p5/p25/p50/p75/p90/p95/p99): 1 / 4 / 13 / 31.5 / 60 / 103 / 134 / 214.75
- Visits with candidate spans: 21,693 (22.82%)
- Candidate span count quantiles per visit (p1/p5/p25/p50/p75/p90/p95/p99): 0 / 0 / 0 / 0 / 0 / 3 / 5 / 15
- Candidate note count quantiles per visit (p1/p5/p25/p50/p75/p90/p95/p99): 0 / 0 / 0 / 0 / 0 / 1 / 2 / 5
- Visits with structured drugs: 10,828 (11.39%)

Note-level detail:
- Notes: 276,260
- Unique note IDs: 276,260
- Empty note text: 0 (0.00%)
- Notes per visit quantiles (p1/p5/p25/p50/p75/p90/p95/p99): 1 / 1 / 1 / 2 / 3 / 4 / 6 / 32
- Notes per patient quantiles (p1/p5/p25/p50/p75/p90/p95/p99): 2 / 7 / 36 / 86.5 / 170 / 301 / 411.75 / 726.85
- Visits with >1 note: 50,267 (52.87%)
- Note length quantiles chars (p1/p5/p25/p50/p75/p90/p95/p99): 28 / 56 / 198 / 984 / 2000 / 2000 / 2000 / 2000
- Mean note length: 1085.58
- SD note length: 839.49
- Max note length: 2000
- Notes length == 2000: 110,004 (39.82%)
- Notes length >= 1900: 40.61%
- Notes length >= 1800: 41.41%

### Downstream evaluation cohort (detailed)

Visit-level detail:
- Visits: 10,828
- Patients: 2,041
- Visits per patient quantiles (p1/p5/p25/p50/p75/p90/p95/p99): 1 / 1 / 2 / 5 / 7 / 10 / 12 / 16
- Visits with candidate spans: 10,828 (100.00%)
- Candidate span count quantiles per visit (p1/p5/p25/p50/p75/p90/p95/p99): 1 / 1 / 1 / 3 / 6 / 12 / 20 / 49.73
- Candidate note count quantiles per visit (p1/p5/p25/p50/p75/p90/p95/p99): 1 / 1 / 1 / 1 / 2 / 4 / 9 / 19
- Visits with structured drugs: 10,828 (100.00%)

Note-level detail:
- Notes: 64,165
- Unique note IDs: 64,165
- Empty note text: 0 (0.00%)
- Notes per visit quantiles (p1/p5/p25/p50/p75/p90/p95/p99): 1 / 1 / 1 / 2 / 4 / 10 / 27 / 71
- Notes per patient quantiles (p1/p5/p25/p50/p75/p90/p95/p99): 1 / 2 / 11 / 22 / 42 / 68 / 86 / 144
- Visits with >1 note: 7,313 (67.54%)
- Note length quantiles chars (p1/p5/p25/p50/p75/p90/p95/p99): 28 / 58 / 254 / 1282 / 2000 / 2000 / 2000 / 2000
- Mean note length: 1167.19
- SD note length: 824.32
- Max note length: 2000
- Notes length == 2000: 27,826 (43.37%)
- Notes length >= 1900: 44.19%
- Notes length >= 1800: 45.08%

### Adjudication subset (detailed)

Visit-level detail:
- Visits: 5,000
- Patients: 2,051
- Visits per patient quantiles (p1/p5/p25/p50/p75/p90/p95/p99): 1 / 1 / 2 / 3 / 3 / 3 / 3 / 3
- Visits with candidate spans: 5,000 (100.00%)
- Candidate span count quantiles per visit (p1/p5/p25/p50/p75/p90/p95/p99): 1 / 1 / 1 / 2 / 5 / 11 / 21 / 50
- Candidate note count quantiles per visit (p1/p5/p25/p50/p75/p90/p95/p99): 1 / 1 / 1 / 1 / 2 / 3 / 8 / 21
- Visits with structured drugs: 2,651 (53.02%)

Note-level detail:
- Notes: 28,705
- Unique note IDs: 28,705
- Empty note text: 0 (0.00%)
- Notes per visit quantiles (p1/p5/p25/p50/p75/p90/p95/p99): 1 / 1 / 1 / 2 / 3 / 8 / 24 / 79
- Notes per patient quantiles (p1/p5/p25/p50/p75/p90/p95/p99): 1 / 2 / 4 / 6 / 11 / 34 / 61 / 114
- Visits with >1 note: 3,338 (66.76%)
- Note length quantiles chars (p1/p5/p25/p50/p75/p90/p95/p99): 28 / 60 / 282 / 1440 / 2000 / 2000 / 2000 / 2000
- Mean note length: 1203.26
- SD note length: 816.30
- Max note length: 2000
- Notes length == 2000: 12,861 (44.80%)
- Notes length >= 1900: 45.74%
- Notes length >= 1800: 46.56%

## Length-Cap Preservation Check (Current Data)

Raw chunk text-column audit across 20 source chunk files:
- note_text_full column present: 0/20 chunks
- full_note_text column present: 0/20 chunks
- text column present: 0/20 chunks
- note_text column present: 20/20 chunks

Raw note_text rollup across all chunks:
- Total notes scanned: 400,283
- Notes with length == 2000: 151,006 (37.72%)
- Notes with length > 2000: 0
- Max observed note_text length: 2000

Preservation conclusion:
- At this time, full note content is not preserved in the currently available source chunk text field.
- The active source appears hard-capped at 2000 characters.
- To preserve full content, a true full-text source must be attached (for example via run_attach_full_note_text.py) and then re-run Stage 1/Stage 2 and cohort generation using note_text_full first.

## Step 1: Finalize Cohort Definition (Frozen Methods Rule)

Frozen run configuration (current generated corpus):
- note text column priority: note_text_full, full_note_text, note_text, text
- actual note text column used: note_text
- min note chars: 20
- allowed note types: all (no explicit whitelist)
- template-only exclusion: off
- require structured drugs for downstream evaluation cohort: on
- require candidate spans for adjudication subset: on
- adjudication max visits per patient: 3
- adjudication max total visits: 5,000
- adjudication sampling mode: stratified
- adjudication strata keys: note_type, visit_count_bin, candidate_count_bin

Frozen eligibility and aggregation rules:
- Eligible visit: has valid person_id and visit_occurrence_id and at least one eligible note after note-level filtering and de-dup.
- Included note types: all note_title values (note type is not used as an exclusion gate in this run).
- Multi-note visit aggregation: notes are pooled at visit level for visit metrics, candidate coverage, and downstream concordance pairing.
- Notes pooled across visit: yes.
- Duplicate note versions removed: yes, in two stages.
	1. De-dup by note_id, keeping the latest by note_datetime or note_date when present.
	2. De-dup exact normalized note_text within the same person_id and visit_occurrence_id.
- Structured drugs role:
	- Full eligible cohort: structured drug presence not required.
	- Downstream evaluation cohort: structured drug presence required.
	- Adjudication subset: structured drug presence not required (candidate-rich sampling rule applies).

## Step 2: Quantify Truncation Risk

### 2.1 Percent of notes at the cap
- Full eligible: 39.82% at 2000 chars
- Evaluation: 43.37% at 2000 chars
- Adjudication: 44.80% at 2000 chars

### 2.2 Candidate span density by capped vs non-capped notes

| Cohort | Group | N notes | % notes in cohort | % notes with >=1 candidate | Mean candidates per note | Mean candidates per 1k chars |
|:--|:--|--:|--:|--:|--:|--:|
| Full eligible | Capped (2000) | 110,004 | 39.82% | 19.72% | 0.560 | 0.280 |
| Full eligible | Non-capped | 166,256 | 60.18% | 9.32% | 0.193 | 0.564 |
| Evaluation | Capped (2000) | 27,826 | 43.37% | 49.54% | 1.374 | 0.687 |
| Evaluation | Non-capped | 36,339 | 56.63% | 27.60% | 0.577 | 1.762 |
| Adjudication | Capped (2000) | 12,861 | 44.80% | 49.64% | 1.422 | 0.711 |
| Adjudication | Non-capped | 15,844 | 55.20% | 27.64% | 0.572 | 1.670 |

Interpretation:
- Capped notes are more likely to contain candidates in absolute terms (likely because long, dense clinical narratives hit the cap).
- But candidate density per 1k characters is lower in capped notes than non-capped notes, consistent with possible information loss at the tail.

### 2.3 Boundary-risk review for capped notes

Boundary metric definition:
- near-boundary span = candidate span_end >= 1950 on notes with note_len == 2000

Observed near-boundary rates:
- Full eligible capped notes with near-boundary spans: 1,996 / 110,004 (1.81%)
- Evaluation capped notes with near-boundary spans: 1,255 / 27,826 (4.51%)
- Adjudication capped notes with near-boundary spans: 638 / 12,861 (4.96%)

Targeted sample review file:
- episode_notes/manifests/capped_note_boundary_sample.csv (12-note sample)

Sample signals from that file:
- 9 / 12 note tails end with alphabetic characters (suggesting abrupt cut-off, often mid-token).
- 7 / 12 note tails still contain medication-like terms near the truncation boundary.

Conclusion for Step 2:
- Truncation risk is material and likely recall-limiting.
- Because content beyond 2000 chars is absent, this is a lower-bound estimate of missed mentions.

Mitigation status:
- Implemented tooling to support full-note remediation once source text is available:
	- resources/script/run_attach_full_note_text.py
	- resources/script/run_candidates_overnight.py note-text fallback support
	- resources/script/run_select_note_corpus.py note-text fallback support and cap diagnostics
- Until full text is attached, report truncation as a formal limitation in methods/results.

## Step 3: Freeze Adjudication Subset Policy

### Frozen policy logic
- Source pool: full eligible visits.
- Enrichment gate: keep only visits with >=1 candidate span.
- Patient cap: maximum 3 visits per patient (randomized per-patient ranking with fixed seed).
- Sample size cap: 5,000 visits.
- Sampling mode: proportional stratified sampling by combined strata key:
	- note_type_mode
	- visit_count_bin
	- candidate_count_bin

### Realized subset composition (current run)

Candidate count bin distribution:
- 01: 1,489 (29.78%)
- 02-03: 1,540 (30.80%)
- 04-07: 1,124 (22.48%)
- 08+: 847 (16.94%)

Visit frequency bin distribution:
- 01: 17 (0.34%)
- 02-03: 68 (1.36%)
- 04-06: 261 (5.22%)
- 07-12: 639 (12.78%)
- 13+: 4,015 (80.30%)

Structured-drug composition:
- Adjudication subset: 53.02% structured-drug positive
- Full eligible pool: 11.39% structured-drug positive

Policy gaps to acknowledge before Path B evaluation:
- Not explicitly stratified by note length (long-note burden appears indirectly through candidate and note-type strata).
- Not yet explicitly enriched for unresolved Path A cases.
- Not explicitly balanced by structured-drug presence vs absence.

## Step 4: Refine Baseline and Path A (Before Path B Redesign)

### Frozen operational definitions

Baseline (minimal):
- run_rq1_step4_similarity.py
- method_label: baseline
- drug_normalizer: baseline
- drug_linker: none

Path A (deterministic and auditable):
- run_rq1_step4_similarity.py
- method_label: path_a
- drug_normalizer: v2
- drug_linker: none
- alias map: resources/lexicons/rq1_drug_aliases.json
- alias map size: 30 pairs
- alias map SHA256: ec467c6fae11e800ccf0f80e6bd170d097e8499eaaba3889b0a0dd00f698aec6

Current observed drug ablation (k=0, non-empty note visits):
- Baseline relaxed containment: 11.65%
- Path A relaxed containment: 13.21% (delta +1.56)
- Path A relaxed overlap: 22.44% (delta +2.35 vs baseline)
- Path A+B currently does not improve containment beyond Path A in this table.

Unresolved burden after Path A (current outputs):
- Drug non-empty visits (k=0): 9,164
- Path A unresolved visits (relaxed overlap == 0): 7,108 (77.56%)
- Baseline unresolved visits (relaxed overlap == 0): 7,323 (79.91%)

Interpretation:
- Path A gives measurable but modest gains and remains the correct deterministic layer.
- A large unresolved set remains, which should become the primary target for revised Path B.

Current content evaluation (baseline and Path A):
- Baseline status: acceptable as a minimal reference (frozen, reproducible, no linker side effects).
- Path A status: deterministic and auditable; alias map is versioned and checksummed.
- Path A effectiveness: positive but modest gains over baseline; unresolved burden remains high (77.56%).
- Main limitation before Path B redesign: unresolved set quality is still constrained by upstream note truncation.
- Main opportunity for Path B: large unresolved pool is present and sufficient for calibration-focused redesign once full-text remediation is available or truncation is explicitly accepted as a study limitation.

### Step 4 artifacts produced in this update
- Baseline snapshot frozen:
	- episode_extraction_results/rq1/snapshots/rq1_baseline_snapshot/
- Unmatched drug error bucket refreshed:
	- episode_extraction_results/rq1/diagnostics/rq1_error_bucket_drugs_unmatched.csv (500 rows)

## Step 5 Status (Implemented Redesign)

Step 5 is now implemented in code as a transparent, abstention-capable Path B.

Operational Path B design now wired in `run_rq1_step4_similarity.py`:
- `drug_linker=canonical_transparent`
- Candidate universe from canonical vocabulary inputs (not same-visit EHR terms):
	- alias map (`resources/lexicons/rq1_drug_aliases.json`),
	- optional canonical vocabulary file (`--drug-canonical-vocab-path`),
	- optional adjudicated canonical labels (`--drug-adjudicated-labels-csv`).
- Candidate generation (high recall, controlled):
	- exact normalized/synonym hit,
	- token overlap indexes,
	- first-token retrieval,
	- char-ngram top-k lexical backstop.
- Transparent feature scoring per mention-candidate pair with audit features saved in diagnostics.
- Hard rejection rules enabled:
	- short/noisy mention,
	- score below threshold,
	- top1-top2 margin below threshold,
	- ambiguous short-term rejection,
	- combo mismatch,
	- calibrated confidence below target precision,
	- optional out-of-scope mention status.
- Calibration-aware acceptance:
	- optional calibration JSON (`--drug-linker-calibration-json`),
	- accept only when calibrated confidence reaches target precision (`--drug-linker-min-calibrated-confidence`, default 0.90).

Diagnostics output contract for unresolved Path A mentions now includes:
- best canonical candidate,
- raw score,
- calibrated confidence,
- accept/reject,
- reason codes,
- top-k candidates,
- feature values used by scoring.

Legacy mode remains available for backward comparability:
- `drug_linker=embedding_cpu`