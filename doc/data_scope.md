# Data Scope

## Core Scope

This repository supports a treatment-context medication extraction study in clinic notes.

The intended scope is narrow:

- drug domain only
- treatment-context documentation only
- note-grounded adjudication as the truth source
- downstream structured EHR comparison only after adjudication

## Note Scope Clarification

The study should not be described as a broad full-note medication extraction benchmark.

At the same time, full note text should be preserved when available because it is useful for:

- candidate discovery
- local context recovery
- recall safety when the initial span window is incomplete
- reviewer verification when a local packet alone is insufficient

Therefore:

- full note text may be stored and scanned
- candidate extraction may operate over full note text or chunked full-note payloads
- adjudication should stay focused on medication mention units plus enough local/full-note context to judge status
- final claims should remain treatment-context focused rather than claiming exhaustive medication extraction from any note

## Corpus Layers

### Full eligible cohort

- all cleaned eligible notes/visits after note-quality filtering and deduplication
- used for infrastructure and downstream cohort construction

### Patient-complete paper subcohort

- selected at the patient level
- keeps all cleaned visits and notes for selected patients
- designed to stay within a practical paper-scale note budget while preserving longitudinal context

### Adjudication subset

- note/visit subset used for mention-level LLM plus human review
- enriched for treatment-context relevance and difficult cases

## Structured EHR Use

Structured EHR is not the source of truth for whether a note mention is correct.

Structured EHR is used only for:

- downstream concordance analysis
- documentation-gap analysis
- evaluating how often note-grounded comparable medications align with structured medication records
