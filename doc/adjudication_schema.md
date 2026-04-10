# Adjudication Schema

## Purpose

The adjudication schema defines the note-grounded reference set for treatment-context medication mentions.

The adjudication unit is a medication mention-level record with a stable ID. Multiple mentions of the same drug may appear in the same note if their status or local context differs.

## Required Fields

- `adjudication_unit_id`
- `person_id`
- `visit_id`
- `note_id`
- `span_id_or_local_reference`
- `raw_mention_text`
- `context_text`
- `adjudicated_canonical_label`
- `mention_status`
- `compare_to_structured_ehr`
- `reviewer_notes`

## Allowed `mention_status` Values

- `active_current`
- `newly_started_or_prescribed`
- `planned_or_considering`
- `discontinued_or_stopped`
- `held_or_paused`
- `historical_prior`
- `reference_only_or_discussion_only`
- `unclear`

## Allowed `compare_to_structured_ehr` Values

- `yes`
- `no`
- `uncertain`

## Core Rules

### Truth Rule

- Note-grounded adjudication determines whether extraction was correct.
- Structured EHR is not used to decide whether an extracted note mention is true or false.

### Structured-EHR Comparability Rule

- `compare_to_structured_ehr` is a downstream analysis flag, not a truth flag.
- Historical, discussion-only, comparison-only, or clearly non-active mentions should usually not be compared to structured EHR.
- Current or intended treatment mentions are more likely to be marked comparable.

### Canonical Label Rule

- Use ingredient-level canonicalization by default.
- If a mention cannot be resolved confidently to a specific drug, leave the canonical label blank and use `mention_status = unclear` when appropriate.

### Duplicate And Multi-Mention Rule

- Allow multiple mention records for the same drug within the same note if statuses differ or the local evidence differs.
- Do not collapse `planned_or_considering trastuzumab` and `historical_prior trastuzumab` into one row.
- Duplicate string mentions within the same note may be merged only when they refer to the same local evidence and same adjudicated status.
- Duplicate mentions across different notes in the same visit should remain separate mention-level rows during adjudication; they may be aggregated later for visit-level downstream analyses.

## Join And Traceability Expectations

- `adjudication_unit_id` should be the preferred join key when packets are reviewed in-place.
- `span_id_or_local_reference` should preserve traceability back to the candidate span or local evidence packet.
- If a reviewer adds a missing mention that was not present in the seed packet, the row must still include note, visit, and local reference fields so it can be audited as a potential false negative.
