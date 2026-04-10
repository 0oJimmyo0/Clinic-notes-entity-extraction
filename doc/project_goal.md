PROJECT GOAL

Build an adjudication-first, note-grounded evaluation pipeline for treatment-context medication extraction in clinic notes.

PRIMARY STUDY CLAIM

The main paper is about drug mention extraction and normalization in treatment-context clinical documentation, evaluated against adjudicated note truth.

This is not:
- a structured-EHR-as-gold-standard benchmark
- a same-visit EHR linker paper
- a full-note medication extraction benchmark
- a generic all-domain information extraction paper

FINAL STUDY SCOPE

- primary domain: drugs only
- primary truth source: adjudicated note reference set built from mention packets, LLM-assisted drafting, and human review
- downstream structured EHR: concordance target only
- note scope: treatment-context medication extraction study
- note handling: preserve full note text when available for candidate discovery and context recovery, but keep adjudication focused on medication mentions plus enough local/full-note context to judge meaning

PAPER LAYERS

1. corpus selection
   - define the eligible note cohort and adjudication subset
   - preserve full note payload when available

2. candidate and context extraction
   - Stage 1 finds treatment-context candidate spans
   - Stage 2 provides seed medication mentions and status hints

3. adjudication packet build
   - construct mention-level review packets with stable IDs
   - include raw mention text, local context, note metadata, and optional seed suggestions

4. adjudication join
   - join reviewed labels back to seeded extracted mentions
   - produce adjudicated mention truth tables, extraction-vs-truth alignments, and Path B leftover files

5. primary extraction evaluation
   - evaluate extraction correctness against adjudicated note truth
   - no structured EHR input required

6. primary normalization/linking evaluation
   - evaluate baseline, Path A, and Path B against adjudicated canonical drug labels
   - Path B uses a canonical drug vocabulary and abstains frequently when uncertain

7. downstream note-to-EHR concordance
   - only after adjudication
   - only on adjudicated note mentions marked comparable to structured EHR
   - this is a secondary documentation concordance analysis, not the extraction truth criterion

WHAT THE REPO SHOULD CONTINUE TO PRESERVE

- the current candidate extraction backbone
- the current stage-2 extraction utilities
- cohort manifests and note traceability
- visit-level downstream concordance code
- deterministic Path A and explicit unresolved leftovers

WHAT THE REPO SHOULD NO LONGER PRESENT AS PRIMARY EVALUATION

- note-vs-EHR overlap as the main metric
- same-visit EHR drug lists as the Path B candidate universe
- Step 4 similarity as if it were extraction truth evaluation
