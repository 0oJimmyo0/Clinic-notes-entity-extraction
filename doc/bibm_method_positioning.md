BIBM METHOD POSITIONING (CLINIC-NOTE-ONLY, ADJUDICATION-FIRST)

Primary venue fit:
- Biomedical and Health Informatics: information retrieval, ontology-aware NLP/text mining.

Secondary fit:
- Clinical and health information systems.
- Electronic health records and standards.

Scope:
- Treatment-context medication extraction and normalization from clinic notes.
- Primary truth source is note-grounded adjudication (LLM-assisted, human-reviewed).
- Structured EHR medications are used only for downstream concordance/verification.

Out-of-scope framing:
- Not a structured-EHR-as-gold-standard extraction benchmark.
- Not a deep-learning-vs-rules model-comparison study.
- Not a SOTA extraction-accuracy claim.

Refined controlled normalization definitions:

1) Baseline (surface-only exact canonical lookup)
- Lowercase normalization.
- Punctuation and whitespace cleanup.
- Simple token cleanup.
- Exact match to canonical vocabulary only.
- No alias map, no fuzzy retrieval, no same-visit EHR matching.

Question:
How much is recoverable from literal/surface normalization plus exact canonical lookup?

2) Path A (deterministic, high-precision, intentionally incomplete)
A1. Deterministic lexical cleanup.
A2. Exact canonical vocabulary match.
A3. Curated stable alias mapping (brand to ingredient, vetted local shorthand).
A4. Safe deterministic decomposition only when unambiguous.

Governance:
- Alias artifact is versioned CSV with metadata columns:
  alias_raw, alias_normalized, canonical_label, mapping_type, confidence, include_flag, notes, source_reference.
- Explicit exclusion list for ambiguous abbreviations and regimen shorthand.
- Regression check: no active alias maps to multiple canonical labels.

Question:
How much additional high-confidence gain comes from transparent deterministic normalization beyond baseline?

3) Path B (calibrated abstaining linker over canonical vocabulary)
- Candidate retrieval from canonical vocabulary (exact, alias, token overlap, char n-gram shortlist).
- Transparent scoring features (exact match, alias hit, token overlap, containment, edit similarity, ingredient overlap).
- Hard guards (too short, weak score, low top1-top2 margin, ambiguity penalties).
- Frequent abstention when confidence is insufficient.
- Outputs top-k candidates, score, calibrated confidence, accept/reject, reason codes.

Question:
After deterministic normalization saturates, how much safe incremental recovery is possible before precision-coverage tradeoff becomes unfavorable?

Layered study claims:
1. Note-grounded adjudication first (truth definition).
2. Controlled normalization gains second (baseline -> path A -> path B).
3. Downstream EHR concordance third (verification, not extraction truth).
