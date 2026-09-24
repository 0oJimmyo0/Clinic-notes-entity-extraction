# Independent Raw-Note Gold Evaluation Contract

Status: **draft for pilot; NOT FROZEN OR SCORED** (2026-09-24)

Purpose: define RQ1's human-gold target and evaluator before the independent sample is drawn or any gold-test predictions/metrics are inspected. This contract is distinct from `adjudication_schema.md`, which starts from candidate packets and cannot measure medications missed by candidate generation. Keep patient IDs, note text, and ID-level manifests in restricted storage, not this document or the manuscript repository.

## Freeze record (complete after the excluded-patient/development pilot)

| Item | Locked value / artifact / hash |
|---|---|
| Governance and full-text authorization | **OPEN** |
| Eligible patient/note frame and exclusion rules | **OPEN** |
| Primary task boundary and annotation-guide version | **OPEN** |
| Patient and note sampling seed, strata, per-patient cap, inclusion probabilities | **OPEN** |
| Primary estimand (note-weighted or patient-weighted) | **OPEN** |
| Full-note pipeline and all resource/code versions, including development-only aliases | **OPEN** |
| Gold annotation file schema and blinded adjudication procedure | **OPEN** |
| Evaluator code/version and matching rules below | **OPEN** |
| Legacy 2,000-character sensitivity input definition | **OPEN** |
| Freeze date and approving investigators | **OPEN** |

**No gold-test sampling/annotation/scoring until all OPEN fields are resolved and the guide, pipeline, and evaluator are frozen.** The pilot reserve is excluded from final gold and untouched replication. Any rule changed after inspecting test performance creates a new exploratory analysis, not a revised primary endpoint.

## Sampling and target population

Use a two-stage probability sample from the certified untouched raw-note frame: sample patients, then up to a prespecified cap of notes per patient (planning range 3--6, fixed after the pilot). Record both-stage inclusion probabilities in restricted storage. Specify the primary estimand and whether weights are used **before the final draw**. Include notes with zero medication predictions. Aim for ~75--100 patients and 300--500 notes, adjusting the note count from pilot event density and expected uncertainty rather than from gold-test results. Keep any action-enriched challenge set separate.

Primary input and annotation context: the **complete raw clinic note**, if authorized and technically accessible. Run all systems/comparators on the same note text. The legacy 2,000-character view is a paired sensitivity analysis on the same gold notes; gold mentions outside that view stay in the denominator and are potential false negatives. If complete text cannot be used, amend this contract before the final draw and explicitly relabel the target input.

## Annotation task boundary (pilot decisions required)

Required human labels for every in-scope medication mention: validity, contiguous mention offsets, and canonical identity at the selected primary level. Record an explicit uncertain/unresolvable identity code; never infer a specific ingredient from structured EHR. Optional or secondary labels: action and temporality; negation/certainty/rationale only as needed for validity or secondary analysis. Annotators see no pipeline candidates, LLM outputs, aliases, projected labels, or structured medication history.

The guide must give examples and a prespecified in-scope/excluded decision for each of: historical medications, negated mentions, planned/discussed medications, medication classes or vague terms (for example “steroids”), combinations, brand names, dosage-only references, and repeat mentions. Do not silently include a class-only mention in an ingredient-level denominator or silently exclude a difficult mention after scoring. Specify whether uncertain/unresolvable gold mentions count for detection only, are excluded from identity scoring, or form an explicit abstention category; report their count either way.

## One-to-one matching contract (choose exact rules in the pilot)

The evaluator must make a **one-to-one** assignment within each note: one gold mention matches at most one prediction, and vice versa. Duplicate predictions cannot all receive credit for the same gold. Unmatched in-scope gold mentions are false negatives; unmatched in-scope predictions are false positives. Cross-note or cross-patient matches are forbidden.

Freeze the following before scoring:

1. **Span detection:** exact boundaries versus a defined overlap criterion; whether action words, punctuation, and dosage tokens may be included in a predicted span; how a partial match is scored. Report a strict-boundary diagnostic if the primary rule is relaxed.
2. **Tie breaking:** deterministic maximum one-to-one assignment when several predictions overlap one gold or vice versa; define priority by span quality, then identity, then stable ID, with no dependence on hidden gold-test aggregate results.
3. **Primary canonical identity:** ingredient or ingredient-set representation, versioned RxNorm/terminology mapping, brand/generic equivalence, salts/forms, and whether a combination requires **all** ingredients to match. A broad therapeutic class must not count as a correct specific ingredient unless the gold target itself is class-level and that scoring rule is prespecified.
4. **Correct normalized prediction:** require both a valid mention match under rule 1 and identity equality under rule 3. Specify whether a correct identity with a nonmatching text span is a false positive plus false negative (default: yes).
5. **Ambiguous or uncertain gold/predictions:** explicit inclusion, exclusion, or abstention rule and denominator; no post hoc removal.
6. **Note-level medication set:** deduplicate identities within a note only for this secondary endpoint; specify how repeats with differing action/temporality are treated. Do not substitute note-level sets for mention-level primary P/R/F1.
7. **Candidate recall:** every in-scope gold mention is the denominator, including candidate-negative notes. Match against the *candidate stage before normalization*, using its own frozen span rule. Report candidate-negative and candidate-positive note misses separately.

Report mention extraction P/R/F1 and **end-to-end normalized mention P/R/F1** separately. Conditional identity accuracy is calculated only among valid matched mentions and must not be presented as end-to-end performance. Action metrics are secondary, including class prevalence, per-class P/R/F1, macro-F1, and majority-class baseline if counts permit. A comparator unable to emit a field is marked unsupported for that field, not scored using a different note set.

## Uncertainty, reporting, and audit

Resample patients (not rows) for 95% confidence intervals, respecting the two-stage sampling design and prespecified weights. Freeze bootstrap seed, replicate count, zero-denominator behavior, and interval method before test scoring. Report patient/note/mention counts, exclusions, annotation agreement, prevalence of uncertain/unresolvable labels, candidate-negative notes, and the paired full-note versus 2,000-character sensitivity. Save code/resource hashes and the restricted ID-level sample-manifest hash. Do not publish note text or identifiers.

## Predeclared interpretation after RQ1/RQ3 results

| Finding | Consequence for journal claims |
|---|---|
| Candidate recall and end-to-end identity sufficiently reliable for the intended descriptive use | Proceed to RQ3, with measured error/uncertainty stated; optional separate replication. No universal F1 threshold is asserted. |
| Candidate misses dominate | Do not enlarge the cohort as a substitute; narrow cross-source claims or redesign and independently retest candidate generation. |
| Extraction reasonable but canonical identity weak | Focus RQ2 on normalization limitations; do not treat large-cohort canonical labels as strongly validated. |
| Identity strong but action weak | Keep medication-identity/cross-source paper; action remains exploratory. |
| Full notes substantially outperform the 2,000-character view | Present full-note result as primary and truncation as a material legacy limitation. |
| Broad/late overlap weakly exceeds patient-shuffled null, or human review finds many unrelated matches | Soften or remove claims that relaxed overlap represents clinical correspondence. |
| Separate same-institution replication differs quantitatively | Report heterogeneity and possible cohort differences; do not force a pooled headline estimate. |
