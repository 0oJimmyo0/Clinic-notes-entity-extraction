# Independent Raw-Note Gold Evaluation Contract

Status: **draft for pilot; NOT CLINICIAN-APPROVED, FROZEN, OR SCORED** (2026-09-24)

Purpose: define RQ1's human-gold target and evaluator before the independent sample is drawn or any gold-test predictions/metrics are inspected. The [provisional raw-note annotation guide](raw_note_annotation_guide_draft.md) contains proposed clinical rules from a **methods draft that examined no patient notes**; it is not a completed human/clinician review. This contract is distinct from `adjudication_schema.md`, which starts from candidate packets and cannot measure medications missed by candidate generation. Keep patient IDs, note text, and ID-level manifests in restricted storage, not this document or the manuscript repository.

## Freeze record (complete after the excluded-patient/development pilot)

| Item | Locked value / artifact / hash |
|---|---|
| Governance and full-text authorization | **OPEN** |
| Eligible patient/note frame and exclusion rules | **OPEN** |
| Primary task boundary and annotation-guide version | **OPEN** |
| Clinician sign-off and second independent annotator | **OPEN** |
| Broad mention target versus affirmative/current subset; class-only/allergy/uncertain rules | **OPEN** |
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

Proposed required labels for every in-scope medication mention: validity, minimal contiguous medication-identity offsets, identity resolvability, and canonical ingredient/complete ingredient set when resolvable. Record explicit `UNRESOLVABLE` or `UNCERTAIN_IDENTITY` codes; never infer a specific ingredient from structured EHR. Record temporality and simple assertion (affirmed/negated/uncertain) separately for each mention **if the clinician/pilot confirms their feasibility**. Treatment action remains secondary; certainty/rationale may be limited to ambiguous cases. Do not overload the legacy `mention_status` field. Annotators see no pipeline candidates, LLM outputs, project aliases, projected labels, or structured medication history.

The guide must give examples and a prespecified in-scope/excluded decision for each of: current, historical, planned/discussed, negated, allergy-only, family-member/general-discussion, class-only/vague, combination, brand, dose-only/anaphoric, and repeated mentions. Provisional recommendation: retain patient-linked historical/planned/negated mentions in detection with separate strata, exclude general discussion and allergy-only references from the treatment-medication target, and report an affirmative/current subset. **Before freeze, check whether this broader detection target matches the pre-existing pipeline's intended scope**; prespecify any narrower primary target on development data, not after gold scores. Do not silently include class-only or uncertain identity mentions in ingredient-level scoring or remove difficult mentions after scoring.

## One-to-one matching contract (choose exact rules in the pilot)

The evaluator must make a **one-to-one** assignment within each note: one gold mention matches at most one prediction, and vice versa. Duplicate predictions cannot all receive credit for the same gold. Unmatched in-scope gold mentions are false negatives; unmatched in-scope predictions are false positives. Cross-note or cross-patient matches are forbidden.

Freeze the following before scoring:

1. **Span detection:** gold uses minimal identity-bearing offsets. Decide exact boundaries versus a defined relaxed criterion; a proposed relaxed rule requires coverage of **all** drug-identity-bearing gold tokens, not merely one overlapping token. Specify limits on extra action/dose/context text so an excessively broad prediction cannot get unearned credit. Report strict-boundary matching separately if the primary rule is relaxed.
2. **Tie breaking:** deterministic maximum one-to-one assignment when several predictions overlap one gold or vice versa; define priority by span quality, then identity, then stable ID, with no dependence on hidden gold-test aggregate results.
3. **Primary canonical identity:** one ingredient or a complete unordered active-ingredient set, versioned RxNorm/terminology mapping where unambiguous, brand/generic equivalence, and frozen salt/form rules. Dose, strength, route, frequency, and formulation are not identity criteria. A broad therapeutic class must not count as a correct specific ingredient; if class-level scoring is wanted, report a separate endpoint. A combination missing any active ingredient is not an exact ingredient-set match.
4. **Correct normalized prediction:** require both a valid mention match under rule 1 and identity equality under rule 3. Specify whether a correct identity with a nonmatching text span is a false positive plus false negative (default: yes).
5. **Ambiguous, class-only, or uncertain gold/predictions:** freeze separate detection and normalized-identity scoring masks and the treatment of system predictions overlapping excluded/unresolvable mentions. Report the count and proportion of gold mentions excluded from ingredient scoring and, where useful, both full-sample joint yield and conditional performance. No post hoc removal or unexplained denominator change.
6. **Note-level medication set:** deduplicate identities within a note only for this secondary endpoint; specify how repeats with differing action/temporality are treated. Do not substitute note-level sets for mention-level primary P/R/F1.
7. **Candidate recall:** every in-scope gold mention in the prespecified detection target is the denominator, including candidate-negative notes. Match against the *candidate stage before normalization*, using its own frozen span rule. Report candidate-negative and candidate-positive note misses separately and provide important clinical strata (current, historical, planned, negated) when adequately powered.

Report mention extraction P/R/F1, evaluable ingredient-identity coverage, and **end-to-end normalized mention P/R/F1** separately, with explicit gold/prediction denominators for each. Conditional identity accuracy is calculated only among valid matched mentions and must not be presented as end-to-end performance. Action metrics are secondary, including class prevalence, per-class P/R/F1, macro-F1, and majority-class baseline if counts permit; a drug switch is provisionally a stop plus start, while `change` is an explicit same-drug regimen modification. A comparator unable to emit a field is marked unsupported for that field, not scored using a different note set.

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
