# Raw-Note Medication Annotation Guide — Draft for Clinician and Pilot Review

Status: **PROVISIONAL METHODS DRAFT; not clinician-approved, not pilot-tested, not frozen** (2026-09-24)

This guide translates a biomedical-informatics/clinical-methods review into proposed annotation rules. The review examined **no real patient notes** and is **not** human gold annotation, inter-rater agreement, IRB approval, or clinician sign-off. A qualified, authorized clinician and a second independent annotator must review the task before any final gold annotation. The actual study authorization and patient-exposure classification remain separate gates.

## 1. Task and information barrier

Read the **entire raw clinic note** and identify explicit medication evidence about the **patient**. Do not use pipeline candidates, LLM outputs, project aliases, structured EHR medication history, or inferred medication identities from other records. Review every assigned note, including notes with no in-scope mention; record that the note was completed with zero mentions rather than silently omitting it.

The proposed primary annotation is at the **textual mention** level. Record every distinct explicit medication occurrence and its minimal medication-identity-bearing span. The same drug appearing twice in one note yields two mention records; a separate note-level medication set may deduplicate later. This default must be checked during the pilot for copy-forward lists and excessive repeat weighting.

## 2. Proposed in-scope boundary — clinician must confirm before freeze

| Text type | Proposed handling | Reason / example (synthetic) |
|---|---|---|
| Current named medication | Include in mention and resolvable-identity strata | “Continue metformin 500 mg” -> span `metformin`. |
| Historical patient medication | Include in a **historical** stratum, not automatically current-use correspondence | “Previously received tamoxifen.” |
| Firm future plan or tentative patient-specific consideration | Include with planned/considered status, kept separate from current use | “Will start pembrolizumab” versus “May consider pembrolizumab.” |
| Explicitly negated patient medication use | Include the named medication with **negated** assertion; do not infer current exposure | “Not taking warfarin.” |
| General pharmacology, comparison, or family-member medication | Exclude unless the text explicitly concerns this patient's treatment | General teaching or “Her father takes X” is not patient medication evidence. |
| Allergy/intolerance-only drug name | Provisionally exclude from the **treatment-medication target** unless actual patient treatment exposure is also documented | “Allergic to penicillin” alone is not treatment exposure. Pilot must test boundary reliability. |
| Class-only/vague patient medication | Retain as a **class/vague** stratum for detection/descriptive counts, not ingredient-level identity credit | “Continue steroids”; do not invent prednisone. |
| Dose-only/pronoun reference without a local explicit name | Do not create a standalone ingredient mention | “Increase it to 20 mg”; do not use structured EHR to guess the drug. |

**Open scope decision:** The historical/planned/negated strata could be broader than the legacy treatment-context pipeline was built to detect. Before freezing, compare these proposed labels with the *pre-existing* pipeline specification on development data. Prespecify either a broad mention-detection headline plus an affirmative/current subset, or a narrower primary target with separate strata; do not change the target after seeing gold-test results.

## 3. Orthogonal fields for each mention

| Field | Proposed values / rule | Requiredness |
|---|---|---|
| `mention_start`, `mention_end` | Character offsets for minimal medication-identity-bearing text; record the exact observed span | Required |
| `mention_validity` | In-scope / excluded / uncertain boundary | Required |
| `identity_resolvability` | Resolvable ingredient / resolvable fixed ingredient set / class-only / unresolvable / uncertain | Required |
| `canonical_ingredient_set` | One ingredient or unordered complete active-ingredient set when resolvable; **no guess** for vague or ambiguous text | Required when resolvable |
| `rxnorm_id`, `mapping_reason` | Prespecified terminology release and unambiguous mapping if available; otherwise record why not mapped | Record when possible; code not required for a valid mention |
| `temporality` | Current / planned-future / historical / unclear | Proposed required; clinician/pilot confirmation pending |
| `assertion` | Affirmed / negated / uncertain | Proposed required; clinician/pilot confirmation pending |
| `action` | Start / stop / hold / continue / change / discussed / unclear | Secondary; use `unclear` rather than inferring action from list presence |
| `certainty`, `rationale` | Conditional/uncertain plan and brief explanation of difficult decisions | Only when needed |

The legacy `mention_status` field mixes these dimensions and must **not** be reused as the sole new-gold label. `compare_to_structured_ehr` is a later analysis/review judgment, not part of blinded note annotation.

## 4. Identity and span rules proposed for the pilot

- Canonical identity is **ingredient** for one active ingredient and the **complete unordered ingredient set** for a fixed combination. One component of a combination is not an exact combination match. Dose, strength, route, frequency, and formulation are outside the primary identity target. Salt/form equivalence requires a frozen terminology rule; do not decide case by case on gold notes.
- An unambiguous brand maps to its ingredient or complete ingredient set while the observed brand remains the text span. RxNorm is recorded when a frozen terminology release supports an unambiguous mapping. No forced RxNorm code for class-only or unresolved mentions.
- Mark the **minimal lexical expression** carrying the medication identity. Normally exclude action words, dose, route, frequency, and external punctuation. Combination components and separators belong in the span.
- Do not create a new ingredient mention from a pronoun or dose alone. If an explicit medication name occurs nearby, annotate that explicit occurrence.
- For action, a drug switch is provisionally two labels (stop old drug, start new drug), not one generic change. `Change` is for an explicit modification of the same regimen. Current use in a copied list does not by itself establish a new `continue` action.
- “Has been off X” supports non-current status; it may not establish who stopped it or when. Record unclear action if the language does not support one.

## 5. Synthetic training examples (not patient data)

| Text | Proposed annotation |
|---|---|
| “Lisinopril was stopped last month. We may restart lisinopril.” | Two `lisinopril` spans; same identity; first historical/stopped, second considered/future. |
| “She is no longer taking warfarin.” | `warfarin`; identity resolvable; current-use assertion negative; stopping action only if the guide's evidence rule is satisfied. |
| “Continue steroids for five more days.” | `steroids` in class/vague stratum; no invented ingredient. |
| “Her dose was increased to 20 mg daily.” | No standalone named medication mention. |
| “Allergic to penicillin. Previously treated with azithromycin.” | `penicillin` allergy-only/out of primary treatment target; `azithromycin` historical patient treatment. |
| “Tylenol was stopped; lisinopril/hydrochlorothiazide continues.” | `Tylenol` -> acetaminophen; combination -> complete {hydrochlorothiazide, lisinopril} set; actions separate. |

Before the authorized pilot, build ~15–20 adjudicated **synthetic or development-only** training examples covering copied lists, brand/generic, combinations, negation, allergy, class-only, uncertainty, action versus status, and offsets. The clinician and second annotator review them independently, then resolve ambiguities in a documented guide revision. Do not use final gold notes for training.

## 6. Scoring decisions that this guide does not settle

The [gold evaluation contract](gold_evaluation_contract.md) must freeze these **before final gold scoring**: primary broad versus current-use target; handling of excluded, class-only, and unresolvable mentions in each denominator; whether a system's specific-drug prediction on an unresolvable/class-only gold span is ignored or penalized; one-to-one span matching and limits on extra span text; exact-boundary diagnostic; duplicate handling; ingredient-set equality; patient/note weights; and bootstrap rules. Report detection, resolvable-identity coverage, normalized extraction, and note-level set metrics as distinct quantities. Do not make normalized F1 look better by silently removing difficult mentions.

## 7. Approval and pilot record (leave OPEN until factual review)

| Gate | Status / name / date / evidence |
|---|---|
| PI/data owner confirms full-note annotation authority | **OPEN** |
| Candidate patient exposure classification complete | **OPEN** |
| Qualified clinician approves/revises the clinical boundary and examples | **OPEN** |
| Second independent annotator designated | **OPEN** |
| Secure pilot training/adjudication meeting completed | **OPEN** |
| Pilot mention density, time/note, and disagreement summary recorded | **OPEN** |
| Final guide, evaluator, sampling design, and pipeline frozen before gold draw | **OPEN** |
