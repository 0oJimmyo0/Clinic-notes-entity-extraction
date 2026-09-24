# Independent Validation Sampling Frame: Preliminary ID-Only Audit

Status: **NOT ELIGIBILITY-CERTIFIED; no sample drawn**  
Audit date: 2026-09-24  
Reproduce Stage 1 aggregate counts: from `resources/`, run `python script/audit_validation_frame.py`. Reproduce Stage 2 aggregate exposure counts: run `python script/audit_validation_exposure.py`.

This audit compares patient and note **identifiers only** across existing manifests and selected prior artifacts. Some source artifacts contain clinical-text columns, but the audit does not analyze or output those columns or inspect clinical content. It cannot determine human-review provenance, verify legal/IRB authorization, or select pilot/gold patients. No IDs belong in this document.

## Aggregate findings

| Check | Count | Interpretation |
|---|---:|---|
| Patients in broader clinic-only evaluation note manifest | 2,029 | Source manifest population, not an independent test set |
| Patients in BIBM cohort manifest | 761 | Existing characterization cohort |
| Broader-manifest patients outside BIBM cohort | 1,276 | Preliminary candidate pool; 753 BIBM patients also occur in broader manifest |
| Note rows for those 1,276 patients | 23,211 | 23,211 unique note IDs and no duplicate patient–note rows |
| Candidate patients with at least 3 / at least 6 manifest notes | 1,124 / 962 | Two-stage capped-note sampling could be feasible **if** independence and access pass |
| Median manifest notes per candidate patient | 12 | Descriptive frame check only |
| Candidate note keys present in full-text parquet index | 23,211 / 23,211 | Key presence only; **does not prove nonempty, complete, usable, or authorized full text** |

Prior-artifact patient overlaps (not additive):

| Artifact | Out-of-BIBM patients with exact ID overlap | What it does *not* establish |
|---|---:|---|
| Clinic-only adjudication **selection** manifest | 1,261 | Selection does not prove a human read notes or tuned a model |
| Global adjudication packet-note artifact | 1,251 | Packet creation does not prove manual inspection |
| Clinic-only packet-note artifact | 627 | Same caveat |
| Clinic-only reviewed-adjudication bridge | 0 | Exact-ID nonoverlap is not proof of no prior review elsewhere |
| `medications.jsonl` note IDs | 0 | Exact-ID nonoverlap is not proof of no prior LLM/manual work under other identifiers |
| Any packet or reviewed-bridge artifact above | 1,261 | Only **15** patients are absent from these selected artifacts |

The Stage 1 packet/review union leaves 15 patients absent from **those** artifacts. That is not the same as absence from every automated source: when the selection manifest is also included, Stage 2 finds only **one** patient absent from all three checked automated sources. The decisive point is that **“outside the 761-patient BIBM cohort” is not equivalent to “never processed or inspected.”** Automatic packet generation is nevertheless compatible with a frozen-test evaluation **if** the patients' outputs, notes, and errors were never manually inspected or used for alias, prompt, rule, threshold, or analysis development. The [packet builder](../script/run_build_adjudication_packets.py) initializes human-review fields blank; [the review join](../script/run_join_adjudication_labels.py) is a later step. Therefore, automated exposure alone is not grounds to discard 1,275 patients.

## Stage 2: exact-ID review and development artifact audit

`script/audit_validation_exposure.py` checks completed/review-linked files (including the 300-row and 1,000-row random audits, no-overlap review, completed Path B/leftover reviews, and clinic-only reviewed bridge) separately from automated manifests/packets. It also checks patient-linked development-source rows for development-only aliases and Path A v2 refinement. It does **not** assume that a file named “reviewed” proves independent manual annotation of every row.

| Provisional class from checked files | Patients | Meaning |
|---|---:|---|
| A candidate: no exact-ID trace in the checked automated/review/development artifacts | 1 | Still not certified; other sources and human recollection remain untested |
| B candidate: automated source only, no exact-ID trace in checked completed review/development files | 1,275 | Potentially eligible **only after** human/development exposure is verified |
| C linked: checked completed review artifact | 0 | Zero exact-ID matches in this source list; does **not** prove no human exposure elsewhere |
| D linked: checked patient-linked alias-source artifact | 0 | Manual alias CSV/JSON resources lack patient IDs and remain untraceable by this join |
| **Certified eligible** | **0** | Governance and investigator attestation still pending |

The script has created a patient-level *provisional* exposure matrix in the existing restricted results area at `episode_extraction_results/clinic_like_20k_30k/rq1/validation_exposure_matrix.csv`. That CSV contains patient identifiers; **do not commit, attach, publish, or upload it to GitHub/Overleaf/arXiv**. The script prints aggregate counts by default and only writes an ID-level CSV when given an explicit absolute `--restricted-output` path under the existing `episode_extraction_results/` tree. It uses exclusive creation so a reviewed register is not silently overwritten.

Four investigator decisions are needed in parallel with the governance check:

1. Does the actual IRB/data-use determination permit complete clinic-note access and new manual annotation for this medication-evidence study?
2. Did anyone open/read notes or packet outputs from out-of-BIBM patients for adjudication, debugging, presentations, or manuscript work? If yes, which patients/notes?
3. Did out-of-BIBM patient information influence aliases, prompts, candidate rules, thresholds, categories, model choice, or error-analysis decisions? Trace manual `pathA` alias resources to source patients where possible.
4. If fewer than 75–100 patients can be documented as uninspected and development-independent, is an approved later-time or otherwise separate slice available?

Classify each restricted matrix row as **A (no known exposure), B (automated-only), C (human inspected), D (development-informing), or Unknown**, with a source/attestation and decision date. B is eligible in principle; C/D should be excluded from pristine gold; Unknown remains ineligible until resolved. Preserve the provisional class separately from the confirmed class. Do not use the pilot reserve until governance and classification pass.

## Required resolution before pilot or gold draw

1. Obtain the actual IRB/non-human-subjects and consent/waiver record or institutional determination, plus confirmation that raw full-note access and new annotation are within authorized scope. Do not invent statements.
2. For prior selection/packet artifacts, determine when and how they were generated, who saw them, and whether any of their patient-level outputs informed pipeline, alias, prompt, error review, or manuscript decisions. Inspect run logs/commits and ask the investigators who conducted manual review. Distinguish **automated processing only** from **human inspection/model development** at patient level; the Stage 2 exact-ID zeros do not settle this.
3. Check other project artifacts and historical working copies not covered by the five exact-ID comparisons. Confirm whether IDs were transformed across files; the zero overlaps above are exact string joins only.
4. Verify note-level full-text **content completeness and permitted access** in a restricted environment after authorization. The parquet key match is not a content check.
5. Complete the existing restricted **provisional** patient-level matrix with confirmed A/B/C/D/Unknown classifications, source attestations, reason codes, and an aggregate exclusion flow. The ID-level register and any note text must remain outside public/manuscript/Overleaf repositories.
6. If at least 75–100 genuinely uninspected, eligible patients remain, reserve separate pilot patients and freeze the two-stage sample protocol. Otherwise obtain a new approved temporal/patient slice or revise the independent-validation claim. **Do not draw a sample from the current 1,276-patient list yet.**

## Current gate decision

**Not passed.** The repository supports frame *feasibility* and reveals extensive prior automated packet exposure, but cannot certify an untouched patient pool or study authorization. Next human decision: PI/data owner confirms governance and investigators classify packet/selection exposure versus actual manual review or model-development use. Until then, work may proceed on read-only metric/provenance documentation, not clinical-note pilot annotation or a gold draw.
