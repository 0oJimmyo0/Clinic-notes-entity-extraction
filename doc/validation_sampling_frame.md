# Independent Validation Sampling Frame: Preliminary ID-Only Audit

Status: **NOT ELIGIBILITY-CERTIFIED; no sample drawn**  
Audit date: 2026-09-24  
Reproduce aggregate counts: from `resources/`, run `python script/audit_validation_frame.py`.

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

The decisive finding is that **“outside the 761-patient BIBM cohort” is not equivalent to “never processed or inspected.”** If independence is defined as no presence in any prior selection/packet artifact, only 15 patients remain, below the planned 75–100. However, automatic packet generation alone may be compatible with a new frozen-test evaluation **if** those patients' outputs, notes, and errors were never manually inspected or used for alias, prompt, rule, threshold, or analysis development. This requires a provenance audit and PI/data-owner judgment, not an automatic exclusion of all 1,261.

## Required resolution before pilot or gold draw

1. Obtain the actual IRB/non-human-subjects and consent/waiver record or institutional determination, plus confirmation that raw full-note access and new annotation are within authorized scope. Do not invent statements.
2. For prior selection/packet artifacts, determine when and how they were generated, who saw them, and whether any of their patient-level outputs informed pipeline, alias, prompt, error review, or manuscript decisions. Inspect run logs/commits and ask the investigators who conducted manual review. Distinguish **automated processing only** from **human inspection/model development** at patient level.
3. Check other project artifacts and historical working copies not covered by the five exact-ID comparisons. Confirm whether IDs were transformed across files; the zero overlaps above are exact string joins only.
4. Verify note-level full-text **content completeness and permitted access** in a restricted environment after authorization. The parquet key match is not a content check.
5. Produce a restricted patient-level eligibility register with reason codes and an aggregate exclusion flow. The ID-level register and any note text must remain outside public/manuscript/Overleaf repositories.
6. If at least 75–100 genuinely uninspected, eligible patients remain, reserve separate pilot patients and freeze the two-stage sample protocol. Otherwise obtain a new approved temporal/patient slice or revise the independent-validation claim. **Do not draw a sample from the current 1,276-patient list yet.**

## Current gate decision

**Not passed.** The repository supports frame *feasibility* and reveals extensive prior automated packet exposure, but cannot certify an untouched patient pool or study authorization. Next human decision: PI/data owner confirms governance and investigators classify packet/selection exposure versus actual manual review or model-development use. Until then, work may proceed on read-only metric/provenance documentation, not clinical-note pilot annotation or a gold draw.
