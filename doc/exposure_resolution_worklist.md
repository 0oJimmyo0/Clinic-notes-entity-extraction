# Validation-Pool Exposure Resolution Worklist

Status: **aggregate-only audit reproduced; human/development exposure not certified** (2026-09-24). This is the next step after the investigator's confirmation that local private-domain processing is permitted. It does not record an IRB determination, certify eligible patients, or authorize public data release. Keep all patient IDs and note text in the restricted project area.

## Read-only audit reproduced

From `resources/`, `python script/audit_validation_frame.py` and `python script/audit_validation_exposure.py` ran without writing patient-level output. The existing restricted `validation_exposure_matrix.csv` has one header and 1,276 patient rows; it was not overwritten. Key aggregate results:

| Check | Patients / notes | Interpretation |
|---|---:|---|
| Outside the 761-patient BIBM cohort | 1,276 patients; 23,211 manifest note rows | Candidate frame only, not an independent sample |
| In at least one checked automated selection/packet source | 1,275 patients | Automated processing does not prove human inspection |
| Exact-ID hit in checked completed-review sources | 0 patients | Does not exclude off-file, differently keyed, or other review |
| Exact-ID hit in checked patient-linked development sources | 0 patients | Does not resolve manual alias provenance |
| Certified eligible | 0 patients | Attestations and restricted reconciliation remain necessary |
| Candidate note keys present in full-text parquet index | 23,211 / 23,211 | Key presence does not establish text completeness |

## Batch questions to send to original investigators

Use the [PHI-free attestation template](investigator_exposure_attestation_template.md) once per investigator and repeat its artifact block for each coherent run/batch. Ask each person to identify their own scope, dates, and any other person who viewed material. Patient-level exception lists must be passed only through the approved restricted channel.

| Batch / source type | Checked artifact or source | Exact-ID overlap in candidate pool | Required attestation |
|---|---|---:|---|
| Automated note selection | `episode_notes/manifests_clinic_only/adjudication_note_manifest.csv` | 1,261 | Who opened selected notes, candidate outputs, or patient-level reports? Did any influence rules, prompts, aliases, or paper interpretation? |
| Automated global packet | `episode_extraction_results/rq1/adjudication_packets/adjudication_packets_notes.csv` | 1,251 | Was the packet directory merely generated, or viewed by anyone? Identify recoverable reviewed IDs and exceptions. |
| Automated clinic-only packet | `episode_extraction_results/clinic_only/rq1/adjudication_packets/adjudication_packets_notes.csv` | 627 | Same distinction; state run/date and whether anyone inspected content. |
| Completed/review-linked files | Clinic-only reviewed bridge; BIBM 300- and 1,000-row audits; no-overlap review; Path B and leftover completed reviews | 0 in the checked exact-ID joins | Confirm who reviewed these files and whether other copies, keys, or review logs include out-of-BIBM patients. A zero join alone is not an attestation. |
| Patient-linked development files | Development-alias source rows; Path A v2 reviewed failure rows | 0 in the checked exact-ID joins | Confirm whether any other patient-linked development work used out-of-BIBM notes or outputs. |
| Manual alias resources lacking patient IDs | `resources/manual/pathA_alias_review.csv`, `pathA_alias_exclusions.csv`, `pathA_v2_alias_supplement.csv`, `pathA_alias_map.json` | Not joinable by patient ID from these files | Identify source reviews/notes and any restricted crosswalk; otherwise mark affected provenance unresolved. Do not infer that absence of IDs means independence. |
| Off-file exposure | Presentations, screenshots, notebooks, meetings, email, local copies, verbal examples | Not captured by scripts | List types and approximate dates, whether content influenced development, and where exact restricted IDs can be recovered; otherwise mark unresolved. |

## Reconciliation rule in the restricted workspace

1. Collect an attestation from each original investigator/developer who could have inspected candidate-pool material. A batch assertion must cover the relevant artifact, dates, viewers, and any exceptions; no person can attest for another's private work without evidence.
2. Join any recoverable reviewed/development IDs or crosswalks to the existing restricted patient matrix. Record source, attestor, date, and reason alongside the confirmed class; preserve the provisional automated flags.
3. Confirm **B (automated-only)** only where every applicable batch is covered and no human inspection or development use is found. Mark human-inspected **C**, development-informing **D**, and unresolved **Unknown**. If an individual patient cannot be safely classified, do not silently include them in the validation pool.
4. Publish only aggregate A/B/C/D/Unknown counts and an exclusion flow. Do not upload the restricted matrix, attestation exceptions, patient IDs, note text, or alias-source rows to this repository.
5. After classification, check that at least 75–100 eligible patients remain; otherwise seek an approved new temporal/patient slice. Only then select a separate pilot reserve and later freeze/draw the final gold sample.

**Next human answer needed:** for each automatically generated batch above, did any original investigator actually view its out-of-BIBM notes, excerpts, predictions, or error reports—or use them to change the pipeline or interpret results? Start with the person who generated/reviewed the global and clinic-only packets. Record their answer in the PHI-free attestation and exceptions in restricted storage, not as a blanket assumption about all 1,275 patients.
