# JAMIA Open Submission Checklist — Medication-Evidence Study

Status: working checklist, not a claim of approval or completed validation. Last checked: 2026-09-24. Target article type: **Research and Applications**. Use the [revision plan](revision_plan.md) for full methods and the [gold evaluation contract](gold_evaluation_contract.md) for the pre-test freeze. Recheck the [JAMIA Open instructions](https://academic.oup.com/jamiaopen/pages/general_instructions) immediately before submission.

Mark a box complete only when its **evidence** exists and the named owner has verified it. A written plan, provisional methods review, file name, or automated artifact is not proof that the study gate passed. Do not put patient IDs, note text, restricted review packets, or the exposure matrix in this public repository.

## 1. Governance and independent-patient eligibility — blocking gate

- [ ] **PI/data owner:** produce the actual institutional ethics determination (IRB approval and number, exemption/non-human-subjects determination, or other applicable record), consent/waiver facts, and data-use authorization for complete clinic notes and new human annotation. Evidence stays in approved restricted records; the public checklist records only date/decision, not sensitive documentation.
- [ ] **Original investigators:** complete the [PHI-free artifact/batch exposure attestations](investigator_exposure_attestation_template.md), including off-file viewing and whether notes/predictions informed aliases, rules, prompts, thresholds, or interpretation. Keep patient-level exceptions and source IDs in restricted storage.
- [ ] **Analysis team + PI:** reconcile attestations, alias provenance, and restricted ID joins into confirmed A/B/C/D/Unknown classifications. Produce an aggregate exclusion flow; certify at least 75–100 uninspected, development-independent eligible patients with usable authorized notes, or obtain an approved new slice. The [preliminary audit](validation_sampling_frame.md) currently certifies **zero** patients.

**Pass condition:** documented authority and a verified eligible frame. No new-patient pilot or final gold draw before this gate passes.

## 2. Clinical target and annotation team — blocking gate

- [ ] **Qualified clinician:** review the [handoff packet](clinician_review_handoff.md) and return the [decision form](clinician_review_response_template.md). Give a clinical rationale—not just a code-based rationale—for which named patient medications are primary treatment-relevant, broader-sensitivity-only, or excluded. Set explicit rules for copied/current lists, remote history, firm versus tentative plans, not-taking statements, allergy-only, class-only, combinations, and unresolved identity.
- [ ] **Study lead:** designate a second authorized, independent annotator and an adjudication process; document any prior exposure or conflict.
- [ ] **Clinician + analysis team:** confirm training examples, annotation fields, matching/false-positive rules, and which decisions may be refined in a separate pilot. Keep action attribution secondary unless reliability supports it.

**Pass condition:** clinician-approved guide and independent coding team; the current guide is provisional, not signed off.

## 3. Baseline provenance and pre-test freeze — blocking gate

- [ ] **Analysis team:** create `doc/metric_ledger.md` with each retained BIBM manuscript number, source artifact, population, denominator, formula, split, and reproducibility check. Reconcile 27,552 versus 27,752 reference rows and held-out 0.8429 versus full-resource 0.845777; do not treat conditional accuracy as raw-note end-to-end performance.
- [ ] **Analysis team + original developers:** create `doc/resource_chronology.md` for candidate lexicons, normalizers, base/development-only aliases, Qwen-assisted reference construction, terminology, structured data, note-text view, code version, and which patients could have informed each resource. Resolve the provenance of manual alias files or label it unresolved.
- [ ] **Analysis team:** reconcile the [legacy task-scope audit](legacy_task_scope_audit.md) with the exact historical run configuration. Select full-note primary input if authorized; define the paired 2,000-character sensitivity and count truncation.
- [ ] **Clinical and analysis leads:** after the separate feasibility pilot, lock the [gold evaluation contract](gold_evaluation_contract.md): primary/broader denominators, handling of predictions on non-primary spans, one-to-one mention and ingredient-set matching, patient-first sample with note cap and inclusion probabilities, estimand/weights, bootstrap and comparator rules, code/resources/hashes, and freeze date. Do not use final gold outcomes to tune these choices.

**Pass condition:** traceable old numbers and a signed/versioned freeze record before final gold draw or scoring. Metric ledger and resource chronology do not yet exist.

## 4. Independent note-side validation (RQ1) — submission-critical result

- [ ] **Authorized annotators:** run a separate 20–30-note feasibility pilot from excluded patients or approved development notes; report time/note, mention density, ambiguities, and guide changes. Do not report pilot performance as the independent test result.
- [ ] **Analysis team:** draw the frozen two-stage probability sample of approximately 75–100 eligible patients and 300–500 raw notes, including candidate-negative notes; secure and hash the restricted sample manifest before annotation.
- [ ] **Annotators:** annotate raw notes blinded to pipeline/Qwen/structured EHR output; independently double-code 20–30%, adjudicate, and report agreement and unresolved prevalence.
- [ ] **Analysis team:** evaluate frozen candidate recall, span detection, normalized ingredient identity, primary end-to-end P/R/F1, broader-mention sensitivity, and patient-clustered/design-aware intervals. Compare systems on the same notes, gold target, and supported fields; report the 2,000-character sensitivity separately.

**Pass condition:** reproducible independent results with denominators, agreement, uncertainty, and important candidate misses visible. Strong BIBM conditional normalization alone does not pass.

## 5. Cross-source interpretation (RQ2/RQ3) — submission-critical calibration

- [ ] **Analysis team:** retain the BIBM normalization ablation as **conditional mechanism evidence**, with correct population and denominators; state that Qwen helped construct the historical large reference but did not create the independent RQ1 gold truth.
- [ ] **Analysis team:** calculate the predeclared semantic-by-temporal matrix (exact/ingredient/category by same visit and 7/14/30/90-day windows), with structured-record timing, missingness, and patient-clustered uncertainty documented.
- [ ] **Authorized reviewers:** evaluate a prespecified sample of relaxed semantic/temporal matches, distinguishing same clinical event from related-but-different, unrelated, and uncertain; report sampling and reviewer agreement.
- [ ] **Analysis team:** run a patient-shuffled structured-history negative control with frozen matching rules. If broad or late overlaps are near the null or clinically weak, reduce the reconciliation claim.

**Pass condition:** clinical and statistical support for each cross-source claim, or an explicitly narrower claim. More patients, extra LLMs, and same-institution replication are conditional, not substitutes for these checks.

## 6. Manuscript and submission integrity — final gate

- [ ] **Authors:** rewrite the paper around independent raw-note validity, then normalization mechanisms and calibrated cross-source characterization. Separate new gold results from constructed-reference and held-out BIBM results; treat action as secondary unless independently reliable. Match title, abstract, tables, and conclusion to the actual results.
- [ ] **Authors + PI:** include factual ethics/consent, data availability and restricted-access conditions, funding, conflicts, author contributions, and AI-use statements. Perform a PHI/security review of manuscript, figures, code, and supplement; release only approved non-sensitive material.
- [ ] **Corresponding author:** prepare title page, accurate author/affiliation/contact information, up to five keywords, word count, structured abstract, and figure alt text. For Research and Applications, current limits are 4,000 main-text words, 250 abstract words, four tables, and six figures; prepare a lay summary of no more than 200 words for revision and check the live portal requirements. A compiled PDF is acceptable when the source is LaTeX; verify submission files in the portal.
- [ ] **Corresponding author:** disclose the arXiv preprint and related work in the cover letter; supply any related **published** material, including conference proceedings if any, for overlap assessment. Do not describe a rejected BIBM submission as published proceedings. Explain exactly what independent validation and calibration are new.
- [ ] **All authors:** inspect the final rendered manuscript and supplement, approve claims and disclosures, verify references/figures/tables and upload files, then recheck the live journal instructions before submission.

**Pass condition:** all required statements are factual, every result is traceable, and the journal-format package is complete. A finished checklist does not guarantee acceptance.

## Work one item at a time

**Start with item 1.1:** ask the PI/data owner for the existing institutional determination and a yes/no decision on whether complete clinic-note access and new manual annotation are authorized for this study. Do not upload the determination or patient data here. Record only the verified decision, date, responsible person, and restricted evidence location. In parallel, the analysis team may work on the non-PHI metric ledger and resource chronology, but no pilot or final gold draw begins until sections 1–3 pass.
