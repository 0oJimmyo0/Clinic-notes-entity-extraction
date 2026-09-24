# Clinician Review Response — Fillable Template

Status: methods review; **no patient identifiers or note excerpts in this file**. Save a completed copy in the approved project location and return it to the study lead. Do not edit the source template in place if multiple reviewers will use it.

Reviewer name/role:  
Date:  
Documents/versions reviewed:  
Potential role after access approval (pilot annotator / adjudicator / clinical methods adviser / other):  
Any conflict with independent review (for example, previously saw study notes or tuned rules)? Describe without IDs:  

## 1. Study scope — `revision_plan.md`

- Are RQ1 (independent note-side validity), RQ2 (normalization mechanism), and RQ3 (cross-source characterization) clinically coherent?  
- Should medication identity remain primary and action secondary? Why?  
- Top three necessary scientific changes (or “none”):  
  1.  
  2.  
  3.  
- Should a category-only or nearby-date overlap be called “same medication event,” “clinically related but different event,” “unrelated,” or “uncertain”? What minimum evidence is needed?  

## 2. Primary annotation rules — `raw_note_annotation_guide_draft.md` and `gold_evaluation_contract.md`

For each row, write **Include / Exclude / Separate stratum / Uncertain**, a concise rule, and one *synthetic* example. The answer should be usable by two independent annotators.

| Mention type | Decision | Rule and synthetic example |
|---|---|---|
| Current, specific medication | | |
| Historical medication | | |
| Planned or considered medication | | |
| Negated medication | | |
| Discussion or comparison only | | |
| Class-only/vague mention (e.g., “steroids”) | | |
| Brand name versus generic ingredient | | |
| Combination medication | | |
| Dose-only or pronoun reference without drug name | | |
| Repeated same-drug mention in one note | | |
| Identity cannot be resolved confidently | | |

Primary canonical identity level (ingredient / ingredient set / other):  
When should an RxNorm link be required versus merely recorded if available?  
What counts as a valid medication span if nearby action/dose words are included?  
Does the proposed broad detection target (including historical/planned/negated mentions) match the intended system task? If not, what primary target and separately reported strata would you choose **before test scoring**?  
How should class-only and unresolvable mentions contribute to detection, identity denominators, and false-positive counts?  
Any rule that must remain open until the pilot, and why?  

## 3. Secondary labels and annotation workload — `gold_evaluation_contract.md` and `adjudication_schema.md`

- Which action categories can be judged reliably from one note: start / stop / hold / change / continue / discussed / unclear? Give problematic examples.  
- Is current/planned/historical temporality necessary for validity, or secondary only?  
- Are negation, certainty, and free-text rationale required for all mentions or only ambiguous cases?  
- Which legacy schema labels/rules should we **keep, change, or drop**?  
- For each section of the provisional raw-note guide: **Accept / Change / Keep open until pilot** and reason:  
- Expected minutes per typical clinic note (estimate now; measure in pilot):  
- Minimum training examples or guidance needed before independent annotation:  

## 4. Sampling and prior exposure — `validation_sampling_frame.md`

- Is the distinction between automated-only packet creation and human/development exposure clear?  
- Have you personally viewed any out-of-BIBM notes/outputs or used them in methods development? **Do not list IDs here**; tell the PI through the restricted channel if yes or unsure.  
- Which exposure questions should the PI/original investigators answer before the pilot?  

## 5. Overall recommendation

Choose one: **Ready for an authorized pilot after governance/exposure clearance / Revise the guide first / Major scope concern**.  
Reasons and unresolved questions:  
Suggested pilot training or adjudication meeting:  

## Completion checklist

- [ ] I reviewed the listed files and supplied explicit decisions or flagged what remains open.
- [ ] Examples in this response are synthetic; there are no patient IDs, note excerpts, or other clinical data.
- [ ] I have not started reviewing real notes under this methods-review request.
- [ ] I understand that IRB/data-use authority and prior-use attestation must come from the PI/data owner and original investigators.
