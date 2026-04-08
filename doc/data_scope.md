DATA CORPUS SCOPE AND REASONING

UNIT OF ANALYSIS
Use the visit as the main unit of analysis.
Reason:
- the downstream concordance question is visit-level
- structured EHR medications are naturally aligned to visits/encounters
- visit-level analysis is less distorted by patient history than patient-level pooling


NOTE SCOPE
Use treatment-context clinic note spans, not full notes.
Reason:
- this matches the current pipeline
- it keeps the problem clinically focused
- it is more feasible for adjudication
- it avoids overclaiming full-note medication extraction
- it is consistent with the paper’s narrow scope


PRIMARY DOMAIN
Drug domain only for primary analyses.
Reason:
- this is where the current pipeline already has meaningful signal
- adding conditions/labs/procedures now would dilute the paper
- similar prior work already measures broad concept overlap, so our differentiation should come from better note-grounded evaluation, not breadth


VISIT INCLUSION CRITERIA
Include a visit if:
- it has an eligible treatment-context clinic note
- the note contains at least one treatment-context span
- the visit has valid patient and encounter identifiers
- structured medication data exists if the visit is used for downstream concordance analysis

Optional quality filters:
- minimum note text length
- exclude obviously empty/template-only notes
- exclude duplicate note versions if needed


PATIENT INCLUSION CRITERIA
Include all patients with at least one eligible visit.
Do NOT exclude patients solely for having too many or too few visits.

Reason:
- high visit counts are clinically real, especially in longitudinal specialty care
- low visit counts are also clinically real
- arbitrary trimming can bias the sample and hurt comparability


HOW TO HANDLE EXTREME VISIT COUNTS
Do not remove first.
First describe the distribution.
Then use sensitivity analyses:
- all eligible visits
- one random visit per patient
- capped visits per patient
- optional robustness check excluding top 1% high-visit patients

Reason:
- the problem is not that frequent-visit patients are “wrong”
- the problem is that they can dominate the statistics
- this should be handled analytically, not by automatic exclusion


ADJUDICATION SUBSET SCOPE
Do not adjudicate the full corpus unless it is easy.
Instead, build a designed adjudication subset.

The adjudication subset should be:
- large enough to evaluate extraction and normalization reliably
- diverse in note type, service, note length, and medication density
- stratified to include easy and hard cases
- enriched for unresolved mentions after Path A so Path B can be tested

Reason:
- this is much more feasible
- it keeps the human review burden manageable
- it gives enough difficult cases to evaluate linking and calibration


STRUCTURED EHR CONCORDANCE SUBSET
For downstream concordance, only include adjudicated note medications marked as comparable to structured EHR.
This usually means current or intended-for-care medications.

Reason:
- this avoids unfairly penalizing the system when notes mention drugs only as history, discussion, or comparison
- it makes the note-to-EHR comparison clinically meaningful


COHORT JUSTIFICATION FOR THE PAPER
Suggested wording:
We defined eligibility at the visit level because the study evaluates visit-level treatment-context medication extraction and downstream note-to-EHR concordance. Patients were included if they had at least one eligible visit. We did not exclude patients solely on the basis of high or low visit counts, because utilization extremes are clinically meaningful rather than necessarily erroneous. Instead, we characterized visit-count distributions and used patient-aware sensitivity analyses to ensure that results were not driven by a small number of high-utilization patients.

WHY THIS SCOPE IS NOVEL, FEASIBLE, AND COMPARABLE
Novel:
- extraction truth comes from adjudicated note context, not structured EHR alone
- note status is separated from downstream concordance
- normalization/linking is evaluated in a layered way

Feasible:
- treatment-context spans are narrower than full-note review
- adjudication is done on a designed subset, not the entire corpus
- the main pipeline remains CPU-feasible

Comparable:
- similar work already measures structured-vs-unstructured overlap and contextual medication extraction
- our study remains comparable by reporting mention-level extraction metrics, normalization results, and downstream concordance
- our main distinction is that we separate note truth from structured documentation agreement