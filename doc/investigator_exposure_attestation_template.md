# Investigator Exposure Attestation — PHI-Free Template

Status: blank template; **not a signed attestation, eligibility decision, or IRB determination**

Purpose: document what prior investigators actually viewed or used, so the restricted patient-level exposure matrix can be classified in **batches** where evidence is complete. Do not put patient identifiers, note excerpts, screenshots, or medication strings here. Patient-level joins and exceptions stay in the approved restricted workspace.

Investigator name and role:  
Date completed:  
Period of involvement:  
Artifacts/runs covered (exact directory or file names and approximate dates):  
How you know the scope is complete (logs, saved review files, personal notes, other):  

For **each** artifact/run or coherent batch, copy and complete this block:

1. Artifact/run name and version/date:  
2. Was it generated automatically only? **Yes / No / Unknown**. If yes, did you or another person subsequently open its patient-level content?  
3. Did you directly view complete notes, excerpts, candidate spans, predictions, error reports, presentations, or screenshots? **None / Specify types / Unknown**.  
4. Approximately how many patients/notes were viewed? Provide aggregate count only; where is the **restricted** list of exact IDs or note keys?  
5. Did observations from this batch influence aliases, candidate rules, prompts, thresholds, medication categories, model choice, debugging, evaluation rules, or manuscript interpretation? **No / Yes (describe) / Unknown**.  
6. Can every patient whose material was viewed or used be recovered from named restricted files? **Yes / No / Unknown**. If not, describe untracked/off-file exposure (meetings, email, working copies, notebooks, verbal examples) without PHI.  
7. Were patient/note IDs transformed or replaced by alternate keys? If yes, where is the restricted crosswalk?  
8. Exposure conclusion for the **batch**, with any exceptions: **automated-only / human-inspected / development-informing / unresolved**. State the evidence and why the conclusion applies to the whole batch.  

Manual alias resources to address explicitly: `resources/manual/pathA_alias_review.csv`, `pathA_alias_exclusions.csv`, `pathA_v2_alias_supplement.csv`, and `pathA_alias_map.json`. These files generally do not carry patient IDs; identify their source review files/patients or mark provenance unresolved. Cover completed random audits, no-overlap review, Path B/leftover reviews, clinic-only and global packets, and any off-repository copies.

Final statement: “The above describes my own review and development use to the best of my knowledge. I have identified known exceptions and unresolved sources rather than assuming that an absent exact-ID join proves no exposure.”  
Investigator signature or documented electronic attestation:  
Date:  

## Processing rule for the analysis team

Do not mark all 1,275 provisional automated-exposure patients as confirmed B solely from packet generation or from an unsigned batch statement. Check that the attestation covers the relevant run, dates, reviewers, and recoverable exceptions. Translate **verified** batch assertions and ID-linked exceptions into the restricted `validation_exposure_matrix.csv`; store source/attestor/date and mark unresolved patients `Unknown`. A/B may become eligible only after governance and all applicable investigators' exposure evidence are reconciled. Keep the public report aggregate-only.
