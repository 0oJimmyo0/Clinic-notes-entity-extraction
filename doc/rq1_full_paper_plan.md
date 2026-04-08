# Full Paper Plan 

## 1) Title and What This Project Is About

### Recommended title
Recoverable treatment-context drug concordance between clinical notes and structured EHR under transparent normalization and calibrated semantic linking

### One-sentence study framing
We quantify how much visit-level drug agreement can be recovered between treatment-context note entities and structured reference entities using an interpretable baseline, deterministic harmonization (Path A), and a calibrated ontology-constrained linker (Path B).

### Current progress baseline
- End-to-end pipeline is already running and reproducible.
- Baseline and Path A are complete with measurable drug-domain gains.
- Existing Path B (char-ngram TF-IDF linker) shows limited incremental value, motivating redesign.

## 2) Motivation and Main Contribution

### Why this matters
- Medication history in free text is rich but messy; structured references are cleaner but incomplete in expression.
- Most studies focus on extraction model scores, while operational harmonization and calibration are underreported.
- Clinical deployment needs auditable decisions, not only higher headline metrics.

### Main contribution claims for the paper
1. A reproducible treatment-context concordance framework with explicit scope control.
2. Empirical saturation analysis of deterministic harmonization (Path A).
3. A redesigned Path B that uses ontology-constrained linking with calibration rather than unconstrained fuzzy similarity.
4. A full baseline/A/B evaluation with utility-risk accounting, adjudication, and inferential reporting.
5. A boundary-result interpretation: if Path B shows only modest incremental utility, we characterize the saturation point where added flexibility mostly increases risk.

### Scope guardrails (keep explicit)
- Primary claim: drug-domain concordance under treatment-context span extraction.
- Structured data is a reference modality, not absolute ground truth.
- Conditions/procedures/labs are secondary descriptive analyses, not primary endpoints.
- Avoid all-domain or full-note generalization claims unless expansion is completed.

## 3) Literature Review (Recent and Integrated)

### Clinical NLP foundations and reproducibility
- cTAKES introduced modular clinical NLP architecture and practical pipeline composition [1].
- CLAMP emphasized rapid configurable clinical pipeline development [2].
- medspaCy showed lightweight, practical rule/context tooling for clinical text workflows [3].
- Reproducibility differences across clinical NLP suites highlight why transparent methods and diagnostics are needed [4].

### Medication extraction and contextual event understanding
- The 2022 n2c2 contextualized medication event shared task showed high NER performance but much lower event/context performance, especially for indirect cues and long-range context [5].
- ADE extraction reviews (2024) show strong recent deep-learning progress, but continued dependence on benchmark-specific pipelines and heterogeneous reporting [6].
- Clinical NER/RE systematic reviews (2023) report transformer dominance with persistent transportability and implementation gaps [7].

### Linking landscape informing Path B
- BioBERT demonstrated gains from biomedical-domain pretraining [8].
- ClinicalBERT released domain-specific clinical embeddings and showed improvements on several clinical tasks [9].
- SapBERT introduced self-aligned biomedical entity representations for medical entity linking and supports ontology-aware semantic normalization [10].
- Recent PLM surveys in medicine emphasize reliability, explainability, and deployment constraints as open issues [11].

### Positioning of this paper
- Not a claim of new SOTA medication extractor.
- Claiming an implementation-grounded concordance framework: deterministic recovery and calibrated ontology-constrained linking under strict reporting rules.
- Publishability does not depend on large Path B gains; a well-calibrated small-gain or boundary result is still a valid contribution.

## 4) Data Expansion and Feasibility Plan

### Current data (already available)
- Primary cohort: VUMC treatment-context notes and visit-level structured references.
- Current extraction scope: trigger-centered spans, not full-note ingestion.

### Expansion targets (minimum effort, maximum publishability)

#### Phase 1 (must do now)
- Keep VUMC as primary dataset.
- Add stronger inference rigor:
  - strict non-empty primary metrics,
  - bootstrap confidence intervals,
  - paired significance tests,
  - adjudication-based precision/risk estimates.

#### Phase 2 (optional, only if low-friction)
- Add MIMIC-IV portability analysis with the same pipeline design.
- Report transfer behavior as an external validity check (no major architecture changes).
- This is not a submission gate for the first paper.

#### Phase 3 (optional)
- Add temporal split or second-site sensitivity analysis.

## 5) Pipeline Design Philosophy and Technical Plan

### 5.1 Design principles
- Keep every path auditable and stage-wise comparable.
- Keep all primary methods CPU-feasible and reproducible in constrained environments.
- Evaluate downstream concordance impact, not only extractor-internal metrics.

### 5.2 Existing script mapping
- Candidate spans: resources/script/run_candidates_overnight.py
- Stage 2 extraction: resources/script/run_stage2_overnight.py
- Note visit aggregation: resources/script/run_rq1_step2_aggregate.py
- Structured visit aggregation: resources/script/run_rq1_step3_build_ehr_by_visit.py
- Timeline prep: resources/script/run_rq1_step3b_build_timeline.py
- Similarity runner: resources/script/run_rq1_step4_similarity.py
- Drug linking helpers: resources/script/rq1_drug_linking.py
- Linker calibration: resources/script/run_rq1_step4b_calibrate_drug_linker.py
- Result table builder: resources/script/run_rq1_step5_make_outputs.py

### 5.3 Paths for baseline/A/B comparison

#### Baseline
- Minimal normalization and lexical overlap.

#### Path A (deterministic harmonization)
- Canonicalization, alias expansion, brand-to-generic mapping, rule-constrained normalization.

#### New Path B (redesigned from current low-yield linker)
Replace current char-ngram-only linker with a calibrated ontology-constrained linker:
1. Candidate generation:
   - unresolved Path A mentions -> RxNorm/UMLS candidate set via lexical retrieval (BM25 or TF-IDF).
2. Candidate scoring:
   - compute candidate similarity using conservative lexical plus ontology-aware signals (term overlap, normalized edit features, alias membership, optional embedding reranker if available).
   - in manuscript wording, describe this as constrained scoring rather than deep semantic understanding unless embedding reranking is truly dominant.
3. Constraint filtering:
   - enforce hard guards (route/form conflicts, negation context, explicit discontinuation mismatch flags).
4. Score calibration:
   - map raw score to calibrated confidence (isotonic or Platt scaling) using adjudicated sample.
5. Acceptance policy:
   - threshold by calibrated precision target (for example >=0.90) instead of fixed cosine heuristic.

Expected benefit of new Path B:
- Better synonym/abbreviation resolution than lexical-only matching.
- Lower false-link risk via ontology constraints and calibrated acceptance.

Pre-specified interpretation if Path B gain is modest:
- If Path B adds only small incremental concordance but increases risk beyond calibrated operating points, we report this as evidence of deterministic saturation and limited marginal utility.


## 6) Evaluation Matrix and Result Plan (Baseline vs A vs B)

### 6.1 Primary outcomes
- Drug-domain strict non-empty visits:
  - relaxed containment,
  - relaxed overlap,
  - Jaccard.
- Report 95% bootstrap CIs and paired deltas for:
  - baseline -> A,
   - A -> B,
   - baseline -> B.

### 6.2 Secondary outcomes
- Domain reliability map across non-drug domains.
- Temporal window sensitivity (k=0,1,2).
- Coverage rates to contextualize denominator effects.

### 6.3 Calibration and safety reporting (required)
- Threshold sweep for Path B calibrated scores.
- Utility-risk curve:
  - utility = concordance gain,
  - risk = adjudicated false-link rate / low-confidence accept fraction.
- Manual review set stratified by score bands and error type.
- Error buckets by stage:
  - unresolved after baseline,
  - unresolved after A,
   - accepted-correct / accepted-incorrect / rejected-correct / rejected-incorrect for B.

### 6.4 Decision rule and publishable outcomes
- Path A is expected to provide the primary recoverable gain.
- Path B is judged by calibrated utility-risk behavior, not by absolute gain alone.
- Two publishable outcomes are pre-accepted:
   - meaningful Path B gain at acceptable risk, or
   - modest Path B gain with rising risk beyond threshold, establishing a practical boundary.

### 6.5 Minimum figure/table package for submission
- Table 1: baseline/A/B primary drug metrics with CI.
- Table 2: Path B calibration sweep and selected operating point.
- Table 3: adjudication precision by confidence band.
- Figure 1: path ablation gains.
- Figure 2: utility-risk frontier.
- Figure 3: stage-wise error buckets.

## 7) Manuscript Narrative by Section

### Title/About
Frame as a concordance recovery and calibration study, not as generic medication extraction SOTA.

### Motivation/Contribution
Emphasize practical harmonization gap, transparent method design, and calibrated decision-making.

### Related Work
Bridge rule-based toolkits, contextual medication extraction limits, transformer gains, and linking constraints.

### Data
State treatment-context extraction scope and cohort characteristics; present portability (for example MIMIC-IV) as optional extension.

### Methods
Describe baseline/A/B, calibration protocol, adjudication schema, and primary endpoint policy.

### Results
Lead with strict non-empty drug outcomes and calibrated B operating point, including explicit reporting if Path B gains are modest.

### Discussion/Conclusion
Interpret saturation boundary of deterministic rules and quantified value (or limited marginal value) of calibrated ontology-constrained linking.

## 8) Checklist (Actionable, Ordered)

### Priority 1: must complete for publishable manuscript
- [ ] Implement new Path B candidate generation + constrained scoring + calibration hooks in resources/script/rq1_drug_linking.py.
- [ ] Add calibration fit and sweep export for new Path B in resources/script/run_rq1_step4b_calibrate_drug_linker.py.
- [ ] Extend path runner to baseline/A/B outputs in resources/script/run_rq1_step4_similarity.py.
- [ ] Add CI and paired delta script for strict non-empty primary outcomes.
- [ ] Produce adjudication sample sheets and complete manual review.
- [ ] Regenerate paper-ready tables/figures in episode_extraction_results/rq1/.

### Priority 2: strong boost with moderate effort
- [ ] Run one portability analysis slice on MIMIC-IV.
- [ ] Add subgroup analysis by note type/service if available.

Note:
- Priority 2 items are optional and do not block initial submission.

### Priority 3: optional enhancement
- [ ] Add full-note extraction sensitivity to quantify trigger-span limitation.
- [ ] Add compute/cost table for deployment framing.

## 9) References (Curated Starter Set)
[1] Savova GK, Masanz JJ, Ogren PV, et al. cTAKES: architecture, component evaluation and applications. J Am Med Inform Assoc. 2010;17(5):507-513. doi:10.1136/jamia.2009.001560.

[2] Soysal E, Wang J, Jiang M, et al. CLAMP: a toolkit for efficiently building customized clinical NLP pipelines. J Am Med Inform Assoc. 2018;25(3):331-336. doi:10.1093/jamia/ocx132.

[3] Eyre H, Chapman AB, Peterson KS, et al. Launching into clinical space with medspaCy. AMIA Annu Symp Proc. 2022;2021:438-447. PMID:35308962.

[4] Digan W, Neveol A, Neuraz A, et al. Can reproducibility be improved in clinical NLP? J Am Med Inform Assoc. 2021;28(3):504-515. doi:10.1093/jamia/ocaa261.

[5] Mahajan D, Liang JJ, Tsou CH, Uzuner O. Overview of the 2022 n2c2 shared task on contextualized medication event extraction in clinical notes. J Biomed Inform. 2023;144:104432. doi:10.1016/j.jbi.2023.104432. PMID:37356640.

[6] Modi S, Kasmiran KA, Mohd Sharef N, Sharum MY. Extracting adverse drug events from clinical notes: a systematic review. J Biomed Inform. 2024;151:104603. doi:10.1016/j.jbi.2024.104603. PMID:38331081.

[7] Fraile Navarro D, Ijaz K, Rezazadegan D, et al. Clinical named entity recognition and relation extraction: a systematic review. Int J Med Inform. 2023;177:105122. doi:10.1016/j.ijmedinf.2023.105122. PMID:37295138.

[8] Lee J, Yoon W, Kim S, et al. BioBERT: a pre-trained biomedical language representation model for biomedical text mining. Bioinformatics. 2020;36(4):1234-1240. doi:10.1093/bioinformatics/btz682. PMID:31501885.

[9] Alsentzer E, Murphy J, Boag W, et al. Publicly Available Clinical BERT Embeddings. ClinicalNLP Workshop. 2019:72-78. doi:10.18653/v1/W19-1909.

[10] Liu F, Shareghi E, Meng Z, Basaldella M, Collier N. Self-Alignment Pretraining for Biomedical Entity Representations (SapBERT). NAACL-HLT. 2021:4228-4238. doi:10.18653/v1/2021.naacl-main.334.

[11] Luo X, Deng Z, Yang B, Luo MY. Pre-trained language models in medicine: A survey. Artif Intell Med. 2024;154:102904. doi:10.1016/j.artmed.2024.102904. PMID:38917600.
