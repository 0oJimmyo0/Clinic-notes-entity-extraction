# Lexicon Utility Scripts

These scripts maintain the lexicon assets used by the adjudication-first medication extraction workflow.

## Option A: Process local raw vocabularies

If raw sources are already present under `raw/`, run:

```bash
python script/process_raw_vocabularies.py --raw-dir raw --lexicon-dir lexicons
```

## Option B: Download then build

From repository root:

```bash
python script/build_public_lexicons.py download
python script/build_public_lexicons.py build
```

## Candidate term expansion

```bash
python script/discover_terms_from_corpus.py --help
```

## Output location

Lexicon outputs are written under `lexicons/`.

## Scope note

These utilities support terminology preparation and are not part of the core adjudication-first evaluation sequence documented in the root README.
