# Module 05 dataset acceptance preparation

The engineered dataset generator produces a new, untrained feature contract. It does not certify compatibility with historical engineered checkpoints. Generate only into a new, disposable directory; existing artifacts must not be replaced.

## Integrity and provenance

The versioned manifest records each final NPZ's SHA-256, shapes, dtypes, feature-contract reference, dataset and split, unit and window counts, and preprocessing identity. `verify_generated_dataset` reloads the frozen contract and validates these records, the preprocessing manifest identity, partitions and finite tensor contents before accepting the bundle. Contract order is authoritative for feature semantics; the generator's validated transformation supplies those semantics. Hashes detect changes relative to the manifest, but are not signatures and cannot authenticate a coordinated replacement of both files and manifest. Retain a trusted external copy of the manifest hash where adversarial tampering matters.

## Short histories and missing values

Module 04 left-pads short histories by repeating the first observed feature row. The dataset manifest records each test unit's observed length, padding count, final observed cycle and policy. A 30-row window may therefore contain fewer than 30 distinct observations. The RUL label belongs to its final observed cycle. Padding remains within an engine history.

Module 04 offline imputation forward-fills, then backfills leading missing values, then uses training medians. Leading backfill may use a later reading, so the complete pipeline is not strictly causal for all possible missing inputs. The prior read-only census found no missing or non-finite feature values in the local FD001–FD004 source files; this is not a guarantee for other inputs.

## Full-data run

Do not run on the previously checked memory-constrained machine. In a separately provisioned environment, first inspect available RAM, free disk, input file hashes and output path. A starting reservation of at least 8 GiB available RAM and 2–4 GiB free disk is an estimate, not measured peak usage. The generator retains histories, transformed frames and window chunks, and concatenation adds copies. A streaming or preallocated writer could reduce peak memory, but needs separate exact-output equivalence tests before adoption.

```bash
python -m ml.features.dataset_generation --input-dir /path/to/cmapss --output-dir /path/to/new/disposable/module05-output --seed 42 --validation-fraction 0.2
python -c 'from ml.features.dataset_generation import verify_generated_dataset; import sys; m=verify_generated_dataset(sys.argv[1]); print(m["manifest_sha256"])' /path/to/new/disposable/module05-output
```

Record the actual resources, runtime, source hashes, output counts and sizes, contract and manifest hashes, feature order, partitions, padding records, shapes, finite values and label alignment. Check that no input or historical artifacts changed. Investigate any failure before accepting the build.

### Controlled acceptance checklist

1. Inventory the twelve `train_FD00*.txt`, `test_FD00*.txt` and `RUL_FD00*.txt` files from a trusted, separately supplied source. Record SHA-256 and sizes before the run; do not download an unverified dataset. On Linux, `sha256sum /path/to/cmapss/{train,test,RUL}_FD00{1,2,3,4}.txt` records the hashes.
2. Record available memory, CPU and disk (`free -h`, `df -h`, `nproc`); stop if headroom is insufficient. The estimates above are starting points, not a capacity guarantee. Never assume a hosted runner has the required free RAM.
3. Select a new output path outside the source and artifact directories. Record its resolved absolute path. Monitor peak resident memory and elapsed time, for example with `/usr/bin/time -v`, and free disk during the build. Preserve the failure logs.
4. Run the generator and verifier commands above. Retain the manifest hash separately from the output bundle. Compare the recorded source hashes with the pre-run inventory and repeat the input hashes afterward. Examine train, validation and per-dataset test counts, shapes, dtypes, common feature order and unit partitions in the manifest.
5. Independently reconstruct sample windows from each dataset and split, including final-cycle RUL alignment, and compare against the generated arrays. Inspect each short-history record for the observed cycle count, within-unit padding count and final label. The manifest is provenance, not independent proof of those semantics.
6. Load every NPZ with `ml.models.dataset.WindowedNpzDataset` only after bundle verification; confirm its X/y sizes, 30-row window dimension, contract feature count and finite values. This does not certify any historical checkpoint.
7. Pass only if source hashes are stable and trusted, generation and verification succeed, all six output files and counts agree with independently checked partitions and windows, labels and padding agree with the source, the reader smoke checks pass, and no protected artifacts changed. A resource failure, missing output, mismatch or unverified source is a failure or incomplete result, never acceptance.

If the process is interrupted, the generator may leave a partial directory. It will refuse to reuse it. Inspect the resolved path and preserve needed failure evidence; only then may the operator remove that explicitly identified disposable output directory or choose a different new path. Never recursively remove a computed or production artifact path.

## Hosted CI design

Use a dedicated pull-request-to-main workflow on a GitHub-hosted runner with `contents: read`, a bounded job timeout, Python setup and pinned compatible test dependencies. Explicitly collect `tests/module05_feature_checks.py`, `tests/module05_dataset_checks.py`, `tests/module05_phase3_checks.py` and `tests/module05_integrity_checks.py`, plus the established offline Module 04 compatibility regression. Fixtures create synthetic inputs and output only under pytest temporary directories. Do not use Docker, Kafka, secrets, model training, production resources or historical artifact writes. Publish test counts and failures. This lightweight job does not replace full FD001–FD004 acceptance.
