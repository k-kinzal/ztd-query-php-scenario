# Executable user workflows

Each `domain/SCN-.../scenario.json` points to one expectation, executable PHP, configuration and remaining gaps. Test code stays in the existing adapter directories; discovery uses the manifests and `lab.py catalog`.

```bash
python3 scripts/lab.py catalog
python3 scripts/lab.py run SCN-sqlite-fixture-lifecycle --cycle CYC-...
```

Copy an existing manifest, review its assertions and follow [the record contract](../docs/records.md). Keep observations in cycles and findings. Tests without a manifest remain available for exploration and regression investigation; they do not gain current verification status automatically.
