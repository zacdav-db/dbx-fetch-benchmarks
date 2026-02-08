# Results

Raw benchmark outputs are written as JSON files under:

- `results/<client_id>/*.json` (gitignored by default)

Use the Quarto report to aggregate and visualize results:

```bash
quarto render report/benchmark_report.qmd
```

This generates:
- `report/benchmark_report.html`
- `results/iterations.csv`
- `results/summary.csv`
