# Peninsula School Enrollment vs. Housing Production

**Rebuts the "new housing overwhelms schools" argument** by showing that K-12 enrollment in most San Mateo County districts has been falling for a decade — and new housing construction has not reversed that.

Live: **[https://yourusername.github.io/enrollment-vs-housing](https://yourusername.github.io/enrollment-vs-housing)**

---

## What this shows

| Claim | What the data shows |
|-------|---------------------|
| "More housing = more kids in schools" | Enrollment peaked in most SMC districts around 2015–2017 and has declined since |
| "We can't build until schools are ready" | The districts building the most units have seen no enrollment reversal |
| "Our schools are already full" | Most elementary districts have lost 5–15% of students since their peak |

## Data sources

| Source | What | Years | URL |
|--------|------|-------|-----|
| CA Dept. of Education Census Day Enrollment | K-12 enrollment by district | 2014–15 → 2023–24 | [cde.ca.gov](https://www.cde.ca.gov/ds/ad/filesenrcensus.asp) |
| HCD Housing Element APR | Building permits by jurisdiction + income category | 2018 → 2023 | [data.ca.gov](https://data.ca.gov/dataset/housing-element-annual-progress-report-apr-data-by-jurisdiction-and-year) |
| Census Bureau BPS | Annual permit totals (pre-APR supplement) | 2010–2017 | [census.gov](https://www2.census.gov/econ/bps/County/) |

## Quickstart

```bash
git clone https://github.com/yourusername/enrollment-vs-housing
cd enrollment-vs-housing
make install
make run
```

This downloads ~30 MB of raw data, processes it, and writes `docs/data/viz_data.json`.
Open `docs/index.html` in a browser to see the visualization.

## Pipeline

```
pipeline/
  config.py            # All URLs, constants, jurisdiction→district crosswalk
  fetch_enrollment.py  # CDE enrollment (two file format eras)
  fetch_housing.py     # HCD APR + Census BPS
  merge.py             # Join + compute indices → viz_data.json
  run_all.py           # Orchestrator
```

Raw files are cached in `data/raw/` (gitignored). The final JSON (`docs/data/viz_data.json`) is committed and served by GitHub Pages.

## Known data quality issues

1. **TK discontinuity (2022–23 → 2023–24):** CDE changed how TK is reported. The pipeline uses K-12 only for consistency; see `config.py` for the TK supplement option.

2. **APR self-reporting noise:** HCD APR data is self-reported by jurisdictions. Duplicate entries and misassigned years occur. The pipeline deduplicates by keeping the latest submission per jurisdiction+year. See [Possibility Lab (2024)](https://possibilitylab.berkeley.edu/project/housing-and-community-development-hcd-annual-progress-reports/) for a full accounting.

3. **Jurisdiction ↔ district crosswalk:** This is manually curated. Redwood City feeds two elementary districts; some cities straddle district lines. Review `JURISDICTION_DISTRICT_MAP` in `config.py` and correct as needed.

4. **HCD APR URL:** The CSV resource URL on data.ca.gov has changed once since 2022. If `fetch_housing.py` fails, the CKAN API fallback will auto-discover the current URL; failing that, update `HCD_APR_CSV_URL` in `config.py`.

## Deployment (GitHub Pages)

1. In repo Settings → Pages, set source to `docs/` folder on `main`.
2. The `refresh_data.yml` workflow runs weekly and commits an updated `viz_data.json`.
3. The React app is a single static `docs/index.html` — no build step needed.

## License

Data: California Open Data (CC BY) and Census Bureau (public domain).  
Code: MIT.
