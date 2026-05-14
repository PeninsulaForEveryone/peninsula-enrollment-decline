"""
merge.py
────────
Joins cleaned enrollment and housing data into a single JSON payload
consumed by the React visualization.

Produces: data/processed/viz_data.json

JSON structure:
  {
    "meta": {
      "generated_at": "...",
      "enrollment_years": [...],
      "housing_years": [...],
      "index_year": 2018,
      "county": "San Mateo"
    },
    "districts": [
      {
        "district_code": "...",
        "district_name": "...",
        "type": "elementary" | "high_school" | "unified",
        "enrollment": [
          {"year": 2015, "total": 4200, "indexed": 100.0},
          ...
        ],
        "peak_year": 2016,
        "peak_enrollment": 4450,
        "pct_change_since_peak": -8.5,
        "pct_change_2018_2023": -6.2
      }
    ],
    "jurisdictions": [
      {
        "jurisdiction": "Redwood City",
        "permits_by_year": [
          {"year": 2018, "total": 320, "vl": 40, "low": 30, "mod": 50, "above_mod": 200},
          ...
        ],
        "cumulative_2018_2023": 1480,
        "district_codes": ["4161119", "4168437"]
      }
    ],
    "scatter": [
      {
        "label": "Redwood City / Redwood City ESD",
        "cumulative_permits_2018_2023": 1480,
        "enrollment_pct_change_2018_2023": -4.2,
        "district_code": "4168437",
        "jurisdiction": "Redwood City"
      }
    ],
    "county_totals": {
      "enrollment": [...],
      "permits": [...]
    }
  }

Run standalone:  python -m pipeline.merge
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import (
    DATA_PROCESSED,
    HS_DISTRICTS,
    INDEX_YEAR,
    JURISDICTION_DISTRICT_MAP,
    VIZ_OUTPUT_PATH,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_enrollment() -> pd.DataFrame:
    p = DATA_PROCESSED / "enrollment_clean.parquet"
    if not p.exists():
        raise FileNotFoundError(
            f"{p} not found. Run `python -m pipeline.fetch_enrollment` first."
        )
    return pd.read_parquet(p)


def _load_housing() -> pd.DataFrame:
    p = DATA_PROCESSED / "housing_clean.parquet"
    if not p.exists():
        raise FileNotFoundError(
            f"{p} not found. Run `python -m pipeline.fetch_housing` first."
        )
    return pd.read_parquet(p)


def _district_type(code: str, name: str) -> str:
    name_l = (name or "").lower()
    if "high" in name_l and "unified" not in name_l:
        return "high_school"
    elif "unified" in name_l or "usd" in name_l:
        return "unified"
    return "elementary"


def _safe_pct_change(a: float | None, b: float | None) -> float | None:
    """Return 100*(b-a)/a, or None if either is missing/zero."""
    if a is None or b is None or a == 0:
        return None
    return round(100.0 * (b - a) / a, 2)


# ── District enrollment series ─────────────────────────────────────────────────

def build_district_series(enr: pd.DataFrame) -> list[dict]:
    """
    For each district, compute:
      - year-by-year enrollment with index (INDEX_YEAR = 100)
      - peak year and peak enrollment
      - % change from peak to last year
      - % change from INDEX_YEAR to last year
    """
    districts = []
    for dcode, group in enr.groupby("district_code"):
        group = group.sort_values("year")
        dname = group["district_name"].dropna().iloc[-1] if group["district_name"].notna().any() else dcode
        dtype = _district_type(dcode, dname)

        # Build enrollment series
        series = group[["year", "enrollment"]].rename(columns={"enrollment": "total"})
        series = series.set_index("year")

        # Compute index relative to INDEX_YEAR
        base = series.loc[INDEX_YEAR, "total"] if INDEX_YEAR in series.index else None
        series["indexed"] = (
            (series["total"] / base * 100).round(2) if base else None
        )

        # Peak
        peak_year = int(series["total"].idxmax())
        peak_enrollment = int(series["total"].max())

        # % changes
        last_year = int(series.index.max())
        last_enr = float(series.loc[last_year, "total"])
        index_enr = float(series.loc[INDEX_YEAR, "total"]) if INDEX_YEAR in series.index else None
        pct_from_peak  = _safe_pct_change(peak_enrollment, last_enr)
        pct_since_index = _safe_pct_change(index_enr, last_enr)

        enrollment_records = []
        for yr, row in series.iterrows():
            enrollment_records.append({
                "year": int(yr),
                "total": int(row["total"]),
                "indexed": float(row["indexed"]) if row["indexed"] is not None and not pd.isna(row["indexed"]) else None,
            })

        districts.append({
            "district_code": str(dcode),
            "district_name": str(dname),
            "type": dtype,
            "enrollment": enrollment_records,
            "peak_year": peak_year,
            "peak_enrollment": peak_enrollment,
            "last_year": last_year,
            "last_enrollment": int(last_enr),
            "pct_change_since_peak": pct_from_peak,
            "pct_change_since_index": pct_since_index,
        })

    return sorted(districts, key=lambda d: d["district_name"])


# ── Jurisdiction housing series ────────────────────────────────────────────────

def build_jurisdiction_series(housing: pd.DataFrame) -> list[dict]:
    """
    For each jurisdiction, compute year-by-year permit counts
    and cumulative 2018–2023 total.
    """
    jurisdictions = []
    for juris, group in housing[housing["source"] == "HCD_APR"].groupby("jurisdiction"):
        group = group.sort_values("year")
        permits_by_year = []
        for _, row in group.iterrows():
            permits_by_year.append({
                "year": int(row["year"]),
                "total": int(row["permits_total"]),
                "vl": int(row["permits_vl"]),
                "low": int(row["permits_low"]),
                "mod": int(row["permits_mod"]),
                "above_mod": int(row["permits_above_mod"]),
            })

        window = group[(group["year"] >= 2018) & (group["year"] <= 2023)]
        cumulative = int(window["permits_total"].sum())

        jurisdictions.append({
            "jurisdiction": str(juris),
            "permits_by_year": permits_by_year,
            "cumulative_2018_2023": cumulative,
            "district_codes": JURISDICTION_DISTRICT_MAP.get(str(juris), []),
        })

    return sorted(jurisdictions, key=lambda j: j["jurisdiction"])


# ── Scatter data ───────────────────────────────────────────────────────────────

def build_scatter(
    district_series: list[dict],
    jurisdiction_series: list[dict],
) -> list[dict]:
    """
    Create one scatter point per (jurisdiction, district) pair
    for the cumulative permits vs. enrollment change chart.

    Uses the JURISDICTION_DISTRICT_MAP crosswalk.
    Only produces points for the INDEX_YEAR → last_available window.
    """
    # Index lookups
    juris_by_name = {j["jurisdiction"]: j for j in jurisdiction_series}
    district_by_code = {d["district_code"]: d for d in district_series}

    scatter = []
    for juris_name, district_codes in JURISDICTION_DISTRICT_MAP.items():
        juris = juris_by_name.get(juris_name)
        if juris is None:
            continue  # jurisdiction not in APR data

        for dcode in district_codes:
            district = district_by_code.get(dcode)
            if district is None:
                continue

            pct_chg = district.get("pct_change_since_index")
            cumulative = juris["cumulative_2018_2023"]
            if pct_chg is None or cumulative == 0:
                continue

            scatter.append({
                "label": f"{juris_name} / {district['district_name']}",
                "jurisdiction": juris_name,
                "district_code": dcode,
                "district_name": district["district_name"],
                "district_type": district["type"],
                "cumulative_permits_2018_2023": cumulative,
                "enrollment_pct_change": pct_chg,
                "peak_year": district["peak_year"],
                "peak_enrollment": district["peak_enrollment"],
                "last_enrollment": district["last_enrollment"],
            })

    return sorted(scatter, key=lambda s: s["label"])


# ── County aggregates ──────────────────────────────────────────────────────────

def build_county_totals(enr: pd.DataFrame, housing: pd.DataFrame) -> dict:
    """Aggregate all SMC districts and all SMC jurisdictions to county level."""
    # Enrollment: sum all districts per year
    enr_totals = (
        enr.groupby("year")["enrollment"]
        .sum()
        .reset_index()
        .sort_values("year")
    )
    base_enr = enr_totals[enr_totals["year"] == INDEX_YEAR]["enrollment"].values
    base_enr_val = float(base_enr[0]) if len(base_enr) > 0 else None

    enrollment_series = []
    for _, row in enr_totals.iterrows():
        indexed = round(float(row["enrollment"]) / base_enr_val * 100, 2) if base_enr_val else None
        enrollment_series.append({
            "year": int(row["year"]),
            "total": int(row["enrollment"]),
            "indexed": indexed,
        })

    # Permits: sum APR jurisdictions per year
    apr = housing[housing["source"] == "HCD_APR"]
    permit_totals = (
        apr.groupby("year")["permits_total"]
        .sum()
        .reset_index()
        .sort_values("year")
    )
    permit_series = [
        {"year": int(r["year"]), "total": int(r["permits_total"])}
        for _, r in permit_totals.iterrows()
    ]

    return {"enrollment": enrollment_series, "permits": permit_series}


# ── Main ───────────────────────────────────────────────────────────────────────

def run() -> dict:
    """Produce viz_data.json from cleaned parquet files."""
    log.info("=== Building visualization dataset ===")

    enr = _load_enrollment()
    housing = _load_housing()

    log.info("  Enrollment: %d rows, years %s", len(enr), sorted(enr["year"].unique()))
    log.info("  Housing: %d rows, years %s", len(housing), sorted(housing["year"].unique()))

    district_series    = build_district_series(enr)
    jurisdiction_series = build_jurisdiction_series(housing)
    scatter            = build_scatter(district_series, jurisdiction_series)
    county_totals      = build_county_totals(enr, housing)

    enrollment_years = sorted(enr["year"].unique().tolist())
    housing_years    = sorted(housing[housing["source"] == "HCD_APR"]["year"].unique().tolist())

    viz_data = {
        "meta": {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "enrollment_years": [int(y) for y in enrollment_years],
            "housing_years": [int(y) for y in housing_years],
            "index_year": INDEX_YEAR,
            "county": "San Mateo",
            "notes": [
                "Enrollment excludes TK for consistency across the 2014-2023 series.",
                "Housing data: HCD APR (2018+), Census BPS (pre-2018, county-aggregate only).",
                "APR data is self-reported; see Possibility Lab (2024) for known quality issues.",
                "Crosswalk between HCD jurisdictions and CDE districts is manually curated.",
            ],
        },
        "districts": district_series,
        "jurisdictions": jurisdiction_series,
        "scatter": scatter,
        "county_totals": county_totals,
    }

    VIZ_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(VIZ_OUTPUT_PATH, "w") as f:
        json.dump(viz_data, f, indent=2, default=str)
    log.info("Saved viz_data.json → %s", VIZ_OUTPUT_PATH)

    # Summary diagnostics
    log.info("\n── Summary ──")
    log.info("Districts: %d", len(district_series))
    log.info("Jurisdictions: %d", len(jurisdiction_series))
    log.info("Scatter points: %d", len(scatter))
    if scatter:
        declines = [s for s in scatter if (s["enrollment_pct_change"] or 0) < 0]
        log.info(
            "Districts declining since %d: %d / %d (%.0f%%)",
            INDEX_YEAR,
            len(declines),
            len(scatter),
            100 * len(declines) / len(scatter),
        )

    return viz_data


if __name__ == "__main__":
    run()
