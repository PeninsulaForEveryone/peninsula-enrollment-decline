"""
fetch_enrollment.py
────────────────────
Downloads and cleans CDE enrollment bundle files for San Mateo County (county 41).

CDE now distributes historical enrollment as multi-year bundles at:
  https://www3.cde.ca.gov/demo-downloads/enrsch/

Three bundles cover 2014-15 through 2022-23 (school-level, tab-delimited,
has header row). A separate Census Day file covers 2023-24+.

Bundle file layout (fsenrps.asp):
  CDS_CODE (14-char), ACADEMIC_YEAR, ETHNIC, GENDER,
  KDGN, GR_1..GR_12, UNGR_ELM, UNGR_SEC, ENR_TOTAL, ADULT

Output: data/processed/enrollment_clean.parquet
        Columns: year (int), district_code (str), district_name (str),
                 enrollment (int)

Run standalone:  python -m pipeline.fetch_enrollment
"""

import logging
import sys
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline.config import (
    CDE_BUNDLE_FILES,
    CDE_CENSUS_FILES,
    DATA_PROCESSED,
    DATA_RAW,
    GRADE_COLS_BUNDLE,
    MIN_ENROLLMENT,
    NEW_KEY_COLS,
    SMC_COUNTY_CODE,
)

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)

ENROLLMENT_RAW_DIR = DATA_RAW / "enrollment"
ENROLLMENT_RAW_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": "https://www.cde.ca.gov/ds/ad/fileshistenr8122.asp",
    "Accept": "text/plain,text/html,*/*",
}


# ── Helpers ────────────────────────────────────────────────────────────────────

def _download(url: str, dest: Path, force: bool = False) -> Optional[Path]:
    """Download url -> dest. Returns dest on success, None on failure."""
    if dest.exists() and not force:
        log.info("  cache hit: %s", dest.name)
        return dest
    log.info("  fetching %s", url)
    try:
        with requests.get(url, timeout=(15, 300), headers=HEADERS, stream=True) as r:
            r.raise_for_status()
            total = 0
            with open(dest, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
                    total += len(chunk)
                    if total % (10 * 1024 * 1024) < 1024 * 1024:
                        log.info("  ... %.0f MB", total / 1e6)
        log.info("  saved %s (%.1f MB)", dest.name, total / 1e6)
        return dest
    except requests.HTTPError as e:
        log.warning("  HTTP %s for %s", e.response.status_code, url)
        if dest.exists(): dest.unlink()
        return None
    except Exception as e:
        log.warning("  failed (%s)", e)
        if dest.exists(): dest.unlink()
        return None


def _academic_year_to_int(ay: str) -> int:
    """'2022-23' -> 2023"""
    return 2000 + int(ay.split("-")[1])


# ── Bundle file parser (2014-15 through 2022-23) ──────────────────────────────

def _parse_bundle(path: Path, target_years: List[str]) -> pd.DataFrame:
    """
    Parse a CDE multi-year bundle file and return district-level enrollment.

    CDS_CODE is a 14-character string: CC DDD SSSSSSS
      CC  = 2-digit county code (positions 0-1)
      DDD = 5-digit district code (positions 2-6)
      SSS = 7-digit school code (positions 7-13)

    We filter rows where CDS_CODE[:2] == '41' (San Mateo County) and
    exclude school_code == '0000000' (district aggregate rows).
    """
    log.info("  parsing %s (years: %s)", path.name, target_years)

    chunks = []
    try:
        reader = pd.read_csv(
            path,
            sep="\t",
            dtype=str,
            low_memory=False,
            encoding="latin-1",
            chunksize=50_000,
        )
        for chunk in reader:
            chunk.columns = [c.strip() for c in chunk.columns]
            if "ACADEMIC_YEAR" in chunk.columns:
                chunk = chunk[chunk["ACADEMIC_YEAR"].isin(target_years)]
            if "CDS_CODE" in chunk.columns:
                chunk = chunk[chunk["CDS_CODE"].str[:2] == SMC_COUNTY_CODE]
            if not chunk.empty:
                chunks.append(chunk)
    except Exception as e:
        log.error("  failed to parse %s: %s", path.name, e)
        return pd.DataFrame()

    if not chunks:
        log.warning("  no SMC rows found in %s", path.name)
        return pd.DataFrame()

    df = pd.concat(chunks, ignore_index=True)
    log.info("  SMC rows: %d", len(df))

    # ENR_TYPE='C' is the Census Day combined count (primary + short-term).
    # ENR_TYPE='P' is primary-only. Both rows have identical ENR_TOTAL — summing
    # both would double every count. Keep only 'C'.
    df = df[df["ENR_TYPE"] == "C"].copy()

    df["district_code"] = df["CDS_CODE"].str[2:7]
    df["school_code"]   = df["CDS_CODE"].str[7:14]
    df = df[df["school_code"] != "0000000"].copy()

    # Use ENR_TOTAL directly (pre-summed, excludes ADULT).
    # Fall back to summing grade columns if ENR_TOTAL is absent.
    if "ENR_TOTAL" in df.columns:
        df["ENR_TOTAL"] = pd.to_numeric(df["ENR_TOTAL"], errors="coerce").fillna(0)
        df["k12_total"] = df["ENR_TOTAL"]
    else:
        present_grade_cols = [c for c in GRADE_COLS_BUNDLE if c in df.columns]
        for col in present_grade_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        df["k12_total"] = df[present_grade_cols].sum(axis=1)

    df["year"] = df["ACADEMIC_YEAR"].apply(_academic_year_to_int)

    result = (
        df.groupby(["district_code", "year"])
        .agg(enrollment=("k12_total", "sum"))
        .reset_index()
    )
    result["county_code"] = SMC_COUNTY_CODE
    return result


def fetch_bundle_enrollment(force: bool = False) -> pd.DataFrame:
    """Download all three bundles and return combined district x year dataframe."""
    frames = []
    for filename, url, years in CDE_BUNDLE_FILES:
        dest = ENROLLMENT_RAW_DIR / filename
        path = _download(url, dest, force=force)
        if path is None:
            log.warning("Skipping bundle %s (download failed)", filename)
            continue
        df = _parse_bundle(path, years)
        if not df.empty:
            frames.append(df)

    if not frames:
        raise RuntimeError(
            "No enrollment bundle files could be downloaded.\n"
            "Verify https://www3.cde.ca.gov/demo-downloads/enrsch/ is reachable."
        )
    return pd.concat(frames, ignore_index=True)


# ── District name lookup ───────────────────────────────────────────────────────

def _fetch_district_names() -> Dict[str, str]:
    """
    Return {district_code: district_name} by reading the cached Census Day file.
    The Census Day files contain DistrictName directly — no separate lookup needed.
    """
    log.info("  extracting district names from cached Census Day file...")
    # Use the most recent cached Census Day file available
    for _, url in reversed(CDE_CENSUS_FILES):
        filename = url.split("/")[-1]
        path = ENROLLMENT_RAW_DIR / filename
        if not path.exists():
            continue
        try:
            df = pd.read_csv(path, sep="\t", dtype=str, low_memory=False, encoding="latin-1")
            df.columns = [c.strip() for c in df.columns]
            df = df[df["AggregateLevel"].str.upper() == "D"].copy()
            df = df[df["ReportingCategory"].str.upper() == "TA"].copy()
            df["CountyCode"] = df["CountyCode"].str.zfill(2)
            df = df[df["CountyCode"] == SMC_COUNTY_CODE].copy()
            df = df.drop_duplicates(subset=["DistrictCode"])
            names = dict(zip(df["DistrictCode"].str.zfill(5), df["DistrictName"].str.strip()))
            log.info("  loaded %d district names from %s", len(names), filename)
            return names
        except Exception as e:
            log.warning("  could not extract names from %s: %s", filename, e)
    log.warning("  no cached Census Day file found for district names")
    return {}


# ── New Census Day format (2023-24+) ──────────────────────────────────────────

def fetch_new_enrollment(force: bool = False) -> pd.DataFrame:
    """Download and parse all Census Day enrollment files (2023-24 onward).

    File columns (confirmed from cdenroll2324-v2.txt):
      AggregateLevel: D=district, S=school, C=county, T=state
      ReportingCategory: TA=Total All Students (the row we want)
      TOTAL_ENR: total enrollment count
      DistrictName: district name (no separate lookup needed)
    """
    frames = []
    for year, url in CDE_CENSUS_FILES:
        filename = url.split("/")[-1]
        dest = ENROLLMENT_RAW_DIR / filename
        path = _download(url, dest, force=force)
        if path is None:
            log.warning("  %s unavailable — skipping", filename)
            continue
        try:
            df = pd.read_csv(path, sep="\t", dtype=str, low_memory=False, encoding="latin-1")
            df.columns = [c.strip() for c in df.columns]

            # Keep district-level, Total All Students rows
            df = df[df["AggregateLevel"].str.upper() == "D"].copy()
            df = df[df["ReportingCategory"].str.upper() == "TA"].copy()

            # Filter to San Mateo County
            df["CountyCode"] = df["CountyCode"].str.zfill(2)
            df = df[df["CountyCode"] == SMC_COUNTY_CODE].copy()

            # Each district has 2-3 rows: one district total + breakdowns by funding type.
            # The total row has the highest enrollment. Use grade-column sum (K-12, no TK)
            # for consistency with bundle files, then keep the max-enrollment row per district.
            k12_cols = ["GR_KN"] + [f"GR_{str(i).zfill(2)}" for i in range(1, 13)]
            present = [c for c in k12_cols if c in df.columns]
            for c in present:
                df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0)
            df["enrollment"] = df[present].sum(axis=1)

            # Take the row with the highest K-12 enrollment per district (= total row)
            df = df.loc[df.groupby("DistrictCode")["enrollment"].idxmax()].copy()

            df["district_code"] = df["DistrictCode"].str.zfill(5)
            df["district_name"] = df["DistrictName"].str.strip()
            df["year"]          = year

            result = df[["district_code", "district_name", "year", "enrollment"]].copy()
            frames.append(result)
            log.info("  parsed Census Day year %d: %d SMC districts", year, len(result))
        except Exception as e:
            log.warning("  failed to parse %s: %s", filename, e)

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ── Main ───────────────────────────────────────────────────────────────────────

def run(force: bool = False) -> pd.DataFrame:
    """Full enrollment pipeline. Returns clean district-level dataframe."""
    log.info("=== Fetching enrollment bundles (2014-15 to 2022-23) ===")
    bundle_df = fetch_bundle_enrollment(force=force)
    log.info("Bundle rows: %d, years: %s", len(bundle_df), sorted(bundle_df["year"].unique()))

    log.info("=== Fetching district names ===")
    name_lookup = _fetch_district_names()
    bundle_df["district_name"] = bundle_df["district_code"].map(name_lookup)

    log.info("=== Fetching 2023-24 Census Day enrollment ===")
    new_df = fetch_new_enrollment(force=force)

    if not new_df.empty:
        if "district_name" not in new_df.columns:
            new_df["district_name"] = new_df["district_code"].map(name_lookup)
        combined = pd.concat(
            [bundle_df, new_df[["district_code", "year", "enrollment", "district_name"]]],
            ignore_index=True,
        )
    else:
        combined = bundle_df.copy()

    combined = combined.drop_duplicates(subset=["district_code", "year"])

    district_max = combined.groupby("district_code")["enrollment"].max()
    valid = district_max[district_max >= MIN_ENROLLMENT].index
    combined = combined[combined["district_code"].isin(valid)].copy()

    combined = combined.sort_values(["district_code", "year"]).reset_index(drop=True)

    combined.to_parquet(DATA_PROCESSED / "enrollment_clean.parquet", index=False)
    combined.to_csv(DATA_PROCESSED / "enrollment_clean.csv", index=False)
    log.info(
        "Saved %d rows -> %s  (%d districts, years %s-%s)",
        len(combined),
        DATA_PROCESSED / "enrollment_clean.parquet",
        combined["district_code"].nunique(),
        combined["year"].min(),
        combined["year"].max(),
    )
    return combined


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    run(force=parser.parse_args().force)
