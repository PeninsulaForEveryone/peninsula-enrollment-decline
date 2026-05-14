"""
run_all.py
──────────
Orchestrates the full pipeline:
  1. fetch_enrollment  → data/processed/enrollment_clean.parquet
  2. fetch_housing     → data/processed/housing_clean.parquet
  3. merge             → data/processed/viz_data.json
                       → docs/data/viz_data.json  (symlinked for GitHub Pages)

Usage:
  python -m pipeline.run_all            # run all steps, use cache
  python -m pipeline.run_all --force    # re-download everything
  python -m pipeline.run_all --step enrollment   # single step
"""

import argparse
import logging
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline import fetch_enrollment, fetch_housing, merge
from pipeline.config import DATA_PROCESSED, DOCS, VIZ_OUTPUT_PATH

logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
log = logging.getLogger(__name__)


def copy_to_docs():
    """Copy viz_data.json into docs/data/ for GitHub Pages serving."""
    dest_dir = DOCS / "data"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / "viz_data.json"
    shutil.copy2(VIZ_OUTPUT_PATH, dest)
    log.info("Copied viz_data.json → %s", dest)


def main():
    parser = argparse.ArgumentParser(description="Run the enrollment-vs-housing pipeline")
    parser.add_argument(
        "--force", action="store_true",
        help="Re-download raw files even if cached"
    )
    parser.add_argument(
        "--step",
        choices=["enrollment", "housing", "merge", "all"],
        default="all",
        help="Which step(s) to run (default: all)"
    )
    args = parser.parse_args()

    if args.step in ("enrollment", "all"):
        log.info("\n──────────────────────────────────────")
        log.info("STEP 1: Enrollment")
        log.info("──────────────────────────────────────")
        fetch_enrollment.run(force=args.force)

    if args.step in ("housing", "all"):
        log.info("\n──────────────────────────────────────")
        log.info("STEP 2: Housing production")
        log.info("──────────────────────────────────────")
        fetch_housing.run(force=args.force)

    if args.step in ("merge", "all"):
        log.info("\n──────────────────────────────────────")
        log.info("STEP 3: Merge → viz_data.json")
        log.info("──────────────────────────────────────")
        merge.run()
        copy_to_docs()

    log.info("\n✓ Pipeline complete.")


if __name__ == "__main__":
    main()
