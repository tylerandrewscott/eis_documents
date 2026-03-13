#!/usr/bin/env python3
"""
scrape_projects.py

For each forest in forest_url_map.csv, scrapes the project listing pages
(current and archive) to build a complete project_num → title mapping.

Output: usfs/metadata/usfs_project_listing.csv
  Columns: project_num, title, forest_slug, region, archived, project_url

Safe to re-run: merges with existing file, only fetches forests not already
in the output (unless --refresh-forest NAME is passed).

Usage:
    python3 scrape_projects.py                      # all forests not yet scraped
    python3 scrape_projects.py --force              # re-scrape all forests
    python3 scrape_projects.py --forest payette     # re-scrape one forest
"""

import sys
import time
import argparse
from pathlib import Path

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE = "https://www.fs.usda.gov"
META_DIR = Path(__file__).parent.parent / "metadata"
FOREST_MAP = META_DIR / "forest_url_map.csv"
OUT_FILE   = META_DIR / "usfs_project_listing.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}
DELAY = 0.75


def scrape_listing_page(url: str, region: str, forest_slug: str, archived: bool) -> list[dict]:
    """Scrape one listing page and return list of project dicts."""
    try:
        time.sleep(DELAY)
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code != 200:
            print(f"    {url} → {r.status_code}")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        rows = []
        prefix = f"/{region}/{forest_slug}/projects/"
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            if not href.startswith(prefix):
                continue
            suffix = href[len(prefix):]
            if not suffix.isdigit():
                continue
            project_num = int(suffix)
            title = a.text.strip()
            rows.append({
                "project_num":  project_num,
                "title":        title,
                "forest_slug":  forest_slug,
                "region":       region,
                "archived":     archived,
                "project_url":  f"{BASE}{href}",
            })
        return rows
    except Exception as e:
        print(f"    ERROR scraping {url}: {e}")
        return []


def scrape_forest(region: str, forest_slug: str) -> list[dict]:
    """Scrape current and archive listings for a forest."""
    rows = []
    base_url = f"{BASE}/{region}/{forest_slug}/projects"

    current = scrape_listing_page(base_url, region, forest_slug, archived=False)
    rows.extend(current)
    print(f"    current: {len(current)} projects")

    archive_url = f"{base_url}/archive"
    archived = scrape_listing_page(archive_url, region, forest_slug, archived=True)
    rows.extend(archived)
    print(f"    archive: {len(archived)} projects")

    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--force",   action="store_true", help="Re-scrape all forests")
    parser.add_argument("--forest",  default=None,        help="Re-scrape one forest by slug")
    args = parser.parse_args()

    if not FOREST_MAP.exists():
        print(f"Forest map not found: {FOREST_MAP}")
        print("Run discover_forests.py first.")
        sys.exit(1)

    forest_map = pd.read_csv(FOREST_MAP)

    # Load existing output if present
    if OUT_FILE.exists() and not args.force:
        existing = pd.read_csv(OUT_FILE, dtype={"project_num": "Int64"})
    else:
        existing = pd.DataFrame()

    already_done = set()
    if not existing.empty and not args.force:
        already_done = set(existing["forest_slug"].unique())

    # Determine which forests to (re-)scrape
    if args.forest:
        to_scrape = forest_map[forest_map["forest_slug"] == args.forest]
        if to_scrape.empty:
            print(f"Forest '{args.forest}' not found in forest_url_map.csv")
            sys.exit(1)
        # Remove this forest from existing before re-adding
        if not existing.empty:
            existing = existing[existing["forest_slug"] != args.forest]
    elif args.force:
        to_scrape = forest_map
        existing = pd.DataFrame()
    else:
        to_scrape = forest_map[~forest_map["forest_slug"].isin(already_done)]

    print(f"Forests to scrape: {len(to_scrape)}")
    print(f"Already done:      {len(already_done)}")
    print()

    new_rows = []
    for _, row in to_scrape.iterrows():
        region = row["region"]
        slug   = row["forest_slug"]
        name   = row["forest_name"]
        print(f"[{region}/{slug}] {name}")
        rows = scrape_forest(region, slug)
        print(f"    total: {len(rows)}")
        new_rows.extend(rows)

    if new_rows:
        new_df = pd.DataFrame(new_rows)
        combined = pd.concat([existing, new_df], ignore_index=True)
        # Keep most recent entry per project_num (in case of duplicates)
        combined = combined.drop_duplicates(subset=["project_num"], keep="last")
        combined.sort_values(["region", "forest_slug", "project_num"], inplace=True)
        combined.to_csv(OUT_FILE, index=False)
        print(f"\nWrote {len(combined)} projects → {OUT_FILE}")
    elif not existing.empty:
        print("No new forests scraped; existing file unchanged.")
    else:
        print("No data collected.")


if __name__ == "__main__":
    main()
