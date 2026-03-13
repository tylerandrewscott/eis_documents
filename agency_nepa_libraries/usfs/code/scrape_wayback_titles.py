#!/usr/bin/env python3
"""
scrape_wayback_titles.py

Fetches project titles from Internet Archive snapshots of old USFS project
pages for project numbers not found on the current USFS website.

Designed to run incrementally — safe to stop and restart at any time.

Output: usfs/metadata/usfs_wayback_titles.csv
  Columns: project_num, title, snapshot_url, wayback_status

Statuses:
  found    — title extracted successfully
  no_snap  — no snapshot available in Wayback Machine
  error    — fetch or parse error (will be retried on next run)
  skip     — project_num had no match in CDX API

After running, re-run update_metadata.py to incorporate the new titles.

Usage:
    python3 scrape_wayback_titles.py              # process all unmatched
    python3 scrape_wayback_titles.py --limit 500  # process first 500
    python3 scrape_wayback_titles.py --retry      # retry previous errors
"""

import sys
import time
import argparse
import re
from pathlib import Path

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
META_DIR      = Path(__file__).parent.parent / "metadata"
UNMATCHED_FILE = META_DIR / "usfs_unmatched_projects.csv"
OUT_FILE       = META_DIR / "usfs_wayback_titles.csv"

CDX_URL   = "http://web.archive.org/cdx/search/cdx"
WB_BASE   = "https://web.archive.org/web"
USFS_PROJ = "https://www.fs.usda.gov/project/?project={num}"

# Throttle
CDX_DELAY  = 0.5   # between CDX API calls
FETCH_DELAY = 1.5  # between snapshot fetches

HEADERS = {
    "User-Agent": "Mozilla/5.0 (academic research bot; contact: usfs-nepa-data)",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}


# ---------------------------------------------------------------------------
# CDX / Wayback helpers
# ---------------------------------------------------------------------------
def cdx_lookup(project_num: int, timeout: int = 20) -> str | None:
    """
    Return the most recent archived timestamp for the project page, or None.
    """
    url = USFS_PROJ.format(num=project_num)
    try:
        r = requests.get(
            CDX_URL,
            params={
                "url":        url,
                "output":     "json",
                "limit":      "1",
                "fl":         "timestamp",
                "filter":     "statuscode:200",
                "fastLatest": "true",
            },
            timeout=timeout,
        )
        if r.status_code != 200:
            return None
        data = r.json()
        if len(data) < 2:
            return None
        return data[1][0]  # first result's timestamp
    except Exception:
        return None


def fetch_title(timestamp: str, project_num: int, timeout: int = 30) -> str | None:
    """Fetch a Wayback Machine snapshot and extract the project title."""
    url = USFS_PROJ.format(num=project_num)
    snap_url = f"{WB_BASE}/{timestamp}/{url}"
    try:
        r = requests.get(snap_url, headers=HEADERS, timeout=timeout)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")

        # The old USFS site had the project title in <h1> or <title> tag
        # Title format: "Project Name | Forest Service" or "<h1>Project Name</h1>"
        h1 = soup.find("h1")
        if h1:
            txt = h1.get_text(separator=" ", strip=True)
            # Strip Wayback Machine banner text
            if txt and len(txt) > 3 and "web.archive.org" not in txt.lower():
                return txt

        # Fallback: page title, stripping "| Forest Service"
        title_tag = soup.find("title")
        if title_tag:
            txt = title_tag.get_text(strip=True)
            txt = re.sub(r"\s*\|?\s*Forest Service\s*$", "", txt, flags=re.I).strip()
            if txt and len(txt) > 3:
                return txt

        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=None,
                        help="Max projects to process in this run")
    parser.add_argument("--retry", action="store_true",
                        help="Retry previous errors (status=error)")
    args = parser.parse_args()

    if not UNMATCHED_FILE.exists():
        print(f"Unmatched file not found: {UNMATCHED_FILE}")
        print("Run update_metadata.py first.")
        sys.exit(1)

    unmatched = pd.read_csv(UNMATCHED_FILE)
    all_nums  = set(unmatched["project_num"].astype(int).unique())

    # Load previous results
    if OUT_FILE.exists():
        prev = pd.read_csv(OUT_FILE, dtype={"project_num": "Int64"})
    else:
        prev = pd.DataFrame(columns=["project_num", "title", "snapshot_url", "wayback_status"])

    done_nums = set(prev["project_num"].astype(int).unique())

    if args.retry:
        # Remove errors from done set so they get retried
        error_nums = set(prev.loc[prev["wayback_status"] == "error", "project_num"].astype(int))
        done_nums -= error_nums
        prev = prev[prev["wayback_status"] != "error"]

    todo = sorted(all_nums - done_nums)
    if args.limit:
        todo = todo[: args.limit]

    print(f"Total unmatched:   {len(all_nums):,}")
    print(f"Already processed: {len(done_nums):,}")
    print(f"To process now:    {len(todo):,}")
    if args.limit:
        print(f"(limited to {args.limit})")
    print()

    new_rows = []
    for i, pnum in enumerate(todo, 1):
        if i % 100 == 0:
            print(f"  {i}/{len(todo)} — found so far: {sum(1 for r in new_rows if r['wayback_status']=='found')}")
            # Flush checkpoint
            _flush(prev, new_rows)

        time.sleep(CDX_DELAY)
        ts = cdx_lookup(pnum)
        if ts is None:
            new_rows.append({
                "project_num":   pnum,
                "title":         None,
                "snapshot_url":  None,
                "wayback_status": "no_snap",
            })
            continue

        snap_url = f"{WB_BASE}/{ts}/{USFS_PROJ.format(num=pnum)}"
        time.sleep(FETCH_DELAY)
        title = fetch_title(ts, pnum)

        if title:
            new_rows.append({
                "project_num":   pnum,
                "title":         title,
                "snapshot_url":  snap_url,
                "wayback_status": "found",
            })
        else:
            new_rows.append({
                "project_num":   pnum,
                "title":         None,
                "snapshot_url":  snap_url,
                "wayback_status": "error",
            })

    _flush(prev, new_rows)

    # Final summary
    final = pd.read_csv(OUT_FILE)
    print(f"\nFinal status summary:")
    print(final["wayback_status"].value_counts().to_string())
    print(f"\nTotal with titles: {(final['wayback_status']=='found').sum():,}")


def _flush(prev: pd.DataFrame, new_rows: list) -> None:
    """Append new_rows to prev and save checkpoint."""
    if not new_rows:
        return
    combined = pd.concat([prev, pd.DataFrame(new_rows)], ignore_index=True)
    combined = combined.drop_duplicates(subset=["project_num"], keep="last")
    combined.to_csv(OUT_FILE, index=False)


if __name__ == "__main__":
    main()
