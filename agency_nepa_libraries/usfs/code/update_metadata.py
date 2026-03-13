#!/usr/bin/env python3
"""
update_metadata.py

Reconciles files on disk with project titles from usfs_project_listing.csv
to produce updated metadata CSVs.

Steps:
  1. Scan documents/ directory to build a file inventory (project_num, year, filename)
  2. Join to usfs_project_listing.csv for titles
  3. Join to existing forest_service_document_record.csv for doc-level fields
     (Stage, Document_Name, Document_File)
  4. Write updated:
       forest_service_document_record.csv   (file-level records)
       forest_service_project_overview.csv  (one row per project)

Also writes:
       usfs_file_inventory.csv  (raw disk scan — project_num, year, filename, path)
       usfs_unmatched_projects.csv  (project_nums on disk not in listing)

Usage:
    python3 update_metadata.py
    python3 update_metadata.py --dry-run   # print stats without writing files
"""

import re
import argparse
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
USFS_DIR   = Path(__file__).parent.parent
META_DIR   = USFS_DIR / "metadata"
DOCS_DIR   = USFS_DIR / "documents"

LISTING_FILE  = META_DIR / "usfs_project_listing.csv"
OLD_DOC_REC   = META_DIR / "forest_service_document_record.csv"
OLD_OVERVIEW  = META_DIR / "forest_service_project_overview.csv"
FOREST_MAP    = META_DIR / "forest_url_map.csv"

OUT_DOC_REC      = META_DIR / "forest_service_document_record.csv"
OUT_OVERVIEW     = META_DIR / "forest_service_project_overview.csv"
OUT_INVENTORY    = META_DIR / "usfs_file_inventory.csv"
OUT_UNMATCHED    = META_DIR / "usfs_unmatched_projects.csv"
OUT_NEW_PROJECTS = META_DIR / "usfs_new_projects.csv"
WAYBACK_FILE     = META_DIR / "usfs_wayback_titles.csv"

# Filename pattern: {project_num}_{file_id}_FSPLT{n}_{hash}.{ext}
# or other patterns. The project_num is always the first segment.
FNAME_RE = re.compile(r"^(\d+)_(.+)$")


# ---------------------------------------------------------------------------
# Step 1: Scan disk
# ---------------------------------------------------------------------------
def scan_documents() -> pd.DataFrame:
    """Walk documents/{year}/ and build file inventory."""
    rows = []
    for year_dir in sorted(DOCS_DIR.iterdir()):
        if not year_dir.is_dir():
            continue
        year = year_dir.name
        for f in year_dir.iterdir():
            if not f.is_file():
                continue
            m = FNAME_RE.match(f.name)
            project_num = int(m.group(1)) if m else None
            rows.append({
                "project_num":  project_num,
                "year":         year,
                "filename":     f.name,
                "size_bytes":   f.stat().st_size,
                "path":         str(f),
            })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Step 2: Load reference data
# ---------------------------------------------------------------------------
def load_listing() -> pd.DataFrame:
    if not LISTING_FILE.exists():
        print(f"WARNING: {LISTING_FILE} not found — run scrape_projects.py first")
        return pd.DataFrame(columns=["project_num", "title", "forest_slug", "region",
                                     "archived", "project_url"])
    df = pd.read_csv(LISTING_FILE, dtype={"project_num": "Int64"})
    df["project_num"] = df["project_num"].astype("Int64")

    # Supplement with Wayback titles for projects not in listing
    if WAYBACK_FILE.exists():
        wb = pd.read_csv(WAYBACK_FILE, dtype={"project_num": "Int64"})
        wb = wb[wb["wayback_status"] == "found"][["project_num", "title"]].copy()
        wb["project_num"] = wb["project_num"].astype("Int64")
        # Only add rows for project_nums not already in listing
        listing_nums = set(df["project_num"].dropna())
        new_wb = wb[~wb["project_num"].isin(listing_nums)].copy()
        new_wb["forest_slug"] = None
        new_wb["region"]      = None
        new_wb["archived"]    = None
        new_wb["project_url"] = wb.get("snapshot_url", None)
        df = pd.concat([df, new_wb[df.columns]], ignore_index=True)
        print(f"  wayback titles: +{len(new_wb)} additional projects")

    return df


def load_old_doc_record() -> pd.DataFrame:
    if not OLD_DOC_REC.exists():
        return pd.DataFrame()
    return pd.read_csv(OLD_DOC_REC, low_memory=False, dtype=str)


def load_forest_map() -> pd.DataFrame:
    if not FOREST_MAP.exists():
        return pd.DataFrame()
    return pd.read_csv(FOREST_MAP)


# ---------------------------------------------------------------------------
# Step 3: Build updated document record
# ---------------------------------------------------------------------------
def build_doc_record(
    inventory: pd.DataFrame,
    listing: pd.DataFrame,
    old_rec: pd.DataFrame,
) -> pd.DataFrame:
    """
    Produce one row per file on disk, with:
      - project_num, filename, year, size_bytes (from inventory)
      - title, forest_slug, region, archived, project_url (from listing join)
      - Stage, Document_Name, Document_File (from old record if available)
    """
    # Build old_rec lookup by File_Name
    old_by_fname: dict = {}
    if not old_rec.empty and "File_Name" in old_rec.columns:
        for _, r in old_rec.iterrows():
            fn = str(r.get("File_Name", "")).strip()
            if fn:
                old_by_fname[fn] = r

    # Join listing on project_num
    listing_dict: dict = {}
    if not listing.empty:
        for _, r in listing.iterrows():
            pn = r["project_num"]
            if pd.notna(pn):
                listing_dict[int(pn)] = r

    rows = []
    for _, inv in inventory.iterrows():
        pn = inv["project_num"]
        lst = listing_dict.get(pn) if pn is not None else None
        old = old_by_fname.get(inv["filename"])

        row = {
            "project_num":    pn,
            "year":           inv["year"],
            "filename":       inv["filename"],
            "size_bytes":     inv["size_bytes"],
            # From listing
            "title":          lst["title"]        if lst is not None else None,
            "forest_slug":    lst["forest_slug"]  if lst is not None else None,
            "region":         lst["region"]       if lst is not None else None,
            "archived":       lst["archived"]     if lst is not None else None,
            "project_url":    lst["project_url"]  if lst is not None else None,
            # From old record
            "Stage":          old["Stage"]          if old is not None else None,
            "Document_Name":  old["Document_Name"]  if old is not None else None,
            "Document_File":  old["Document_File"]  if old is not None else None,
            "Document_Status":old["Document_Status"]if old is not None else None,
        }
        rows.append(row)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Step 4: Build project overview (one row per project)
# ---------------------------------------------------------------------------
def build_overview(doc_record: pd.DataFrame, listing: pd.DataFrame) -> pd.DataFrame:
    """One row per project, with file counts and title."""
    if doc_record.empty:
        return pd.DataFrame()

    # Count files per project
    counts = (
        doc_record.groupby("project_num")
        .agg(
            file_count=("filename", "count"),
            total_size_bytes=("size_bytes", "sum"),
            year=("year", "first"),
        )
        .reset_index()
    )

    if not listing.empty:
        listing_cols = listing[["project_num", "title", "forest_slug", "region",
                                "archived", "project_url"]].drop_duplicates("project_num")
        counts = counts.merge(listing_cols, on="project_num", how="left")

    return counts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Print stats only")
    args = parser.parse_args()

    print("Scanning documents directory...")
    inventory = scan_documents()
    print(f"  {len(inventory):,} files across {inventory['year'].nunique()} year directories")
    print(f"  {inventory['project_num'].nunique():,} unique project numbers")

    print("\nLoading reference data...")
    listing  = load_listing()
    old_rec  = load_old_doc_record()
    print(f"  listing: {len(listing):,} projects with titles")
    print(f"  old doc record: {len(old_rec):,} rows")

    # Stats on matches
    disk_projects  = set(inventory["project_num"].dropna().astype(int).unique())
    listing_projects = set(listing["project_num"].dropna().astype(int).unique()) if not listing.empty else set()
    matched   = disk_projects & listing_projects
    unmatched = disk_projects - listing_projects

    print(f"\n  On disk:        {len(disk_projects):,} projects")
    print(f"  In listing:     {len(listing_projects):,} projects")
    print(f"  Matched:        {len(matched):,}")
    print(f"  Unmatched:      {len(unmatched):,} (no title available)")

    if args.dry_run:
        print("\nDry run — no files written.")
        return

    # Save inventory
    inventory.to_csv(OUT_INVENTORY, index=False)
    print(f"\nWrote inventory: {len(inventory):,} rows → {OUT_INVENTORY}")

    # Save unmatched project list (for Wayback Machine enrichment or manual lookup)
    if unmatched:
        unmatched_inv = inventory[
            inventory["project_num"].notna() &
            inventory["project_num"].astype(int).isin(unmatched)
        ][["project_num", "year"]].drop_duplicates("project_num")
        unmatched_inv.to_csv(OUT_UNMATCHED, index=False)
        print(f"Wrote unmatched: {len(unmatched_inv):,} rows → {OUT_UNMATCHED}")

    # Save new projects (in listing but not on disk)
    if not listing.empty:
        new_projects = listing[~listing["project_num"].astype(int).isin(disk_projects)].copy()
        if not new_projects.empty:
            new_projects.to_csv(OUT_NEW_PROJECTS, index=False)
            print(f"Wrote new projects: {len(new_projects):,} rows → {OUT_NEW_PROJECTS}")

    # Build updated document record
    print("\nBuilding updated document record...")
    doc_record = build_doc_record(inventory, listing, old_rec)
    doc_record.to_csv(OUT_DOC_REC, index=False)
    print(f"Wrote doc record: {len(doc_record):,} rows → {OUT_DOC_REC}")

    # Build project overview
    print("\nBuilding project overview...")
    overview = build_overview(doc_record, listing)
    overview.to_csv(OUT_OVERVIEW, index=False)
    print(f"Wrote overview: {len(overview):,} rows → {OUT_OVERVIEW}")

    # Summary
    titled = doc_record["title"].notna().sum()
    print(f"\nFiles with titles: {titled:,} / {len(doc_record):,} "
          f"({100*titled/len(doc_record):.1f}%)")


if __name__ == "__main__":
    main()
