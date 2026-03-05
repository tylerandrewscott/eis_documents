#!/usr/bin/env python3
"""
One-off script to sanitize filenames across the enepa_repository.

Removes ( ) & ' from PDF filenames and derived outputs (marker conversions,
text conversions, text_as_datatable), collapses consecutive underscores,
strips leading/trailing underscores before extensions, and updates all
metadata CSV/PKL files accordingly.

Usage:
    python rename_sanitize_files.py          # dry-run (default)
    python rename_sanitize_files.py --execute  # actually rename files
"""

import argparse
import os
import re
import sys
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths (relative to this script, which lives in enepa_repository/code/)
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
BOX_FILES = REPO_ROOT / "box_files"
METADATA_DIR = REPO_ROOT / "metadata"

# Directories whose files may need renaming
FILE_DIRS = {
    "documents":          (BOX_FILES / "documents",          "*.pdf"),
    "marker_conversions": (BOX_FILES / "marker_conversions", "*.md"),
    "text_conversions":   (BOX_FILES / "text_conversions",   "*.txt"),
    "text_as_datatable":  (BOX_FILES / "text_as_datatable",  "*"),
}

# Characters to strip from filenames
BAD_CHARS = re.compile(r"[()&']")


# ---------------------------------------------------------------------------
# Filename helpers
# ---------------------------------------------------------------------------

def sanitize_stem(stem: str) -> str:
    """Remove bad chars, collapse underscores, strip leading/trailing _."""
    clean = BAD_CHARS.sub("", stem)
    clean = re.sub(r"_+", "_", clean)
    clean = clean.strip("_")
    return clean


def needs_rename(name: str) -> bool:
    """Return True if the filename contains any character we want to remove."""
    return bool(BAD_CHARS.search(name))


# ---------------------------------------------------------------------------
# Phase 1 – scan & rename files on disk
# ---------------------------------------------------------------------------

def collect_renames(base_dir: Path, glob_pattern: str) -> list[tuple[Path, Path]]:
    """Return list of (old_path, new_path) for files that need renaming."""
    renames = []
    if not base_dir.exists():
        return renames

    for year_dir in sorted(base_dir.iterdir()):
        if not year_dir.is_dir() or not year_dir.name.isdigit():
            continue
        for fpath in sorted(year_dir.glob(glob_pattern)):
            if not fpath.is_file():
                continue
            if not needs_rename(fpath.name):
                continue

            stem = fpath.stem
            suffix = fpath.suffix
            new_stem = sanitize_stem(stem)
            new_name = new_stem + suffix
            if new_name != fpath.name:
                renames.append((fpath, fpath.parent / new_name))

    return renames


def execute_renames(renames: list[tuple[Path, Path]], dry_run: bool) -> dict[str, str]:
    """Rename files on disk.  Returns mapping old_name → new_name."""
    mapping: dict[str, str] = {}
    for old_path, new_path in renames:
        mapping[old_path.name] = new_path.name
        if not dry_run:
            old_path.rename(new_path)
    return mapping


# ---------------------------------------------------------------------------
# Phase 2 – update metadata CSV files
# ---------------------------------------------------------------------------

def update_filename_column(series: pd.Series, name_map: dict[str, str]) -> pd.Series:
    """Replace bare filenames using the old→new mapping."""
    return series.map(lambda v: name_map.get(v, v) if pd.notna(v) else v)


def update_path_column(series: pd.Series, name_map: dict[str, str]) -> pd.Series:
    """Replace the filename portion inside a full-path string."""
    def _fix(val):
        if pd.isna(val):
            return val
        val_str = str(val)
        basename = os.path.basename(val_str)
        if basename in name_map:
            return val_str[: val_str.rfind(basename)] + name_map[basename]
        return val_str
    return series.map(_fix)


def update_csv(csv_path: Path, columns_bare: list[str], columns_path: list[str],
               name_map: dict[str, str], dry_run: bool) -> int:
    """
    Update a CSV file in place.  Returns number of cells changed.

    columns_bare: columns that contain bare filenames (e.g. "filename").
    columns_path: columns that contain full paths (e.g. "source_file").
    """
    if not csv_path.exists():
        print(f"  [skip] {csv_path.name} — file not found")
        return 0

    df = pd.read_csv(csv_path)
    changes = 0

    for col in columns_bare:
        if col not in df.columns:
            continue
        new_col = update_filename_column(df[col], name_map)
        changes += (new_col != df[col]).sum()
        df[col] = new_col

    for col in columns_path:
        if col not in df.columns:
            continue
        new_col = update_path_column(df[col], name_map)
        changes += (new_col != df[col]).sum()
        df[col] = new_col

    if changes > 0 and not dry_run:
        df.to_csv(csv_path, index=False)

    return changes


# ---------------------------------------------------------------------------
# Phase 3 – regenerate PKL files from updated CSVs
# ---------------------------------------------------------------------------

PKL_FROM_CSV = [
    ("download_status_api.csv",       "download_status_api.pkl"),
    ("marker_conversion_status.csv",  "marker_conversion_status.pkl"),
    ("text_conversion_status.csv",    "text_conversion_status.pkl"),
]


def regenerate_pkls(dry_run: bool):
    for csv_name, pkl_name in PKL_FROM_CSV:
        csv_path = METADATA_DIR / csv_name
        pkl_path = METADATA_DIR / pkl_name
        if not csv_path.exists():
            print(f"  [skip] {csv_name} — not found")
            continue
        if not dry_run:
            df = pd.read_csv(csv_path)
            df.to_pickle(pkl_path)
        print(f"  {pkl_name} ← {csv_name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Sanitize filenames in enepa_repository")
    parser.add_argument("--execute", action="store_true",
                        help="Actually rename files and update metadata (default is dry-run)")
    args = parser.parse_args()
    dry_run = not args.execute

    if dry_run:
        print("=== DRY RUN (pass --execute to apply changes) ===\n")
    else:
        print("=== EXECUTING RENAMES ===\n")

    # -- Phase 1: collect and execute renames across all directories ----------
    all_name_map: dict[str, str] = {}
    total_renames = 0

    for label, (base_dir, pattern) in FILE_DIRS.items():
        renames = collect_renames(base_dir, pattern)
        if not renames:
            print(f"[{label}] No files need renaming.")
            continue

        print(f"[{label}] {len(renames)} file(s) to rename:")
        for old, new in renames[:20]:
            print(f"  {old.name}")
            print(f"    → {new.name}")
        if len(renames) > 20:
            print(f"  ... and {len(renames) - 20} more")

        mapping = execute_renames(renames, dry_run)
        all_name_map.update(mapping)
        total_renames += len(renames)
        print()

    print(f"Total files to rename: {total_renames}")
    print(f"Unique filename mappings: {len(all_name_map)}\n")

    if not all_name_map:
        print("Nothing to update in metadata. Done.")
        return

    # -- Phase 2: update metadata CSVs ----------------------------------------
    print("--- Updating metadata CSVs ---")

    csv_updates = [
        ("download_status_api.csv",            ["filename"],     []),
        ("corrupt_pdfs_removed.csv",           ["file", "filename"], []),
        ("extra_docs.csv",                     ["File_Name"],    []),
        ("comment_letter_download_status.csv", ["filename"],     []),
        ("marker_conversion_status.csv",       [],               ["source_file", "output_file"]),
        ("marker_conversion_failures.csv",     [],               ["source_file"]),
        ("text_conversion_status.csv",         [],               ["source_file", "output_file"]),
        ("text_conversion_failures.csv",       [],               ["source_file"]),
    ]

    for csv_name, bare_cols, path_cols in csv_updates:
        csv_path = METADATA_DIR / csv_name
        n = update_csv(csv_path, bare_cols, path_cols, all_name_map, dry_run)
        if n:
            print(f"  {csv_name}: {n} cell(s) updated")
        else:
            print(f"  {csv_name}: no changes")

    # -- Phase 3: regenerate PKL files ----------------------------------------
    print("\n--- Regenerating PKL files ---")
    regenerate_pkls(dry_run)

    if dry_run:
        print("\n=== DRY RUN COMPLETE — no files were modified ===")
    else:
        print("\n=== DONE ===")


if __name__ == "__main__":
    main()
