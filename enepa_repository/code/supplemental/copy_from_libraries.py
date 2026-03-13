#!/usr/bin/env python3
"""
copy_from_libraries.py

Copies EIS PDFs from local agency libraries into supplemental_eis_documents/
for all rows in supplemental_download_queue.csv with status=found_library.

Usage:
    python3 copy_from_libraries.py           # dry run (no files changed)
    python3 copy_from_libraries.py --copy    # execute copies and update queue

After copying, rows are marked downloaded and the queue is saved.
"""

import sys
import shutil
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
EIS_DOCS = Path("/Users/tscott1/Documents/GitHub/eis_documents")
ENEPA    = EIS_DOCS / "enepa_repository"
PDF_DIR  = ENEPA / "box_files" / "supplemental_eis_documents"
QUEUE_FILE = ENEPA / "metadata" / "supplemental_download_queue.csv"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    do_copy = "--copy" in sys.argv

    if not QUEUE_FILE.exists():
        print(f"Queue file not found: {QUEUE_FILE}")
        print("Run build_queue.py first.")
        return

    queue   = pd.read_csv(QUEUE_FILE, dtype=str).fillna("")
    to_copy = queue[queue["status"] == "found_library"].copy()

    print(f"MODE: {'COPY' if do_copy else 'DRY RUN (pass --copy to execute)'}")
    print(f"Rows to copy   : {len(to_copy)}")
    print(f"Already done   : {(queue['status'] == 'downloaded').sum()}")
    print(f"Pending (no URL): {(queue['status'] == 'pending').sum()}")
    print()

    plan = []
    for idx, row in to_copy.iterrows():
        ceq     = row["ceqNumber"]
        src_url = row["source_url"]
        dest_fn = row["local_filename"]

        if not src_url.startswith("local:"):
            print(f"[{ceq}] WARNING: source_url is not a local path, skipping: {src_url}")
            continue

        src  = Path(src_url[len("local:"):])
        dest = PDF_DIR / dest_fn

        plan.append({
            "idx":      idx,
            "ceq":      ceq,
            "src":      src,
            "dest":     dest,
            "exists":   dest.exists(),
            "src_ok":   src.exists(),
            "notes":    row["notes"],
        })

    # Print plan summary grouped by ceq
    by_ceq = {}
    for p in plan:
        by_ceq.setdefault(p["ceq"], []).append(p)

    for ceq, entries in by_ceq.items():
        new_count   = sum(1 for e in entries if not e["exists"] and e["src_ok"])
        exist_count = sum(1 for e in entries if e["exists"])
        miss_count  = sum(1 for e in entries if not e["src_ok"])
        print(f"  [{ceq}]  {new_count} to copy  "
              f"({exist_count} already exist, {miss_count} source missing)")
        for e in entries:
            if not e["exists"] and e["src_ok"]:
                print(f"    {e['src'].name}")
                print(f"      → {e['dest'].name}")
            elif not e["src_ok"]:
                print(f"    MISSING SOURCE: {e['src']}")

    new_total = sum(1 for p in plan if not p["exists"] and p["src_ok"])
    print(f"\nTotal new files to copy: {new_total}")

    if not do_copy:
        print("\nDry run complete. Run with --copy to execute.")
        return

    # Execute
    PDF_DIR.mkdir(parents=True, exist_ok=True)
    copied, failed = 0, 0

    for p in plan:
        idx = p["idx"]
        if not p["src_ok"]:
            print(f"[{p['ceq']}] SKIP — source not found: {p['src']}")
            queue.at[idx, "status"] = "failed"
            queue.at[idx, "notes"] = (queue.at[idx, "notes"] or "") + " | source file not found"
            failed += 1
            continue

        if not p["exists"]:
            shutil.copy2(p["src"], p["dest"])
            print(f"[{p['ceq']}] copied: {p['dest'].name}")

        queue.at[idx, "status"] = "downloaded"
        copied += 1

    queue.to_csv(QUEUE_FILE, index=False)
    print(f"\nCopied: {copied}  Failed: {failed}")
    print("Queue updated.")
    print("\nStatus summary:")
    print(queue["status"].value_counts().to_string())


if __name__ == "__main__":
    main()
