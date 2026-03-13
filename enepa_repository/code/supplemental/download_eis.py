#!/usr/bin/env python3
"""
download_eis.py

Downloads PDFs for rows in supplemental_download_queue.csv with
status=found_ia, then rebuilds supplemental_eis_metadata.parquet.

Safe to re-run: skips rows already marked downloaded, failed, or skip.
"""

import re
import time
from pathlib import Path

import requests
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
EIS_DOCS = Path("/Users/tscott1/Documents/GitHub/eis_documents")
ENEPA    = EIS_DOCS / "enepa_repository"
PDF_DIR  = ENEPA / "box_files" / "supplemental_eis_documents"
QUEUE_FILE   = ENEPA / "metadata" / "supplemental_download_queue.csv"
META_OUT     = ENEPA / "metadata" / "supplemental_eis_metadata.parquet"
META_PARQUET = ENEPA / "metadata" / "eis_record_api.parquet"

HEADERS = {"User-Agent": "Mozilla/5.0 (research bot; contact: academic use)"}
DELAY   = 1.5
CHUNK   = 1024 * 256   # 256 KB


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def sanitize(name: str) -> str:
    name = re.sub(r"[^\w\-.]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def download_pdf(url: str, dest: Path) -> tuple:
    """Download url to dest. Returns (success: bool, message: str)."""
    try:
        time.sleep(DELAY)
        r = requests.get(url, headers=HEADERS, stream=True, timeout=60)
        if r.status_code != 200:
            return False, f"HTTP {r.status_code}"
        with open(dest, "wb") as f:
            for chunk in r.iter_content(CHUNK):
                if chunk:
                    f.write(chunk)
        size = dest.stat().st_size
        if size < 1000:
            dest.unlink()
            return False, f"file too small ({size} bytes) — likely an error page"
        return True, f"{size:,} bytes"
    except Exception as e:
        if dest.exists():
            dest.unlink()
        return False, str(e)


def build_metadata(queue: pd.DataFrame) -> pd.DataFrame:
    """Build supplemental_eis_metadata.parquet from all downloaded rows."""
    eis_meta   = pd.read_parquet(META_PARQUET)[["ceqNumber", "eisId"]].drop_duplicates("ceqNumber")
    ceq_to_eis = dict(zip(eis_meta["ceqNumber"], eis_meta["eisId"]))

    downloaded = queue[queue["status"] == "downloaded"].copy()
    rows = []
    for _, r in downloaded.iterrows():
        fpath = PDF_DIR / r["local_filename"]
        if not fpath.exists():
            continue
        size_bytes = fpath.stat().st_size
        # Derive a rough title from the filename (strip ceqNumber prefix and extension)
        raw_title  = r["local_filename"][len(r["ceqNumber"]) + 1:-4].replace("_", " ")
        rows.append({
            "eisId":              ceq_to_eis.get(r["ceqNumber"]),
            "ceqNumber":          r["ceqNumber"],
            "attachmentId":       None,
            "name":               r["local_filename"],
            "title":              raw_title,
            "fileNameForDownload": r["local_filename"],
            "type":               "EIS_Document",
            "size":               size_bytes,
            "sizeKb":             str(round(size_bytes / 1024)),
            "pages":              None,
            "source_url":         r.get("source_url", ""),
            "notes":              r.get("notes", ""),
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    if not QUEUE_FILE.exists():
        print(f"Queue file not found: {QUEUE_FILE}")
        print("Run build_queue.py first.")
        return

    queue       = pd.read_csv(QUEUE_FILE, dtype=str).fillna("")
    to_download = queue[
        (queue["status"] == "found_ia") & (queue["source_url"] != "")
    ].copy()

    print(f"Rows to download : {len(to_download)}")
    print(f"Already downloaded: {(queue['status'] == 'downloaded').sum()}")
    print(f"Pending (no URL) : {(queue['status'] == 'pending').sum()}")
    print()

    for idx, row in to_download.iterrows():
        ceq   = row["ceqNumber"]
        url   = row["source_url"]
        fname = row["local_filename"]

        if not fname:
            raw = url.rstrip("/").split("/")[-1].split("?")[0]
            if not raw.lower().endswith(".pdf"):
                raw += ".pdf"
            fname = f"{ceq}_{sanitize(raw[:-4])}.pdf"
            queue.at[idx, "local_filename"] = fname

        dest = PDF_DIR / fname
        if dest.exists():
            print(f"[{ceq}] already on disk: {fname}")
            queue.at[idx, "status"] = "downloaded"
            continue

        print(f"[{ceq}] {url}")
        ok, msg = download_pdf(url, dest)
        if ok:
            print(f"  OK — {msg}")
            queue.at[idx, "status"] = "downloaded"
        else:
            print(f"  FAILED — {msg}")
            queue.at[idx, "status"] = "failed"
            queue.at[idx, "notes"] = (queue.at[idx, "notes"] or "") + f" | error: {msg}"

    queue.to_csv(QUEUE_FILE, index=False)
    print(f"\nQueue updated: {QUEUE_FILE}")

    meta_df = build_metadata(queue)
    if not meta_df.empty:
        meta_df.to_parquet(META_OUT, index=False)
        print(f"Metadata written: {len(meta_df)} records → {META_OUT}")
    else:
        print("No downloaded files yet — metadata not written.")

    print("\nStatus summary:")
    print(queue["status"].value_counts().to_string())


if __name__ == "__main__":
    main()
