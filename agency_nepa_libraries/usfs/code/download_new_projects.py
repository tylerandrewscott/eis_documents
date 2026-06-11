#!/usr/bin/env python3
"""
download_new_projects.py

Downloads documents for new USFS projects (in usfs_new_projects.csv) from
Box PinyonPublic using Playwright to generate signed download URLs.

Strategy:
  For each project, navigate to each subfolder's Box page, click the
  "Download" button to generate a signed boxcloud.com ZIP URL, intercept
  it before the browser downloads, then download the ZIP ourselves and
  extract the files.

Usage:
    python3 download_new_projects.py              # download all new projects
    python3 download_new_projects.py --dry-run    # list folders without downloading
    python3 download_new_projects.py --limit 10   # process first N projects

Safe to re-run: skips projects already marked done in usfs_new_projects.csv.

After running, run update_metadata.py to refresh the metadata CSVs.
"""

import re
import sys
import time
import zipfile
import argparse
import tempfile
import shutil
from pathlib import Path

import requests
import pandas as pd
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
USFS_DIR      = Path(__file__).parent.parent
META_DIR      = USFS_DIR / "metadata"
DOCS_DIR      = USFS_DIR / "documents"
NEW_PROJ_FILE = META_DIR / "usfs_new_projects.csv"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
SCRAPE_DELAY   = 0.75
DOWNLOAD_DELAY = 0.5
CHUNK_SIZE     = 256 * 1024

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
}

BOX_BASE = "https://usfs-public.app.box.com"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def sanitize(name: str) -> str:
    name = re.sub(r"[^\w\-. ]", "_", name)
    return re.sub(r"_+", "_", name).strip("_. ")


def scrape_box_info(project_url: str) -> dict | None:
    """
    Scrape USFS project page to extract Box folder info.
    Returns {"folder_id": "...", "shared_token": "...", "folder_url": "..."}
    or None if not found.
    """
    try:
        time.sleep(SCRAPE_DELAY)
        r = requests.get(project_url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")

        # Look for: embed iframe src (has /s/{token}) and folder link (has /folder/{id})
        shared_token = None
        folder_id    = None

        # Embed iframe: /embed/s/{token}
        for iframe in soup.find_all("iframe", src=True):
            m = re.search(r"box\.com/embed/s/([a-z0-9]+)", iframe.get("src", ""))
            if m:
                shared_token = m.group(1)

        # Pinyon folder link: /PinyonPublic/folder/{id}
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            m = re.search(r"PinyonPublic/folder/(\d+)", href)
            if m:
                folder_id = m.group(1)

        if not folder_id:
            return None

        if shared_token:
            folder_url = f"{BOX_BASE}/s/{shared_token}/folder/{folder_id}"
        else:
            folder_url = f"{BOX_BASE}/v/PinyonPublic/folder/{folder_id}"

        return {"folder_id": folder_id, "shared_token": shared_token or "", "folder_url": folder_url}
    except Exception:
        return None


def get_subfolders(page, folder_url: str) -> list[dict]:
    """
    Navigate to a Box shared folder page and return list of subfolders.
    Returns [{"name": ..., "id": ..., "url": ...}]
    """
    page.goto(folder_url, wait_until="networkidle", timeout=30000)
    time.sleep(2)

    subfolders = []
    # Box renders folder names as links in the list view
    body = page.inner_text("body")
    links = page.query_selector_all("a[href]")
    for a in links:
        href = a.get_attribute("href") or ""
        m = re.search(r"/s/[a-z0-9]+/folder/(\d+)$", href)
        if m:
            name = a.inner_text().strip()
            folder_id = m.group(1)
            subfolders.append({"name": name, "id": folder_id, "url": f"{BOX_BASE}{href}"})

    return subfolders


def list_files_in_folder(page, folder_url: str, folder_id: str) -> list[str]:
    """Return list of filenames visible in a Box folder listing."""
    page.goto(folder_url, wait_until="networkidle", timeout=30000)
    time.sleep(2)
    body = page.inner_text("body")
    files = [l.strip() for l in body.split("\n")
             if l.strip() and "." in l and
             any(l.strip().lower().endswith(ext) for ext in
                 [".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt"])]
    return files


def download_folder_as_zip(page, folder_url: str, folder_id: str) -> tuple[bool, str]:
    """
    Navigate to a Box folder, click Download, intercept the boxcloud URL,
    then download the ZIP with requests.

    Returns (success: bool, zip_path_or_error: str)
    """
    captured_url: list = []

    def intercept(route):
        captured_url.append(route.request.url)
        route.abort()

    page.route("**/dl.boxcloud.com/**", intercept)

    try:
        page.goto(folder_url, wait_until="networkidle", timeout=30000)
        time.sleep(2)

        # Hover over first file to reveal Download button
        body = page.inner_text("body")
        pdf_lines = [l.strip() for l in body.split("\n")
                     if l.strip() and any(l.strip().lower().endswith(e)
                                          for e in [".pdf", ".doc", ".docx", ".xls"])]

        if pdf_lines:
            try:
                page.get_by_text(pdf_lines[0], exact=True).first.hover()
                time.sleep(0.3)
            except Exception:
                pass

        # Click the Download button
        dl_btn = page.get_by_role("button", name="Download", exact=True).first
        dl_btn.click()
        page.wait_for_timeout(3000)

    except PWTimeout:
        return False, f"timeout navigating to {folder_url}"
    except Exception as e:
        return False, f"playwright error: {e}"
    finally:
        page.unroute("**/dl.boxcloud.com/**")

    if not captured_url:
        return False, "no download URL captured (folder may be empty or have no Download button)"

    # Download the ZIP
    url = captured_url[0]
    try:
        time.sleep(DOWNLOAD_DELAY)
        r = requests.get(url, headers=HEADERS, stream=True, timeout=120)
        if r.status_code != 200:
            return False, f"HTTP {r.status_code}: {r.text[:100]}"
        tmp = tempfile.mktemp(suffix=".zip")
        with open(tmp, "wb") as f:
            for chunk in r.iter_content(CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
        return True, tmp
    except Exception as e:
        return False, str(e)


def extract_zip_to_dir(zip_path: str, dest_dir: Path, project_num: str) -> tuple[int, int]:
    """
    Extract a Box ZIP to dest_dir, prefixing filenames with {project_num}_.
    Returns (extracted_count, skipped_count).
    """
    extracted = 0
    skipped = 0
    with zipfile.ZipFile(zip_path) as z:
        for name in z.namelist():
            # Box ZIPs use folder/filename structure
            basename = Path(name).name
            if not basename or not any(basename.lower().endswith(e)
                                       for e in [".pdf", ".doc", ".docx", ".xls",
                                                 ".xlsx", ".ppt", ".pptx", ".txt"]):
                continue
            dest_name = f"{project_num}_{sanitize(basename)}"
            dest = dest_dir / dest_name
            if dest.exists():
                skipped += 1
                continue
            with z.open(name) as src, open(dest, "wb") as dst:
                shutil.copyfileobj(src, dst)
            extracted += 1
    return extracted, skipped


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="List folders only")
    parser.add_argument("--limit",   type=int,            help="Max projects to process")
    args = parser.parse_args()

    if not NEW_PROJ_FILE.exists():
        print(f"Not found: {NEW_PROJ_FILE}")
        print("Run update_metadata.py first.")
        sys.exit(1)

    projects = pd.read_csv(NEW_PROJ_FILE, dtype=str)
    if "box_folder_id"   not in projects.columns: projects["box_folder_id"]   = ""
    if "box_folder_url"  not in projects.columns: projects["box_folder_url"]  = ""
    if "download_status" not in projects.columns: projects["download_status"] = "pending"

    todo = projects[~projects["download_status"].isin(["done", "skip"])].copy()
    if args.limit:
        todo = todo.head(args.limit)

    print(f"Projects to process: {len(todo):,}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'DOWNLOAD'}\n")

    DOCS_DIR.mkdir(parents=True, exist_ok=True)

    total_extracted = total_skipped = total_failed = 0

    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context()
        page    = context.new_page()

        for i, (idx, row) in enumerate(todo.iterrows(), 1):
            pnum   = str(row["project_num"]).strip()
            title  = str(row.get("title", "")).strip()
            p_url  = str(row.get("project_url", "")).strip()

            # Treat pandas NaN / literal "nan" as empty
            def _val(col):
                v = row.get(col, "")
                return "" if (pd.isna(v) or str(v).strip().lower() == "nan") else str(v).strip()

            folder     = _val("box_folder_id")
            folder_url = _val("box_folder_url")

            print(f"[{i}/{len(todo)}] #{pnum} {title[:60]}")

            # Step 1: scrape project page if we don't have a valid folder URL yet
            if (not folder or not folder_url) and p_url:
                info = scrape_box_info(p_url)
                if info:
                    folder     = info["folder_id"]
                    folder_url = info["folder_url"]
                    projects.at[idx, "box_folder_id"]  = folder
                    projects.at[idx, "box_folder_url"] = folder_url
                    print(f"  folder URL: {folder_url}")
                else:
                    print(f"  no Box folder found — skipping")
                    projects.at[idx, "download_status"] = "skip"
                    projects.to_csv(NEW_PROJ_FILE, index=False)
                    continue

            if not folder_url:
                print(f"  no folder URL — skipping")
                projects.at[idx, "download_status"] = "skip"
                continue

            # Destination directory
            year_dir = DOCS_DIR / "new"
            year_dir.mkdir(exist_ok=True)

            if args.dry_run:
                print(f"  [dry-run] would download from {folder_url}")
                continue

            # Step 2: find all subfolders + root folder to download
            print(f"  listing subfolders...")
            try:
                page.goto(folder_url, wait_until="networkidle", timeout=30000)
                time.sleep(2)
                subfolders = get_subfolders(page, folder_url)
                print(f"  {len(subfolders)} subfolders")
            except Exception as e:
                print(f"  ERROR listing subfolders: {e}")
                projects.at[idx, "download_status"] = "failed"
                total_failed += 1
                continue

            # Download each subfolder (and the root folder itself)
            folders_to_download = [{"name": "root", "id": folder, "url": folder_url}] + subfolders
            proj_extracted = proj_skipped = proj_failed = 0

            for sf in folders_to_download:
                sf_url = sf["url"]
                print(f"  downloading folder: {sf['name']}")
                ok, result = download_folder_as_zip(page, sf_url, sf["id"])
                if ok:
                    extracted, skipped = extract_zip_to_dir(result, year_dir, pnum)
                    import os
                    os.unlink(result)
                    print(f"    extracted {extracted}, skipped {skipped}")
                    proj_extracted += extracted
                    proj_skipped   += skipped
                else:
                    print(f"    FAILED: {result}")
                    proj_failed += 1

            total_extracted += proj_extracted
            total_skipped   += proj_skipped
            total_failed    += proj_failed

            status = "done" if proj_failed == 0 else ("partial" if proj_extracted > 0 else "failed")
            projects.at[idx, "download_status"] = status
            print(f"  → {proj_extracted} new, {proj_skipped} skipped, {proj_failed} failed subfolder(s)")

            if i % 10 == 0:
                projects.to_csv(NEW_PROJ_FILE, index=False)

        browser.close()

    projects.to_csv(NEW_PROJ_FILE, index=False)

    print(f"\n{'='*50}")
    print(f"Files extracted: {total_extracted:,}")
    print(f"Files skipped:   {total_skipped:,}")
    print(f"Folders failed:  {total_failed:,}")
    print(f"\nRun update_metadata.py to refresh metadata CSVs.")


if __name__ == "__main__":
    main()
