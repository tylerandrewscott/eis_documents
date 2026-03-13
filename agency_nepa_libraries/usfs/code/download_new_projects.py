#!/usr/bin/env python3
"""
download_new_projects.py

Downloads documents for new USFS projects (in usfs_new_projects.csv) from
Box PinyonPublic using the Box API with OAuth2 credentials.

Setup (one-time):
  1. Go to https://developer.box.com → My Apps → Create New App
  2. Choose Custom App → User Authentication (OAuth 2.0)
  3. Under Configuration, set Redirect URI to: http://localhost
  4. Copy Client ID and Client Secret into usfs/box_credentials.json:
       {"client_id": "...", "client_secret": "..."}
  5. Run this script — it will open a browser for authorization the first time.

Usage:
    python3 download_new_projects.py              # download all new projects
    python3 download_new_projects.py --dry-run    # list files without downloading
    python3 download_new_projects.py --limit 50   # process first N projects

Safe to re-run: skips files already on disk; resumes interrupted downloads.

After running, run update_metadata.py to refresh the metadata CSVs.
"""

import json
import re
import sys
import time
import argparse
import webbrowser
import urllib.parse
from pathlib import Path

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
USFS_DIR      = Path(__file__).parent.parent
META_DIR      = USFS_DIR / "metadata"
DOCS_DIR      = USFS_DIR / "documents"
CREDS_FILE    = USFS_DIR / "box_credentials.json"
TOKEN_CACHE   = USFS_DIR / "box_token_cache.json"
NEW_PROJ_FILE = META_DIR / "usfs_new_projects.csv"

# ---------------------------------------------------------------------------
# Box API config
# ---------------------------------------------------------------------------
AUTH_URL  = "https://account.box.com/api/oauth2/authorize"
TOKEN_URL = "https://api.box.com/oauth2/token"
API_BASE  = "https://api.box.com/2.0"
SHARED_LINK = "https://usfs-public.app.box.com/v/PinyonPublic"

HEADERS_BASE = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
}

DOWNLOAD_DELAY = 0.5    # seconds between Box API calls
SCRAPE_DELAY   = 0.75   # seconds between USFS page requests
CHUNK_SIZE     = 256 * 1024  # 256 KB

# File extensions to download
DOWNLOAD_EXTS = {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt"}


# ---------------------------------------------------------------------------
# OAuth2 helpers
# ---------------------------------------------------------------------------
def load_credentials() -> tuple[str, str]:
    if not CREDS_FILE.exists():
        print(f"Credentials file not found: {CREDS_FILE}")
        print()
        print("Create it with:")
        print('  {"client_id": "YOUR_CLIENT_ID", "client_secret": "YOUR_CLIENT_SECRET"}')
        print()
        print("Get credentials at: https://developer.box.com → My Apps → Create New App")
        print("  App type: Custom App → User Authentication (OAuth 2.0)")
        print("  Redirect URI: http://localhost")
        sys.exit(1)
    with open(CREDS_FILE) as f:
        creds = json.load(f)
    return creds["client_id"], creds["client_secret"]


def save_tokens(tokens: dict) -> None:
    with open(TOKEN_CACHE, "w") as f:
        json.dump(tokens, f)


def refresh_token(client_id: str, client_secret: str, refresh: str) -> dict | None:
    r = requests.post(TOKEN_URL, data={
        "grant_type":    "refresh_token",
        "refresh_token": refresh,
        "client_id":     client_id,
        "client_secret": client_secret,
    }, timeout=15)
    if r.status_code == 200:
        tokens = r.json()
        save_tokens(tokens)
        return tokens
    return None


def authorize(client_id: str, client_secret: str) -> str:
    """Run OAuth2 authorization code flow. Returns access_token."""
    auth_url = f"{AUTH_URL}?" + urllib.parse.urlencode({
        "response_type": "code",
        "client_id":     client_id,
    })
    print(f"\nOpening browser for Box authorization...")
    print(f"If the browser doesn't open, visit:\n  {auth_url}\n")
    webbrowser.open(auth_url)
    print("After authorizing, you'll be redirected to http://localhost/?code=...")
    print("(The page may show an error — that's fine.)")
    redirect = input("Paste the full redirect URL here: ").strip()

    qs = urllib.parse.parse_qs(urllib.parse.urlparse(redirect).query)
    code = qs.get("code", [None])[0]
    if not code:
        print("No code found in URL. Exiting.")
        sys.exit(1)

    r = requests.post(TOKEN_URL, data={
        "grant_type":    "authorization_code",
        "code":          code,
        "client_id":     client_id,
        "client_secret": client_secret,
    }, timeout=15)
    r.raise_for_status()
    tokens = r.json()
    save_tokens(tokens)
    print("Authorization successful. Token cached.")
    return tokens["access_token"]


def get_access_token(client_id: str, client_secret: str) -> str:
    """Return a valid access token, refreshing or re-authorizing as needed."""
    if TOKEN_CACHE.exists():
        with open(TOKEN_CACHE) as f:
            tokens = json.load(f)
        if tokens.get("refresh_token"):
            refreshed = refresh_token(client_id, client_secret, tokens["refresh_token"])
            if refreshed:
                return refreshed["access_token"]
    return authorize(client_id, client_secret)


# ---------------------------------------------------------------------------
# Box API helpers
# ---------------------------------------------------------------------------
def box_headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "BoxApi":        f"shared_link={SHARED_LINK}",
    }


def list_folder(folder_id: str, token: str, timeout: int = 20) -> list[dict]:
    """
    Return all items in a Box folder (auto-paginates).
    Each item: {"id", "name", "type", "size"}
    """
    items = []
    url = f"{API_BASE}/folders/{folder_id}/items"
    params = {"fields": "id,name,type,size", "limit": 1000, "offset": 0}

    while True:
        time.sleep(DOWNLOAD_DELAY)
        r = requests.get(url, headers=box_headers(token), params=params, timeout=timeout)
        if r.status_code == 401:
            raise PermissionError("Box API returned 401 — token may have expired")
        if r.status_code != 200:
            raise RuntimeError(f"Box API {r.status_code}: {r.text[:200]}")
        data = r.json()
        items.extend(data.get("entries", []))
        total = data.get("total_count", 0)
        offset = data.get("offset", 0) + len(data.get("entries", []))
        if offset >= total:
            break
        params["offset"] = offset

    return items


def list_folder_recursive(folder_id: str, token: str, depth: int = 0) -> list[dict]:
    """Recursively list all files in a folder and its subfolders."""
    if depth > 5:
        return []
    items = list_folder(folder_id, token)
    files = []
    for item in items:
        if item["type"] == "file":
            files.append(item)
        elif item["type"] == "folder":
            files.extend(list_folder_recursive(item["id"], token, depth + 1))
    return files


def download_file(file_id: str, dest: Path, token: str, timeout: int = 60) -> tuple[bool, str]:
    """Download a Box file to dest. Returns (success, message)."""
    url = f"{API_BASE}/files/{file_id}/content"
    try:
        time.sleep(DOWNLOAD_DELAY)
        r = requests.get(url, headers=box_headers(token), stream=True, timeout=timeout)
        if r.status_code != 200:
            return False, f"HTTP {r.status_code}"
        with open(dest, "wb") as f:
            for chunk in r.iter_content(CHUNK_SIZE):
                if chunk:
                    f.write(chunk)
        size = dest.stat().st_size
        if size < 500:
            dest.unlink()
            return False, f"too small ({size} bytes)"
        return True, f"{size:,} bytes"
    except Exception as e:
        if dest.exists():
            dest.unlink()
        return False, str(e)


# ---------------------------------------------------------------------------
# USFS page scraping
# ---------------------------------------------------------------------------
def scrape_box_folder_id(project_url: str) -> str | None:
    """Scrape a USFS project page to extract the Box PinyonPublic folder ID."""
    try:
        time.sleep(SCRAPE_DELAY)
        r = requests.get(project_url, headers=HEADERS_BASE, timeout=15)
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            m = re.search(r"PinyonPublic/folder/(\d+)", href)
            if m:
                return m.group(1)
        return None
    except Exception:
        return None


def sanitize_filename(name: str) -> str:
    name = re.sub(r"[^\w\-. ]", "_", name)
    name = re.sub(r"_+", "_", name).strip("_. ")
    return name


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="List files only, no downloads")
    parser.add_argument("--limit",   type=int,            help="Max projects to process")
    args = parser.parse_args()

    # Load credentials and get token
    client_id, client_secret = load_credentials()
    if not args.dry_run:
        print("Authenticating with Box API...")
        token = get_access_token(client_id, client_secret)
        print("Authenticated.\n")
    else:
        token = None

    # Load project list
    if not NEW_PROJ_FILE.exists():
        print(f"Not found: {NEW_PROJ_FILE}")
        print("Run update_metadata.py first.")
        sys.exit(1)

    projects = pd.read_csv(NEW_PROJ_FILE, dtype=str)
    if "box_folder_id" not in projects.columns:
        projects["box_folder_id"] = None
    if "download_status" not in projects.columns:
        projects["download_status"] = "pending"

    # Skip already done
    todo = projects[~projects["download_status"].isin(["done", "skip"])].copy()
    if args.limit:
        todo = todo.head(args.limit)

    print(f"Projects to process: {len(todo):,}")
    print(f"Mode: {'DRY RUN' if args.dry_run else 'DOWNLOAD'}")
    print()

    DOCS_DIR.mkdir(parents=True, exist_ok=True)
    total_downloaded = 0
    total_skipped    = 0
    total_failed     = 0

    for i, (idx, row) in enumerate(todo.iterrows(), 1):
        pnum   = str(row["project_num"]).strip()
        title  = str(row.get("title", "")).strip()
        p_url  = str(row.get("project_url", "")).strip()
        folder = str(row.get("box_folder_id", "")).strip() if pd.notna(row.get("box_folder_id")) else ""

        print(f"[{i}/{len(todo)}] #{pnum} {title[:60]}")

        # Step 1: get Box folder ID if missing
        if not folder and p_url:
            print(f"  scraping project page...")
            folder = scrape_box_folder_id(p_url) or ""
            if folder:
                projects.at[idx, "box_folder_id"] = folder
                print(f"  folder: {folder}")
            else:
                print(f"  no Box folder found — skipping")
                projects.at[idx, "download_status"] = "skip"
                # Save checkpoint
                projects.to_csv(NEW_PROJ_FILE, index=False)
                continue

        # Step 2: determine year folder
        year = str(row.get("year", "")).strip()
        if not year or year == "nan":
            # Try to parse from project URL (project pages sometimes have a date)
            # Fall back to "new" subdirectory
            year = "new"
        year_dir = DOCS_DIR / year
        year_dir.mkdir(exist_ok=True)

        # Step 3: list Box folder files
        if args.dry_run:
            print(f"  [dry-run] would list Box folder {folder}")
            continue

        try:
            files = list_folder_recursive(folder, token)
        except PermissionError:
            print("  Token expired — re-authenticating...")
            token = get_access_token(client_id, client_secret)
            try:
                files = list_folder_recursive(folder, token)
            except Exception as e:
                print(f"  ERROR listing folder: {e}")
                projects.at[idx, "download_status"] = "failed"
                total_failed += 1
                continue
        except Exception as e:
            print(f"  ERROR listing folder: {e}")
            projects.at[idx, "download_status"] = "failed"
            total_failed += 1
            continue

        # Step 4: filter to downloadable file types
        downloadable = [
            f for f in files
            if Path(f["name"]).suffix.lower() in DOWNLOAD_EXTS
        ]
        print(f"  {len(files)} files in folder, {len(downloadable)} downloadable")

        project_downloaded = 0
        project_skipped    = 0
        project_failed     = 0

        for box_file in downloadable:
            safe_name = sanitize_filename(box_file["name"])
            dest_name = f"{pnum}_{safe_name}"
            dest = year_dir / dest_name

            if dest.exists():
                project_skipped += 1
                continue

            ok, msg = download_file(box_file["id"], dest, token)
            if ok:
                print(f"    + {dest_name} ({msg})")
                project_downloaded += 1
            else:
                print(f"    FAILED {safe_name}: {msg}")
                project_failed += 1

        total_downloaded += project_downloaded
        total_skipped    += project_skipped
        total_failed     += project_failed

        status = "done" if project_failed == 0 else "partial"
        projects.at[idx, "download_status"] = status
        print(f"  done: {project_downloaded} new, {project_skipped} skipped, {project_failed} failed")

        # Save checkpoint every 10 projects
        if i % 10 == 0:
            projects.to_csv(NEW_PROJ_FILE, index=False)

    # Final save
    projects.to_csv(NEW_PROJ_FILE, index=False)

    print(f"\n{'='*50}")
    print(f"Total downloaded: {total_downloaded:,}")
    print(f"Total skipped:    {total_skipped:,}")
    print(f"Total failed:     {total_failed:,}")
    print(f"\nRun update_metadata.py to refresh metadata CSVs.")


if __name__ == "__main__":
    main()
