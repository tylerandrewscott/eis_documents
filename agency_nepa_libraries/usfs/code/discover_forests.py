#!/usr/bin/env python3
"""
discover_forests.py

Scrapes each USFS region's forests-grasslands page to build a complete
mapping of forest short-name → (region_code, url_slug) combinations.

Output: usfs/metadata/forest_url_map.csv
  Columns: region, forest_slug, forest_name, projects_url
"""

import time
from pathlib import Path

import requests
import pandas as pd
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE = "https://www.fs.usda.gov"
REGIONS = ["r01", "r02", "r03", "r04", "r05", "r06", "r08", "r09", "r10"]
META_DIR = Path(__file__).parent.parent / "metadata"
OUT_FILE = META_DIR / "forest_url_map.csv"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}
DELAY = 1.0


# Keywords in forest names that confirm a link points to an actual forest/grassland unit
_FOREST_KEYWORDS = (
    "national forest", "national grassland", "national prairie",
    "national scenic area", "tallgrass prairie", "management unit",
    "national forests", "national grasslands",
    "savannah river",  # edge case without "national"
)

_NAV_SLUGS = {
    "alerts", "events", "forests-grasslands", "passes", "permits",
    "recreation", "maps-guides", "animals-plants", "climate", "fire",
    "forest-products", "natural-resources", "planning", "state-private-tribal",
    "wilderness", "data-tools", "educational", "multimedia", "publications",
    "contact-us", "about-area", "leadership", "newsroom", "offices",
    "working-with-us", "signup",
}


def _is_forest_link(slug: str, name: str) -> bool:
    if slug in _NAV_SLUGS:
        return False
    name_lower = name.lower()
    return any(kw in name_lower for kw in _FOREST_KEYWORDS)


def get_forests_for_region(region: str) -> list[dict]:
    """Scrape a region's forests-grasslands page for forest links."""
    url = f"{BASE}/{region}/forests-grasslands"
    try:
        time.sleep(DELAY)
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print(f"  [{region}] forests-grasslands → {r.status_code}")
            return []
        soup = BeautifulSoup(r.text, "html.parser")
        forests = []
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            name = a.text.strip()
            # Forest links look like /r04/payette — exactly two path segments
            parts = href.strip("/").split("/")
            if len(parts) != 2 or parts[0] != region:
                continue
            slug = parts[1]
            if not _is_forest_link(slug, name):
                continue
            forests.append({
                "region":       region,
                "forest_slug":  slug,
                "forest_name":  name,
                "projects_url": f"{BASE}/{region}/{slug}/projects",
            })
        # Deduplicate by slug
        seen = set()
        unique = []
        for f in forests:
            if f["forest_slug"] not in seen:
                seen.add(f["forest_slug"])
                unique.append(f)
        return unique
    except Exception as e:
        print(f"  [{region}] ERROR: {e}")
        return []


def main():
    META_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    for region in REGIONS:
        forests = get_forests_for_region(region)
        print(f"{region}: {len(forests)} forests")
        for f in forests:
            print(f"  {f['forest_slug']}: {f['forest_name']}")
        rows.extend(forests)

    df = pd.DataFrame(rows)
    df.to_csv(OUT_FILE, index=False)
    print(f"\nWrote {len(df)} forests → {OUT_FILE}")


if __name__ == "__main__":
    main()
