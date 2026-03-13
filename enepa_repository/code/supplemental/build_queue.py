#!/usr/bin/env python3
"""
build_queue.py

Builds supplemental_download_queue.csv for EIS records that have no
EIS_Document entry in the EPA NEPA API parquet.

Discovery waterfall per ceqNumber:
  1. Salinas carry-over  (preserves terminal states from the prior project)
  2. DOE library match   (via uniqueIdentificationNumber DOE/EIS-XXXX)
  3. BLM library match   (fuzzy title against ePlanning EIS projects)
  4. USFS library match  (fuzzy title against FS Analysis-stage EIS docs)
  5. Internet Archive search
  6. status=pending

Usage:
    python3 build_queue.py --years 2010 2011 2012

Re-run safe: rows in terminal states (downloaded, failed, skip) are preserved
unchanged. Low-confidence library matches are written to
supplemental_match_review.csv for manual inspection; those rows are queued
as found_library but flagged in notes.

Draft/final separation: files are filtered to match each ceqNumber's eis_type.
A file is only included if it is unambiguously the correct type OR cannot be
classified (ambiguous filenames pass through conservatively).
"""

import argparse
import re
import shutil
import time
import warnings
from pathlib import Path

import requests
import pandas as pd

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
EIS_DOCS = Path("/Users/tscott1/Documents/GitHub/eis_documents")
SALINAS  = Path("/Users/tscott1/Documents/GitHub/salinas/salinasbox")

ENEPA        = EIS_DOCS / "enepa_repository"
PDF_DIR      = ENEPA / "box_files" / "supplemental_eis_documents"
QUEUE_FILE   = ENEPA / "metadata" / "supplemental_download_queue.csv"
REVIEW_FILE  = ENEPA / "metadata" / "supplemental_match_review.csv"
DOCS_PARQUET = ENEPA / "metadata" / "eis_document_record_api.parquet"
META_PARQUET = ENEPA / "metadata" / "eis_record_api.parquet"

SALINAS_QUEUE = SALINAS / "supplemental_eis_documents" / "download_queue.csv"
SALINAS_PDFS  = SALINAS / "supplemental_eis_documents" / "pdfs"

BLM_DOC_CSV      = EIS_DOCS / "agency_nepa_libraries/blm/metadata/document_record.csv"
BLM_NEW_PROJ_CSV = EIS_DOCS / "agency_nepa_libraries/blm/metadata/new_project_record.csv"
BLM_DOCS_DIR     = EIS_DOCS / "agency_nepa_libraries/blm/nepa_documents"

USFS_DOC_CSV  = EIS_DOCS / "agency_nepa_libraries/usfs/metadata/forest_service_document_record.csv"
USFS_DOCS_DIR = EIS_DOCS / "agency_nepa_libraries/usfs/documents"

DOE_DOCS_DIR = EIS_DOCS / "agency_nepa_libraries/doe/documents"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
HEADERS          = {"User-Agent": "Mozilla/5.0 (research bot; contact: academic use)"}
IA_DELAY         = 1.0
MATCH_THRESHOLD  = 0.35   # below this → pending (no library match)
REVIEW_THRESHOLD = 0.55   # between MATCH_THRESHOLD and this → also written to review CSV

TERMINAL_STATES = {"downloaded", "failed", "skip"}

# Agencies with records likely on Internet Archive
IA_AGENCIES = {
    "Bureau of Land Management", "Forest Service", "U.S. Army Corps of Engineers",
    "Federal Highway Administration", "National Park Service", "Bureau of Reclamation",
    "Department of Energy", "Bonneville Power Administration",
    "Western Area Power Administration", "Bureau of Indian Affairs",
    "Rural Utilities Service", "Minerals Management Service",
    "Federal Energy Regulatory Commission", "Bureau of Ocean Energy Management",
}

QUEUE_COLS = [
    "ceqNumber", "eis_type", "lead_agency", "project_title", "eisId",
    "source_url", "local_filename", "status", "notes",
]

# ---------------------------------------------------------------------------
# Title normalization & Jaccard similarity
# ---------------------------------------------------------------------------
_STOP_WORDS = {
    "the", "a", "an", "of", "for", "and", "or", "in", "on", "at", "to",
    "by", "with", "from", "its", "be", "is", "are", "was", "were",
    "environmental", "impact", "statement", "draft", "final",
    "supplemental", "supplement", "revised", "second", "third",
    "eis", "deis", "feis", "nepa", "programmatic", "peis",
}


def normalize_title(s: str) -> set:
    s = re.sub(r"[^\w\s]", " ", s.lower())
    return {w for w in s.split() if w not in _STOP_WORDS and len(w) > 2}


def title_similarity(a: str, b: str) -> float:
    ta, tb = normalize_title(a), normalize_title(b)
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


# ---------------------------------------------------------------------------
# Draft / final classification
# ---------------------------------------------------------------------------
_DRAFT_STARTS = ("draft", "revised draft", "second draft", "third draft")
_FINAL_STARTS = ("final", "revised final", "second final", "third final")
_DRAFT_CODES  = {"ld", "rd", "d2", "dd", "dc", "de", "df"}
_FINAL_CODES  = {"lf", "rf", "f2", "fd", "fc", "fe", "nf", "f3"}


def eis_type_to_df(eis_type: str) -> str:
    """Map a CEQ eis_type string to 'draft', 'final', or 'unknown'."""
    t = eis_type.strip().lower()
    if any(t.startswith(x) for x in _DRAFT_STARTS):
        return "draft"
    if any(t.startswith(x) for x in _FINAL_STARTS):
        return "final"
    if t in _DRAFT_CODES:
        return "draft"
    if t in _FINAL_CODES:
        return "final"
    return "unknown"


def classify_draft_final(text: str) -> str | None:
    """Return 'draft', 'final', or None (ambiguous) from a filename or doc name."""
    t = text.lower().replace("-", " ").replace("_", " ")
    has_draft = any(x in t for x in ("deis", "draft eis", "draft environmental", "draft supplement"))
    has_final = any(x in t for x in ("feis", "final eis", "final environmental", "final supplement",
                                      "rod ", " rod.", "_rod."))
    if has_draft and not has_final:
        return "draft"
    if has_final and not has_draft:
        return "final"
    return None


def passes_df_filter(text: str, target_df: str) -> bool:
    """True if text's classification matches target_df. Ambiguous files pass."""
    if target_df == "unknown":
        return True
    c = classify_draft_final(text)
    return c is None or c == target_df


# ---------------------------------------------------------------------------
# Filename helpers
# ---------------------------------------------------------------------------
def sanitize(name: str) -> str:
    name = re.sub(r"[^\w\-.]", "_", name)
    name = re.sub(r"_+", "_", name)
    return name.strip("_")


def make_local_filename(ceq: str, source_name: str) -> str:
    base = source_name if source_name.lower().endswith(".pdf") else source_name + ".pdf"
    return f"{ceq}_{sanitize(base[:-4])}.pdf"


# ---------------------------------------------------------------------------
# Placeholder: detect volume gaps
# ---------------------------------------------------------------------------
def missing_volume_note(filenames: list) -> str | None:
    """Return a note if volume numbers in found filenames suggest a gap."""
    vols = []
    for fn in filenames:
        m = re.search(r"vol(?:ume)?[\s_\-]?(\d+)", fn.lower())
        if m:
            vols.append(int(m.group(1)))
    if len(set(vols)) < 2:
        return None
    lo, hi = min(vols), max(vols)
    missing = [v for v in range(lo, hi + 1) if v not in set(vols)]
    return f"volume gap: missing vol(s) {missing}" if missing else None


# ---------------------------------------------------------------------------
# Target identification
# ---------------------------------------------------------------------------
def get_targets(years: list) -> pd.DataFrame:
    meta = pd.read_parquet(META_PARQUET)
    docs = pd.read_parquet(DOCS_PARQUET)
    ceqs_with_eis = set(docs.loc[docs["type"] == "EIS_Document", "ceqNumber"])

    targets = meta[
        meta["ceqNumber"].str[:4].isin(years)
        & ~meta["ceqNumber"].isin(ceqs_with_eis)
        & (meta["status"] != "Withdrawn")
    ].drop_duplicates("ceqNumber").copy()

    return targets[[
        "ceqNumber", "eisId", "type", "title", "leadAgency", "uniqueIdentificationNumber"
    ]].rename(columns={
        "type": "eis_type",
        "title": "project_title",
        "leadAgency": "lead_agency",
        "uniqueIdentificationNumber": "unique_id",
    })


# ---------------------------------------------------------------------------
# Salinas carry-over
# ---------------------------------------------------------------------------
def load_salinas_carryover(meta: pd.DataFrame) -> dict:
    """Load salinas queue. Returns {ceqNumber: [row_dict, ...]}."""
    if not SALINAS_QUEUE.exists():
        return {}
    sal = pd.read_csv(SALINAS_QUEUE, dtype=str).fillna("")
    meta_idx = meta.set_index("ceqNumber")

    carryover = {}
    for _, r in sal.iterrows():
        ceq = r["ceqNumber"]
        raw = meta_idx.loc[[ceq]] if ceq in meta_idx.index else None
        m   = raw.iloc[0] if raw is not None and len(raw) > 0 else None
        row = {
            "ceqNumber":      ceq,
            "eis_type":       r.get("eis_type", ""),
            "lead_agency":    m["leadAgency"] if m is not None else "",
            "project_title":  r.get("project_title", ""),
            "eisId":          str(int(m["eisId"])) if m is not None and pd.notna(m["eisId"]) else "",
            "source_url":     r.get("source_url", ""),
            "local_filename": r.get("local_filename", ""),
            "status":         r.get("status", "pending"),
            "notes":          (r.get("notes", "") + " | migrated from salinas").strip(" |"),
        }
        carryover.setdefault(ceq, []).append(row)
    return carryover


# ---------------------------------------------------------------------------
# DOE library index
# ---------------------------------------------------------------------------
_DOE_SKIP = ("comment", "-noa", "_noa", "epanoa", "webpage", "non-technical", "nontechnical")


def build_doe_index() -> dict:
    """Map 'EIS-XXXX' → list of (filename, Path)."""
    index = {}
    for yr_dir in sorted(DOE_DOCS_DIR.iterdir()):
        if not yr_dir.is_dir():
            continue
        for f in yr_dir.iterdir():
            if not f.is_file():
                continue
            m = re.match(r"^(EIS-\d+)--(.+)$", f.name, re.IGNORECASE)
            if m:
                index.setdefault(m.group(1).upper(), []).append((f.name, f))
    return index


def match_doe(unique_id: str | None, eis_type: str, ceq: str, doe_index: dict) -> list:
    """Match via uniqueIdentificationNumber. Returns list of entry dicts or []."""
    if not unique_id:
        return []
    m = re.search(r"(EIS-\d+)", str(unique_id), re.IGNORECASE)
    if not m:
        return []
    eis_num = m.group(1).upper()
    all_files = doe_index.get(eis_num, [])
    if not all_files:
        return []

    target_df = eis_type_to_df(eis_type)
    body = [
        (fn, p) for fn, p in all_files
        if not any(t in fn.lower() for t in _DOE_SKIP)
        and passes_df_filter(fn, target_df)
    ]
    return [
        {
            "source_url":     f"local:{p}",
            "local_filename": make_local_filename(ceq, fn),
            "status":         "found_library",
            "notes":          f"DOE library: {eis_num}",
        }
        for fn, p in body
    ]


# ---------------------------------------------------------------------------
# BLM library index
# ---------------------------------------------------------------------------
_BLM_EXCLUDE = (
    "notice of intent", "noi", "fact sheet", "biological opinion",
    "memorandum", "scoping", "figures", "fonsi", "ea_508",
    "right_of_way", "right-of-way", "dear_reader",
)


def _blm_is_eis_body(doc_name: str, file_name: str) -> bool:
    combined = (doc_name + " " + file_name).lower().replace("-", " ").replace("_", " ")
    return not any(ex in combined for ex in _BLM_EXCLUDE)


def build_blm_index() -> tuple:
    """
    Returns:
      proj_list : [{nepa_id, name}]  for fuzzy title matching
      file_index: {nepa_id: [(doc_name, filename, Path)]}
    """
    # Project names — prefer new_project_record.csv (cleaner names)
    new_proj = pd.read_csv(BLM_NEW_PROJ_CSV, low_memory=False)
    eis_proj  = new_proj[new_proj["pids"].str.contains("-EIS", na=False)]
    name_map  = dict(zip(eis_proj["pids"].str.strip(), eis_proj["pnames"].str.strip()))

    # Documents on disk
    doc_df  = pd.read_csv(BLM_DOC_CSV, low_memory=False)
    eis_doc = doc_df[doc_df["NEPA_ID"].str.contains("-EIS", na=False)].copy()

    # For NEPA_IDs not in name_map, derive a name from Document Name column
    extra_ids = set(eis_doc["NEPA_ID"].dropna().unique()) - set(name_map.keys())
    for nid in extra_ids:
        candidates = eis_doc.loc[eis_doc["NEPA_ID"] == nid, "Document Name"].dropna()
        eis_names = [n for n in candidates if any(k in n.lower() for k in ("eis", "environmental impact"))]
        pool = eis_names or list(candidates)
        if pool:
            name_map[nid] = max(pool, key=len)

    proj_list = [{"nepa_id": k, "name": v} for k, v in name_map.items()]

    # File index
    file_index = {}
    for _, d in eis_doc.iterrows():
        nid      = str(d["NEPA_ID"])
        doc_name = str(d.get("Document Name", ""))
        fn       = str(d["File_Name"])
        yr_m     = re.search(r"-(\d{4})-\d{4}-", nid)
        if not yr_m:
            continue
        path = BLM_DOCS_DIR / yr_m.group(1) / f"{nid}--{fn}"
        if path.exists():
            file_index.setdefault(nid, []).append((doc_name, fn, path))

    return proj_list, file_index


def match_blm(title: str, eis_type: str, ceq: str,
              proj_list: list, file_index: dict) -> tuple:
    """Returns (entries, score, matched_nepa_id)."""
    best_score, best_nepa = 0.0, ""
    for p in proj_list:
        s = title_similarity(title, p["name"])
        if s > best_score:
            best_score, best_nepa = s, p["nepa_id"]

    if best_score < MATCH_THRESHOLD:
        return [], best_score, best_nepa

    target_df  = eis_type_to_df(eis_type)
    body_files = [
        (dn, fn, path) for dn, fn, path in file_index.get(best_nepa, [])
        if _blm_is_eis_body(dn, fn) and passes_df_filter(dn + " " + fn, target_df)
    ]
    if not body_files:
        return [], best_score, best_nepa

    entries = [
        {
            "source_url":     f"local:{path}",
            "local_filename": make_local_filename(ceq, f"{best_nepa}--{fn}"),
            "status":         "found_library",
            "notes":          f"BLM library: {best_nepa} (score {best_score:.2f})",
        }
        for dn, fn, path in body_files
    ]
    return entries, best_score, best_nepa


# ---------------------------------------------------------------------------
# USFS library index
# ---------------------------------------------------------------------------
def build_usfs_index() -> tuple:
    """
    Returns:
      title_index: {project_num: [doc_names]}  for fuzzy matching
      file_index:  {project_num: [(doc_name, filename, Path)]}
    """
    df = pd.read_csv(USFS_DOC_CSV, low_memory=False)
    analysis_eis = df[
        (df["Stage"] == "Analysis")
        & df["Document_Name"].str.lower().str.contains(
            r"eis|environmental impact|deis|feis", na=False, regex=True
        )
    ]

    # Pre-scan disk: filename → Path
    print("  USFS: scanning documents directory...")
    file_map = {}
    for yr_dir in USFS_DOCS_DIR.iterdir():
        if not yr_dir.is_dir():
            continue
        for f in yr_dir.iterdir():
            if f.is_file():
                file_map[f.name] = f

    title_index = {}
    file_index  = {}
    for _, row in analysis_eis.iterrows():
        if pd.isna(row["Project_Num"]):
            continue
        pnum     = str(int(float(row["Project_Num"])))
        doc_name = str(row["Document_Name"])
        fn       = str(row["File_Name"]) if pd.notna(row["File_Name"]) else None

        title_index.setdefault(pnum, []).append(doc_name)
        if fn and fn in file_map:
            file_index.setdefault(pnum, []).append((doc_name, fn, file_map[fn]))

    return title_index, file_index


def match_usfs(title: str, eis_type: str, ceq: str,
               title_index: dict, file_index: dict) -> tuple:
    """Returns (entries, score, matched_project_num)."""
    best_score, best_pnum = 0.0, ""
    for pnum, doc_names in title_index.items():
        s = max(title_similarity(title, dn) for dn in doc_names)
        if s > best_score:
            best_score, best_pnum = s, pnum

    if best_score < MATCH_THRESHOLD:
        return [], best_score, best_pnum

    target_df = eis_type_to_df(eis_type)
    filtered  = [
        (dn, fn, path) for dn, fn, path in file_index.get(best_pnum, [])
        if passes_df_filter(dn + " " + fn, target_df)
    ]
    if not filtered:
        return [], best_score, best_pnum

    entries = [
        {
            "source_url":     f"local:{path}",
            "local_filename": make_local_filename(ceq, fn),
            "status":         "found_library",
            "notes":          f"USFS library: project {best_pnum} (score {best_score:.2f}) — {dn}",
        }
        for dn, fn, path in filtered
    ]
    return entries, best_score, best_pnum


# ---------------------------------------------------------------------------
# Internet Archive search
# ---------------------------------------------------------------------------
def _ia_get(url: str, **kwargs):
    time.sleep(IA_DELAY)
    return requests.get(url, headers=HEADERS, timeout=20, **kwargs)


def _ia_pdfs_for_identifier(identifier: str) -> list:
    try:
        r = _ia_get(f"https://archive.org/metadata/{identifier}")
        if r.status_code != 200:
            return []
        files = r.json().get("files", [])
        pdfs  = [
            f for f in files
            if f.get("format") == "Text PDF"
            or (
                f.get("name", "").lower().endswith(".pdf")
                and f.get("format", "") not in ("Metadata", "Archive BitTorrent")
                and not f["name"].startswith("__")
            )
        ]
        base = f"https://archive.org/download/{identifier}/"
        return [{"url": base + f["name"], "name": f["name"],
                 "notes": f"Internet Archive: {identifier}"}
                for f in pdfs]
    except Exception as e:
        print(f"    [IA] metadata fetch failed for {identifier}: {e}")
        return []


def ia_search(project_title: str, eis_type: str) -> list:
    words = [w for w in re.split(r"\W+", project_title) if len(w) > 4][:4]
    if not words:
        return []
    query = (
        "mediatype:texts AND subject:\"environmental impact statement\" AND ("
        + " AND ".join(f'title:"{w}"' for w in words)
        + ")"
    )
    try:
        r = _ia_get("https://archive.org/advancedsearch.php",
                    params={"q": query, "output": "json", "rows": 5, "fl[]": "identifier,title"})
        if r.status_code != 200:
            return []
        target_df = eis_type_to_df(eis_type)
        for doc in r.json().get("response", {}).get("docs", []):
            hit = doc.get("title", "").lower()
            if any(w.lower() in hit for w in words):
                pdfs     = _ia_pdfs_for_identifier(doc["identifier"])
                filtered = [p for p in pdfs if passes_df_filter(p["name"], target_df)]
                use      = filtered or pdfs
                if use:
                    print(f"    [IA] matched \"{doc['title']}\" ({doc['identifier']})")
                    return use
    except Exception as e:
        print(f"    [IA] search failed: {e}")
    return []


# ---------------------------------------------------------------------------
# Discovery waterfall
# ---------------------------------------------------------------------------
def discover(row: dict, doe_index: dict, blm_proj: list, blm_files: dict,
             usfs_titles: dict, usfs_files: dict) -> tuple:
    """
    Returns (queue_rows, review_rows).
    """
    ceq      = row["ceqNumber"]
    eis_type = row["eis_type"]
    title    = row["project_title"]
    agency   = row["lead_agency"]
    eis_id   = str(row.get("eisId", "") or "")
    uid      = row.get("unique_id")

    def base(extra: dict) -> dict:
        return {
            "ceqNumber":      ceq,
            "eis_type":       eis_type,
            "lead_agency":    agency,
            "project_title":  title,
            "eisId":          eis_id,
            "source_url":     extra.get("source_url", ""),
            "local_filename": extra.get("local_filename", ""),
            "status":         extra.get("status", "pending"),
            "notes":          extra.get("notes", ""),
        }

    def pending(note: str) -> list:
        return [base({"status": "pending", "notes": note})]

    def with_gap_check(entries: list) -> list:
        note = missing_volume_note([e["local_filename"] for e in entries])
        if note:
            entries.append(base({"status": "pending", "notes": note}))
        return entries

    # 1. DOE library (deterministic via unique_id, then fall through)
    if agency == "Department of Energy":
        doe_entries = match_doe(uid, eis_type, ceq, doe_index)
        if doe_entries:
            return with_gap_check([base(e) for e in doe_entries]), []

    # 2. BLM library
    if agency == "Bureau of Land Management":
        blm_entries, blm_score, blm_id = match_blm(title, eis_type, ceq, blm_proj, blm_files)
        if blm_entries:
            rows = with_gap_check([base(e) for e in blm_entries])
            review = []
            if blm_score < REVIEW_THRESHOLD:
                matched_name = next((p["name"] for p in blm_proj if p["nepa_id"] == blm_id), "")
                review = [{"ceqNumber": ceq, "lead_agency": agency, "ceq_title": title,
                           "library": "BLM", "matched_id": blm_id,
                           "matched_name": matched_name, "score": round(blm_score, 3)}]
                for r in rows:
                    r["notes"] += " | LOW CONFIDENCE — review supplemental_match_review.csv"
            return rows, review

    # 3. USFS library
    if agency == "Forest Service":
        usfs_entries, usfs_score, usfs_pnum = match_usfs(title, eis_type, ceq, usfs_titles, usfs_files)
        if usfs_entries:
            rows = with_gap_check([base(e) for e in usfs_entries])
            review = []
            if usfs_score < REVIEW_THRESHOLD:
                matched_names = usfs_titles.get(usfs_pnum, [])
                review = [{"ceqNumber": ceq, "lead_agency": agency, "ceq_title": title,
                           "library": "USFS", "matched_id": usfs_pnum,
                           "matched_name": matched_names[0] if matched_names else "",
                           "score": round(usfs_score, 3)}]
                for r in rows:
                    r["notes"] += " | LOW CONFIDENCE — review supplemental_match_review.csv"
            return rows, review

    # 4. Internet Archive search
    if agency in IA_AGENCIES and not row.get("_skip_ia"):
        ia_results = ia_search(title, eis_type)
        if ia_results:
            rows = [
                base({
                    "source_url":     p["url"],
                    "local_filename": make_local_filename(ceq, p["name"]),
                    "status":         "found_ia",
                    "notes":          p.get("notes", ""),
                })
                for p in ia_results
            ]
            return with_gap_check(rows), []

    return pending("automated discovery failed"), []


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Build supplemental EIS download queue.")
    parser.add_argument("--years", nargs="+", default=None,
                        help="Year prefixes to target (e.g. 2010 2011 2012)")
    parser.add_argument("--migrate-salinas", action="store_true",
                        help="Copy salinas PDFs into the supplemental_eis_documents directory")
    parser.add_argument("--no-ia", action="store_true",
                        help="Skip Internet Archive search (library matching only)")
    args = parser.parse_args()

    years = args.years or [str(y) for y in range(2000, 2026)]
    print(f"Target years: {years}\n")

    PDF_DIR.mkdir(parents=True, exist_ok=True)

    # Load full metadata (needed for carry-over enrichment)
    meta = pd.read_parquet(META_PARQUET)

    # Load existing queue — preserve all terminal-state rows
    existing = {}   # ceqNumber → [row_dict, ...]
    if QUEUE_FILE.exists():
        prev = pd.read_csv(QUEUE_FILE, dtype=str).fillna("")
        for _, r in prev.iterrows():
            if r["status"] in TERMINAL_STATES:
                existing.setdefault(r["ceqNumber"], []).append(r.to_dict())

    # Merge salinas carry-over into existing
    carryover = load_salinas_carryover(meta)
    for ceq, rows in carryover.items():
        for r in rows:
            if r["status"] in TERMINAL_STATES and ceq not in existing:
                existing.setdefault(ceq, []).append(r)

    # Optionally migrate salinas PDFs
    if args.migrate_salinas and SALINAS_PDFS.exists():
        print(f"Migrating salinas PDFs → {PDF_DIR}")
        copied = 0
        for f in SALINAS_PDFS.iterdir():
            if f.is_file():
                dest = PDF_DIR / f.name
                if not dest.exists():
                    shutil.copy2(f, dest)
                    copied += 1
        print(f"  Copied {copied} file(s)\n")

    # Get targets
    targets = get_targets(years)
    print(f"Targets: {len(targets)} ceqNumbers")
    print(f"Already in terminal state: {sum(1 for c in targets['ceqNumber'] if c in existing)}\n")

    # Build library indexes
    print("Building library indexes...")
    doe_index = build_doe_index()
    print(f"  DOE : {sum(len(v) for v in doe_index.values())} files, "
          f"{len(doe_index)} EIS numbers")

    blm_proj, blm_files = build_blm_index()
    print(f"  BLM : {len(blm_proj)} EIS projects, "
          f"{sum(len(v) for v in blm_files.values())} files on disk")

    usfs_titles, usfs_files = build_usfs_index()
    print(f"  USFS: {len(usfs_titles)} EIS projects, "
          f"{sum(len(v) for v in usfs_files.values())} files on disk")
    print()

    all_rows   = []
    review_rows = []

    for _, target in targets.iterrows():
        ceq = target["ceqNumber"]

        if ceq in existing:
            all_rows.extend(existing[ceq])
            continue

        row = target.to_dict()
        row["_skip_ia"] = args.no_ia
        print(f"[{ceq}] {row['lead_agency']} — {row['eis_type']} — {str(row['project_title'])[:60]}")

        entries, review = discover(row, doe_index, blm_proj, blm_files, usfs_titles, usfs_files)
        all_rows.extend(entries)
        review_rows.extend(review)

        for e in entries:
            print(f"  → {e['status']}  {e.get('notes', '')[:80]}")

    # Write queue
    df = pd.DataFrame(all_rows, columns=QUEUE_COLS)
    df.to_csv(QUEUE_FILE, index=False)
    print(f"\nQueue written: {len(df)} rows → {QUEUE_FILE}")
    print(df["status"].value_counts().to_string())

    # Write review file
    if review_rows:
        review_df = pd.DataFrame(review_rows)
        # Append to existing review file if present
        if REVIEW_FILE.exists():
            old = pd.read_csv(REVIEW_FILE, dtype=str).fillna("")
            review_df = pd.concat([old, review_df], ignore_index=True).drop_duplicates("ceqNumber")
        review_df.to_csv(REVIEW_FILE, index=False)
        print(f"\nLow-confidence matches for review: {len(review_rows)} → {REVIEW_FILE}")

    print("\nDone.")


if __name__ == "__main__":
    main()
