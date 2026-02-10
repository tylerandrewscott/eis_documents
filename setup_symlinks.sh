#!/usr/bin/env bash
#
# setup_symlinks.sh
#
# Creates symlinks from this repo into the Box-synced eis_documents folder.
# Run once after cloning (or whenever symlinks need to be recreated).
#
# Usage:
#   bash setup_symlinks.sh
#   bash setup_symlinks.sh /path/to/box/eis_documents   # override auto-detection
#

set -euo pipefail

REPO_DIR="$(cd "$(dirname "$0")" && pwd)"

# --- Detect or accept the Box eis_documents root --------------------------

if [[ -n "${1:-}" ]]; then
    BOX_EIS="$1"
else
    # Try common Box mount names on macOS
    BOX_BASE="$HOME/Library/CloudStorage"
    if [[ -d "$BOX_BASE/Box-Box/eis_documents" ]]; then
        BOX_EIS="$BOX_BASE/Box-Box/eis_documents"
    elif [[ -d "$BOX_BASE/Box/eis_documents" ]]; then
        BOX_EIS="$BOX_BASE/Box/eis_documents"
    elif [[ -d "$HOME/Box/eis_documents" ]]; then
        BOX_EIS="$HOME/Box/eis_documents"
    else
        echo "ERROR: Could not find eis_documents in Box."
        echo "Searched:"
        echo "  $BOX_BASE/Box-Box/eis_documents"
        echo "  $BOX_BASE/Box/eis_documents"
        echo "  $HOME/Box/eis_documents"
        echo ""
        echo "Re-run with an explicit path:"
        echo "  bash setup_symlinks.sh /path/to/box/eis_documents"
        exit 1
    fi
fi

echo "Using Box eis_documents at: $BOX_EIS"

# --- Define symlinks: repo_relative_path -> box_relative_path -------------

declare -a LINKS=(
    "agency_nepa_libraries/blm/nepa_documents:agency_nepa_libraries/blm/nepa_documents"
    "agency_nepa_libraries/doe/documents:agency_nepa_libraries/doe/documents"
    "agency_nepa_libraries/doe/text_as_datatable:agency_nepa_libraries/doe/text_as_datatable"
    "agency_nepa_libraries/usfs/documents:agency_nepa_libraries/usfs/documents"
    "agency_nepa_libraries/usfs/text_as_datatable:agency_nepa_libraries/usfs/text_as_datatable"
    "enepa_repository/box_files/documents:enepa_repository/box_files/documents"
    "enepa_repository/box_files/epa_comment_letters:enepa_repository/box_files/epa_comment_letters"
    "enepa_repository/box_files/marker_conversions:enepa_repository/box_files/marker_conversions"
    "enepa_repository/box_files/reference_jsons:enepa_repository/box_files/reference_jsons"
    "enepa_repository/box_files/text_as_datatable:enepa_repository/box_files/text_as_datatable"
    "enepa_repository/box_files/text_conversions:enepa_repository/box_files/text_conversions"
)

# --- Create symlinks -------------------------------------------------------

for entry in "${LINKS[@]}"; do
    LINK_PATH="$REPO_DIR/${entry%%:*}"
    TARGET="$BOX_EIS/${entry##*:}"

    # Remove existing symlink or warn if something else is in the way
    if [[ -L "$LINK_PATH" ]]; then
        rm "$LINK_PATH"
    elif [[ -e "$LINK_PATH" ]]; then
        echo "WARNING: $LINK_PATH exists and is not a symlink — skipping."
        continue
    fi

    # Ensure parent directory exists
    mkdir -p "$(dirname "$LINK_PATH")"

    if [[ -d "$TARGET" ]]; then
        ln -s "$TARGET" "$LINK_PATH"
        echo "  OK  $LINK_PATH -> $TARGET"
    else
        echo "  MISSING  $TARGET  (symlink not created)"
    fi
done

echo ""
echo "Done. Symlinks created."
