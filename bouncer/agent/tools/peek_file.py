"""
peek_file — quick file/directory preview for the classify node.

Returns enough information for the LLM to determine file type and
which reader tool to call next. Handles text, binary (FCS), and
directory (EDS) inputs without loading full data.
"""

import os


def peek_file(path: str, n_lines: int = 30) -> dict:
    """
    Preview a file or directory for classification.

    Returns a structured dict with detected format hints, first N lines
    for text files, magic bytes for binary files, or a directory listing
    for EDS-style output directories.
    """
    result: dict = {"path": path}

    # ── Directory (EDS output, 10x CellRanger output, etc.) ───────────────────
    if os.path.isdir(path):
        try:
            contents = os.listdir(path)
        except PermissionError as e:
            return {"path": path, "type": "directory", "error": str(e)}

        result["type"] = "directory"
        result["contents"] = contents[:30]

        # Detect known directory-based formats
        if any(f in contents for f in ("quants_mat.gz", "quants_mat_cols.txt", "counts.eds", "alevin")):
            result["detected_format"] = "eds_alevin_fry"
        elif "matrix.mtx.gz" in contents or "matrix.mtx" in contents:
            result["detected_format"] = "10x_cellranger"
        else:
            result["detected_format"] = "unknown_directory"

        return result

    if not os.path.isfile(path):
        return {"path": path, "type": "not_found"}

    result["size_bytes"] = os.path.getsize(path)
    result["extension"] = os.path.splitext(path)[1].lower()

    # ── Binary detection via magic bytes ──────────────────────────────────────
    try:
        with open(path, "rb") as f:
            magic = f.read(16)
    except OSError as e:
        return {**result, "type": "error", "error": str(e)}

    # FCS: magic bytes are "FCS" at offset 0
    if magic[:3] == b"FCS":
        result["type"] = "binary"
        result["detected_format"] = "fcs"
        result["fcs_version"] = magic[:6].decode("ascii", errors="replace").strip()
        return result

    # EDS (QuantStudio qPCR): ZIP archive with .eds extension
    # ZIP magic: PK\x03\x04
    if magic[:4] == b"PK\x03\x04" and result["extension"] == ".eds":
        result["type"] = "binary"
        result["detected_format"] = "eds_quantstudio"
        return result

    # HDF5: used by 10x .h5, AnnData .h5ad
    if magic[:8] == b"\x89HDF\r\n\x1a\n":
        result["type"] = "binary"
        result["detected_format"] = "hdf5"
        return result

    # ── Text files ────────────────────────────────────────────────────────────
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            lines = []
            for _ in range(n_lines):
                line = f.readline()
                if not line:
                    break
                lines.append(line.rstrip("\n"))

        result["type"] = "text"
        result["first_lines"] = lines
        result["line_count_preview"] = len(lines)

        # Cheap format hints from content
        if lines:
            first = lines[0]
            if first.startswith("{") or first.startswith("["):
                result["detected_format"] = "json"
            elif first.startswith("version:") or "assay_type:" in "\n".join(lines[:5]):
                result["detected_format"] = "bouncer_yaml"
            elif "\t" in first:
                result["detected_format"] = "tsv"
            elif "," in first:
                result["detected_format"] = "csv"
            elif first.startswith("%PDF"):
                result["detected_format"] = "pdf"
            elif result["extension"] in (".yaml", ".yml"):
                result["detected_format"] = "yaml"

    except UnicodeDecodeError:
        result["type"] = "binary"
        result["detected_format"] = "unknown_binary"
        result["magic_hex"] = magic.hex()

    return result
