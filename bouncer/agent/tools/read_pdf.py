"""
read_pdf — extracts text from protocol PDF documents.

Used by the extract node to pull structured parameters from protocol
documents: passage ranges, reagent lots, treatment durations,
incubation times, instrument settings, etc.

The agent then sends the extracted text to the LLM to parse out
specific fields relevant to the assay type.
"""


def read_pdf(path: str, max_pages: int = 20) -> dict:
    """
    Extract text from a PDF file using pdfplumber.

    Args:
        path:      Absolute path to the PDF.
        max_pages: Maximum number of pages to extract (avoids huge payloads
                   for long protocol documents).

    Returns dict with:
        n_pages, pages (list of {page_num, text}), full_text (concatenated)
    """
    try:
        import pdfplumber
    except ImportError:
        return {"path": path, "error": "pdfplumber not installed"}

    try:
        with pdfplumber.open(path) as pdf:
            total_pages = len(pdf.pages)
            pages_to_read = min(total_pages, max_pages)

            pages = []
            for i in range(pages_to_read):
                text = pdf.pages[i].extract_text() or ""
                pages.append({
                    "page_num": i + 1,
                    "text": text,
                    "char_count": len(text),
                })

        full_text = "\n\n".join(p["text"] for p in pages if p["text"])

        return {
            "path": path,
            "n_pages": total_pages,
            "pages_extracted": pages_to_read,
            "pages": pages,
            "full_text": full_text,
        }

    except Exception as e:
        return {"path": path, "error": str(e)}
