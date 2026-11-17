"""Content comparison for review (re-verification) runs.

A review re-fetches a previously collected announcement from its original
URL and compares the freshly extracted title/body against the snapshot kept
from the original collection. Only three verdicts are possible once a fetch
definitively completes:

  unchanged  - title and body text are byte-identical to the stored snapshot
  modified   - a 2xx page was fetched and extracted, but something changed
  withdrawn  - the origin answered 404/410 (the ONLY withdrawal signals)

Rate limiting, network errors, redirect-limit/parsing failures never reach
this module as verdicts: the engine records them as inconclusive "error"
items, which cannot alter the previously published report.
"""
import difflib

MAX_DIFF_LINES = 200


def _line_diff(old: str, new: str) -> list[str]:
    lines = list(difflib.unified_diff(
        old.splitlines() or [""],
        new.splitlines() or [""],
        fromfile="previous",
        tofile="current",
        lineterm="",
        n=3,
    ))
    truncated = False
    if len(lines) > MAX_DIFF_LINES:
        lines = lines[:MAX_DIFF_LINES]
        truncated = True
    if truncated:
        lines.append(f"... diff truncated at {MAX_DIFF_LINES} lines ...")
    return lines


def compare_documents(old_title: str, old_content: str,
                      new_title: str, new_content: str) -> dict:
    """Return a verdict + change description for one announcement."""
    title_changed = (old_title or "") != (new_title or "")
    content_changed = (old_content or "") != (new_content or "")
    if not title_changed and not content_changed:
        return {"verdict": "unchanged", "changes": None}

    changes = {
        "title_changed": title_changed,
        "content_changed": content_changed,
    }
    if title_changed:
        changes["title"] = {"previous": old_title or "", "current": new_title or ""}
    if content_changed:
        changes["content_diff"] = _line_diff(old_content or "", new_content or "")
    return {"verdict": "modified", "changes": changes}
