from __future__ import annotations

import re
from datetime import datetime, date, timezone
from pathlib import Path


_DATE_RE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def parse_iso_utc(s: str) -> datetime:
    """Parse an ISO 8601 string to a timezone-aware UTC datetime."""
    # Python 3.10 doesn't support 'Z' suffix in fromisoformat
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def extract_date_from_filename(name: str) -> date | None:
    """Extract the first YYYY-MM-DD date found in a filename."""
    m = _DATE_RE.search(name)
    if not m:
        return None
    return date.fromisoformat(m.group(1))


def filter_files_by_date_range(
    files: list[Path],
    time_from: datetime | None = None,
    time_to: datetime | None = None,
) -> list[Path]:
    """Keep files whose embedded date falls within [from.date(), to.date()] (inclusive).

    Files without an embedded date are always excluded.
    """
    date_from = time_from.date() if time_from else None
    date_to = time_to.date() if time_to else None

    result = []
    for f in files:
        d = extract_date_from_filename(f.name)
        if d is None:
            continue
        if date_from and d < date_from:
            continue
        if date_to and d > date_to:
            continue
        result.append(f)
    return result


