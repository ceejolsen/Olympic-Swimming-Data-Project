"""
swim_parser.py
--------------
Parses FINA/USA Swimming 400m Freestyle result PDFs into a pandas DataFrame.

Usage:
    from swim_parser import parse_pdf

    df = parse_pdf("path/to/results.pdf")
    # or from a URL:
    df = parse_pdf("https://example.com/results.pdf")

Output columns:
    heat, rank, lane, last_name, first_name, reaction_time,
    split_50m, split_100m, split_150m, split_200m,
    split_250m, split_300m, split_350m, final_time
"""

import re
import io
import requests
import pdfplumber
import pandas as pd
from pathlib import Path


# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Matches heat section headers.
# Two forms exist across different PDF types:
#   "Final A Event No. 16"  — lettered heat with "Event" on same line
#   "Final B"               — lettered heat alone on its line (page-break style)
#   "Final Event No. 12"    — single unnamed heat (World Cup style)
HEAT_HEADER = re.compile(
    r'^(Final\s+([ABC]))\s*(?:Event.*)?$'   # Final A/B/C (with or without Event)
    r'|^(Final)\s+Event',                    # unnamed Final ... Event
    re.IGNORECASE | re.MULTILINE
)

# Matches a swimmer header line.
# Anchors on rank+lane at the start and M:SS.ss final time near the end.
# Everything in between is the name+code blob, preceded by an optional RT.
#
# Examples handled:
#   "1 5 ZHANG Zhongchao CHN 3:46.00 834"          — no RT, FINA pts
#   "1 8 DOKI Kenichi JPN 0.73 3:47.20 821"         — RT, FINA pts
#   "1 4 CALDWELL Nicholas SYS-FL 0.71 3:52.88"     — RT, club code, no trailing
#   "2 2 COCHRANE Ryan CAN 0.92 3:46.78 2.05"       — RT, time-behind
#   "1 4 PARK Tae Hwan KOR 0.68 3:44.73"            — multi-word first name
SWIMMER_LINE = re.compile(
    r'^(\d+)\s+(\d+)\s+'       # group 1=rank, 2=lane
    r'(.+?)\s+'                 # group 3=raw name+code (non-greedy)
    r'(?:(0\.\d{2})\s+)?'      # group 4=reaction time (optional)
    r'(\d:\d{2}\.\d{2})'       # group 5=final time  e.g. 3:46.00
    r'(?:\s+[\d\.]+)?\s*$',    # optional trailing FINA pts or time-behind
    re.MULTILINE
)

# Strips a trailing NOC / club code from the raw name blob, e.g.:
#   "CHN", "JPN", "SYS-FL", "CW-MI", "FCSTGU", "TWSTGU", "LAC-MI"
CODE_SUFFIX = re.compile(r'\s+[A-Z]{2,8}(?:-[A-Z]{2,3})?\s*$')

# Matches cumulative-split entries within a split line, e.g.:
#   "50m ﴾1﴿ 26.22 100m ﴾1﴿ 54.52 150m ﴾1﴿ 1:23.39 ..."
# Position markers use Arabic presentation parentheses (U+FD3E / U+FD3F).
# Standard and fullwidth parentheses are also accepted for robustness.
SPLIT_ENTRY = re.compile(
    r'(\d{2,3})m\s*'
    r'[\u0028\uFD3E\uff08(]\d+[\u0029\uFD3F\uff09)]\s*'
    r'(\d+:\d{2}\.\d{2}|\d{2,3}\.\d{2})'
)

# Lines that should be unconditionally ignored during swimmer parsing
_NOISE_RES = [
    r'^Legend',
    r'^R\.T\.',
    r'^Timing',
    r'^SWM\d',
    r'^Record\s+Splits',
    r'^(?:WR|WC|CR|AR|US)\s+',
    r'^\d+:\d{2}\.\d{2}\s+\d+:\d{2}',   # WR split continuation lines
    r'^(?:Rank|NOC|Code|Points|Behind)',
    r'^Results',
    r'^R.sultats',
    r'^Name\s+(?:Club|NOC)',
    r'^Page\s+\d',
    r'^Report\s+Created',
    r'^FINA/',
    r'^RIO DE JANEIRO',
    r'^Event\s+\d',
    r'^Final\s*[ABC]?\s*(?:Event|$)',
    r'^(?:Club|NOC)\s+(?:Time|FINA)',
    r'^(?:August|Aug\.)',
    r'^\d{4}\s+(?:Pan|FINA|Speedo|Conoco)',
]
NOISE_PATTERNS = [re.compile(p, re.IGNORECASE) for p in _NOISE_RES]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_pdf_bytes(source: str) -> bytes:
    """Load PDF bytes from a local file path or an HTTP/HTTPS URL."""
    if source.startswith("http://") or source.startswith("https://"):
        response = requests.get(source, timeout=30)
        response.raise_for_status()
        return response.content
    return Path(source).read_bytes()


def _extract_full_text(pdf_bytes: bytes) -> str:
    """
    Extract text from every PDF page and join with newlines.
    Soft-hyphens (U+00AD) are normalised to regular hyphens so that
    hyphenated club codes (SYS-FL) and surnames (FRASER-HOLMES) are
    handled consistently by the same regex.
    """
    pages = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages.append(text.replace("\u00ad", "-"))
    return "\n".join(pages)


def _is_noise(line: str) -> bool:
    """Return True if a line should be skipped during swimmer parsing."""
    stripped = line.strip()
    if not stripped:
        return True
    return any(p.match(stripped) for p in NOISE_PATTERNS)


def _parse_splits(line: str) -> dict:
    """
    Extract cumulative split times from a splits line.
    Returns a dict like {'50': '26.22', '100': '54.52', ...}.
    """
    return {m.group(1): m.group(2) for m in SPLIT_ENTRY.finditer(line)}


def _split_name(raw: str) -> tuple:
    """
    Split a 'LAST Firstname' string into (last_name, first_name).

    Last name  = all leading ALL-CAPS tokens (handles hyphenated surnames like
                 FRASER-HOLMES and multi-word surnames like ABDEL KHALIK).
    First name = remaining mixed-case tokens.
    """
    tokens = raw.split()
    last_parts, first_parts = [], []
    for token in tokens:
        if re.match(r"^[A-Z][A-Z\-']+$", token) and not first_parts:
            last_parts.append(token)
        else:
            first_parts.append(token)
    return " ".join(last_parts) or raw, " ".join(first_parts)


def _split_into_heat_sections(text: str) -> list:
    """
    Locate each heat header and return a list of (heat_label, section_text).

    Handles:
      - "Final A/B/C Event No. ..." (header + Event on same line)
      - "Final B" / "Final C" alone (page-break style, no Event keyword)
      - "Final Event No. ..." (single unnamed heat, World Cup style)
    """
    matches = list(HEAT_HEADER.finditer(text))
    if not matches:
        return [("Final", text)]

    sections = []
    for i, m in enumerate(matches):
        # First alternation: "Final A/B/C" (group 1) with letter in group 2
        # Second alternation: unnamed "Final" in group 3
        if m.group(1):
            heat_label = m.group(1).strip()   # e.g. "Final A"
        else:
            heat_label = "Final"

        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        sections.append((heat_label, text[start:end]))

    return sections


def _parse_heat_section(heat_label: str, section_text: str) -> list:
    """Parse one heat section and return a list of swimmer dicts."""
    lines = section_text.splitlines()
    rows = []
    i = 0

    while i < len(lines):
        line = lines[i].strip()
        m = SWIMMER_LINE.match(line)

        if m:
            rank       = int(m.group(1))
            lane       = int(m.group(2))
            raw_middle = m.group(3).strip()
            reaction   = float(m.group(4)) if m.group(4) else None
            final_time = m.group(5)

            # Strip trailing NOC/club code, then split into last / first name
            name_clean = CODE_SUFFIX.sub("", raw_middle).strip()
            last_name, first_name = _split_name(name_clean)

            # Look ahead for the cumulative splits line
            splits = {}
            j = i + 1
            while j < len(lines):
                next_line = lines[j].strip()
                if not next_line:
                    j += 1
                    continue
                if SPLIT_ENTRY.search(next_line):
                    splits = _parse_splits(next_line)
                    i = j   # advance outer loop past this splits line
                    break
                if SWIMMER_LINE.match(next_line):
                    break   # next swimmer reached — no splits found
                j += 1

            rows.append({
                "heat":          heat_label,
                "rank":          rank,
                "lane":          lane,
                "last_name":     last_name,
                "first_name":    first_name,
                "reaction_time": reaction,
                "split_50m":     splits.get("50"),
                "split_100m":    splits.get("100"),
                "split_150m":    splits.get("150"),
                "split_200m":    splits.get("200"),
                "split_250m":    splits.get("250"),
                "split_300m":    splits.get("300"),
                "split_350m":    splits.get("350"),
                "final_time":    final_time,
            })

        i += 1

    return rows


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def process_single_link(source: str) -> pd.DataFrame:
    """
    Parse a 400m freestyle swimming results PDF into a pandas DataFrame.

    Parameters
    ----------
    source : str
        A local file path or an HTTP/HTTPS URL pointing to the PDF.

    Returns
    -------
    pd.DataFrame
        One row per swimmer with columns:
            heat, rank, lane, last_name, first_name, reaction_time,
            split_50m, split_100m, split_150m, split_200m,
            split_250m, split_300m, split_350m, final_time

        reaction_time is float (NaN when not recorded in the PDF).
        All split and final_time values are strings (M:SS.ss or SS.ss format).
        heat is one of: "Final", "Final A", "Final B", "Final C".
    """
    pdf_bytes = _load_pdf_bytes(source)
    full_text = _extract_full_text(pdf_bytes)
    sections  = _split_into_heat_sections(full_text)

    all_rows = []
    for heat_label, section_text in sections:
        all_rows.extend(_parse_heat_section(heat_label, section_text))

    _COLUMNS = [
        "heat", "rank", "lane", "last_name", "first_name",
        "reaction_time", "split_50m", "split_100m", "split_150m",
        "split_200m", "split_250m", "split_300m", "split_350m", "final_time",
    ]

    if not all_rows:
        return pd.DataFrame(columns=_COLUMNS)

    df = pd.DataFrame(all_rows)[_COLUMNS]
    df["reaction_time"] = pd.to_numeric(df["reaction_time"], errors="coerce")
    return df