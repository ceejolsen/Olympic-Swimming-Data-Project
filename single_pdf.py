import re
import io
import requests
import pdfplumber
import pandas as pd
from pathlib import Path

HEAT_HEADER = re.compile(
    r'^(Final\s+([ABC]))\s*(?:Event.*)?$'
    r'|^(Final)\s+Event',                    
    re.IGNORECASE | re.MULTILINE
)

SWIMMER_LINE = re.compile(
    r'^([=T]?\d+[=T]?)\s+(\d+)\s+'        # rank lane
    r'(.+?)\s+'                          # name blob
    r'(?:(0\.\d{2})\s+)?'                # optional reaction time
    r'(\d+:\d{2}\.\d{2}|\d{2,3}\.\d{2})' # final time
    r'(?:\s+.*)?$',                      # allow any trailing junk/columns
    re.MULTILINE
)

# Strips a trailing NOC / club code from the raw name blob, e.g.:
#   "CHN", "JPN", "SYS-FL", "CW-MI", "FCSTGU", "TWSTGU", "LAC-MI"
CODE_SUFFIX = re.compile(r'\s+[A-Z]{2,8}(?:-[A-Z]{2,3})?\s*$')

SPLIT_ENTRY = re.compile(
    r'(\d{2,3})m\s*'
    r'(?:[\(\uFD3E\uff08]\d+[\)\uFD3F\uff09]\s*)?'
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
    r'^\d+:\d{2}\.\d{2}\s+\d+:\d{2}', 
    r'^(?:Rank|NOC|Code|Points|Behind)',
    r'^Results',
    r'^R.sultats',
    r'^Name\s+(?:Club|NOC)',
    r'^Page\s+\d',
    r'^Report\s+Created',
    r'^FINA/',
    r'^Event\s+\d',
    r'^Final\s*[ABC]?\s*(?:Event|$)',
]

#list comprehension for how they appear
NOISE_PATTERNS = [re.compile(p, re.IGNORECASE) for p in _NOISE_RES]

def _load_pdf_bytes(source: str) -> bytes:
    """Load PDF bytes from a local file path or an HTTP/HTTPS URL.
    Keyword arguments:
        Parameters:
            source: link to the pdf
        Return arguments: 
            the path in byte form (I think?)
    """
    if source.startswith("http://") or source.startswith("https://"):
        response = requests.get(source, timeout=30)
        response.raise_for_status()
        return response.content
    return Path(source).read_bytes()


def _extract_full_text(pdf_bytes: bytes) -> str:
    """
    Extract text from every PDF page and join with newlines.
    Soft-hyphens (U+00AD) are normalised to regular hyphens so that
    hyphenated club codes and names are
    handled consistently by the same regex.

    Keyword Arguments:
        Parameters:
            pdf_bytes: the page from every pdf
        Return arguments:
            the text after being filtered 
    """
    pages = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                pages.append(text.replace("\u00ad", "-"))
    return "\n".join(pages)


def _is_noise(line: str) -> bool:
    """Returns if a line should be skipped during swimmer parsing, if it matches the noise rege.
    Keyword Arguments:
        Parameters:
            Line: the line of text in str form
        Return arguments:
            A bool saying to skip a line or not
    """
    stripped = line.strip()
    if not stripped:
        return True
    return any(p.match(stripped) for p in NOISE_PATTERNS)


def parse_splits(line: str) -> dict:
    """
    Extract cumulative split times from a splits line.
    Returns a dict like {'50': '26.22', '100': '54.52', ...}.

    Keyword arguments:
        Parameters:
            line: the line with splits
        Return arguments:
            dict: a dictionary with splits in cumulative time
    """
    return {m.group(1): m.group(2) for m in SPLIT_ENTRY.finditer(line)}


def split_name(raw: str) -> tuple:
    """
    Helper function to split a 'LAST Firstname' string into (last_name, first_name).

    Keyword arguments:
        Parameters
        Return argments:
            Tuple:
                (Last name, 
                First name)
    """
    tokens = raw.split()
    last_parts, first_parts = [], []
    for token in tokens:
        if re.match(r"^[A-Z][A-Z\-']+$", token) and not first_parts:
            last_parts.append(token)
        else:
            first_parts.append(token)
    return " ".join(last_parts) or raw, " ".join(first_parts)


def split_into_heat_sections(text: str) -> list:
    """
    Helper function to locate each heat header and return a list of (heat_label, section_text).

    Handles:
      - "Final A/B/C Event No. ..." (header + Event on same line)
      - "Final B" / "Final C" alone (page-break style, no Event keyword)
      - "Final Event No. ..." (single unnamed heat, World Cup style)

    Keyword argument:
        Paremeters:

        Return arguments:
            
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


def parse_heat_section(heat_label: str, section_text: str) -> list:
    """Helper function to parse one heat section and return a list of swimmer dicts.
    
    Parameters:
        

    Return arguments:
        Swimmer dictionaries
    """

    lines = section_text.splitlines()
    rows = []
    i = 0

    while i < len(lines):
        line = lines[i].strip()
        m = SWIMMER_LINE.match(line)

        if m:
            raw_rank = m.group(1)
            rank_digits = re.sub(r"\D", "", raw_rank)
            if not rank_digits:
                i += 1
                continue
            rank = int(rank_digits)
            lane       = int(m.group(2))
            raw_middle = m.group(3).strip()
            reaction   = float(m.group(4)) if m.group(4) else None
            final_time = m.group(5)

            # Strip trailing NOC/club code, then split into last / first name
            name_clean = CODE_SUFFIX.sub("", raw_middle).strip()
            last_name, first_name = split_name(name_clean)

            # Look ahead for the cumulative splits line
            splits = {}
            j = i + 1
            while j < len(lines):
                next_line = lines[j].strip()
                if not next_line:
                    j += 1
                    continue
                if SPLIT_ENTRY.search(next_line):
                    splits = parse_splits(next_line)
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

def process_single_link(link: str) -> list:
    """
    Calls parse_pdf() on a URL and returns a list of dicts with 'Link' attached.
    Returns an empty list on failure so the caller can skip gracefully.

    Parameters:

    Return arguments:
    
    """
    try:
        df = parse_pdf(link)
        if df.empty:
            return []
        df.insert(0, "Link", link)
        return df.to_dict("records")
    except Exception as e:
        print(f"Failed to parse {link}: {e}")
        return []


def parse_pdf(source: str) -> pd.DataFrame:
    """
    Turn a 400m freestyle swimming results PDF into a pandas DataFrame.

    Keyword arguments
    source : str
        a url pointing to the pdf

    pd.DataFrame
        One row per swimmer with columns:
            heat, rank, lane, last_name, first_name, reaction_time,
            split_50m, split_100m, split_150m, split_200m,
            split_250m, split_300m, split_350m, final_time

        reaction_time is float (NaN when not recorded in the PDF).
        All split and final_time values are strings (M:SS.ss or SS.ss format). FIX THIS
        heat is one of: "Final", "Final A", "Final B", "Final C".
        NaN if not found
    """
    pdf_bytes = _load_pdf_bytes(source)
    full_text = _extract_full_text(pdf_bytes)
    sections  = split_into_heat_sections(full_text)

    all_rows = []
    for heat_label, section_text in sections:
        all_rows.extend(parse_heat_section(heat_label, section_text))

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