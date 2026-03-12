"""
pdf_debug.py
------------
Utilities for inspecting OmegaTiming PDF formats and testing the parser.
"""

import single_pdf as spdf


def peek(link, n=1200):
    """
    Print first n characters of extracted text for debugging PDF formats.
    """
    b = spdf._load_pdf_bytes(link)
    t = spdf._extract_full_text(b)

    print("====", link)
    print(t[:n])


def test_parser(link):
    """
    Run the parser on a single PDF and print the resulting dataframe.
    """
    print("\nTesting parser on:", link)
    df = spdf.process_single_link(link)

    if df is None or len(df) == 0:
        print("No rows parsed.")
    else:
        print(df.head())
        print(f"\nRows parsed: {len(df)}")
        
    return df

def test_multiple_pdfs(links):
    """Run parser on multiple PDFs for quick testing."""
    for link in links:
        print("\nTesting:", link)

        try:
            df = spdf.process_single_link(link)
            print("Rows:", len(df))
            print(df.head(2))
        except Exception as e:
            print("Error:", e)
            
        

def inspect_lines(link, n=80):
    """Print extracted lines for regex debugging."""
    b = spdf._load_pdf_bytes(link)
    text = spdf._extract_full_text(b)

    lines = text.split("\n")

    for i, line in enumerate(lines[:n]):
    print(i, line)


def test_swimmer_regex(link, pattern):
    """Test swimmer regex pattern against extracted lines."""
    import re

    b = spdf._load_pdf_bytes(link)
    text = spdf._extract_full_text(b)

    for i, line in enumerate(text.split("\n")):
        if re.match(pattern, line):
            print("MATCH:", i, line)




if __name__ == "__main__":

    # Used to debug format differences across years
    test_links = [
        "https://www.omegatiming.com/File/00011A00000101EF0104FFFFFFFFFF01.pdf",
        "https://www.omegatiming.com/File/00010A0C0021000000FFFFFFFFFFFF01.pdf",
        "https://www.omegatiming.com/File/00010A0B0031000000FFFFFFFFFFFF01.pdf",
        "https://www.omegatiming.com/File/00010A09001B000000FFFFFFFFFFFF01.pdf",
        "https://www.omegatiming.com/File/00010A08001C000000FFFFFFFFFFFF01.pdf",
    ]

    # peek(test_links[0])  # uncomment to inspect raw text
    test_parser(test_links[0])