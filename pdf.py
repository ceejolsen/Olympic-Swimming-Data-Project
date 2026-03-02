from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import single_pdf as spdf
import pymupdf

def time_to_seconds(time_val) -> float:
    if not time_val or pd.isna(time_val): return None
    try:
        if ":" in str(time_val):
            m, s = str(time_val).split(":")
            return round(int(m) * 60 + float(s), 2)
        return round(float(time_val), 2)
    except: return None

def get_links_df(file: pd.DataFrame) -> list:
    """
    Extracts men's and women's PDF links from the dataframe.
    
    Returns:
        tuple: (mens_links, womens_links)
        which are the respective sexes link of events
    """
    if not isinstance(file, pd.DataFrame):
        return None, None
    mens_links = file['mens_400_free_pdf'].dropna().tolist()
    womens_links = file['womens_400_free_pdf'].dropna().tolist()
    return mens_links, womens_links

def scrape_omega(links: list, max_workers: int = 10) -> pd.DataFrame:
    all_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(spdf.process_single_link, links)
        for r_list in results:
            all_results.extend(r_list)
        
    df = pd.DataFrame(all_results)
    if df.empty:
        return df

    # Convert all split and time columns to seconds
    time_cols = [c for c in df.columns if 'm' in c or "Final" in c]
    for col in time_cols:
        df[col] = df[col].apply(time_to_seconds)
        
    return df


import os
from pathlib import Path

def dump_pdf_text_to_files(df: pd.DataFrame, limit: int = 10):
    """
    Downloads PDFs and saves their raw text content to /debug_dumps/
    to inspect the actual characters PyMuPDF is seeing.
    """
    # Create directory for the text files
    output_dir = Path("debug_dumps")
    output_dir.mkdir(exist_ok=True)
    
    # Get links from your existing function
    mens_links, womens_links = get_links_df(df) #
    all_links = (mens_links + womens_links)[:limit] 
    
    print(f"Dumping text for {len(all_links)} PDFs into '{output_dir}/'...")

    for i, link in enumerate(all_links):
        try:
            with requests.Session() as s:
                response = s.get(link, impersonate="chrome110", timeout=15)
                if response.status_code != 200: continue
                content = response.content
            
            # Extract text using your current library
            with pymupdf.open(stream=content, filetype="pdf") as doc:
                full_text = ""
                for page in doc:
                    full_text += f"--- PAGE {page.number} ---\n"
                    full_text += page.get_text("text") #
            
            # Save to file
            filename = f"pdf_dump_{i}.txt"
            with open(output_dir / filename, "w", encoding="utf-8") as f:
                f.write(f"SOURCE LINK: {link}\n\n")
                f.write(full_text)
            
            print(f"Saved: {filename}")
            
        except Exception as e:
            print(f"Failed to dump {link}: {e}")

# Add this to your Project.ipynb to run it:
# pdf.dump_pdf_text_to_files(df, limit=5)