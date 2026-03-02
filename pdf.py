from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor
from typing import List
import pdfplumber
import fitz
import io
import re
import pandas as pd

swimmer_re = re.compile(r'(\d+)\s+(\d+)\s+(\d+)\s+(.*?)\s+([A-Z-]{2,})\s+', re.MULTILINE)
splits_re = re.compile(r'50m\s+(\d+:\d+\.\d+|\d+\.\d+)\s+100m\s+(\d+:\d+\.\d+|\d+\.\d+)\s+150m\s+(\d+:\d+\.\d+|\d+\.\d+)\s+200m\s+(\d+:\d+\.\d+|\d+\.\d+)\s+250m\s+(\d+:\d+\.\d+|\d+\.\d+)\s+300m\s+(\d+:\d+\.\d+|\d+\.\d+)\s+350m\s+(\d+:\d+\.\d+|\d+\.\d+)')

def get_links_df(file: pd.DataFrame) -> list:
    """
    Extracts men's and women's PDF links from the dataframe.
    
    Returns:
        tuple: (mens_links, womens_links)
        which are the respective sexes link of events
    """
    if not isinstance(file, pd.DataFrame):
        return None, None
    mens_links = file['mens_400_free_pdf'].tolist()
    womens_links = file['womens_400_free_pdf'].tolist()
    return mens_links, womens_links

def time_to_seconds(time) -> float: #or a string, if that's what it is...
    if pd.isna(time) or time == "" or str(time).upper() == "NONE":
        return None    
    time_str = str(time).strip()
    try:
        if ":" in time_str:
            parts = time_str.split(":")
            return (float(parts[0]) * 60) + float(parts[1])
        return float(time_str)
    except (ValueError, IndexError):
        return None

def process_single_link(link):
    """Worker function to handle a single PDF."""
    if not isinstance(link, str) or not link.strip():
        return []
    local_data = []
    try:
        response = requests.get(link, impersonate="chrome110", timeout=20)
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            full_text = "\n".join([page.extract_text() or "" for page in pdf.pages])
            lines = full_text.split('\n')
            
            for i, line in enumerate(lines):
                swimmer_match = swimmer_re.match(line)
                if swimmer_match:
                    groups = swimmer_match.groups()
                    rank = groups[0]
                    heat = groups[1]
                    lane = groups[2]
                    name = groups[3]
                    club = groups[4]                
                    line_parts = lines[i].split()                    
                    rt = line_parts[-3] if len(line_parts) >= 8 else None
                    final_time = line_parts[-2] if len(line_parts) >= 8 else None
                    if i + 1 < len(lines):
                        splits_match = splits_re.search(lines[i+1])
                        if splits_match:
                            s50, s100, s150, s200, s250, s300, s350 = splits_match.groups()
                            local_data.append({
                                "Rank": rank,
                                "Name": name.strip(),
                                "Lane": lane,        
                                "Reaction Time": rt, 
                                "50m": s50, "100m": s100, "150m": s150, "200m": s200,
                                "250m": s250, "300m": s300, "350m": s350, 
                                "Final Time": final_time, 
                            }) 
    except Exception as e:
        print(f"Error processing {link}: {e}")
        
    return local_data

def scrape_omega(links: list, max_workers: int = 10) -> pd.DataFrame:
    all_results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # map ensures we process the links list in parallel
        results = list(executor.map(process_single_link, links))
    
    # Flatten the list of lists
    for result_list in results:
        all_results.extend(result_list)
        
    df = pd.DataFrame(all_results)
    
    # Vectorized time conversion (Pandas optimized)
    time_cols = [c for c in df.columns if "0m" in c or "Final" in c]
    for col in time_cols:
        df[col] = df[col].apply(time_to_seconds)
        
    return df