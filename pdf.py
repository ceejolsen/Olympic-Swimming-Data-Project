from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor
import pymupdf  
import re
import pandas as pd

# 1. Matches Rank, optional Heat, Lane, Name, Reaction Time, and Final Time
SWIMMER_ROW_RE = re.compile(
    r'"(?P<rank>\d+)"\s*,\s*(?:"\d+"\s*,\s*)?"(?P<lane>\d+)"\s*,\s*"(?P<name>[^"]+)"\s*,.*?"(?P<rt>\d\.\d{2})"\s*,\s*"(?P<final>\d:\d{2}\.\d{2})"', 
    re.DOTALL
)

# Handles "50m 26.76"  and "26.39 50m" 
SPLIT_RE = re.compile(
    r'(?P<d1>50|100|150|200|250|300|350)m.*?(\d{1,2}[:.]\d{2})|(\d{1,2}[:.]\d{2}).*?(?P<d2>50|100|150|200|250|300|350)m'
)

def process_single_link(link):
    local_data = []
    try:
        with requests.Session() as s:
            response = s.get(link, impersonate="chrome110", timeout=15)
            
        with pymupdf.open(stream=response.content, filetype="pdf") as doc:
            for page in doc:
                # Use page_text directly from the provided extracted format
                page_text = page.get_text("text")
                
                # Find all swimmer rows on the page
                swimmers = list(SWIMMER_ROW_RE.finditer(page_text))
                
                for i, match in enumerate(swimmers):
                    # Define context: from current swimmer start to next swimmer start
                    start_idx = match.start()
                    end_idx = swimmers[i+1].start() if i+1 < len(swimmers) else len(page_text)
                    context = page_text[start_idx:end_idx]
                    
                    # Extract metadata from the main row match
                    data = match.groupdict()
                    
                    entry = {
                        "Rank": data['rank'],
                        "Name": data['name'].strip().replace('\n', ' '),
                        "Lane": data['lane'],
                        "Reaction Time": data['rt'],
                        "Final Time": data['final']
                    }
                    
                    # Initialize empty splits
                    for d in ["50m", "100m", "150m", "200m", "250m", "300m", "350m"]:
                        entry[d] = None

                    # Extract Splits from the context block
                    for s_match in SPLIT_RE.finditer(context):
                        # Determine which group captured the distance and time
                        dist = f"{s_match.group('d1') or s_match.group('d2')}m"
                        time_val = s_match.group(2) if s_match.group('d1') else s_match.group(3)
                        entry[dist] = time_val
                    
                    local_data.append(entry)
                    
    except Exception as e:
        print(f"Error processing {link}: {e}")
    return local_data

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
    mens_links = file['mens_400_free_pdf'].tolist()
    womens_links = file['womens_400_free_pdf'].tolist()
    return mens_links, womens_links

def scrape_omega(links: list, max_workers: int = 10) -> pd.DataFrame:
    all_results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = executor.map(process_single_link, links)
        for r_list in results:
            all_results.extend(r_list)
        
    df = pd.DataFrame(all_results)
    if df.empty: return df

    # Convert all split and time columns to seconds
    time_cols = [c for c in df.columns if 'm' in c or "Final" in c]
    for col in time_cols:
        df[col] = df[col].apply(time_to_seconds)
        
    return df