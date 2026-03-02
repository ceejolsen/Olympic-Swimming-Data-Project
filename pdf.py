from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor
import pymupdf  
import re
import pandas as pd

SWIMMER_ROW_RE = re.compile(
    r"(?P<rank>\d+)?\s+(?P<lane>\d+)\s+(?P<name>[A-Z][A-Z\s,]+)\s+(?P<noc>[A-Z]{3})\s+(?P<rt>0\.\d{2})?\s+(?P<final>\d:?\d{2}\.\d{2}|DNS|DSQ|DNF)?"
)

SPLIT_RE = re.compile(r"(?P<dist>50|100|150|200|250|300|350)m\s+﴾\d+﴿\s+(?P<time>\d:?\d{2}\.\d{2}|\d{2}\.\d{2})")

def process_single_link(link):
    if not isinstance(link, str) or not link.startswith("http"):
        return []
        
    local_data = []
    try:
        with requests.Session() as s:
            response = s.get(link, impersonate="chrome110", timeout=15)
            if response.status_code != 200: return []
            content = response.content
            
        with pymupdf.open(stream=content, filetype="pdf") as doc:
            for page in doc:
                text = page.get_text("text")
                # Normalize the text: remove weird spaces, keep lines
                lines = [l.strip() for l in text.split('\n') if l.strip()]
                
                current_entry = None
                
                for i, line in enumerate(lines):
                    # 1. Identify a Swimmer: Usually All Caps, followed by a Club Code
                    # Example: GEMMELL Andrew followed by DST-MA
                    if line.isupper() and i + 1 < len(lines) and any(char.isdigit() for char in lines[i+1]) == False:
                        # Save the previous one if it exists
                        if current_entry and current_entry.get("50m"): 
                            local_data.append(current_entry)
                        
                        current_entry = {
                            "Name": line,
                            "Club": lines[i+1] if i+1 < len(lines) else "",
                            "Link": link
                        }
                        for m in range(50, 400, 50): current_entry[f"{m}m"] = None
                        continue
                    
                    if current_entry:
                        # 2. Look for Rank/Lane/RT/Final (they appear near the name)
                        if "R.T." in line and i + 1 < len(lines):
                             current_entry["Reaction Time"] = lines[i+1]
                        
                        # 3. Look for Splits
                        split_match = SPLIT_RE.search(line)
                        if split_match:
                            dist = f"{split_match.group('dist')}m"
                            current_entry[dist] = split_match.group('time')
                        
                        # 4. Look for the Final Time (Usually a standalone time after the splits or near RT)
                        # This catches the 3:53.05 style format
                        if re.match(r"^\d:\d{2}\.\d{2}$", line) and not SPLIT_RE.search(line):
                            if "Final" not in line: # Avoid matching the header "Final"
                                current_entry["Final Time"] = line

                if current_entry and current_entry.get("50m"):
                    local_data.append(current_entry)
                    
    except Exception as e:
        print(f"Failed on {link}: {e}")
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
    mens_links = file['mens_400_free_pdf'].dropna().tolist()
    womens_links = file['womens_400_free_pdf'].dropna().tolist()
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