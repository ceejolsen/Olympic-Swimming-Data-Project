import pymupdf
import re
from curl_cffi import requests

# Matches: 50m  ﴾6﴿  27.58 (Captures the distance and cumulative time)
SPLIT_RE = re.compile(r"(?P<dist>50|100|150|200|250|300|350)m\s+﴾\d+﴿\s+(?P<cum_time>\d:?\d{2}\.\d{2}|\d{2}\.\d{2})")
TIME_RE = re.compile(r"^\d:?\d{2}\.\d{2}$") # Standard time format M:SS.hh
RT_RE = re.compile(r"^0\.\d{2}$")           # Reaction Time format 0.xx

def process_single_link(link):
    if not isinstance(link, str) or not link.startswith("http"):
        return []
        
    local_data = []
    BLACKLIST = {
        "CLUB", "CODE", "TIME", "RANK", "LANE", "BEHIND", "R.T.", 
        "FINAL", "RESULTS", "RECORD", "LOCATION", "DATE", "NOC", "SPLITS", 
        "EVENT", "WR", "AR", "US", "CR", "MEN'S", "WOMEN'S", "FREESTYLE"
    }

    try:
        with requests.Session() as s:
            response = s.get(link, impersonate="chrome110", timeout=15)
            if response.status_code != 200: return []
            content = response.content
            
        with pymupdf.open(stream=content, filetype="pdf") as doc:
            for page in doc:
                text = page.get_text("text")
                if "400m Freestyle" not in text: continue
                
                lines = [l.strip() for l in text.split('\n') if l.strip()]
                current_entry = None
                collecting_meta = False
                
                for i, line in enumerate(lines):
                    # --- 1. Identify Swimmer Name ---
                    # Logic: Mixed case, not a header, contains a space, has letters
                    if (line.upper() not in BLACKLIST and " " in line and 
                        re.search(r'[a-zA-Z]', line) and not TIME_RE.match(line) and 
                        not RT_RE.match(line) and len(line) > 5):
                        
                        # Save previous swimmer data before starting a new one
                        if current_entry and current_entry.get("split_50"): 
                            local_data.append(current_entry)
                        
                        current_entry = {
                            "Name": line.replace('\xa0', ' '),
                            "Club": lines[i+1] if i+1 < len(lines) else "",
                            "Link": link,
                            "Lane": None, "Rank": None, "Reaction Time": None, "Final Time": None
                        }
                        # Pre-fill segment split columns
                        for d in [50, 100, 150, 200, 250, 300, 350, 400]:
                            current_entry[f"split_{d}"] = None
                        
                        collecting_meta = True
                        continue

                    if current_entry:
                        # --- 2. Capture Splits ---
                        sm = SPLIT_RE.search(line)
                        if sm:
                            collecting_meta = False # Splits have started; stop metadata search
                            dist_num = int(sm.group('dist'))
                            
                            # The first 50m time is both the cumulative and the first split
                            if dist_num == 50:
                                current_entry["split_50"] = sm.group('cum_time')
                            
                            # The lap split for the NEXT 50m is on the line below the cumulative line
                            if i + 1 < len(lines):
                                next_line = lines[i+1]
                                if re.match(r"^\d:?\d{2}\.\d{2}$|^\d{2}\.\d{2}$", next_line):
                                    next_dist = dist_num + 50
                                    current_entry[f"split_{next_dist}"] = next_line
                            continue

                        # --- 3. Capture Metadata (Lane, Rank, RT, Final Time) ---
                        if collecting_meta:
                            if RT_RE.match(line):
                                current_entry["Reaction Time"] = line
                            elif TIME_RE.match(line) and not current_entry["Final Time"]:
                                current_entry["Final Time"] = line
                            elif line.isdigit():
                                val = int(line)
                                if not current_entry["Lane"]: current_entry["Lane"] = val
                                elif not current_entry["Rank"]: current_entry["Rank"] = val
                
                if current_entry and current_entry.get("split_50"): 
                    local_data.append(current_entry)
                    
    except Exception as e:
        print(f"Failed on {link}: {e}")
    return local_data