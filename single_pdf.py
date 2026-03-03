import pdfplumber
import re
import pandas as pd
import numpy as np
from tqdm import tqdm
import io
import curl_cffi as requests


def process_single_link(pdf_path):
    data = []
    # Regex to find cumulative splits: looking for '100m (1) 55.02'
    # It captures the distance and the first time following it (the cumulative one)
    split_pattern = re.compile(r'(\d{2,3})m\s*\(\d\)\s*(\d{1,2}:?\d{2}\.\d{2})')
    time_pattern = re.compile(r'\d{1,2}:\d{2}\.\d{2}|\d{2}\.\d{2}')

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            # Get all text objects with their coordinates
            words = page.extract_words(extra_attrs=["top", "bottom", "x0", "x1"])
            print(words)
            
            # Group words into lines by their top coordinate (approximate for jitter)
            lines = []
            if not words: continue
            
            # Sort words top-to-bottom, then left-to-right
            words.sort(key=lambda x: (x['top'], x['x0']))
            
            # Simple line grouping logic
            current_line_y = words[0]['top']
            current_line = []
            all_lines = []
            
            for w in words:
                if abs(w['top'] - current_line_y) < 3: # 3pt tolerance for same line
                    current_line.append(w)
                else:
                    all_lines.append(current_line)
                    current_line = [w]
                    current_line_y = w['top']
            all_lines.append(current_line)

            for i, line_words in enumerate(all_lines):
                line_text = " ".join([w['text'] for w in line_words])
                
                # Identify a swimmer row: Usually starts with a Rank/Lane or a Name
                # We look for the "Time" at the end of a line as an anchor
                times_in_line = time_pattern.findall(line_text)
                
                # Logic: If line has a Name-like structure and a Final Time
                if len(times_in_line) >= 1 and any(char.isupper() for char in line_text):
                    # We found a primary swimmer line
                    swimmer = {
                        "Name": "", "Lane": np.nan, "RT": np.nan, "Final": times_in_line[-1],
                        "50m": np.nan, "100m": np.nan, "150m": np.nan, "200m": np.nan,
                        "250m": np.nan, "300m": np.nan, "350m": np.nan
                    }
                    
                    # 1. Extract R.T. (usually a 0.xx number)
                    rt_match = re.search(r'0\.\d{2}', line_text)
                    if rt_match:
                        swimmer["RT"] = rt_match.group()

                    # 2. Extract Lane (Look at the first few words of the line)
                    # Often the 1st or 2nd word is Lane/Rank
                    digit_words = [w['text'] for w in line_words if w['text'].isdigit()]
                    if len(digit_words) >= 2:
                        swimmer["Lane"] = digit_words[0] # Adjust index based on specific PDF layout
                    
                    # 3. Extract Name (Words that are mostly uppercase)
                    name_parts = [w['text'] for w in line_words if any(c.isalpha() for c in w['text']) 
                                  and w['text'] not in ["R.T.", "Time", "FINA"]]
                    swimmer["Name"] = " ".join(name_parts[:3]) # Simplistic name capture

                    # 4. Look at the NEXT line for Splits
                    if i + 1 < len(all_lines):
                        next_line_text = " ".join([w['text'] for w in all_lines[i+1]])
                        splits = split_pattern.findall(next_line_text)
                        for dist, val in splits:
                            if f"{dist}m" in swimmer:
                                swimmer[f"{dist}m"] = val
                    
                    # Only add if it's not a header or record line
                    if swimmer["Name"] and not any(x in swimmer["Name"] for x in ["WR", "OR", "AR", "WC"]):
                        data.append(swimmer)
                        
    return pd.DataFrame(data)