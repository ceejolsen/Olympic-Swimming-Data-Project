import os
import pandas as pd
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import single_pdf as spdf

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

def scrape_omega(links, output_file="scraped_results.csv", max_workers=4):
    """
    Scrapes PDFs with a live progress bar and checkpointing to avoid redundant work.
    """
    all_results = []
    processed_links = set()

    # Load existing data to see what we've already done
    if os.path.exists(output_file):
        existing_df = pd.read_csv(output_file)
        if not existing_df.empty:
            processed_links = set(existing_df['Link'].unique())
            all_results = existing_df.to_dict('records')
            print(f"Resuming: {len(processed_links)} PDFs already processed.")

    # Filter out links we have already finished
    remaining_links = [l for l in links if l not in processed_links]
    
    if not remaining_links:
        print("All links already processed.")
        return pd.DataFrame(all_results)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_link = {executor.submit(spdf.process_single_link, link): link for link in remaining_links}
        
        # Live percentage bar via tqdm
        for future in tqdm(as_completed(future_to_link), total=len(remaining_links), desc="Scraping"):
            try:
                result = future.result()
                if result:
                    all_results.extend(result)
                    # Incremental save every 10 PDFs to protect data
                    if len(all_results) % 10 == 0:
                        pd.DataFrame(all_results).to_csv(output_file, index=False)
            except Exception as e:
                print(f"Error on {future_to_link[future]}: {e}")

    # Final save and return
    final_df = pd.DataFrame(all_results)
    final_df.to_csv(output_file, index=False)
    return final_df