"""
pdf.py
------
Runs the OmegaTiming PDF scraping and parsing pipeline.

This module coordinates the process of downloading and parsing
400m freestyle result PDFs.

Usage:
    import pdf

    df = pdf.scrape_omega(pdf_links)

Input:
    A list of OmegaTiming PDF URLs.

Process:
    1. Load an existing results CSV if present (resume support)
    2. Use multiprocessing to process PDFs in parallel
    3. Call the parser in single_pdf.py for each PDF
    4. Collect swimmer results into a unified dataset
    5. Periodically checkpoint results to disk

Output:
    pandas DataFrame with columns:
        heat, rank, lane, last_name, first_name, reaction_time,
        split_50m, split_100m, split_150m, split_200m,
        split_250m, split_300m, split_350m, final_time, Link
"""
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
    all_results = []
    processed_links = set()

    # Load existing data (resume)
    if os.path.exists(output_file):
        try:
            existing_df = pd.read_csv(output_file)
        except pd.errors.EmptyDataError:
            existing_df = pd.DataFrame()

        if not existing_df.empty and "Link" in existing_df.columns:
            processed_links = set(existing_df["Link"].dropna().astype(str).unique())
            all_results = existing_df.to_dict("records")
            print(f"Resuming: {len(processed_links)} PDFs already processed.")
        elif not existing_df.empty:
            print(f"Resume disabled: 'Link' column not found in {output_file}. Columns={list(existing_df.columns)}")
            all_results = existing_df.to_dict("records")

    remaining_links = [l for l in links if l not in processed_links]
    if not remaining_links:
        print("All links already processed.")
        return pd.DataFrame(all_results)

    completed_pdfs = 0

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_link = {
            executor.submit(spdf.process_single_link, link): link
            for link in remaining_links
        }

        for future in tqdm(as_completed(future_to_link), total=len(remaining_links), desc="Scraping"):
            link = future_to_link[future]
            try:
                df = future.result()  # returns DataFrame (possibly empty) 
                if df is not None:
                    print(link, "rows:", len(df))
                if df is None or df.empty:
                    continue

                rows = df.to_dict("records")
                for r in rows:
                    r["Link"] = link

                all_results.extend(rows)
                completed_pdfs += 1

                # Save every 10 PDFs successfully parsed
                if completed_pdfs % 10 == 0:
                    pd.DataFrame(all_results).to_csv(output_file, index=False)

            except Exception as e:
                print(f"Error on {link}: {e}")

    final_df = pd.DataFrame(all_results)
    final_df.to_csv(output_file, index=False)
    return final_df