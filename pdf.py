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
    """
    Convert a race time value into total seconds.

    Supports both:
    - mm:ss.ss format (example: 3:46.88)
    - regular seconds format (example: 26.39)

    Arguments:
        time_val: A string, number, or pandas value representing time.

    Returns:
        float: Time in seconds rounded to 2 decimals, or None if invalid.
    """
    if not time_val or pd.isna(time_val): return None
    try:
        if ":" in str(time_val):
            m, s = str(time_val).split(":")
            return round(int(m) * 60 + float(s), 2)
        return round(float(time_val), 2)
    except: return None

def get_links_df(file: pd.DataFrame) -> list:
    """
    Extract men's and women's 400m freestyle PDF links from a dataframe.

    Expected input columns:
    - mens_400_free_pdf
    - womens_400_free_pdf

    Argument:
        file (pd.DataFrame): DataFrame containing OmegaTiming PDF link columns.

    Returns:
        tuple[list, list]:
            - mens_links: list of men's PDF URLs
            - womens_links: list of women's PDF URLs

        If the input is not a DataFrame, returns (None, None).
    """
    if not isinstance(file, pd.DataFrame):
        return None, None
    mens_links = file['mens_400_free_pdf'].dropna().tolist()
    womens_links = file['womens_400_free_pdf'].dropna().tolist()
    return mens_links, womens_links

def scrape_omega(links, output_file="data/scraped_results.csv", max_workers=4):
    """
    Parse a list of OmegaTiming PDF links into a unified race results dataframe.

    This function will complete this behavior:
    if an output CSV already exists and contains a 'Link' column, links that
    were already processed will be skipped.

    Arguments:
        links (list[str]): List of OmegaTiming PDF URLs to parse.
        output_file (str): CSV file used for checkpointing and final output.
        max_workers (int): Number of worker processes for multiprocessing.

    Returns:
        pd.DataFrame: Combined parsed results from all processed PDFs.
    """
    
    all_results = []
    processed_links = set()


    # If a previous output file exists, 
    #load it so we can skip links we already processed in eariler runs.
    
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

    #Process PDFs in parallel using multiple worker processes.
    #Each worker calls single_pdf.process_single_link(link)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        future_to_link = {
            executor.submit(spdf.process_single_link, link): link
            for link in remaining_links
        }

        for future in tqdm(as_completed(future_to_link), total=len(remaining_links), desc="Scraping"):
            link = future_to_link[future]
            try:
                #each completed future returns a list of parsed swimmer rows
                rows = future.result()   

                if rows is not None:
                    print(link, "rows:", len(rows))

                #skip pdfs that produce no rows
                if not rows:
                    continue

                #tag each parsed row with its source pdf link
                for r in rows:
                    r["Link"] = link

                #add parsed rows to the global result list
                all_results.extend(rows)
                completed_pdfs += 1

                #save a checkpoint every 10 succesfully processed pdfs
                if completed_pdfs % 10 == 0:
                    pd.DataFrame(all_results).to_csv(output_file, index=False)
            
            except Exception as e:
                print(f"Error on {link}: {e}")

    #final save after all the links are processed
    final_df = pd.DataFrame(all_results)
    final_df.to_csv(output_file, index=False)
    return final_df