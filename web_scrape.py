"""
web_scraper.py
--------------
Scrapes OmegaTiming competition pages to collect PDF result links.

This module navigates the OmegaTiming results website and extracts
PDF links for the men's and women's 400m freestyle events.

Usage:
    from web_scraper import scrape_links

    df = scrape_links()

Process:
    1. Load the OmegaTiming results webpage
    2. Filter competitions by year
    3. Identify competitions containing swimming events
    4. Extract PDF result links for the 400m freestyle
    5. Return a structured dataset of competition metadata
       and PDF URLs

Output columns:
    year
    competition_name
    mens_400_free_pdf
    womens_400_free_pdf
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, NoSuchElementException
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import csv
import os
import time

#domain and url for the links w/ dropdown  of result
BASE_URL = "https://www.omegatiming.com"
url = f"{BASE_URL}/sports-timing-live-results"

#path to directory so that it stores the csv in data dirctory 
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)

OUT_CSV = os.path.join(DATA_DIR, "omega_pdfs.csv")

def ensure_csv():
    if not os.path.exists(OUT_CSV) or os.path.getsize(OUT_CSV) == 0:
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["year", "competition", "mens_400_free_pdf", "womens_400_free_pdf"])

def append_row(year, competition, mens_pdf, womens_pdf):
    with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([year, competition, mens_pdf or "", womens_pdf or ""])

class OmegaScraper:
    "the class that is initialized for the Webscraper"
    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")

        # Return once DOMContentLoaded fires (don’t wait for all images/fonts/etc)
        options.page_load_strategy = "eager"

        # Block common heavy assets (images already blocked in your code)
        options.add_experimental_option("prefs", {
            "profile.managed_default_content_settings.images": 2,
            "profile.managed_default_content_settings.stylesheets": 2,  # if layout breaks, set to 1
            "profile.managed_default_content_settings.fonts": 2,
            "profile.managed_default_content_settings.media": 2,
        })

        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36"
        )

        self.driver = webdriver.Chrome(options=options)

        #  8s is fine 
        self.wait = WebDriverWait(self.driver, 8)

    def switch_to_content(self):
        """Switches into the AppBody iframe where the data lives."""
        try:
            self.wait.until(EC.frame_to_be_available_and_switch_to_it((By.NAME, "AppBody")))
        except TimeoutException:
            # If the frame isn't found, we might already be in it or it's not required on this specific page
            pass

    def select_year(self, target_year):
        target = str(target_year)
        self.switch_to_content()
    
        try:
            # Snapshot something from the table BEFORE changing year
            before_href = None
            try:
                first_link = self.driver.find_element(By.CSS_SELECTOR, ".results-page .block-table .row h3.detail a")
                before_href = first_link.get_attribute("href")
            except Exception:
                pass
    
            dd = self.wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".select-selected")))
            dd.click()
    
            opt_xpath = f"//div[contains(@class,'select-items')]//div[normalize-space(text())='{target}']"
            opt = self.wait.until(EC.element_to_be_clickable((By.XPATH, opt_xpath)))
            self.driver.execute_script("arguments[0].click();", opt)
    
            # Confirm the dropdown label updated
            self.wait.until(lambda d: d.find_element(By.CSS_SELECTOR, ".select-selected").text.strip() == target)
    
            # Wait until the table actually refreshes (first link changes + includes /{year}/)
            def table_refreshed(d):
                try:
                    a = d.find_element(By.CSS_SELECTOR, ".results-page .block-table .row h3.detail a")
                    href = a.get_attribute("href") or ""
                    if f"/{target_year}/" not in href:
                        return False
                    if before_href and href == before_href:
                        return False
                    return True
                except Exception:
                    return False
    
            self.wait.until(table_refreshed)

        except Exception as e:
            print(f"Error selecting year {target}: {e}")
        finally:
            self.driver.switch_to.default_content()

    def get_comp_links(self, year):
        self.switch_to_content()
        rows = self.safe_find_rows(".results-page .block-table .row")
        links = []
        year_pattern = f"/{year}/"

        for r in rows:
            try:
                if "SWIMMING" not in r.text.upper(): 
                    continue
                
                a = r.find_element(By.CSS_SELECTOR, "h3.detail a")
                href = a.get_attribute("href")
                name = a.text.strip()
                
                # Verify the year is in the link to avoid pulling stale data from previous year
                if year_pattern in href:
                    links.append((name, href))
            except (NoSuchElementException, StaleElementReferenceException):
                continue
        
        self.driver.switch_to.default_content()
        return links

    def get_pdfs(self, link):
        self.driver.get(link)
    
        # more specific: wait for at least 1 row
        self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".page-wrapper .block-table .row")))
    
        results = {"men": None, "women": None}
        event_rows = self.safe_find_rows(".page-wrapper .block-table .row")
    
        for row in event_rows:
            try:
                text = row.find_element(By.CSS_SELECTOR, "p.round").text.upper()
                if "FREESTYLE" in text and "400" in text:
                    links = row.find_elements(By.CSS_SELECTOR, "p.two a[href]")
                    if len(links) >= 2:
                        pdf_url = links[1].get_attribute("href")
                        if "WOMEN" in text:
                            results["women"] = pdf_url
                        elif "MEN" in text:
                            results["men"] = pdf_url
            except NoSuchElementException:
                continue
    
        return results["men"], results["women"]

    def safe_find_rows(self, selector, retries=3):
        """Prevents stale element errors by retrying the find operation."""
        for i in range(retries):
            try:
                return self.driver.find_elements(By.CSS_SELECTOR, selector)
            except StaleElementReferenceException:
                #adds a cap on number of retries, just in case
                if i == retries - 1: raise
                time.sleep(0.5)

# ---------------------------------------------------- RUN ----------------------------------------------------
def get_csv(start, end):
    #initializes omegascraper class
    scraper = OmegaScraper()
    ensure_csv() # Using your existing helper
    
    try:
        scraper.driver.get(url)
        for year in range(start, end + 1): #iterate through years, inclusive
            scraper.select_year(year)
            competitions = scraper.get_comp_links(year)
            print(f"Year {year}: Found {len(competitions)} comps.")

            for name, link in competitions:
                try:
                    m, w = scraper.get_pdfs(link)
                    if m or w:
                        append_row(year, name, m, w)
                        print(f"  [SAVED] {name}")
                except Exception as e:
                    print(f"  [ERROR] {name}: {e}")
    finally:
        scraper.driver.quit() #closes the process out at the end
