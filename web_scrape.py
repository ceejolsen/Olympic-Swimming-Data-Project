from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
import csv
import undetected_chromedriver as uc
import os
import time

BASE_URL = "https://www.omegatiming.com"
options = uc.ChromeOptions()
prefs = {"profile.managed_default_content_settings.images": 2}
options.add_experimental_option("prefs", prefs) 
options.add_argument("--window-size=1920,1080")

driver = uc.Chrome(
    options=options, 
    headless=True, 
    version_main=145
)

url = "https://www.omegatiming.com/sports-timing-live-results"
driver.delete_all_cookies()
driver.get(url)
wait = WebDriverWait(driver, 15)

#path to directory so that it stores the csv in data dirctory 
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))

DATA_DIR = os.path.join(PROJECT_ROOT, "data")
os.makedirs(DATA_DIR, exist_ok=True)

OUT_CSV = os.path.join(DATA_DIR, "omega_pdfs.csv")


# ---------------------------------------------------- CSV helpers --------------------------------------------------
def ensure_csv():
    if not os.path.exists(OUT_CSV) or os.path.getsize(OUT_CSV) == 0:
        with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["year", "competition", "mens_400_free_pdf", "womens_400_free_pdf"])

def append_row(year, competition, mens_pdf, womens_pdf):
    with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([year, competition, mens_pdf or "", womens_pdf or ""])



# ---------------------------------------------------- Year selection ----------------------------------------------------
def select_year(target_year: int):
    target = str(target_year)

    dd = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".select-selected")))
    dd.click()

    opt = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, f"//div[contains(@class,'select-items')]//div[normalize-space(text())='{target}']")
        )
    )
    driver.execute_script("arguments[0].scrollIntoView(true);", opt)
    driver.execute_script("arguments[0].click();", opt)

    wait.until(lambda d: d.find_element(By.CSS_SELECTOR, ".select-selected").text.strip() == target)

    year_path = f"/{target}/"
    try:
        # This approach is more robust against staleness
        element = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".results-page .block-table .row h3.detail a[href]")))
        href = element.get_attribute("href")
        
        if year_path not in href:
            # Handle the logic if it's the wrong year
            pass
    except StaleElementReferenceException:
        # If it goes stale, just try one more time
        return select_year(target_year)
        



# ---------------------------------------------------- Competition list ----------------------------------------------------
def get_swimming_competitions_for_year(target_year: int):
    year_path = f"/{target_year}/"
    rows = driver.find_elements(By.CSS_SELECTOR, ".results-page .block-table .row")

    comps = []
    seen = set()

    for r in rows:
        try:
            if "SWIMMING" not in r.text.upper():
                continue

            a = r.find_element(By.CSS_SELECTOR, "h3.detail a[href]")
            href = a.get_attribute("href") or ""
            name = a.text.strip()

            if href.startswith("/"):
                href = BASE_URL + href

            if year_path not in href:
                continue

            if href not in seen:
                seen.add(href)
                comps.append((name, href))
        except:
            continue

    return comps



# ---------------------------------------------------- 400 Free PDF extraction (your new part) ----------------------------------------------------
def get_400_free_result_pdfs_from_comp_page():
    mens_pdf = None
    womens_pdf = None

    # each event is packaged in a div.row
    event_rows = driver.find_elements(By.CSS_SELECTOR, ".page-wrapper .block-table .row")

    for row in event_rows:
        try:
            round_el = row.find_element(By.CSS_SELECTOR, "p.round")
            t = round_el.text.upper()

            if "FREESTYLE" not in t or "400" not in t:
                continue

            # there are TWO p.two blocks; we want the SECOND (total ranking/results)
            two_links = row.find_elements(By.CSS_SELECTOR, "p.two a[href]")
            if len(two_links) < 2:
                continue

            href = two_links[1].get_attribute("href") or ""
            if href.startswith("/"):
                href = BASE_URL + href

            if "WOMEN" in t:
                womens_pdf = href
            elif "MEN" in t:
                mens_pdf = href

        except:
            continue

    return mens_pdf, womens_pdf


# ---------------------------------------------------- RUN ----------------------------------------------------
def get_csv(start_year, end_year):
    ensure_csv()
    for year in range(start_year, end_year):
        try:
            driver.get(url)
            select_year(year)
            
            comps = get_swimming_competitions_for_year(year)
            print(f"\n====== target={year} ======")
            
            for comp_name, comp_link in comps:
                try:
                    # Open new tab
                    driver.execute_script("window.open(arguments[0], '_blank');", comp_link)
                    
                    # Wait for the new handle to actually exist
                    wait.until(lambda d: len(d.window_handles) > 1)
                    
                    # Switch to the newest tab
                    all_handles = driver.window_handles
                    driver.switch_to.window(all_handles[-1])
                    
                    # Wait for page content
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".page-wrapper")))
                    
                    mens_pdf, womens_pdf = get_400_free_result_pdfs_from_comp_page()
                    
                    if mens_pdf or womens_pdf:
                        append_row(year, comp_name, mens_pdf, womens_pdf)
                        print(f"[SAVE] {comp_name}")
                    else:
                        print(f"[SKIP] {comp_name}")

                except Exception as e:
                    print(f"[WARN] Error on comp {comp_name}: {e}")
                
                finally:
                    # ROBUST TAB CLOSING:
                    # If we have more than one tab, close the current and go back to index 0
                    handles = driver.window_handles
                    if len(handles) > 1:
                        driver.close()
                        driver.switch_to.window(handles[0])
                    else:
                        # If for some reason the main tab was closed, restart the driver or refresh
                        driver.switch_to.window(driver.window_handles[0])

        except Exception as e:
            print(f"Critical error for year {year}: {e}")
            # If the window is totally lost, we break or restart
            if "no such window" in str(e).lower():
                print("Window lost. Attempting to recover...")
                driver.get(url) # Re-navigate to main page
                continue

    
    input("Press Enter to close...")
    driver.quit()
    return os.path.abspath(OUT_CSV)