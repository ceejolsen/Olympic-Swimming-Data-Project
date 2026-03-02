import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException
import csv
import os
import time

BASE_URL = "https://www.omegatiming.com"
# We will define these inside the function to ensure they are fresh
driver = None
wait = None
OUT_CSV = ""

# ---------------------------------------------------- CSV helpers --------------------------------------------------
def ensure_csv(csv_path):
    if not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["year", "competition", "mens_400_free_pdf", "womens_400_free_pdf"])

def append_row(csv_path, year, competition, mens_pdf, womens_pdf):
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([year, competition, mens_pdf or "", womens_pdf or ""])

# ---------------------------------------------------- Year selection ----------------------------------------------------
def select_year(target_year: int, local_driver, local_wait):
    target = str(target_year)
    dd = local_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".select-selected")))
    dd.click()

    opt = local_wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, f"//div[contains(@class,'select-items')]//div[normalize-space(text())='{target}']")
        )
    )
    local_driver.execute_script("arguments[0].scrollIntoView(true);", opt)
    local_driver.execute_script("arguments[0].click();", opt)

    local_wait.until(lambda d: d.find_element(By.CSS_SELECTOR, ".select-selected").text.strip() == target)

# ---------------------------------------------------- Competition list ----------------------------------------------------
def get_swimming_competitions_for_year(target_year: int, local_driver):
    year_path = f"/{target_year}/"
    rows = local_driver.find_elements(By.CSS_SELECTOR, ".results-page .block-table .row")
    comps = []
    seen = set()

    for r in rows:
        try:
            if "SWIMMING" not in r.text.upper():
                continue
            a = r.find_element(By.CSS_SELECTOR, "h3.detail a[href]")
            href = a.get_attribute("href") or ""
            name = a.text.strip()
            if href.startswith("/"): href = BASE_URL + href
            if year_path in href and href not in seen:
                seen.add(href)
                comps.append((name, href))
        except:
            continue
    return comps

# ---------------------------------------------------- 400 Free PDF extraction ----------------------------------------------------
def get_400_free_result_pdfs_from_comp_page(local_driver):
    mens_pdf, womens_pdf = None, None
    event_rows = local_driver.find_elements(By.CSS_SELECTOR, ".page-wrapper .block-table .row")

    for row in event_rows:
        try:
            round_el = row.find_element(By.CSS_SELECTOR, "p.round")
            t = round_el.text.upper()
            if "FREESTYLE" not in t or "400" not in t:
                continue

            two_links = row.find_elements(By.CSS_SELECTOR, "p.two a[href]")
            if len(two_links) < 2: continue
            
            href = two_links[1].get_attribute("href") or ""
            if href.startswith("/"): href = BASE_URL + href

            if "WOMEN" in t: womens_pdf = href
            elif "MEN" in t: mens_pdf = href
        except:
            continue
    return mens_pdf, womens_pdf

# ---------------------------------------------------- RUN ----------------------------------------------------
def get_csv(start_year, end_year):
    # 1. Setup paths
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
    DATA_DIR = os.path.join(PROJECT_ROOT, "data")
    os.makedirs(DATA_DIR, exist_ok=True)
    csv_path = os.path.join(DATA_DIR, "omega_pdfs.csv")
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 50: 
        user_input = input(f"Found existing data at {csv_path}. Run anyway? (Y/N): ").strip().upper()
        if user_input != 'Y':
            print("Operation cancelled by user.")
        return os.path.abspath(csv_path)
    ensure_csv(csv_path)

    # 2. Setup Driver
    options = uc.ChromeOptions()
    options.add_argument("--window-size=1920,1080")
    options.add_argument('--no-sandbox')
    # Use a specific version_main only if the error persists
    local_driver = uc.Chrome(options=options, headless=True, version_main=145)
    local_wait = WebDriverWait(local_driver, 15)
    
    url = "https://www.omegatiming.com/sports-timing-live-results"
    
    try:
        for year in range(start_year, end_year):
            print(f"\n====== target={year} ======")
            local_driver.get(url)
            
            try:
                select_year(year, local_driver, local_wait)
                local_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".results-page .block-table")))
                
                comps = get_swimming_competitions_for_year(year, local_driver)
                
                for comp_name, comp_link in comps:
                    try:
                        local_driver.get(comp_link)
                        local_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".page-wrapper .block-table")))
                        
                        m_pdf, w_pdf = get_400_free_result_pdfs_from_comp_page(local_driver)
                        
                        if m_pdf or w_pdf:
                            append_row(csv_path, year, comp_name, m_pdf, w_pdf)
                            print(f"[SAVE] {comp_name}")
                        else:
                            print(f"[SKIP] {comp_name}")
                    except Exception as e:
                        print(f"[WARN] Error on comp {comp_name}: {e}")

            except Exception as e:
                print(f"Critical error for year {year}: {e}")

    finally:
        local_driver.quit()
    
    return os.path.abspath(csv_path)