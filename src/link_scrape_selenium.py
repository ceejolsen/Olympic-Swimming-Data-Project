"""from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
url = 'https://www.omegatiming.com/sports-timing-live-results'
driver = webdriver.Chrome()
driver.get(url)
#input("Press Enter to close the browser...")
#driver.quit()

#<div class="select-selected">Year</div>
#div class="select-items select-hide"><div>2026</div><div>2025</div><div>2024</div><div>2023</div><div>2022</div><div>2021</div><div>2020</div><div>2019</div><div>2018</div><div>2017</div><div>2016</div><div>2015</div><div>2014</div><div>2013</div><div>2012</div><div>2011</div><div>2010</div><div>2009</div><div>2008</div><div>2007</div><div>2006</div><div>2005</div><div>2004</div><div>2003</div><div>2002</div><div>2001</div><div>2000</div></div>

wait = WebDriverWait(driver, 15)

def open_year_dropdown():
    dd = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".select-selected")))
    dd.click()

def select_year(target_year: int):
    target = str(target_year)

    # Open dropdown
    dd = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, ".select-selected")))
    dd.click()

    # Click the exact year option
    opt = wait.until(
        EC.element_to_be_clickable(
            (By.XPATH, f"//div[contains(@class,'select-items')]//div[normalize-space(text())='{target}']")
        )
    )
    driver.execute_script("arguments[0].scrollIntoView(true);", opt)
    driver.execute_script("arguments[0].click();", opt)

    # Wait until the dropdown label updates to the chosen year
    wait.until(lambda d: d.find_element(By.CSS_SELECTOR, ".select-selected").text.strip() == target)

    # Wait until the results list reflects that year (content refresh, not element replacement)
    year_path = f"/{target}/"
    try:
        wait.until(lambda d: year_path in d.find_element(
            By.CSS_SELECTOR, ".results-page .block-table .row h3.detail a[href]"
        ).get_attribute("href"))
    except TimeoutException:
        # fallback: wait until ANY link in table contains that year
        wait.until(lambda d: any(
            year_path in (a.get_attribute("href") or "")
            for a in d.find_elements(By.CSS_SELECTOR, ".results-page .block-table .row h3.detail a[href]")
        ))
def get_swimming_competitions_for_year(target_year: int):
    year_path = f"/{target_year}/"

    rows = driver.find_elements(By.CSS_SELECTOR, ".results-page .block-table .row")

    comps = []
    seen = set()

    for r in rows:
        try:
            # keep only rows that say SWIMMING somewhere in the row text (we'll tighten to exact sport cell later)
            if "SWIMMING" not in r.text.upper():
                continue

            a = r.find_element(By.CSS_SELECTOR, "h3.detail a[href]")
            href = a.get_attribute("href") or ""
            name = a.text.strip()

            # normalize to absolute
            if href.startswith("/"):
                href = BASE_URL + href

            # CRITICAL: enforce year match using href
            if year_path not in href:
                continue

            if href not in seen:
                seen.add(href)
                comps.append((name, href))
        except:
            continue

    return comps

for year in range(2010, 2027):
    select_year(year)

    # sanity check: confirm UI year matches
    ui_year = driver.find_element(By.CSS_SELECTOR, ".select-selected").text.strip()
    print(f"\n====== target={year} ui={ui_year} ======")

    comps = get_swimming_competitions_for_year(year)
    print("Swimming competitions found:", len(comps))
    for name, link in comps:
        print(name, "=>", link)

input("Press Enter to close...")
driver.quit()




#<div class=​"page-wrapper">​
#<div class=​"block-table">​
#<div class=​"row">​…​</div>​
#<p class=​"round">​Women's Freestyle 400m   Heats​</p>​
#<div class=​"other">​
#<p class=​"two">​
#<a href=​"/​File/​00011A00000101EF0101FFFFFFFFFF01.pdf" target=​"_blank">​…​</a>​
#there are two blocks of class = two  i only want the second one not the first one  the first one is the start list the second is the total ranking pdf
#<p class=​"round">​Men's Freestyle 400m   Heats​</p>​
#<p class=​"two">​
#<a href=​"/​File/​00011A00000101EF0101FFFFFFFFFF01.pdf" target=​"_blank">​…​</a>​
#there are two blocks of class = two  i only want the second one not the first one  the first one is the start list the second is the total ranking pdf



#<div class="results-page">
#<div class="page-wrapper">
#<div class="block-table">
#<div class="row">
#<h3 class="detail">
#<a href="/2025/2025-tyr-pro-swim-series01-live-results">…</a>"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support import expected_conditions as EC
import csv
import os


#domain and url for the links w/ dropdown  of result
BASE_URL = "https://www.omegatiming.com"
url = f"{BASE_URL}/sports-timing-live-results"

#initilaize chrome webdriver be able to navigate the result page and a wait objects so elements can load
driver = webdriver.Chrome()
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
        wait.until(lambda d: year_path in d.find_element(
            By.CSS_SELECTOR, ".results-page .block-table .row h3.detail a[href]"
        ).get_attribute("href"))
    except TimeoutException:
        wait.until(lambda d: any(
            year_path in (a.get_attribute("href") or "")
            for a in d.find_elements(By.CSS_SELECTOR, ".results-page .block-table .row h3.detail a[href]")
        ))



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
ensure_csv()

for year in range(2010, 2027):

    driver.get(url)              # always start from main page
    select_year(year)            # then select year

    ui_year = driver.find_element(By.CSS_SELECTOR, ".select-selected").text.strip()
    print(f"\n====== target={year} ui={ui_year} ======")

    comps = get_swimming_competitions_for_year(year)
    print("Swimming competitions found:", len(comps))

    main_tab = driver.current_window_handle

    for comp_name, comp_link in comps:
        try:
            # open competition in new tab
            driver.execute_script("window.open(arguments[0], '_blank');", comp_link)
            driver.switch_to.window(driver.window_handles[-1])

            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            mens_pdf, womens_pdf = get_400_free_result_pdfs_from_comp_page()

            if mens_pdf or womens_pdf:
                append_row(year, comp_name, mens_pdf, womens_pdf)
                print(f"[SAVE] {comp_name} | M={'Y' if mens_pdf else 'N'} W={'Y' if womens_pdf else 'N'}")
            else:
                print(f"[SKIP] {comp_name} (no 400 free PDFs found)")

        except Exception as e:
            print(f"[WARN] Failed comp page: {comp_name} | {e}")

        finally:
            # close comp tab and return
            if driver.current_window_handle != main_tab:
                driver.close()
                driver.switch_to.window(main_tab)

input("Press Enter to close...")
driver.quit()