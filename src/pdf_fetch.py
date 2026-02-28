import csv
from pathlib import Path
import requests

SEEDS = Path("data/omega_pdfs.csv")
OUT_DIR = Path("pdfs")

def main():
    if not SEEDS.exists():
        print("Seed CSV not found.")
        return

    OUT_DIR.mkdir(exist_ok=True)

    with SEEDS.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        print("No rows found in CSV.")
        return

    for row in rows:
        url = row["pdf_url"].strip()
        if not url:
            continue

        filename = url.split("/")[-1]
        out_path = OUT_DIR / filename

        print(f"Downloading {filename}...")

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            with open(out_path, "wb") as f:
                f.write(response.content)

            print("Success!")
        except Exception as e:
            print(f"Failed: {e}")

if __name__ == "__main__":
    main()
