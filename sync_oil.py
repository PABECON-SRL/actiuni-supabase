import pandas as pd
import requests
import os
import sys

URL = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not URL or not KEY:
    print("-" * 50)
    print("EROARE: GitHub Actions NU a transmis secretele!")
    print(f"URL: {'OK' if URL else 'GOL'}")
    print(f"KEY: {'OK' if KEY else 'GOL'}")
    print("-" * 50)
    sys.exit(1)

# Curățăm URL-ul pentru a evita dublu slash //
BASE_URL = URL.strip().rstrip('/')
HEADERS = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def sync_data():
    xl_url = "https://energy.ec.europa.eu/document/download/906e60ca-8b6a-44e7-8589-652854d2fd3f_en?filename=Weekly_Oil_Bulletin_Prices_History_maticni_4web.xlsx"
    
    # Preluăm ID-urile combustibililor
    r = requests.get(f"{BASE_URL}/rest/v1/fuel_types?select=id,slug", headers=HEADERS)
    r.raise_for_status()
    f_map = {item['slug']: item['id'] for item in r.json()}
    
    countries = ["EU_", "EUR_", "AT_", "BE_", "BG_", "CY_", "CZ_", "DE_", "DK_", "EE_", "EL_", "ES_", "FI_", "FR_", "HR_", "HU_", "IE_", "IT_", "LT_", "LU_", "LV_", "MT_", "NL_", "PL_", "PT_", "RO_", "SE_", "SI_", "SK_"]
    fuel_offsets = {1: 'euro_95', 2: 'diesel', 3: 'heating_oil', 4: 'fuel_oil_low_sulphur', 5: 'fuel_oil_high_sulphur', 6: 'lpg'}

    df = pd.read_excel(xl_url, sheet_name="Prices with taxes", header=None)
    payload = []
    
    # Analizăm ultimele 15 rânduri
    for _, row in df.tail(15).iterrows():
        date = str(row[0]).split()[0]
        if len(date) < 10 or "-" not in date: continue
        
        for col_idx, cell in enumerate(row):
            clean_ctr = str(cell).strip()
            if clean_ctr in countries:
                for off, slug in fuel_offsets.items():
                    val = row[col_idx + off]
                    if pd.notnull(val) and isinstance(val, (int, float)) and val > 0:
                        payload.append({
                            "report_date": date,
                            "country_code": clean_ctr,
                            "fuel_id": f_map[slug],
                            "price_with_tax": float(val)
                        })

    if payload:
        res = requests.post(f"{BASE_URL}/rest/v1/fuel_prices", json=payload, headers=HEADERS)
        print(f"Succes! Status: {res.status_code}. Date: {len(payload)}")

if __name__ == "__main__":
    sync_data()
