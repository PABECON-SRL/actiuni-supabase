import pandas as pd
import requests
import os
import sys

# Preluăm variabilele din mediul GitHub
URL = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# VERIFICARE CRITICĂ
if not URL or not KEY:
    print("-" * 50)
    print("EROARE: GitHub Actions NU a transmis secretele către Python!")
    print(f"SUPABASE_URL detectat: {'DA' if URL else 'NU (GOL)'}")
    print(f"SUPABASE_KEY detectat: {'DA' if KEY else 'NU (GOL)'}")
    print("-" * 50)
    sys.exit(1)

# Curățăm URL-ul de eventuale spații sau slash-uri la final
BASE_URL = URL.strip().rstrip('/')
HEADERS = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def sync_data():
    xl_url = "https://energy.ec.europa.eu/document/download/906e60ca-8b6a-44e7-8589-652854d2fd3f_en?filename=Weekly_Oil_Bulletin_Prices_History_maticni_4web.xlsx"
    
    print(f"Conectare la baza de date: {BASE_URL}")
    
    try:
        # 1. Testăm conexiunea prin preluarea ID-urilor
        r = requests.get(f"{BASE_URL}/rest/v1/fuel_types?select=id,slug", headers=HEADERS)
        r.raise_for_status()
        f_map = {item['slug']: item['id'] for item in r.json()}
        
        # 2. Procesăm Excel-ul
        df = pd.read_excel(xl_url, sheet_name="Prices with taxes", header=None)
        countries = ["EU_", "EUR_", "AT_", "BE_", "BG_", "CY_", "CZ_", "DE_", "DK_", "EE_", "EL_", "ES_", "FI_", "FR_", "HR_", "HU_", "IE_", "IT_", "LT_", "LU_", "LV_", "MT_", "NL_", "PL_", "PT_", "RO_", "SE_", "SI_", "SK_"]
        fuel_offsets = {1: 'euro_95', 2: 'diesel', 3: 'heating_oil', 4: 'fuel_oil_low_sulphur', 5: 'fuel_oil_high_sulphur', 6: 'lpg'}

        payload = []
        # Luăm ultimele 10 rânduri (cele mai recente săptămâni)
        for _, row in df.tail(10).iterrows():
            date = str(row[0]).split()[0]
            if len(date) < 10 or "-" not in date: continue
            
            for col_idx, cell in enumerate(row):
                ctr = str(cell).strip()
                if ctr in countries:
                    for off, slug in fuel_offsets.items():
                        val = row[col_idx + off]
                        if pd.notnull(val) and isinstance(val, (int, float)) and val > 0:
                            payload.append({
                                "report_date": date,
                                "country_code": ctr,
                                "fuel_id": f_map[slug],
                                "price_with_tax": float(val),
                                "currency": "EUR"
                            })

        if payload:
            res = requests.post(f"{BASE_URL}/rest/v1/fuel_prices", json=payload, headers=HEADERS)
            print(f"Succes! Status: {res.status_code}. Date trimise: {len(payload)}")
        else:
            print("Nu s-au găsit date noi în Excel.")

    except Exception as e:
        print(f"Eroare procesare: {e}")
        sys.exit(1)

if __name__ == "__main__":
    sync_data()
