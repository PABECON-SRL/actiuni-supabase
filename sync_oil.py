import pandas as pd
import requests
import os
import sys

# Preluăm variabilele
URL = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# DEBUG: Verificăm dacă variabilele au ajuns în script
if not URL:
    print("EROARE: SUPABASE_URL este gol! Verifică YAML-ul și numele Secretului.")
    sys.exit(1)
if not KEY:
    print("EROARE: SUPABASE_SERVICE_ROLE_KEY este gol!")
    sys.exit(1)

BASE_URL = URL.strip().rstrip('/')
HEADERS = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def sync_data():
    xl_url = "https://energy.ec.europa.eu/document/download/906e60ca-8b6a-44e7-8589-652854d2fd3f_en?filename=Weekly_Oil_Bulletin_Prices_History_maticni_4web.xlsx"
    
    print(f"Sincronizare pornită către: {BASE_URL}")
    
    try:
        # Obținem tipurile de combustibil
        r = requests.get(f"{BASE_URL}/rest/v1/fuel_types?select=id,slug", headers=HEADERS)
        r.raise_for_status()
        f_map = {item['slug']: item['id'] for item in r.json()}
        
        # Procesare Excel (Prices with taxes)
        df = pd.read_excel(xl_url, sheet_name="Prices with taxes", header=None)
        countries = ["EU_", "EUR_", "AT_", "BE_", "BG_", "CY_", "CZ_", "DE_", "DK_", "EE_", "EL_", "ES_", "FI_", "FR_", "HR_", "HU_", "IE_", "IT_", "LT_", "LU_", "LV_", "MT_", "NL_", "PL_", "PT_", "RO_", "SE_", "SI_", "SK_"]
        fuel_offsets = {1: 'euro_95', 2: 'diesel', 3: 'heating_oil', 4: 'fuel_oil_low_sulphur', 5: 'fuel_oil_high_sulphur', 6: 'lpg'}

        payload = []
        for _, row in df.tail(10).iterrows():
            date = str(row[0]).split()[0]
            if "-" not in date: continue
            
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
                                "price_with_tax": float(val)
                            })

        if payload:
            res = requests.post(f"{BASE_URL}/rest/v1/fuel_prices", json=payload, headers=HEADERS)
            print(f"Status: {res.status_code}. Date inserate: {len(payload)}")
        else:
            print("Nu s-au găsit date noi.")

    except Exception as e:
        print(f"Eroare: {e}")
        sys.exit(1)

if __name__ == "__main__":
    sync_data()
