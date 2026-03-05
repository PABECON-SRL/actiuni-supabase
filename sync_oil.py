import pandas as pd
import requests
import os
import sys

# Preluăm variabilele de mediu
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Validare de siguranță
if not SUPABASE_URL or not SUPABASE_KEY:
    print("!!! EROARE CRITICĂ: SUPABASE_URL sau SUPABASE_SERVICE_ROLE_KEY nu sunt configurate în GitHub Secrets.")
    sys.exit(1)

# Asigurăm formatul corect al URL-ului
SUPABASE_URL = SUPABASE_URL.strip().rstrip('/')

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def sync_data():
    url = "https://energy.ec.europa.eu/document/download/906e60ca-8b6a-44e7-8589-652854d2fd3f_en?filename=Weekly_Oil_Bulletin_Prices_History_maticni_4web.xlsx"
    
    print(f"Încerc conectarea la Supabase: {SUPABASE_URL}")
    
    try:
        # 1. Obținem ID-urile tipurilor de combustibil
        r_fuel = requests.get(f"{SUPABASE_URL}/rest/v1/fuel_types?select=id,slug", headers=HEADERS)
        r_fuel.raise_for_status()
        fuel_id_map = {item['slug']: item['id'] for item in r_fuel.json()}
        
        fuel_cols = {1: 'euro_95', 2: 'diesel', 3: 'heating_oil', 4: 'fuel_oil_low_sulphur', 5: 'fuel_oil_high_sulphur', 6: 'lpg'}
        target_countries = ["EU_", "EUR_", "AT_", "BE_", "BG_", "CY_", "CZ_", "DE_", "DK_", "EE_", "EL_", "ES_", "FI_", "FR_", "HR_", "HU_", "IE_", "IT_", "LT_", "LU_", "LV_", "MT_", "NL_", "PL_", "PT_", "RO_", "SE_", "SI_", "SK_"]

        print("Descarc și procesez Excel-ul...")
        # Citim doar tab-ul de prețuri
        df_prices = pd.read_excel(url, sheet_name="Prices with taxes", header=None)
        
        price_payload = []
        # Procesăm ultimele 15 rânduri
        for _, row in df_prices.tail(15).iterrows():
            date_raw = str(row[0]).split()[0]
            if len(date_raw) < 10 or "-" not in date_raw:
                continue
            
            for col_idx, cell in enumerate(row):
                clean_ctr = str(cell).strip()
                if clean_ctr in target_countries:
                    for offset, slug in fuel_cols.items():
                        price = row[col_idx + offset]
                        # Verificăm dacă prețul este un număr valid
                        if pd.notnull(price) and isinstance(price, (int, float)) and price > 0:
                            price_payload.append({
                                "report_date": date_raw,
                                "country_code": clean_ctr,
                                "fuel_id": fuel_id_map[slug],
                                "price_with_tax": float(price),
                                "currency": "EUR"
                            })

        if price_payload:
            res = requests.post(f"{SUPABASE_URL}/rest/v1/fuel_prices", json=price_payload, headers=HEADERS)
            res.raise_for_status()
            print(f"Succes! S-au sincronizat {len(price_payload)} înregistrări.")
        else:
            print("Nu s-au găsit date noi valide în rândurile procesate.")

    except Exception as e:
        print(f"Eroare în timpul execuției: {e}")
        sys.exit(1)

if __name__ == "__main__":
    sync_data()
