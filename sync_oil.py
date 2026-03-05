import pandas as pd
import requests
import os

# Preluăm variabilele din GitHub Secrets
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates" # Permite UPSERT
}

def sync_data():
    # URL-ul fișierului de la Comisia Europeană
    url = "https://energy.ec.europa.eu/document/download/906e60ca-8b6a-44e7-8589-652854d2fd3f_en?filename=Weekly_Oil_Bulletin_Prices_History_maticni_4web.xlsx"
    
    # 1. Obținem maparea tipurilor de combustibil din DB
    r_fuel = requests.get(f"{SUPABASE_URL}/rest/v1/fuel_types?select=id,slug", headers=HEADERS)
    fuel_id_map = {item['slug']: item['id'] for item in r_fuel.json()}
    
    # Coloanele de combustibil (offset față de coloana CTR)
    fuel_cols = {1: 'euro_95', 2: 'diesel', 3: 'heating_oil', 4: 'fuel_oil_low_sulphur', 5: 'fuel_oil_high_sulphur', 6: 'lpg'}
    # Lista de țări (așa cum apar în headerele Excel)
    target_countries = ["EU_", "EUR_", "AT_", "BE_", "BG_", "CY_", "CZ_", "DE_", "DK_", "EE_", "EL_", "ES_", "FI_", "FR_", "HR_", "HU_", "IE_", "IT_", "LT_", "LU_", "LV_", "MT_", "NL_", "PL_", "PT_", "RO_", "SE_", "SI_", "SK_"]

    print("Descarc și procesez Prețurile...")
    # --- PROCESARE PREȚURI ---
    df_prices = pd.read_excel(url, sheet_name="Prices with taxes", header=None)
    price_payload = []
    # Luăm ultimele 10 rânduri pentru a evita duplicarea inutilă a întregului istoric
    for _, row in df_prices.tail(10).iterrows():
        date_raw = str(row[0]).split()[0] # Extragem doar YYYY-MM-DD
        if len(date_raw) < 10 or "-" not in date_raw: continue
        
        for col_idx, cell in enumerate(row):
            clean_ctr = str(cell).strip()
            if clean_cell in target_countries:
                for offset, slug in fuel_cols.items():
                    val = row[col_idx + offset]
                    if pd.notnull(val) and isinstance(val, (int, float)) and val > 0:
                        price_payload.append({
                            "report_date": date_raw,
                            "country_code": clean_cell, # Păstrează 'RO_' sau adaugă .replace("_","") dacă ai 'RO' în DB
                            "fuel_id": fuel_id_map[slug],
                            "price_with_tax": float(val),
                            "currency": "EUR"
                        })
    
    if price_payload:
        res = requests.post(f"{SUPABASE_URL}/rest/v1/fuel_prices", json=price_payload, headers=HEADERS)
        print(f"Prețuri sincronizate: {len(price_payload)} rânduri. Status: {res.status_code}")

    # --- PROCESARE CONSUM (Bonus) ---
    print("Procesez Consumul...")
    df_cons = pd.read_excel(url, sheet_name="Consumption", header=None)
    last_year_row = df_cons.iloc[-1]
    year = int(last_year_row[0])
    cons_payload = []
    for col_idx, cell in enumerate(last_year_row):
        clean_ctr = str(cell).strip()
        if clean_ctr in target_countries:
            for offset, slug in fuel_cols.items():
                qty = last_year_row[col_idx + offset]
                if pd.notnull(qty) and qty > 0:
                    cons_payload.append({
                        "year": year, "country_code": clean_ctr,
                        "fuel_id": fuel_id_map[slug], "quantity": float(qty)
                    })
    
    if cons_payload:
        res_c = requests.post(f"{SUPABASE_URL}/rest/v1/fuel_consumption", json=cons_payload, headers=HEADERS)
        print(f"Consum sincronizat pentru anul {year}. Status: {res_c.status_code}")

if __name__ == "__main__":
    sync_data()
