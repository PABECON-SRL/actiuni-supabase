import pandas as pd
import requests
import os

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def get_fuel_map():
    r = requests.get(f"{SUPABASE_URL}/rest/v1/fuel_types?select=id,slug", headers=HEADERS)
    return {item['slug']: item['id'] for item in r.json()}

def sync_data():
    url = "https://energy.ec.europa.eu/document/download/906e60ca-8b6a-44e7-8589-652854d2fd3f_en?filename=Weekly_Oil_Bulletin_Prices_History_maticni_4web.xlsx"
    fuel_id_map = get_fuel_map()
    countries = ["EU_", "EUR_", "AT_", "BE_", "BG_", "CY_", "CZ_", "DE_", "DK_", "EE_", "EL_", "ES_", "FI_", "FR_", "HR_", "HU_", "IE_", "IT_", "LT_", "LU_", "LV_", "MT_", "NL_", "PL_", "PT_", "RO_", "SE_", "SI_", "SK_"]
    fuel_cols = {1: 'euro_95', 2: 'diesel', 3: 'heating_oil', 4: 'fuel_oil_low_sulphur', 5: 'fuel_oil_high_sulphur', 6: 'lpg'}

    # --- 1. PROCESARE PREȚURI (Prices with taxes) ---
    df_prices = pd.read_excel(url, sheet_name="Prices with taxes", header=None)
    price_payload = []
    for _, row in df_prices.tail(15).iterrows(): # Ultimele 15 săpt.
        date = str(row[0]).split()[0]
        if "-" not in date: continue
        for col_idx, cell in enumerate(row):
            if str(cell).strip() in countries:
                for offset, slug in fuel_cols.items():
                    price = row[col_idx + offset]
                    if pd.notnull(price) and isinstance(price, (int, float)) and price > 0:
                        price_payload.append({
                            "report_date": date, "country_code": str(cell).strip(),
                            "fuel_id": fuel_id_map[slug], "price_with_tax": float(price)
                        })
    requests.post(f"{SUPABASE_URL}/rest/v1/fuel_prices", json=price_payload, headers=HEADERS)

    # --- 2. PROCESARE CONSUM (Consumption) ---
    df_cons = pd.read_excel(url, sheet_name="Consumption", header=None)
    last_year_row = df_cons.iloc[-1]
    year = int(last_year_row[0])
    cons_payload = []
    for col_idx, cell in enumerate(last_year_row):
        if str(cell).strip() in countries:
            for offset, slug in fuel_cols.items():
                qty = last_year_row[col_idx + offset]
                if pd.notnull(qty) and qty > 0:
                    cons_payload.append({
                        "year": year, "country_code": str(cell).strip(),
                        "fuel_id": fuel_id_map[slug], "quantity": float(qty)
                    })
    requests.post(f"{SUPABASE_URL}/rest/v1/fuel_consumption", json=cons_payload, headers=HEADERS)

    print("Sincronizare completă: Prețuri și Consum!")

if __name__ == "__main__":
    sync_data()
