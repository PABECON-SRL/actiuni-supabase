import pandas as pd
import requests
import os

# Configurații din Environment Variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
def sync_prices():
    url = "https://energy.ec.europa.eu/document/download/906e60ca-8b6a-44e7-8589-652854d2fd3f_en?filename=Weekly_Oil_Bulletin_Prices_History_maticni_4web.xlsx"
    
    # 1. Citim Excel-ul (GitHub are 7GB RAM, nicio problemă aici)
    df = pd.read_excel(url, sheet_name="Prices with taxes", header=None)
    
    # 2. Curățăm datele (luăm ultimele 10 rânduri)
    df_recent = df.tail(10)
    
    # Mapare combustibili (coloanele din Excel)
    fuel_map = {1: 'euro_95', 2: 'diesel', 3: 'heating_oil', 4: 'fuel_oil_low_sulphur', 5: 'fuel_oil_high_sulphur', 6: 'lpg'}
    countries = ["EU_", "EUR_", "AT_", "BE_", "BG_", "CY_", "CZ_", "DE_", "DK_", "EE_", "EL_", "ES_", "FI_", "FR_", "HR_", "HU_", "IE_", "IT_", "LT_", "LU_", "LV_", "MT_", "NL_", "PL_", "PT_", "RO_", "SE_", "SI_", "SK_"]

    # Luăm ID-urile din Supabase
    f_types = requests.get(f"{SUPABASE_URL}/rest/v1/fuel_types?select=id,slug", headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"}).json()
    f_id_map = {item['slug']: item['id'] for item in f_types}

    payload = []
    for _, row in df_recent.iterrows():
        date = str(row[0]).split()[0]
        if "-" not in date: continue # Sări peste rânduri fără dată
        
        # Căutăm fiecare țară în rând
        for col_idx, cell in enumerate(row):
            clean_cell = str(cell).strip()
            if clean_cell in countries:
                for offset, slug in fuel_map.items():
                    price = row[col_idx + offset]
                    if pd.notnull(price) and isinstance(price, (int, float)) and price > 0:
                        payload.append({
                            "report_date": date,
                            "country_code": clean_cell,
                            "fuel_id": f_id_map[slug],
                            "price_with_tax": float(price),
                            "currency": "EUR"
                        })

    # 3. Trimitem datele către Supabase (Upsert)
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates" # Asta face UPSERT
    }
    r = requests.post(f"{SUPABASE_URL}/rest/v1/fuel_prices", json=payload, headers=headers)
    print(f"Status: {r.status_code}, Inserate: {len(payload)} rânduri.")

if __name__ == "__main__":
    sync_prices()
