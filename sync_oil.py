import pandas as pd
import requests
import os
import sys

URL = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not URL or not KEY:
    sys.exit("Eroare: Secretele nu sunt configurate.")

BASE_URL = URL.strip().rstrip('/')
HEADERS = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def sync_data():
    xl_url = "https://energy.ec.europa.eu/document/download/906e60ca-8b6a-44e7-8589-652854d2fd3f_en?filename=Weekly_Oil_Bulletin_Prices_History_maticni_4web.xlsx"
    
    # 1. Obținem maparea tipurilor de combustibil
    try:
        r_fuel = requests.get(f"{BASE_URL}/rest/v1/fuel_types?select=id,slug", headers=HEADERS)
        r_fuel.raise_for_status()
        f_map = {item['slug']: item['id'] for item in r_fuel.json()}
    except Exception as e:
        sys.exit(f"Eroare conectare Supabase: {e}")

    # 2. Descarcă și citește Excel
    print("Descarc și procesez Excel...")
    df = pd.read_excel(xl_url, sheet_name="Prices with taxes", header=None)
    
    countries = ["EU_", "EUR_", "AT_", "BE_", "BG_", "CY_", "CZ_", "DE_", "DK_", "EE_", "EL_", "ES_", "FI_", "FR_", "HR_", "HU_", "IE_", "IT_", "LT_", "LU_", "LV_", "MT_", "NL_", "PL_", "PT_", "RO_", "SE_", "SI_", "SK_"]
    fuel_slugs = ['euro_95', 'diesel', 'heating_oil', 'fuel_oil_low_sulphur', 'fuel_oil_high_sulphur', 'lpg']

    payload = []
    # Procesăm ultimele 20 de rânduri pentru a fi siguri că prindem datele noi
    for _, row in df.tail(20).iterrows():
        # Verificăm dacă prima coloană e o dată
        date_val = row[0]
        if pd.isna(date_val) or not hasattr(date_val, 'strftime'):
            continue
        date_str = date_val.strftime('%Y-%m-%d')

        # Căutăm țările în rând
        for col_idx, cell in enumerate(row):
            clean_ctr = str(cell).strip()
            if clean_ctr in countries:
                # Am găsit țara, extragem cele 6 valori de după ea
                for offset, slug in enumerate(fuel_slugs):
                    price = row[col_idx + offset + 1]
                    if pd.notnull(price) and isinstance(price, (int, float)) and price > 0:
                        payload.append({
                            "report_date": date_str,
                            "country_code": clean_ctr,
                            "fuel_id": f_map[slug],
                            "price_with_tax": float(price),
                            "currency": "EUR"
                        })

    if payload:
        print(f"Trimit {len(payload)} înregistrări către Supabase...")
        res = requests.post(f"{BASE_URL}/rest/v1/fuel_prices", json=payload, headers=HEADERS)
        print(f"Status Final: {res.status_code}")
    else:
        print("Nu s-au găsit date valide în Excel.")

if __name__ == "__main__":
    sync_data()
