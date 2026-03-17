import pandas as pd
import requests
import os
import sys
import time
import io

URL = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not URL or not KEY:
    sys.exit("EROARE: Secretele lipsesc.")

BASE_URL = URL.strip().rstrip('/')
HEADERS_SUPABASE = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates" 
}

COUNTRIES = ["EU_", "EUR_", "AT_", "BE_", "BG_", "CY_", "CZ_", "DE_", "DK_", "EE_", "EL_", "ES_", "FI_", "FR_", "HR_", "HU_", "IE_", "IT_", "LT_", "LU_", "LV_", "MT_", "NL_", "PL_", "PT_", "RO_", "SE_", "SI_", "SK_"]
FUEL_SLUGS = ['euro_95', 'diesel', 'heating_oil', 'fuel_oil_low_sulphur', 'fuel_oil_high_sulphur', 'lpg']

# URL cu cache busting
XL_URL = f"https://energy.ec.europa.eu/document/download/906e60ca-8b6a-44e7-8589-652854d2fd3f_en?filename=Weekly_Oil_Bulletin_Prices_History_maticni_4web.xlsx&t={int(time.time())}"

def get_fuel_map():
    r = requests.get(f"{BASE_URL}/rest/v1/fuel_types?select=id,slug", headers=HEADERS_SUPABASE)
    r.raise_for_status()
    return {item['slug']: item['id'] for item in r.json()}

def clean_val(val):
    if pd.isna(val) or val == "" or str(val).strip().lower() in ['n.a', 'n.a.']:
        return None
    try:
        return float(val)
    except:
        return None

def sync_prices(f_map, file_content):
    print("--- ÎNCEPERE SINCRONIZARE PREȚURI ---")
    # Citim tab-urile complet (fără tail)
    df_with = pd.read_excel(file_content, sheet_name="Prices with taxes", header=None)
    df_wo = pd.read_excel(file_content, sheet_name="Prices wo taxes", header=None)
    
    price_data = {}

    def process_df(df, field_name):
        # Scanăm rândurile de la 1000 în sus pentru a fi siguri (sau tot df-ul)
        # Am verificat: data de 9 martie este în zona rândului 1064
        for i, row in df.iterrows():
            if i < 4: continue # Sărim headerele
            
            date = pd.to_datetime(row[0], errors='coerce')
            if pd.isna(date) or date.year < 2020:
                continue
            
            date_str = date.strftime('%Y-%m-%d')

            for col_idx, cell in enumerate(row):
                ctr = str(cell).strip()
                if ctr in COUNTRIES:
                    # +1 Exchange Rate, datele încep de la +2
                    ex_rate = clean_val(row[col_idx + 1])
                    if ex_rate is None: ex_rate = 1.0
                    
                    for offset, slug in enumerate(FUEL_SLUGS):
                        val = clean_val(row[col_idx + offset + 2])
                        if val is not None:
                            key = (date_str, ctr, f_map[slug])
                            if key not in price_data:
                                price_data[key] = {
                                    "report_date": date_str, 
                                    "country_code": ctr, 
                                    "fuel_id": f_map[slug],
                                    "currency": "EUR",
                                    "exchange_rate": ex_rate
                                }
                            price_data[key][field_name] = val

    process_df(df_with, "price_with_tax")
    process_df(df_wo, "price_wo_tax")
    
    payload = list(price_data.values())
    if payload:
        # Sortăm să vedem în consolă dacă am găsit 9 martie
        payload.sort(key=lambda x: x['report_date'], reverse=True)
        print(f"Cea mai recentă dată procesată din Excel: {payload[0]['report_date']}")
        
        for i in range(0, len(payload), 1000):
            res = requests.post(f"{BASE_URL}/rest/v1/fuel_prices", json=payload[i:i+1000], headers=HEADERS_SUPABASE)
            print(f"Batch {i//1000 + 1}: Status {res.status_code}")
    else:
        print("Nu s-au găsit date valide!")

if __name__ == "__main__":
    f_map = get_fuel_map()
    print(f"Descarc fișier de la: {XL_URL}")
    resp = requests.get(XL_URL, headers={'User-Agent': 'Mozilla/5.0'}, timeout=60)
    file_bytes = io.BytesIO(resp.content)
    
    sync_prices(f_map, file_bytes)
    print("Sincronizare terminată.")
