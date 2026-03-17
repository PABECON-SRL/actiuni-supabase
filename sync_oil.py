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
HEADERS = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    # REPARARE: Adăugăm on_conflict pentru a specifica cheia unică
    "Prefer": "resolution=merge-duplicates" 
}

COUNTRIES = ["EU_", "EUR_", "AT_", "BE_", "BG_", "CY_", "CZ_", "DE_", "DK_", "EE_", "EL_", "ES_", "FI_", "FR_", "HR_", "HU_", "IE_", "IT_", "LT_", "LU_", "LV_", "MT_", "NL_", "PL_", "PT_", "RO_", "SE_", "SI_", "SK_"]
FUEL_SLUGS = ['euro_95', 'diesel', 'heating_oil', 'fuel_oil_low_sulphur', 'fuel_oil_high_sulphur', 'lpg']

XL_URL = f"https://energy.ec.europa.eu/document/download/906e60ca-8b6a-44e7-8589-652854d2fd3f_en?filename=Weekly_Oil_Bulletin_Prices_History_maticni_4web.xlsx&t={int(time.time())}"

def get_fuel_map():
    r = requests.get(f"{BASE_URL}/rest/v1/fuel_types?select=id,slug", headers=HEADERS)
    r.raise_for_status()
    return {item['slug']: item['id'] for item in r.json()}

def clean_val(val):
    if pd.isna(val) or val == "" or str(val).strip().lower() in ['n.a', 'n.a.']:
        return None
    try:
        return float(val)
    except:
        return None

def force_parse_date(val):
    if pd.isna(val): return None
    d = pd.to_datetime(val, errors='coerce', dayfirst=True)
    return d if pd.notna(d) else None

def sync_prices(f_map, fb):
    print("--- START PROCESARE DATE ---")
    xls = pd.ExcelFile(fb)
    sheet_names = {s.strip().lower(): s for s in xls.sheet_names}
    
    df_with = pd.read_excel(xls, sheet_name=sheet_names.get('prices with taxes'), header=None)
    df_wo = pd.read_excel(xls, sheet_name=sheet_names.get('prices wo taxes'), header=None)
    
    price_data = {}

    def process_df(df, field):
        # Scanăm DOAR ultimele 50 de rânduri pentru a fi rapizi și a evita 409 pe mii de rânduri inutile
        # Dacă vrei tot istoricul, înlocuiește df.tail(50) cu df
        for i, row in df.tail(100).iterrows():
            date = force_parse_date(row[0])
            if date is None or date.year < 2020: continue
            
            date_str = date.strftime('%Y-%m-%d')

            for col_idx, cell in enumerate(row):
                ctr = str(cell).strip()
                if ctr in COUNTRIES:
                    ex_rate = clean_val(row[col_idx + 1]) or 1.0
                    for offset, slug in enumerate(FUEL_SLUGS):
                        try:
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
                                price_data[key][field] = val
                        except: continue

    process_df(df_with, "price_with_tax")
    process_df(df_wo, "price_wo_tax")
    
    payload = list(price_data.values())
    if payload:
        payload.sort(key=lambda x: x['report_date'], reverse=True)
        print(f"TRIMIT DATE RECENTE. CEA MAI NOUĂ: {payload[0]['report_date']}")
        
        # Specificăm on_conflict în URL pentru a rezolva eroarea 409
        upsert_url = f"{BASE_URL}/rest/v1/fuel_prices?on_conflict=report_date,country_code,fuel_id"
        
        for i in range(0, len(payload), 100):
            batch = payload[i:i+100]
            res = requests.post(upsert_url, json=batch, headers=HEADERS)
            if res.status_code >= 400:
                print(f"Batch {i//100 + 1} | Status: {res.status_code} | Eroare: {res.text}")
            else:
                print(f"Batch {i//100 + 1} | Status: {res.status_code} (Succes/Update)")
    else:
        print("EROARE: Nu s-au găsit date.")

if __name__ == "__main__":
    f_map = get_fuel_map()
    print("Descarc fișier...")
    resp = requests.get(XL_URL, headers={'User-Agent': 'Mozilla/5.0'}, timeout=60)
    fb = io.BytesIO(resp.content)
    sync_prices(f_map, fb)
