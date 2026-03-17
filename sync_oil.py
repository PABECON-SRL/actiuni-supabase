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
    "Prefer": "resolution=merge-duplicates" 
}

COUNTRIES = ["EU_", "EUR_", "AT_", "BE_", "BG_", "CY_", "CZ_", "DE_", "DK_", "EE_", "EL_", "ES_", "FI_", "FR_", "HR_", "HU_", "IE_", "IT_", "LT_", "LU_", "LV_", "MT_", "NL_", "PL_", "PT_", "RO_", "SE_", "SI_", "SK_"]
FUEL_SLUGS = ['euro_95', 'diesel', 'heating_oil', 'fuel_oil_low_sulphur', 'fuel_oil_high_sulphur', 'lpg']

# URL cu timestamp pentru a forța serverul să dea versiunea fresh
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
    # Pandas to_datetime este foarte flexibil cu formatele de Excel
    d = pd.to_datetime(val, errors='coerce', dayfirst=True)
    return d if pd.notna(d) else None

def sync_prices(f_map, fb):
    print("--- START PROCESARE (SCANARE TOTALĂ) ---")
    xls = pd.ExcelFile(fb)
    # Căutăm tab-urile după nume (case-insensitive)
    sheet_names = {s.strip().lower(): s for s in xls.sheet_names}
    
    df_with = pd.read_excel(xls, sheet_name=sheet_names.get('prices with taxes'), header=None)
    df_wo = pd.read_excel(xls, sheet_name=sheet_names.get('prices wo taxes'), header=None)
    
    price_data = {}

    def process_df(df, field):
        found_rows = 0
        for i, row in df.iterrows():
            # SĂRIM DOAR PESTE PRIMELE 3 RÂNDURI (HEADERELE)
            if i < 3: continue 
            
            date = force_parse_date(row[0])
            # Filtrăm doar datele din 2020 până în prezent
            if date is None or date.year < 2020: continue
            
            date_str = date.strftime('%Y-%m-%d')
            found_rows += 1

            for col_idx, cell in enumerate(row):
                ctr = str(cell).strip()
                if ctr in COUNTRIES:
                    # Exchange Rate e coloana imediat următoare țării
                    ex_rate = clean_val(row[col_idx + 1]) or 1.0
                    
                    for offset, slug in enumerate(FUEL_SLUGS):
                        try:
                            # Prețurile încep de la coloana CTR + 2
                            val = clean_val(row[col_idx + offset + 2])
                            if val:
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
        return found_rows

    p1 = process_df(df_with, "price_with_tax")
    p2 = process_df(df_wo, "price_wo_tax")
    
    payload = list(price_data.values())
    if payload:
        # Sortăm descrescător după dată ca să vedem ce e mai nou în log
        payload.sort(key=lambda x: x['report_date'], reverse=True)
        print(f"ULTIMA DATĂ DETECTATĂ: {payload[0]['report_date']}")
        print(f"Total înregistrări procesate: {len(payload)}")
        
        for i in range(0, len(payload), 1000):
            res = requests.post(f"{BASE_URL}/rest/v1/fuel_prices", json=payload[i:i+1000], headers=HEADERS)
            print(f"Batch {i//1000 + 1}: Status {res.status_code}")
    else:
        print("EROARE: Nu s-au găsit date valide!")

if __name__ == "__main__":
    f_map = get_fuel_map()
    print("Descarc fișierul...")
    resp = requests.get(XL_URL, headers={'User-Agent': 'Mozilla/5.0'}, timeout=60)
    fb = io.BytesIO(resp.content)
    
    sync_prices(f_map, fb)
    print("Sincronizare terminată.")
