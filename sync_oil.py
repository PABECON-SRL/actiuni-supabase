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

# Forțăm spargerea cache-ului cu un timestamp
XL_URL = f"https://energy.ec.europa.eu/document/download/906e60ca-8b6a-44e7-8589-652854d2fd3f_en?filename=Weekly_Oil_Bulletin_Prices_History_maticni_4web.xlsx&t={int(time.time())}"

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
    if pd.notna(d): return d
    try:
        return pd.to_datetime(str(val).strip(), format='%d/%m/%Y', errors='coerce')
    except:
        return None

def get_fuel_map():
    r = requests.get(f"{BASE_URL}/rest/v1/fuel_types?select=id,slug", headers=HEADERS)
    r.raise_for_status()
    return {item['slug']: item['id'] for item in r.json()}

def sync_prices(f_map, fb):
    print("--- 1. Sincronizare Prețuri ---")
    xls = pd.ExcelFile(fb)
    
    # Căutăm tab-urile indiferent de spații sau litere mari/mici
    sheet_names = {s.strip().lower(): s for s in xls.sheet_names}
    
    df_with = pd.read_excel(xls, sheet_name=sheet_names.get('prices with taxes'))
    df_wo = pd.read_excel(xls, sheet_name=sheet_names.get('prices wo taxes'))
    
    price_data = {}

    def process_prices(df, field):
        for i, row in df.iterrows():
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
                            if val:
                                key = (date_str, ctr, f_map[slug])
                                if key not in price_data:
                                    price_data[key] = {"report_date": date_str, "country_code": ctr, "fuel_id": f_map[slug], "currency": "EUR", "exchange_rate": ex_rate}
                                price_data[key][field] = val
                        except: continue

    process_prices(df_with, "price_with_tax")
    process_prices(df_wo, "price_wo_tax")
    
    payload = list(price_data.values())
    if payload:
        payload.sort(key=lambda x: x['report_date'], reverse=True)
        print(f"ULTIMA DATĂ GĂSITĂ ÎN EXCEL: {payload[0]['report_date']}")
        for i in range(0, len(payload), 1000):
            requests.post(f"{BASE_URL}/rest/v1/fuel_prices", json=payload[i:i+1000], headers=HEADERS)

def sync_taxes(f_map, fb):
    print("--- 2. Sincronizare Taxe ---")
    xls = pd.ExcelFile(fb)
    sheet_names = {s.strip().lower(): s for s in xls.sheet_names}
    
    tax_config = {
        "vat": "vat_rate_percent",
        "excise duties": "excise_duty_value",
        "other indirect taxes": "other_indirect_taxes"
    }
    
    merged_taxes = {}
    for search_name, col_db in tax_config.items():
        actual_name = sheet_names.get(search_name)
        if not actual_name:
            print(f"Sărire tab: {search_name} (nu a fost găsit)")
            continue
            
        df = pd.read_excel(xls, sheet_name=actual_name, header=None)
        for _, row in df.iterrows():
            date = force_parse_date(row[0])
            if date is None or date.year < 2020: continue
            date_str = date.strftime('%Y-%m-%d')
            
            for col_idx, cell in enumerate(row):
                if str(cell).strip() in COUNTRIES:
                    for offset, slug in enumerate(FUEL_SLUGS):
                        try:
                            val = clean_val(row[col_idx + offset + 1])
                            if val is not None:
                                key = (date_str, str(cell).strip(), f_map[slug])
                                if key not in merged_taxes:
                                    merged_taxes[key] = {"applicable_from": date_str, "country_code": str(cell).strip(), "fuel_id": f_map[slug]}
                                merged_taxes[key][col_db] = val
                        except: continue

    payload = list(merged_taxes.values())
    if payload:
        for i in range(0, len(payload), 1000):
            requests.post(f"{BASE_URL}/rest/v1/fuel_taxes", json=payload[i:i+1000], headers=HEADERS)

if __name__ == "__main__":
    f_map = get_fuel_map()
    print(f"Descarcare fișier...")
    resp = requests.get(XL_URL, headers={'User-Agent': 'Mozilla/5.0'}, timeout=60)
    fb = io.BytesIO(resp.content)
    
    sync_prices(f_map, fb)
    sync_taxes(f_map, fb)
    print("Sincronizare terminată.")
