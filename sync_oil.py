import pandas as pd
import requests
import os
import sys

URL = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not URL or not KEY:
    sys.exit("EROARE: Variabilele de mediu lipsesc.")

BASE_URL = URL.strip().rstrip('/')
HEADERS = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates" 
}

# Configurații
COUNTRIES = ["EU_", "EUR_", "AT_", "BE_", "BG_", "CY_", "CZ_", "DE_", "DK_", "EE_", "EL_", "ES_", "FI_", "FR_", "HR_", "HU_", "IE_", "IT_", "LT_", "LU_", "LV_", "MT_", "NL_", "PL_", "PT_", "RO_", "SE_", "SI_", "SK_"]
FUEL_SLUGS = ['euro_95', 'diesel', 'heating_oil', 'fuel_oil_low_sulphur', 'fuel_oil_high_sulphur', 'lpg']
XL_URL = "https://energy.ec.europa.eu/document/download/906e60ca-8b6a-44e7-8589-652854d2fd3f_en?filename=Weekly_Oil_Bulletin_Prices_History_maticni_4web.xlsx"

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

def sync_prices(f_map):
    print("Sincronizare Prețuri (2020 - Prezent)...")
    df_with = pd.read_excel(XL_URL, sheet_name="Prices with taxes", header=None)
    df_wo = pd.read_excel(XL_URL, sheet_name="Prices wo taxes", header=None)
    
    price_data = {}

    def process_df(df, field_name):
        # Scanăm tot tabelul, dar filtrăm în buclă
        for _, row in df.iterrows():
            date = pd.to_datetime(row[0], errors='coerce')
            
            # FILTRU TEMPORAL: Doar din 2020 încoace
            if pd.isna(date) or date.year < 2020: 
                continue
                
            date_str = date.strftime('%Y-%m-%d')

            for col_idx, cell in enumerate(row):
                ctr = str(cell).strip()
                if ctr in COUNTRIES:
                    ex_rate = clean_val(row[col_idx + 1])
                    if ex_rate is None: ex_rate = 1.0
                    
                    for offset, slug in enumerate(FUEL_SLUGS):
                        val = clean_val(row[col_idx + offset + 2])
                        if val:
                            key = (date_str, ctr, f_map[slug])
                            if key not in price_data:
                                price_data[key] = {
                                    "report_date": date_str, 
                                    "country_code": ctr, 
                                    "fuel_id": f_map[slug],
                                    "currency": "EUR"
                                }
                            price_data[key][field_name] = val
                            price_data[key]["exchange_rate"] = ex_rate

    process_df(df_with, "price_with_tax")
    process_df(df_wo, "price_wo_tax")
    
    payload = list(price_data.values())
    if payload:
        print(f"Trimit {len(payload)} rânduri de prețuri...")
        # Trimitem în bucăți de 1000 pentru a nu bloca API-ul
        for i in range(0, len(payload), 1000):
            res = requests.post(f"{BASE_URL}/rest/v1/fuel_prices", json=payload[i:i+1000], headers=HEADERS)
        print(f"Prices Sync: Gata.")

def sync_taxes(f_map):
    print("Sincronizare Taxe (2020 - Prezent)...")
    tax_sheets = {"VAT": "vat_rate_percent", "Excise duties": "excise_duty_value", "Other indirect taxes": "other_indirect_taxes"}
    
    merged_taxes = {}
    for sheet, column in tax_sheets.items():
        df = pd.read_excel(XL_URL, sheet_name=sheet, header=None)
        for _, row in df.iterrows():
            date = pd.to_datetime(row[0], errors='coerce')
            if pd.isna(date) or date.year < 2020: continue
            date_str = date.strftime('%Y-%m-%d')

            for col_idx, cell in enumerate(row):
                ctr = str(cell).strip()
                if ctr in COUNTRIES:
                    for offset, slug in enumerate(FUEL_SLUGS):
                        val = clean_val(row[col_idx + offset + 1])
                        if val is not None:
                            key = (date_str, ctr, f_map[slug])
                            if key not in merged_taxes:
                                merged_taxes[key] = {"applicable_from": date_str, "country_code": ctr, "fuel_id": f_map[slug]}
                            merged_taxes[key][column] = val

    payload = list(merged_taxes.values())
    if payload:
        for i in range(0, len(payload), 1000):
            requests.post(f"{BASE_URL}/rest/v1/fuel_taxes", json=payload[i:i+1000], headers=HEADERS)
        print(f"Taxes Sync: Gata.")

def sync_consumption(f_map):
    print("Sincronizare Consum (2020 - Prezent)...")
    df = pd.read_excel(XL_URL, sheet_name="Consumption", header=None)
    payload = []
    for _, row in df.iterrows():
        try:
            year = int(row[0])
            if year < 2020: continue
            for col_idx, cell in enumerate(row):
                if str(cell).strip() in COUNTRIES:
                    ctr = str(cell).strip()
                    for offset, slug in enumerate(FUEL_SLUGS):
                        qty = clean_val(row[col_idx + offset + 1])
                        if qty is not None:
                            payload.append({"year": year, "country_code": ctr, "fuel_id": f_map[slug], "quantity": qty})
        except: continue
    if payload:
        requests.post(f"{BASE_URL}/rest/v1/fuel_consumption", json=payload, headers=HEADERS)
        print(f"Consumption Sync: Gata.")

if __name__ == "__main__":
    f_map = get_fuel_map()
    sync_prices(f_map)
    sync_taxes(f_map)
    sync_consumption(f_map)
    print("Misiune finalizată: Datele din 2020 până în prezent au fost sincronizate.")
