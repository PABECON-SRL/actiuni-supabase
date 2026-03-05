import pandas as pd
import requests
import os
import sys

URL = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not URL or not KEY:
    sys.exit("EROARE: Variabilele SUPABASE_URL sau SUPABASE_SERVICE_ROLE_KEY lipsesc din GitHub Secrets.")

BASE_URL = URL.strip().rstrip('/')
HEADERS = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates" # Permite update-ul coloanelor pe rânduri existente
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
    print("--- Sincronizare Prețuri (fuel_prices) ---")
    df_with = pd.read_excel(XL_URL, sheet_name="Prices with taxes", header=None)
    df_wo = pd.read_excel(XL_URL, sheet_name="Prices wo taxes", header=None)
    
    price_data = {}

    def process_prices(df, field_name):
        for _, row in df.tail(15).iterrows(): # Ultimele 15 rânduri
            date = pd.to_datetime(row[0], errors='coerce')
            if pd.isna(date) or date.year < 2000: continue
            date_str = date.strftime('%Y-%m-%d')

            for col_idx, cell in enumerate(row):
                ctr = str(cell).strip()
                if ctr in COUNTRIES:
                    # Exchange rate e coloana imediat următoare
                    ex_rate = clean_val(row[col_idx + 1]) if field_name == "price_with_tax" else None
                    if ex_rate is None and field_name == "price_with_tax": ex_rate = 1.0
                    
                    for offset, slug in enumerate(FUEL_SLUGS):
                        price = clean_val(row[col_idx + offset + 2]) # Sărim data, CTR și Exchange
                        if price:
                            key = (date_str, ctr, f_map[slug])
                            if key not in price_data:
                                price_data[key] = {"report_date": date_str, "country_code": ctr, "fuel_id": f_map[slug]}
                            price_data[key][field_name] = price
                            if ex_rate: price_data[key]["exchange_rate"] = ex_rate

    process_prices(df_with, "price_with_tax")
    process_prices(df_wo, "price_wo_tax")
    
    payload = list(price_data.values())
    if payload:
        res = requests.post(f"{BASE_URL}/rest/v1/fuel_prices", json=payload, headers=HEADERS)
        print(f"Prices Sync Status: {res.status_code}")

def sync_taxes(f_map):
    print("--- Sincronizare Taxe (fuel_taxes) ---")
    # Tab-urile: 'VAT', 'Excise duties', 'Other indirect taxes'
    tax_sheets = {
        "VAT": "vat_rate_percent",
        "Excise duties": "excise_duty_value",
        "Other indirect taxes": "other_indirect_taxes"
    }
    
    merged_taxes = {}

    for sheet, column in tax_sheets.items():
        try:
            df = pd.read_excel(XL_URL, sheet_name=sheet, header=None)
            for _, row in df.tail(10).iterrows():
                date = pd.to_datetime(row[0], errors='coerce')
                if pd.isna(date): continue
                date_str = date.strftime('%Y-%m-%d')

                for col_idx, cell in enumerate(row):
                    ctr = str(cell).strip()
                    if ctr in COUNTRIES:
                        for offset, slug in enumerate(FUEL_SLUGS):
                            val = clean_val(row[col_idx + offset + 1]) # Taxele nu au exchange rate column
                            if val is not None:
                                key = (date_str, ctr, f_map[slug])
                                if key not in merged_taxes:
                                    merged_taxes[key] = {"applicable_from": date_str, "country_code": ctr, "fuel_id": f_map[slug]}
                                merged_taxes[key][column] = val
        except Exception as e:
            print(f"Eroare la procesarea tab-ului {sheet}: {e}")

    payload = list(merged_taxes.values())
    if payload:
        res = requests.post(f"{BASE_URL}/rest/v1/fuel_taxes", json=payload, headers=HEADERS)
        print(f"Taxes Sync Status: {res.status_code}")

def sync_consumption(f_map):
    print("--- Sincronizare Consum (fuel_consumption) ---")
    df = pd.read_excel(XL_URL, sheet_name="Consumption", header=None)
    payload = []
    # Luăm ultimii 3 ani de date
    for _, row in df.tail(3).iterrows():
        try:
            year = int(row[0])
            for col_idx, cell in enumerate(row):
                ctr = str(cell).strip()
                if ctr in COUNTRIES:
                    for offset, slug in enumerate(FUEL_SLUGS):
                        qty = clean_val(row[col_idx + offset + 1])
                        if qty is not None:
                            payload.append({"year": year, "country_code": ctr, "fuel_id": f_map[slug], "quantity": qty})
        except: continue
    
    if payload:
        res = requests.post(f"{BASE_URL}/rest/v1/fuel_consumption", json=payload, headers=HEADERS)
        print(f"Consumption Sync Status: {res.status_code}")

def sync_excise_components(f_map):
    print("--- Sincronizare Componente Acciză (excise_components) ---")
    # Aici procesăm tab-ul 'Excise duties - components'
    df = pd.read_excel(XL_URL, sheet_name="Excise duties - components", header=None)
    payload = []
    
    # Acest tab are o structură unde Componenta e adesea un header. 
    # Pentru simplitate, tratăm valoarea ca 'Total Excise Component' sau poți extinde logica.
    for _, row in df.tail(10).iterrows():
        date = pd.to_datetime(row[0], errors='coerce')
        if pd.isna(date): continue
        date_str = date.strftime('%Y-%m-%d')

        for col_idx, cell in enumerate(row):
            ctr = str(cell).strip()
            if ctr in COUNTRIES:
                for offset, slug in enumerate(FUEL_SLUGS):
                    val = clean_val(row[col_idx + offset + 1])
                    if val is not None:
                        payload.append({
                            "report_date": date_str,
                            "country_code": ctr,
                            "fuel_id": f_map[slug],
                            "component_name": "Standard Excise",
                            "value_amount": val
                        })
    
    if payload:
        res = requests.post(f"{BASE_URL}/rest/v1/excise_components", json=payload, headers=HEADERS)
        print(f"Excise Components Sync Status: {res.status_code}")

if __name__ == "__main__":
    fuel_id_map = get_fuel_map()
    sync_prices(fuel_id_map)
    sync_taxes(fuel_id_map)
    sync_consumption(fuel_id_map)
    sync_excise_components(fuel_id_map)
    print("Misiune îndeplinită!")
