import pandas as pd
import requests
import os
import sys
import time
import io

URL = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not URL or not KEY:
    sys.exit("EROARE: Secretele SUPABASE_URL sau SUPABASE_SERVICE_ROLE_KEY lipsesc.")

BASE_URL = URL.strip().rstrip('/')
HEADERS = {
    "apikey": KEY,
    "Authorization": f"Bearer {KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates" 
}

COUNTRIES = ["EU_", "EUR_", "AT_", "BE_", "BG_", "CY_", "CZ_", "DE_", "DK_", "EE_", "EL_", "ES_", "FI_", "FR_", "HR_", "HU_", "IE_", "IT_", "LT_", "LU_", "LV_", "MT_", "NL_", "PL_", "PT_", "RO_", "SE_", "SI_", "SK_"]
FUEL_SLUGS = ['euro_95', 'diesel', 'heating_oil', 'fuel_oil_low_sulphur', 'fuel_oil_high_sulphur', 'lpg']
# URL cu timestamp agresiv pentru a forța un fișier nou
XL_URL = f"https://energy.ec.europa.eu/document/download/906e60ca-8b6a-44e7-8589-652854d2fd3f_en?filename=Weekly_Oil_Bulletin_Prices_History_maticni_4web.xlsx&t={int(time.time() * 1000)}"

def force_parse_date(val):
    if pd.isna(val): return None
    # Încearcă conversia directă (Excel serial date sau string standard)
    d = pd.to_datetime(val, errors='coerce', dayfirst=True)
    return d if pd.notna(d) else None

def clean_val(val):
    if pd.isna(val) or val == "" or str(val).strip().lower() in ['n.a', 'n.a.']:
        return None
    try:
        return float(val)
    except:
        return None

def sync_prices(f_map, fb):
    print("\n--- FAZA 1: ANALIZĂ ȘI PROCESARE PREȚURI ---")
    xls = pd.ExcelFile(fb)
    sheet_names = {s.strip().lower(): s for s in xls.sheet_names}
    
    # Citim tab-urile
    df_with = pd.read_excel(xls, sheet_name=sheet_names.get('prices with taxes'), header=None)
    df_wo = pd.read_excel(xls, sheet_name=sheet_names.get('prices wo taxes'), header=None)
    
    print(f"DEBUG: Sheet 'Prices with taxes' are {len(df_with)} rânduri.")
    print("DEBUG: Primele 5 rânduri din coloana A (Date):")
    for idx, val in df_with.iloc[:5, 0].items():
        print(f"  Rând {idx+1}: Valoare raw='{val}' | Interpretată ca={force_parse_date(val)}")

    price_data = {}

    def process_prices(df, field):
        match_9_march = 0
        for i, row in df.iterrows():
            if i < 2: continue # Sărim doar headerele de sus de tot
            
            date = force_parse_date(row[0])
            if date is None or date.year < 2020: continue
            
            date_str = date.strftime('%Y-%m-%d')
            if date_str == "2026-03-09":
                match_9_march += 1

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
                                        "report_date": date_str, "country_code": ctr, "fuel_id": f_map[slug], 
                                        "currency": "EUR", "exchange_rate": ex_rate,
                                        "price_with_tax": None, "price_wo_tax": None
                                    }
                                price_data[key][field] = val
                        except: continue
        print(f"INFO: Găsite {match_9_march} înregistrări pentru 09.03.2026 în coloana {field}")

    process_prices(df_with, "price_with_tax")
    process_prices(df_wo, "price_wo_tax")
    
    payload = list(price_data.values())
    if payload:
        payload.sort(key=lambda x: x['report_date'], reverse=True)
        recent_date = payload[0]['report_date']
        print(f"RAPORT: Ultima dată procesată în payload: {recent_date}")
        
        # Filtru special pentru log: numărăm 9 martie în payload-ul final
        target_count = sum(1 for x in payload if x['report_date'] == "2026-03-09")
        print(f"RAPORT: Total rânduri pentru 2026-03-09 pregătite pentru trimitere: {target_count}")

        # URL cu on_conflict
        url = f"{BASE_URL}/rest/v1/fuel_prices?on_conflict=report_date,country_code,fuel_id"
        
        for i in range(0, len(payload), 300):
            batch = payload[i:i+300]
            res = requests.post(url, json=batch, headers=HEADERS)
            print(f"Batch {i//300 + 1} ({len(batch)} rânduri) | Status: {res.status_code}")
            if res.status_code >= 400:
                print(f"ALERTA BAZA DE DATE: {res.text}")
    else:
        print("EROARE CRITICĂ: Nu s-a putut genera niciun payload din Excel!")

if __name__ == "__main__":
    # 1. Preluăm maparea
    r_f = requests.get(f"{BASE_URL}/rest/v1/fuel_types?select=id,slug", headers=HEADERS)
    r_f.raise_for_status()
    f_map = {item['slug']: item['id'] for item in r_f.json()}
    
    # 2. Descărcăm
    print(f"Descărcare fișier de la: {XL_URL}")
    resp = requests.get(XL_URL, headers={'User-Agent': 'Mozilla/5.0'}, timeout=60)
    fb = io.BytesIO(resp.content)
    
    # 3. Sincronizăm
    sync_prices(f_map, fb)
    print("\n--- SINCRONIZARE TERMINATĂ ---")
