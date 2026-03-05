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
    
    try:
        r_fuel = requests.get(f"{BASE_URL}/rest/v1/fuel_types?select=id,slug", headers=HEADERS)
        r_fuel.raise_for_status()
        f_map = {item['slug']: item['id'] for item in r_fuel.json()}
    except Exception as e:
        sys.exit(f"Eroare conectare Supabase: {e}")

    print("Descarc fișierul...")
    # Citim tot fișierul fără a specifica tail, ca să nu ratăm datele din cauza rândurilor goale de la final
    df = pd.read_excel(xl_url, sheet_name="Prices with taxes", header=None)
    
    countries = ["EU_", "EUR_", "AT_", "BE_", "BG_", "CY_", "CZ_", "DE_", "DK_", "EE_", "EL_", "ES_", "FI_", "FR_", "HR_", "HU_", "IE_", "IT_", "LT_", "LU_", "LV_", "MT_", "NL_", "PL_", "PT_", "RO_", "SE_", "SI_", "SK_"]
    fuel_slugs = ['euro_95', 'diesel', 'heating_oil', 'fuel_oil_low_sulphur', 'fuel_oil_high_sulphur', 'lpg']

    payload = []
    
    print(f"Procesez {len(df)} rânduri...")

    for i, row in df.iterrows():
        # Încercăm să convertim prima coloană la dată
        try:
            raw_date = row[0]
            if pd.isna(raw_date): continue
            
            # Convertim la datetime (funcționează și pt string-uri și pt numere Excel)
            clean_date = pd.to_datetime(raw_date, errors='coerce')
            if pd.isna(clean_date): continue
            
            date_str = clean_date.strftime('%Y-%m-%d')
            
            # Verificăm dacă anul este rezonabil (să nu luăm headerele ca date)
            if clean_date.year < 2000: continue

        except:
            continue

        # Căutăm țările în rând
        for col_idx, cell in enumerate(row):
            val_str = str(cell).strip()
            if val_str in countries:
                for offset, slug in enumerate(fuel_slugs):
                    try:
                        price = row[col_idx + offset + 1]
                        if pd.notnull(price) and isinstance(price, (int, float, complex)) and float(price) > 0:
                            payload.append({
                                "report_date": date_str,
                                "country_code": val_str,
                                "fuel_id": f_map[slug],
                                "price_with_tax": float(price),
                                "currency": "EUR"
                            })
                    except:
                        continue

    if payload:
        # Trimitem în bucăți de câte 1000 (pentru a evita erori de mărime a request-ului)
        print(f"Am găsit {len(payload)} înregistrări. Trimit către Supabase...")
        # Luăm doar ultimele date pentru a nu supraîncărca la fiecare rulare (opțional)
        # payload = payload[-2000:] 
        
        res = requests.post(f"{BASE_URL}/rest/v1/fuel_prices", json=payload, headers=HEADERS)
        print(f"Status Final: {res.status_code}")
        if res.status_code >= 400:
            print(f"Eroare: {res.text}")
    else:
        print("Nu s-au găsit date valide. Verifică dacă numele tab-ului 'Prices with taxes' este corect.")

if __name__ == "__main__":
    sync_data()
