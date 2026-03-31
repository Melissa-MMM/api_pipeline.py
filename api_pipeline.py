import requests
import pandas as pd
import sqlite3
from datetime import datetime

# STEP 1: Extract
url = "https://api.exchangerate.host/latest?base=EUR"
response = requests.get(url)
data = response.json()

print(data)  

# STEP 2: Transform (safe access)
rates = data.get('rates', {})

usd = rates.get('USD')
eur = rates.get('EUR')
gbp = rates.get('GBP')
zar = rates.get('ZAR')
 
# Create DataFrame
df = pd.DataFrame([{
    "base_currency": "EUR",
    "usd_rate": usd,
    "eur_rate": eur,
    "gbp_rate": gbp,
    "zar_rate": zar,
    "timestamp": datetime.now()
}])

# STEP 3: Load
conn = sqlite3.connect("api_pipeline.db")

df.to_sql("exchange_rates", conn, if_exists="append", index=False)

print("API pipeline executed successfully!")
