# Currency Exchange Rate Data Pipeline (ETL)

An automated Python-driven ETL (Extract, Transform, Load) pipeline designed to extract real-time foreign currency exchange rates, structure the data, and store it in a local SQLite database for historical tracking.

## Pipeline Architecture
1. **Extract:** Fetches live exchange rate JSON data from the Exchange Rate API using Python's `requests` library.
2. **Transform:** Extracts target currencies (USD, EUR, GBP, ZAR), handles null safety, formats data into a Pandas DataFrame, and attaches execution timestamps.
3. **Load:** Automatically initializes/connects to a local SQLite database (`api_pipeline.db`) and appends clean records into an `exchange_rates` table.

## Tech Stack
* **Language:** Python
* **Libraries:** Pandas, Requests, SQLite3, Datetime
* **Database:** SQLite
