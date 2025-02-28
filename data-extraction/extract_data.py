import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import duckdb

now = datetime.now()
rounded = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=round(now.minute / 60))
rounded_str = rounded.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]  

data = requests.get(f'https://data.ny.gov/resource/wujg-7c2s.json?transit_timestamp=2024-12-31T00:00:00.000&$limit=50000')
json_data = json.loads(data.text)
df = pd.DataFrame(json_data)

# Connect to DuckDB
conn = duckdb.connect('../data/transit_data.duckdb')

# Create table with appropriate schema
conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_mta_ridership (
        transit_timestamp TIMESTAMP,
        transit_mode VARCHAR,
        station_complex_id VARCHAR,
        station_complex VARCHAR,
        borough VARCHAR,
        payment_method VARCHAR,
        fare_class_category VARCHAR,
        ridership INTEGER,
        transfers INTEGER,
        latitude DOUBLE,
        longitude DOUBLE,
        georeference VARCHAR
    )
""")

# Load data into DuckDB
conn.execute("INSERT INTO raw_mta_ridership SELECT * FROM df")

# Verify data loaded correctly
result = conn.execute("SELECT COUNT(*) FROM raw_mta_ridership").fetchall()
print(f"Loaded {result[0][0]} records into DuckDB")

conn.close()