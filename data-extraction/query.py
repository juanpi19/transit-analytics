import duckdb
import os
import pandas as pd

# Get path to database
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(project_root, 'data', 'transit_data.duckdb')

# Connect to DuckDB
conn = duckdb.connect(db_path)

# Simple query
result = conn.execute("""
    SELECT 
        transit_mode,
        borough,
        SUM(ridership) as total_riders
    FROM raw_mta_ridership
    WHERE transit_timestamp >= '2024-01-01'
    GROUP BY transit_mode, borough
    ORDER BY total_riders DESC
    LIMIT 10
""").fetchall()

print(result)