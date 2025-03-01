import duckdb
import os
import pandas as pd

# Get path to database
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
db_path = os.path.join(project_root, 'data', 'transit_data.duckdb')

# Connect to DuckDB
conn = duckdb.connect(db_path)

# Simple query
# result = conn.execute("""
#     SELECT *
#     FROM raw_mta_ridership

# """).fetchall()

# print(result)

################################################################################

# Query your new model
# result = conn.execute("""
#     SELECT *
#     FROM stg_mta_ridership
#     LIMIT 5;  -- just to see a few rows
# """).fetchall()

# print(result)


################################################################################
# Show all tables
result = conn.execute("""
    SHOW TABLES;
""").fetchall()

print(result)