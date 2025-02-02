
import pandas as pd
# from sqlalchemy import create_engine
from db_utils import get_db_engine, get_metadata

# Initialize database connection
engine = get_db_engine()
metadata = get_metadata()

print('*' * 80)
# Hardcoded for Testing

query = """
SELECT mts."Abbreviation", 
       mts."PTS", 
       mts."Total_Payroll", 
       ROUND(CAST(tets."CF%%" * 100 AS numeric), 4) AS "avg_cf_percent"
FROM merged_team_stats_2016 mts
JOIN team_event_totals_season_20152016 tets
    ON mts."Team_ID" = tets."team_id";
"""

try:
    df = pd.read_sql(query, engine)
    print(df)
except Exception as e:
    print("Query execution failed:")
    print(e)

