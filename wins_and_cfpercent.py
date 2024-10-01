import pandas as pd
from sqlalchemy import text

from db_utils import get_db_engine, get_metadata

engine = get_db_engine()
metadata = get_metadata()

# Define the SQL query
sql_query = """
SELECT
	mt."Abbreviation",
    mt."PTS",
    mt."Total_Payroll",
    ROUND(AVG(ac."CF_Percent")::numeric, 3) AS avg_cf_percent
FROM public.aggregated_corsi_20152016 ac
JOIN public.merged_team_stats_2016 mt ON ac.team_id = mt."Team_ID"
GROUP BY ac.team_id,mt."Abbreviation", mt."PTS", mt."Total_Payroll"
ORDER BY mt."PTS" DESC;
"""

# Execute the query and fetch the results into a Pandas DataFrame
with engine.connect() as connection:
    result = connection.execute(text(sql_query))
    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    df_pts_cf_percent_2016 = df

# Display the DataFrame
print(df_pts_cf_percent_2016)
