import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Load environment variables from .env file
load_dotenv()

# Database connection parameters
DATABASE_TYPE = os.getenv("DATABASE_TYPE")
DBAPI = os.getenv("DBAPI")
ENDPOINT = os.getenv("ENDPOINT")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
PORT = int(os.getenv("PORT", 5432))
DATABASE = os.getenv("DATABASE", "hockey_stats")

# Create the connection string
connection_string = (
    f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}"
)

# Create the SQLAlchemy engine
engine = create_engine(connection_string)

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
