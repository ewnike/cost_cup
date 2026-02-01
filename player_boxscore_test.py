from sqlalchemy import text
import pandas as pd
from db_utils import get_db_engine
from schema_utils import fq

season = 20182019
engine = get_db_engine()
try:
    with engine.connect() as conn:
        df = pd.read_sql_query(
            text(f"SELECT * FROM {fq('derived', f'game_plays_{season}_from_raw_pbp')} LIMIT 5"),
            conn,
        )
    print(df.columns.tolist())
finally:
    engine.dispose()
