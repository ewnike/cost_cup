import glob
import os
import pandas as pd
from sqlalchemy import text
from db_utils import get_db_engine

CSV_GLOB = "player_cap_hits/player_cap_hits_*.csv"  # your output dir/files


def season_from_filename(path: str) -> int:
    # player_cap_hits_2018.csv -> 2018
    base = os.path.basename(path)
    year_str = base.split("_")[-1].split(".")[0]
    return int(year_str)


def main():
    engine = get_db_engine()
    files = sorted(glob.glob(CSV_GLOB))

    if not files:
        raise FileNotFoundError(f"No files matched {CSV_GLOB}")

    with engine.begin() as conn:
        for path in files:
            season_year = season_from_filename(path)
            df = pd.read_csv(path)

            # basic cleanup
            df["season_year"] = season_year
            df["capHit"] = pd.to_numeric(df["capHit"], errors="coerce")
            df["spotrac_url"] = df["spotrac_url"].astype(str)

            # insert via executemany (fast enough for ~1k rows)
            records = df[["season_year", "spotrac_url", "firstName", "lastName", "capHit"]].to_dict(
                "records"
            )

            conn.execute(
                text("""
                    INSERT INTO public.player_cap_hit_spotrac
                      (season_year, spotrac_url, "firstName", "lastName", "capHit")
                    VALUES
                      (:season_year, :spotrac_url, :firstName, :lastName, :capHit)
                    ON CONFLICT (season_year, spotrac_url) DO UPDATE
                    SET "firstName" = EXCLUDED."firstName",
                        "lastName"  = EXCLUDED."lastName",
                        "capHit"    = EXCLUDED."capHit";
                """),
                records,
            )

            print(f"Loaded {len(df)} rows for {season_year} from {path}")

    engine.dispose()


if __name__ == "__main__":
    main()
