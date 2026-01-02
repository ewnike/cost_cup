import pandas as pd
from pathlib import Path
import os


def stitch_together():
    """Stitch dfs from same season together."""
    project_dir = Path(__file__).resolve().parent
    data_dir = project_dir / "data_pbp_raw"
    data_dir.mkdir(parents=True, exist_ok=True)
    print("project_dir:", project_dir.resolve())
    print("data_dir:", data_dir.resolve())

    df1 = pd.read_csv(r"/Users/ericwiniecke/Documents/github/cost_cup/pbp_20242025A.csv")
    df2 = pd.read_csv(r"/Users/ericwiniecke/Documents/github/cost_cup/pbp_20242025B.csv")
    df3 = pd.read_csv(r"/Users/ericwiniecke/Documents/github/cost_cup/pbp_20242025C.csv")

    out = pd.concat([df1, df2, df3], ignore_index=True)

    out_path = data_dir / "pbp_20242025.csv"
    out.to_csv(out_path, index=False)
    print("hello")
    print(f"Saved to: {out_path.resolve()}")
    return out_path


if __name__ == "__main__":
    stitch_together()
