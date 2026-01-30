"""Scratch Code File."""

# import os
# from pathlib import Path

# import pandas as pd

from load_data import load_data

# def stitch_together():
#     """Stitch dfs from same season together."""
#     project_dir = Path(__file__).resolve().parent
#     data_dir = project_dir / "data_pbp_raw"
#     data_dir.mkdir(parents=True, exist_ok=True)
#     print("project_dir:", project_dir.resolve())
#     print("data_dir:", data_dir.resolve())

#     df1 = pd.read_csv(r"/Users/ericwiniecke/Documents/github/cost_cup/pbp_20242025A.csv")
#     df2 = pd.read_csv(r"/Users/ericwiniecke/Documents/github/cost_cup/pbp_20242025B.csv")
#     df3 = pd.read_csv(r"/Users/ericwiniecke/Documents/github/cost_cup/pbp_20242025C.csv")

#     out = pd.concat([df1, df2, df3], ignore_index=True)

#     out_path = data_dir / "pbp_20242025.csv"
#     out.to_csv(out_path, index=False)
#     print("hello")
#     print(f"Saved to: {out_path.resolve()}")
#     return out_path


def tested():
    """Docstring for tested."""
    df = load_data(game_id=2015020001, debug_print_head=True)
    print({k: len(v) for k, v in df.items()})


if __name__ == "__main__":
    tested()
