"""Docstring for stats_utils."""

import os
import pathlib

import pandas as pd

if os.getenv("DEBUG_IMPORTS") == "1":
    print(f"[IMPORT] {__name__} -> {pathlib.Path(__file__).resolve()}")


def add_corsi_rates_and_merge(
    df_season: pd.DataFrame,
    df_corsi: pd.DataFrame,
) -> pd.DataFrame:
    """
    Add CF60/CA60/CF_pct to df_corsi, then left-merge into df_season on player_id+season.

    Expects df_corsi to have: time_on_ice, corsi_for, corsi_against, cf_percent, cap_hit.
    """
    required = {
        "player_id",
        "season",
        "time_on_ice",
        "corsi_for",
        "corsi_against",
        "cf_percent",
        "cap_hit",
    }
    missing = required - set(df_corsi.columns)
    if missing:
        raise KeyError(f"df_corsi missing required columns: {sorted(missing)}")

    df_corsi = df_corsi.copy()
    df_corsi["toi_corsi_min"] = df_corsi["time_on_ice"] / 60.0
    df_corsi = df_corsi[df_corsi["toi_corsi_min"] > 0].copy()
    df_corsi["CF60"] = df_corsi["corsi_for"] / df_corsi["toi_corsi_min"] * 60.0
    df_corsi["CA60"] = df_corsi["corsi_against"] / df_corsi["toi_corsi_min"] * 60.0
    df_corsi["CF_pct"] = df_corsi["cf_percent"]

    return df_season.merge(
        df_corsi[["player_id", "season", "CF60", "CA60", "CF_pct", "cap_hit"]],
        on=["player_id", "season"],
        how="left",
    )
