"""
October 30, 2024.
Code to test and debug Corsi Calculations.
Tested tracking a single player and all players
Logic for tallying Blocked Shots assigns correctly.
Blocked For is -1 for the blocking team and a +1 for
the against team. (Corsi is an offensive statistic that
has a main purpose of calculating all shot attempts
taken when a player is on the ice. Not a defensive stat).
"""

import logging
import os

import pandas as pd

from load_data import get_env_vars, load_data

# Set up logging with explicit confirmation of path
log_file_path = "/Users/ericwiniecke/Documents/github/cost_cup/data_processing.log"
log_directory = os.path.dirname(log_file_path)
if not os.path.exists(log_directory):
    os.makedirs(log_directory)
    print(f"Created log directory: {log_directory}")
else:
    print(f"Log directory exists: {log_directory}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(log_file_path, mode="w"), logging.StreamHandler()],
)

# Test to confirm logger output
logging.info("Logger configured successfully. Test message to ensure logging works.")
print(f"Logging to file: {log_file_path}")

# Function Definitions


def get_num_players(shift_df):
    shifts_melted = pd.melt(
        shift_df,
        id_vars=["game_id", "player_id"],
        value_vars=["shift_start", "shift_end"],
    ).sort_values("value", ignore_index=True)
    shifts_melted["change"] = (
        2 * (shifts_melted["variable"] == "shift_start").astype(int) - 1
    )
    shifts_melted["num_players"] = shifts_melted["change"].cumsum()
    df_num_players = shifts_melted.groupby("value")["num_players"].last().reset_index()
    return df_num_players[
        df_num_players["num_players"].shift() != df_num_players["num_players"]
    ].reset_index(drop=True)


def get_penalty_exclude_times(game_shifts, game_skater_stats):
    if game_shifts.empty:
        logging.warning("Warning: game_shifts is empty in get_penalty_exclude_times")
        return pd.DataFrame()  # Return an empty DataFrame if no shifts are available

    # Merge the `team_id` column from `game_skater_stats` into `game_shifts`
    game_shifts = pd.merge(
        game_shifts,
        game_skater_stats[["game_id", "player_id", "team_id"]],
        on=["game_id", "player_id"],
        how="left",
    )
    game_shifts = game_shifts.drop(columns=["team_id_y"]).rename(
        columns={"team_id_x": "team_id"}
    )

    # Divide shifts by team
    team_1 = game_shifts.iloc[0]["team_id"]
    mask = game_shifts["team_id"] == team_1
    shifts_1 = game_shifts[mask]
    shifts_2 = game_shifts[~mask]

    # Calculate the number of players on each team and proceed as before
    df_num_players_1 = get_num_players(shifts_1)
    df_num_players_2 = get_num_players(shifts_2)

    # Rename and merge the player counts for each team
    df_num_players_1 = df_num_players_1.rename(
        columns={"value": "time", "num_players": "team_1"}
    )
    df_num_players_2 = df_num_players_2.rename(
        columns={"value": "time", "num_players": "team_2"}
    )

    df_exclude = pd.concat([df_num_players_1, df_num_players_2]).sort_values(
        "time", ignore_index=True
    )
    df_exclude = df_exclude.ffill()

    mask = df_exclude["time"].shift(-1) != df_exclude["time"]
    df_exclude = df_exclude[mask]

    # Determine exclusions based on player counts
    diff = df_exclude["team_1"] != df_exclude["team_2"]
    missing = (df_exclude["team_1"] < 5) | (df_exclude["team_2"] < 5)
    df_exclude["exclude"] = diff & missing
    df_exclude = df_exclude.reset_index(drop=True)

    # Log the penalty exclude times for verification
    logging.info("Penalty Exclude Times:")
    for _, row in df_exclude.iterrows():
        logging.info(
            f"Time: {row['time']}, Team 1 Players: {row['team_1']}, Team 2 Players: {row['team_2']}, Exclude: {row['exclude']}"
        )

    return df_exclude


def calculate_and_save_corsi_stats(season_game_ids, season):
    env_vars = get_env_vars()
    df_master = load_data(env_vars)

    if (
        "game_plays" not in df_master
        or "game_shifts" not in df_master
        or "game_skater_stats" not in df_master
    ):
        logging.error(
            "One or more required dataframes are missing from the loaded data."
        )
        return

    season_corsi_stats = []

    for game_id in season_game_ids:
        # Filter data by the current game_id
        df_game = {name: df[df["game_id"] == game_id] for name, df in df_master.items()}

        # Skip if any game-specific DataFrame is empty
        if (
            df_game["game_shifts"].empty
            or df_game["game_plays"].empty
            or df_game["game_skater_stats"].empty
        ):
            logging.warning(
                f"Skipping game {game_id}: One or more required DataFrames are empty."
            )
            continue

        # Calculate Corsi stats for the current game
        df_corsi = df_game["game_skater_stats"][
            ["game_id", "player_id", "team_id"]
        ].copy()
        corsi_stats = create_corsi_stats(df_corsi, df_game)

        if corsi_stats is not None and not corsi_stats.empty:
            season_corsi_stats.append(corsi_stats)
            logging.info(f"Completed Corsi calculation for game {game_id}.")

    # Combine all game data into a single DataFrame
    if season_corsi_stats:
        final_season_df = pd.concat(season_corsi_stats, ignore_index=True)
        output_dir = os.path.join(
            os.getcwd(), "corsi_stats"
        )  # Relative to current working directory
        os.makedirs(output_dir, exist_ok=True)  # Ensure directory exists

        output_file = os.path.join(
            output_dir, f"corsi_stats_{season}.csv"
        )  # Set output path
        final_season_df.to_csv(output_file, index=False)
        logging.info(f"Saved Corsi data for the {season} season to {output_file}.")
    else:
        logging.warning(f"No valid Corsi data was generated for the {season} season.")


def organize_by_season(seasons, df):
    df_orig, nhl_dfs = df, []
    game_id = 2015020002

    for season in seasons:
        df = df_orig.copy()
        df["game"] = df["game"].query(f"season == {season} and game_id == {game_id}")
        if df["game"].empty:
            logging.warning(f"Game ID {game_id} not found in season {season}.")
            continue

        for name in ["game_skater_stats", "game_plays", "game_shifts"]:
            df[name] = pd.merge(
                df[name][df[name]["game_id"] == game_id],
                df["game"][["game_id"]],
                on="game_id",
            ).drop_duplicates()

        df_corsi = df["game_skater_stats"].sort_values(
            ["game_id", "player_id"], ignore_index=True
        )[["game_id", "player_id", "team_id"]]
        nhl_dfs.append([season, create_corsi_stats(df_corsi, df)])

    return nhl_dfs


def create_corsi_stats(df_corsi, df):
    logging.info("Entered create_corsi_stats")

    # Initialize Corsi statistics columns with zeros for all players
    df_corsi[["corsi_for", "corsi_against", "corsi"]] = 0
    relevant_events = ["Shot", "Blocked Shot", "Missed Shot", "Goal"]

    # Ensure the 'time' column is created and available in game_plays
    if "game_plays" in df:
        if (
            "periodTime" in df["game_plays"].columns
            and "period" in df["game_plays"].columns
        ):
            # Calculate 'time' based on period and periodTime
            df["game_plays"]["time"] = (
                df["game_plays"]["periodTime"] + (df["game_plays"]["period"] - 1) * 1200
            )
            logging.info("Calculated 'time' column in game_plays.")
        else:
            logging.error("'periodTime' or 'period' column missing in game_plays.")
            return

        # Filter game_plays to include only relevant events
        game_plays = df["game_plays"][df["game_plays"]["event"].isin(relevant_events)]
        game_plays = game_plays.dropna(subset=["team_id_for", "team_id_against"])

        # Verify 'time' column presence after filtering
        if "time" not in game_plays.columns:
            logging.error(
                "The 'time' column is missing from game_plays after filtering."
            )
            return
        else:
            logging.info("Verified 'time' column exists in game_plays after filtering.")
    else:
        logging.error("'game_plays' DataFrame missing in df.")
        return

    game_id_prev = None

    for i, row in df_corsi.iterrows():
        game_id, player_id, team_id = row.iloc[:3]

        if game_id != game_id_prev:
            game_id_prev = game_id

            # Filter game_shifts and game_plays for this game_id
            game_shifts = df["game_shifts"].query(f"game_id == {game_id}")
            plays_game = df["game_plays"].query(f"game_id == {game_id}")

            # Log the 'time' column in plays_game to verify its presence
            if "time" not in plays_game.columns:
                logging.error(
                    "'time' column missing in plays_game after filtering by game_id."
                )
                return
            else:
                logging.info(
                    f"'time' column verified in plays_game for game_id {game_id}."
                )

            # Continue with the rest of the function as needed, adding further logs as appropriate

            # Merge team_id into game_shifts and ensure team_id is an integer after merging
            game_shifts = pd.merge(
                game_shifts,
                df["game_skater_stats"][["game_id", "player_id", "team_id"]],
                on=["game_id", "player_id"],
                how="left",
            )

            game_shifts["shift_start"] = game_shifts["shift_start"].astype(int)
            game_shifts["shift_end"] = game_shifts["shift_end"].astype(int)

            gss = df["game_skater_stats"].query(f"game_id == {game_id}")
            df_num_players = get_penalty_exclude_times(game_shifts, gss).reset_index(
                drop=True
            )

            idx = df_num_players["time"].searchsorted(plays_game["time"]) - 1
            idx[idx < 0] = 0
            mask = df_num_players["exclude"][idx].reset_index(drop=True).to_numpy()
            plays_game = plays_game.loc[~mask]

    for _, event in plays_game.iterrows():
        event_time = event["time"]
        team_for = event["team_id_for"]
        team_against = event["team_id_against"]

        # Find all players on ice for both teams at this event time
        players_on_ice = game_shifts[
            (game_shifts["shift_start"] <= event_time)
            & (game_shifts["shift_end"] >= event_time)
        ]

        # Separate players by team
        players_for_team = players_on_ice[players_on_ice["team_id"] == team_for]
        players_against_team = players_on_ice[players_on_ice["team_id"] == team_against]

        # Log the number of players on each team at this event
        logging.info(
            f"Event: {event['event']} at time {event_time} - "
            f"Players for team {team_for} on ice: {len(players_for_team)}, "
            f"Players against team {team_against} on ice: {len(players_against_team)}"
        )

        # Log player IDs for detailed tracking
        logging.info(
            f"Player IDs for team {team_for} on ice: {players_for_team['player_id'].tolist()}"
        )
        logging.info(
            f"Player IDs for team {team_against} on ice: {players_against_team['player_id'].tolist()}"
        )

        # Corsi calculations and log updates
        if event["event"] in ["Shot", "Goal", "Missed Shot"]:
            df_corsi.loc[
                df_corsi["player_id"].isin(players_for_team["player_id"]), "corsi_for"
            ] += 1
            df_corsi.loc[
                df_corsi["player_id"].isin(players_against_team["player_id"]),
                "corsi_against",
            ] += 1

            logging.info(
                f"{event['event']} - CF updated for players on team {team_for} "
                f"and CA updated for players on team {team_against}"
            )

        elif event["event"] == "Blocked Shot":
            df_corsi.loc[
                df_corsi["player_id"].isin(players_for_team["player_id"]),
                "corsi_against",
            ] += 1
            df_corsi.loc[
                df_corsi["player_id"].isin(players_against_team["player_id"]),
                "corsi_for",
            ] += 1

            logging.info(
                f"Blocked Shot - CA updated for players on team {team_for} "
                f"and CF updated for players on team {team_against}"
            )

        # After the loop processing `plays_game` and updating `df_corsi`
    df_corsi["corsi"] = df_corsi["corsi_for"] - df_corsi["corsi_against"]

    df_corsi["CF_Percent"] = (
        (
            (
                df_corsi["corsi_for"]
                / (df_corsi["corsi_for"] + df_corsi["corsi_against"])
            )
            * 100
        )
        .fillna(0)
        .round(4)
    )

    # Final log summary
    logging.info("Corsi calculations complete. Summary of first few rows:")
    logging.info(df_corsi.head(5))
    return df_corsi


if __name__ == "__main__":
    env_vars = get_env_vars()
    df_master = load_data(env_vars)

    if "game" in df_master:
        # List of seasons to process
        seasons = [20152016, 20162017, 20172018]

        for season in seasons:
            # Filter for the current season and get unique game IDs
            season_game_ids = (
                df_master["game"]
                .loc[df_master["game"]["season"] == season, "game_id"]
                .unique()
            )

            if len(season_game_ids) > 0:
                logging.info(
                    f"Found {len(season_game_ids)} games for the {season} season."
                )
                # Pass the game IDs and the season to the function to process and save Corsi stats
                calculate_and_save_corsi_stats(season_game_ids, season)
            else:
                logging.warning(f"No games found for the {season} season. Skipping.")
    else:
        logging.error(
            "The 'game' DataFrame is missing from the loaded data. Cannot proceed."
        )
