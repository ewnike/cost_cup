# def create_corsi_stats(df_corsi, df):
#     """
#     Function calculates Corsi statistics for individual players across different
#     games using a DataFrame (df_corsi) that contains player and game information.
#     Corsi is an advanced hockey statistic used to measure shot attempts and is often
#     used as a proxy for puck possession.
#     """
#     print("Entered create_corsi_stats")
#     df_corsi[["corsi_for", "corsi_against", "corsi"]] = np.nan

#     game_id_prev = None
#     shifts_game, plays_game = None, None
#     t1 = perf_counter()

#     for i, row in df_corsi.iterrows():
#         game_id, player_id, team_id = row.iloc[:3]

#         if i % 1000 == 0:
#             print(f"{i:>6}/{len(df_corsi)}, {perf_counter() - t1:.2f} s")

#         if pd.isna(game_id):
#             print(f"Skipping row with NaN game_id: {row}")
#             continue

#         if game_id != game_id_prev:
#             game_id_prev = game_id
#             shifts_game = df["game_shifts"].query(f"game_id == {game_id}")
#             plays_game = df["game_plays"].query(f"game_id == {game_id}")

#             gss = df["game_skater_stats"].query(f"game_id == {game_id}")
#             if 0 in [len(shifts_game), len(gss)]:
#                 print(f"game_id: {game_id}")
#                 print("Empty DF before Merge.")
#                 continue  # Skip to the next iteration if there's an empty DataFrame

#             df_num_players = get_penalty_exclude_times(shifts_game, gss).reset_index(
#                 drop=True
#             )
#             idx = df_num_players["time"].searchsorted(plays_game["time"]) - 1
#             idx[idx < 0] = 0
#             mask = df_num_players["exclude"][idx]
#             mask = mask.reset_index(drop=True).to_numpy()
#             plays_game = plays_game.loc[~mask]

#         shifts_player = shifts_game.query(f"player_id == {player_id}")
#         mask = (
#             shifts_player["shift_start"].searchsorted(plays_game["time"])
#             - shifts_player["shift_end"].searchsorted(plays_game["time"])
#         ).astype(bool)

#         plays_player = plays_game[mask]

#         # Calculate Corsi For and Corsi Against
#         # Regular Shot Events:
#         # - Corsi For (CF) should be attributed to the team that took the shot (team_id_for)
#         # - Corsi Against (CA) should be attributed to the opposing team (team_id_against)
#         corsi_for = (plays_player["team_id_for"] == team_id).sum()
#         corsi_against = (plays_player["team_id_against"] == team_id).sum()

#         # Special handling for Blocked Shots:
#         blocked_shots = plays_player.query("event == 'Blocked Shot'")
#         # For Blocked Shots:
#         # - CF should be assigned to the team in `team_id_against` (the shooting team)
#         # - CA should be assigned to the team in `team_id_for` (the defending team)

#         corsi_for += (blocked_shots["team_id_against"] == team_id).sum()  # CF for the team that took the shot
#         corsi_against += (blocked_shots["team_id_for"] == team_id).sum()  # CA for the team that blocked the shot

#         corsi = corsi_for - corsi_against
#         df_corsi.iloc[i, 3:] = [corsi_for, corsi_against, corsi]

#     df_corsi["CF_Percent"] = df_corsi["corsi_for"] / (
#         df_corsi["corsi_for"] + df_corsi["corsi_against"]
#     )

#     print(df_corsi.head())  # Print first few rows of df_corsi for debugging

#     if game_id_prev is not None:
#         print(f"Processed Corsi stats for game {game_id_prev}")


#     return df_corsi
# def create_corsi_stats(df_corsi, df):
#     """
#     Function calculates Corsi statistics for all players on the ice during events
#     for both Corsi For and Corsi Against.
#     """
#     print("Entered create_corsi_stats")
#     df_corsi[["corsi_for", "corsi_against", "corsi"]] = np.nan

#     # Access game_shifts from the df argument
#     game_shifts = df["game_shifts"]
#     shifts_game = df["game_shifts"]  # Access the shifts data
#     game_skater_stats = df["game_skater_stats"]
#     # skater_stats = game_skater_stats  # Access skater stats data

#     # Merge game_shifts with skater_stats to include team_id
#     shifts_game = pd.merge(
#         game_shifts,
#         df["game_skater_stats"][["player_id", "team_id"]],
#         on="player_id",
#         how="left",
#     )

#     game_id_prev = None
#     shifts_game, plays_game = None, None
#     t1 = perf_counter()

#     for i, row in df_corsi.iterrows():
#         game_id, player_id, team_id = row.iloc[:3]

#         if i % 1000 == 0:
#             print(f"{i:>6}/{len(df_corsi)}, {perf_counter() - t1:.2f} s")

#         if pd.isna(game_id):
#             print(f"Skipping row with NaN game_id: {row}")
#             continue

#         if game_id != game_id_prev:
#             game_id_prev = game_id
#             shifts_game = df["game_shifts"].query(f"game_id == {game_id}")
#             plays_game = df["game_plays"].query(f"game_id == {game_id}")

#             gss = df["game_skater_stats"].query(f"game_id == {game_id}")
#             if 0 in [len(shifts_game), len(gss)]:
#                 print(f"game_id: {game_id}")
#                 print("Empty DF before Merge.")
#                 continue  # Skip to the next iteration if there's an empty DataFrame

#             df_num_players = get_penalty_exclude_times(shifts_game, gss).reset_index(
#                 drop=True
#             )
#             idx = df_num_players["time"].searchsorted(plays_game["time"]) - 1
#             idx[idx < 0] = 0
#             mask = df_num_players["exclude"][idx]
#             mask = mask.reset_index(drop=True).to_numpy()
#             plays_game = plays_game.loc[~mask]

#         shifts_player = shifts_game.query(f"player_id == {player_id}")
#         mask = (
#             shifts_player["shift_start"].searchsorted(plays_game["time"])
#             - shifts_player["shift_end"].searchsorted(plays_game["time"])
#         ).astype(bool)

#         plays_player = plays_game[mask]

#         # Merge game_shifts with game_skater_stats to include team_id in shifts_game
#         shifts_game = pd.merge(
#             df["game_shifts"],  # Access game_shifts from df
#             game_skater_stats[
#                 ["player_id", "team_id"]
#             ],  # Merge with game_skater_stats on player_id
#             on="player_id",
#             how="left",
#         )

#         # Iterate through all plays and assign Corsi For and Corsi Against to all players on the ice
#         for _, play in plays_player.iterrows():
#             event_team_id = play["team_id_for"]  # Team that attempted the shot
#             opposing_team_id = play["team_id_against"]  # Team that was defending

#             # Get all players on the ice for both teams during the event
#             players_on_ice_for = shifts_game.query(
#                 f"team_id == {event_team_id} and shift_start <= {play['time']} and shift_end >= {play['time']}"
#             )["player_id"]
#             players_on_ice_against = shifts_game.query(
#                 f"team_id == {opposing_team_id} and shift_start <= {play['time']} and shift_end >= {play['time']}"
#             )["player_id"]

#             # Special handling for Blocked Shots
#             if play["event"] == "Blocked Shot":
#                 # Corsi For should be assigned to `team_id_against` (shooting team)
#                 # Corsi Against should be assigned to `team_id_for` (blocking team)
#                 players_on_ice_for, players_on_ice_against = (
#                     players_on_ice_against,
#                     players_on_ice_for,
#                 )

#             # Assign Corsi For to all players on the ice for the shooting team
#             df_corsi.loc[
#                 df_corsi["player_id"].isin(players_on_ice_for), "corsi_for"
#             ] = df_corsi["corsi_for"].fillna(0) + 1

#             # Assign Corsi Against to all players on the ice for the defending team
#             df_corsi.loc[
#                 df_corsi["player_id"].isin(players_on_ice_against), "corsi_against"
#             ] = df_corsi["corsi_against"].fillna(0) + 1

#         # Calculate individual Corsi (Corsi For - Corsi Against)
#         df_corsi["corsi"] = df_corsi["corsi_for"] - df_corsi["corsi_against"]

#     # Calculate Corsi percentage (CF%)
#     df_corsi["CF_Percent"] = df_corsi["corsi_for"] / (
#         df_corsi["corsi_for"] + df_corsi["corsi_against"]
#     )

#     print(df_corsi.head())  # Print first few rows of df_corsi for debugging

#     if game_id_prev is not None:
#         print(f"Processed Corsi stats for game {game_id_prev}")

#     return df_corsi

# def create_corsi_stats(df_corsi, df):
#     from time import perf_counter
#     t1 = perf_counter()

#     print("Entered create_corsi_stats")

#     # Access game_shifts and game_skater_stats from df
#     game_shifts = df["game_shifts"]
#     game_skater_stats = df["game_skater_stats"]

#     # Merge game_shifts with game_skater_stats to include team_id in shifts_game
#     shifts_game = pd.merge(
#         game_shifts,
#         game_skater_stats[["player_id", "team_id"]],
#         on="player_id",
#         how="left"
#     )

#     t2 = perf_counter()
#     print(f"Time after merging shifts: {t2 - t1:.2f} s")

#     # Initialize 'corsi_for', 'corsi_against', and 'corsi' columns to 0
#     df_corsi["corsi_for"] = 0
#     df_corsi["corsi_against"] = 0
#     df_corsi["corsi"] = 0

#     # Iterate over df_corsi rows
#     for i, row in df_corsi.iterrows():
#         game_id, player_id, team_id = row.iloc[:3]

#         if i % 1000 == 0:
#             print(f"{i:>6}/{len(df_corsi)}, {perf_counter() - t2:.2f} s")

#         if pd.isna(game_id):
#             print(f"Skipping row with NaN game_id: {row}")
#             continue

#         # Filter game data for the current game_id
#         plays_game = df["game_plays"].query(f"game_id == {game_id}")

#         # Pre-filter shifts_game for the player's team and the opposing team
#         shifts_team_for = shifts_game[(shifts_game["game_id"] == game_id) & (shifts_game["team_id"] == team_id)]
#         shifts_team_against = shifts_game[(shifts_game["game_id"] == game_id) & (shifts_game["team_id"] != team_id)]

#         # Process each play in the game
#         for _, play in plays_game.iterrows():
#             play_time = play["time"]
#             event_team_id = play["team_id_for"]
#             opposing_team_id = play["team_id_against"]

#             # Boolean index to find players on ice for the shooting team
#             players_on_ice_for = shifts_team_for[
#                 (shifts_team_for["shift_start"] <= play_time) &
#                 (shifts_team_for["shift_end"] >= play_time)
#             ]["player_id"]

#             # Boolean index to find players on ice for the defending team
#             players_on_ice_against = shifts_team_against[
#                 (shifts_team_against["shift_start"] <= play_time) &
#                 (shifts_team_against["shift_end"] >= play_time)
#             ]["player_id"]

#             # Special handling for Blocked Shots
#             if play["event"] == "Blocked Shot":
#                 players_on_ice_for, players_on_ice_against = players_on_ice_against, players_on_ice_for

#             # Tally Corsi For (CF) for all players on the ice for the shooting team
#             df_corsi.loc[df_corsi["player_id"].isin(players_on_ice_for), "corsi_for"] += 1

#             # Tally Corsi Against (CA) for all players on the ice for the defending team
#             df_corsi.loc[df_corsi["player_id"].isin(players_on_ice_against), "corsi_against"] += 1

#     t3 = perf_counter()
#     print(f"Finished processing Corsi stats: {t3 - t1:.2f} s")

#     return df_corsi
# def create_corsi_stats(df_corsi, df):
#     from time import perf_counter
#     t1 = perf_counter()

#     print("Entered create_corsi_stats")

#     # Access game_shifts and game_skater_stats from df
#     game_shifts = df["game_shifts"]
#     game_skater_stats = df["game_skater_stats"]

#     # Merge game_shifts with game_skater_stats to include team_id in shifts_game
#     shifts_game = pd.merge(
#         game_shifts,
#         game_skater_stats[["player_id", "team_id"]],
#         on="player_id",
#         how="left"
#     )

#     t2 = perf_counter()
#     print(f"Time after merging shifts: {t2 - t1:.2f} s")

#     # Initialize 'corsi_for', 'corsi_against', and 'corsi' columns to 0
#     df_corsi["corsi_for"] = 0
#     df_corsi["corsi_against"] = 0
#     df_corsi["corsi"] = 0

#     # Only process the first 250 rows for testing purposes
#     df_corsi_subset = df_corsi.head(250)

#     # Iterate over df_corsi_subset rows
#     for i, row in df_corsi_subset.iterrows():
#         game_id, player_id, team_id = row.iloc[:3]

#         if i % 100 == 0:
#             print(f"{i:>6}/{len(df_corsi_subset)}, {perf_counter() - t2:.2f} s")

#         if pd.isna(game_id):
#             print(f"Skipping row with NaN game_id: {row}")
#             continue

#         # Filter game data for the current game_id
#         plays_game = df["game_plays"].query(f"game_id == {game_id}")

#         # Pre-filter shifts_game for the player's team and the opposing team
#         shifts_team_for = shifts_game[(shifts_game["game_id"] == game_id) & (shifts_game["team_id"] == team_id)]
#         shifts_team_against = shifts_game[(shifts_game["game_id"] == game_id) & (shifts_game["team_id"] != team_id)]

#         # Process each play in the game
#         for _, play in plays_game.iterrows():
#             play_time = play["time"]
#             event_team_id = play["team_id_for"]  # Team that took the shot
#             opposing_team_id = play["team_id_against"]  # Team that was defending

#             # Boolean index to find players on ice for the shooting team (CF)
#             players_on_ice_for = shifts_team_for[
#                 (shifts_team_for["shift_start"] <= play_time) &
#                 (shifts_team_for["shift_end"] >= play_time) &
#                 (shifts_team_for["team_id"] == event_team_id)  # Ensure it's the shooting team
#             ]["player_id"]

#             # Boolean index to find players on ice for the defending team (CA)
#             players_on_ice_against = shifts_team_against[
#                 (shifts_team_against["shift_start"] <= play_time) &
#                 (shifts_team_against["shift_end"] >= play_time) &
#                 (shifts_team_against["team_id"] == opposing_team_id)  # Ensure it's the defending team
#             ]["player_id"]

#             # Special handling for Blocked Shots (assign CF and CA correctly)
#             if play["event"] == "Blocked Shot":
#                 players_on_ice_for, players_on_ice_against = players_on_ice_against, players_on_ice_for

#             # Only assign CF to players on the shooting team
#             df_corsi.loc[df_corsi["player_id"].isin(players_on_ice_for), "corsi_for"] += 1

#             # Only assign CA to players on the defending team
#             df_corsi.loc[df_corsi["player_id"].isin(players_on_ice_against), "corsi_against"] += 1

#     # Calculate Corsi (CF - CA) for each player
#     df_corsi["corsi"] = df_corsi["corsi_for"] - df_corsi["corsi_against"]

#     # Save the subset to a CSV file for review
#     df_corsi_subset.to_csv('corsi_subset_250.csv', index=False)
#     print("Subset of 250 rows saved as 'corsi_subset_250.csv'")

#     # End the program after processing and saving the subset
#     return
