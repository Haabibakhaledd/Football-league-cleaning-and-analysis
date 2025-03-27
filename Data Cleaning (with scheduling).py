# imports
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from pyspark.sql import SparkSession
import schedule
import time
from datetime import datetime
import logging



# Define the task to be scheduled
def process_data_task():

    # Create a connection to PostgreSQL
    engine = create_engine('postgresql+psycopg2://postgres:postgres@localhost:5432/Task3')
    
    from tabulate import tabulate
    # Create and configure log file
    file_path = r"C:\Users\Habiba Khaled\Desktop\Atos\Task3\data_cleaning.log"

    # Configure the logger
    logging.basicConfig(
        filename=file_path,
        filemode='w',  
        level=logging.INFO, 
        format='%(message)s', 
    )

    # Initialize the log table
    cleaning_log = []
    table_headers = ["Table ID", "Table Name", "Cleaning Reason"]

    def log_cleaned_row(row_id, table_name, reason):
        cleaning_log.append({"Table ID": row_id, "Table Name": table_name, "Cleaning Reason": reason})
        table_rows = [[entry["Table ID"], entry["Table Name"], entry["Cleaning Reason"]] for entry in cleaning_log]
        table_string = tabulate(table_rows, headers=table_headers, tablefmt="grid")
        
        # writing to the log file
        with open(file_path, 'w') as log_file:
            log_file.write("Data Cleaning Log\n")
            log_file.write(table_string)


    # Load CSV files
    teams_df = pd.read_csv(r"C:\Users\Habiba Khaled\Desktop\Atos\Task3\CSVs\teams.csv")
    players_df = pd.read_csv(r"C:\Users\Habiba Khaled\Desktop\Atos\Task3\CSVs\players.csv")
    matches_df = pd.read_csv(r"C:\Users\Habiba Khaled\Desktop\Atos\Task3\CSVs\matches.csv")
    transfers_df = pd.read_csv(r"C:\Users\Habiba Khaled\Desktop\Atos\Task3\CSVs\transfers.csv")
    stats_df = pd.read_csv(r"C:\Users\Habiba Khaled\Desktop\Atos\Task3\CSVs\player_stats.csv")

    # Rename columns in the dataframe
    teams_df.rename(columns={
        "TeamID": "team_id",
        "TeamName" : "team_name",
        "FoundedYear": "founded_year",
        "HomeCity" : "home_city",
        "ManagerName":"manager_name",
        "StadiumName" : "stadium_name",
        "StadiumCapacity": "stadium_capacity",
        "Country" : "country"
        }, inplace=True)

    players_df.rename(columns={
        'PlayerID': 'player_id',
        'TeamID': 'team_id',
        'Name': 'player_name',
        'Position': 'position',
        'DateOfBirth': 'birthdate',
        'Nationality': 'nationality',
        'ContractUntil': 'contract_until',
        'MarketValue': 'market_value'
    }, inplace=True)

    matches_df.rename(columns={
        'MatchID': 'match_id',
        'Date': 'match_date',
        'HomeTeamID': 'home_team_id',
        'AwayTeamID': 'away_team_id',
        'HomeTeamScore': 'home_team_score',
        'AwayTeamScore': 'away_team_score',
        'Stadium': 'stadium',
        'Referee': 'referee'
    }, inplace=True)

    stats_df.rename(columns={
        'StatID': 'stat_id',
        'PlayerID': 'player_id',
        'MatchID': 'match_id',
        'Goals': 'goals',
        'Assists': 'assists',
        'YellowCards': 'yellow_cards',
        'RedCards': 'red_cards',
        'MinutesPlayed': 'mins_played'
    }, inplace=True)

    transfers_df.rename(columns={
        'TransferID': 'trans_id',
        'PlayerID': 'player_id',
        'FromTeamID': 'from_team_id',
        'ToTeamID': 'to_team_id',
        'TransferDate': 'trans_date',
        'TransferFee': 'trans_fee',
        'ContractDuration': 'contract_duration'
    }, inplace=True)

    # Fetch existing data from the database
    existing_teams = pd.read_sql_table("teams", con=engine)
    existing_players = pd.read_sql_table("players", con=engine)
    existing_matches = pd.read_sql_table("matches", con=engine)
    existing_stats = pd.read_sql_table("player_stats", con=engine)
    existing_transfers = pd.read_sql_table("transfer_history", con=engine)

    # Player history

    # Identify rows that were deleted
    deleted_players = existing_players.loc[~existing_players['player_id'].isin(players_df['player_id'])]

    # Set 'player_id' as the index for both DataFrames
    existing_players = existing_players.set_index('player_id')
    players_df = players_df.set_index('player_id')

    # Identify rows that were updated
    common_indices = existing_players.index.intersection(players_df.index)

    # Compare and get the rows where values have changed
    updated_players = existing_players.loc[common_indices].ne(players_df.loc[common_indices])

    # Get the rows where there are differences
    updated_players = existing_players[updated_players.any(axis=1)]

    # Reset indices
    updated_players = updated_players.reset_index()
    existing_players = existing_players.reset_index()
    players_df = players_df.reset_index()

    # Concatenate deleted and updated players into the history table
    history_players = pd.concat([deleted_players, updated_players], ignore_index=True)

    # Add a column to mark whether the operation is 'Deleted' or 'Updated'
    history_players['operation'] = history_players.apply(
        lambda row: 'Deleted' if row['player_id'] in deleted_players['player_id'] else 'Updated', axis=1)

    # Add a timestamp for when the operation was logged
    history_players['Time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Store the history in the history table
    history_players.to_sql("players_history", con=engine, if_exists="append", index=False)

    # Synchronize Data

    # If there is an updated row, the updated one is inserted instead of old
    # If there is a new row, it will be inserted
    synced_teams = pd.concat([existing_teams, teams_df]).drop_duplicates(subset="team_id", keep="last")
    synced_players = pd.concat([existing_players, players_df]).drop_duplicates(subset="player_id", keep="last")
    synced_matches = pd.concat([existing_matches, matches_df]).drop_duplicates(subset="match_id", keep="last")
    synced_stats = pd.concat([existing_stats, stats_df]).drop_duplicates(subset="stat_id", keep="last")
    synced_transfers = pd.concat([existing_transfers, transfers_df]).drop_duplicates(subset="trans_id", keep="last")

    # Handle the FK constraint in transfer history

    # get player ids that exist in table players
    valid_player_ids = existing_players['player_id'].tolist()


    # create df for rows that have player ids not in players
    invalid_rows = synced_transfers[~synced_transfers['player_id'].isin(valid_player_ids)].copy()
    invalid_rows['reason'] = 'ForeignKeyViolation: player_id not present in players table'

    # Insert Invalid Rows into Error Table and log file
    invalid_rows.to_sql('transfer_history_errors', engine, if_exists='replace', index=False)
    for _, row in invalid_rows.iterrows():
        log_cleaned_row(
            row['trans_id'], 
            "Player Transfers",
            "ForeignKeyViolation: player_id not present in players table"
        )

    # Remove Invalid Rows from Original DataFrame
    synced_transfers = synced_transfers[synced_transfers['player_id'].isin(valid_player_ids)]


    # Handle the FK constraint in player stats
    
    # get player ids that exist in table players
    valid_player_ids = existing_players['player_id'].tolist()
    # valid_match_ids = existing_matches['match_id'].tolist()

    # create df for rows that have player ids not in players
    invalid_player = synced_stats[~synced_stats['player_id'].isin(valid_player_ids)].copy()
    invalid_player['reason'] = 'ForeignKeyViolation: player_id not present in players table'

    # invalid_matches = synced_matches[~synced_matches['match_id'].isin(valid_match_ids)].copy()
    # invalid_matches['reason'] = 'ForeignKeyViolation: match_id not present in matches table'

    # Insert Invalid Rows into Error Table and log file
    invalid_player.to_sql('player_stats_errors', engine, if_exists='replace', index=False)
    # invalid_matches.to_sql('player_stats_errors', engine, if_exists='append', index=False)
    for _, row in invalid_player.iterrows():
        log_cleaned_row(
            row['stat_id'], 
            "Player Stats",
            "ForeignKeyViolation: player_id not present in players table"
        )
    # for _, row in invalid_matches.iterrows():
    #     log_cleaned_row(
    #         row['stat_id'], 
    #         "Player Stats",
    #         "ForeignKeyViolation: match_id not present in matches table"
    #     )

    # Remove Invalid Rows from Original DataFrame
    synced_stats = synced_stats[synced_stats['player_id'].isin(valid_player_ids)]
    # synced_matches = synced_matches[synced_matches['match_id'].isin(valid_match_ids)]




    # Insert synced data into the raw tables in the database

    # First delete data from the tables
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(text("DELETE FROM player_stats"))
            connection.execute(text("DELETE FROM transfer_history"))
            connection.execute(text("DELETE FROM matches"))
            connection.execute(text("DELETE FROM players"))
            connection.execute(text("DELETE FROM teams"))
            
    # Add the data to the database
    synced_teams.to_sql("teams", con=engine, if_exists="append", index=False)
    synced_players.to_sql("players", con=engine, if_exists="append", index=False)
    synced_matches.to_sql("matches", con=engine, if_exists="append", index=False)
    synced_stats.to_sql("player_stats", con=engine, if_exists="append", index=False)
    synced_transfers.to_sql("transfer_history", con=engine, if_exists="append", index=False)

    # creating dataframes from database to clean

    query1 = "SELECT * FROM teams"  
    teams_df_new = pd.read_sql(query1, engine)
    query2 = "SELECT * FROM players"  
    players_df_new = pd.read_sql(query2, engine)
    query3 = "SELECT * FROM matches"  
    matches_df_new = pd.read_sql(query3, engine)
    query4 = "SELECT * FROM transfer_history"  
    transfers_df_new = pd.read_sql(query4, engine)
    query5 = "SELECT * FROM player_stats"
    stats_df_new = pd.read_sql(query5, engine)

    # Teams have more than 11 players, add to error table

    player_counts = players_df_new['team_id'].value_counts()

    teams_id_more_than_11 = player_counts[player_counts > 11].index

    teams_errors = teams_df_new[teams_df_new['team_id'].isin(teams_id_more_than_11)].copy()

    teams_errors['reason'] = 'Team has more than 11 players'

    # Insert Invalid Rows into Error Table and log file
    teams_errors.to_sql('teams_errors', engine, if_exists='replace', index=False)
    for _, row in teams_errors.iterrows():
        log_cleaned_row(
            row['team_id'], 
            "Teams",
            row['reason']
        ) 
    teams_errors.shape

    # STATS TABLE CLEANING
    stats_invalid = pd.DataFrame(columns=stats_df_new.columns)
    stats_invalid['reason'] = None

    # check if there are any red cards greater than 1
    invalid_red_cards = stats_df_new[stats_df_new['red_cards'] > 1].copy()
    invalid_red_cards['reason'] = 'Red Cards greater than 1, should be 1'
    stats_invalid = pd.concat([stats_invalid, invalid_red_cards], ignore_index=True)

    # # check if there are any yellow cards greater than 2 
    invalid_yellow_cards = stats_df_new[stats_df_new['yellow_cards'] > 2].copy()
    invalid_yellow_cards['reason'] = 'Yellow Cards greater than 2, should be maximum 2'
    stats_invalid = pd.concat([stats_invalid, invalid_yellow_cards], ignore_index=True)

    # if there are any yellow cards=2 then player should have a red card
    invalid_combination= stats_df_new[(stats_df_new['yellow_cards'] == 2) & (stats_df_new['red_cards'] != 1)].copy()
    invalid_combination['reason'] = 'If there are any yellow cards = 2, then player should have a red card'
    stats_invalid = pd.concat([stats_invalid, invalid_combination], ignore_index=True)

    # Insert Invalid Rows into Error Table and log file
    stats_invalid.to_sql('player_stats_errors', engine, if_exists='replace', index=False)
    for _, row in stats_invalid.iterrows():
        log_cleaned_row(
            row['stat_id'], 
            "Player Stats",
            row['reason']
        ) 

    # fix errors in dataframe
    stats_df_new.loc[stats_df_new['red_cards'] > 1, 'red_cards'] = 1
    stats_df_new.loc[stats_df_new['yellow_cards'] > 2, 'yellow_cards'] = 2
    stats_df_new.loc[stats_df_new['yellow_cards'] == 2, 'red_cards'] = 1

    # include each table count before and after cleaning in the log file

    old_transfer_records=len(transfers_df)
    new_transfer_records=len(transfers_df_new)

    old_matches_records=len(matches_df)
    new_matches_records=len(matches_df_new)

    old_teams_records=len(teams_df)
    new_teams_records=len(teams_df_new)

    old_stats_records=len(stats_df)
    new_stats_records=len(stats_df_new)

    old_players_records=len(players_df)
    new_players_records=len(players_df_new)

    from tabulate import tabulate
    record_comparison_log = []

    # Function to add entries to the log
    def log_record_comparison(table_name, old_count, new_count):
        record_comparison_log.append({
            "Table Name": table_name,
            "Old Count": old_count,
            "New Count": new_count
        })

    def write_log_file():
        table_rows = [
            [entry["Table Name"], entry["Old Count"], entry["New Count"]]
            for entry in record_comparison_log
        ]
        log_table_string = tabulate(
            table_rows,
            headers=["Table Name", "Old Count", "New Count"],
            tablefmt="grid"
        )
        with open(file_path, "a") as log_file:
            log_file.write("\nRecord Comparison\n")
            log_file.write(log_table_string)
            log_file.write("\n")

    # Log record counts for each table
    log_record_comparison("Player Transfers", old_transfer_records, new_transfer_records)
    log_record_comparison("Matches", old_matches_records, new_matches_records)
    log_record_comparison("Teams", old_teams_records, new_teams_records)
    log_record_comparison("Player Stats", old_stats_records, new_stats_records)
    log_record_comparison("Players", old_players_records, new_players_records)

    write_log_file()

    # Write clean data to sql database

    # First delete data from the tables
    with engine.connect() as connection:
        with connection.begin():
            connection.execute(text("DELETE FROM cleaned_player_stats"))
            connection.execute(text("DELETE FROM cleaned_transfer_history"))
            connection.execute(text("DELETE FROM cleaned_matches"))
            connection.execute(text("DELETE FROM cleaned_players"))
            connection.execute(text("DELETE FROM cleaned_teams"))

    teams_df_new.to_sql('cleaned_teams', engine, if_exists='append', index=False)
    players_df_new.to_sql('cleaned_players', engine, if_exists='append', index=False)
    matches_df_new.to_sql('cleaned_matches', engine, if_exists='append', index=False)
    stats_df_new.to_sql('cleaned_player_stats', engine, if_exists='append', index=False)
    transfers_df_new.to_sql('cleaned_transfer_history', engine, if_exists='append', index=False)

    # ------------------------CREATE VIEW ------------------------------

    # Aggregate stats by player_id and calculate required fields
    stats_aggregated_df = stats_df_new.groupby('player_id').agg(
        TotalGoals=('goals', 'sum'),
        TotalAssists=('assists', 'sum'),
        TotalMinutesPlayed=('mins_played', 'sum'),
        AverageMinutesPlayed=('mins_played','mean'),
        # Flags
        PlayedOver300Min=('mins_played', lambda x: 1 if x.sum() > 300 else 0),
        Scored3PlusGoalsInMatch=('goals', lambda x: 1 if (x >= 3).sum() > 0 else 0),
        EstimatedMatchesPlayed=('mins_played', lambda x: x.sum() // 90),
    ).reset_index()

    stats_aggregated_df['EstimatedMatchesPlayed'] = stats_aggregated_df['EstimatedMatchesPlayed'].astype(str) + ' match ' + \
                                                    (stats_aggregated_df['TotalMinutesPlayed'] % 90).astype(str) + ' mins'

    stats_aggregated_df['AgeBetween25And30'] = players_df_new['birthdate'].apply(lambda x: 1 if 25 <= (datetime.now() - pd.to_datetime(x)).days / 365 <= 30 else 0)

    final_view = stats_aggregated_df.copy()

    # Add players that had no stats

    players_with_no_stats = players_df_new[~players_df_new['player_id'].isin(stats_aggregated_df['player_id'])]
    no_stats_view=pd.DataFrame()
    no_stats_view['player_id'] = players_with_no_stats['player_id']
    no_stats_view['TotalGoals']=0
    no_stats_view['TotalAssists']=0
    no_stats_view['TotalMinutesPlayed']=0
    no_stats_view['AverageMinutesPlayed']=0 
    no_stats_view['PlayedOver300Min']=0
    no_stats_view['Scored3PlusGoalsInMatch']=0 
    no_stats_view['EstimatedMatchesPlayed']=0
    no_stats_view['AgeBetween25And30']= players_df_new['birthdate'].apply(lambda x: 1 if 25 <= (datetime.now() - pd.to_datetime(x)).days / 365 <= 30 else 0)

    final_view = pd.concat([final_view, no_stats_view], ignore_index=True)

    # Identify teams located in France and Italy
    france_teams = teams_df_new[teams_df_new['country'] == 'France']
    italy_teams = teams_df_new[teams_df_new['country'] == 'Italy']

    # Merge players with their transfer details
    player_transfers = transfers_df_new.merge(players_df_new, on='player_id', how='left')

    # Find transfers to French teams
    france_transfers = player_transfers[player_transfers['to_team_id'].isin(france_teams['team_id'])]

    # Find transfers to Italian teams
    italy_transfers = player_transfers[player_transfers['to_team_id'].isin(italy_teams['team_id'])]

    # Get the current team of each player
    current_france_players = players_df_new[players_df_new['team_id'].isin(france_teams['team_id'])]
    current_italy_players = players_df_new[players_df_new['team_id'].isin(italy_teams['team_id'])]

    # Calculate 'PlayedInFrance' and 'DateJoinedFrenchTeam'
    final_view['PlayedInFrance'] = final_view['player_id'].isin(france_transfers['player_id']) | final_view['player_id'].isin(current_france_players['player_id'])
    final_view['PlayedInFrance'] = final_view['PlayedInFrance'].astype(int)
    final_view['DateJoinedFrenchTeam'] = final_view['player_id'].map(
        france_transfers.groupby('player_id')['trans_date'].min().to_dict()
    )
    final_view.loc[final_view['PlayedInFrance'] == 1, 'DateJoinedFrenchTeam'] = final_view.loc[
        final_view['PlayedInFrance'] == 1, 'DateJoinedFrenchTeam'].fillna(np.nan)

    # Calculate 'PlayedInItaly' and 'DateJoinedItalianTeam'
    final_view['PlayedInItaly'] = final_view['player_id'].isin(italy_transfers['player_id']) | final_view['player_id'].isin(current_italy_players['player_id'])
    final_view['PlayedInItaly'] = final_view['PlayedInItaly'].astype(int)
    final_view['DateJoinedItalianTeam'] = final_view['player_id'].map(
        italy_transfers.groupby('player_id')['trans_date'].min().to_dict()
    )
    final_view.loc[final_view['PlayedInItaly'] == 1, 'DateJoinedItalianTeam'] = final_view.loc[
        final_view['PlayedInItaly'] == 1, 'DateJoinedItalianTeam'].fillna(np.nan)

    # Visualize data
    final_view = final_view.merge(players_df_new[['player_id', 'player_name','team_id']], on='player_id', how='left')
    final_view = final_view.merge(teams_df_new[['team_id','team_name']], left_on='team_id', right_on='team_id', how='left')

    final_view = final_view[['player_id', 'player_name', 'team_name','TotalGoals','TotalAssists',
                            'AverageMinutesPlayed','PlayedOver300Min','AgeBetween25And30','Scored3PlusGoalsInMatch',
                                    'EstimatedMatchesPlayed',
                                    'PlayedInFrance', 'DateJoinedFrenchTeam', 'PlayedInItaly', 'DateJoinedItalianTeam']]

    final_view.to_csv(r"C:\Users\Habiba Khaled\Desktop\Atos\Task3\CSVs\final_view.csv",index=False, header=True)
    final_view.to_sql('final_view', engine, if_exists='replace', index=False)
    print("Task executed successfully!")

# Schedule the task
schedule.every(10).seconds.do(process_data_task)

# schedule.clear(process_data_task)
# schedule.every().day.at("10:00").do(process_data_task) 

while True:
    schedule.run_pending()
    time.sleep(10)
