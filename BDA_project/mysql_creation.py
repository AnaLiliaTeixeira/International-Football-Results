import mysql.connector
import pandas as pd
from json_files import *
from pymongo import MongoClient

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password='1234'
)
mycursor = mydb.cursor()
mycursor.execute("SHOW DATABASES")
databases = mycursor.fetchall()
desired_database = 'bda2324_4'
db_exists = any(desired_database in db for db in databases)
if db_exists:
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password='1234',
        database=desired_database
    )
    print(f"Connected to database '{desired_database}'")
else:
    mycursor.execute(f"CREATE DATABASE {desired_database}")
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password='1234',
        database=desired_database
    )
    print(f"Database '{desired_database}' created and connected successfully")


mycursor = mydb.cursor()

mycursor.execute("DROP TABLE IF EXISTS  Shootouts") 
mycursor.execute("DROP TABLE IF EXISTS Goalscorers")
mycursor.execute("DROP TABLE IF EXISTS Matches")
mycursor.execute("DROP TABLE IF EXISTS Teams") 
mycursor.execute("DROP TABLE IF EXISTS Tournaments")
mycursor.execute("DROP TABLE IF EXISTS Countries") 

mycursor.execute('''CREATE TABLE Countries (
        country_id INTEGER AUTO_INCREMENT PRIMARY KEY,
        country_name VARCHAR(245)
    )
''')
mydb.commit()
mycursor.execute('''CREATE TABLE Tournaments(
        tournament_id INTEGER PRIMARY KEY AUTO_INCREMENT,
        tournament_name VARCHAR(245)
    )
''')
mydb.commit()
mycursor.execute('''CREATE TABLE Teams (
        team_id INTEGER AUTO_INCREMENT PRIMARY KEY,
        team_name VARCHAR(245)
    )
''')
mydb.commit()
mycursor.execute('''CREATE TABLE Matches (
        match_id INTEGER AUTO_INCREMENT PRIMARY KEY,
        date DATE,
        home_team_id INTEGER,
        away_team_id INTEGER,
        home_score INTEGER,
        away_score INTEGER,
        tournament_id INTEGER,
        city VARCHAR(245),
        country_id INTEGER,
        neutral BOOLEAN NOT NULL,
        FOREIGN KEY(home_team_id) REFERENCES Teams(team_id),
        FOREIGN KEY(away_team_id) REFERENCES Teams(team_id),
        FOREIGN KEY(tournament_id) REFERENCES Tournaments(tournament_id),
        FOREIGN KEY(country_id) REFERENCES Countries(country_id)
    )
''')
mydb.commit()
mycursor.execute('''CREATE TABLE Shootouts (
        shootout_id INTEGER PRIMARY KEY AUTO_INCREMENT,
        match_id INTEGER,
        winner_id INTEGER,
        first_shooter_id INTEGER,
        FOREIGN KEY(match_id) REFERENCES Matches(match_id),
        FOREIGN KEY(winner_id) REFERENCES Teams(team_id),
        FOREIGN KEY(first_shooter_id) REFERENCES Teams(team_id)
    )
''')
mydb.commit()
mycursor.execute('''CREATE TABLE Goalscorers (
        goal_id INTEGER PRIMARY KEY AUTO_INCREMENT,
        match_id INTEGER,
        team_id INTEGER,
        scorer VARCHAR(245),
        minute INTEGER,
        own_goal BOOLEAN,
        penalty BOOLEAN,
        FOREIGN KEY(match_id) REFERENCES Matches(match_id),
        FOREIGN KEY(team_id) REFERENCES Teams(team_id)
    )
''')
mydb.commit()

#INSERT COUNTRIES VALUES
unique_countries = df_results['country'].unique()
sql = "INSERT INTO countries (country_name) VALUES (%s)"
country_id_mapping = {}
for country in unique_countries:
    val = (country,)
    mycursor.execute(sql, val) 
    mydb.commit()
    country_id = mycursor.lastrowid
    country_id_mapping[country] = country_id

print("The values of countries were inserted!")
df_results['country'] = df_results['country'].map(country_id_mapping)
df_results = df_results.rename(columns={'country': 'country_id'})

#INSERT TEAMS VALUES
unique_teams =  df_results['home_team'].tolist() + df_results['away_team'].tolist()
unique_teams_set = set(unique_teams)
unique_teams_list = list(unique_teams_set)
team_id_mapping = {}
sql = "INSERT INTO teams (team_name) VALUES (%s)"
for team in unique_teams_list:
    val = (team,)
    mycursor.execute(sql, val)
    mydb.commit()
    team_id = mycursor.lastrowid
    team_id_mapping[team] = team_id
print("The values of teams were inserted!")
df_results['home_team'] = df_results['home_team'].map(team_id_mapping)
df_results = df_results.rename(columns={'home_team': 'home_team_id'})
df_results['away_team'] = df_results['away_team'].map(team_id_mapping)
df_results = df_results.rename(columns={'away_team': 'away_team_id'})
df_goalscores['home_team'] = df_goalscores['home_team'].map(team_id_mapping)
df_goalscores = df_goalscores.rename(columns={'home_team': 'home_team_id'})
df_goalscores['away_team'] = df_goalscores['away_team'].map(team_id_mapping)
df_goalscores = df_goalscores.rename(columns={'away_team': 'away_team_id'})
df_goalscores['team'] = df_goalscores['team'].map(team_id_mapping)
df_goalscores = df_goalscores.rename(columns={'team': 'team_id'})
df_shootouts['home_team'] = df_shootouts['home_team'].map(team_id_mapping)
df_shootouts = df_shootouts.rename(columns={'home_team': 'home_team'})
df_shootouts['away_team'] = df_shootouts['away_team'].map(team_id_mapping)
df_shootouts = df_shootouts.rename(columns={'away_team': 'away_team_id'})
df_shootouts['winner'] = df_shootouts['winner'].map(team_id_mapping)
df_shootouts = df_shootouts.rename(columns={'winner': 'winner_id'})
df_shootouts['first_shooter'] = df_shootouts['first_shooter'].map(team_id_mapping)
df_shootouts = df_shootouts.rename(columns={'first_shooter': 'first_shooter_id'})


#INSERT TOURNAMENTS VALUES
unique_tournaments=  df_results['tournament'].unique()
sql = "INSERT INTO tournaments (tournament_name) VALUES (%s)"
tournament_id_mapping = {}
for tournament in unique_tournaments:
    val = (tournament,)
    mycursor.execute(sql, val)
    mydb.commit()
    tournament_id = mycursor.lastrowid
    tournament_id_mapping[tournament] = tournament_id
df_results['tournament'] = df_results['tournament'].map(tournament_id_mapping)
df_results = df_results.rename(columns={'tournament': 'tournament_id'})
print("The values of tournaments were inserted!")

df_results_list = df_results.values.tolist()
# matches_insert_query = "INSERT INTO Matches (date, home_team_id, away_team_id, home_score, away_score, tournament_id, city, country_id, neutral) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
# mycursor.executemany(matches_insert_query, df_results_list)
# mydb.commit()
match_id_mapping = {}
for match in df_results_list:
    matches_insert_query = "INSERT INTO Matches (date, home_team_id, away_team_id, home_score, away_score, tournament_id, city, country_id, neutral) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mycursor.execute(matches_insert_query,match)
    mydb.commit()
    match_id = mycursor.lastrowid
    unique_key = (match[0], match[1], match[2])
    match_id_mapping[unique_key] = match_id
print("The values of Matches were inserted!")

# Inserção na tabela Shootouts
df_shootouts_list = df_shootouts.values.tolist()
for shootout in df_shootouts_list:
    unique_key = (shootout[0], shootout[1], shootout[2])
    match_id = match_id_mapping.get(unique_key)
    if match_id is not None:
        shootouts_insert_query = "INSERT INTO Shootouts (match_id, winner_id, first_shooter_id) VALUES (%s, %s, %s)"
        shootout_data = (match_id, shootout[3], shootout[4])
        mycursor.execute(shootouts_insert_query, shootout_data)
        mydb.commit()
print("The values of Shootouts were inserted!")

# Inserção na tabela Goalscorers
df_goalscores_list = df_goalscores.values.tolist()
for goalscore in df_goalscores_list:
    unique_key = (goalscore[0], goalscore[1], goalscore[2])
    match_id = match_id_mapping.get(unique_key)
    if match_id is not None:
        goalscorers_insert_query = "INSERT INTO Goalscorers (match_id, team_id, scorer, minute, own_goal, penalty) VALUES (%s, %s, %s, %s, %s, %s)"
        goalscore_data = (match_id, goalscore[3], goalscore[4], goalscore[5], goalscore[6], goalscore[7])
        mycursor.execute(goalscorers_insert_query, goalscore_data)
        mydb.commit()
print("The values of Goalscorers were inserted!")


