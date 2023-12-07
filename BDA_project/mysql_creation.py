import mysql.connector
import pandas as pd
from json_files_creation import *

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
print("Table Countries created!")
mycursor.execute('''CREATE TABLE Tournaments(
        tournament_id INTEGER PRIMARY KEY AUTO_INCREMENT,
        tournament_name VARCHAR(245)
    )
''')
mydb.commit()
print("Table Tournaments created!")
mycursor.execute('''CREATE TABLE Teams (
        team_id INTEGER AUTO_INCREMENT PRIMARY KEY,
        team_name VARCHAR(245)
    )
''')
mydb.commit()
print("Table Teams created!")
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
print("Table Matches created!")
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
print("Table Shootouts created!")
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
print("Table Goalscorers created!")

#INSERT COUNTRIES VALUES
unique_countries = df_results['country'].unique()
unique_countries_list = [(country,) for country in unique_countries]
sql = "INSERT INTO countries (country_name) VALUES (%s)"
mycursor.executemany(sql,unique_countries_list)
mydb.commit()
print("The values of Countries were inserted!")
query = "SELECT * FROM countries"
mycursor.execute(query)
country_data = mycursor.fetchall()
country_id_mapping = {country_id: country_name for country_id, country_name in country_data}
df_results['country'] = df_results['country'].map({country_name: country_id for country_id, country_name in country_id_mapping.items()})
df_results = df_results.rename(columns={'country': 'country_id'})


#INSERT TEAMS VALUES
unique_teams =  df_results['home_team'].tolist() + df_results['away_team'].tolist()
unique_teams_set = set(unique_teams)
unique_teams_list = list(unique_teams_set)
team_data = [(team,) for team in unique_teams_list]
sql = "INSERT INTO teams (team_name) VALUES (%s)"
mycursor.executemany(sql,team_data)
mydb.commit()
query = "SELECT * FROM teams"
mycursor.execute(query)
team_data = mycursor.fetchall()
team_id_mapping = {team_name: team_id for team_id, team_name in team_data}
print("The values of Teams were inserted!")


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
df_shootouts = df_shootouts.rename(columns={'home_team': 'home_team_id'})
df_shootouts['away_team'] = df_shootouts['away_team'].map(team_id_mapping)
df_shootouts = df_shootouts.rename(columns={'away_team': 'away_team_id'})
df_shootouts['winner'] = df_shootouts['winner'].map(team_id_mapping)
df_shootouts = df_shootouts.rename(columns={'winner': 'winner_id'})
df_shootouts['first_shooter'] = df_shootouts['first_shooter'].map(team_id_mapping)
df_shootouts = df_shootouts.rename(columns={'first_shooter': 'first_shooter_id'})


#INSERT TOURNAMENTS VALUES
unique_tournaments=  df_results['tournament'].unique()
tournment_data = [(tournament,) for tournament in unique_tournaments]
sql = "INSERT INTO tournaments (tournament_name) VALUES (%s)"
mycursor.executemany(sql,tournment_data)
mydb.commit()
query = "SELECT * FROM tournaments"
mycursor.execute(query)
tournment_data = mycursor.fetchall()
tournament_id_mapping = {tournament_name: tournament_id for tournament_id, tournament_name in tournment_data}
print("The values of tournaments were inserted!")
df_results['tournament'] = df_results['tournament'].map(tournament_id_mapping)
df_results = df_results.rename(columns={'tournament': 'tournament_id'})


#INSERT MATCHES VALUES
df_results_list = df_results.values.tolist()
matches_insert_query = "INSERT INTO Matches (date, home_team_id, away_team_id, home_score, away_score, tournament_id, city, country_id, neutral) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
mycursor.executemany(matches_insert_query, df_results_list)
mydb.commit()
print("The values of Matches were inserted!")
query = "SELECT match_id,date,home_team_id,away_team_id FROM Matches"
mycursor.execute(query)
matchs_data = mycursor.fetchall()
match_id_mapping = {(date,home_team_id,away_team_id): match_id for match_id, date,home_team_id,away_team_id in matchs_data}




#INSERT SHOOTOUTS VALUES
df_shootouts['date'] = pd.to_datetime(df_shootouts['date']).dt.date
df_shootouts['match_id'] = df_shootouts.apply(lambda row: match_id_mapping.get((row['date'], row['home_team_id'], row['away_team_id'])), axis=1)
colunas_para_remover = ['date', 'home_team_id', 'away_team_id']
df_shootouts = df_shootouts.drop(colunas_para_remover, axis=1)
colunas = list(df_shootouts.columns)
colunas = ['match_id'] + [coluna for coluna in colunas if coluna != 'match_id']
df_shootouts = df_shootouts[colunas]
df_shootouts_list = df_shootouts.values.tolist()
shootouts_insert_query = "INSERT INTO Shootouts (match_id, winner_id, first_shooter_id) VALUES (%s, %s, %s)"
mycursor.executemany(shootouts_insert_query, df_shootouts_list)
mydb.commit()
print("The values of Shootouts were inserted!")


# Inserção na tabela Goalscorers
df_goalscores['date'] = pd.to_datetime(df_goalscores['date']).dt.date
df_goalscores['match_id'] = df_goalscores.apply(lambda row: match_id_mapping.get((row['date'], row['home_team_id'], row['away_team_id'])), axis=1)
colunas_para_remover = ['date', 'home_team_id', 'away_team_id']
df_goalscores = df_goalscores.drop(colunas_para_remover, axis=1)
colunas = list(df_goalscores.columns)
colunas = ['match_id'] + [coluna for coluna in colunas if coluna != 'match_id']
df_goalscores = df_goalscores[colunas]
df_goalscores_list = df_goalscores.values.tolist()
goalscorers_insert_query = "INSERT INTO Goalscorers (match_id, team_id, scorer, minute, own_goal, penalty) VALUES (%s, %s, %s, %s, %s, %s)" 
mycursor.executemany(goalscorers_insert_query, df_goalscores_list)
mydb.commit()
print("The values of Goalscorers were inserted!")
