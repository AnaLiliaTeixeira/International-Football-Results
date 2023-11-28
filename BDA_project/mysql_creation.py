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
        neutral BOOLEAN,
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
for country in unique_countries:
    val = (country,)
    mycursor.execute(sql, val)
mydb.commit()
print("The values of countries were inserted!")

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

df_results['home_team_id'] = df_results['home_team'].map(team_id_mapping)
df_results['away_team_id'] = df_results['away_team'].map(team_id_mapping)

# Salvar o DataFrame atualizado de volta ao arquivo CSV se desejar
df_results.to_csv('db_dataet/arquivo_atualizado.csv', index=False)

print("As colunas 'home_team' e 'away_team' foram atualizadas para 'team_id'!")
    
#INSERT TOURNAMENTS VALUES
unique_tournaments=  df_results['tournament'].unique()
sql = "INSERT INTO tournaments (tournament_name) VALUES (%s)"
for tournament in unique_tournaments:
    val = (tournament,)
    mycursor.execute(sql, val)
mydb.commit()
print("The values of tournaments were inserted!")



def get_id_from_name(table_name, column_id,column_name,name):
    query = f"SELECT {column_id} FROM {table_name} WHERE {column_name} = %s"
    mycursor.execute(query, (name,))
    result = mycursor.fetchone()
    if result:
        return result[0]
    else:
        return None
    
for index, row in df_results.iterrows():
    home_team_id = get_id_from_name('Teams', 'team_id','team_name',row['home_team'])
    away_team_id = get_id_from_name('Teams', 'team_id','team_name',row['away_team'])
    df_results.at[index, 'home_team_id'] = home_team_id
    df_results.at[index, 'away_team_id'] = away_team_id

matches_insert_query = "INSERT INTO Matches (date, home_team_id, away_team_id, home_score, away_score, tournament_id, city, country_id, neutral) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
mycursor.executemany(matches_insert_query, df_results)
mydb.commit()
print("The values of Matches were inserted!")

# Inserção na tabela Shootouts
shootouts_insert_query = "INSERT INTO Shootouts (match_id, winner_id, first_shooter_id) VALUES (%s, %s, %s)"
mycursor.executemany(shootouts_insert_query, df_shootouts)
mydb.commit()
print("The values of Shootouts were inserted!")

# Inserção na tabela Goalscorers
goalscorers_insert_query = "INSERT INTO Goalscorers (match_id, team_id, scorer, own_goal, penalty) VALUES (%s, %s, %s, %s, %s)"
mycursor.executemany(goalscorers_insert_query, df_goalscores)
mydb.commit()
print("The values of Goalscorers were inserted!")



