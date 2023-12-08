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
desired_database = 'bda2324_4_op'
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

mycursor.execute('''CREATE TABLE Matches (
        match_id INTEGER AUTO_INCREMENT PRIMARY KEY,
        date DATE,
        home_team VARCHAR(245),
        away_team VARCHAR(245),
        home_score INTEGER,
        away_score INTEGER,
        tournament VARCHAR(245),
        city VARCHAR(245),
        country VARCHAR(245),
        neutral BOOLEAN NOT NULL
    )
''')
mydb.commit()
print("Table Matches created!")
mycursor.execute('''CREATE TABLE Shootouts (
        shootout_id INTEGER PRIMARY KEY AUTO_INCREMENT,
        match_id INTEGER,
        winner VARCHAR(245),
        first_shooter VARCHAR(245),
        FOREIGN KEY(match_id) REFERENCES Matches(match_id)
    )
''')
mydb.commit()
print("Table Shootouts created!")
mycursor.execute('''CREATE TABLE Goalscorers (
        goal_id INTEGER PRIMARY KEY AUTO_INCREMENT,
        match_id INTEGER,
        team VARCHAR(245),
        scorer VARCHAR(245),
        minute INTEGER,
        own_goal BOOLEAN,
        penalty BOOLEAN,
        FOREIGN KEY(match_id) REFERENCES Matches(match_id)
    )
''')
mydb.commit()
print("Table Goalscorers created!")





#INSERT MATCHES VALUES
df_results_list = df_results.values.tolist()
matches_insert_query = "INSERT INTO Matches (date, home_team, away_team, home_score, away_score, tournament, city, country, neutral) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
mycursor.executemany(matches_insert_query, df_results_list)
mydb.commit()
print("The values of Matches were inserted!")
query = "SELECT match_id,date,home_team,away_team FROM Matches"
mycursor.execute(query)
matchs_data = mycursor.fetchall()
match_id_mapping = {(date,home_team_id,away_team_id): match_id for match_id, date,home_team_id,away_team_id in matchs_data}




#INSERT SHOOTOUTS VALUES
df_shootouts['date'] = pd.to_datetime(df_shootouts['date']).dt.date
df_shootouts['match_id'] = df_shootouts.apply(lambda row: match_id_mapping.get((row['date'], row['home_team'], row['away_team'])), axis=1)
colunas_para_remover = ['date', 'home_team', 'away_team']
df_shootouts = df_shootouts.drop(colunas_para_remover, axis=1)
colunas = list(df_shootouts.columns)
colunas = ['match_id'] + [coluna for coluna in colunas if coluna != 'match_id']
df_shootouts = df_shootouts[colunas]
df_shootouts_list = df_shootouts.values.tolist()
shootouts_insert_query = "INSERT INTO Shootouts (match_id, winner, first_shooter) VALUES (%s, %s, %s)"
mycursor.executemany(shootouts_insert_query, df_shootouts_list)
mydb.commit()
print("The values of Shootouts were inserted!")


# Inserção na tabela Goalscorers
df_goalscores['date'] = pd.to_datetime(df_goalscores['date']).dt.date
df_goalscores['match_id'] = df_goalscores.apply(lambda row: match_id_mapping.get((row['date'], row['home_team'], row['away_team'])), axis=1)
colunas_para_remover = ['date', 'home_team', 'away_team']
df_goalscores = df_goalscores.drop(colunas_para_remover, axis=1)
colunas = list(df_goalscores.columns)
colunas = ['match_id'] + [coluna for coluna in colunas if coluna != 'match_id']
df_goalscores = df_goalscores[colunas]
df_goalscores_list = df_goalscores.values.tolist()
goalscorers_insert_query = "INSERT INTO Goalscorers (match_id, team, scorer, minute, own_goal, penalty) VALUES (%s, %s, %s, %s, %s, %s)" 
mycursor.executemany(goalscorers_insert_query, df_goalscores_list)
mydb.commit()
print("The values of Goalscorers were inserted!")
mydb.close()
