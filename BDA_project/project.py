import pandas as pd
import json
from pymongo import MongoClient

client=MongoClient()
client = MongoClient('localhost', 27017)
client.drop_database("BDA2324_4")

db = client.BDA2324_4

goalscores = db.goalscores
shootouts = db.shootouts
teams = db.teams
matches = db.matches
countries = db.countries
tournaments = db.tournaments

df_goalscores = pd.read_csv('bd_dataset/goalscorers.csv')
df_goalscores.to_json("goalscorers.json",orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)

df_shootouts = pd.read_csv('bd_dataset/shootouts.csv')
df_shootouts.to_json("shootouts.json",orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)

df_results = pd.read_csv('bd_dataset/results.csv')
df_results.to_json("results.json",orient = "records", date_format = "epoch", double_precision = 10, force_ascii = True, date_unit = "ms", default_handler = None, indent=2)

data_goalscores = json.load(open("goalscorers.json"))
data_shootouts = json.load(open("shootouts.json"))
data_results = json.load(open("results.json"))

#scorer, own_goal, penalty, team, 
#goalscores.insert_many(data_goalscores)
#goalscores.insert_many(data_shootouts)
#goalscores.insert_many(data_results)
# criar uma tabela com as equipas individualmente a partir de home team and away team do shootouts

team = df_shootouts['home_team']+ df_shootouts['away_team']
#falta o to_list
print(team)
#print(team)
team = pd.DataFrame(team)
team = team.drop_duplicates()
team = team.to_dict()
teams.insert_many([{' tema' :team.values()}])

client.close()

############################SQL######################


import mysql.connector
import pandas as pd
import json
from pymongo import MongoClient

# Create Connection
mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password='senha123',
    database = 'BDA2324_4'
)

mycursor = mydb.cursor()
#mycursor.execute("CREATE DATABASE BDA2324_4")

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

mycursor.execute('''CREATE TABLE Tournaments(
        tournament_id INTEGER PRIMARY KEY AUTO_INCREMENT,
        tournament_name VARCHAR(245)
    )
''')

mycursor.execute('''CREATE TABLE Teams (
        team_id INTEGER AUTO_INCREMENT PRIMARY KEY,
        team_name VARCHAR(245)
    )
''')

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

unique_countries = df_results['country'].unique()

sql = "INSERT INTO countries (country_name) VALUES (%s)"
for country in unique_countries:
    val = (country,)
    mycursor.execute(sql, val)
mydb.commit()


unique_teams =  df_shootouts['home_team'].tolist() + df_results['away_team'].tolist()
unique_teams_set = set(unique_teams)
unique_teams_list = list(unique_teams_set)
print(len(unique_teams_list))

sql = "INSERT INTO teams (team_name) VALUES (%s)"
for team in unique_teams_list:
    val = (team,)
    mycursor.execute(sql, val)
mydb.commit()

