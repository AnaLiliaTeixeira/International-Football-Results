import pandas as pd
import json
from pymongo import MongoClient
from json_files import *

data_goalscores = json.load(open("goalscorers.json"))
data_shootouts = json.load(open("shootouts.json"))
data_results = json.load(open("results.json"))

############################Mongo######################
#scorer, own_goal, penalty, team, 
#goalscores.insert_many(data_goalscores)
#goalscores.insert_many(data_shootouts)
#goalscores.insert_many(data_results)
# criar uma tabela com as equipas individualmente a partir de home team and away team do shootouts

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






