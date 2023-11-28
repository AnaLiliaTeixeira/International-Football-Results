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

#Inserir dados na tabela teams
all_teams = pd.concat([df_shootouts['home_team'], df_shootouts['away_team']])
all_teams = pd.Series(all_teams)
unique_teams = all_teams.drop_duplicates().to_list()
teams.insert_many([{'team_name' : team} for team in unique_teams])

#Inserir dados na tabela tournaments
all_tournaments = df_results['tournament']
tournaments.insert_many([{' tournament_name' : tournament} for tournament in all_tournaments])

#Inserir dados na tabela countries
all_countries = df_results['country']
all_countries = pd.Series(all_countries)
unique_countries = all_countries.drop_duplicates().to_list()
countries.insert_many([{' country_name' : country} for country in unique_countries])

# def convertListToIds(matchesList, column) :
#     listIds = []
#     for match in matchesList:
#         listIds.append(teams.find(match[column]))
#     return listIds
    
# #Inserir dados na tabela matches
# #all_matches = pd.concat([df_results['date'], ])
# print(convertListToIds(df_results, 'home_team'))
# tournaments.insert_many([{' tournament_name' : tournament} for tournament in all_tournaments])

client.close()


