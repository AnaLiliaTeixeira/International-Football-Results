import pandas as pd
import json
from pymongo import MongoClient
from json_files_creation import *

data_goalscores = json.load(open("bd_json_files/goalscorers.json"))
data_shootouts = json.load(open("bd_json_files/shootouts.json"))
data_results = json.load(open("bd_json_files/results.json"))

############################Mongo######################
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
all_teams = pd.concat([df_shootouts['home_team'], df_shootouts['away_team'], df_results['home_team'], df_results['away_team']])
all_teams = pd.Series(all_teams)
unique_teams = all_teams.drop_duplicates().to_list()
teams.insert_many([{'team_name' : team} for team in unique_teams])
print("Inserted teams")

#Inserir dados na tabela tournaments
all_tournaments = df_results['tournament']
tournaments.insert_many([{'tournament_name' : tournament} for tournament in all_tournaments])
print("Inserted tournaments")

#Inserir dados na tabela countries
all_countries = df_results['country']
all_countries = pd.Series(all_countries)
unique_countries = all_countries.drop_duplicates().to_list()
countries.insert_many([{'country_name' : country} for country in unique_countries])
print("Inserted countries")

def convertItemToIds(element, collection, name):
    document = collection.find_one({name: element})
    return document['_id'] if document else None

def convertItemToMatch(date, home_team, away_team):
    document = matches.find_one({'date': date, 'home_team': home_team, 'away_team': away_team})
    return document['_id'] if document else None

#Inserir dados na tabela matches
for row in df_results.itertuples():
    home_team_id = convertItemToIds(row.home_team, teams, 'team_name')
    away_team_id = convertItemToIds(row.away_team, teams, 'team_name')
    tournament_id = convertItemToIds(row.tournament, tournaments, 'tournament_name')
    country_id = convertItemToIds(row.country, countries, 'country_name')
    matches.insert_one({'date': row.date, 'home_team': home_team_id, 'away_team': away_team_id, 'home_score': row.home_score, 'away_score': row.away_score, 'tournament': tournament_id, 'city': row.city, 'country': country_id, 'neutral': row.neutral})
print("Inserted matches")

#Inserir dados na tabela shootouts
for row in df_shootouts.itertuples():
    home_team_id = convertItemToIds(row.home_team, teams, 'team_name')
    away_team_id = convertItemToIds(row.away_team, teams, 'team_name')
    match_id = convertItemToMatch(row.date, home_team_id, away_team_id)
    winner_id = convertItemToIds(row.winner, teams, 'team_name')
    first_shooter_id = convertItemToIds(row.first_shooter, teams, 'team_name')
    shootouts.insert_one({'match': match_id, 'winner': winner_id, 'first_shooter': first_shooter_id})

print("Inserted shootouts")

#Inserir dados na tabela goalscores
for row in df_goalscores.itertuples():
    home_team_id = convertItemToIds(row.home_team, teams, 'team_name')
    away_team_id = convertItemToIds(row.away_team, teams, 'team_name')
    match_id = convertItemToMatch(row.date, home_team_id, away_team_id)
    team_id = convertItemToIds(row.team, teams, 'team_name')
    goalscores.insert_one({'match': match_id, 'team': team_id, 'scorer': row.scorer, 'minute':row.minute, 'penalty': row.penalty, 'own_goal': row.own_goal})

print("Inserted goalscores")

client.close()