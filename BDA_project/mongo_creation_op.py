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
client.drop_database("BDA2324_4_OP")

db = client.BDA2324_4_OP

goalscores = db.goalscores
shootouts = db.shootouts
#teams = db.teams
matches = db.matches
#countries = db.countries
tournaments = db.tournaments

# #Inserir dados na tabela teams
# all_teams = pd.concat([df_results['home_team'], df_results['away_team']])
# all_teams = pd.Series(all_teams)
# unique_teams = all_teams.drop_duplicates().to_list()
# teams.insert_many([{'team_name' : team} for team in unique_teams])
# team_data = teams.find()
# team_id_mapping = {team_doc['team_name']: team_doc['_id'] for team_doc in team_data}
# print("Inserted teams")

#Atualizar as colunas que contêm teams para terem o id do team
# df_results['home_team'] = df_results['home_team'].map(team_id_mapping)
# df_results = df_results.rename(columns={'home_team': 'home_team_id'})
# df_results['away_team'] = df_results['away_team'].map(team_id_mapping)
# df_results = df_results.rename(columns={'away_team': 'away_team_id'})
# df_goalscores['home_team'] = df_goalscores['home_team'].map(team_id_mapping)
# df_goalscores = df_goalscores.rename(columns={'home_team': 'home_team_id'})
# df_goalscores['away_team'] = df_goalscores['away_team'].map(team_id_mapping)
# df_goalscores = df_goalscores.rename(columns={'away_team': 'away_team_id'})
# df_goalscores['team'] = df_goalscores['team'].map(team_id_mapping)
# df_goalscores = df_goalscores.rename(columns={'team': 'team_id'})
# df_shootouts['home_team'] = df_shootouts['home_team'].map(team_id_mapping)
# df_shootouts = df_shootouts.rename(columns={'home_team': 'home_team_id'})
# df_shootouts['away_team'] = df_shootouts['away_team'].map(team_id_mapping)
# df_shootouts = df_shootouts.rename(columns={'away_team': 'away_team_id'})
# df_shootouts['winner'] = df_shootouts['winner'].map(team_id_mapping)
# df_shootouts = df_shootouts.rename(columns={'winner': 'winner_id'})
# df_shootouts['first_shooter'] = df_shootouts['first_shooter'].map(team_id_mapping)
# df_shootouts = df_shootouts.rename(columns={'first_shooter': 'first_shooter_id'})

#Inserir dados na tabela tournaments
# all_tournaments = df_results['tournament']
# tournaments_ids = tournaments.insert_many([{'tournament_name' : tournament} for tournament in all_tournaments])

# #Atualizar as colunas que contêm tournaments para terem o id do tournament
# tournaments_data = tournaments.find()
# tournament_id_mapping = {tournament_doc['tournament_name']: tournament_doc['_id'] for tournament_doc in tournaments_data}
# df_results['tournament'] = df_results['tournament'].map(tournament_id_mapping)
# df_results = df_results.rename(columns={'tournament': 'tournament_id'})
# print("Inserted tournaments")

# #Inserir dados na tabela countries
# all_countries = df_results['country']
# all_countries = pd.Series(all_countries)
# unique_countries = all_countries.drop_duplicates().to_list()
# countries.insert_many([{'country_name' : country} for country in unique_countries])

#Atualizar as colunas que contêm countries para terem o id do country
# countries_data = countries.find()
# countries_id_mapping = {country_doc['country_name']: country_doc['_id'] for country_doc in countries_data}
# df_results['country'] = df_results['country'].map(countries_id_mapping)
# df_results = df_results.rename(columns={'country': 'country_id'})
# print("Inserted countries")

#Inserir dados na tabela matches
matches_data = df_results.to_dict(orient='records')
matches.insert_many(matches_data)

matches_data = matches.find()
matches_id_mapping = {(doc['date'], doc['home_team'], doc['away_team']): doc for doc in matches_data}
print("Inserted matches")


#Inserir dados na tabela shootouts
df_shootouts['match_id'] = df_shootouts.apply(lambda row: matches_id_mapping.get((row['date'], row['home_team'], row['away_team']))['_id'], axis=1)
colunas_para_remover = ['date', 'home_team', 'away_team']
df_shootouts = df_shootouts.drop(colunas_para_remover, axis=1)
colunas = list(df_shootouts.columns)
colunas = ['match_id'] + [coluna for coluna in colunas if coluna != 'match_id']
df_shootouts = df_shootouts[colunas]

shootouts_data = df_shootouts.to_dict(orient='records')
shootouts.insert_many(shootouts_data)
print("Inserted shootouts")

#Inserir dados na tabela goalscores
df_goalscores['match_id'] = df_goalscores.apply(lambda row: matches_id_mapping.get((row['date'], row['home_team'], row['away_team']))['_id'], axis=1)
colunas_para_remover = ['date', 'home_team', 'away_team']
df_goalscores = df_goalscores.drop(colunas_para_remover, axis=1)
colunas = list(df_goalscores.columns)
colunas = ['match_id'] + [coluna for coluna in colunas if coluna != 'match_id']
df_goalscores = df_goalscores[colunas]

goalscores_data = df_goalscores.to_dict(orient='records')
goalscores.insert_many(goalscores_data)
print("Inserted goalscores")

client.close()