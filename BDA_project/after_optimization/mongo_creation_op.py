import pandas as pd
import json
from pymongo import MongoClient
from json_files_creation import *

data_goalscores = json.load(open("bd_json_files/goalscorers.json"))
data_shootouts = json.load(open("bd_json_files/shootouts.json"))
data_results = json.load(open("bd_json_files/results.json"))

############################Mongo######################
client = MongoClient('localhost', 27017)
client.drop_database("BDA2324_4_OP")

db = client.BDA2324_4_OP

goalscores = db.goalscores
shootouts = db.shootouts
matches = db.matches

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