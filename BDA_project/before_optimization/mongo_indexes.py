import time
from pymongo import MongoClient
import pprint
from mongo_queries import simpleQuery1, simpleQuery2, complexQuery1, complexQuery2

import pymongo

client=MongoClient()
client = MongoClient('localhost', 27017)

db = client.BDA2324_4

goalscores = db.goalscores
shootouts = db.shootouts
teams = db.teams
matches = db.matches
countries = db.countries
tournaments = db.tournaments

#Index para a simple query 1
matches.create_index([('city'), ('date')])
result_simpleQuery1 = matches.find(simpleQuery1)
explanaiton_simpleQuery1 = result_simpleQuery1.explain()
pprint.pprint(explanaiton_simpleQuery1)

# Index para a simple query 2 - index composto nas chaves 'scorer' e 'minute'
goalscores.create_index([('scorer'), ('minute')])
result_simpleQuery2 = goalscores.find(simpleQuery2)
explanaiton_simpleQuery2 = result_simpleQuery2.explain()
# pprint.pprint(explanaiton_simpleQuery2)

# Index para a complex query 1
goalscores.create_index("match_id") # Criação de índices para $lookup
# goalscores.create_index("team_id") # Criação de índices para $lookup
matches.create_index("tournament_id") # Criação de índices para $lookup
matches.create_index("country_id") # Criação de índices para $lookup
goalscores.create_index([("team.team_name", pymongo.ASCENDING), ("matches.tournament.tournament_name", pymongo.ASCENDING), ("matches.country.country_name", pymongo.ASCENDING)]) # Criação de index composto para $match
goalscores.create_index([("scorer", pymongo.ASCENDING), ("total_goals", pymongo.DESCENDING)]) # Criação de index para $group e $sort

# print("Complex Query 1:")
start_time_complexQuery1 = time.time()
result_complexQuery1 = goalscores.aggregate(complexQuery1)
end_time_complexQuery1 = time.time()
time_complexQuery1 = end_time_complexQuery1 - start_time_complexQuery1

# Index para a complex query 2
matches.create_index([("home_team_id"), ("away_team_id")]) # Criar índices para a coleção 'matches'
# matches.create_index("away_team_id") # Criar índices para a coleção 'matches'
# matches.create_index("date") # Criar índices para a coleção 'matches'
teams.create_index("team_name") # Criar índices para a coleção 'teams'
# goalscores.create_index("match_id") # Criar índices para a coleção 'Goalscorers'
goalscores.create_index([("match_id", pymongo.ASCENDING), ("home_score", pymongo.ASCENDING), ("away_score", pymongo.ASCENDING)]) # Certifique-se de que você tem um índice composto para os campos usados no $group e $sort
matches.create_index([("home_team_id", pymongo.ASCENDING), ("away_team_id", pymongo.ASCENDING), ("date", pymongo.ASCENDING)]) # Certifique-se de que você tem um índice composto para os campos usados no $group

# print("Complex Query 2:")
start_time_complexQuery2 = time.time()
result_complexQuery2 = matches.aggregate(complexQuery2)
end_time_complexQuery2 = time.time()
time_complexQuery2 = end_time_complexQuery2 - start_time_complexQuery2

with open('before_optimization/performance_mongo_bo.csv', 'a') as querys_archive:
    querys_archive.write(', ' + str("{:.7f}".format(explanaiton_simpleQuery1['executionStats']['executionTimeMillis'] / 1000)) + ', ' + str("{:.7f}".format(explanaiton_simpleQuery2['executionStats']['executionTimeMillis'] / 1000)) + ', ' + str(time_complexQuery1) + ', ' + str(time_complexQuery2) + "\n")

# Drop indexes
matches.drop_index('city_1_date_1')
goalscores.drop_index('scorer_1_minute_1')
goalscores.drop_index("match_id_1") # Criação de índices para $lookup
matches.drop_index("tournament_id_1") # Criação de índices para $lookup
matches.drop_index("country_id_1") # Criação de índices para $lookup
goalscores.drop_index("team.team_name_1_matches.tournament.tournament_name_1_matches.country.country_name_1") # Criação de index composto para $match
# goalscores.drop_index("scorer_1_total_gols_-1") # Criação de index para $group e $sort
goalscores.drop_index("scorer_1_total_goals_-1") # Criação de index para $group e $sort
matches.drop_index("home_team_id_1_away_team_id_1") # Criar índices para a coleção 'matches'
teams.drop_index("team_name_1") # Criar índices para a coleção 'teams'
goalscores.drop_index("match_id_1_home_score_1_away_score_1") # Certifique-se de que você tem um índice composto para os campos usados no $group e $sort
matches.drop_index("home_team_id_1_away_team_id_1_date_1") # Certifique-se de que você tem um índice composto para os campos usados no $group
print("Dropped indexes")