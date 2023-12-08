import time
from pymongo import MongoClient
import pprint

import pymongo

client=MongoClient()
client = MongoClient('localhost', 27017)

db = client.BDA2324_4

goalscores = db.goalscores
shootouts = db.shootouts
# teams = db.teams
matches = db.matches
# countries = db.countries
tournaments = db.tournaments

# Queries
simpleQuery1 = { '$and': [ { 'city': "Lisbon" }, {'date': {"$regex": "2023"}} ] }
simpleQuery2 = {'$and':[{'scorer': 'Cristiano Ronaldo'}, {'minute': {'$lt': 5}}]}
complexQuery1 = [
    {
        "$lookup": {
            "from": "matches",
            "localField": "match_id",
            "foreignField": "_id",
            "as": "match"
        }
    },
    {
        "$match": {
            'match.tournament': 'FIFA World Cup qualification',
            'match.country': 'Portugal',
            'team': 'Portugal'
        }
    },
        {
        "$group": {
            "_id": "$scorer",
            "total_goals": { "$sum": 1 }
        }
    },
    {
        "$sort": { "total_goals": -1 } 
    },
    {
        "$limit": 5
    }
]

#Complex Query 2
complexQuery2 = [
    
    {"$lookup": {"from": "Goalscorers",
                "localField": "match_ids",
                "foreignField": "_id",
                "as": "goalscorers"
                }
    },
    {
        "$group": {
            "_id": "$_id",
            "date": { "$first": "$date" },
            "home_team": { "$first": "$home_team" },
            "away_team": { "$first": "$away_team" },
            "home_score": { "$first": "$home_score" },
            "away_score": { "$first": "$away_score" },
            "total_goals": {
                "$sum": { "$add": ["$home_score", "$away_score"] }
            }
        }
    },
    {"$sort": {"total_goals": -1}}, 
    {"$limit": 5}
]

#Index para a simple query 1
matches.create_index([('city'), ('date')])
result_simpleQuery1 = matches.find(simpleQuery1)
for doc in result_simpleQuery1:
    pprint.pprint(doc)
explanaiton_simpleQuery1 = result_simpleQuery1.explain()
pprint.pprint(explanaiton_simpleQuery1)
time_simpleQuery1 = explanaiton_simpleQuery1['executionStats']['executionTimeMillis'] / 1000
print("---------------------------------------Tempo total da operação de SimpleQuery1:", time_simpleQuery1, 'segundos')

# Index para a simple query 2 - index composto nas chaves 'scorer' e 'minute'
goalscores.create_index([('scorer'), ('minute')])
result_simpleQuery2 = goalscores.find(simpleQuery2)
for doc in result_simpleQuery2:
    pprint.pprint(doc)
explanaiton_simpleQuery2 = result_simpleQuery2.explain()
pprint.pprint(explanaiton_simpleQuery2)
time_simpleQuery2 = explanaiton_simpleQuery2['executionStats']['executionTimeMillis'] / 1000
print("---------------------------------------Tempo total da operação de SimpleQuery2:", time_simpleQuery2, 'segundos')

# Index para a complex query 1
goalscores.create_index("match_id") # Criação de índices para $lookup
# goalscores.create_index("team") # Criação de índices para $lookup
matches.create_index("tournament_id") # Criação de índices para $lookup
# matches.create_index("country_id") # Criação de índices para $lookup
goalscores.create_index([("team", pymongo.ASCENDING), ("matches.tournament.tournament_name", pymongo.ASCENDING), ("matches.country", pymongo.ASCENDING)]) # Criação de index composto para $match
goalscores.create_index([("scorer", pymongo.ASCENDING), ("total_goals", pymongo.DESCENDING)]) # Criação de index para $group e $sort

print("Complex Query 1:")
start_time_complexQuery1 = time.time()
result_complexQuery1 = goalscores.aggregate(complexQuery1)
end_time_complexQuery1 = time.time()
for doc in result_complexQuery1:
    pprint.pprint(doc)
time_complexQuery1 = end_time_complexQuery1 - start_time_complexQuery1
print("---------------------------------------Tempo total da operação de ComplexQuery1:", time_complexQuery1, 'segundos')

# Index para a complex query 2
matches.create_index("home_team") # Criar índices para a coleção 'matches'
matches.create_index("away_team") # Criar índices para a coleção 'matches'
matches.create_index("date") # Criar índices para a coleção 'matches'
# teams.create_index("team_name") # Criar índices para a coleção 'teams'
# goalscores.create_index("match_id") # Criar índices para a coleção 'Goalscorers'
goalscores.create_index([("match_id", pymongo.ASCENDING), ("home_score", pymongo.ASCENDING), ("away_score", pymongo.ASCENDING)]) # Certifique-se de que você tem um índice composto para os campos usados no $group e $sort
matches.create_index([("home_team_id", pymongo.ASCENDING), ("away_team_id", pymongo.ASCENDING), ("date", pymongo.ASCENDING)]) # Certifique-se de que você tem um índice composto para os campos usados no $group

print("Complex Query 2:")
start_time_complexQuery2 = time.time()
result_complexQuery2 = matches.aggregate(complexQuery2)
end_time_complexQuery2 = time.time()
for doc in result_complexQuery2:
    pprint.pprint(doc)
time_complexQuery2 = end_time_complexQuery2 - start_time_complexQuery2
print("---------------------------------------Tempo total da operação de ComplexQuery2:", time_complexQuery2, 'segundos')

with open('performance_mongo_ao.csv', 'a') as querys_archive:
    querys_archive.write(', ' + str("{:.7f}".format(time_simpleQuery1)) + ', ' + str("{:.7f}".format(time_simpleQuery2)) + ', ' + str(time_complexQuery1) + ', ' + str(time_complexQuery2))