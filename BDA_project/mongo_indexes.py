from pymongo import MongoClient
import pprint

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

teams.create_index([('team_name', pymongo.ASCENDING)])
tournaments.create_index([('tournament_name', pymongo.ASCENDING)])
countries.create_index([('country_name', pymongo.ASCENDING)])

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
        "$lookup": {
            "from": "teams",
            "localField": "team_id",
            "foreignField": "_id",
            "as": "team"
        }
    },
    {
        "$lookup": {
            "from": "tournaments",
            "localField": "match.tournament_id",
            "foreignField": "_id",
            "as": "tournament"
        }
    },
    {
        "$lookup": {
            "from": "countries",
            "localField": "match.country_id",
            "foreignField": "_id",
            "as": "country"
        }
    },
    {
        "$match": {
            "team.team_name": "Portugal",
            'tournament.tournament_name': 'FIFA World Cup qualification',
            "country.country_name": "Portugal"
        }
    },
        {
        "$group": {
            "_id": "$scorer",
            "total_gols": { "$sum": 1 }
        }
    },
    {
        "$sort": { "total_gols": -1 } 
    },
    {
        "$limit": 5
    }
]

complexQuery2 = [
    {"$lookup": {"from": "teams",
                "localField": "home_team_id",
                "foreignField": "_id",
                "as": "home_team"
                }
    },
    {"$lookup": {"from": "teams",
                "localField": "away_team_id",
                "foreignField": "_id",
                "as": "away_team"
                }
    },
    {"$lookup": {"from": "Goalscorers",
                "localField": "match_id",
                "foreignField": "_id",
                "as": "goalscorers"
                }
    },
    {"$group": {"_id": {"match_id": "$match_id",
                        "date": "$date",
                        "home_team": "$home_team.team_name",
                        "away_team": "$away_team.team_name",
                        "home_score": "$home_score",
                        "away_score": "$away_score"
                        },
                "total_goals": {
                    "$sum": {
                    "$add": ["$home_score", "$away_score"]
                    }
                }}
    },
    {"$sort": {"total_goals": -1}}, 
    {"$limit": 5}
]

#Index para a simple query 1
matches.create_index([('date', pymongo.ASCENDING)])
result_simpleQuery1 = matches.find(simpleQuery1)
for doc in result_simpleQuery1:
    pprint.pprint(doc)
explanaiton_simpleQuery1 = result_simpleQuery1.explain()
pprint.pprint(explanaiton_simpleQuery1)

# Index para a simple query 2 - index composto nas chaves 'scorer' e 'minute'
goalscores.create_index([('scorer', 1), ('minute', 1)])
result_simpleQuery2 = goalscores.find(simpleQuery2)
for doc in result_simpleQuery2:
    pprint.pprint(doc)
explanaiton_simpleQuery2 = result_simpleQuery2.explain()
pprint.pprint(explanaiton_simpleQuery2)

# Index para a complex query 1 - Criação de índices para $lookup
goalscores.create_index("match_id")
goalscores.create_index("team_id")
matches.create_index("tournament_id")
matches.create_index("country_id")

# Criação de índice composto para $match
goalscores.create_index([("team.team_name", pymongo.ASCENDING), ("matches.tournament.tournament_name", pymongo.ASCENDING), ("matches.country.country_name", pymongo.ASCENDING)])

# Criação de índice para $group e $sort
goalscores.create_index([("scorer", pymongo.ASCENDING), ("total_gols", pymongo.DESCENDING)])

# result_complexQuery1 = goalscores.aggregate(complexQuery1)
# result_complexQuery1 = goalscores.find(complexQuery1)
# for doc in result_complexQuery1:
#     pprint.pprint(doc)
# explanaiton_complexQuery1 = result_complexQuery1.explain()
# pprint.pprint(explanaiton_complexQuery1)

with open('performance_mongo.txt', 'a') as querys_archive:
    querys_archive.write(', ' + str(explanaiton_simpleQuery1['executionStats']['executionTimeMillis'] / 1000) + ', ' + str(explanaiton_simpleQuery2['executionStats']['executionTimeMillis'] / 1000))
