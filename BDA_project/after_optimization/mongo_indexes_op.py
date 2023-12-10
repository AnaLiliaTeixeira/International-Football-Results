import time
from pymongo import MongoClient
import pprint

import pymongo

client=MongoClient()
client = MongoClient('localhost', 27017)

db = client.BDA2324_4_OP

goalscores = db.goalscores
shootouts = db.shootouts
matches = db.matches
tournaments = db.tournaments

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
    {
        "$group": {
            "_id": "$_id",
            "date": { "$first": "$date" },
            "home_team": { "$first": "$home_team.team_name" },
            "away_team": { "$first": "$away_team.team_name" },
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

db.matches.create_index([("city", 1), ("date", 1)])
db.goalscores.create_index([("scorer", 1), ("minute", 1)])
db.matches.create_index([("home_team_id", 1)])
db.matches.create_index([("away_team_id", 1)])
db.matches.create_index([("home_team_id", 1), ("away_team_id", 1), ("match_id", 1)])
db.goalscores.create_index([("team_id", 1)])
db.goalscores.create_index([("match_id", 1)])
db.matches.create_index([("tournament_id", 1)])
db.matches.create_index([("country_id", 1)])
db.countries.create_index([("country_name", 1)])
db.teams.create_index([("team_name", 1), ("team_id", 1)])
db.tournaments.create_index([("tournament_id", 1), ("tournament_name", 1)])
db.countries.create_index([("country_name", 1), ("country_id", 1)])
db.goalscores.create_index([("scorer", 1), ("match_id", 1), ("team_id", 1)])

result_simpleQuery1 = matches.find(simpleQuery1)
explanaiton_simpleQuery1 = result_simpleQuery1.explain()

time_simpleQuery1 = explanaiton_simpleQuery1['executionStats']['executionTimeMillis'] / 1000
print("SimpleQuery1")
#pprint.pprint(explanaiton_simpleQuery1)
print("Time:",time_simpleQuery1)

result_simpleQuery2 = goalscores.find(simpleQuery2)
explanaiton_simpleQuery2 = result_simpleQuery2.explain()

time_simpleQuery2 = explanaiton_simpleQuery2['executionStats']['executionTimeMillis'] / 1000
print("SimpleQuery2")
#pprint.pprint(explanaiton_simpleQuery2)
print("Time:",time_simpleQuery2)

start_time_complexQuery1 = time.time()
result_complexQuery1 = goalscores.aggregate(complexQuery1)
end_time_complexQuery1 = time.time()
time_complexQuery1 = end_time_complexQuery1 - start_time_complexQuery1
print("complexQuery1")
print("Time:",time_complexQuery1)

start_time_complexQuery2 = time.time()
result_complexQuery2 = matches.aggregate(complexQuery2)
end_time_complexQuery2 = time.time()
time_complexQuery2 = end_time_complexQuery2 - start_time_complexQuery2
print("complexQuery1")
print("Time:",time_complexQuery2)
