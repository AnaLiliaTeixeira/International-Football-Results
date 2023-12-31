import time
from pymongo import MongoClient
import pprint

client=MongoClient()
client = MongoClient('localhost', 27017)

db = client.BDA2324_4

goalscores = db.goalscores
shootouts = db.shootouts
teams = db.teams
matches = db.matches
countries = db.countries
tournaments = db.tournaments

simpleQuery1 = { '$and': [ { 'city': "Lisbon" }, {'date': {"$regex": "2023"}} ] }

result_simpleQuery1 = matches.find(simpleQuery1)
explanaiton_simpleQuery1 = result_simpleQuery1.explain()

print("Matches in Lisbon in 2023:", end='')
pprint.pprint(list(result_simpleQuery1))
time_simpleQuery1 = explanaiton_simpleQuery1['executionStats']['executionTimeMillis'] / 1000



print("---------------------------------------Tempo total da operação de SimpleQuery1:", time_simpleQuery1 , 'segundos')

simpleQuery2 = {'$and':[{'scorer': 'Cristiano Ronaldo'}, {'minute': {'$lt': 5}}]}

result_simpleQuery2 = goalscores.find(simpleQuery2)
explanaiton_simpleQuery2 = result_simpleQuery2.explain()
print("\nGolos que o Ronaldo marcou antes dos 5 minutos:", end='')
pprint.pprint(list(result_simpleQuery2))
time_simpleQuery2 = explanaiton_simpleQuery2['executionStats']['executionTimeMillis'] / 1000



print("---------------------------------------Tempo total da operação de SimpleQuery2:", time_simpleQuery2, 'segundos')


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

start_time_complexQuery1 = time.time()
result = goalscores.aggregate(complexQuery1)
end_time_complexQuery1 = time.time()

print("\nResultado da complex query 1:")
pprint.pprint(list(result))
time_complexQuery1 = end_time_complexQuery1 - start_time_complexQuery1
print("---------------------------------------Tempo total da operação de ComplexQuery1:", time_complexQuery1, 'segundos')


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

start_time_complexQuery2 = time.time()
result2 = matches.aggregate(complexQuery2)
end_time_complexQuery2 = time.time()

print("\nResultado da complex query 2:")
pprint.pprint(list(result2))
time_complexQuery2 = end_time_complexQuery2 - start_time_complexQuery2
print("---------------------------------------Tempo total da operação de ComplexQuery2:", time_complexQuery2, 'segundos')

with open('performance_mongo.csv', 'a') as querys_archive:
    querys_archive.write(str(time_simpleQuery1) + ', ' + str(time_simpleQuery2) + ', ' + str(time_complexQuery1) + ', ' + str(time_complexQuery2))

updateQuery = {'date':'1882-02-18'}
newvalues = {"$set": {'home_score': 3, 'away_score': 12, 'home_team': teams.find_one({'team_name': 'Myanmar'})['_id']} }
matches.update_one(updateQuery, newvalues)

updated = matches.find({'date': '1882-02-18'})
print("\nMatches updated:")
for doc in updated:
    pprint.pprint(doc)


team_id_1 = teams.insert_one({'team_name': 'BDA2324_4_team1'})
team_id_2 = teams.insert_one({'team_name': 'BDA2324_4_team2'})
tournament_id = tournaments.insert_one({'tournament_name': 'BDA2324_4_tournament'})
country_id = countries.insert_one({'country_name': 'BDA2324_4_country'})
match_id = matches.insert_one({'date':'2023-11-30', 'home_team_id': team_id_1.inserted_id, 'away_team_id': team_id_2.inserted_id, 'home_score': 1, 'away_score': 1, 'tournament_id': tournament_id.inserted_id, 'city': 'Lisbon', 'country_id': country_id.inserted_id, 'neutral': False})
goalscores.insert_one({'match_id': match_id.inserted_id, 'team_id': team_id_1.inserted_id, 'scorer':'Tomas Piteira', 'minute': 44, 'own_goal':'false', 'penalty':'false'})
goalscores.insert_one({'match_id': match_id.inserted_id, 'team_id': team_id_2.inserted_id, 'scorer':'Daniel Lopes', 'minute': 45, 'own_goal':'false', 'penalty':'false'})
shootouts.insert_one({'match_id': match_id.inserted_id, 'winner_id': team_id_2.inserted_id, 'first_shooter': team_id_1.inserted_id})
