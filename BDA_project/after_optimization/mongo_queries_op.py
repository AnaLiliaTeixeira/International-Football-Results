import time
from pymongo import MongoClient
import pprint

client=MongoClient()
client = MongoClient('localhost', 27017)

db = client.BDA2324_4_OP

goalscores = db.goalscores
shootouts = db.shootouts
teams = db.teams
matches = db.matches
countries = db.countries
tournaments = db.tournaments


simpleQuery1 = { '$and': [ { 'city': "Lisbon" }, {'date': {"$regex": "2023"}} ] }

start_time_simpleQuery1 = time.time()
mydoc = matches.find(simpleQuery1)
end_time_simpleQuery1 = time.time()

print("Matches in Lisbon in 2023:", end='')
counter=0
for doc in mydoc:
    pprint.pprint(doc)
    counter=counter+1
print(' ', counter, ' resultados')

time_simpleQuery1 = end_time_simpleQuery1 - start_time_simpleQuery1
print("---------------------------------------Tempo total da operação de SimpleQuery1:", time_simpleQuery1 , 'segundos')

simpleQuery2 = {'$and':[{'scorer': 'Cristiano Ronaldo'}, {'minute': {'$lt': 5}}]}

start_time_simpleQuery2 = time.time()
mydoc = goalscores.find(simpleQuery2)
end_time_simpleQuery2 = time.time()

print("\nGolos que o Ronaldo marcou antes dos 5 minutos:", end='')
counter=0
for doc in mydoc:
    pprint.pprint(doc)
    counter=counter+1
print(' ', counter, ' resultados')
time_simpleQuery2 = end_time_simpleQuery2 - start_time_simpleQuery2
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

start_time_complexQuery1 = time.time()
result = goalscores.aggregate(complexQuery1)
end_time_complexQuery1 = time.time()

print("\nResultado da complex query 1:")
pprint.pprint(list(result))
time_complexQuery1 = end_time_complexQuery1 - start_time_complexQuery1
print("---------------------------------------Tempo total da operação de ComplexQuery1:", time_complexQuery1, 'segundos')


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

start_time_complexQuery2 = time.time()
result2 = matches.aggregate(complexQuery2)
end_time_complexQuery2 = time.time()

print("\nResultado da complex query 2:")
pprint.pprint(list(result2))
time_complexQuery2 = end_time_complexQuery2 - start_time_complexQuery2
print("---------------------------------------Tempo total da operação de ComplexQuery2:", time_complexQuery2, 'segundos')


updateQuery = {'date':'1882-02-18'}
newvalues = {"$set": {'home_score': 3, 'away_score': 12, 'home_team': 'Myanmar'} }
matches.update_one(updateQuery, newvalues)

updated = matches.find({'date': '1882-02-18'})
print("\nMatches updated:")
for doc in updated:
    pprint.pprint(doc)


match_id = matches.insert_one({'date':'2023-11-30', 'home_team': 'BDA2324_4_team1', 'away_team': 'BDA2324_4_team2', 'home_score': 1, 'away_score': 1, 'tournament': 'BDA2324_4_tournament', 'city': 'Lisbon', 'country': 'BDA2324_4_country', 'neutral': False})
goalscores.insert_one({'match_id': match_id.inserted_id, 'team': 'BDA2324_4_team1', 'scorer':'Tomas Piteira', 'minute': 44, 'own_goal':'false', 'penalty':'false'})
goalscores.insert_one({'match_id': match_id.inserted_id, 'team': 'BDA2324_4_team2', 'scorer':'Daniel Lopes', 'minute': 45, 'own_goal':'false', 'penalty':'false'})
shootouts.insert_one({'match_id': match_id.inserted_id, 'winner': 'BDA2324_4_team2', 'first_shooter': 'BDA2324_4_team1'})
