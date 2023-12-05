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

# a. Two simples queries, selecting data from one or two columns/fields

#Query simples que irá obter todos os jogos que foram realizados em Lisbon, em 2023
simpleQuery1 = { '$and': [ { 'city': "Lisbon" }, {'date': {"$regex": "2023"}} ] }
mydoc = matches.find(simpleQuery1)
print("Matches in Lisbon in 2023:")
counter=0
for doc in mydoc:
#    pprint.pprint(doc)
    counter=counter+1
print(counter)

simpleQuery2 = {'$and':[{'scorer': 'Cristiano Ronaldo'}, {'minute': {'$lt': 5}}]}
mydoc = goalscores.find(simpleQuery2)
print("\nGolos que o Ronaldo marcou antes dos 5 minutos:")
counter=0
for doc in mydoc:
#    pprint.pprint(doc)
    counter=counter+1
print(counter)
# b. Two complex queries, using joins and aggregates, involving at least 2 tables/collections of your database (em que uma tem que ter mais do 5 joins)

# aggregate the results for two queries: find all the scorer's names starting with A and count the repeats

# este foi o exemplo que a prof deu na aula convém mudar um pouco
# mydoc= goalscores.aggregate([{ "$match": { "scorer": {"$regex": "^A"} } },
#                              {"$group" : {"_id" : "$scorer", "count repeats" : {"$sum" : 1} }}])
            


#COMPLEX QUERY1: Top 5 jogadores (scorer), de Portugal(team), que tem mais golos no torneio: FIFA World Cup Qualification, 
# cujo os jogos foram realizados em Portugal 
# e que esses jogos podem ter ido ou não a disputa de penaltys. 

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


result = goalscores.aggregate(complexQuery1)
pprint.pprint(list(result))


 #Complex Query 2
complexQuery2 = [
    {
        "$lookup": {
            "from": "teams",
            "localField": "home_team_id",
            "foreignField": "_id",
            "as": "home_team"
        }
    },
    {
        "$lookup": {
            "from": "teams",
            "localField": "away_team_id",
            "foreignField": "_id",
            "as": "away_team"
        }
    },
    {
        "$lookup": {
            "from": "Goalscorers",
            "localField": "match_id",
            "foreignField": "_id",
            "as": "goalscorers"
        }
    },
    {
        "$group": {
            "_id": {
                "match_id": "$match_id",
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
            }
        }
    },
    {
    "$sort": {
      "total_goals": -1 
    }
    }, 
    {
        "$limit": 5
    }
]

result2 = matches.aggregate(complexQuery2)
print()
print("Complex query 2:")
pprint.pprint(list(result2))

# c. One update
updateQuery = {'date':'1882-02-18'}
newvalues = {"$set": {'home_score': 3, 'away_score': 12, 'home_team': teams.find_one({'team_name': 'Myanmar'})['_id']} }
matches.update_one(updateQuery, newvalues)

updated = matches.find({'date': '1882-02-18'})
print("Matches updated:")
for doc in updated:
    pprint.pprint(doc)

# d. One insert
teams.insert_one({'team_name': 'BDA2324_4_team1'})
teams.insert_one({'team_name': 'BDA2324_4_team2'})
tournaments.insert_one({'tournament_name': 'BDA2324_4_tournament'})
countries.insert_one({'country_name': 'BDA2324_4_country'})
matches.insert_one({'date':'2023-11-30', 'home_team': teams.find_one({'team_name': 'BDA2324_4_team1'})['_id'], 'away_team': teams.find_one({'team_name': 'BDA2324_4_team2'})['_id'], 'home_score': 1, 'away_score': 1, 'tournament': tournaments.find_one({'tournament_name': 'BDA2324_4_tournament'})['_id'], 'city': 'Lisbon', 'country': countries.find_one({'country_name': 'BDA2324_4_country'})['_id'], 'neutral': False})
goalscores.insert_one({'match': matches.find_one({'date':'2023-11-30'}), 'team': teams.find_one({'team_name': 'BDA2324_4_team1'})['_id'], 'scorer':'Tomas Piteira', 'minute': 44, 'own_goal':'false', 'penalty':'false'})
goalscores.insert_one({'match': matches.find_one({'date':'2023-11-30'}), 'team': teams.find_one({'team_name': 'BDA2324_4_team2'})['_id'], 'scorer':'Daniel Lopes', 'minute': 45, 'own_goal':'false', 'penalty':'false'})
shootouts.insert_one({'match': matches.find_one({'date':'2023-11-30'}), 'winner': teams.find_one({'team_name': 'BDA2324_4_team2'})['_id'], 'first_shooter':teams.find_one({'team_name': 'BDA2324_4_team1'})['_id']})