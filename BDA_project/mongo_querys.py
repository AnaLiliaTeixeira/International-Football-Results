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
query1 = { '$and': [ { 'city': "Lisbon" }, {'date': {"$regex": "2023"}} ] }
mydoc = matches.find(query1)
for doc in mydoc:
    pprint.pprint(doc)

# b. Two complex queries, using joins and aggregates, involving at least 2 tables/collections of your database (em que uma tem que ter mais do 5 joins)

# aggregate the results for two queries: find all the scorer's names starting with A and count the repeats
# este foi o exemplo que a prof deu na aula convém mudar um pouco
mydoc= goalscores.aggregate([{ "$match": { "scorer": {"$regex": "^A"} } },
                             {"$group" : {"_id" : "$scorer", "count repeats" : {"$sum" : 1} }}])
            

for doc in mydoc:
    pprint.pprint(doc)
