import time
import mysql.connector
import pandas as pd

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password='1234',
    database='bda2324_4_op'
)
mycursor = mydb.cursor()

print("----First Simple Query----")
# a. Two simples queries, selecting data from one or two columns/fields
# Query simples que irá obter todos os jogos que foram realizados em Lisbon, em 2023
query = f'SELECT * FROM Matches WHERE city = "Lisbon" AND YEAR(date) = 2023;'
start_time = time.time()
mycursor.execute(query)
games_result = mycursor.fetchall()
end_time = time.time()
for row in games_result:
    print(row)
print("--------------------------------------------Tempo - Query Simples 1:", end_time - start_time)

    
    
print("----Second Simple Query----")
# a. Two simples queries, selecting data from one or two columns/fields
# Query simples que irá obter todos os jogos que foram realizados em Lisbon, em 2023
query = f'SELECT * FROM Goalscorers WHERE scorer = "Cristiano Ronaldo" AND minute<5;'
start_time = time.time()
mycursor.execute(query)
games_result = mycursor.fetchall()
end_time = time.time()
for row in games_result:
    print(row)
print("--------------------------------------------Tempo - Query Simples 2:", end_time - start_time)



print("----First Complex Query----")
# b. Two queries, using joins and aggregates, involving at least 2 tables/collections of your database (em que uma tem que ter mais do 5 joins)
# COMPLEX QUERY b.1: Top 5 jogadores (scorer),
# de Portugal(team),
# que tem mais golos no torneio: FIFA World Cup Qualification,
# cujo os jogos foram realizados em Portugal
# e que esses jogos podem ter ido ou não a disputa de penaltys.
# ----------------------
query = """SELECT gs.scorer, COUNT(gs.scorer) AS total_gols
        FROM Goalscorers gs
        JOIN Matches m ON gs.match_id = m.match_id
        LEFT JOIN Shootouts s ON s.match_id = m.match_id
        WHERE gs.team ="Portugal"
            AND m.tournament = "FIFA World Cup Qualification"
            AND m.country = "Portugal"
        GROUP BY gs.scorer
        ORDER BY total_gols DESC
        LIMIT 5;"""
start_time = time.time()
mycursor.execute(query)
query_result = mycursor.fetchall()
end_time = time.time()
for row in query_result:
    print(row)
print("--------------------------------------------Tempo - Query Complexa 1:", end_time - start_time)

print("----Second Complex Query----")
query = """ SELECT Matches.match_id, Matches.date, Matches.home_team, Matches.away_team,
        Matches.home_score, Matches.away_score,
        COUNT(Goalscorers.goal_id) AS total_goals
        FROM Matches
        LEFT JOIN Goalscorers ON Matches.match_id = Goalscorers.match_id
        GROUP BY Matches.match_id, Matches.date, home_team, away_team, Matches.home_score, Matches.away_score
        ORDER BY total_goals DESC
        LIMIT 5;"""

start_time = time.time()
mycursor.execute(query)
query_result = mycursor.fetchall()
end_time = time.time()
for row in query_result:
    print(row)
print("--------------------------------------------Tempo - Query Complexa 2:", end_time - start_time)


# c. One update
print("-----------UPDATED---------")
print("--Select Before Updated----")
query = """SELECT * FROM Matches
        WHERE date = '1882-02-18';"""
start_time_select_before_update = time.time()
mycursor.execute(query)
query_result = mycursor.fetchall()
for row in query_result:
    print(row)
end_time_select_before_update = time.time()
print("--------------------------------------------Tempo Select antes do UPDATE:", end_time_select_before_update - start_time_select_before_update)

query = """UPDATE Matches
        SET home_score = 3,
            away_score = 12,
            home_team = "Myanmar"
        WHERE date = '1882-02-18';"""
start_time_update = time.time()
mycursor.execute(query)
end_time_update = time.time()
print("--------------------------------------------Tempo do UPDATE:", end_time_update - start_time_update)

print("--Select After Updated----")
query = """SELECT * FROM Matches
        WHERE date = '1882-02-18';"""
start_time_select_after_update = time.time()
mycursor.execute(query)
query_result = mycursor.fetchall()
for row in query_result:
    print(row)
end_time_select_after_update = time.time()
print("--------------------------------------------Tempo  do Select após UPDATE:", end_time_select_after_update - start_time_select_after_update)

total_time_update = end_time_select_after_update - start_time_select_before_update
print("--------------------------------------------Tempo total das operações de UPDATE:", total_time_update)



# Insert one
start_time_insert = time.time()
query = "INSERT INTO Matches (date, home_team, away_team, home_score, away_score, tournament, city, country, neutral) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
data_to_insert = ('2023-11-30',"BDA2324_4_team1","BDA2324_4_team2",1,1,"BDA2324_4_tournament",'Lisbon',"BDA2324_4_country",False)
match_id = mycursor.execute(query,data_to_insert)
query="INSERT INTO Goalscorers (match_id, team, scorer, minute, own_goal, penalty) VALUES (%s, %s, %s, %s, %s, %s)"
data_to_insert = (match_id,"BDA2324_4_team1",'Tomas Piteira',44,False,False)
mycursor.execute(query,data_to_insert)
data_to_insert = (match_id,"BDA2324_4_team2",'Daniel Lopes',44,False,False)
mycursor.execute(query,data_to_insert)
query = "INSERT INTO Shootouts (match_id, winner, first_shooter) VALUES (%s, %s, %s)"
data_to_insert = (match_id,"BDA2324_4_team1","BDA2324_4_team2")
mycursor.execute(query,data_to_insert)
print("-----------INSERT-----------")
end_time_insert = time.time()
print("--------------------------------------------Tempo de todos os INSERT:", end_time_insert - start_time_insert)
mydb.commit()
mydb.close()


