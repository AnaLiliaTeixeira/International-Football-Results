import time
import mysql.connector
import pandas as pd

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password='1234',
    database='bda2324_4'
)
mycursor = mydb.cursor()

print("----First Simple Query----")
# a. Two simples queries, selecting data from one or two columns/fields
# Query simples que irá obter todos os jogos que foram realizados em Lisbon, em 2023
query1 = f'SELECT * FROM Matches WHERE city = "Lisbon" AND YEAR(date) = 2023;'
start_time = time.time()
mycursor.execute(query1)
games_result = mycursor.fetchall()
end_time = time.time()
time1=end_time - start_time
for row in games_result:
    print(row)
print("--------------------------------------------Tempo - Query Simples 1:", time1)

    
    
print("----Second Simple Query----")
# a. Two simples queries, selecting data from one or two columns/fields
# Query simples que irá obter todos os golos marcados por Cristiano Ronaldo antes dos primeiros 5 minutos de jogo
query2 = f'SELECT * FROM Goalscorers WHERE scorer = "Cristiano Ronaldo" AND minute<5;'
start_time = time.time()
mycursor.execute(query2)
games_result = mycursor.fetchall()
end_time = time.time()
time2=end_time - start_time
for row in games_result:
    print(row)
print("--------------------------------------------Tempo - Query Simples 2:",time2)



print("----First Complex Query----")
# b. Two queries, using joins and aggregates, involving at least 2 tables/collections of your database (em que uma tem que ter mais do 5 joins)
# COMPLEX QUERY b.1: Top 5 jogadores (scorer) descrescente,
# de Portugal(team),
# que tem mais golos no torneio: FIFA World Cup Qualification,
# cujo os jogos foram realizados em Portugal
# e que esses jogos podem ter ido ou não a disputa de penaltys.
# ----------------------
query3 = """SELECT gs.scorer, COUNT(gs.scorer) AS total_gols
        FROM Goalscorers gs
        JOIN Matches m ON gs.match_id = m.match_id
        LEFT JOIN Shootouts s ON s.match_id = m.match_id
        JOIN Teams t ON gs.team_id = t.team_id
        JOIN Tournaments trn ON m.tournament_id = trn.tournament_id
        JOIN Countries c ON m.country_id = c.country_id
        WHERE gs.team_id = (SELECT team_id FROM Teams WHERE team_name = "Portugal")
            AND trn.tournament_name = "FIFA World Cup Qualification"
            AND m.country_id = (SELECT country_id FROM Countries WHERE country_name = "Portugal")
        GROUP BY gs.scorer
        ORDER BY total_gols DESC
        LIMIT 5;"""
start_time = time.time()
mycursor.execute(query3)
query_result = mycursor.fetchall()
end_time = time.time()
time3=end_time - start_time
for row in query_result:
    print(row)
print("--------------------------------------------Tempo - Query Complexa 1:", time3)
# Query Complexa que irá obter os primeiros 5 jogos por ordem decrescente em que foram marcados mais golos.
print("----Second Complex Query----")
query4 = """ SELECT Matches.match_id, Matches.date, Teams.team_name AS home_team, TeamsAway.team_name AS away_team,
        Matches.home_score, Matches.away_score,
        CAST(SUM(Matches.home_score + Matches.away_score) AS UNSIGNED) AS total_goals
        FROM Matches
        JOIN Teams ON Matches.home_team_id = Teams.team_id
        JOIN Teams AS TeamsAway ON Matches.away_team_id = TeamsAway.team_id
        GROUP BY Matches.match_id, Matches.date, home_team, away_team, Matches.home_score, Matches.away_score
        ORDER BY total_goals DESC
        LIMIT 5;"""
start_time = time.time()
mycursor.execute(query4)
query_result = mycursor.fetchall()
end_time = time.time()
time4=end_time - start_time
for row in query_result:
    print(row)
print("--------------------------------------------Tempo - Query Complexa 2:", time4)
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
            home_team_id = (SELECT team_id FROM Teams WHERE team_name = 'Myanmar')
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
query = "INSERT INTO Teams (team_name) VALUES (%s);"
mycursor.execute(query,["BDA2324_4_team1"])
team1_id = mycursor.lastrowid
mycursor.execute(query,["BDA2324_4_team2"])
team2_id = mycursor.lastrowid
query= """INSERT INTO tournaments (tournament_name) VALUES (%s);"""
mycursor.execute(query,["BDA2324_4_tournament"])
tournment_id = mycursor.lastrowid
query = """INSERT INTO countries (country_name) VALUES (%s);"""
mycursor.execute(query,['BDA2324_4_country'])
country_id = mycursor.lastrowid
query = "INSERT INTO Matches (date, home_team_id, away_team_id, home_score, away_score, tournament_id, city, country_id, neutral) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
data_to_insert = ('2023-11-30',team1_id,team2_id,1,1,tournment_id,'Lisbon',country_id,False)
match_id = mycursor.execute(query,data_to_insert)
query="INSERT INTO Goalscorers (match_id, team_id, scorer, minute, own_goal, penalty) VALUES (%s, %s, %s, %s, %s, %s)"
data_to_insert = (match_id,team1_id,'Tomas Piteira',44,False,False)
mycursor.execute(query,data_to_insert)
data_to_insert = (match_id,team2_id,'Daniel Lopes',44,False,False)
mycursor.execute(query,data_to_insert)
query = "INSERT INTO Shootouts (match_id, winner_id, first_shooter_id) VALUES (%s, %s, %s)"
data_to_insert = (match_id,team2_id,team1_id)
mycursor.execute(query,data_to_insert)
print("-----------INSERT-----------")
end_time_insert = time.time()
print("--------------------------------------------Tempo de todos os INSERT:", end_time_insert - start_time_insert)
mydb.commit()
mydb.close()


