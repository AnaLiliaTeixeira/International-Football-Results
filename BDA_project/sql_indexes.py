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

# ---------------------INDEX 1-----------------------------
query = "CREATE INDEX idx_city_date ON matches(city, date)"
mycursor.execute(query)

sqlShowIndexes = "show index from Matches"

mycursor.execute(sqlShowIndexes)
indexList = mycursor.fetchall()
#---------------------INDEX 2-----------------------------
query = "CREATE INDEX idx_home_away_team_id ON matches(home_team_id, away_team_id);"
mycursor.execute(query)
sqlShowIndexes = "show index from Matches"
mycursor.execute(sqlShowIndexes)
indexList = mycursor.fetchall()
#---------------------INDEX 3-----------------------------
query = "CREATE INDEX idx_scorer_minute ON Goalscorers(scorer, minute)"
mycursor.execute(query)
sqlShowIndexes = "show index from Goalscorers"
mycursor.execute(sqlShowIndexes)
indexList = mycursor.fetchall()
# #---------------------INDEX 4-----------------------------
query = "CREATE INDEX idx_team_scorer ON Goalscorers(team_id, scorer)"
mycursor.execute(query)
sqlShowIndexes = "show index from Goalscorers"
mycursor.execute(sqlShowIndexes)
indexList = mycursor.fetchall()

# # CREATE INDEX idx_city_date ON Matches(city, date);
# # CREATE INDEX idx_date ON Matches(date);
# # CREATE INDEX idx_home_away_team_id ON Matches(home_team_id, away_team_id);
# # CREATE INDEX idx_scorer_minute ON Goalscorers(scorer, minute);
# # CREATE INDEX idx_match_id ON Goalscorers(match_id);
# # CREATE INDEX idx_team_scorer ON Goalscorers(team_id, scorer);
# # CREATE INDEX idx_match_id_shootouts ON Shootouts(match_id);
# # CREATE INDEX idx_team_name ON Teams(team_name);
# # CREATE INDEX idx_team_id ON Teams(team_id);
# # CREATE INDEX idx_country_name ON Countries(country_name);
# # CREATE INDEX idx_country_id ON Countries(country_id);
# # CREATE INDEX idx_tournament_name ON Tournaments(tournament_name);
# # CREATE INDEX idx_tournament_id ON Tournaments(tournament_id);


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
mycursor.execute(query)
query_result = mycursor.fetchall()
end_time = time.time()
for row in query_result:
    print(row)
print("--------------------------------------------Tempo - Query Complexa 1:", end_time - start_time)

print("----Second Complex Query----")
query = """ SELECT Matches.match_id, Matches.date, Teams.team_name AS home_team, TeamsAway.team_name AS away_team,
        Matches.home_score, Matches.away_score,
        COUNT(Goalscorers.goal_id) AS total_goals
        FROM Matches
        JOIN Teams ON Matches.home_team_id = Teams.team_id
        JOIN Teams AS TeamsAway ON Matches.away_team_id = TeamsAway.team_id
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

