import mysql.connector
import pandas as pd

mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password='1234',
        database='bda2324_4'
)
mycursor = mydb.cursor()

# a. Two simples queries, selecting data from one or two columns/fields
#Query simples que irá obter todos os jogos que foram realizados em Lisbon, em 2023
query = f'SELECT * FROM Matches WHERE city = "Lisbon" AND YEAR(date) = 2023;'
mycursor.execute(query)
games_result = mycursor.fetchall()
for row in games_result:
    print(row)

#COMPLEX QUERY1: Top 5 jogadores (scorer), de Portugal(team), que tem mais golos no torneio: FIFA World Cup Qualification, 
# cujo os jogos foram realizados em Portugal 
# e que esses jogos podem ter ido ou não a disputa de penaltys. 
#----------------------
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
mycursor.execute(query)
query_result = mycursor.fetchall()
for row in query_result:
    print(row)
# b. Two complex queries, using joins and aggregates, involving at least 2 tables/collections of your database (em que uma tem que ter mais do 5 joins)

# c. One update

# d. One insert