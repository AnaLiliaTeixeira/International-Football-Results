import time
import mysql.connector
import pandas as pd
from mysql_queries_op import query1,query2,query3,query4

mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password='1234',
    database='bda2324_4_op'
)
mycursor = mydb.cursor()
# ---------------------INDEX 1-----------------------------
query = "CREATE INDEX idx_city_date ON Matches(city, date)"
mycursor.execute(query)
#---------------------INDEX 2-----------------------------
query = "CREATE INDEX idx_scorer_minute ON Goalscorers(scorer, minute)"
mycursor.execute(query)
#---------------------INDEX 3-----------------------------
query = "CREATE INDEX idx_home_team_name ON Matches(home_team);"
mycursor.execute(query)
#---------------------INDEX 4-----------------------------
query = "CREATE INDEX idx_away_team_name ON Matches(away_team);"
mycursor.execute(query)
#---------------------INDEX 5-----------------------------
query = "CREATE INDEX idx_home_away_match_id ON Matches(home_team, away_team,match_id);"
mycursor.execute(query)
#---------------------INDEX 6-----------------------------
query = "CREATE INDEX idx_team_id ON Goalscorers(team);"
mycursor.execute(query)
#---------------------INDEX 7-----------------------------
query = "CREATE INDEX idx_match_id ON Goalscorers(match_id);"
mycursor.execute(query)
#---------------------INDEX 12-----------------------------
query = "CREATE INDEX idx_tournament_name ON Matches (tournament);"
mycursor.execute(query)
#---------------------INDEX 13-----------------------------
query = "CREATE INDEX idx_countries_name_id ON Matches  (country);"
mycursor.execute(query)
#---------------------INDEX 14-----------------------------
query = "CREATE INDEX idx_scorer_match_team ON Goalscorers   (scorer, match_id,team);"
mycursor.execute(query)

#---------------------PRINT INDEXEX-----------------------
sqlShowIndexes = """SELECT DISTINCT table_name, index_name
                FROM information_schema.statistics
                WHERE table_schema = 'bda2324_4';"""
mycursor.execute(sqlShowIndexes)
indexList = mycursor.fetchall()
print("Indexes were created")
print(indexList)

explain_query = f"EXPLAIN {query1}"
start_time = time.time()
mycursor.execute(query1)
query_result = mycursor.fetchall()
end_time = time.time()
time1=end_time - start_time
print("query1 with index:",time1)

explain_query = f"EXPLAIN {query2}"
start_time = time.time()
mycursor.execute(query2)
query_result = mycursor.fetchall()
end_time = time.time()
time2=end_time - start_time
print("query2 with index:",time2)

explain_query = f"EXPLAIN {query3}"
start_time = time.time()
mycursor.execute(query3)
query_result = mycursor.fetchall()
end_time = time.time()
time3=end_time - start_time
print("query3 with index:",time3)

explain_query = f"EXPLAIN {query4}"
start_time = time.time()
mycursor.execute(query4)
query_result = mycursor.fetchall()
end_time = time.time()
time4=end_time - start_time
print("query4 with index:",time4)

