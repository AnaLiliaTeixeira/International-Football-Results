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
#---------------------INDEX 2-----------------------------
query = "CREATE INDEX idx_home_away_team_id ON matches(home_team_id, away_team_id);"
mycursor.execute(query)
#---------------------INDEX 3-----------------------------
query = "CREATE INDEX idx_scorer_minute ON Goalscorers(scorer, minute)"
mycursor.execute(query)
#---------------------INDEX 4-----------------------------
query = "CREATE INDEX idx_team_scorer ON Goalscorers(team_id, scorer)"
mycursor.execute(query)
#---------------------PRINT INDEXEX-----------------------
sqlShowIndexes = "show index from Goalscorers"
mycursor.execute(sqlShowIndexes)
indexList = mycursor.fetchall()
print("Indexes were created")
print(indexList)


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

