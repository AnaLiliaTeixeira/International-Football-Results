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

# b. Two complex queries, using joins and aggregates, involving at least 2 tables/collections of your database (em que uma tem que ter mais do 5 joins)

# c. One update

# d. One insert