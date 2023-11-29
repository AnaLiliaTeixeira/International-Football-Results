import mysql.connector
import pandas as pd

mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password='1234',
        database='bda2324_4'
)


# a. Two simples queries, selecting data from one or two columns/fields

# b. Two complex queries, using joins and aggregates, involving at least 2 tables/collections of your database

# c. One update

# d. One insert