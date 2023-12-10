import subprocess
import pandas as pd
import json
from pymongo import MongoClient
from json_files_creation import *
import matplotlib.pyplot as plt

with open('performance_mongo.csv', 'w') as querys_archive:
    querys_archive.write("Simple Query1, Simple Query2, ComplexQuery1, ComplexQuery2, Simple Query1 Indexing W OP, Simple Query2 Indexing W OP, ComplexQuery1 Indexing W OP, ComplexQuery2 Indexing W OP\n")

for i in range(30):
    print("Iteration number:", (i+1))
    subprocess.run(['python3', 'before_optimization/mongo_creation.py'])
    subprocess.run(['python3', 'before_optimization/mongo_queries.py'])
    subprocess.run(['python3', 'after_optimization/mongo_creation_op.py'])
    subprocess.run(['python3', 'after_optimization/mongo_indexes_op.py'])

