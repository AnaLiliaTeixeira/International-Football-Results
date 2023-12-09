import subprocess
import pandas as pd
import json
from pymongo import MongoClient
from json_files_creation import *

with open('after_optimization/performance_mongo_ao.csv', 'w') as querys_archive:
    querys_archive.write("Simple Query1, Simple Query2, ComplexQuery1, ComplexQuery2, Simple Query1 indexing, Simple Query2 indexing, ComplexQuery1 indexing, ComplexQuery2 indexing\n")

for i in range(30):
    print("Iteration number:", (i+1))
    subprocess.run(['python3', 'after_optimization/mongo_creation_op.py'])
    subprocess.run(['python3', 'after_optimization/mongo_indexes_op.py'])