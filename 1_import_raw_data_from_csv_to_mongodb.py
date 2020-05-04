#!/usr/bin/env python
import pandas as pd
import json
from pymongo import MongoClient
from pathlib import Path

csv_file_path = Path("/home/priyesh/Documents/PDA_CA3/Dataset/")
db_name = "pdaproject"
db_url = "localhost"
db_port = 27017

# Import CSV file and load it in Mongo DB in different collections
def import_file(csv_file_name, coll_name):
    mongoClient = MongoClient(db_url, db_port)
    dbName = mongoClient[db_name]
    print ('dbName: ' + str(dbName))
    collName = dbName[coll_name]
    print ('collName: ' + str(collName))

    fileName = csv_file_path / csv_file_name
    print ('fileName: ' + str(fileName))
    csvData = pd.read_csv(fileName)
    jsonData = json.loads(csvData.to_json(orient='records'))

    collName.remove()
    collName.insert(jsonData)
    print ('insert data count: ' + str(collName.count()))

# Call import_file function with CSV file name and collection name
import_file("hepatitis.csv", "hepatitis")
import_file("measles.csv", "measles")
import_file("mumps.csv", "mumps")
