#!/usr/bin/env python
from pymongo import MongoClient

db_name = "pdaproject"
db_url = "localhost"
db_port = 27017

# Update null values of columns cases and incidence_per_capita to 0 in Mongo DB in different collections
def update_data(coll_name):
    mongoClient = MongoClient(db_url, db_port)
    dbName = mongoClient[db_name]
    print ('dbName: ' + str(dbName))
    collName = dbName[coll_name]
    print ('collName: ' + str(collName))

    if (coll_name == "hepatitis"):
        print ('Updating records for hepatitis')

        existingValues = { "cases": "" }
        newValues = { "$set": { "cases": 0 } }
        rowCount = collName.update_many(existingValues, newValues)
        print(rowCount.modified_count, " records updated.")

        existingValues = { "incidence_per_capita": "" }
        newValues = { "$set": { "incidence_per_capita": 0 } }
        rowCount = collName.update_many(existingValues, newValues)
        print(rowCount.modified_count, " records updated.")

    elif (coll_name == "measles"):
        print ('Updating records for measles')

        existingValues = { "cases": "" }
        newValues = { "$set": { "cases": 0 } }
        rowCount = collName.update_many(existingValues, newValues)
        print(rowCount.modified_count, " records updated.")

        existingValues = { "incidence_per_capita": "" }
        newValues = { "$set": { "incidence_per_capita": 0 } }
        rowCount = collName.update_many(existingValues, newValues)
        print(rowCount.modified_count, " records updated.")

    elif (coll_name == "mumps"):
        print ('Updating records for mumps')

        existingValues = { "cases": "" }
        newValues = { "$set": { "cases": 0 } }
        rowCount = collName.update_many(existingValues, newValues)
        print(rowCount.modified_count, " records updated.")

        existingValues = { "incidence_per_capita": "" }
        newValues = { "$set": { "incidence_per_capita": 0 } }
        rowCount = collName.update_many(existingValues, newValues)
        print(rowCount.modified_count, " records updated.")

# Call update_data function with collection name
update_data("hepatitis")
update_data("measles")
update_data("mumps")
