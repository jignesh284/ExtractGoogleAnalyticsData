from pymongo import MongoClient
import pandas as pd
import fetch_data as fd
import datetime
from bson.objectid import ObjectId

# connect to MongoDB, change the << MONGODB URL >> to reflect your own connection string
#  'mongodb://localhost:27017/researchImpact2'
client = MongoClient(port=27017) 
print("connected to mongo database")
db = client.researchImpact2

def insertToDB(data):
    collection = db["analytics"]
    for i, rowData in data.iterrows(): 
        elem = {}
        for col in list(data):
            if(col == "dimension1"):
                elem["userId"] = ObjectId(rowData[col])
            else:
                elem[col] = rowData[col]
        updated = collection.insert_one(elem)


def main():
    today = datetime.date.today() 
    start_date = today - datetime.timedelta(6) # since dates are including the boundaries
    data = fd.fetch_data(str(start_date), str(today))
    insertToDB(data)
    print("Sucessfully updated data to Database")

main()


