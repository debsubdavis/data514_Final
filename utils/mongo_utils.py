from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import json

class MongoUtils:

    def write_to_collection(json_string, collection_str, city):
        
        uri = "mongodb+srv://{username}:{password}@{clustername}/?retryWrites=true&w=majority"

        #Create a new client and connect to the server
        client = MongoClient(uri, server_api=ServerApi('1'))
        db = client["{databasename}}"]
        collection = db[collection_str]

        # Send a ping to confirm a successful connection
        try:
            client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            print(e)
        #print(json_string)
        records_list = json.loads(json_string)
        print("printing first record to be inserted into the database")
        print(records_list[1])
        print(f'Number of records to be inserted {len(records_list)}')

        if (collection_str == "Reviews") and (city != "portland"):
            print("Reviews, city is not portland, so won't delete records in the MongoDB Collection")
        else:
            collection.delete_many({})

        test = collection.insert_many(records_list) 
        print(f"test: {test}")