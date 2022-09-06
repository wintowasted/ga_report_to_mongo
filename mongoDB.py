from pymongo import MongoClient

class MongoDB:
    def __init__(self, connection_string,db_name,collection_name):
        self.connect_to_mongo(connection_string,db_name,collection_name)
        

    def connect_to_mongo(self,connection_string, db_name, collection_name):
        try:
            cluster = MongoClient(connection_string)
        except:
            print("There is no such database with given connection string...")
            return
        db = cluster[db_name]
        self.collection = db[collection_name]
        
    
    def write_to_mongo(self, report):
        try:
            print("Writing report to mongoDB...")
            self.collection.insert_many(report)
            print("All report has successfully written to mongoDB!")
        except Exception as e: 
            print(str(e))
            
        