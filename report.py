from google_analytics import GoogleAnalytics
from mongoDB import MongoDB
from dotenv import load_dotenv
import os
load_dotenv()


# ga report 
api_key = 'api_keys.json'
ga_view_id = '147632634'
start_date = '2017-08-30'
end_date = '2022-08-30'
metric_list = ['ga:bounceRate']
dimension_list = ['ga:pagePath']
max_result = 100000

# mongoDB
connection_string = os.getenv("MONGO_URI")
database_name = "ga-report-testsss"
collection_name = "class_page"

ga = GoogleAnalytics(api_key, ga_view_id, start_date, end_date, metric_list, dimension_list,max_result)
mongo = MongoDB(connection_string, database_name, collection_name)

report = ga.get_report()
mongo.write_to_mongo(report)
