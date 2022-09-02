import config
from dotenv import load_dotenv
import os
load_dotenv()

# ga report 
api_key = 'api_keys.json'
ga_view_id = '147632634'
start_date = '7daysAgo'
end_date = '2022-08-30'
metric_list = ['ga:bounceRate']
dimension_list = ['ga:pagePath']
max_result = 2000

# mongoDB
connection_string = os.getenv("MONGO_URI")
database_name = "ga-report-test"
collection_name = "page_report"

report = config.write_report_to_mongo(api_key, ga_view_id, start_date, end_date, metric_list, dimension_list,max_result,connection_string, database_name, collection_name)
