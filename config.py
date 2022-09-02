
from pymongo import MongoClient
import timer
import time

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account

def get_credentials(credentials):
    if not credentials or credentials == 'false':
        print('Given credentials is not valid')
    if type(credentials).__name__ == 'str':
        credentials = service_account.Credentials.from_service_account_file(credentials, 
                                scopes = ['https://www.googleapis.com/auth/analytics.readonly'])
    return credentials

def write_to_mongo(connection_string,db_name,collection_name,report):
    try:
        cluster = MongoClient(connection_string)
        db = cluster[db_name]
        collection = db[collection_name]
        print("Writing report to mongoDB...")
        collection.insert_many(report)
    except:
        print("There is error while connecting to mongoDB")
    print("All report has successfully written to mongoDB!")
    
def get_analytics_report(credentials, view_id, start_date, end_date, metrics, dimensions, max_result,
                            include_empty_rows=True):
    
    crd = get_credentials(credentials)
    service = build('analyticsreporting', 'v4', credentials=crd, cache_discovery=False)
    dimension_list = [{"name": dimension} for dimension in dimensions]
    metric_list = [{"expression": metric} for metric in metrics]

    next_page = "0"
    number_of_retry = 0
    
    while next_page:
        try:
            print('Response is getting ...')
            
            results = service.reports().batchGet(body={
                'reportRequests': [
                    {
                        'viewId': view_id,
                        # Add View ID from GA | Go to Google Analytics > Admin > View > View Settings and copy the View ID.
                        'dateRanges': [{'startDate': start_date, 'endDate': end_date}],
                        # [{'startDate': '30daysAgo', 'endDate': 'today'}],
                        'metrics': metric_list,  # [{'expression': 'ga:sessions'}],
                        # Get Pages [{"name": "ga:pagePath"}]
                        'dimensions': dimension_list,
                        "includeEmptyRows": include_empty_rows,
                        # "filtersExpression":"ga:pagePath=~products;ga:pagePath!@/translate", #Filter by condition "containing products"
                        # 'orderBys': [{"fieldName": "ga:sessions", "sortOrder": "DESCENDING"}],
                        'pageSize': max_result,
                        'pageToken' : next_page
                    }]
            }).execute()
            
            print("Response is received")
            
                 # Extract Data
            for report in results.get('reports', []):
                columnHeader = report.get('columnHeader', {})
                dimensionHeaders = columnHeader.get('dimensions', [])
                metricHeaders = columnHeader.get(
                    'metricHeader', {}).get('metricHeaderEntries', [])
                rows = report.get('data', {}).get('rows', [])

                for row in rows:

                    dictionary = dict()
                    dimensions = row.get('dimensions', [])
                    dateRangeValues = row.get('metrics', [])

                    for header, dimension in zip(dimensionHeaders, dimensions):
                        dictionary[header] = dimension
                    for i, values in enumerate(dateRangeValues):
                        for metricHeader, value in zip(metricHeaders, values.get('values')):
                            dictionary[metricHeader.get('name')] = value

                    yield dictionary
          
            print(f"{int(next_page)//max_result + 1}.page is loaded")
            next_page = results.get('reports', [])[0].get('nextPageToken')
        
        except:
            number_of_retry += 1
            print(f"There is connection error. {number_of_retry}. retry after 10 seconds..")
            time.sleep(10)
            if number_of_retry < 5:
                continue
            else:
                print("Data could not be loaded due to some problems")
                break
    

def write_report_to_mongo(credentials, view_id, start_date, end_date, metrics, dimensions, max_result,
connection_string,db_name,collection_name):
    t= timer.Timer()
    t.start()
    report = get_analytics_report(credentials, view_id, start_date, end_date, metrics, dimensions, max_result,
                            include_empty_rows=True)
    write_to_mongo(connection_string,db_name,collection_name,report)
    t.stop()