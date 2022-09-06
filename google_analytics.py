
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google.oauth2 import service_account
import time
import httplib2
import google


class GoogleAnalytics:

    def __init__(self, credentials, view_id, start_date, end_date, metrics, dimensions, max_result):
        self.view_id = view_id
        self.start_date = start_date
        self.end_date = end_date
        self.metrics = metrics
        self.dimensions = dimensions
        self.max_result = max_result
        self.crd = GoogleAnalytics.get_credentials(credentials)
        self.service = build('analyticsreporting', 'v4',
                             credentials=self.crd, cache_discovery=False)
        self.dimension_list = [{"name": dimension} for dimension in dimensions]
        self.metric_list = [{"expression": metric} for metric in metrics]

    @staticmethod
    def get_credentials(credentials):
        if not credentials or credentials == 'false':
            print('Given credentials is not valid')

        if type(credentials).__name__ == 'str':
            try:
                credentials = service_account.Credentials.from_service_account_file(credentials,
                                                                                    scopes=['https://www.googleapis.com/auth/analytics.readonly'])
            except:
                print("Given credentials file is not found...")
                return
        return credentials

    def get_report(self, page='0', include_empty_rows=True, retry=1):

        if not page:
            print("All pages have been read")
            return
        else:
            print('Response is getting ...')

        while retry < 5:
            try:
                results = self.service.reports().batchGet(body={
                    'reportRequests': [
                        {
                            'viewId': self.view_id,
                            # Add View ID from GA | Go to Google Analytics > Admin > View > View Settings and copy the View ID.
                            'dateRanges': [{'startDate': self.start_date, 'endDate': self.end_date}],
                            # [{'startDate': '30daysAgo', 'endDate': 'today'}],
                            # [{'expression': 'ga:sessions'}],
                            'metrics': self.metric_list,
                            # Get Pages [{"name": "ga:pagePath"}]
                            'dimensions': self.dimension_list,
                            "includeEmptyRows": include_empty_rows,
                            # "filtersExpression":"ga:pagePath=~products;ga:pagePath!@/translate", #Filter by condition "containing products"
                            # 'orderBys': [{"fieldName": "ga:sessions", "sortOrder": "DESCENDING"}],
                            'pageSize': self.max_result,
                            'pageToken': page
                        }]
                }).execute()

            except (TimeoutError, httplib2.ServerNotFoundError, google.auth.exceptions.TransportError):
                print(f"Connection problem! {retry}. retrying... ")
                if retry < 5:
                    retry += 1
                    time.sleep(3)
                    continue
                print("The operation was aborted because the connection could not be established!")
                exit(1)

            except Exception as e:
                print(repr(e))
                exit(1)

            retry = 5
            
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
                    get_dimensions = row.get('dimensions', [])
                    dateRangeValues = row.get('metrics', [])

                    for header, dimension in zip(dimensionHeaders, get_dimensions):
                        dictionary[header] = dimension
                    for i, values in enumerate(dateRangeValues):
                        for metricHeader, value in zip(metricHeaders, values.get('values')):
                            dictionary[metricHeader.get('name')] = value
                    yield dictionary

            print(f"{int(page)//self.max_result + 1}.page is loaded")
            page = results.get('reports', [])[0].get('nextPageToken')
            yield from GoogleAnalytics.get_report(self, page, retry=1)
            
