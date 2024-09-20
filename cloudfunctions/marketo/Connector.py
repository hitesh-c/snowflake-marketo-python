import pandas as pd
from io import StringIO
import time
import json
import logging
from .Request import Request
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CustomException(Exception):
    def __init__(self, success, message, operation ):
        self.success = success
        self.message = message
        self.operation = operation
        self.table_name = "MARKETO_CLASS"
        super().__init__(message)

class Connector:
    def __init__(self):
        self.request = Request()
        self.today = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")

    def getToday(self):
        logger.info(self.today)

    def getStaticListLeadsData(self, listId):
        # lead_list = self.request.get('/rest/v1/leads/describe2.json', params={'sinceDatetime': '2021-10-01'}).json()
        # field_names = [field['name'] for field in lead_list['result'][0]['fields']]
        params = {
            # 'fields': ','.join(field_names),
            'batchSize': 300
        }
        # print(params)
        list_dicts = []
        try:
            # for listId in listIds:
            nextPageToken = None
            while True:
                if nextPageToken:
                    params['nextPageToken'] = nextPageToken
                response = self.request.get(f'/rest/v1/lists/{listId}/leads.json', params=params)
                static_list_data = response.json()
                if response.status_code == 200:
                    print(static_list_data.get('result', []))
                    list_dicts.extend(static_list_data.get('result', []))
                    nextPageToken = static_list_data.get('nextPageToken')
                    if not nextPageToken:
                        break
                else:
                    break

        except Exception as e:
            print(e)
            raise CustomException(success=False, message="Error Getting Static List Data", operation="getStaticListsData")

        df = pd.DataFrame(list_dicts, dtype=str)
        df['ListID'] = listId
        df = df.fillna("")
        return df
    
    def getStaticListData(self):
        # lead_list = self.request.get('/rest/v1/leads/describe2.json', params={'sinceDatetime': '2021-10-01'}).json()
        # field_names = [field['name'] for field in lead_list['result'][0]['fields']]
        params = {
            # 'fields': ','.join(field_names),
            'batchSize': 300
        }
        # print(params)
        list_dicts = []
        try:
            # for listId in listIds:
            nextPageToken = None
            while True:
                if nextPageToken:
                    params['nextPageToken'] = nextPageToken
                response = self.request.get(f'/rest/v1/lists.json', params=params)
                static_list_data = response.json()
                if response.status_code == 200:
                    list_dicts.extend(static_list_data.get('result', []))
                    nextPageToken = static_list_data.get('nextPageToken')
                    if not nextPageToken:
                        break
                else:
                    break

        except Exception as e:
            print(e)
            raise CustomException(success=False, message="Error Getting Static List Data", operation="getStaticListsData")

        df = pd.DataFrame(list_dicts, dtype=str)
        df = df.fillna("")
        return df

    def getEmailsData(self, initial_count):
        if not initial_count:
            raise CustomException(success=False, message="Initial count not provided", operation="getEmailsData")

        url = "/rest/asset/v1/emails.json"
        params = {
            'maxReturn': '200',
            'offset': initial_count
        }
        email_dicts = []
        while True:
            response = self.request.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                emails = data.get('result', [])
                if not emails:
                    break
                email_dicts.extend(emails)
                params['offset'] += 200
            else:
                logger.error(f"Failed to retrieve emails. Status code: {response.status_code}")
                logger.error(response.text)
                break
        df = pd.DataFrame(email_dicts, dtype=str)
        return df
    
    ###########   Deprecated Endpoint
    # def getCampaignsData(self):
    #     url = "/rest/v1/campaigns.json"
    #     params = {
    #         "batchSize": 300,  # Maximum number of records to return
    #         "nextPageToken": ""  # A token if the result set is greater than batch size
    #     }
    #     campaign_data = []
    #     while True:
    #         data = self.request.get(url, params=params)
    #         campaign_data.extend(data["result"])
    #         next_page_token = data.get("nextPageToken")
    #         if not next_page_token:
    #             break
    #         params["nextPageToken"] = next_page_token
    #         #print(len(campaign_data))
    #     #print(len(campaign_data))
    #     return campaign_data

    def getSmartCampaignsData(self, initial_count):
        if not initial_count:
            raise CustomException(success=False, message="Initial count not provided", operation="getSmartCampaignsData")
        
        url = "/rest/asset/v1/smartCampaigns.json"
        params = {
            'maxReturn': '200',
            'offset': initial_count
        }
        smart_campaign_dicts = []
        while True:
            response = self.request.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                smart_campaigns = data.get('result', [])
                if not smart_campaigns:
                    break
                smart_campaign_dicts.extend(smart_campaigns)
                params['offset'] += 200
            else:
                logger.error(f"Failed to retrieve smart campaigns. Status code: {response.status_code}")
                logger.error(response.text)
                break
        df = pd.DataFrame(smart_campaign_dicts, dtype=str)
        df = df.fillna("")
        return df

# ERROR:__main__:Lambda function updateMarketoPrograms raised an exception: 'NoneType' object has 
# no attribute 'columns'

    #Optimised with Async in different file
    def getActivitiesData(self, since_datetime):

        if not since_datetime:
            raise CustomException(success=False, message="Since Datetime not provided", operation="getActivitiesData")

        # logger.info(f"Getting Activities since {since_datetime}")
        activities_paging_token_url = "/rest/v1/activities/pagingtoken.json"
        params = {"sinceDatetime": since_datetime}
        paging_token_data = self.request.get(activities_paging_token_url, params=params).json()
        next_page_token = paging_token_data.get("nextPageToken", "")
        # logger.info(f"Paging Token for activities since {since_datetime}: {next_page_token}")

        params = {
            "nextPageToken": next_page_token,
            "batchSize": 300,
            "activityTypeIds": [1, 2, 3, 6, 7, 8, 9, 10, 11, 27]
        }
        activity_data = []
        while True:
            try:
                activities_url = f"/rest/v1/activities.json"
                data = self.request.get(activities_url, params=params).json()
                activity_data.extend(data['result'])
                if data['moreResult']:
                    params['nextPageToken'] = data['nextPageToken']
                else:
                    break
            except Exception as e:
                logger.error(f"Error processing activities: {e}")
                break

        df = pd.DataFrame(activity_data, dtype=str)
        df = df.fillna("")
        max_date = df['activityDate'].max()
        # logger.info(f"Got activities till {max_date}")
        return df

    def getProgramsData(self, initial_count):
        if not initial_count:
            raise CustomException(success=False, message="Initial count not provided", operation="getProgramsData")
        print(initial_count)
        # return
        api_url = f'/rest/asset/v1/programs.json'
        params = {
            'maxReturn': '200',
            'offset': initial_count-1,
        }
        program_dicts = []
        while True:
            response = self.request.get(api_url, params)
            if response.status_code == 200:
                data = response.json()
                programs = data.get('result', [])
                if not programs:
                    break
                    # raise CustomException(success=False, message="Failed to get Marketo Programs Data", operation="getProgramsData")
                program_dicts.extend(programs)
                params['offset'] += 200
            else:
                logger.error(f"Failed to retrieve programs. Status code: {response.status_code}")
                logger.error(response.text)
                break
        # print(program_dicts)
        df = pd.DataFrame(program_dicts, dtype=str)
        df = df.fillna("")
        return df

    def getLeadsDataById(self, fields, id):
        url = '/rest/v1/leads.json'
        params = { 'filterType': 'id',
                   'filterValues': id,
                   'fields' : fields,
                   "batchSize": 300,  # Maximum number of records to return
                   "nextPageToken": ""  # A token if the result set is greater than batch size
                 }
        
        data = self.request.get(url, params=params).json()
        #print(id)
        return data

    def getLeadsData(self, start_date):
        if not start_date:
            raise CustomException(success=False, message="Start Date not provided", operation="getLeadsData")
        
        logger.info(f"Getting Leads for {start_date} - {self.today}")
        # logger.info(f"Getting Leads for {start_date}")
        lead_list = self.request.get('/rest/v1/leads/describe2.json', params={'sinceDatetime': '2021-10-01'}).json()
        field_names = [field['name'] for field in lead_list['result'][0]['fields']]
        create_job_url = f"/bulk/v1/leads/export/create.json"
        job_payload = {
            "format": "CSV",
            "fields": field_names,
            "filter": {
                "createdAt": {
                    "startAt": f"{start_date}",
                    "endAt": f"{self.today}"
                }
            }
        }

        job_response = self.request.post(create_job_url, data=job_payload).json()
        print(job_response)
        if job_response["success"] is False:
            logger.error(f"Failed to create job: {job_response}")
            raise Exception(f"Failed to create job: {job_response}")
        export_id = job_response["result"][0]["exportId"]
        enqueue_url = f"/bulk/v1/leads/export/{export_id}/enqueue.json"
        self.request.post(enqueue_url)

        # Polling
        status_url = f"/bulk/v1/leads/export/{export_id}/status.json"
        while True:
            status_response = self.request.get(status_url).json()
            status = status_response["result"][0]["status"]
            # logger.info(f"Polling: {status}")
            if status in ["Completed", "Failed"]:
                break
            time.sleep(15)

        # After response, convert file
        file_url = f"/bulk/v1/leads/export/{export_id}/file.json"
        response = self.request.get(file_url)

        if response.status_code == 200:
            data = str(response.text)
            # print(data)
            try:
                data_io = StringIO(data)
                # file_path = 'lead_file.txt'
                # # Open the file in write mode and write the string to it
                # with open(file_path,'w', encoding='utf-8') as file:
                #     file.write(str(data))
                df = pd.read_csv(data_io, dtype=str)
                # df.to_csv(f"lead_{export_id}.csv", index=False)
                # df = df.fillna("")
                # print(df.head(10))
                # print(df.shape[0])
                return df
                # max_date = df['createdAt'].max()
                # logger.info(f"Got Leads till {max_date}")
                # return df
            except Exception as e:
                # file_path = 'lead_error_file.txt'
                # Open the file in write mode and write the string to it
                # with open(file_path,'w', encoding='utf-8') as file:
                #     file.write(str(data))
                # csv_filename = f"lead_error.csv"
                # df.to_csv(csv_filename, index=False)
                # logger.error(f"Failed to Parse Leads data for {self.today}. Error: {e}"
                print(e)
                # raise Exception(f"Failed to Parse Leads data for {self.today}.")
                raise CustomException(success=False, message="Error Getting File Data", operation="getLeadsData")

        else:
            logger.error("Failed to retrieve the data.")
            raise Exception("Failed to retrieve the data.")
