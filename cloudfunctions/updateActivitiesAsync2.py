import pandas as pd
import logging
import asyncio
from io import StringIO
from .marketo.Request import Request
from .snowflake.Connector import Connector as SnowflakeConnector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def getActivitiesDataAsync2(next_page_token):
    request = Request()
    snowflake = SnowflakeConnector()
    # logger.info(f"Getting Activities since {since_datetime}")
    # since_datetime = "2023-09-01T00:00:01Z"
    # activities_paging_token_url = "/rest/v1/activities/pagingtoken.json"
    # params = {"sinceDatetime": since_datetime}
    # paging_token_data = request.get(activities_paging_token_url, params=params).json()
    # next_page_token = paging_token_data.get("nextPageToken")
    # logger.info(f"Paging Token for activities since {since_datetime}: {next_page_token}")
    # print(next_page_token)
    # asyncio.run(snowflake.writeLog(next_page_token, 'MARKETO_ACTIVITIES', 'nextPageToken2', success=True))
    # return
    print(next_page_token)
    # UM4GUSUPTFWIKLPLLIF2Q3AO5F55ABPY2RA7MRTISRTXDMOPK76Q====
    if next_page_token is None:
        raise Exception("next_page_token is Null.")


    table_name = "MARKETO_ACTIVITIES"
    params = {
        "nextPageToken": next_page_token,
        "batchSize": 300,
        "activityTypeIds": [12,13,23,26,30,42,46,32]
    }
    while True:
        try:
            activities_url = "/rest/v1/activities.json"
            data = request.get(activities_url, params=params).json()
            if data['result'] is None:
                raise Exception("No activities found.")
            
            df = pd.DataFrame(data['result'], dtype=str)
            # df.to_csv(f"data_{data['nextPageToken']}.csv",index=False)
            result = snowflake.writeDataframe(df, table_name)
            logger.info(f"Uploaded {len(df)} activities to Snowflake")
            # print(result)
            if data['moreResult']:
                params['nextPageToken'] = data['nextPageToken']
                asyncio.run(snowflake.writeLog(str(data['nextPageToken']), table_name, 'nextPageToken2', success=True))
            else:
                break
        except Exception as e:
            logger.error(f"Error: {e}")
            asyncio.run(snowflake.writeLog(str(e), table_name, 'getActivitiesDataAsync2'))
            # traceback.print_exc()  # This will print the traceback to the console, you can replace it with logger.error
            break

    return True

# Example function call
# getActivitiesDataAsync("2023-11-28T16:00:24Z")
