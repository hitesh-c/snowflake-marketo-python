import pandas as pd
from marketo.Connector import Connector as Marketo_connector
from snowflake.Connector import Connector as Snowflake_connector
# Correct way if your file is named updateActivitiesAsync.py
from updateActivitiesAsync import getActivitiesDataAsync


def update_marketo_activities():
    marketo = Marketo_connector()
    snowflake = Snowflake_connector()
    lead_df = pd.DataFrame()
    meta_df = pd.DataFrame()

    meta_df = snowflake.getMetaData()
    meta_timestamp_dict = meta_df.set_index('TABLE_NAME')['MAX_TIMESTAMP'].to_dict()
    # activity_df = marketo.getActivitiesData(since_datetime=meta_timestamp_dict["MARKETO_ACTIVITIES"])
    
    update_marketo_activities_async = getActivitiesDataAsync(since_datetime=meta_timestamp_dict["MARKETO_ACTIVITIES"])
    # print(activity_df.head(10))
    # res = snowflake.writeDataframe(activity_df, "MARKETO_ACTIVITIES")
    # print(snowflake.getMetaData())
    print(update_marketo_activities_async)

# Call the function to execute the code
update_marketo_activities()
