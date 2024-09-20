from cloudfunctions.marketo.Connector import Connector as MarketoConnector
from cloudfunctions.snowflake.Connector import Connector as SnowflakeConnector
from cloudfunctions.updateActivitiesAsync import getActivitiesDataAsync
from cloudfunctions.updateMissingLeads import updateMissingLeads
from cloudfunctions.addColumns import addColumns
from cloudfunctions.updateActivitiesAsync2 import getActivitiesDataAsync2


marketo = MarketoConnector()
snowflake = SnowflakeConnector()
meta_df = snowflake.getMetaData()

meta_dict = meta_df.set_index('TABLE_NAME')['MAX_TIMESTAMP'].to_dict()
count_dict = meta_df.set_index('TABLE_NAME')['ROW_COUNT'].to_dict()

def updateStaticLists():
    table_name = "MARKETO_STATIC_LIST_IDS"
    static_list_df = marketo.getStaticListData()
    print(static_list_df)
    # addColumns(table_name, static_list_df)
    snowflake.writeDataframe(static_list_df, table_name)
    return

def updateStaticListLeads(listId):
    table_name = "MARKETO_STATIC_LIST_LEADS"
    static_list_df = marketo.getStaticListLeadsData(listId)
    # print(static_list_df)
    # addColumns(table_name, static_list_df)
    snowflake.writeDataframe(static_list_df, table_name)
    return

#Always Use this:
def updateStaticLeadsData():
    ListDf = snowflake.getRemainingStaticListIdData()
    ListIds = ListDf['id'].tolist()
    for id in ListIds:
        updateStaticListLeads(id)
    return

def updateStaticData():
    ListDf = snowflake.getStaticListIdData()
    ListIds = ListDf['id'].tolist()
    for id in ListIds:
        updateStaticListLeads(id)
    return

def updateMarketoActivities2():
    nextPageToken = snowflake.getNextPageToken2()
    print(nextPageToken)
    getActivitiesDataAsync2(nextPageToken)
    return

def updateMarketoActivities():
    nextPageToken = snowflake.getNextPageToken()
    print(nextPageToken)
    getActivitiesDataAsync(nextPageToken)
    return

def updateMarketoEmails():
    table_name = "MARKETO_EMAILS"
    email_df = marketo.getEmailsData(count_dict[table_name]-1)
    addColumns(table_name, email_df)
    snowflake.writeDataframe(email_df, table_name)
    return

def updateMarketoPrograms():
    table_name = "MARKETO_PROGRAMS"
    program_df = marketo.getProgramsData(count_dict[table_name]-1)
    # program_df.to_csv("programs.csv")
    addColumns(table_name, program_df)
    # print(program_df)
    snowflake.writeDataframe(program_df, table_name)
    return

def updateSmartCampaigns():
    table_name = "MARKETO_SMART_CAMPAIGNS"
    smart_campaign_df = marketo.getSmartCampaignsData(count_dict[table_name]-1)
    addColumns(table_name, smart_campaign_df)
    snowflake.writeDataframe(smart_campaign_df, table_name)
    return

def updateLeads():
    table_name = "MARKETO_LEADS"
    df = marketo.getLeadsData(meta_dict[table_name])
    # df.to_csv(f"data_{table_name}.csv",index=False)
    addColumns(table_name, df)
    snowflake.writeDataframe(df, table_name)
    updateMissingLeads()
    return

# Example function call
# updateLeads()

# functions = [ updateMarketoEmails, updateMarketoPrograms, updateSmartCampaigns, updateMarketoActivities, updateLeads]

# functions = [updateStaticLeadsData]

# functions = [ updateMarketoPrograms ]

# functions = [ updateStaticData ]

# Run only One Time
# functions = [ updateStaticLists]

functions = [ updateMarketoActivities2 ]