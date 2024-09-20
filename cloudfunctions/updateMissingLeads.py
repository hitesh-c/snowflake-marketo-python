import logging
import pandas as pd
from .marketo.Connector import Connector as MarketoConnector
from .snowflake.Connector import Connector as SnowflakeConnector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def updateMissingLeads():
    marketo = MarketoConnector()
    snowflake = SnowflakeConnector()
    meta_df = snowflake.getTableColumns("MARKETO_LEADS")
    all_fields = meta_df['name'].tolist()
    lead_df = snowflake.getMissingLeads()
    # ONLY THESE FIELDS WILL BE UPDATED FOR MISSING LEADS
    selected_fields = ['id', 'mktoName', 'firstName', 'lastName', 'email', 'createdAt', 'updatedAt', 'CRD__c', 'acquisitionProgramId', 'address', 'advisorName', 'brokerDealer', 'Current_Broker_Dealer__pc', 'unsubscribed', 'emailInvalid', 'emailSuspended', 'formName', 'formSource', 'mobilePhone']
    ids = lead_df['leadId_MA'].tolist()
    # logger.info(f"Number of leads to update: {len(ids)}")

    leads = []
    missing_leads = []
    for lead_id in ids:
        try:
            data = marketo.getLeadsDataById(fields=selected_fields, id=lead_id)
            if not data["result"]:
                logger.info(f"Missing Lead not found: {lead_id}")
                missing_leads.append({"id": lead_id, "data": data})
            else:
                leads.append(data["result"][0])
        except Exception as e:
            logger.error(f"Error processing lead {lead_id}: {e}")
            break

    if leads:
        df = pd.DataFrame(columns=all_fields, data=leads, dtype="str")
        logger.info(f"Updating {len(leads)} Missing leads in Snowflake")
        snowflake.writeDataframe(df, "MARKETO_LEADS")
    else:
        logger.info("No missing leads found. Nothing to update.")

