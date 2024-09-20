import pandas as pd
import logging
import asyncio
from io import StringIO
from .marketo.Request import Request
from .snowflake.Connector import Connector as SnowflakeConnector

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def addColumns(table_name, df):
    if df.empty:
        logger.info(f"Dataframe is empty, no columns to add to {table_name}")
        return True
    # request = Request()
    snowflake = SnowflakeConnector()
    existing_columns = snowflake.getTableColumns(table_name)
    # print(len(existing_columns['name'].tolist()))
    # print(len(set(df.columns.tolist())))
    new_columns = list(set(df.columns.tolist()) - set(existing_columns['name'].tolist()))
    # print(new_columns)
    # return {
    #     "table_name": table_name,
    #     "new_columns": new_columns,
    #     "existing_columns": existing_columns['name'].tolist(),
    #     "df_columns": df.columns.tolist()
    # }
    if new_columns:
        logger.info(f"Adding {len(new_columns)} columns to {table_name}")
        for column in new_columns:
            snowflake.addColumn(table_name, column)
    else:
        logger.info(f"No new columns to add to {table_name}")
    return True


# 