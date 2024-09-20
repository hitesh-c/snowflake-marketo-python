import os
import asyncio
import pandas as pd
import snowflake.connector as sf
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

class CustomException(Exception):
    def __init__(self, success, message, operation ):
        self.success = success
        self.message = message
        self.operation = operation
        self.table_name = "SNOWFLAKE_CLASS"
        super().__init__(message)

class Connector:
    def __init__(self):
        self.user = os.environ.get("SNOWFLAKE_USER")
        self.password = os.environ.get("SNOWFLAKE_PASSWORD")
        self.account = os.environ.get("SNOWFLAKE_ACCOUNT")
        self.warehouse = os.environ.get("SNOWFLAKE_WAREHOUSE")
        self.database = os.environ.get("SNOWFLAKE_DATABASE")
        self.schema = os.environ.get("SNOWFLAKE_SCHEMA")

    def connectSnowflake(self):
        try:
            conn = sf.connect(
                user=self.user,
                password=self.password,
                account=self.account,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema
            )
            # logger.info("Connected to Snowflake")
            return conn
        except Exception as e:
            logger.error(f"Connection to Snowflake failed: {e}")
            raise CustomException(success=False, message=f"Connection to Snowflake failed: {e}", operation="connectSnowflake")

    def addColumn(self, table_name, column_name):
        try:
            conn = self.connectSnowflake()
            # logger.info(f"Altering table {table_name}, connection {conn}")
            cursor = conn.cursor()
            query = f'ALTER TABLE {table_name} ADD COLUMN "{column_name}" VARCHAR(16777216)'
            cursor.execute(query)
            cursor.close()
            conn.close()
            asyncio.run(self.writeLog(message= f'Adding "{column_name}" to {table_name}', table_name = table_name, operation_name= "addColumn", success=True))
            # logger.info(f"Table {table_name} altered")
            return

        except Exception as e:
            logger.error(f"alter_table Error: {e}")
            raise CustomException(success=False, message=f"Error: {e}", operation="alterTable")

    def writeDataframe(self, df, table_name):
        try:
            df = df.fillna("")
            if df.empty:
                # Log a message and return without writing to the database
                print("No data found in the DataFrame. Aborting write operation.")
                asyncio.run(self.writeLog(success=True, message=f"No more data found", operation_name="write_dataframe", table_name=table_name))
                return None
            # logger.info(df.head(10))
            conn = self.connectSnowflake()
            # logger.info(f"Uploading data to {table_name}, connection {conn}")
            # df.to_csv("data.csv",index=False)
            # return
            
            res = write_pandas(
                conn=conn,
                df=df,
                table_name=table_name,
                database=self.database,
                schema=self.schema,
                auto_create_table=True
            )
            # Commit the changes
            conn.commit()
            conn.close()
            # logger.info(res)
            asyncio.run(self.writeLog(success=True, message=f"Successfully Uploaded {df.shape[0]} rows", operation_name="write_dataframe", table_name=table_name))
            return res

        except Exception as e:
            print(e)
            # Handle other exceptions
            logger.error(f"write_dataframe Error: {e}")
            asyncio.run(self.writeLog(success=False, message=f"Error: {e}", operation_name="writeDataframe", table_name=table_name))
            # raise CustomException(success=False, message=f"Error: {e}", operation="write_dataframe", table_name=table_name)

    async def writeLog(self, message, table_name, operation_name, success=False):
        if not table_name or not operation_name:
            logger.error("Table Name or Operation Name not provided")
            raise Exception("Table Name or Operation Name not provided")
        try:
            update_dict = {
                "message": [message],
                "table_name": [table_name],
                "operation_name": [operation_name],
                "success": [success],
            }
            df = pd.DataFrame(update_dict)
            conn = self.connectSnowflake()
            # logger.info(conn)
            
            res = write_pandas(
                conn=conn,
                df=df,
                table_name="MARKETO_AWS_LOGS",
                database=self.database,
                schema=self.schema,
                # auto_create_table=True   #DANGER
            )
            
            # Commit the changes
            conn.commit()
            conn.close()
            # logger.info("Log entry created")
            return res

        except Exception as e:
            logger.error(f"writeLog Error: {e}")
            raise CustomException(success=False, message=f"Error: {e}", operation="writeLog")

    def getMetaData(self):
        try:
            conn = self.connectSnowflake()
            # logger.info(f"Getting Meta Data, connection {conn}")
            cursor = conn.cursor()
            query = 'SELECT * FROM MARKETO_META_DATA_VIEW'
            cursor.execute(query)
            df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
            cursor.close()
            conn.close()
            return df

        except Exception as e:
            logger.error(f"get_meta_data Error: {e}")
            raise CustomException(success=False, message=f"Error: {e}", operation="getMetaData")

    def getStaticListIdData(self):
            try:
                conn = self.connectSnowflake()
                # logger.info(f"Getting Meta Data, connection {conn}")
                cursor = conn.cursor()
                query = 'SELECT "id" FROM MARKETO_STATIC_LIST_IDS'
                cursor.execute(query)
                df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
                cursor.close()
                conn.close()
                return df

            except Exception as e:
                logger.error(f"get_meta_data Error: {e}")
                raise CustomException(success=False, message=f"Error: {e}", operation="getMetaData")

    def getRemainingStaticListIdData(self):
            try:
                conn = self.connectSnowflake()
                # logger.info(f"Getting Meta Data, connection {conn}")
                cursor = conn.cursor()
                query = 'SELECT "id" FROM MARKETO_STATIC_REMAINING_IDS'
                cursor.execute(query)
                df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
                cursor.close()
                conn.close()
                return df

            except Exception as e:
                logger.error(f"get_meta_data Error: {e}")
                raise CustomException(success=False, message=f"Error: {e}", operation="getMetaData")

    def getNextPageToken(self):
        try:
            conn = self.connectSnowflake()
            # logger.info(f"Getting Next Page Token, connection {conn}")
            cursor = conn.cursor()
            query = 'SELECT "message" FROM MARKETO_AWS_LOGS WHERE "operation_name" = \'nextPageToken\' AND "table_name"= \'MARKETO_ACTIVITIES\' ORDER BY "timestamp" DESC LIMIT 1'
            cursor.execute(query)
            df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
            cursor.close()
            conn.close()
            token = str(df["message"][0])
            if token is None:
                raise Exception("next_page_token is Null.")
            return token
        except Exception as e:
            logger.error(f"get_next_page_token Error: {e}")
            raise CustomException(success=False, message=f"Error: {e}", operation="getNextPageToken")

    def getNextPageToken2(self):
        try:
            conn = self.connectSnowflake()
            # logger.info(f"Getting Next Page Token, connection {conn}")
            cursor = conn.cursor()
            query = 'SELECT "message" FROM MARKETO_AWS_LOGS WHERE "operation_name" = \'nextPageToken2\' AND "table_name"= \'MARKETO_ACTIVITIES\' ORDER BY "timestamp" DESC LIMIT 1'
            cursor.execute(query)
            df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
            cursor.close()
            conn.close()
            token = str(df["message"][0])
            if token is None:
                raise Exception("next_page_token is Null.")
            return token
        except Exception as e:
            logger.error(f"get_next_page_token Error: {e}")
            raise CustomException(success=False, message=f"Error: {e}", operation="getNextPageToken2")

    def getMissingLeads(self):
        try:
            conn = self.connectSnowflake()
            # logger.info(f"Getting Missing Leads, connection {conn}")
            cursor = conn.cursor()
            query = 'select DISTINCT "leadId_MA" from MARKETODATAMART where "id_ML" is NULL order By TRY_CAST("leadId_MA" as NUMBER) ASC'
            cursor.execute(query)
            df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
            cursor.close()
            conn.close()
            return df

        except Exception as e:
            logger.error(f"get_missing_leads Error: {e}")
            raise CustomException(success=False, message=f"Error: {e}", operation="getMissingLeads")

    def getTableColumns(self, table_name):
        try:
            conn = self.connectSnowflake()
            # logger.info(f"Getting Column Names for table {table_name}, connection {conn}")
            cursor = conn.cursor()
            query = f"DESCRIBE TABLE {table_name}"
            cursor.execute(query)
            df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
            cursor.close()
            conn.close()
            return df

        except Exception as e:
            logger.error(f"get_table_columns Error: {e}")
            raise CustomException(success=False, message=f"Error: {e}", operation="getTableColumns")

# Example usage:
# connector_instance = Connector()
# df_example = pd.DataFrame({"column1": [1, 2, 3], "column2": ["a", "b", "c"]})
# table_name_example = "example_table"
# connector_instance.write_dataframe(df_example, table_name_example)
