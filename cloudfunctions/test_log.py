from snowflake.Connector import Connector as Snowflake_connector
snowflake = Snowflake_connector()
snowflake.writeLog( success=True, message = "Testing log", table_name = "test", operation_name = "log_test")
# print(snowflake.getMetaData())

