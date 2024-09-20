'''
create or replace TABLE env[SNOWFLAKE_DATABASE].env[SNOWFLAKE_SCHEMA].MARKETO_AWS_LOGS (
"id" NUMBER(38,0) NOT NULL autoincrement start 1 increment 1 order,
"message" VARCHAR(16777216) NOT NULL,
"table_name" VARCHAR(255) NOT NULL,
"operation_name" VARCHAR(255) NOT NULL,
"success" BOOLEAN,
"timestamp" TIMESTAMP_LTZ(9) DEFAULT CURRENT_TIMESTAMP(),
primary key ("id")
);

check writeLog() in ./snowflake

'''

class CustomException(Exception):
    def __init__(self, success, message, operation, ):
        self.success = success
        self.message = message
        self.operation = operation
    
        super().__init__(message)



#USAGE
# Raise a custom exception with "success" and "message" attributes
# try:
#     raise CustomException(success=False, message="Something went wrong.")
# except CustomException as ce:
#     print(f"Caught a custom exception - Success: {ce.success}, Message: {ce.message}")
