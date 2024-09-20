# SNOWFLAKE MARKETO PYTHON CONNECTOR

This project is a Python-based system that integrates with Marketo and Snowflake using AWS Lambda, Serverless Framework, and NodeJS, with a focus on data retrieval, processing, and storage. The system utilizes threads for parallel execution, providing benefits such as increased throughput, improved responsiveness, efficient resource utilization, concurrency, and asynchronous operations.

# Technologies Used

```plaintext

   -  Python
   -  Snowflake
   -  Marketo
   -  AWS - Lambda, S3
   -  Serverless Computing
   -  NodeJS
   -  NPM
   -  Docker

```

# Project Structure


```plaintext

  ├── cloudfunctions
  │   ├── data (SELFMADE)
  |   |    ├── backups
  |   |    ├── tables
  |   |    ├── views
  |   |
  │   ├── logger
  │   ├── Marketo Connector 
  │   ├── Snowflake Connector  
  |   |__ Scripts files
  |
  ├── handler.py
  ├── services.py
  ├── readme.md
  |__ requirements.txt

```
- **`handler.py`**: The entry point to run the System. Functions are executed in threads parallely. Use command "python handler.py" to run the system.
- **`services.py`**: The List of Functions which you want to handle ( run ). Export "functions" array is list of functions which will be run and managed by Handler.py 
- **`cloudfunctions/`**: This Directory contains custom python packages and scripts. More functionality can be added here as per requirements.
- **`cloudfunctions/marketo`**: This Directory contains custom python packages and scripts speacially dedicated to connect Marketo Bulk/Rest APIs for several Entities. More functionality can be added here as per requirements. Refer to https://developers.marketo.com/rest-api/endpoint-reference/
- **`cloudfunctions/snowflake`**: This Directory contains custom python packages and scripts speacially dedicated to connect Snowflake for CRUD operations. More functionality can be added here as per requirements
Packages used are :
      snowflake-connector-python==3.4.1
      snowflake-snowpark-python==1.10.0
      pyarrow==10.0.1
Refer to https://docs.snowflake.com/en/developer
- **`node_modules/`**: Node.js modules, including Serverless Framework and plugins. If using serverless Deployments.
- **`tests/`**: Test scripts or data.
- **`some_file_to_exclude.txt`**: File excluded during deployment.
- **`serverless.yml`**: The Serverless Framework configuration file specifying AWS Lambda functions, settings, and deployment details.


## Packages Used:

```plaintext

asyncio==3.4.3
chardet==5.2.0
openpyxl==3.1.2
  et-xmlfile==1.1.0
pandas==2.0.3
  numpy==1.26.1
  python-dateutil==2.8.2
    six==1.16.0
  pytz==2023.3.post1
  tzdata==2023.3
pip==23.0.1
pipdeptree==2.13.1
pipreqs==0.4.13
  docopt==0.6.2
  yarg==0.1.9
    requests==2.31.0
      certifi==2023.7.22
      charset-normalizer==3.3.2
      idna==3.4
      urllib3==1.26.18
pyarrow==10.0.1
  numpy==1.26.1
python-dotenv==1.0.0
scikit-learn==1.3.2
  joblib==1.3.2
  numpy==1.26.1
  scipy==1.11.4
    numpy==1.26.1
  threadpoolctl==3.2.0
snowflake-snowpark-python==1.10.0
  cloudpickle==2.0.0
  PyYAML==6.0.1
  setuptools==65.5.0
  snowflake-connector-python==3.4.1
    asn1crypto==1.5.1
    certifi==2023.7.22
    cffi==1.16.0
      pycparser==2.21
    charset-normalizer==3.3.2
    cryptography==41.0.5
      cffi==1.16.0
        pycparser==2.21
    filelock==3.13.1
    idna==3.4
    packaging==23.2
    platformdirs==3.11.0
    PyJWT==2.8.0
    pyOpenSSL==23.3.0
      cryptography==41.0.5
        cffi==1.16.0
          pycparser==2.21
    pytz==2023.3.post1
    requests==2.31.0
      certifi==2023.7.22
      charset-normalizer==3.3.2
      idna==3.4
      urllib3==1.26.18
    sortedcontainers==2.4.0
    tomlkit==0.12.2
    typing_extensions==4.8.0
    urllib3==1.26.18
  typing_extensions==4.8.0
  wheel==0.41.3

```  

# Cost and Billing Estimation of this solution

Cost depend on the platform you want to deploy. For AWS, Please calculate cost as per resources you are using. Refer to https://calculator.aws/

```plaintext
Weekly Estimation of Running system on AWS Lambda with it's limitations

Running Cost on AWS Lambda =  4 * (256 / 1024) * (6000 / 1000) * $0.00001667 ~ $0
S3 Package Size = 78 MB ~ $0.00005
Total Cost = $0.00006667 ~ $0 (Under AWS Always free-tier).

Limitations: 
1. Function can only run for upto 15 minutes per invocation.
2. Bundle Size can't exceed 270MB

```

## AWS Configuration

### Provider

- **Name**: AWS
- **Runtime**: Python 3.10
- **Region**: us-west-2

## AWS Lambda Functions

### Function: ams_MarketoSnowflakeService

- **Handler**: handler.handler
- **Memory Size**: 256 MB
- **Timeout**: 6000 seconds
- **Event**: Run at 9:00 AM every Wednesday. (Confugrable)

## Deployment Configuration

### Package Settings

- **Individually**: True (Each function packaged separately)
- **Exclude**:
  - node_modules/**
  - tests/**
  - data/**
  - cloudfunctions/data**
  - documentation.md

## Deployment

1. Install dependencies: `yarn install`.
2. Deploy: `serverless deploy`.

OR 

1. Use Cloud9 and Import .Zip File


# Marketo Connector Module

The `Connector` class is a Python implementation for interacting with the Marketo API to retrieve data related to emails, campaigns, smart campaigns, activities, programs, and leads.

### Features

1. **getEmailsData(initial_count):**
   - Retrieves email data from Marketo.
   - Parameters:
     - `initial_count`: Initial count for pagination.

2. **getSmartCampaignsData(initial_count):**
   - Retrieves data for smart campaigns from Marketo.
   - Parameters:
     - `initial_count`: Initial count for pagination.

3. **getActivitiesData(next_page_token):**
   - Retrieves activity data from Marketo.
   - Parameters:
     - `next_page_token`: Next page token for pagination.
     - `since_datetime`: Timestamp indicating the starting date and time for retrieving activities.

4. **getProgramsData(initial_count):**
   - Retrieves program data from Marketo.
   - Parameters:
     - `initial_count`: Initial count for pagination.

5. **getLeadsDataById(fields, id):**
   - Retrieves lead data by ID from Marketo.
   - Parameters:
     - `fields`: List of fields to retrieve.
     - `id`: Lead ID.

6. **getLeadsData(start_date):**
   - Retrieves lead data based on a specified date range.
   - Parameters:
     - `start_date`: Start date for filtering leads.

### Dependencies

- Pandas: Used for handling data in DataFrame format.
- Request: A custom module for making HTTP requests to the Marketo API.

# Marketo Request Module

The `Request` module is a Python implementation for making authenticated requests to the Marketo API. It includes methods for handling authentication, GET, and POST requests.

### Functions

1. **Authentication:**
   - Handles authentication using the client credentials flow.
   - Retrieves an access token for authorization.

2. **GET Request:**
   - Sends authenticated GET requests to the Marketo API.
   - Handles token refresh on authentication failure.
   - Parameters:
     - `url`: API endpoint URL.
     - `params`: Optional query parameters.
     - `retry`: Number of retry attempts in case of authentication failure.

3. **POST Request:**
   - Sends authenticated POST requests to the Marketo API.
   - Handles token refresh on authentication failure.
   - Parameters:
     - `url`: API endpoint URL.
     - `data`: Request payload in JSON format.
     - `retry`: Number of retry attempts in case of authentication failure.

### Dependency

- `requests`: Used for making HTTP requests.
- `dotenv`: Used for loading environment variables from - `.env` file.

# Snowflake Connector Module

The `Connector` module is a Python implementation for interacting with Snowflake, a cloud-based data warehousing platform. It includes methods for establishing a connection, writing DataFrame data to Snowflake tables, writing logs, and retrieving metadata.

### Features

1. **Connection:**
   - Establishes a connection to Snowflake using provided credentials.
   - Parameters:
     - `user`: Snowflake username.
     - `password`: Snowflake password.
     - `account`: Snowflake account URL.
     - `warehouse`: Snowflake warehouse.
     - `database`: Snowflake database.
     - `schema`: Snowflake schema.

2. **Write DataFrame Data:**
   - Writes data from a DataFrame to a specified Snowflake table.
   - Parameters:
     - `df`: DataFrame containing the data.
     - `table_name`: Name of the Snowflake table to write the data.

3. **Write Log Entry:**
   - Writes a log entry to the `MARKETO_AWS_LOGS` table.
   - Parameters:
     - `message`: Log message.
     - `table_name`: Name of the table related to the log entry.
     - `operation_name`: Name of the operation related to the log entry.
     - `success`: Boolean indicating the success status of the operation.

4. **Get Metadata:**
   - Retrieves metadata from the `MARKETO_META_DATA_VIEW` view.

5. **Get Missing Leads:**
   - Retrieves distinct lead IDs from the `MARKETODATAMART` table where `id_ML` is NULL.

6. **Get Table Columns:**
   - Retrieves column names and details for a specified Snowflake table.

## Dependencies

- `pandas`: Used for handling data in DataFrame format.
- `snowflake-connector-python`: Snowflake connector for Python.
- `dotenv`: Used for loading environment variables from a `.env` file.

# Error Handling

### CustomException

The `CustomException` class is a custom exception designed for use in the Marketo Connector Python implementation. It is raised to handle specific error scenarios and provides attributes for indicating the success status, error message, operation name, and table name.

### Class Definition

```python
class CustomException(Exception):
    def __init__(self, success, message, operation, table_name):
        """
        Initializes a CustomException instance.

        Parameters:
        - `success`: Boolean, indicating the success status of the operation.
        - `message`: A descriptive message providing details about the exception.
        - `operation`: The name of the operation during which the exception occurred.
        - `table_name`: The name of the table or class related to the exception.

        Example:
        ```python
        raise CustomException(success=False, message="Error message", operation="getData", table_name="MARKETO_CLASS")
        ```
        """
        self.success = success
        self.message = message
        self.operation = operation
        self.table_name = table_name
        super().__init__(message)
        """
```

# Optimisations

In the provided Lambda function handler script, using threads provides several benefits for concurrent execution. Here's an explanation of the advantages:

## 1. Parallelism:
   - **Increased Throughput:** Threads enable parallel execution of Lambda functions, leading to increased throughput by leveraging the processing capabilities of multi-core systems.

## 2. Responsiveness:
   - **Improved Responsiveness:** By using threads, the main program can remain responsive even when executing time-consuming Lambda functions. This is crucial for applications that require interaction while performing background tasks.

## 3. Resource Sharing:
   - **Efficient Resource Utilization:** Threads share the same memory space, allowing efficient communication and data sharing between Lambda functions. This enhances resource utilization and reduces the need for duplicated data structures.

## 4. Concurrency:
   - **Concurrent Execution:** Threads support concurrent execution of tasks, allowing the Lambda functions to proceed independently. This is beneficial for applications with multiple, independent operations.

## 5. Asynchronous Operations:
   - **Asynchronous Processing:** Threads facilitate asynchronous execution, allowing the Lambda functions to operate independently without waiting for each other. This contributes to more efficient resource usage.

