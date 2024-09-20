from services import functions
from cloudfunctions.snowflake.Connector import Connector as SnowflakeConnector
import logging
import asyncio
import time
import concurrent.futures
from cloudfunctions.logger.logger import CustomException


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def handler():
    snowflake = SnowflakeConnector()
    funcs = functions
    # ThreadPoolExecutor for optimisation
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {executor.submit(func): func for func in funcs}
        for future in concurrent.futures.as_completed(futures):
            func = futures[future]
            try:
                result = future.result()  
                message = f"Lambda function {func.__name__} executed SuccessFully " + str(result)
                logger.info(message)
                loop = asyncio.get_event_loop()
                loop.run_until_complete(snowflake.writeLog(message, 'LAMBDA', func.__name__, success=True))

            except CustomException as ce:
                logger.error(f"Lambda function {func.__name__} raised a CustomException: {ce.message}")
                loop = asyncio.get_event_loop()
                loop.run_until_complete(snowflake.writeLog(ce.message, ce.table_name, ce.operation, success=False))        

            except Exception as e:
                logger.error(f"Lambda function {func.__name__} raised an exception: {e}")
                loop = asyncio.get_event_loop()
                loop.run_until_complete(snowflake.writeLog(str(e), 'LAMBDA', func.__name__, success=False))
    
    print("COOLING DOWN...")
    time.sleep(20)
    handler()
    logger.info("Lambda functions executed SuccessFully")
    asyncio.run(snowflake.writeLog("Lambda functions executed SuccessFully", 'LAMBDA', 'handler', success=True))
    return True

handler()

