import logging
import time
import functools
import traceback
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger('db_operations')
handler = logging.FileHandler('data/db_operations.log')
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

def log_db_operation(func):
    """Decorator to log database operations"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        operation = func.__name__
        start_time = time.time()
        logger.info(f"Starting {operation} - args: {args[1:]} kwargs: {kwargs}")
        
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"Completed {operation} in {duration:.2f}s")
            return result
        except SQLAlchemyError as e:
            duration = time.time() - start_time
            logger.error(f"Database error in {operation} after {duration:.2f}s: {str(e)}")
            logger.error(traceback.format_exc())
            raise
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error in {operation} after {duration:.2f}s: {str(e)}")
            logger.error(traceback.format_exc())
            raise
            
    return wrapper