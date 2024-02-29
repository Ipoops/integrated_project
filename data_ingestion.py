""""
The module contain functions to create sql engine, extract data from a database, logging information and also function to read csv file either from the internet or local system.

arg:
logger: logger object
db_path (str): path to bd file
sql_query (str): sql query to be run on the db
weather_data_URL (str): URL to weather_data_csv file on the web
weather_mapping_data_URL (str): URL to weather mappping data csv on the web
"""
from sqlalchemy import create_engine, text
import logging
import pandas as pd
# Name our logger so we know that logs from this module come from the data_ingestion module
logger = logging.getLogger('data_ingestion')
# Set a basic logging message up that prints out a timestamp, the name of our logger, and the message
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

"""db_path = 'sqlite:///Maji_Ndogo_farm_survey_small.db'

sql_query = """
"""SELECT *
FROM geographic_features
LEFT JOIN weather_features USING (Field_ID)
LEFT JOIN soil_and_crop_features USING (Field_ID)
LEFT JOIN farm_management_features USING (Field_ID)
"""

"""weather_data_URL = "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_station_data.csv"
weather_mapping_data_URL = "https://raw.githubusercontent.com/Explore-AI/Public-Data/master/Maji_Ndogo/Weather_data_field_mapping.csv"
"""

def create_db_engine(db_path):
    """
    This function is to create a sql lite database engine.
    
    Parameter:
    1. db_path (str): the path to the database which can be in the same folder or a different folder.
    
    output:
    engine (database object): The database object to interact with the database if no error,    
    """
    try:
        engine = create_engine(db_path)
        # Test connection
        with engine.connect() as conn:
            pass
        # test if the database engine was created successfully
        logger.info("Database engine created successfully.")
        return engine # Return the engine object if it all works well
    except ImportError: #If we get an ImportError, inform the user SQLAlchemy is not installed
        logger.error("SQLAlchemy is required to use this function. Please install it first.")
        raise e
    except Exception as e:# If we fail to create an engine inform the user
        logger.error(f"Failed to create database engine. Error: {e}")
        raise e
    
def query_data(engine, sql_query):
    """
    This function is to query a datbase and store the result in a Dataframe
    
    parameter
    engine, database object: this is the Database object to connect to the database
    sql_query, str: This is the query to be ran to pull the data from the database
    
    output
    df, DataFrame: The output is a dataframe with the data
    """
    try:
        with engine.connect() as connection:
            df = pd.read_sql_query(text(sql_query), connection)
        if df.empty:
            # Log a message or handle the empty DataFrame scenario as needed
            msg = "The query returned an empty DataFrame."
            logger.error(msg)
            raise ValueError(msg)
        logger.info("Query executed successfully.")
        return df
    except ValueError as e: 
        logger.error(f"SQL query failed. Error: {e}")
        raise e
    except Exception as e:
        logger.error(f"An error occurred while querying the database. Error: {e}")
        raise e
    
def read_from_web_CSV(URL):
    """
    This function is to read csv file from a url and save it in a dataframe.
    
    input
    URL, str: The URL or path to the CSV file either on the internet or a local computer
    
    output
    df, DataFrame: A pandas Dataframe object with the data from the CSV
    
    """
    try:
        df = pd.read_csv(URL)
        logger.info("CSV file read successfully from the web.")
        return df
    except pd.errors.EmptyDataError as e:
        logger.error("The URL does not point to a valid CSV file. Please check the URL and try again.")
        raise e
    except Exception as e:
        logger.error(f"Failed to read CSV from the web. Error: {e}")
        raise e
    
### END FUNCTION