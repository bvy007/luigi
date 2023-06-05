# importing libraries
import luigi
import os
import requests
import json
import logging
import time
import jsonschema
import pandas as pd
import mysql.connector
from mysql.connector import Error
from requests.exceptions import RequestException, ConnectionError, Timeout, HTTPError
from pydantic import BaseModel, ValidationError, validator

from yaml_config import project_arguments as parameters
from utils import process_data

# Set up logging configuration
logging.basicConfig(
    level=logging.DEBUG,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    filename='app.log',   # Specify the log file name
    filemode='w',         # Specify the file mode ('w' for write, 'a' for append)
    format='%(asctime)s - %(levelname)s - %(message)s'  # Specify the log message format
)

class FetchAPIData(luigi.Task):
    """
    This is a Luigi task that fetches data from API and save the output in a json file.
    """

    # create parameters for this task
    api_url = luigi.Parameter(default=parameters['arguments']['data_fetch_params']['url_api'])
    output_file_path = luigi.Parameter(parameters['arguments']['outputs']['raw_api_data_path'])
    
    def run(self):
        """
        Execution of this task's logic.
        """

        logging.info('Running task: %s and Task id: %s', self.__class__.__name__, self.task_id)

        # initialize parameters to retry for fetching API data 
        max_retries = 5
        delay = 1

        # loop to retry fetching API data and error handling 
        for attempt in range(max_retries + 1):
            try:
                response = requests.get(self.api_url)
                response.raise_for_status()
                data = response.json()

                # save the obtained data as json 
                if len(data) > 0:
                    with self.output().open('w') as f:
                        json.dump(data, f, indent=4)

                    logging.info('Fetching data was completed and it has %s objects.', str(len(data)))           
                    logging.info('Saved the json file from Task %s at path: %s', self.__class__.__name__, self.output_file_path)
                
                else:
                    logging.error("No data found")
                    raise luigi.TaskFailException("No data found")

                logging.info('Successfully executed the task %s in %s attempt.', self.__class__.__name__, str(attempt + 1))
                
                break
            
            # handling Timeout related error 
            except Timeout as e:
                if attempt < max_retries:
                    print(f"Timeout Error: {e}")
                    print(f"Retrying in {delay} seconds...")
                    logging.error('Caught Timeout Exception in the task %s in %s attempt', self.__class__.__name__, str(attempt + 1))
                    time.sleep(delay)
                else:
                    print(f"Maximum retries exceeded. Timeout Error: {e}")
                    logging.error('Maximum retries exceeded. Timeout Error: %s', e)

            # handling Connection related error
            except ConnectionError as e:
                if attempt < max_retries:
                    print(f"An error occurred: {e}\n")
                    print(f"Retrying in {delay} seconds...")
                    logging.error('Caught ConnectionError Exception in the task %s in %s attempt', self.__class__.__name__, str(attempt + 1))
                    time.sleep(delay)
                else:
                    print(f"Maximum retries exceeded. Connection Error: {e}")
                    logging.error('Maximum retries exceeded. ConnectionError Error: %s', e)

            # handling HTTP related error
            except HTTPError as e:
                if attempt < max_retries:
                    print(f"HTTP error occurred: {e}\n")
                    print(f"Retrying in {delay} seconds...")
                    logging.error('Caught HTTPError Exception in the task %s in %s attempt', self.__class__.__name__, str(attempt + 1))
                    time.sleep(delay)
                else:
                    print(f"Maximum retries exceeded. HTTPError Error: {e}")
                    logging.error('Maximum retries exceeded. HTTPError Error: %s', e)
            
            # handling RequestException related error
            except RequestException as e:
                if attempt < max_retries:
                    print(f"An error occurred: {e}\n")
                    print(f"Retrying in {delay} seconds...")
                    logging.error('Caught RequestException Exception in the task %s in %s attempt', self.__class__.__name__, str(attempt + 1))
                    time.sleep(delay)
                else:
                    print(f"Maximum retries exceeded. RequestException Error: {e}")
                    logging.error('Maximum retries exceeded. RequestException Error: %s', e)

    def output(self):
        """
        Defines the output(s) of the task.
        Returns:
            .json file with api data
        """
        # output as a raw json file
        return luigi.LocalTarget(self.output_file_path)
    

class CleaningAPIData(luigi.Task):
    """
    This is a Luigi task that does the following activities:
    1) Cleans the raw data obatined from API 
    2) Does validates the data using jsonschema
    3) finally saves the output in a json file.
    
    Ideas: Further validation techniques can also be applied on other datasets like : Date format normalization, Numeric value range 
    checks, Limiting the length of a string, Checking duplicate entries and empty strings, Validating email address, Check for unique 
    names or identifiers, Check for invalid characters, Checking for some substrings, ans so on.
    """

    # create parameters for this task
    output_file_path = luigi.Parameter(parameters['arguments']['outputs']['clean_data_path'])

    def requires(self):
        """
        Specifies the task dependencies.
        Returns:
            Luigi Task Object
        """
        return FetchAPIData()
    
    def validate_json(self, data, schema):
        """
        Validate the json objects using the jsonschema config (or definition)
        Returns:
            List of json objects
        """

        cleaned_data_json = []

        # Process the data as needed
        try:
            for record in data:
                jsonschema.validate(record, schema)
                cleaned_data_json.append(record)

        except jsonschema.exceptions.ValidationError as e:
            raise Exception(f"Data quality check failed: {e}")
        
        return cleaned_data_json

    def isEmptyOrNullFields(self, data):
        """
        Check for null or empty values in the required fields of jsonschema config (or definition)
        Returns:
            List of json objects
        """

        cleaned_data_json = []
        required_fields = parameters["arguments"]["schema_style"]["required"]

        # Check for null values in required fields for each object in json
        for record in data:
            try:
                if all(record.get(field) is not None and record.get(field) != '' for field in required_fields):
                    cleaned_data_json.append(record)
                else:
                    raise Exception('Data quality check failed: Null or empty value in required fields')
            
            except Exception as e:
                print(f"Found invalid record: {record} - {e}")
        
        return cleaned_data_json

    def run(self):
        """
        Execution of this task's logic.
        """

        logging.info('Running task: %s and Task id: %s', self.__class__.__name__, self.task_id)

        # reading the file from previous task
        with self.input().open('r') as f:
            data = json.load(f)

        # Process the data as needed
        processed_data = process_data(data)

        # get json schema definition
        schema = parameters["arguments"]["schema_style"]
        
        # Validate data using JSON schema
        cleaned_data_json = self.validate_json(data, schema)
        
        logging.info('JSON Validation completed and it has %s objects', str(len(cleaned_data_json)))  

        # Check for empty or none values for required or mandatory fields in json objects 
        cleaned_data_json = self.isEmptyOrNullFields(cleaned_data_json)
        
        logging.info('Check for mandatory(required) fields completed and it has %s objects', str(len(cleaned_data_json)))

        # save the cleaned dataset  
        if len(cleaned_data_json) > 0:
            with self.output().open('w') as f:
                json.dump(cleaned_data_json, f, indent=4)
            
            logging.info('Cleaning data was completed and it has %s objects.', str(len(cleaned_data_json)))
            logging.info('Saved the json file from Task %s at path: %s', self.__class__.__name__, self.output_file_path)
        
        else:
            logging.error("No data found as it did not match the criteria")
            raise luigi.TaskFailException("No data found")

    def output(self):
        """
        Defines the output(s) of the task.
        Returns:
            .json file with cleaned data
        """

        # output as a cleaned json file
        return luigi.LocalTarget(self.output_file_path)
    

class PostsDataModel(BaseModel):
    """
    Represents the posts posted by different users.
    """

    userId: int
    id: int
    title: str
    body: str

    @validator('id')
    def validate_id(cls, id):
        """
        Custom validator to check for emptiness in 'id' field 
        """
        if id is None or pd.isnull(id):
            raise ValueError('id must not be empty.')
        return id
    
    @validator('userId')
    def validate_userId(cls, userId):
        """
        Custom validator to check for emptiness in 'userId' field 
        """
        if id is None or pd.isnull(userId):
            raise ValueError('userId must not be empty.')
        return id

    @validator('title')
    def validate_title(cls, title):
        """
        Custom validator to check for emptiness in 'title' field 
        """
        if not title.isascii():
            raise ValueError('title must contain only alphabetic characters.')
        elif len(title) < 1:
            raise ValueError('title must have at least 1 characters.')
        
        return title
    
    @validator('body')
    def validate_body(cls, body):
        """
        Custom validator to check for emptiness in 'body' field 
        """
        if not body.isascii():
            raise ValueError('Body must contain only alphabetic characters.')
        elif len(body) < 1:
            raise ValueError('Body must have at least 1 characters.')

        return body
    

class TransformingAPIData(luigi.Task):
    """
    This is a Luigi task that does the following activities:
    1) Transform list of objects to a pandas dataframe 
    2) Does validates the data using pydantic data model
    3) finally saves the output in a CSV file.
    """

    # create parameters for this task
    output_file_path = luigi.Parameter(parameters['arguments']['outputs']['loading_df_path'])

    def requires(self):
        """
        Specifies the task dependencies.
        Returns:
            Luigi Task Object
        """
        return CleaningAPIData()
    
    def validate_dataframe(self, df):
        """
        Check if the given datafame is valid according to the validators defined in the pydantic data model
        Returns:
            True or False
        """
        # Validate the DataFrame
        for idx, row in df.iterrows():
            try:
                PostsDataModel.validate(row.to_dict())
            except ValidationError as e:
                print(f"Validation error: {str(e)}")
                return False
        return True

    def run(self):
        """
        Execution of this task's logic.
        """

        logging.info('Running task: %s and Task id: %s', self.__class__.__name__, self.task_id)

        # reading the file from previous task
        with self.input().open('r') as f:
            data = json.load(f)

        # create dataframe from json 
        data_df = pd.DataFrame(data)

        # Perform data validation
        is_valid = self.validate_dataframe(data_df)

        # save as CSV file if valid, else raise error
        if data_df.shape[0]==len(data):
            if is_valid:
                data_df.to_csv(self.output_file_path, index=False)

                logging.info('Transforming data from json to dataframe was completed and it has %s records.', str(data_df.shape[0]))
                logging.info('Saved the csv file from Task %s at path: %s', self.__class__.__name__, self.output_file_path)

            else:
                logging.error('Failed while validating dataframe in Task %s', self.__class__.__name__)
                raise ValueError("Failed while validating dataframe")
        else:
            logging.error('Mismatch in number of objects between JSON (%s objects) and Dataframe (%s records)', str(len(data)), str(data_df.shape[0]))
        

    def output(self):
        """
        Defines the output(s) of the task.
        Returns:
            .csv file with cleaned transformed data
        """

        # output as a csv file
        return luigi.LocalTarget(self.output_file_path)


class LoadingDatatoDB(luigi.Task):
    """
    This is a Luigi task that does the following activities:
    1) Connecting to a database
    2) Read the data from CSV and load into MySQL DB installed in local system
    """

    # create parameters for this task
    host = luigi.Parameter(parameters['arguments']['db_arguments']['host'])
    port = luigi.Parameter(parameters['arguments']['db_arguments']['port'])
    user = luigi.Parameter(parameters['arguments']['db_arguments']['user'])
    password = luigi.Parameter(parameters['arguments']['db_arguments']['password'])
    database = luigi.Parameter(parameters['arguments']['db_arguments']['database'])
    table = luigi.Parameter(parameters['arguments']['db_arguments']['table'])

    # create the schema for the incoming dataset
    db_schema = """
        (
            userId INT,
            id INT,
            title VARCHAR(250),
            body VARCHAR(250)
        )
    """
    
    def connect_to_database(self, host, port, user, password, database):
        """
        create a connection to database and perform error handling, raise an exception, or return None
        Returns:
            db connection
        """

        try:
            # Establish a connection to the database
            connection = mysql.connector.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database
            )

            print("Connected to the database!")
            return connection
        
        except mysql.connector.Error as error:
            print("Error connecting to the database: {}".format(error))
            

    def requires(self):
        """
        Specifies the task dependencies.
        Returns:
            Luigi Task Object
        """
        return TransformingAPIData()

    def run(self):
        """
        Execution of this task's logic.
        """

        logging.info('Running task: %s and Task id: %s', self.__class__.__name__, self.task_id)

        # reading the file from previous task
        with self.input().open('r') as input_file:
            data = pd.read_csv(input_file)

        # Perform loading data to database, error handling, raise an exception
        try:
            #connect to database
            sql_conn = self.connect_to_database(self.host, self.port, self.user, self.password, self.database) 

            if sql_conn.is_connected():
                cursor = sql_conn.cursor()

                # Create the table if it doesn't exist
                create_table_query = f"CREATE TABLE IF NOT EXISTS {self.table} {self.db_schema}"  # Define the table schema
                cursor.execute(create_table_query)

                count = 0

                # Dump the DataFrame into the database table
                for idx, row in data.iterrows():
                    insert_query = f"INSERT INTO {self.table} VALUES {tuple(row)}"
                    cursor.execute(insert_query)
                    count += 1

                sql_conn.commit()
                logging.info("Data loaded into the database: %s and table: %s successfully.", self.database, self.table)
                logging.info("Loaded successfully %s records into the table: %s ", str(count), self.table)

        except Error as e:
            print(f"Error connecting to the database: {e}")
            logging.error("Error connecting to the database: %s", e)

        finally:
            if sql_conn.is_connected():
                cursor.close()
                sql_conn.close()


if __name__ == '__main__':

    # Create a logger
    logger = logging.getLogger()

    # get directory for saving outputs 
    output_dir = parameters['arguments']['outputs']['output_dir']

    # create a output folder if doen't exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        logger.info("Directory created: %s", output_dir)
    else:
        logger.info("Directory already exists: %s", output_dir)

    # initiating the pipeline
    luigi.build(
        [
            LoadingDatatoDB(), 
            TransformingAPIData(), 
            CleaningAPIData(), 
            FetchAPIData()
        ], 
        local_scheduler=True, 
        detailed_summary=True
    )
