import os
import sys
from src.exception import CustomException
from src.logger import logging
import pandas as pd
from pymongo import MongoClient
from dataclasses import dataclass
from sklearn.model_selection import train_test_split
print("hello world")
@dataclass
class DataIngestionConfig:
    train_data_path: str = os.path.join('data', "train.csv")
    test_data_path: str = os.path.join('data', "test.csv")
    raw_data_path: str = os.path.join('data', "raw.csv")

class DataIngestion:
    """
    Class for ingesting the cancer mortality data from MongoDB database.
    """
    def __init__(self):
        """
        Initialize the class with the DataIngestionConfig.
        """
        self.ingestion_config = DataIngestionConfig()
    
    def connect_to_mongo(self, connection_string, database_name, collection_name):
        """
        Connect to MongoDB and return the collection object.
        :param connection_string: Connection string for Mongodb connection
        :param database_name: String of database name
        :param collection_name: String of collection name
        :return: A Mongodb Collection Object
        """
        try:
            logging.info("Connecting to MongoDB")
            client = MongoClient(connection_string)
            db = client[database_name]
            collection = db[collection_name]
            logging.info("Connected to MongoDB")
            return collection
        except Exception as e:
            raise CustomException(f"Error connecting to MongoDB: {e}", sys)

    def fetch_data_from_mongo(self, collection):
        """
        Fetch data from MongoDB collection and return as a pandas DataFrame
        :param connection: Mongodb Collection Object
        :return: A pandas dataframe
        """
        try:
            logging.info("Fetching data from MongoDB")
            data = list(collection.find())
            df = pd.DataFrame(data).drop(columns=["_id"], errors="ignore")
            logging.info("Data fetched successfully")
            return df
        except Exception as e:
            raise CustomException(f"Error fetching data from MongoDB: {e}", sys)

    def initiate_data_ingestion(self):
        """
        Main method to handle data ingestion: fetch data, save raw, train, and test splits.
        :return: Train Test Split Configaration
        """
        connection_string = "mongodb://localhost:27017/"
        database_name = "students"  
        collection_name = "performance"

        logging.info("Initiating data ingestion process")
        try:
            # Step 1: Connect to MongoDB
            collection = self.connect_to_mongo(connection_string, database_name, collection_name)

            # Step 2: Fetch data from MongoDB
            df = self.fetch_data_from_mongo(collection)

            # Step 3: Save raw data
            os.makedirs(os.path.dirname(self.ingestion_config.train_data_path), exist_ok=True)
            df.to_csv(self.ingestion_config.raw_data_path, index=False, header=True)

            logging.info("Train-test split initiated")
            train_set, test_set = train_test_split(df, test_size=0.2, random_state=42)

            # Step 4: Save train and test datasets
            train_set.to_csv(self.ingestion_config.train_data_path, index=False, header=True)
            test_set.to_csv(self.ingestion_config.test_data_path, index=False, header=True)

            logging.info("Ingestion of the data is completed")
            return (
                self.ingestion_config.train_data_path,
                self.ingestion_config.test_data_path
            )
        except Exception as e:
            raise CustomException(e, sys)

if __name__ == "__main__":
    obj = DataIngestion()
    obj.initiate_data_ingestion()
    