import os
import holidays
import math as m
import numpy as np
import pandas as pd
import pytz
from astral import LocationInfo
from astral.sun import sun
from bson.son import SON
from datetime import datetime
from pymongo.database import Database
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from typing import List

# Define constants
STATE_TIMEZONES = {
    'NSW': 'Australia/Sydney',
    'QLD': 'Australia/Brisbane',
    'SA': 'Australia/Adelaide',
    'VIC': 'Australia/Melbourne',
}

# Define outputs columns
OUTPUT_COLUMNS = [
    'datetime',
    'state',
    'demand',
    'temperature',
    'year',
    'month',
    'day_of_month',
    'day_of_week',
    'period_of_day',
    'is_weekday',
    'is_public_holiday',
    'is_daylight'
]


class FeatureEngineering:
    def __init__(
        self,
        url: str,
        db_name: str,
        demand_collection_name: str,
        temperature_collection_name: str,
        target_collection_name: str
    ) -> None:
        self.client = self.mongo_client(url)
        self.db = self.mongo_database(db_name)
        self.demand_collection_name = demand_collection_name
        self.temperature_collection_name = temperature_collection_name
        self.target_collection_name = target_collection_name
        
    def mongo_client(self, url: str) -> MongoClient:
        """Establishes a connection to a MongoDB client

        Args:
            url (str): The connection URL for the MongoDB client

        Returns:
            MongoClient (MongoClient): The MongoDB client object
        """
        client = MongoClient(url, server_api=ServerApi(version='1'))
        print(f"Connected to MongoDB client: {url}")
        return client
    
    def mongo_database(self, db_name:str) -> Database:
        """Connects to a MongoDB database

        Args:
            db_name (str): The name of the database to connect to

        Returns:
            Database (Database): The MongoDB database object
        """
        db = self.client[db_name]
        print(f"Connected to MongoDB database: {db_name}")
        return db
    
    def close_connection(self) -> None:
        """Closes the connection to the MongoDB client"""
        self.client.close()
    
    def drop_collection(self, collection_name:str) -> None:
        """Drops a collection from the database"""
        self.db[collection_name].drop()
        print(f"Dropped collection: {collection_name}")
    
    def check_collection_exists(self, collection_name:str) -> bool:
        """Checks if a collection exists in the database

        Args:
            collection_name (str): The name of the collection to check

        Returns:
            bool: Indicates if the collection exists
        """
        return collection_name in self.db.list_collection_names()

    def is_public_holiday(self, date: datetime, state: str) -> bool:
        """Determines if a given date is a public holiday in a given state

        Args:
            date (datetime): The date to check
            state (str): The Australian state to check

        Returns:
            bool: Indicates if the date is a public holiday
        """
        au_holidays = holidays.AU(prov=state)
        return date in au_holidays

    def is_daylight(self, utc_datetime: datetime, state: str) -> bool:
        """Determines if a given datetime is during daylight hours in a given Australian state

        Args:
            utc_datetime (datetime): The datetime to check
            state (str): The Australian state to check

        Returns:
            bool: Indicates if the datetime is during daylight hours
        """
        # Convert UTC datetime to local datetime
        local_timezone = pytz.timezone(STATE_TIMEZONES[state])
        local_datetime = utc_datetime.replace(tzinfo=pytz.utc).astimezone(local_timezone)

        city_info = LocationInfo(timezone=STATE_TIMEZONES[state])
        s = sun(city_info.observer, date=local_datetime, tzinfo=local_timezone)
        
        return s['sunrise'] < local_datetime < s['sunset']

    def get_feature_batches(self) -> List:
        """Returns a list of unique state, year, and month combinations to batch process the feature engineering

        Returns:
            List: A list of unique state, year, and month combinations
        """
        # Define the pipeline to get unique state and year combinations
        pipeline = [
            {
                "$group": {
                    "_id": {
                        "state": "$state",
                        "year": {"$year": "$DATETIME"},
                        "month": {"$month": "$DATETIME"}
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "state": "$_id.state",
                    "year": "$_id.year",
                    "month": "$_id.month"
                }
            },
            {
                "$sort": SON([("state", 1), ("year", 1), ("month", 1)])  # Sorting by state then year
            }
        ]
        
        # Execute the pipeline and return the results
        return list(self.db[self.demand_collection_name].aggregate(pipeline))

    def run(self):
        """Runs the feature engineering pipeline

        Raises:
            e: A generic exception if an error occurs
        """
        try:
            # Get the feature batches
            feature_batches = self.get_feature_batches()
            
            # Drop the collection so it can be re-created
            self.drop_collection(self.target_collection_name)

            # Iterate through the feature batches
            iteration = 1
            for doc in feature_batches:
                print(f"Processing iteration {iteration} / {len(feature_batches)} for: {doc['state']}, {doc['year']}, {doc['month']}")
                state, year, month = doc['state'], doc['year'], doc['month']
                
                # Defines a pipeline used to filter the data
                pipeline = [
                    {"$addFields": {
                        "year": {"$year": "$DATETIME"},
                        "month": {"$month": "$DATETIME"}
                    }},
                    {"$match": {
                        "state": state,
                        "year": year,
                        "month": month
                    }}
                ]
                
                # Convert the data to a DataFrame
                demand_data = pd.DataFrame(list(self.db[self.demand_collection_name].aggregate(pipeline)))
                temperature_data = pd.DataFrame(list(self.db[self.temperature_collection_name].aggregate(pipeline)))
                
                # Handles the coversion of datetime for demand data
                if 'DATETIME' in demand_data.columns:
                    demand_data['datetime'] = pd.to_datetime(demand_data['DATETIME'])
                    demand_data.drop(columns=['DATETIME'], inplace=True)
                else:
                    demand_data['datetime'] = pd.NaT  # Assign NaT where 'DATETIME' does not exist

                # Handles the coversion of datetime for temperature data
                if 'DATETIME' in temperature_data.columns:
                    temperature_data['datetime'] = pd.to_datetime(temperature_data['DATETIME'])
                    temperature_data.drop(columns=['DATETIME'], inplace=True)
                else:
                    temperature_data['datetime'] = pd.NaT  # Assign NaT for missing 'DATETIME'

                # Ensuring 'state' is present in temperature_data so that the merge can be performed
                if 'state' not in temperature_data.columns:
                    temperature_data['state'] = state

                # Merge the data
                combined_data = pd.merge(demand_data, temperature_data, on=['datetime', 'state'], how='outer')
                
                # Rename columns
                combined_data.rename(columns={'TOTALDEMAND': 'demand', 'TEMPERATURE': 'temperature'}, inplace=True)
                
                # Impute missing values for demand and temperature using MICE
                if "temperature" not in combined_data.columns:
                    combined_data["temperature"] = np.nan

                # Only proceed with MICE imputation if both columns are present and not entirely NaN
                if not combined_data[['demand', 'temperature']].isna().all().any():
                    imputer = IterativeImputer(max_iter=10, random_state=0)
                    imputed_values = imputer.fit_transform(combined_data[['demand', 'temperature']])
                    combined_data[['demand', 'temperature']] = imputed_values

                # Additional feature engineering and transformation
                combined_data['year'] = combined_data['datetime'].dt.year
                combined_data['month'] = combined_data['datetime'].dt.month
                combined_data['day_of_month'] = combined_data['datetime'].dt.day
                combined_data['day_of_week'] = combined_data['datetime'].dt.dayofweek
                combined_data['is_weekday'] = combined_data['day_of_week'] < 5
                combined_data['period_of_day'] = combined_data['datetime'].dt.hour * 2 + combined_data['datetime'].dt.minute // 30 + 1
                combined_data['is_public_holiday'] = combined_data['datetime'].apply(lambda x: self.is_public_holiday(x, state))
                combined_data['is_daylight'] = combined_data['datetime'].apply(lambda x: self.is_daylight(x, state))

                # Reduce to output columns
                features = combined_data[OUTPUT_COLUMNS]

                # Insert processed data into MongoDB
                feature_docs = features.to_dict('records')
                self.db[self.target_collection_name].insert_many(feature_docs)

                iteration += 1

        except Exception as e:
            print(f"An error occurred: {e}")
            raise e
        
        finally:
            self.close_connection()
            print("Connection to Mongo client closed successfully")

if __name__ == "__main__":
    # Define constants
    user = os.getenv('MONGO_USER')
    password = os.getenv('MONGO_PASSWORD')
    url = f"mongodb+srv://{user}:{password}@project-data.cfluj8d.mongodb.net/?retryWrites=true&w=majority&appName=project-data"
    db_name = 'data'
    demand_collection_name = 'total_demand'
    temperature_collection_name = 'temperature'
    target_collection_name = 'features'
    
    # Instantiate the class
    feature_engineering = FeatureEngineering(
        url,
        db_name,
        demand_collection_name,
        temperature_collection_name,
        target_collection_name
    )
    
    # Execute the pipeline
    feature_engineering.run()
    print("Feature Engineering script executed successfully")

