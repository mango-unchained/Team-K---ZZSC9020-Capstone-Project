"""
This script performs feature engineering on the demand and temperature data from MongoDB.
"""

import os
from datetime import datetime
import math as m
import holidays
import pandas as pd
import pytz
from astral import LocationInfo
from astral.sun import sun
from pymongo.database import Database
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from sklearn.experimental import enable_iterative_imputer # noqa
from sklearn.impute import IterativeImputer

# Define constants
STATE_TIMEZONES = {
    'NSW': 'Australia/Sydney',
    'QLD': 'Australia/Brisbane',
    'SA': 'Australia/Adelaide',
    'VIC': 'Australia/Melbourne',
}


class FeatureEngineering:
    """The FeatureEngineering class performs feature engineering on demand and temperature data
    """
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
    
    def read_mongo_data(self, collection_name: str) -> pd.DataFrame:
        """Reads data from a MongoDB collection into a pandas DataFrame

        Args:
            collection_name (str): The name of the collection to read from

        Returns:
            pd.DataFrame: The data from the collection as a DataFrame
        """
        # Query all documents in the collection
        data = self.db[collection_name].find()

        # Convert to pandas DataFrame
        df = pd.DataFrame(list(data))

        # Optional: If you don't want the MongoDB '_id' in your DataFrame
        df.drop('_id', axis=1, inplace=True)

        return df

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
    
    def impute_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """Imputes missing values in a DataFrame using an iterative imputer

        Args:
            df (pd.DataFrame): The DataFrame to impute missing values in

        Returns:
            pd.DataFrame: The DataFrame with imputed missing values
        """
        imputer_input = df.drop(['state', 'DATETIME'], axis=1)

        # Initialize the imputer
        imputer = IterativeImputer(max_iter=10, random_state=0)

        # Impute the missing values
        df_imputed = pd.DataFrame(imputer.fit_transform(imputer_input), columns=imputer_input.columns)
        
        # Add the 'state' and 'DATETIME' columns back to the DataFrame
        df_imputed['state'] = df['state']
        df_imputed['DATETIME'] = df['DATETIME']

        return df_imputed
    
    def transform_periodic_values(self, value: int, period: int) -> float:
        """Transforms a periodic value using a sine function

        Args:
            value (int): The value to transform
            period (int): The period of the sine function

        Returns:
            float: The transformed value
        """
        return m.sin(2 * m.pi * value / period)
    
    def add_lagged_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds lagged features for specified columns in the DataFrame for 1 hour ahead and 24 hours ahead.

        Args:
            df (pd.DataFrame): The DataFrame to add lagged features to.

        Returns:
            pd.DataFrame: The DataFrame with added lagged features.
        """
        # Make sure df is sorted by 'state' and 'DATETIME' to ensure logical shifts
        df = df.sort_values(by=['state', 'DATETIME'])
        
        # Columns to create lagged versions of
        lag_columns = ['year', 'month', 'day_of_month', 'day_of_week', 'TOTALDEMAND']

        # Create lagged columns for 1 hour ahead and 24 hours ahead
        # for hours in [1, 24]:
        #     hours_ahead = df.copy()[['DATETIME', *lag_columns]]
        #     hours_ahead.columns = ['DATETIME', *[f'h{hours}_{col}' for col in lag_columns]]
        #     hours_ahead.DATETIME -= timedelta(hours=hours)
        #     df = pd.merge(df, ahead, how='left', on=['DATETIME', 'state'])
        
        for col in lag_columns:
            df[f'h1_{col}'] = df.groupby('state')[col].shift(-2)  # 1 hour ahead
            df[f'h24_{col}'] = df.groupby('state')[col].shift(-48)  # 24 hours ahead
        
        return df
    
    def add_shifted_demand_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Adds shifted demand features to the DataFrame for every 30 minutes up to 24 hours more efficiently.

        Args:
            df (pd.DataFrame): The DataFrame to add shifted features to.

        Returns:
            pd.DataFrame: The DataFrame with added shifted features.
        """
        # Ensure DataFrame is sorted by 'DATETIME'
        df.sort_values('DATETIME', inplace=True)

        # Calculate the shifts for every 30 minutes up to 24 hours (48 shifts)
        for i in range(1, 49):
            shift_minutes = 30 * i
            # Shift 'TOTALDEMAND' directly without creating multiple intermediate DataFrames
            df[f'TM{shift_minutes}'] = df.groupby('state')['TOTALDEMAND'].shift(-i)

        return df

    def run(self):
        """Runs the feature engineering pipeline
        """
        try:
            # Read the demand data from MongoDB
            demand_data = self.read_mongo_data(self.demand_collection_name)
            print("Successfully read demand data from MongoDB")
            
            # Read the temperature data from MongoDB
            temperature_data = self.read_mongo_data(self.temperature_collection_name)
            print("Successfully read temperature data from MongoDB")
            
            # Drop the location column from the temperature data
            temperature_data.drop('LOCATION', axis=1, inplace=True)
            
            # Left join the demand and temperature data on the 'DATETIME' and state columns
            df = pd.merge(demand_data, temperature_data, on=['state', 'DATETIME'], how='left')
            
            # Fill in the missing temperature and demand values using an iterative imputer
            df = self.impute_missing_values(df)
            
            # Convert the 'DATETIME' column to a datetime object
            df['DATETIME'] = pd.to_datetime(df['DATETIME'])
            
            # Add additional features to the DataFrame
            df['year'] = df['DATETIME'].dt.year
            df['month'] = df['DATETIME'].dt.month.apply(lambda x: self.transform_periodic_values(x, 12))
            df['day_of_month'] = df['DATETIME'].dt.day
            df['day_of_week'] = df['DATETIME'].dt.dayofweek.apply(lambda x: self.transform_periodic_values(x, 7))
            df['is_weekday'] = df['day_of_week'] < 5
            df['period_of_day'] = df['DATETIME'].apply(lambda x: m.sin(2 * m.pi * ((x.hour * 2) + (x.minute // 30)) / 48))
            df['is_public_holiday'] = df.apply(lambda x: self.is_public_holiday(x['DATETIME'], x['state']), axis=1)
            df['is_daylight'] = df.apply(lambda x: self.is_daylight(x['DATETIME'], x['state']), axis=1)
            
            # Add lagged features to the DataFrame
            df = self.add_lagged_features(df)
            
            # Add shifted demand features to the DataFrame
            df = self.add_shifted_demand_features(df)
            
            # Remove any rows with missing values
            df.dropna(inplace=True)
            
            # If the target collection already exists, drop it
            if self.check_collection_exists(self.target_collection_name):
                self.drop_collection(self.target_collection_name)
            
            # Write the result to a new collection in MongoDB
            self.db[self.target_collection_name].insert_many(df.to_dict(orient='records'))
            print("Successfully wrote the transformed data to MongoDB")
            
        except Exception as e:
            print(f"An error occurred: {e}")
            
        finally:
            # Close the connection to the MongoDB client
            self.close_connection()


if __name__ == "__main__":
    # Define constants
    user = os.getenv('MONGO_USER')
    password = os.getenv('MONGO_PASSWORD')
    URL = f"mongodb+srv://{user}:{password}@project-data.fyzivf2.mongodb.net/?retryWrites=true&w=majority&appName=project-data"
    DB_NAME = 'data'
    DEMAND_COLLECTION_NAME = 'total_demand'
    TEMPERATURE_COLLECTION_NAME = 'temperature'
    TARGET_COLLECTION_NAME = 'features'
    
    # Instantiate the class
    feature_engineering = FeatureEngineering(
        URL,
        DB_NAME,
        DEMAND_COLLECTION_NAME,
        TEMPERATURE_COLLECTION_NAME,
        TARGET_COLLECTION_NAME
    )
    
    # Execute the pipeline
    feature_engineering.run()
    print("Feature Engineering script executed successfully")
