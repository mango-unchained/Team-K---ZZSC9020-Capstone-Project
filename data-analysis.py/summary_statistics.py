from pymongo import MongoClient
from pymongo.server_api import ServerApi
import pandas as pd
import os

def connect_to_mongo():
    user = os.getenv('MONGO_USER')
    password = os.getenv('MONGO_PASSWORD')
    url = f"mongodb+srv://{user}:{password}@project-data.fyzivf2.mongodb.net/?retryWrites=true&w=majority&appName=project-data"
    client = MongoClient(url, server_api=ServerApi(version='1'))
    return client

def fetch_data(client, db_name, collection_name):
    db = client[db_name]
    collection = db[collection_name]
    data = collection.find()
    df = pd.DataFrame(list(data))
    df.drop('_id', axis=1, inplace=True)
    return df

def analyze_data(df, datetime_col, state_col, value_col):
    analysis = {
        'max_datetime': df[datetime_col].max(),
        'min_datetime': df[datetime_col].min(),
        'unique_states': df[state_col].unique(),
        'max_value': df[value_col].max(),
        'min_value': df[value_col].min(),
        'mean_value': df[value_col].mean(),
        'std_dev_value': df[value_col].std(),
        'nulls_per_column': df.isna().sum(),
        'not_nulls_per_column': df.notna().sum()
    }
    if 'LOCATION' in df.columns:  # Specific to 'temperature' collection
        analysis['unique_locations'] = df['LOCATION'].unique()
    return analysis

def main():
    client = connect_to_mongo()
    total_demand_df = fetch_data(client, 'data', 'total_demand')
    temperature_df = fetch_data(client, 'data', 'temperature')

    total_demand_analysis = analyze_data(total_demand_df, 'DATETIME', 'state', 'TOTALDEMAND')
    temperature_analysis = analyze_data(temperature_df, 'DATETIME', 'state', 'TEMPERATURE')

    print("Total Demand Collection Analysis:")
    print(total_demand_analysis)
    print("\nTemperature Collection Analysis:")
    print(temperature_analysis)

if __name__ == "__main__":
    main()
