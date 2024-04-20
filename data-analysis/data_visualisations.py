"""
This script performs data visualisations on the data stored in the MongoDB database.
"""

import os
import pandas as pd
import matplotlib.pyplot as plt
import math as m
import numpy as np
from pymongo.database import Database
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

class DataVisualisations:
    """The DataVisualisations class contains methods for visualising data
    """
    def __init__(
        self,
        url: str,
        db_name: str,
        feature_collection_name: str,
        target_directory: str,
        source_data: str = None
    ) -> None:
        self.client = self.mongo_client(url)
        self.db = self.mongo_database(db_name)
        self.feature_collection_name = feature_collection_name
        self.target_directory = target_directory
        self.source_data = source_data
        
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
    
    def generate_histograms(self, df: pd.DataFrame) -> None:
        """Generates histograms for TOTALDEMAND and TEMPERATURE

        Args:
            df (pd.DataFrame): The data to visualise
        """
        # Variables to plot
        variables = {
            'TOTALDEMAND': 'Total Demand',
            'TEMPERATURE': 'Temperature'
        }
        
        # Extract unique states from the DataFrame
        states = df['state'].unique()
        
        for variable, pretty_name in variables.items():
            # Determine number of rows needed for the subplot grid, always 2 columns
            num_rows = m.ceil(len(states) / 2)

            # Create subplot
            axs = plt.subplots(num_rows, 2, figsize=(10, 5 * num_rows))[1]

            # Flatten the axes array for easier iteration
            axs = axs.ravel()

            for i, state in enumerate(states):
                # Filter the DataFrame for the state and plot the histogram for the specified variable
                state_data = df[df['state'] == state]
                axs[i].hist(state_data[variable], bins=100, color='blue' if variable == 'TOTALDEMAND' else 'green', alpha=0.7)
                axs[i].set_title(f'{pretty_name} Distribution in {state}')
                axs[i].set_xlabel(pretty_name)
                axs[i].set_ylabel('Frequency')
            
            # If there are odd number of states, turn off the last ax if unused
            if len(states) % 2 != 0:
                axs[-1].axis('off')

            # Adjust layout to prevent overlap
            plt.tight_layout()

            # Check if the directory exists, and if not, create it
            if not os.path.exists(self.target_directory):
                os.makedirs(self.target_directory)

            # Save the plot to a PDF file for the specific variable
            plt.savefig(f'{self.target_directory}/{pretty_name.replace(" ", "_").lower()}_distribution.pdf')

            # Close the plot to free up memory
            plt.close()
            
    def generate_scatter_plots(self, df: pd.DataFrame) -> None:
        """Generates scatter plots of Total Demand vs Temperature with a quadratic fit line

        Args:
            df (pd.DataFrame): The data to visualize
        """
        # Extract unique states from the DataFrame
        states = df['state'].unique()

        # Determine number of rows needed for the subplot grid, always 2 columns
        num_rows = m.ceil(len(states) / 2)

        # Create subplot
        axs = plt.subplots(num_rows, 2, figsize=(10, 5 * num_rows))[1]
        
        # Flatten the axes array for easier iteration
        axs = axs.ravel()

        for i, state in enumerate(states):
            # Filter the DataFrame for the state
            state_data = df[df['state'] == state]
            x = state_data['TEMPERATURE']
            y = state_data['TOTALDEMAND']

            # Fit a quadratic curve
            coefficients = np.polyfit(x, y, 2)  # 2 for quadratic
            polynomial = np.poly1d(coefficients)
            x_line = np.linspace(min(x), max(x), 100)
            y_line = polynomial(x_line)

            # Plotting the scatter plot and the quadratic fit line
            axs[i].scatter(x, y, color='blue', alpha=0.3, s=10)
            axs[i].plot(x_line, y_line, color='red')
            axs[i].set_title(f'Total Demand vs Temperature in {state}')
            axs[i].set_xlabel('Temperature')
            axs[i].set_ylabel('Total Demand')

        # Handle any unused subplots in case of an odd number of states
        if len(states) % 2 != 0:
            axs[-1].axis('off')
        
        # Adjust layout to prevent overlap
        plt.tight_layout()

        # Check if the directory exists, and if not, create it
        if not os.path.exists(self.target_directory):
            os.makedirs(self.target_directory)

        # Save the plot to a PDF file
        plt.savefig(f'{self.target_directory}/demand_vs_temperature_scatter.png')

        # Close the plot to free up memory
        plt.close()
        
    def generate_tufte_plots(self, df: pd.DataFrame):
        """Generates Tufte-style plots for predefined data categorizations.
        
        Args:
            df (pd.DataFrame): The data to visualize
        """
        # Ensure DATETIME is parsed as datetime if not already
        if 'DATETIME' in df.columns and not np.issubdtype(df['DATETIME'].dtype, np.datetime64):
            df['DATETIME'] = pd.to_datetime(df['DATETIME'])

        # Configurations defined within the function
        configurations = [
            {'column_name': 'month', 'labels': ["January", "February", "March", "April", "May", "June", 
                                                "July", "August", "September", "October", "November", "December"], 'column_is_date': True},
            {'column_name': 'day_of_week', 'labels': ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"], 'column_is_date': True},
            {'column_name': 'is_weekday', 'labels': ['Weekday', 'Weekend'], 'column_is_date': False},
            {'column_name': 'is_public_holiday', 'labels': ['Public Holiday', 'Normal Day'], 'column_is_date': False},
            {'column_name': 'is_daylight', 'labels': ['Day', 'Night'], 'column_is_date': False}
        ]

        for config in configurations:
            column_name = config['column_name']
            labels = config['labels']
            column_is_date = config.get('column_is_date', False)

            # Convert column to month or weekday name if required
            if column_is_date:
                if 'month' == column_name:
                    df[column_name] = df['DATETIME'].dt.month_name()
                elif 'day_of_week' == column_name:
                    df[column_name] = df['DATETIME'].dt.day_name()

            # Map boolean values to descriptive labels if necessary
            if not column_is_date:
                df[column_name] = df[column_name].map({True: labels[0], False: labels[1]})

            # Group data by the specified column and collect 'TOTALDEMAND' in lists
            data = df.groupby(column_name)['TOTALDEMAND'].apply(list).reindex(labels)

            # Calculate medians for each group
            medians = data.apply(np.median)

            # Create plot
            ax = plt.subplots(figsize=(10, 5))[1]
            ax.boxplot(
                x=data.tolist(), vert=False, showbox=False,
                medianprops={'linewidth': 0}, whis=5, showcaps=False,
            )

            # Scatter plot for medians
            ax.scatter(medians, range(1, len(medians) + 1), s=20, color='black')

            # Set labels and formatting
            ax.set_yticks(range(1, len(data) + 1))
            ax.set_yticklabels(labels)
            ax.set_xlabel('Total Demand')
            ax.spines['left'].set_visible(False)
            ax.spines['top'].set_visible(False)
            ax.spines['right'].set_visible(False)
            ax.tick_params(axis='y', which='major', left=False)
            ax.grid(True, linestyle='--', linewidth=0.5, axis='x')

            plt.tight_layout()  # Ensure nothing is cut off

            # Check if the directory exists, and if not, create it
            if not os.path.exists(self.target_directory):
                os.makedirs(self.target_directory)

            # Save the plot to a file
            plt.savefig(f'{self.target_directory}/tufte_plot_{column_name}.pdf')

            # Close the plot to free up memory
            plt.close()

    def run(self):
        """Runs the feature engineering pipeline
        """
        try:      
            # Read feature data from MongoDB
            if self.source_data:
                features = pd.read_csv("/Users/dsartor/Repos/uni/Team-K---ZZSC9020-Capstone-Project/data/modelling_data.csv")
                print(f"Read data from {self.source_data}")
            else:
                features = self.read_mongo_data(self.feature_collection_name)
            
            # Generate histograms of demand data
            self.generate_histograms(features)
            
            # Generate scatter plots of demand vs temperature data
            self.generate_scatter_plots(features)
            
            # Generate a Tufte-style plot of demand by day of the week
            self.generate_tufte_plots(features)
            
        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            if not self.source_data:
                # Close the connection to the MongoDB client
                self.close_connection()


if __name__ == "__main__":
    # Define constants
    user = os.getenv('MONGO_USER') or input('Username: ')
    password = os.getenv('MONGO_PASSWORD') or input('Password: ')
    URL = f"mongodb+srv://{user}:{password}@project-data.fyzivf2.mongodb.net/?retryWrites=true&w=majority&appName=project-data"
    DB_NAME = 'data'
    FEATURE_COLLECTION_NAME = 'features'
    TARGET_DIRECTORY = '/Users/dsartor/Repos/uni/Team-K---ZZSC9020-Capstone-Project/data-analysis/plots'
    SOURCE_DATA = '/Users/dsartor/Repos/uni/Team-K---ZZSC9020-Capstone-Project/data/modelling_data.csv'
    
    # Instantiate the class
    data_visualisations = DataVisualisations(
        URL,
        DB_NAME,
        FEATURE_COLLECTION_NAME,
        TARGET_DIRECTORY,
        SOURCE_DATA
    )
    
    # Execute the pipeline
    data_visualisations.run()
    print("Data visualisations generated successfully!")
