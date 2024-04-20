# Feature Engineering

## Overview
This module provides the scripts for transforming the raw input data into a set of features that can be used as inputs for training the neural network model. The scripts work by downloading the raw data into a DataFrame from MongoDB using the `pymongo` data API. MongoDB serves as the data storage technology for the project providing a single source of truth for all team members. After being downloaded a series of steps are used to clean, transform and enhance the data before the final feature set is written to MongoDB.

## Features
This module includes the following components:
- app.py: This script serves as the entry point for the executing the feature engineering pipeline
- requirements.txt: This file provides a list of external python packages required by the module

## Getting started
To begin using this module you perform the following steps once you have python installed:
1. Install the required python packages into your environment using shell command:
``` pip install -r requirements.txt```
2. Run the script of choice using something similar to the following command:
```python3 app.py```