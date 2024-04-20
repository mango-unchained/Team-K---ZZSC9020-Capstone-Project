# Data Analysis Module

## Overview
This module provides the scripts for performing detailed data analysis, aimed at extracting meaningful insights from the data sets including. This model provides the functionality to ingest data from our MongoDB database or a local csv file enabling both rapid prototyping and full pipeline development.

## Features
This module includes the following components:
- plots: This directory is used as the output path to store any visualisations generated as part of this module
- data_visualisations.py: This script ingests the prepared feature data and produces a series of plots including histogams, scatter plots and tufte plots
- requirements.txt: This file provides a list of external python packages required by the module
- summary_statistics.py: This module is used to generate summary statistics about the source total_demand and temperature data.

## Getting started
To begin using this module you perform the following steps once you have python installed:
1. Install the required python packages into your environment using shell command:
``` pip install -r requirements.txt```
2. Run the script of choice using something similar to the following command:
```python3 data_visualisations.py```