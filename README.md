## Overview
The project uses Apache Spark for distributed data processing and Delta Lake for storage, providing a scalable and efficient solution for handling large volumes of financial data.

## Features

- Data ingestion from multiple sources
- Data cleansing and quality checks
- Entity resolution across different data sources
- Dimensional modeling for credit risk analysis
- Credit scoring model using machine learning
- Built-in risk dashboard

## Installation

Clone the repository:

`git clone https://github.com/dvelkow/credit_risk_data_lake_for_lending`

Install the required packages:

`pip install -r requirements.txt`

Run the main data lake:

`python main.py`

#It would run with random/mock data, but you can easily connect it to a real database through the main.py file
