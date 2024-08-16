## Overview
The project is made to make real-time credit risk assessment based on the (bank) client profiles it is given by leveraging big data processing and machine learning, simulating how a bank might evaluate and manage credit risk for its clients.

#### Simulated Output Example
![image](https://github.com/user-attachments/assets/1d1673c8-e598-43f3-800b-93e2b552f3e1)



## How It Works

1. **Data Ingestion**: The system simulates the ingestion of financial data including credit scores, account balances, and transaction histories for thousands of clients.

2. **Data Processing**: Leveraging PySpark's distributed computing capabilities, the raw data is cleaned, transformed, and prepared for analysis at scale.

3. **Machine Learning Model**: 
   - A Random Forest Regressor is trained on historical data to predict credit risk scores.
   - The model considers multiple factors including credit score, account balance, and transaction frequency.

4. **Risk Scoring**: 
   - Each client's risk is calculated using an algorithm that normalizes and weights various financial factors.
   - The calculated risk score is then used to categorize clients into risk categories ranging from "Very Low Risk" to "Very High Risk".

5. **Dynamic Rate Calculation**:
   - Based on the risk assessment, the system dynamically calculates personalized savings APY and lending rates for each client.
   - This ensures competitive rates while managing the bank's overall risk exposure.

6. **Real-time Assessment**: As new financial data comes in, the system can rapidly reassess a client's credit risk, allowing for up-to-date risk management.

7. **Risk Dashboard**: A comprehensive dashboard provides bank managers with key metrics including average credit scores, risk distributions, and potential high-risk clients.


## Installation

Clone the repository:

`git clone https://github.com/dvelkow/credit_risk_data_lake_for_lending`

Install the required packages:

`pip install -r requirements.txt`

Run the main data lake:

`python main.py`

(It would run with random/mock data, but you can easily connect it to a real database through the main.py file)
