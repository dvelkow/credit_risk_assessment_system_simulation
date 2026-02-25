from dataclasses import dataclass

@dataclass
class Profile:
    id: int
    name: str
    age: int
    occupation: str
    annual_income: float
    credit_score: int
    existing_debt: float
    employment_years: int
    num_credit_accounts: int
    payment_history: float        # % on-time payments (0-100)
    credit_utilization: float     # % of credit used (0-100)
    
    # New fields for enhanced algorithm
    loan_amount_requested: float
    loan_purpose: str
    recent_bankruptcies: int
    missed_payments_history: int

    # Computed fields
    dti_ratio: float = 0.0
    risk_score: float = 0.0
    risk_category: str = ""
    savings_apy: float = 0.0
    lending_rate: float = 0.0
    credit_limit: float = 0.0
    monthly_payment: float = 0.0
    reason: str = ""
