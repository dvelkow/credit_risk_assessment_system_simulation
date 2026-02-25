import csv
import sys
from typing import List
from models import Profile

# Export headers
HEADERS = [
    "ID", "Name", "Age", "Occupation", "AnnualIncome", 
    "CreditScore", "ExistingDebt", "EmploymentYears", 
    "NumCreditAccounts", "PaymentHistoryPct", "CreditUtilizationPct", 
    "LoanAmountRequested", "LoanPurpose", "RecentBankruptcies", 
    "MissedPaymentsHistory", 
    "CalculatedRiskScore", "CalculatedRiskCategory", 
    "CalculatedDTI", "CalculatedSavingsAPY", 
    "CalculatedLendingRate", "ApprovedCreditLimit", "EstimatedMonthlyPayment"
]

def load_profiles_from_csv(filepath: str) -> List[Profile]:
    """
    Parses a CSV file containing real applicant data into Profile objects.
    Expected headers (case insensitive):
        id, name, age, occupation, annual_income, credit_score, existing_debt,
        employment_years, num_credit_accounts, payment_history, credit_utilization,
        loan_amount_requested, loan_purpose, recent_bankruptcies, missed_payments_history
    """
    profiles = []
    
    try:
        with open(filepath, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            # Helper to safely parse strings to numbers, defaulting to 0 if missing
            def _parse_float(row, key, default=0.0):
                val = row.get(key, "").strip()
                return float(val) if val else default
                
            def _parse_int(row, key, default=0):
                val = row.get(key, "").strip()
                return int(float(val)) if val else default
            
            for index, row in enumerate(reader):
                # Normalizing keys to allow for slight variations in the header row
                normalized_row = {k.strip().lower(): v for k, v in row.items() if k}
                
                # Auto-assign an ID if missing
                profile_id = _parse_int(normalized_row, "id", index + 1)
                
                p = Profile(
                    id=profile_id,
                    name=normalized_row.get("name", "Unknown Applicant"),
                    age=_parse_int(normalized_row, "age", 30),
                    occupation=normalized_row.get("occupation", "Unemployed"),
                    annual_income=_parse_float(normalized_row, "annual_income", 30000.0),
                    credit_score=_parse_int(normalized_row, "credit_score", 600),
                    existing_debt=_parse_float(normalized_row, "existing_debt", 0.0),
                    employment_years=_parse_int(normalized_row, "employment_years", 0),
                    num_credit_accounts=_parse_int(normalized_row, "num_credit_accounts", 1),
                    payment_history=_parse_float(normalized_row, "payment_history", 100.0),
                    credit_utilization=_parse_float(normalized_row, "credit_utilization", 0.0),
                    loan_amount_requested=_parse_float(normalized_row, "loan_amount_requested", 5000.0),
                    loan_purpose=normalized_row.get("loan_purpose", "Personal"),
                    recent_bankruptcies=_parse_int(normalized_row, "recent_bankruptcies", 0),
                    missed_payments_history=_parse_int(normalized_row, "missed_payments_history", 0)
                )
                profiles.append(p)
    except FileNotFoundError:
        print(f"Error: The input file '{filepath}' was not found.")
        sys.exit(1)
    except Exception as e:
        print(f"Error parsing CSV file '{filepath}': {e}")
        sys.exit(1)
        
    return profiles

def export_profiles_to_csv(profiles: List[Profile], filepath: str) -> None:
    """
    Writes assessed Profile objects (including calculated fields) out into a CSV format.
    """
    try:
        with open(filepath, mode="w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(HEADERS)
            
            for p in profiles:
                row = [
                    p.id,
                    p.name,
                    p.age,
                    p.occupation,
                    f"{p.annual_income:.2f}",
                    p.credit_score,
                    f"{p.existing_debt:.2f}",
                    p.employment_years,
                    p.num_credit_accounts,
                    f"{p.payment_history:.1f}",
                    f"{p.credit_utilization:.1f}",
                    f"{p.loan_amount_requested:.2f}",
                    p.loan_purpose,
                    p.recent_bankruptcies,
                    p.missed_payments_history,
                    f"{p.risk_score:.1f}",
                    p.risk_category,
                    f"{p.dti_ratio:.1f}",
                    f"{p.savings_apy:.2f}",
                    f"{p.lending_rate:.2f}",
                    f"{p.credit_limit:.2f}",
                    f"{p.monthly_payment:.2f}"
                ]
                writer.writerow(row)
    except Exception as e:
        print(f"Error writing to output CSV output file '{filepath}': {e}")
        sys.exit(1)
