import math
from models import Profile

RISK_THRESHOLDS = [
    (80, "Excellent"),
    (65, "Good"),
    (50, "Fair"),
    (30, "Poor"),
    (0, "Very Poor"),
]

def assess_risk(p: Profile) -> Profile:
    """
    Assess credit risk using an advanced scorecard model.
    Weights:
        Credit Score:           30%
        Payment History:        20%
        Debt-to-Income (DTI):   15%
        Credit Utilization:     10%
        Employment Stability:   10%
        Loan Purpose Risk:      5%
        Loan Amount requested:  10% (scaled by income/debt)

    Hard rules:
        - Recent Bankruptcies cap the score at "Very Poor" (<30)
        - High missed payments penalize heavily
    """
    
    # 1. Credit Score (30%)
    cs_norm = ((p.credit_score - 300) / 550) * 100
    
    # 2. Payment History (20%)
    ph_norm = p.payment_history
    
    # Penalize for missed payments in the last 24 months
    if p.missed_payments_history > 0:
        ph_norm = max(0, ph_norm - (p.missed_payments_history * 10))

    # 3. Debt to Income (15%)
    p.dti_ratio = round((p.existing_debt / max(p.annual_income, 1)) * 100, 1)
    dti_norm = max(0, 100 - p.dti_ratio)
    
    # Extra penalty for extremely high DTI (> 50%)
    if p.dti_ratio > 50:
        dti_norm = max(0, dti_norm - 20)
    
    # 4. Employment Stability (10%)
    emp_norm = min(p.employment_years / 10, 1.0) * 100
    
    # 5. Credit Utilization (10%)
    cu_norm = max(0, 100 - p.credit_utilization)
    
    # 6. Loan Purpose Risk (5%)
    # Some loans are statistically safer to underwrite
    purpose_scores = {
        "Mortgage": 90,
        "Auto": 80,
        "Education": 75,
        "Debt Consolidation": 60,
        "Personal": 50,
        "Business": 40
    }
    purpose_norm = purpose_scores.get(p.loan_purpose, 50)
    
    # 7. Loan Amount vs Income capability (10%)
    # Ratio of requested loan to current annual income
    loan_to_income_ratio = (p.loan_amount_requested / max(p.annual_income, 1))
    # E.g., asking for 5x your income is riskier than 0.1x your income
    # Mortgages are an exception since they are collateralized
    if p.loan_purpose == "Mortgage":
        lti_threshold = 4.0 # Cap at 4x income
    else:
        lti_threshold = 0.5 # Cap at 50% income for unsecured/auto

    # Higher ratio -> Lower score
    lti_norm = max(0, 100 - (min(loan_to_income_ratio / lti_threshold, 1.0) * 100))

    # ── Calculate Base Score ──
    base_score = (
        cs_norm * 0.30 +
        ph_norm * 0.20 +
        dti_norm * 0.15 +
        cu_norm * 0.10 +
        emp_norm * 0.10 +
        purpose_norm * 0.05 +
        lti_norm * 0.10
    )

    # ── Hard Rules / Overrides ──
    if p.recent_bankruptcies > 0:
        # Severe penalty for bankruptcy
        base_score = min(base_score - (p.recent_bankruptcies * 30), 29)
        
    p.risk_score = round(max(0, min(100, base_score)), 1)

    # Determine risk category
    for threshold, category in RISK_THRESHOLDS:
        if p.risk_score >= threshold:
            p.risk_category = category
            break

    # Calculate rates based on risk
    risk_factor = p.risk_score / 100
    p.savings_apy = round(0.5 + risk_factor * 4.5, 2)       # 0.5% – 5.0%
    p.lending_rate = round(18.0 - risk_factor * 14.0, 2)    # 4.0% – 18.0%
    
    # For credit limit simulation, we cap it relative to what they asked for, mitigated by risk
    base_limit = p.loan_amount_requested
    # High risk = give them a fraction of what they asked. Low risk = give what they asked or more.
    approval_ratio = max(0.1, risk_factor * 1.5) 
    
    # If they are very poor risk, cap heavily regardless of amount requested
    if p.risk_category in ["Poor", "Very Poor"]:
        p.credit_limit = round(min(5000, base_limit * approval_ratio), 0)
    else:
        p.credit_limit = round(base_limit * approval_ratio, 0)
         
    # Calculated monthly payment assuming standard terms (E.g. 5yr unsecured, 30yr mortgage)
    if p.loan_purpose == "Mortgage":
        term_years = 30
    elif p.loan_purpose == "Auto":
        term_years = 5
    else:
        term_years = 3
        
    monthly_rate = (p.lending_rate / 100) / 12
    num_payments = term_years * 12
    
    if monthly_rate == 0:
        p.monthly_payment = round(p.loan_amount_requested / num_payments, 2)
    else:
        # Standard amortizing loan formula
        payment = p.loan_amount_requested * (monthly_rate * math.pow(1 + monthly_rate, num_payments)) / (math.pow(1 + monthly_rate, num_payments) - 1)
        p.monthly_payment = round(payment, 2)

    # Determine reason(s) for the assessment
    reasons = []
    if p.recent_bankruptcies > 0:
        reasons.append("Bankrupt")
    if p.missed_payments_history > 0:
        reasons.append("Late Pmt")
    if p.dti_ratio > 40:
        reasons.append("High DTI")
    if p.credit_score < 650:
        reasons.append("Low Credit")
    if loan_to_income_ratio > 0.5 and p.loan_purpose != "Mortgage":
        reasons.append("High Ask")

    if not reasons:
        if p.risk_score >= 80:
            reasons.append("Strong")
        elif p.risk_score >= 65:
            reasons.append("Good")
        else:
            reasons.append("Average")

    p.reason = ", ".join(reasons)

    return p
