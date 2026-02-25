import random
from models import Profile

NUM_PROFILES = 50

FIRST_NAMES = [
    "James", "Mary", "Robert", "Patricia", "John", "Jennifer", "Michael", "Linda",
    "David", "Elizabeth", "William", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Christopher", "Karen", "Charles", "Lisa", "Daniel", "Nancy",
    "Matthew", "Betty", "Anthony", "Margaret", "Mark", "Sandra", "Donald", "Ashley",
    "Steven", "Dorothy", "Andrew", "Kimberly", "Paul", "Emily", "Joshua", "Donna",
    "Kenneth", "Michelle", "Kevin", "Carol", "Brian", "Amanda", "George", "Melissa",
    "Timothy", "Deborah", "Ronald", "Stephanie", "Edward", "Rebecca", "Jason", "Sharon",
    "Jeffrey", "Laura", "Ryan", "Cynthia", "Jacob", "Kathleen", "Gary", "Amy",
    "Nicholas", "Angela", "Eric", "Shirley", "Jonathan", "Anna", "Stephen", "Brenda",
    "Larry", "Pamela", "Justin", "Emma", "Scott", "Nicole", "Brandon", "Helen",
]

LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores",
    "Green", "Adams", "Nelson", "Baker", "Hall", "Rivera", "Campbell", "Mitchell",
    "Carter", "Roberts", "Gomez", "Phillips", "Evans", "Turner", "Diaz", "Parker",
    "Cruz", "Edwards", "Collins", "Reyes", "Stewart", "Morris", "Morales", "Murphy",
]

OCCUPATIONS = [
    "Software Engineer", "Teacher", "Nurse", "Accountant", "Sales Manager",
    "Marketing Director", "Electrician", "Pharmacist", "Graphic Designer",
    "Financial Analyst", "Chef", "Architect", "Dentist", "Lawyer",
    "Mechanic", "Data Scientist", "Real Estate Agent", "Consultant",
    "Civil Engineer", "Veterinarian", "Pilot", "Journalist", "Plumber",
    "HR Manager", "Truck Driver", "Paramedic", "Photographer", "Surgeon",
    "Bartender", "Retail Associate",
]

LOAN_PURPOSES = ["Mortgage", "Auto", "Personal", "Education", "Debt Consolidation", "Business"]

def generate_profile(profile_id: int) -> Profile:
    age = random.randint(21, 72)
    employment_years = min(random.randint(0, age - 18), 45)

    # Income correlates loosely with age and employment
    base_income = random.gauss(55000, 25000)
    experience_bonus = employment_years * random.uniform(500, 2000)
    annual_income = max(18000, min(350000, base_income + experience_bonus))

    # Credit score: skewed toward middle, affected by age/employment
    base_score = random.gauss(680, 100)
    stability_bonus = min(employment_years * 3, 50)
    credit_score = int(max(300, min(850, base_score + stability_bonus)))

    # Debt correlates with income
    debt_ratio = random.uniform(0.0, 0.8)
    existing_debt = round(annual_income * debt_ratio, 2)

    num_accounts = random.randint(1, 15)
    payment_history = round(max(40, min(100, random.gauss(88, 15))), 1)
    credit_utilization = round(max(0, min(100, random.gauss(35, 25))), 1)
    
    # New fields logic
    loan_purpose = random.choice(LOAN_PURPOSES)
    if loan_purpose == "Mortgage":
        loan_amount_requested = round(random.uniform(100000, 750000), 2)
    elif loan_purpose == "Auto":
        loan_amount_requested = round(random.uniform(5000, 60000), 2)
    elif loan_purpose == "Business":
        loan_amount_requested = round(random.uniform(10000, 250000), 2)
    else:  # Personal, Education, Debt Consolidation
        loan_amount_requested = round(random.uniform(1000, 35000), 2)

    # Simulate recent bankruptcies (rare but heavily impactful)
    recent_bankruptcies = 0
    if random.random() < 0.03:  # 3% chance
        recent_bankruptcies = random.randint(1, 2)
        credit_score = max(300, credit_score - random.randint(100, 250)) # Penalty for bankruptcy

    # Simulate missed payments history within last 24 months
    missed_payments_history = 0
    if random.random() < 0.20: # 20% chance
        missed_payments_history = random.randint(1, 5)

    return Profile(
        id=profile_id,
        name=f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}",
        age=age,
        occupation=random.choice(OCCUPATIONS),
        annual_income=round(annual_income, 2),
        credit_score=credit_score,
        existing_debt=existing_debt,
        employment_years=employment_years,
        num_credit_accounts=num_accounts,
        payment_history=payment_history,
        credit_utilization=credit_utilization,
        loan_amount_requested=loan_amount_requested,
        loan_purpose=loan_purpose,
        recent_bankruptcies=recent_bankruptcies,
        missed_payments_history=missed_payments_history
    )
