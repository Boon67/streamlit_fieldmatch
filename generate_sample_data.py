#!/usr/bin/env python3
"""
Generate sample claims data files for testing the upload process.
Each file simulates data from a different company with different column naming conventions.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Sample data pools
FIRST_NAMES = ['James', 'Mary', 'John', 'Patricia', 'Robert', 'Jennifer', 'Michael', 'Linda', 
               'William', 'Elizabeth', 'David', 'Barbara', 'Richard', 'Susan', 'Joseph', 'Jessica',
               'Thomas', 'Sarah', 'Charles', 'Karen', 'Christopher', 'Nancy', 'Daniel', 'Lisa',
               'Matthew', 'Betty', 'Anthony', 'Margaret', 'Mark', 'Sandra', 'Donald', 'Ashley']

LAST_NAMES = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis',
              'Rodriguez', 'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson',
              'Thomas', 'Taylor', 'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson',
              'White', 'Harris', 'Sanchez', 'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker']

GROUP_NAMES = ['Acme Corporation', 'Global Industries', 'Tech Solutions Inc', 'Healthcare Partners',
               'United Manufacturing', 'Premier Services LLC', 'National Foods Co', 'Metro Transit Authority',
               'State University', 'City of Springfield', 'County Schools District', 'Regional Hospital System']

PRODUCT_TYPES = ['HMO', 'PPO', 'POS', 'EPO', 'HDHP', 'Medicare Advantage', 'Medicaid']

SERVICE_TYPES = ['Medical', 'Pharmacy', 'Dental', 'Vision', 'Mental Health', 'Physical Therapy']

DRUG_NAMES = ['Lisinopril', 'Metformin', 'Atorvastatin', 'Omeprazole', 'Amlodipine', 
              'Metoprolol', 'Losartan', 'Gabapentin', 'Hydrochlorothiazide', 'Sertraline',
              'Simvastatin', 'Montelukast', 'Escitalopram', 'Rosuvastatin', 'Bupropion']

# ICD-10 codes (sample)
ICD_CODES = ['E11.9', 'I10', 'J06.9', 'M54.5', 'F32.9', 'K21.0', 'J45.909', 'E78.5',
             'G43.909', 'N39.0', 'R10.9', 'J02.9', 'M79.3', 'R05', 'K59.00']

# CPT codes (sample)
CPT_CODES = ['99213', '99214', '99215', '99203', '99204', '99205', '99211', '99212',
             '90834', '90837', '97110', '97140', '36415', '81003', '80053']

# Revenue codes
REVENUE_CODES = ['0250', '0260', '0270', '0300', '0320', '0450', '0510', '0636']

def random_date(start_year=2023, end_year=2024):
    """Generate a random date"""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return start + timedelta(days=random_days)

def random_dob(min_age=18, max_age=85):
    """Generate a random date of birth"""
    today = datetime.now()
    age = random.randint(min_age, max_age)
    birth_year = today.year - age
    return datetime(birth_year, random.randint(1, 12), random.randint(1, 28))

def random_amount(min_val, max_val, decimals=2):
    """Generate a random dollar amount"""
    return round(random.uniform(min_val, max_val), decimals)

def generate_npi():
    """Generate a random NPI number"""
    return ''.join([str(random.randint(0, 9)) for _ in range(10)])

def generate_tin():
    """Generate a random TIN"""
    return f"{random.randint(10, 99)}-{random.randint(1000000, 9999999)}"

def generate_claim_number(prefix='CLM'):
    """Generate a random claim number"""
    return f"{prefix}{random.randint(100000000, 999999999)}"


# ============================================================================
# FILE 1: Anthem Blue Cross - Medical Claims (uses underscores, abbreviated names)
# ============================================================================
def generate_anthem_medical(num_records):
    """Generate Anthem-style medical claims data"""
    records = []
    for _ in range(num_records):
        svc_date = random_date()
        proc_date = svc_date + timedelta(days=random.randint(5, 30))
        billed = random_amount(50, 5000)
        allowed = billed * random.uniform(0.4, 0.9)
        deduct = random_amount(0, min(200, allowed * 0.3))
        copay = random.choice([0, 20, 25, 30, 40, 50])
        coins = (allowed - deduct - copay) * random.uniform(0, 0.2)
        paid = max(0, allowed - deduct - copay - coins)
        
        records.append({
            'MEMBER_FIRST_NAME': random.choice(FIRST_NAMES),
            'MEMBER_LAST_NAME': random.choice(LAST_NAMES),
            'MEMBER_DOB': random_dob().strftime('%Y-%m-%d'),
            'SUBSCRIBER_FIRST': random.choice(FIRST_NAMES),
            'SUBSCRIBER_LAST': random.choice(LAST_NAMES),
            'SUBSCRIBER_DOB': random_dob().strftime('%Y-%m-%d'),
            'GROUP_NAME': random.choice(GROUP_NAMES),
            'POLICY_EFF_DATE': random_date(2022, 2023).strftime('%Y-%m-%d'),
            'CLAIM_NUM': generate_claim_number('ANT'),
            'SERVICE_FROM': svc_date.strftime('%Y-%m-%d'),
            'SERVICE_THRU': (svc_date + timedelta(days=random.randint(0, 3))).strftime('%Y-%m-%d'),
            'PAID_DATE': proc_date.strftime('%Y-%m-%d'),
            'DIAG_1': random.choice(ICD_CODES),
            'DIAG_2': random.choice(ICD_CODES) if random.random() > 0.5 else '',
            'PROC_CODE': random.choice(CPT_CODES),
            'MODIFIER': random.choice(['', '25', '59', '76', 'GT']),
            'REVENUE_CD': random.choice(REVENUE_CODES),
            'PROVIDER_NPI': generate_npi(),
            'PLAN_TYPE': random.choice(PRODUCT_TYPES),
            'BILLED_CHARGES': round(billed, 2),
            'ALLOWED_AMT': round(allowed, 2),
            'DEDUCTIBLE': round(deduct, 2),
            'COPAY': copay,
            'COINSURANCE': round(coins, 2),
            'PLAN_PAID': round(paid, 2),
            'CLAIM_TYPE': 'Medical',
            'PROVIDER_NAME': f"Dr. {random.choice(LAST_NAMES)}",
            'PROVIDER_ADDRESS': f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Park', 'Medical Center'])} {random.choice(['St', 'Ave', 'Blvd', 'Dr'])}",
            'PROVIDER_TIN': generate_tin()
        })
    return pd.DataFrame(records)


# ============================================================================
# FILE 2: UnitedHealth - Pharmacy Claims (different naming convention)
# ============================================================================
def generate_united_pharmacy(num_records):
    """Generate United-style pharmacy claims data"""
    records = []
    for _ in range(num_records):
        fill_date = random_date()
        
        records.append({
            'PatientFirstName': random.choice(FIRST_NAMES),
            'PatientLastName': random.choice(LAST_NAMES),
            'PatientDOB': random_dob().strftime('%m/%d/%Y'),
            'SubscriberFirst': random.choice(FIRST_NAMES),
            'SubscriberLast': random.choice(LAST_NAMES),
            'SubscriberDOB': random_dob().strftime('%m/%d/%Y'),
            'EmployerGroup': random.choice(GROUP_NAMES),
            'EffectiveDate': random_date(2022, 2023).strftime('%m/%d/%Y'),
            'ClaimID': generate_claim_number('UHC'),
            'DateFilled': fill_date.strftime('%m/%d/%Y'),
            'ProcessedDate': (fill_date + timedelta(days=random.randint(1, 5))).strftime('%m/%d/%Y'),
            'DrugName': random.choice(DRUG_NAMES),
            'Quantity': random.randint(30, 90),
            'DaysSupply': random.choice([30, 60, 90]),
            'PharmacyNPI': generate_npi(),
            'ProductType': random.choice(PRODUCT_TYPES),
            'BilledAmt': round(random_amount(10, 500), 2),
            'CopayAmt': random.choice([5, 10, 15, 20, 25, 30, 35, 50]),
            'DeductibleAmt': round(random_amount(0, 50), 2),
            'PlanPaidAmt': round(random_amount(5, 400), 2),
            'PharmacyName': f"{random.choice(['CVS', 'Walgreens', 'Rite Aid', 'Costco', 'Walmart'])} Pharmacy",
            'PharmacyAddress': f"{random.randint(100, 9999)} {random.choice(['Commerce', 'Retail', 'Shopping'])} {random.choice(['Pkwy', 'Plaza', 'Center'])}",
            'PharmacyTIN': generate_tin()
        })
    return pd.DataFrame(records)


# ============================================================================
# FILE 3: Cigna - Mixed Claims (uses spaces in column names)
# ============================================================================
def generate_cigna_mixed(num_records):
    """Generate Cigna-style mixed claims data"""
    records = []
    for _ in range(num_records):
        svc_date = random_date()
        is_rx = random.random() > 0.7
        billed = random_amount(20, 3000)
        allowed = billed * random.uniform(0.5, 0.85)
        member_resp = allowed * random.uniform(0.1, 0.3)
        paid = allowed - member_resp
        
        records.append({
            'Claimant First Name': random.choice(FIRST_NAMES),
            'Claimant Last Name': random.choice(LAST_NAMES),
            'Claimant Date of Birth': random_dob().strftime('%Y%m%d'),
            'Insured First Name': random.choice(FIRST_NAMES),
            'Insured Last Name': random.choice(LAST_NAMES),
            'Insured Date of Birth': random_dob().strftime('%Y%m%d'),
            'Group': random.choice(GROUP_NAMES),
            'Policy Start Date': random_date(2022, 2023).strftime('%Y%m%d'),
            'Claim Reference': generate_claim_number('CIG'),
            'Service Start Date': svc_date.strftime('%Y%m%d'),
            'Service End Date': (svc_date + timedelta(days=random.randint(0, 2))).strftime('%Y%m%d'),
            'Adjudication Date': (svc_date + timedelta(days=random.randint(7, 21))).strftime('%Y%m%d'),
            'Primary Diagnosis': random.choice(ICD_CODES),
            'Secondary Diagnosis': random.choice(ICD_CODES) if random.random() > 0.6 else '',
            'Procedure Code': random.choice(CPT_CODES) if not is_rx else '',
            'HCPCS': random.choice(['J0585', 'J1745', 'J2505', '']) if random.random() > 0.8 else '',
            'Modifier': random.choice(['', '25', '59']),
            'Revenue': random.choice(REVENUE_CODES),
            'Rendering NPI': generate_npi(),
            'Product': random.choice(PRODUCT_TYPES),
            'Drug Name': random.choice(DRUG_NAMES) if is_rx else '',
            'Drug Qty': random.randint(30, 90) if is_rx else '',
            'Days Supply': random.choice([30, 60, 90]) if is_rx else '',
            'Fill Date': svc_date.strftime('%Y%m%d') if is_rx else '',
            'Total Billed': round(billed, 2),
            'Allowed': round(allowed, 2),
            'Member Responsibility': round(member_resp, 2),
            'Paid': round(paid, 2),
            'Service Type': 'Pharmacy' if is_rx else random.choice(['Medical', 'Dental', 'Vision']),
            'Provider': f"{random.choice(['Dr.', 'MD', ''])} {random.choice(LAST_NAMES)} {random.choice(['Medical Group', 'Clinic', 'Associates', 'Health'])}",
            'Provider Tax ID': generate_tin()
        })
    return pd.DataFrame(records)


# ============================================================================
# FILE 4: Aetna - Dental Claims (abbreviated column names)
# ============================================================================
def generate_aetna_dental(num_records):
    """Generate Aetna-style dental claims data"""
    dental_codes = ['D0120', 'D0150', 'D0210', 'D0274', 'D1110', 'D1120', 'D2391', 'D2392', 
                    'D2750', 'D2751', 'D4341', 'D4342', 'D7140', 'D7210']
    
    records = []
    for _ in range(num_records):
        svc_date = random_date()
        billed = random_amount(75, 1500)
        allowed = billed * random.uniform(0.6, 0.95)
        deduct = random_amount(0, min(100, allowed * 0.2))
        coins = (allowed - deduct) * random.uniform(0.2, 0.5)
        paid = max(0, allowed - deduct - coins)
        
        records.append({
            'PAT_FNAME': random.choice(FIRST_NAMES),
            'PAT_LNAME': random.choice(LAST_NAMES),
            'PAT_BIRTH_DT': random_dob().strftime('%m-%d-%Y'),
            'SUB_FNAME': random.choice(FIRST_NAMES),
            'SUB_LNAME': random.choice(LAST_NAMES),
            'SUB_BIRTH_DT': random_dob().strftime('%m-%d-%Y'),
            'GRP_NM': random.choice(GROUP_NAMES),
            'POL_EFF_DT': random_date(2022, 2023).strftime('%m-%d-%Y'),
            'CLM_NBR': generate_claim_number('AET'),
            'SVC_DT': svc_date.strftime('%m-%d-%Y'),
            'PROC_DT': (svc_date + timedelta(days=random.randint(3, 14))).strftime('%m-%d-%Y'),
            'CDT_CD': random.choice(dental_codes),
            'TOOTH_NBR': random.choice(['', '1', '2', '3', '14', '15', '18', '19', '30', '31']),
            'SURFACE': random.choice(['', 'M', 'O', 'D', 'B', 'L', 'MOD', 'DO']),
            'DENTIST_NPI': generate_npi(),
            'PROD_TYPE': random.choice(['DPPO', 'DHMO', 'Indemnity']),
            'CHRG_AMT': round(billed, 2),
            'ALLOW_AMT': round(allowed, 2),
            'DED_AMT': round(deduct, 2),
            'COINS_AMT': round(coins, 2),
            'PAID_AMT': round(paid, 2),
            'DENTIST_NM': f"Dr. {random.choice(LAST_NAMES)}, DDS",
            'DENTIST_ADDR': f"{random.randint(100, 9999)} {random.choice(['Dental', 'Smile', 'Tooth'])} {random.choice(['Way', 'Lane', 'Court'])}",
            'DENTIST_TIN': generate_tin()
        })
    return pd.DataFrame(records)


# ============================================================================
# FILE 5: Kaiser - Vision Claims (CamelCase naming)
# ============================================================================
def generate_kaiser_vision(num_records):
    """Generate Kaiser-style vision claims data"""
    vision_codes = ['92004', '92014', '92015', '92310', '92311', 'V2020', 'V2100', 'V2200', 
                    'V2300', 'V2410', 'V2500', 'S0500', 'S0504', 'S0580']
    
    records = []
    for _ in range(num_records):
        svc_date = random_date()
        billed = random_amount(50, 800)
        allowed = billed * random.uniform(0.5, 0.9)
        copay = random.choice([0, 10, 15, 20, 25])
        paid = max(0, allowed - copay)
        
        records.append({
            'MemberFirstName': random.choice(FIRST_NAMES),
            'MemberLastName': random.choice(LAST_NAMES),
            'MemberBirthDate': random_dob().strftime('%Y/%m/%d'),
            'SubscriberFirstName': random.choice(FIRST_NAMES),
            'SubscriberLastName': random.choice(LAST_NAMES),
            'SubscriberBirthDate': random_dob().strftime('%Y/%m/%d'),
            'GroupName': random.choice(GROUP_NAMES),
            'CoverageEffectiveDate': random_date(2022, 2023).strftime('%Y/%m/%d'),
            'ClaimNumber': generate_claim_number('KP'),
            'DateOfService': svc_date.strftime('%Y/%m/%d'),
            'DateProcessed': (svc_date + timedelta(days=random.randint(5, 15))).strftime('%Y/%m/%d'),
            'VisionCode': random.choice(vision_codes),
            'FrameOrLens': random.choice(['Frame', 'Single Vision Lens', 'Bifocal Lens', 'Progressive Lens', 'Contact Lens', 'Exam']),
            'OptometristNPI': generate_npi(),
            'PlanType': 'Vision',
            'ChargedAmount': round(billed, 2),
            'AllowedAmount': round(allowed, 2),
            'CopayAmount': copay,
            'PlanPaid': round(paid, 2),
            'ProviderName': f"{random.choice(['Vision', 'Eye', 'Optical'])} {random.choice(['Center', 'Care', 'Associates'])}",
            'ProviderAddress': f"{random.randint(100, 9999)} {random.choice(['Vision', 'Optical', 'Eye Care'])} {random.choice(['Blvd', 'Ave', 'St'])}",
            'ProviderTaxID': generate_tin()
        })
    return pd.DataFrame(records)


# ============================================================================
# Main execution
# ============================================================================
def main():
    output_dir = 'sample_data'
    os.makedirs(output_dir, exist_ok=True)
    
    print("Generating sample claims data files...\n")
    
    # File 1: Anthem Medical Claims (CSV)
    num_records = random.randint(800, 1200)
    df = generate_anthem_medical(num_records)
    filename = f"{output_dir}/anthem_bluecross-claims-20240115.csv"
    df.to_csv(filename, index=False)
    print(f"✓ Created {filename} ({len(df)} records)")
    
    # File 2: United Pharmacy Claims (CSV)
    num_records = random.randint(800, 1200)
    df = generate_united_pharmacy(num_records)
    filename = f"{output_dir}/unitedhealth-claims-20240201.csv"
    df.to_csv(filename, index=False)
    print(f"✓ Created {filename} ({len(df)} records)")
    
    # File 3: Cigna Mixed Claims (Excel)
    num_records = random.randint(800, 1200)
    df = generate_cigna_mixed(num_records)
    filename = f"{output_dir}/cigna_healthcare-claims-20240215.xlsx"
    df.to_excel(filename, index=False, engine='openpyxl')
    print(f"✓ Created {filename} ({len(df)} records)")
    
    # File 4: Aetna Dental Claims (CSV)
    num_records = random.randint(800, 1200)
    df = generate_aetna_dental(num_records)
    filename = f"{output_dir}/aetna_dental-claims-20240301.csv"
    df.to_csv(filename, index=False)
    print(f"✓ Created {filename} ({len(df)} records)")
    
    # File 5: Kaiser Vision Claims (Excel)
    num_records = random.randint(800, 1200)
    df = generate_kaiser_vision(num_records)
    filename = f"{output_dir}/kaiser_permanente-claims-20240315.xlsx"
    df.to_excel(filename, index=False, engine='openpyxl')
    print(f"✓ Created {filename} ({len(df)} records)")
    
    print(f"\n✅ All sample files created in '{output_dir}/' directory")
    print("\nColumn naming conventions used:")
    print("  - Anthem: UPPERCASE_WITH_UNDERSCORES")
    print("  - United: CamelCase")
    print("  - Cigna: Spaces In Names")
    print("  - Aetna: ABBREVIATED_CAPS")
    print("  - Kaiser: CamelCase")

if __name__ == '__main__':
    main()

