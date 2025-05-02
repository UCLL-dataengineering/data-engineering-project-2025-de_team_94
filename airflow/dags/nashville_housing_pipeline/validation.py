import datetime
import re

import pandas as pd


non_mandatory_columns = {
    'Suite/ Condo   #', 'Owner Name', 'Address', 'City', 'State',
    'Tax District', 'image', 'Foundation Type', 'Exterior Wall', 'Grade'
}

def validate_parcel_id(value):
    if not isinstance(value, str):
        value = str(value)
    value = value.strip()

    if not value:
        return False

    pattern = r'^\d{3} \d{2} \d[A-Z]? \d{3}\.\d{2}$'
    return re.match(pattern, value) is not None

def validate_land_use(value):
    if not isinstance(value, str):
        return False
    stripped = value.strip()

    if not stripped:
        return False
    
    return stripped == stripped.upper()

def validate_property_address(value):
    if not isinstance(value, str):
        return False
    value = value.strip()

    if not value:
        return False

    pattern = r'^[A-Z0-9\s\.\'\-]+$'
    return re.fullmatch(pattern, value) is not None and value == value.upper()

def validate_property_city(value):
    if not isinstance(value, str):
        return False
    value = value.strip()

    if not value:
        return False

    return bool(re.fullmatch(r"[A-Z\s]+", value))

def validate_date(value):
    if not isinstance(value, str):
        return False
    stripped = value.strip()
    if not stripped:
        return False
    
    try:
        date = pd.to_datetime(value, format='%Y-%m-%d', errors='raise')
        return date <= pd.Timestamp.now()
    except:
        return False

def validate_price(value):
    return isinstance(value, (int, float)) and value >= 0

def validate_legal_reference(value):
    if not isinstance(value, str):
        value = str(value)

    value = value.strip().replace(" ", "")

    if not value:
        return False

    match = re.fullmatch(r'-?(\d{7,8})-(\d{6,8})', value)

    if not match:
        return False
    return True

def validate_sold_as_vacant(value):
    if not isinstance(value, str):
        return False
    value = value.strip()
    if not value:
        return False
    return value in ['Yes', 'No']

def validate_acreage(value):
    try:
        return float(value) >= 0
    except:
        return False
    
def validate_neighborhood(value):
    if not isinstance(value, (int, float)) or value < 0:
        return False
    return len(str(int(value))) <= 5

def validate_year(value):
    try:
        year = int(float(value))
        return 100 <= year <= pd.Timestamp.now().year
    except:
        return False

def validate_numeric(value):
    return isinstance(value, (int, float)) and value >= 0

def validate_bed_bath(value):
    return isinstance(value, (int, float)) and value >= 0 and value <= 20

def validate_row(row):
    errors = []

    validations = {
        'Parcel ID': validate_parcel_id,
        'Land Use': validate_land_use,
        'Property Address': validate_property_address,
        'Property City': validate_property_city,
        'Sale Date': validate_date,
        'Sale Price': validate_price,
        'Legal Reference': validate_legal_reference,
        'Sold As Vacant': validate_sold_as_vacant,
        'Multiple Parcels Involved in Sale': validate_sold_as_vacant,
        'Acreage': validate_acreage,
        'Neighborhood': validate_neighborhood,
        'Land Value': validate_price,
        'Building Value': validate_price,
        'Total Value': validate_price,
        'Finished Area': validate_numeric,
        'Year Built': validate_year,
        'Bedrooms': validate_bed_bath,
        'Full Bath': validate_bed_bath,
        'Half Bath': validate_bed_bath,
    }

    for column, validator in validations.items():
        value = row.get(column)
        if pd.isna(value) and column in non_mandatory_columns:
            continue 
        if not pd.isna(value) and not validator(value):
            errors.append(f"Invalid value in column '{column}': {value}")

    return errors

def validate_dataset(df):
    all_errors = {}
    for index, row in df.iterrows():
        errors = validate_row(row)
        if errors:
            all_errors[index] = errors
    return all_errors
