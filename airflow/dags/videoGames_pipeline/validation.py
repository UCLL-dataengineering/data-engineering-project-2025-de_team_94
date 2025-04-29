import datetime

import pandas as pd


def validate_name(value):
    if not isinstance(value, str):
        return False
    
    value = value.strip()
    if not value:
        return False
    return True

def validate_platform(value):
    valid_platforms = {
        'Wii', 'NES', 'GB', 'DS', 'X360', 'PS3', 'PS2', 'SNES', 'GBA', 'PS4', '3DS', 'N64',
        'PS', 'XB', 'PC', '2600', 'PSP', 'XOne', 'WiiU', 'GC', 'GEN', 'DC', 'PSV', 'SAT',
        'SCD', 'WS', 'NG', 'TG16', '3DO', 'GG', 'PCFX'
    }

    if not isinstance(value, str):
        return False
    value = value.strip()
    return value in valid_platforms

def validate_yearOfRelease(value):
    if not isinstance(value, int/float):
        return False

    if value > datetime.datetime.now().year:
        return False

    return True

def validate_genre(value):
    if not isinstance(value, str):
        return False

    valid_genres = {
        'Action', 'Adventure', 'Fighting', 'Misc', 'Platform', 'Puzzle', 'Racing',
        'Role-Playing', 'Shooter', 'Simulation', 'Sports', 'Strategy'
    }

    value = value.strip()
    return value in valid_genres

def validate_publisher(value):
    if not isinstance(value, str):
        return False
    
    value_correct = value.strip()
    if not value_correct:
        return False

    return True

def validate_sales(value):
    if not isinstance(value, float):
        return False

    if value < 0 or value > 100:
        return False
    return True

def validate_global_sales(value):
    if not isinstance(value, float):
        return False

    return True

def validate_scores_counts(value):
    if not isinstance(value, float):
        return False

    if value < 0:
        return False
    return True

def validate_User_Score(value):
    if not isinstance(value, str):
        return False
    
    if value == 'tbd':
        return True

    try:
        value = float(value)
    except ValueError:
        return False

    if value < 0 or value > 10:
        return False

    return True

def validate_rating(value):
    if not isinstance(value, str):
        return False

    valid_ratings = {
        'E', 'M', 'T', 'E10+', 'K-A', 'AO', 'EC', 'RP'
    }

    value = value.strip()
    return value in valid_ratings


def validate_row(row):
    errors = []

    validations = {
        'Name': validate_name,
        'Platform': validate_platform,
        'Year_of_Release': validate_yearOfRelease,
        'Genre': validate_genre,
        'Publisher': validate_publisher,
        'NA_Sales': validate_sales,
        'EU_Sales': validate_sales,
        'JP_Sales': validate_sales,
        'Other_Sales': validate_sales,
        'Global_Sales': validate_global_sales,
        'Critic_Score': validate_scores_counts,
        'Critic_Count': validate_scores_counts,
        'User_Score': validate_User_Score,
        'User_Count': validate_scores_counts,
        'Developer': validate_name,
        'Rating': validate_rating,
    }

    for column, validator in validations.items():
        value = row.get(column)
        if pd.isna(value):
            continue 
        if not validator(value):
            errors.append(f"Invalid value in column '{column}': {value}")

    try:
        if not pd.isna(row['Global_Sales']):
            na = row.get('NA_Sales', 0.0) or 0.0
            eu = row.get('EU_Sales', 0.0) or 0.0
            jp = row.get('JP_Sales', 0.0) or 0.0
            other = row.get('Other_Sales', 0.0) or 0.0
            global_sales = row['Global_Sales']

            expected_total = na + eu + jp + other

            if abs(global_sales - expected_total) > 0.03:
                errors.append(
                    f"Global_Sales mismatch: expected ~{expected_total:.2f}, got {global_sales:.2f}"
                )
    except Exception as e:
        errors.append(f"Error validating Global_Sales: {str(e)}")

    return errors

def validate_dataset(df):
    all_errors = {}
    for index, row in df.iterrows():
        errors = validate_row(row)
        if errors:
            all_errors[index] = errors
    return all_errors