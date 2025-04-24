import datetime

import numpy as np
import pandas as pd


def process_data(df):

    # Remove NaN values from mandatory columns
    mandatory_columns = ['Parcel ID', 'Land Use', 'Property Address', 'Property City', 'Sale Date', 'Sale Price', 'Legal Reference', 'Sold As Vacant',
                        'Multiple Parcels Involved in Sale', 'Acreage', 'Neighborhood', 'Land Value', 'Building Value', 'Total Value', 'Finished Area', 'Year Built', 'Bedrooms', 'Full Bath', 'Half Bath']
    df = df.dropna(subset=mandatory_columns)
    
    # Drop certain columns
    columns_to_remove = ['image', 'Sold As Vacant', 'Multiple Parcels Involved in Sale']
    df = df.drop(columns=columns_to_remove)


    # Price per square foot
    df['Price per square foot'] = df['Sale Price'] / df['Finished Area']  
    
    # Age of property
    df['Age of property'] = datetime.datetime.today().year - df['Year Built']
    
    # Sale year and sale month
    df['Sale Year'] = pd.to_datetime(df['Sale Date']).dt.year
    df['Sale Month'] = pd.to_datetime(df['Sale Date']).dt.month
    
    # Land-to-building value ratio
    df['Land-to-Building Value Ratio'] = df['Land Value'] / df['Building Value']
    
    # Sale price category
    def categorize_sale_price(price):
        if price < 100000:
            return 'Low'
        elif 100000 <= price <= 300000:
            return 'Medium'
        else:
            return 'High'
    
    df['Sale Price Category'] = df['Sale Price'].apply(categorize_sale_price)
    
    # Family Name and First Name of owner
    def extract_name(owner_name):
        if pd.isna(owner_name):
            return np.nan, np.nan
        name_parts = owner_name.split(', ')
        if len(name_parts) == 2:
            return name_parts[0], name_parts[1]
        return owner_name, np.nan 

    df['Family Name'], df['First Name'] = zip(*df['Owner Name'].apply(extract_name))
    
    return df