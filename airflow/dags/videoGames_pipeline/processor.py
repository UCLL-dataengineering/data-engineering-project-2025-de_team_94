import pandas as pd


def process_data(df):
    mandatory_columns = [
        'Name', 'Platform', 'Year_of_Release', 'Genre', 'Publisher', 'NA_Sales',
        'EU_Sales', 'JP_Sales', 'Other_Sales', 'Global_Sales']
    
    # Drop rows with missing values in mandatory columns
    df = df.dropna(subset=mandatory_columns)

    # Remove duplicates based on mandatory columns
    df_cleaned = df.drop_duplicates(subset=mandatory_columns, keep='first')

    # get the best selling region and the ratio for the best selling region
    def get_best_selling_region_and_ratio(row):
        # Define sales per region
        regions = {
            "North-America": row["NA_Sales"],
            "Europe": row["EU_Sales"],
            "Japan": row["JP_Sales"],
            "Other": row["Other_Sales"]
        }
        
        best_region = max(regions, key=regions.get)
        
        best_region_sales = regions[best_region]
        
        if row["Global_Sales"] != 0:
            sales_ratio = best_region_sales / row["Global_Sales"]
        else:
            sales_ratio = 0  
        
        return pd.Series([best_region, sales_ratio], index=["Best_Selling_Region", "Best_Selling_Region_Sales_Ratio"])

    # apply the function to each row to get the best selling region and ratio
    df_cleaned[['Best_Selling_Region', 'Best_Selling_Region_Sales_Ratio']] = df_cleaned.apply(get_best_selling_region_and_ratio, axis=1)

    # Get the contribution of this game to the total sales of its platform
    platform_sales_total = df_cleaned.groupby("Platform")["Global_Sales"].sum()
    df_cleaned["Platform_Sales_Contribution"] = df_cleaned.apply(
        lambda row: row["Global_Sales"] / platform_sales_total[row["Platform"]] if platform_sales_total[row["Platform"]] != 0 else 0,
        axis=1
    )
    return df_cleaned