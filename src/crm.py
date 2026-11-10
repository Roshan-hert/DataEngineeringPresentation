# Import pandas to read and process CSV files
import pandas as pd

# Define a reusable function to extract CRM data
def extract_crm(file_path):

    # Read the CSV file into a Pandas DataFrame
    crm_df = pd.read_csv(file_path)

    # Return the extracted DataFrame
    return crm_df
    
def transform_crm(crm_df):

    # Create a copy to avoid modifying the original data
    df = crm_df.copy()

    # Remove duplicate rows
    df = df.drop_duplicates()

    # Remove unnecessary spaces from column names
    df.columns = df.columns.str.strip()

    # Remove rows that are completely empty
    df = df.dropna(how="all")

    # Convert empty strings to missing values
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    # Return the cleaned DataFrame
    return df

