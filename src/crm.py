# Import pandas to read and process CSV files
import pandas as pd

# Define a reusable function to extract CRM data
def extract_crm(file_path):

    # Read the CSV file into a Pandas DataFrame
    crm_df = pd.read_csv(file_path)

    # Return the extracted DataFrame
    return crm_df