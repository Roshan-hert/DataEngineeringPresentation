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
      
    # Correct the datetime column name
    df = df.rename(columns={
        "recieved_datatime": "received_datetime"
    })

    # Convert to datetime
    df["received_datetime"] = pd.to_datetime(
        df["received_datetime"], errors="coerce"
    )

    # Clean platform values
    df["platform"] = (
        df["platform"]
        .str.replace(r"[\[\]']", "", regex=True)
        .str.strip()
        .str.lower()
    )

    # Standardise interaction types
    df["type"] = df["type"].str.strip().str.lower()

    # Remove records without customer IDs
    df = df.dropna(subset=["customer_id"])


    # Return the cleaned DataFrame
    return df

def load_crm(output_path,output_path2,df):

    
    # Create output directory only if it doesn't exist
    output_path.parent.mkdir(exist_ok=True)
    output_path2.parent.mkdir(exist_ok=True)

    # Save cleaned data as Parquet
    df.to_parquet(output_path, index=False)
    # Save cleaned data as Parquet
    df.to_csv(output_path2, index=False)

    print(f"Cleaned data saved as csv and parquet to: {output_path}")
    print("ETL completed successfully!")