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

    # Remove unnecessary spaces from column names
    df.columns = df.columns.str.strip()
    
    # Check required columns(Schema Validation)
    required = {"customer_id", "platform", "type", "recieved_datatime"}
    
    missing = required - set(df.columns)
    
    if missing:
        raise ValueError(f"Missing columns: {missing}")

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

    # Validation for missing or invalid dates
    missing_dates = df["received_datetime"].isna().sum()
    
    print(f"Missing or invalid dates: {missing_dates}")
    
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

def engineer_crm_features(df):

    # Group CRM records by customerid and calculate new features
    crm_features = df.groupby("customer_id").agg(
        # Count the total number of interactions per customer
        total_interactions=("customer_id", "size"),

        # Count how many interactions are complaints
        complaint_count=("type", lambda x: (x == "complaint").sum())

    ).reset_index()  # Keep customer_id as a normal column

    # Return the engineered dataset
    return crm_features

def load_crm(output_path,output_path2,df):

    
    # Create output directory only if it doesn't exist
    output_path.parent.mkdir(exist_ok=True)
    output_path2.parent.mkdir(exist_ok=True)

    # Save cleaned data as Parquet
    df.to_parquet(output_path, index=False)
    # Save cleaned data as Parquet
    df.to_csv(output_path2, index=False)


