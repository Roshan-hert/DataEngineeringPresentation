from crmEtl import extract_crm, transform_crm, load_crm,engineer_crm_features
from pathlib import Path

def run_crm_etl():
    # Define project directory
    project_dir = Path(__file__).resolve().parent.parent

    # Define input and output paths
    file_path = project_dir / "data/PerfectPetClub_CRM_report.csv"
    output_path = project_dir / "output"

    # Extract
    crm_df = extract_crm(file_path)

    # Transform
    cleaned_crm_df = transform_crm(crm_df)

    # Feature Engineering
    crm_features_df = engineer_crm_features(cleaned_crm_df)

    # Load
    load_crm(
        output_path / "cleaned_crm.parquet",
        output_path / "cleaned_crm.csv",
        cleaned_crm_df
    )

    # Save engineered customer features
    load_crm(
        output_path / "crm_features.parquet",
        output_path / "crm_features.csv",
        crm_features_df
    )

    print("CRM ETL and feature engineering completed successfully!")

    return crm_df,cleaned_crm_df,crm_features_df



