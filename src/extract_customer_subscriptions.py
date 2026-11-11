"""
Module: extract_customer_subscriptions.py
Author: Rajeev Shahi (rs23aau@herts.ac.uk)
Role: Customer Profile, Subscription Modeling & Data Quality Lead
Component: PerfectPetClub.db Extraction & Transformation
"""

import sqlite3
import pandas as pd
import logging
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def extract_customer_subscriptions(db_path: str = "../data/PerfectPetClub.db") -> pd.DataFrame:
    """
    Extracts customer profiles and subscription information from the SQLite database.
    Performs joins across 'customer', 'subscription', and 'subscription_type'.
    """
    logging.info(f"Connecting to database: {db_path}")
    conn = sqlite3.connect(db_path)

    query = """
    SELECT
        c.id AS customer_id,
        c.name AS customer_name,
        c.email,
        c.county,
        c.postcode,
        s.id AS subscription_id,
        s.start_date AS sub_start_date,
        s.end_date AS sub_end_date,
        s.auto_renew,
        st.name AS subscription_tier,
        st.delivery_service
    FROM customer c
    INNER JOIN subscription s ON c.subscription_id = s.id
    INNER JOIN subscription_type st ON s.subscription_type_id = st.id
    """



    total_customers = pd.read_sql_query(
        "SELECT COUNT(*) AS total FROM customer", conn
    )["total"].iloc[0]


    df = pd.read_sql_query(query, conn)
    conn.close()


    if df.empty:
        raise ValueError("No customer records extracted.")


    if len(df) != total_customers:
        logging.warning(
            f"Customer count mismatch: original={total_customers}, "
            f"extracted={len(df)}"
        )
    else:
        logging.info(
            f"Customer count verified: {total_customers} records."
        )

    logging.info(f"Successfully extracted {len(df)} customer records.")
    return df



def validate_customer_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Performs automated Data Quality (DQ) checks on customer data:
    1. Null check on critical identifiers (ID, email, subscription ID)
    2. Primary key uniqueness check (customer_id)
    3. Binary target validity check (auto_renew in {0, 1})
    4. Date logic check (start_date <= end_date)
    """
    logging.info("Executing automated Data Quality checks on customer subscriptions...")
    results = {}

    # Check 1: Null values
    null_counts = df[['customer_id', 'email', 'subscription_id', 'auto_renew']].isnull().sum().to_dict()
    results['null_checks'] = null_counts
    assert all(count == 0 for count in null_counts.values()), f"DQ FAIL: Null values found: {null_counts}"

    # Check 2: Uniqueness
    is_unique = df['customer_id'].is_unique
    results['is_unique'] = is_unique
    assert is_unique, "DQ FAIL: Duplicate customer_id detected in dataset."

    # Check 3: Valid binary domain for auto_renew
    valid_auto_renew = df['auto_renew'].isin([0, 1]).all()
    results['valid_auto_renew'] = valid_auto_renew
    assert valid_auto_renew, "DQ FAIL: auto_renew contains invalid non-binary values."


    # Check 4: Date consistency
    start = pd.to_datetime(
        df['sub_start_date'],
        format='ISO8601',
        errors='coerce'
    )
    
    end = pd.to_datetime(
        df['sub_end_date'],
        format='ISO8601',
        errors='coerce'
    )

    # Check for missing or invalid dates
    valid_dates = start.notna().all() and end.notna().all()
    results['valid_dates'] = valid_dates
    assert valid_dates, "DQ FAIL: Missing or invalid subscription dates."

    # Check that end dates are not before start dates
    date_valid = (start <= end).all()
    results['date_logic_valid'] = date_valid
    assert date_valid, "DQ FAIL: Subscription end date is before start date."

    logging.info("All Customer Data Quality checks PASSED successfully (5/5).")
    return results



def run_pipeline(db_path: str = "../data/PerfectPetClub.db") -> pd.DataFrame:
    """
    Reusable end-to-end pipeline for customer subscription extraction,
    data quality validation, and feature engineering.

    Steps:
      1. Extract customer + subscription records from SQLite.
      2. Validate data quality (nulls, uniqueness, domain, date logic).
      3. Engineer churn features (is_churned, tenure, postcode district).

    Returns the transformed feature DataFrame.
    """
    logging.info("=== Starting customer subscription ETL pipeline ===")
    raw_df = extract_customer_subscriptions(db_path=db_path)
    validate_customer_data(raw_df)
    features_df = transform_customer_features(raw_df)
    logging.info("=== Customer subscription ETL pipeline complete ===")
    return features_df


def transform_customer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies feature engineering and GDPR compliance transformations:
    - Target Variable: 'is_churned' (1 if auto_renew == 0, else 0)
    - Feature: 'subscription_tenure_days'
    - Feature: 'postcode_district' (first part of UK postcode)
    - GDPR Compliance: Drops raw password_hash and full street address
    """
    logging.info("Applying feature engineering on customer subscriptions...")
    df_transformed = df.copy()

    # 1. Target variable for churn analysis
    df_transformed['is_churned'] = (df_transformed['auto_renew'] == 0).astype(int)

    # 2. Tenure duration in days
    start = pd.to_datetime(df_transformed['sub_start_date'])
    end = pd.to_datetime(df_transformed['sub_end_date'])
    df_transformed['subscription_tenure_days'] = (end - start).dt.days

    # 3. Geographic feature: extract postcode outward code (e.g., 'SL6' from 'SL6 1CD')
    df_transformed['postcode_district'] = df_transformed['postcode'].str.split(' ').str[0]

    # Calculate churn summary
    churn_count = df_transformed['is_churned'].sum()
    total = len(df_transformed)
    churn_pct = (churn_count / total) * 100
    logging.info(f"Target Engineered: {churn_count}/{total} customers churned ({churn_pct:.2f}%).")

    return df_transformed


if __name__ == "__main__":
    raw_df = extract_customer_subscriptions(db_path="data/PerfectPetClub.db")
    validate_customer_data(raw_df)
    features_df = transform_customer_features(raw_df)
    print("\nSample transformed customer data:")
    print(features_df[['customer_id', 'customer_name', 'subscription_tier', 'subscription_tenure_days', 'is_churned']].head())
