
"""
Perfect Pet Club ETL module.

This file contains reusable functions only. It does not execute the pipeline
when imported. The notebook loads it under a unique module name to avoid
conflicts with unrelated packages/modules also named `server`.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pandas as pd

REQUIRED_COLUMNS = ["datetime", "session_id", "ip", "login", "user_agent"]


def extract_data(file_path: str | Path) -> pd.DataFrame:
    """Read the server-log CSV and validate its required columns."""
    file_path = Path(file_path)

    if not file_path.is_file():
        raise FileNotFoundError(f"Input CSV not found: {file_path}")
    if file_path.suffix.lower() != ".csv":
        raise ValueError(f"Expected a CSV file, got: {file_path.suffix}")

    raw_data = pd.read_csv(file_path, dtype="string", low_memory=False)

    missing = [column for column in REQUIRED_COLUMNS if column not in raw_data.columns]
    if missing:
        raise ValueError(f"Missing required CSV columns: {', '.join(missing)}")

    print(f"Extracted {len(raw_data):,} rows from {file_path.name}")
    return raw_data


def normalise_device(user_agent: Any) -> str:
    """Classify a user-agent string as mobile, tablet, desktop or other."""
    if pd.isna(user_agent):
        return "other"

    value = str(user_agent).lower()

    if any(token in value for token in ("ipad", "tablet", "kindle", "silk")):
        return "tablet"
    if any(
        token in value
        for token in ("mobile", "iphone", "ipod", "android", "windows phone")
    ):
        return "mobile"
    if any(
        token in value
        for token in ("windows nt", "macintosh", "mac os", "x11", "linux", "cros")
    ):
        return "desktop"

    return "other"


def _safe_ratio(numerator: pd.Series, denominator: pd.Series) -> pd.Series:
    """Return a numeric ratio, using zero when the denominator is zero."""
    denominator = denominator.replace(0, pd.NA)
    return numerator.div(denominator).fillna(0.0)


def transform_data(raw_data: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Clean event records and aggregate them to one row per customer.

    Returns exactly THREE objects:
      1. clean_data: cleaned event-level records
      2. customer_features: engineered features, one row per customer
      3. quality_report: dictionary of ETL quality metrics

    The supplied server log does not identify confirmed subscription
    cancellations or renewals. Consequently churn_label remains nullable.
    """
    if not isinstance(raw_data, pd.DataFrame):
        raise TypeError(
            "transform_data expects a pandas DataFrame; "
            f"received {type(raw_data).__name__}."
        )

    missing = [column for column in REQUIRED_COLUMNS if column not in raw_data.columns]
    if missing:
        raise ValueError(f"Missing required dataframe columns: {', '.join(missing)}")

    data = raw_data.copy()
    input_rows = len(data)

    # Standardise text columns and login identifiers.
    for column in ("session_id", "ip", "login", "user_agent"):
        data[column] = data[column].astype("string").str.strip()

    data["login"] = data["login"].str.lower()
    data["event_time"] = pd.to_datetime(data["datetime"], errors="coerce")

    invalid_timestamp_rows = int(data["event_time"].isna().sum())
    missing_or_blank_login_rows = int(
        data["login"].isna().sum()
        + data["login"].fillna("").eq("").sum()
    )

    # Discard rows without a usable event timestamp or customer login.
    data = data.dropna(subset=["event_time", "login"])
    data = data.loc[data["login"].ne("")].copy()
    rows_after_required_cleaning = len(data)

    # Remove exact duplicate log events.
    data = data.drop_duplicates(
        subset=["datetime", "session_id", "ip", "login", "user_agent"]
    ).copy()
    duplicate_rows_removed = rows_after_required_cleaning - len(data)

    if data.empty:
        raise ValueError("No usable records remain after timestamp/login cleaning.")

    #storing cleaned data
    cleaned_data = data

    # Deterministic pseudonymous customer key (not guaranteed anonymisation).
    data["customer_id"] = data["login"].map(
        lambda login: hashlib.sha256(str(login).encode("utf-8")).hexdigest()
    )

    # Event-level behavioural features.
    data["device_type"] = data["user_agent"].map(normalise_device)
    data["event_date"] = data["event_time"].dt.date
    data["is_weekend"] = data["event_time"].dt.dayofweek >= 5

    dataset_end = data["event_time"].max()
    data["is_recent_7d"] = data["event_time"] >= (
        dataset_end - pd.Timedelta(days=7)
    )
    data["is_recent_30d"] = data["event_time"] >= (
        dataset_end - pd.Timedelta(days=30)
    )

    # One aggregated row per customer.
    customer_features = data.groupby("customer_id").agg(
        first_activity=("event_time", "min"),
        last_activity=("event_time", "max"),
        total_events=("event_time", "size"),
        unique_sessions=("session_id", "nunique"),
        unique_ips=("ip", "nunique"),
        active_days=("event_date", "nunique"),
        weekend_events=("is_weekend", "sum"),
        events_last_7d=("is_recent_7d", "sum"),
        events_last_30d=("is_recent_30d", "sum"),
    ).reset_index()

    # Device counts for each customer.
    device_counts = data.pivot_table(
        index="customer_id",
        columns="device_type",
        values="event_time",
        aggfunc="count",
        fill_value=0,
    ).rename_axis(columns=None).reset_index()

    for device in ("mobile", "desktop", "tablet", "other"):
        if device not in device_counts.columns:
            device_counts[device] = 0

    device_counts = device_counts.rename(
        columns={
            "mobile": "mobile_events",
            "desktop": "desktop_events",
            "tablet": "tablet_events",
            "other": "other_device_events",
        }
    )

    device_columns = [
        "customer_id",
        "mobile_events",
        "desktop_events",
        "tablet_events",
        "other_device_events",
    ]
    customer_features = customer_features.merge(
        device_counts[device_columns],
        on="customer_id",
        how="left",
        validate="one_to_one",
    )

    # Recency and behavioural ratios.
    customer_features["days_since_last_activity"] = (
        dataset_end - customer_features["last_activity"]
    ).dt.days

    customer_features["weekend_event_ratio"] = _safe_ratio(
        customer_features["weekend_events"], customer_features["total_events"]
    )
    customer_features["mobile_event_ratio"] = _safe_ratio(
        customer_features["mobile_events"], customer_features["total_events"]
    )
    customer_features["desktop_event_ratio"] = _safe_ratio(
        customer_features["desktop_events"], customer_features["total_events"]
    )
    customer_features["tablet_event_ratio"] = _safe_ratio(
        customer_features["tablet_events"], customer_features["total_events"]
    )

    count_columns = [
        "mobile_events",
        "desktop_events",
        "tablet_events",
        "other_device_events",
    ]
    device_names = ["mobile", "desktop", "tablet", "other"]
    customer_features["dominant_device"] = (
        customer_features[count_columns]
        .idxmax(axis=1)
        .map(dict(zip(count_columns, device_names)))
    )

    # No verified churn outcome exists in the input server log.
    customer_features["churn_label"] = pd.Series(
        pd.NA, index=customer_features.index, dtype="Int64"
    )

    preferred_columns = [
        "customer_id",
        "first_activity",
        "last_activity",
        "days_since_last_activity",
        "total_events",
        "unique_sessions",
        "unique_ips",
        "active_days",
        "events_last_7d",
        "events_last_30d",
        "weekend_event_ratio",
        "mobile_event_ratio",
        "desktop_event_ratio",
        "tablet_event_ratio",
        "dominant_device",
        "churn_label",
    ]
    other_columns = [
        column for column in customer_features.columns
        if column not in preferred_columns
    ]
    customer_features = customer_features[preferred_columns + other_columns]

    quality_report = {
        "input_rows": int(input_rows),
        "invalid_timestamp_rows": invalid_timestamp_rows,
        "missing_or_blank_login_rows": missing_or_blank_login_rows,
        "rows_after_required_cleaning": int(rows_after_required_cleaning),
        "duplicate_rows_removed": int(duplicate_rows_removed),
        "clean_event_rows": int(len(data)),
        "customer_rows": int(len(customer_features)),
        "unique_customers": int(customer_features["customer_id"].nunique()),
        "dataset_latest_timestamp": str(dataset_end),
        "churn_label_available": False,
        "churn_label_note": (
            "No confirmed cancellation/renewal field exists in the server log. "
            "Join subscription outcome data before supervised churn modelling."
        ),
    }

    # Validate the customer-level output.
    assert customer_features["customer_id"].is_unique
    assert customer_features["total_events"].ge(1).all()
    assert customer_features["churn_label"].isna().all()

    print(
        f"Transformed {len(data):,} clean events into "
        f"{len(customer_features):,} customer records"
    )

    # Return cleaned event data, customer-level engineered features, and the ETL quality report.
    return cleaned_data, customer_features, quality_report


def load_data(data: pd.DataFrame, output_path: str | Path) -> Path:
    """Save the customer-level dataframe as Snappy-compressed Parquet."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if data.empty:
        raise ValueError("Cannot save an empty customer dataset.")
    if "customer_id" not in data.columns:
        raise ValueError("The dataframe must contain customer_id.")
    if not data["customer_id"].is_unique:
        raise ValueError("customer_id must be unique before saving.")

    data.to_parquet(
        output_path,
        engine="pyarrow",
        compression="snappy",
        index=False,
    )

    print(f"Saved {len(data):,} customer records to {output_path}")
    return output_path
