import sqlite3
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def extract_sales_and_reviews(db_path: str = "../data/PerfectPetClub.db") -> pd.DataFrame:
    """
    Extracts sales transactions, payments, and product reviews.
    Uses Common Table Expressions (CTEs) to aggregate each domain before joining,
    preventing row duplication from Cartesian products.
    """
    logging.info(f"Connecting to database for sales and reviews: {db_path}")
    conn = sqlite3.connect(db_path)

    query = """
    WITH customer_spending AS (
        SELECT
            sa.customer_id,
            COUNT(sa.id) AS total_orders,
            ROUND(SUM(p.amount) / 100.0, 2) AS total_spend_gbp,
            ROUND(AVG(p.amount) / 100.0, 2) AS avg_order_value_gbp,
            MAX(sa.order_date) AS most_recent_order_date
        FROM sale sa
        INNER JOIN payment p ON sa.payment_id = p.id
        GROUP BY sa.customer_id
    ),
    customer_feedback AS (
        SELECT
            customer_id,
            COUNT(id) AS total_reviews_written,
            ROUND(AVG(star_rating), 2) AS avg_star_rating,
            SUM(CASE WHEN star_rating = 1 THEN 1 ELSE 0 END) AS one_star_reviews_count,
            SUM(CASE WHEN star_rating = 5 THEN 1 ELSE 0 END) AS five_star_reviews_count
        FROM product_review
        GROUP BY customer_id
    ),
    preferred_gateway AS (
        SELECT customer_id, payment_gateway AS primary_payment_gateway
        FROM (
            SELECT sa.customer_id, p.payment_gateway,
                   ROW_NUMBER() OVER(PARTITION BY sa.customer_id ORDER BY COUNT(*) DESC) as rn
            FROM sale sa
            JOIN payment p ON sa.payment_id = p.id
            GROUP BY sa.customer_id, p.payment_gateway
        ) WHERE rn = 1
    )
    SELECT
        cs.customer_id,
        cs.total_orders,
        cs.total_spend_gbp,
        cs.avg_order_value_gbp,
        cs.most_recent_order_date,
        pg.primary_payment_gateway,
        COALESCE(cf.total_reviews_written, 0) AS total_reviews_written,
        COALESCE(cf.avg_star_rating, 0.0) AS avg_star_rating,
        COALESCE(cf.one_star_reviews_count, 0) AS one_star_reviews_count,
        COALESCE(cf.five_star_reviews_count, 0) AS five_star_reviews_count
    FROM customer_spending cs
    LEFT JOIN customer_feedback cf ON cs.customer_id = cf.customer_id
    LEFT JOIN preferred_gateway pg ON cs.customer_id = pg.customer_id
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    logging.info(f"Successfully extracted financial and review metrics for {len(df)} customers.")
    return df


def validate_sales_review_data(df: pd.DataFrame) -> bool:
    """
    Performs automated Data Quality (DQ) checks on financial and review data:
    1. Spend non-negativity check
    2. Minimum order threshold check (total_orders >= 1)
    3. Review rating bound check (0 <= rating <= 5)
    """
    logging.info("Executing automated Data Quality checks on sales and reviews...")

    assert (df['total_spend_gbp'] >= 0).all(), "DQ FAIL: Negative spend detected."
    assert (df['total_orders'] >= 1).all(), "DQ FAIL: Found customers with 0 orders."
    assert ((df['avg_star_rating'] >= 0) & (df['avg_star_rating'] <= 5)).all(), "DQ FAIL: Star rating out of bounds."

    logging.info("All Sales/Review Data Quality checks PASSED successfully (3/3).")
    return True

def run_pipeline(db_path="../data/PerfectPetClub.db") -> pd.DataFrame:
    """
    Run extraction and validation.
    Return the validated DataFrame.
    """
    sales_df = extract_sales_and_reviews(db_path)
    validate_sales_review_data(sales_df)

    logging.info("Sales and Reviews pipeline completed successfully.")
    return sales_df


if __name__ == "__main__":
    sales_df = extract_sales_and_reviews(db_path="data/PerfectPetClub.db")
    validate_sales_review_data(sales_df)
    print("\nSample sales and reviews features:")
    print(sales_df[['customer_id', 'total_orders', 'total_spend_gbp', 'avg_star_rating', 'one_star_reviews_count']].head())
