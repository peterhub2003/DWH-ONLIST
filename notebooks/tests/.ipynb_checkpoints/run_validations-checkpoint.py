import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging
from pathlib import Path
import time
from decimal import Decimal
from tabulate import tabulate 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

DB_USER = os.getenv('POSTGRES_USER')
DB_PASSWORD = os.getenv('POSTGRES_PASSWORD')
DB_HOST = 'postgres' # Kết nối từ máy host
DB_PORT = '5432'
DB_NAME = os.getenv('POSTGRES_DB')

DATABASE_URI = f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
try:
    engine = create_engine(DATABASE_URI)
    with engine.connect() as connection:
        logging.info("Kết nối database thành công!")
except Exception as e:
    logging.error(f"Không thể kết nối database: {e}")
    exit(1)


validation_checks = {
    # === 1. Row Count Validation ===
    "count_fact_vs_staging_orders": {
        "description": "So sánh số lượng order_id trong Fact với Staging (có items)",
        "query_dwh": "SELECT COUNT(DISTINCT order_id) FROM dwh.fact_order_delivery;",
        "query_staging": "SELECT COUNT(DISTINCT o.order_id) FROM staging.stg_orders o JOIN staging.stg_order_items i ON o.order_id = i.order_id;",
        "type": "compare_count"
    },
    "count_dim_customer_vs_staging": {
        "description": "So sánh số lượng Customer hiện hành trong Dim với Staging (unique id)",
        "query_dwh": "SELECT COUNT(*) FROM dwh.dim_customer WHERE is_current = TRUE;",
        "query_staging": "SELECT COUNT(DISTINCT customer_id) FROM staging.stg_customers;",
        "type": "compare_count"
    },
    "count_dim_seller_vs_staging": {
        "description": "So sánh số lượng Seller hiện hành trong Dim với Staging (unique id)",
        "query_dwh": "SELECT COUNT(*) FROM dwh.dim_seller WHERE is_current = TRUE;",
        "query_staging": "SELECT COUNT(DISTINCT seller_id) FROM staging.stg_sellers;",
        "type": "compare_count"
    },
    # === 2. Aggregate Value Validation ===
    "agg_fact_vs_staging_items": {
        "description": "So sánh tổng giá trị/số lượng items (Fact vs Staging)",

        "query_dwh": """
            SELECT
                COALESCE(SUM(total_price), 0) AS total_price,
                COALESCE(SUM(total_freight_value), 0) AS total_freight_value,
                COALESCE(SUM(item_count), 0) AS item_count
            FROM dwh.fact_order_delivery;
        """,
        # Lấy tổng từ Staging (chỉ cho các order có trong fact)
        "query_staging": """
            SELECT
                COALESCE(SUM(CAST(i.price AS NUMERIC)), 0) AS total_price,
                COALESCE(SUM(CAST(i.freight_value AS NUMERIC)), 0) AS total_freight_value,
                COALESCE(COUNT(*), 0) AS item_count
            FROM staging.stg_order_items i
            WHERE i.order_id IN (SELECT DISTINCT fd.order_id FROM dwh.fact_order_delivery fd);
        """,
        "type": "compare_aggregates",
        "tolerance": 0.01 # Dung sai cho so sánh số thực
    },
    # === 3. Key Integrity Validation ===
    "key_null_purchase_date": {
        "description": "Kiểm tra NULL purchase_date_key trong Fact (không nên có)",
        "query": "SELECT COUNT(*) FROM dwh.fact_order_delivery WHERE purchase_date_key IS NULL;",
        "type": "expect_zero"
    },
    "key_null_estimated_date": {
        "description": "Kiểm tra NULL estimated_delivery_date_key trong Fact (không nên có)",
        "query": "SELECT COUNT(*) FROM dwh.fact_order_delivery WHERE estimated_delivery_date_key IS NULL;",
        "type": "expect_zero"
    },
    "key_null_customer": {
        "description": "Kiểm tra NULL customer_key trong Fact (chỉ chấp nhận nếu không dùng -1)",
        "query": "SELECT COUNT(*) FROM dwh.fact_order_delivery WHERE customer_key IS NULL;",
        "type": "expect_zero_or_warning"
    },
    "key_unknown_customer": {
        "description": "Kiểm tra customer_key = -1 (nếu dùng)",
        "query": "SELECT COUNT(*) FROM dwh.fact_order_delivery WHERE customer_key = -1;",
        "type": "report_count"
    },
     "key_null_seller": {
        "description": "Kiểm tra NULL seller_key trong Fact (chỉ chấp nhận nếu không dùng -1)",
        "query": "SELECT COUNT(*) FROM dwh.fact_order_delivery WHERE seller_key IS NULL;",
        "type": "expect_zero_or_warning" 
    },
    "key_unknown_seller": {
        "description": "Kiểm tra seller_key = -1 (nếu dùng)",
        "query": "SELECT COUNT(*) FROM dwh.fact_order_delivery WHERE seller_key = -1;",
        "type": "report_count"
    },
    "key_orphan_customer": {
        "description": "Kiểm tra khóa ngoại Customer không tồn tại trong Dim (trừ -1)",
        "query": """
            SELECT COUNT(fod.order_delivery_key)
            FROM dwh.fact_order_delivery fod
            LEFT JOIN dwh.dim_customer dc ON fod.customer_key = dc.customer_key
            WHERE dc.customer_key IS NULL AND fod.customer_key <> -1 AND fod.customer_key IS NOT NULL;
        """,
        "type": "expect_zero"
    },
    "key_orphan_seller": {
        "description": "Kiểm tra khóa ngoại Seller không tồn tại trong Dim (trừ -1)",
        "query": """
            SELECT COUNT(fod.order_delivery_key)
            FROM dwh.fact_order_delivery fod
            LEFT JOIN dwh.dim_seller ds ON fod.seller_key = ds.seller_key
            WHERE ds.seller_key IS NULL AND fod.seller_key <> -1 AND fod.seller_key IS NOT NULL;
        """,
        "type": "expect_zero"
    },
    "key_orphan_approved_date": {
        "description": "Kiểm tra khóa ngoại Approved Date không tồn tại trong Dim Date (trừ NULL/-1)",
         "query": """
            SELECT COUNT(fod.order_delivery_key)
            FROM dwh.fact_order_delivery fod
            LEFT JOIN dwh.dim_date dd ON fod.approved_date_key = dd.date_key
            WHERE dd.date_key IS NULL AND fod.approved_date_key IS NOT NULL AND fod.approved_date_key <> -1;
        """,
        "type": "expect_zero"
    },


    # === 4. Data Consistency / Duplicates ===
    "duplicate_current_customers": {
        "description": "Kiểm tra khách hàng hiện hành bị trùng lặp (theo customer_id)",
        "query": "SELECT customer_id, COUNT(*) FROM dwh.dim_customer WHERE is_current = TRUE GROUP BY customer_id HAVING COUNT(*) > 1;",
        "type": "expect_empty_dataframe"
    },
    "duplicate_current_sellers": {
        "description": "Kiểm tra người bán hiện hành bị trùng lặp (theo seller_id)",
        "query": "SELECT seller_id, COUNT(*) FROM dwh.dim_seller WHERE is_current = TRUE GROUP BY seller_id HAVING COUNT(*) > 1;",
        "type": "expect_empty_dataframe"
    },
     "duplicate_fact_orders": {
        "description": "Kiểm tra order_id bị trùng lặp trong Fact",
        "query": "SELECT order_id, COUNT(*) FROM dwh.fact_order_delivery GROUP BY order_id HAVING COUNT(*) > 1;",
        "type": "expect_empty_dataframe"
    },
    # === 5. Business Rule Validation ===
    "rule_negative_delivery_time": {
        "description": "Kiểm tra delivery_time_days < 0 (không nên có)",
        "query": "SELECT order_id, delivery_time_days FROM dwh.fact_order_delivery WHERE delivery_time_days < 0;",
        "type": "expect_empty_dataframe"
    },
     "rule_negative_processing_hours": {
        "description": "Kiểm tra seller_processing_hours < 0 (không nên có)",
        "query": "SELECT order_id, seller_processing_hours FROM dwh.fact_order_delivery WHERE seller_processing_hours < 0;",
        "type": "expect_empty_dataframe"
    },
    "rule_negative_shipping_hours": {
        "description": "Kiểm tra carrier_shipping_hours < 0",
        "query": "SELECT order_id, carrier_shipping_hours FROM dwh.fact_order_delivery WHERE carrier_shipping_hours < 0;",
        "type": "expect_empty_dataframe" # Hoặc report_dataframe nếu chấp nhận vài trường hợp
    },
     "rule_negative_approve_hours": {
        "description": "Kiểm tra time_to_approve_hours < 0",
        "query": "SELECT order_id, time_to_approve_hours FROM dwh.fact_order_delivery WHERE time_to_approve_hours < 0;",
        "type": "expect_empty_dataframe" # Hoặc report_dataframe
    },
    "rule_approved_before_purchase": {
        "description": "Kiểm tra Approved Date < Purchase Date",
        "query": """
            SELECT fod.order_id, purchase_dt.full_date as purchase, approved_dt.full_date as approved
            FROM dwh.fact_order_delivery fod
            JOIN dwh.dim_date purchase_dt ON fod.purchase_date_key = purchase_dt.date_key
            JOIN dwh.dim_date approved_dt ON fod.approved_date_key = approved_dt.date_key
            WHERE approved_dt.full_date < purchase_dt.full_date;
        """,
        "type": "expect_empty_dataframe"
    },
    "rule_carrier_before_approved": {
        "description": "Kiểm tra Delivered Carrier Date < Approved Date",
        "query": """
            SELECT fod.order_id, approved_dt.full_date as approved, carrier_dt.full_date as carrier
            FROM dwh.fact_order_delivery fod
            JOIN dwh.dim_date approved_dt ON fod.approved_date_key = approved_dt.date_key
            JOIN dwh.dim_date carrier_dt ON fod.delivered_carrier_date_key = carrier_dt.date_key
            WHERE carrier_dt.full_date < approved_dt.full_date;
        """,
        "type": "expect_empty_dataframe"
    },

    "rule_negative_total_price": {
        "description": "Kiểm tra đơn hàng có total_price < 0",
        "query": "SELECT order_id, total_price FROM dwh.fact_order_delivery WHERE total_price < 0;",
        "type": "expect_empty_dataframe"
    },
     "rule_distinct_order_statuses": {
        "description": "Liệt kê các order_status duy nhất trong Fact",
        "query": "SELECT DISTINCT order_status FROM dwh.fact_order_delivery ORDER BY order_status;",
        "type": "report_dataframe" # Chỉ báo cáo, không pass/fail
    },
     # === 6. NULL Value Checks ===
     "null_order_status": {
        "description": "Kiểm tra order_status bị NULL",
        "query": "SELECT COUNT(*) FROM dwh.fact_order_delivery WHERE order_status IS NULL;",
        "type": "expect_zero"
     },
     "null_item_count": {
        "description": "Kiểm tra item_count bị NULL hoặc <= 0",
        "query": "SELECT COUNT(*) FROM dwh.fact_order_delivery WHERE item_count IS NULL OR item_count <= 0;",
        "type": "expect_zero"
     },
      "null_total_price": {
        "description": "Kiểm tra total_price bị NULL",
        "query": "SELECT COUNT(*) FROM dwh.fact_order_delivery WHERE total_price IS NULL;",
        "type": "expect_zero"
     },
      "null_total_freight": {
        "description": "Kiểm tra total_freight_value bị NULL",
        "query": "SELECT COUNT(*) FROM dwh.fact_order_delivery WHERE total_freight_value IS NULL;",
        "type": "expect_zero"
     }

}

def run_validation(check_name, check_config, db_engine):
    """Chạy một kiểm tra validation và trả về kết quả."""
    logging.info(f"Running check: {check_name} - {check_config['description']}")
    start_time = time.time()
    status = "FAIL" 
    message = ""
    details = None 

    try:
        check_type = check_config["type"]
        query = check_config.get("query")
        query_dwh = check_config.get("query_dwh")
        query_staging = check_config.get("query_staging")
        tolerance = check_config.get("tolerance", 0.0)

        if check_type == "compare_count":
            count_dwh = pd.read_sql(query_dwh, db_engine).iloc[0, 0]
            count_staging = pd.read_sql(query_staging, db_engine).iloc[0, 0]
            if count_dwh == count_staging:
                status = "PASS"
                message = f"Counts match: {count_dwh}"
            else:
                message = f"Count mismatch: DWH={count_dwh}, Staging={count_staging}"
            details = {"dwh": count_dwh, "staging": count_staging}

        elif check_type == "compare_aggregates":
            df_dwh = pd.read_sql(query_dwh, db_engine)
            df_staging = pd.read_sql(query_staging, db_engine)
            match = True
            mismatches = []
            # So sánh từng cột aggregate
            for col in df_dwh.columns:
                val_dwh = df_dwh.iloc[0][col]
                val_staging = df_staging.iloc[0][col]
                dec_dwh = Decimal(str(val_dwh)) if val_dwh is not None else Decimal(0)
                dec_staging = Decimal(str(val_staging)) if val_staging is not None else Decimal(0)
                diff = abs(dec_dwh - dec_staging)
                if diff > Decimal(str(tolerance)):
                    match = False
                    mismatches.append(f"{col} (DWH: {dec_dwh}, Staging: {dec_staging}, Diff: {diff})")

            if match:
                status = "PASS"
                message = "Aggregates match within tolerance."
            else:
                message = f"Aggregate mismatch: {'; '.join(mismatches)}"
            details = {"dwh": df_dwh.to_dict('records')[0], "staging": df_staging.to_dict('records')[0]}


        elif check_type in ["expect_zero", "expect_zero_or_warning"]:
            result_count = pd.read_sql(query, db_engine).iloc[0, 0]
            if result_count == 0:
                status = "PASS"
                message = "Count is zero as expected."
            else:
                message = f"Found {result_count} records, expected zero."
                if check_type == "expect_zero_or_warning":
                     status = "WARNING" 
                try:
                    df_details = pd.read_sql(query.replace("COUNT(*)", "*", 1) + " LIMIT 5", db_engine)
                    details = df_details
                except Exception: 
                    details = f"Count: {result_count}"


        elif check_type == "expect_empty_dataframe":
            df_result = pd.read_sql(query, db_engine)
            if df_result.empty:
                status = "PASS"
                message = "No records found, as expected."
            else:
                message = f"Found {len(df_result)} unexpected records."
                details = df_result 

        elif check_type == "report_count":
            result_count = pd.read_sql(query, db_engine).iloc[0, 0]
            status = "INFO"
            message = f"Reported count: {result_count}"
            details = result_count

        elif check_type == "report_dataframe":
            df_result = pd.read_sql(query, db_engine)
            status = "INFO"
            message = f"Reporting {len(df_result)} records found."
            details = df_result

        else:
            status = "ERROR"
            message = f"Unknown check type: {check_type}"

    except Exception as e:
        status = "ERROR"
        message = f"Error executing check: {e}"
        logging.exception(f"Exception occurred during check '{check_name}'")

    end_time = time.time()
    duration = end_time - start_time
    logging.info(f"Check '{check_name}' completed in {duration:.2f}s with status: {status}")

    return {
        "name": check_name,
        "description": check_config['description'],
        "status": status,
        "message": message,
        "details": details,
        "duration": duration
    }

if __name__ == "__main__":
    logging.info("=== STARTING DATA VALIDATION RUN ===")
    all_results = []
    overall_status = "PASS"

    for check_name, check_config in validation_checks.items():
        result = run_validation(check_name, check_config, engine)
        all_results.append(result)
        if result["status"] == "FAIL":
            overall_status = "FAIL"
        elif result["status"] == "ERROR":
            overall_status = "ERROR"
        elif result["status"] == "WARNING" and overall_status == "PASS":
            overall_status = "WARNING"

    logging.info("\n=== VALIDATION SUMMARY ===")
    passed_count = sum(1 for r in all_results if r['status'] == 'PASS')
    failed_count = sum(1 for r in all_results if r['status'] == 'FAIL')
    warning_count = sum(1 for r in all_results if r['status'] == 'WARNING')
    error_count = sum(1 for r in all_results if r['status'] == 'ERROR')
    info_count = sum(1 for r in all_results if r['status'] == 'INFO')

    print(f"Overall Status: {overall_status}")
    print(f"Total Checks: {len(all_results)}")
    print(f"  Passed:  {passed_count}")
    print(f"  Failed:  {failed_count}")
    print(f"  Warning: {warning_count}")
    print(f"  Errors:  {error_count}")
    print(f"  Info:    {info_count}")

    if failed_count > 0 or error_count > 0 or warning_count > 0:
        print("\n--- FAILED / WARNING / ERROR DETAILS ---")
        for result in all_results:
            if result['status'] in ['FAIL', 'WARNING', 'ERROR']:
                print(f"\nCheck: {result['name']} ({result['status']})")
                print(f"  Desc: {result['description']}")
                print(f"  Msg:  {result['message']}")
                if result['details'] is not None:
                     if isinstance(result['details'], pd.DataFrame) and not result['details'].empty:
                         print("  Details (first 5 rows):")
                         print(tabulate(result['details'].head(), headers='keys', tablefmt='psql', showindex=False))
                     elif not isinstance(result['details'], pd.DataFrame):
                         print(f"  Details: {result['details']}")
                print("-" * 20)

    logging.info("=== DATA VALIDATION RUN FINISHED ===")

    if overall_status in ["FAIL", "ERROR"]:
        exit(1)