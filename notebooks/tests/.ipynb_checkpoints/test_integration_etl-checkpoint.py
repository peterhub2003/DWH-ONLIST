import pytest
from sqlalchemy import text
import pandas as pd
import sys


# from .etl.main_etl import extract_load_to_staging, transform_and_load_dimensions, transform_and_load_fact

# Sử dụng các fixtures từ conftest.py: db_engine, setup_test_database, sample_data_dir, sample_csv_files_map

def test_staging_load(setup_test_database, db_engine, sample_data_dir, sample_csv_files_map):
    """Kiểm tra việc load dữ liệu mẫu vào staging."""
    # Chạy bước Extract & Load Staging với dữ liệu mẫu
    extract_load_to_staging(sample_csv_files_map, sample_data_dir, db_engine)

    # Kiểm tra kết quả trong staging
    with db_engine.connect() as connection:
        # Kiểm tra số lượng dòng trong bảng staging orders mẫu
        result = connection.execute(text("SELECT COUNT(*) FROM staging.stg_orders;")).scalar()
        # Thay EXPECTED_ORDER_COUNT bằng số dòng trong file sample_orders.csv
        assert result > 0 # Hoặc assert result == EXPECTED_ORDER_COUNT

        # Kiểm tra một giá trị cụ thể đã biết trong dữ liệu mẫu
        order_status = connection.execute(
            text("SELECT order_status FROM staging.stg_orders WHERE order_id = :order_id;"),
            {'order_id': 'KNOWN_SAMPLE_ORDER_ID'} # Thay bằng ID có trong sample_orders.csv
        ).scalar()
        assert order_status == 'KNOWN_STATUS' # Thay bằng status tương ứng

        # Thêm các kiểm tra khác cho các bảng staging khác nếu cần

def test_dimension_load(setup_test_database, db_engine, sample_data_dir, sample_csv_files_map):
    """Kiểm tra việc load dữ liệu mẫu vào dimensions."""
    # Chạy bước 1: Load staging trước
    extract_load_to_staging(sample_csv_files_map, sample_data_dir, db_engine)
    # Chạy bước 2: Load Dimensions
    transform_and_load_dimensions(db_engine)

    # Kiểm tra kết quả trong dimensions
    with db_engine.connect() as connection:
        # Kiểm tra số lượng khách hàng (phải khớp với số unique customer_id trong sample)
        cust_count = connection.execute(text("SELECT COUNT(*) FROM dwh.dim_customer WHERE is_current = TRUE;")).scalar()
        assert cust_count > 0 # Hoặc assert cust_count == EXPECTED_UNIQUE_CUSTOMERS

        # Kiểm tra chuẩn hóa city/state cho một khách hàng cụ thể
        cust_data = connection.execute(
            text("SELECT customer_city, customer_state FROM dwh.dim_customer WHERE customer_id = :cust_id AND is_current = TRUE;"),
            {'cust_id': 'KNOWN_SAMPLE_CUSTOMER_ID'} # ID khách hàng trong sample
        ).first() # Lấy dòng đầu tiên (tuple)
        assert cust_data is not None
        assert cust_data[0] == 'expected_standardized_city' # Thay bằng city đã chuẩn hóa mong đợi
        assert cust_data[1] == 'EXPECTED_STATE' # Thay bằng state đã chuẩn hóa mong đợi

        # Thêm các kiểm tra tương tự cho dim_seller

# Đánh dấu test này phụ thuộc vào test dimension load (nếu dùng pytest-dependency)
# @pytest.mark.dependency(depends=["test_dimension_load"])
def test_fact_load(setup_test_database, db_engine, sample_data_dir, sample_csv_files_map):
    """Kiểm tra việc load dữ liệu mẫu vào fact table."""
    # Chạy bước 1 và 2 trước
    extract_load_to_staging(sample_csv_files_map, sample_data_dir, db_engine)
    transform_and_load_dimensions(db_engine)
    # Chạy bước 3: Load Fact
    transform_and_load_fact(db_engine)

    # Kiểm tra kết quả trong fact table
    with db_engine.connect() as connection:
        # Kiểm tra số lượng dòng trong fact (phải khớp số order có item trong sample)
        fact_count = connection.execute(text("SELECT COUNT(*) FROM dwh.fact_order_delivery;")).scalar()
        assert fact_count > 0 # Hoặc assert fact_count == EXPECTED_VALID_ORDERS

        # Kiểm tra giá trị tính toán cho một đơn hàng cụ thể
        fact_data = connection.execute(
            text("""
                SELECT
                    order_id,
                    delivery_time_days,
                    seller_processing_hours,
                    is_late_delivery_flag,
                    total_price,
                    item_count,
                    customer_key, -- Kiểm tra FK lookup
                    seller_key,   -- Kiểm tra FK lookup
                    purchase_date_key -- Kiểm tra FK lookup
                FROM dwh.fact_order_delivery
                WHERE order_id = :order_id;
            """),
            {'order_id': 'KNOWN_SAMPLE_ORDER_ID_FOR_FACT'} # ID đơn hàng trong sample
        ).first()
        assert fact_data is not None
        # Thay các giá trị EXPECTED bằng kết quả tính toán mong đợi từ dữ liệu sample
        assert fact_data[1] == EXPECTED_DELIVERY_DAYS # delivery_time_days
        assert fact_data[2] == pytest.approx(EXPECTED_PROCESSING_HOURS) # seller_processing_hours (dùng approx cho float)
        assert fact_data[3] == EXPECTED_LATE_FLAG # is_late_delivery_flag (True/False)
        assert fact_data[4] == pytest.approx(EXPECTED_TOTAL_PRICE) # total_price
        assert fact_data[5] == EXPECTED_ITEM_COUNT # item_count
        assert fact_data[6] is not None and fact_data[6] != -1 # customer_key (phải lookup thành công)
        assert fact_data[7] is not None and fact_data[7] != -1 # seller_key
        assert fact_data[8] is not None and fact_data[8] != -1 # purchase_date_key

        # Kiểm tra trường hợp giá trị thời gian âm đã được đặt thành NULL
        fact_data_neg = connection.execute(
            text("""
                SELECT delivery_time_days, seller_processing_hours
                FROM dwh.fact_order_delivery
                WHERE order_id = :order_id_neg_time;
            """),
            {'order_id_neg_time': 'KNOWN_ORDER_ID_WITH_NEGATIVE_INPUT'} # ID đơn hàng gây ra time âm ban đầu
        ).first()
        assert fact_data_neg is not None
        assert fact_data_neg[0] is None # delivery_time_days phải là NULL
        assert fact_data_neg[1] is None # seller_processing_hours phải là NULL