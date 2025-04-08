# tests/conftest.py
import pytest
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from pathlib import Path
import sys

# # Giả sử bạn import các hàm ETL chính từ script của bạn
# from etl.main_etl import extract_load_to_staging, transform_and_load_dimensions, transform_and_load_fact, CSV_FILES



@pytest.fixture(scope='session') 
def db_engine():

    engine = create_engine(DATABASE_URI)
    yield engine 
    engine.dispose() 

@pytest.fixture(scope='function') # Chạy trước mỗi test function
def setup_test_database(db_engine):
    """Fixture để xóa dữ liệu trong các bảng trước mỗi test."""
    print("\nSetting up test database (Truncating tables)...")
    with db_engine.connect() as connection:
        with connection.begin(): # Dùng transaction
             # Truncate theo thứ tự ngược (Fact -> Dim -> Staging) để tránh lỗi FK
            connection.execute(text("TRUNCATE TABLE dwh.fact_order_delivery CASCADE;"))
            connection.execute(text("TRUNCATE TABLE dwh.dim_customer CASCADE;"))
            connection.execute(text("TRUNCATE TABLE dwh.dim_seller CASCADE;"))
            for table_name in reversed(list(CSV_FILES.values())): # Truncate staging
                connection.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))
    yield 
    print("\nTest finished.")


@pytest.fixture(scope='session')
def sample_data_dir():
    """Trả về đường dẫn đến thư mục data mẫu."""
    return Path(__file__).resolve().parent / 'sample_data'

@pytest.fixture(scope='session')
def sample_csv_files_map(sample_data_dir):
    """Tạo map tên file CSV mẫu đến tên bảng staging."""
    sample_map = {}
    for csv_file, table_name in CSV_FILES.items():
         # Thay thế tên file gốc bằng tên file sample
        sample_csv = f"sample_{csv_file}"
        if (sample_data_dir / sample_csv).exists():
             sample_map[sample_csv] = table_name
        else:
             print(f"Warning: Sample file not found: {sample_data_dir / sample_csv}")
    return sample_map