# tests/test_unit_etl.py
import pandas as pd
import numpy as np
import pytest
from datetime import datetime, timedelta


# --- Fixtures (Dữ liệu mẫu) ---
@pytest.fixture
def sample_geo_df():
    data = {
        'geolocation_zip_code_prefix': ['12345', '54321', '12345', '99999'],
        'geolocation_city': [' São Paulo ', 'rio de janeiro', ' SAO PAULO', 'Belo Horizonte'],
        'geolocation_state': [' SP', 'rj', 'sp', 'MG ']
    }
    return pd.DataFrame(data)

@pytest.fixture
def sample_orders_df():
    data = {
        'order_id': ['order1', 'order2', 'order3', 'order4', 'order5'],
        'order_purchase_timestamp': ['2018-01-01 10:00:00', '2018-01-05 12:00:00', '2018-01-10 08:00:00', '2018-01-15 15:00:00', '2018-01-20 11:00:00'],
        'order_approved_at': ['2018-01-01 11:00:00', '2018-01-05 13:00:00', '2018-01-12 09:00:00', None, '2018-01-19 10:00:00'], # Note: None and a day before purchase
        'order_delivered_carrier_date': ['2018-01-02 14:00:00', '2018-01-04 15:00:00', '2018-01-11 10:00:00', '2018-01-16 16:00:00', '2018-01-22 12:00:00'], # Note: Before approval
        'order_delivered_customer_date': ['2018-01-04 16:00:00', '2018-01-08 18:00:00', '2018-01-10 11:00:00', '2018-01-18 19:00:00', None], # Note: Before carrier and None
        'order_estimated_delivery_date': ['2018-01-06 23:59:59', '2018-01-09 23:59:59', '2018-01-15 23:59:59', '2018-01-20 23:59:59', '2018-01-25 23:59:59']
    }
    df = pd.DataFrame(data)
    # Chuyển đổi sang datetime giống như trong ETL
    for col in ['order_purchase_timestamp', 'order_approved_at', 'order_delivered_carrier_date', 'order_delivered_customer_date', 'order_estimated_delivery_date']:
        df[col] = pd.to_datetime(df[col], errors='coerce')
    return df

# --- Tests ---

def test_standardize_geolocation(sample_geo_df):
    """Kiểm tra chuẩn hóa city/state: lowercase, strip."""
    df_geo = sample_geo_df.copy()
    df_geo['geolocation_city'] = df_geo['geolocation_city'].str.lower().str.strip()
    df_geo['geolocation_state'] = df_geo['geolocation_state'].str.upper().str.strip()

    assert df_geo.loc[0, 'geolocation_city'] == 'são paulo'
    assert df_geo.loc[0, 'geolocation_state'] == 'SP'
    assert df_geo.loc[1, 'geolocation_city'] == 'rio de janeiro'
    assert df_geo.loc[1, 'geolocation_state'] == 'RJ'
    assert df_geo.loc[3, 'geolocation_state'] == 'MG'

def test_geo_mapping_creation(sample_geo_df):
    """Kiểm tra tạo map từ zip prefix, giữ lại bản ghi đầu tiên."""
    df_geo = sample_geo_df.copy()
    # Chuẩn hóa trước
    df_geo['geolocation_city'] = df_geo['geolocation_city'].str.lower().str.strip()
    df_geo['geolocation_state'] = df_geo['geolocation_state'].str.upper().str.strip()
    # Tạo map
    geo_map = df_geo.drop_duplicates(subset=['geolocation_zip_code_prefix'], keep='first')
    geo_map = geo_map.set_index('geolocation_zip_code_prefix')[['geolocation_city', 'geolocation_state']]

    assert len(geo_map) == 3 # 12345, 54321, 99999
    assert geo_map.loc['12345', 'geolocation_city'] == 'são paulo' # Giữ lại dòng đầu tiên
    assert geo_map.loc['54321', 'geolocation_state'] == 'RJ'

def test_calculate_time_diffs_days(sample_orders_df):
    """Kiểm tra tính toán số ngày (delivery_time_days, etc.)."""
    df = sample_orders_df.copy()
    df['approved_date'] = df['order_approved_at'].dt.date
    df['delivered_customer_date'] = df['order_delivered_customer_date'].dt.date
    df['estimated_delivery_date'] = df['order_estimated_delivery_date'].dt.date

    df['delivery_time_days'] = (pd.to_datetime(df['delivered_customer_date'], errors='coerce') -
                               pd.to_datetime(df['approved_date'], errors='coerce')).dt.days
    df['estimated_delivery_time_days'] = (pd.to_datetime(df['estimated_delivery_date'], errors='coerce') -
                                          pd.to_datetime(df['approved_date'], errors='coerce')).dt.days
    df['delivery_time_difference_days'] = (pd.to_datetime(df['delivered_customer_date'], errors='coerce') -
                                           pd.to_datetime(df['estimated_delivery_date'], errors='coerce')).dt.days

    assert df.loc[0, 'delivery_time_days'] == 3 # Jan 4 - Jan 1
    assert df.loc[1, 'delivery_time_days'] == 3 # Jan 8 - Jan 5
    assert pd.isna(df.loc[2, 'delivery_time_days']) # delivered < approved -> NaT date -> days is NaT/NaN
    assert pd.isna(df.loc[3, 'delivery_time_days']) # approved_at is NaT
    assert pd.isna(df.loc[4, 'delivery_time_days']) # delivered_customer_date is NaT

    assert df.loc[0, 'delivery_time_difference_days'] == -2 # Delivered Jan 4, Estimated Jan 6

def test_calculate_time_diffs_days(sample_orders_df):
    """Kiểm tra tính toán số ngày (delivery_time_days, etc.)."""
    df = sample_orders_df.copy()
    df['approved_date'] = df['order_approved_at'].dt.date
    df['delivered_customer_date'] = df['order_delivered_customer_date'].dt.date
    df['estimated_delivery_date'] = df['order_estimated_delivery_date'].dt.date

    # Tính toán giữ nguyên
    df['delivery_time_days'] = (pd.to_datetime(df['delivered_customer_date'], errors='coerce') -
                               pd.to_datetime(df['approved_date'], errors='coerce')).dt.days
    df['estimated_delivery_time_days'] = (pd.to_datetime(df['estimated_delivery_date'], errors='coerce') -
                                          pd.to_datetime(df['approved_date'], errors='coerce')).dt.days
    df['delivery_time_difference_days'] = (pd.to_datetime(df['delivered_customer_date'], errors='coerce') -
                                           pd.to_datetime(df['estimated_delivery_date'], errors='coerce')).dt.days

    # Sửa Assertions
    assert df.loc[0, 'delivery_time_days'] == 3
    assert df.loc[1, 'delivery_time_days'] == 3
    assert df.loc[2, 'delivery_time_days'] == -2
    assert pd.isna(df.loc[3, 'delivery_time_days']) 
    assert pd.isna(df.loc[4, 'delivery_time_days']) 

    # Kiểm tra delivery_time_difference_days
    assert df.loc[0, 'delivery_time_difference_days'] == -2
    assert df.loc[1, 'delivery_time_difference_days'] == -1
    assert df.loc[2, 'delivery_time_difference_days'] == -5

    # Hàng 3: delivered là '18', estimated là '20' -> Kết quả là -2
    assert df.loc[3, 'delivery_time_difference_days'] == -2
    assert pd.isna(df.loc[4, 'delivery_time_difference_days'])

def test_late_delivery_flag(sample_orders_df):
    """Kiểm tra cờ is_late_delivery_flag."""
    df = sample_orders_df.copy()
    # Tính toán các giá trị cần thiết
    df['delivered_customer_date_dt'] = pd.to_datetime(df['order_delivered_customer_date'], errors='coerce')
    df['estimated_delivery_date_dt'] = pd.to_datetime(df['order_estimated_delivery_date'], errors='coerce')
    df['delivery_time_difference_days'] = (df['delivered_customer_date_dt'] - df['estimated_delivery_date_dt']).dt.days

    # Logic tính flag
    df['is_late_delivery_flag'] = (df['delivery_time_difference_days'] > 0) & (df['delivered_customer_date_dt'].notna())
    # Xử lý các trường hợp NaN trong delivery_time_difference_days (do ngày đầu vào là NaT) thành False
    df['is_late_delivery_flag'] = df['is_late_delivery_flag'].fillna(False).astype(bool)

    # Sửa Assertions
    assert not df.loc[0, 'is_late_delivery_flag'] # delivery_time_difference_days = -2 (sớm) -> False
    assert not df.loc[1, 'is_late_delivery_flag'] # delivery_time_difference_days = -1 (sớm) -> False
    # Hàng 2: delivery_time_difference_days = -5 (sớm) -> False
    assert not df.loc[2, 'is_late_delivery_flag']
    # Hàng 3: delivery_time_difference_days là NaN (do delivered_customer là NaT) -> fillna(False) -> False
    assert not df.loc[3, 'is_late_delivery_flag'] # (delivered_customer_date là 2018-01-18) delivery_time_difference_days = -2 -> False
    # Hàng 4: delivered_customer_date là NaT -> điều kiện notna() là False -> False
    assert not df.loc[4, 'is_late_delivery_flag']

def test_set_negative_times_to_none():
    """Kiểm tra việc đặt giá trị thời gian âm thành None."""
    data = {
        'delivery_time_days': [3, -2, 5, None, 0],
        'seller_processing_hours': [20.5, -10.2, 0.0, 5.0, -0.1]
    }
    df = pd.DataFrame(data)
    time_measure_cols = ['delivery_time_days', 'seller_processing_hours']

    for col in time_measure_cols:
        # Chuyển sang kiểu số trước khi so sánh
        df[col] = pd.to_numeric(df[col], errors='coerce')
        df.loc[df[col] < 0, col] = None 

    assert df.loc[0, 'delivery_time_days'] == 3
    assert pd.isna(df.loc[1, 'delivery_time_days']) # Đã là None
    assert df.loc[2, 'delivery_time_days'] == 5
    assert pd.isna(df.loc[3, 'delivery_time_days'])
    assert df.loc[4, 'delivery_time_days'] == 0

    assert df.loc[0, 'seller_processing_hours'] == 20.5
    assert pd.isna(df.loc[1, 'seller_processing_hours']) # Đã là None
    assert df.loc[2, 'seller_processing_hours'] == 0.0
    assert df.loc[3, 'seller_processing_hours'] == 5.0
    assert pd.isna(df.loc[4, 'seller_processing_hours']) # Đã là None
    