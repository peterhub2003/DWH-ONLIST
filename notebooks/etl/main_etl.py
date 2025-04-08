def extract_load_to_staging(csv_files_map, data_dir, db_engine):
    """
    Extract dữ liệu từ các file CSV và load vào bảng staging tương ứng.
    Xóa dữ liệu cũ trong staging trước khi load.
    """
    logging.info("Bắt đầu quá trình Extract và Load vào Staging...")
    with db_engine.connect() as connection:
        for csv_file, table_name in csv_files_map.items():
            start_time = time.time()
            file_path = data_dir / csv_file
            if not file_path.exists():
                logging.warning(f"File không tồn tại: {file_path}, bỏ qua.")
                continue

            try:
                logging.info(f"Đọc file: {csv_file}")

                df = pd.read_csv(file_path, dtype=str)
                df['_load_timestamp'] = pd.Timestamp.now() # Thêm metadata thời gian load

                logging.info(f"Load dữ liệu vào bảng: {table_name}")
                # Xóa dữ liệu cũ trong bảng staging
                connection.execute(text(f"TRUNCATE TABLE {table_name};"))
                # Load dữ liệu mới
                df.to_sql(
                    name=table_name.split('.')[1], # Chỉ lấy tên bảng
                    con=connection,
                    schema=table_name.split('.')[0], # Chỉ lấy tên schema
                    if_exists='append', # Vì đã truncate nên dùng append
                    index=False,
                    chunksize=10000 # Load theo chunk để tiết kiệm bộ nhớ
                )
                connection.commit() # Commit sau mỗi bảng staging
                end_time = time.time()
                logging.info(f"Hoàn thành load {table_name} trong {end_time - start_time:.2f} giây.")

            except Exception as e:
                logging.error(f"Lỗi khi xử lý file {csv_file} hoặc load vào {table_name}: {e}")
                connection.rollback()

    logging.info("Hoàn thành Extract và Load vào Staging.")

def transform_and_load_dimensions(db_engine):
    """
    Transform dữ liệu từ staging và load vào các bảng Dimension
    (dim_customer, dim_seller)
    """
    logging.info("Bắt đầu quá trình Transform và Load Dimensions (snake_case)...")
    with db_engine.connect() as connection:

        with connection.begin(): 
            try:
                # --- 1. Chuẩn hóa Geolocation ---
                logging.info("Chuẩn hóa dữ liệu Geolocation...")
                df_geo = pd.read_sql("SELECT * FROM staging.stg_geolocation", connection)
                df_geo['geolocation_city'] = df_geo['geolocation_city'].str.lower().str.strip()
                df_geo['geolocation_state'] = df_geo['geolocation_state'].str.upper().str.strip()
                # Loại bỏ các prefix trùng lặp, giữ lại bản ghi đầu tiên
                geo_map = df_geo.drop_duplicates(subset=['geolocation_zip_code_prefix'], keep='first')
                # Tạo index bằng zip_code_prefix để merge dễ dàng
                geo_map = geo_map.set_index('geolocation_zip_code_prefix')[['geolocation_city', 'geolocation_state']]
                logging.info(f"Tạo mapping cho {len(geo_map)} zip code prefixes.")

                # --- 2. Load dim_customer ---
                logging.info("Load dữ liệu vào dwh.dim_customer...")
                start_time = time.time()
                df_cust_staging = pd.read_sql("SELECT * FROM staging.stg_customers", connection)

                # Merge với geo_map để lấy city/state chuẩn hóa
                # Đảm bảo kiểu dữ liệu cột join là string
                df_cust_staging['customer_zip_code_prefix'] = df_cust_staging['customer_zip_code_prefix'].astype(str)
                df_merged_cust = pd.merge(
                    df_cust_staging,
                    geo_map,
                    left_on='customer_zip_code_prefix',
                    right_index=True,
                    how='left'
                )
                
                df_dim_cust = df_merged_cust[[
                    'customer_id',
                    'customer_unique_id',
                    'customer_zip_code_prefix',
                    'geolocation_city',  
                    'geolocation_state'
                ]].copy() 
                
                df_dim_cust = df_dim_cust.rename(columns={
                    'geolocation_city': 'customer_city',
                    'geolocation_state': 'customer_state'
                })

                # Xử lý NULL sau merge và chuẩn hóa thêm nếu cần
                df_dim_cust['customer_city'] = df_dim_cust['customer_city'].fillna('Unknown')
                df_dim_cust['customer_state'] = df_dim_cust['customer_state'].fillna('NA')


                dim_customer_cols = [
                    'customer_id', 'customer_unique_id', 'customer_zip_code_prefix',
                    'customer_city', 'customer_state' #, 'customer_state_name', 'customer_region'
                ]
                df_dim_cust = df_dim_cust[dim_customer_cols]

                # Xử lý SCD Type 2 (Phiên bản đơn giản - Chỉ load bản ghi mới nhất/duy nhất)
                # Lấy bản ghi cuối cùng cho mỗi customer_id nếu có trùng lặp trong staging
                df_dim_cust = df_dim_cust.drop_duplicates(subset=['customer_id'], keep='last')

                # Thêm các cột SCD (snake_case)
                df_dim_cust['effective_start_date'] = pd.Timestamp.now()
                df_dim_cust['effective_end_date'] = pd.NaT # NULL trong DB
                df_dim_cust['is_current'] = True

                # Xóa dữ liệu cũ trong DimCustomer (cho lần load đầu hoặc full load)
                logging.info("Truncating dwh.dim_customer...")
                connection.execute(text("TRUNCATE TABLE dwh.dim_customer CASCADE;")) # CASCADE để xóa FK refs

                logging.info(f"Loading {len(df_dim_cust)} rows into dwh.dim_customer...")
                df_dim_cust.to_sql(
                    name='dim_customer', 
                    con=connection,
                    schema='dwh',
                    if_exists='append', # Đã truncate nên dùng append
                    index=False,
                    chunksize=10000
                )
                end_time = time.time()
                logging.info(f"Hoàn thành load dim_customer trong {end_time - start_time:.2f} giây.")

                
                # ------------ 3. LOAD DIM_SELLER ------------------
                logging.info("Load dữ liệu vào dwh.dim_seller...")
                start_time = time.time()
                df_seller_staging = pd.read_sql("SELECT * FROM staging.stg_sellers", connection)

                # Merge với geo_map
                df_seller_staging['seller_zip_code_prefix'] = df_seller_staging['seller_zip_code_prefix'].astype(str)
                df_merged_seller = pd.merge(
                    df_seller_staging,
                    geo_map,
                    left_on='seller_zip_code_prefix',
                    right_index=True,
                    how='left'
                )
                
                df_dim_seller = df_merged_seller[[
                    'seller_id',
                    'seller_zip_code_prefix',
                    'geolocation_city', 
                    'geolocation_state' 
                ]].copy() 
                
                df_dim_seller = df_dim_seller.rename(columns={
                    'geolocation_city': 'seller_city',
                    'geolocation_state': 'seller_state'
                })
                df_dim_seller['seller_city'] = df_dim_seller['seller_city'].fillna('Unknown')
                df_dim_seller['seller_state'] = df_dim_seller['seller_state'].fillna('NA')


                dim_seller_cols = [
                    'seller_id', 'seller_zip_code_prefix', 'seller_city', 'seller_state'
                ]
                df_dim_seller = df_dim_seller[dim_seller_cols]

                # Xử lý SCD Type 2 (Đơn giản)
                df_dim_seller = df_dim_seller.drop_duplicates(subset=['seller_id'], keep='last')
                df_dim_seller['effective_start_date'] = pd.Timestamp.now()
                df_dim_seller['effective_end_date'] = pd.NaT
                df_dim_seller['is_current'] = True

                # Xóa dữ liệu cũ (cho lần load đầu)
                logging.info("Truncating dwh.dim_seller...")
                connection.execute(text("TRUNCATE TABLE dwh.dim_seller CASCADE;"))

                logging.info(f"Loading {len(df_dim_seller)} rows into dwh.dim_seller...")
                df_dim_seller.to_sql(
                    name='dim_seller', 
                    con=connection,
                    schema='dwh',
                    if_exists='append',
                    index=False,
                    chunksize=1000
                )
                end_time = time.time()
                logging.info(f"Hoàn thành load dim_seller trong {end_time - start_time:.2f} giây.")

            except Exception as e:
                logging.error(f"Lỗi trong quá trình Transform và Load Dimensions: {e}")
                raise e 

    logging.info("Hoàn thành Transform và Load Dimensions.")


def transform_and_load_fact(db_engine):
    """
    Transform dữ liệu từ staging, lookup keys từ Dimensions,
    và load vào fact_order_delivery
    """
    logging.info("Bắt đầu quá trình Transform và Load Fact Table...")
    with db_engine.connect() as connection:
        with connection.begin():
            try:
                # --- 1. Đọc dữ liệu cần thiết ---
                logging.info("Đọc dữ liệu từ staging và dimensions...")
                df_orders = pd.read_sql("SELECT * FROM staging.stg_orders", connection)
                df_items = pd.read_sql("SELECT * FROM staging.stg_order_items", connection)
                df_dim_date = pd.read_sql('SELECT date_key, full_date FROM dwh.dim_date', connection, parse_dates=['full_date'])
                df_dim_cust = pd.read_sql('SELECT customer_key, customer_id FROM dwh.dim_customer WHERE is_current = TRUE', connection)
                df_dim_seller = pd.read_sql('SELECT seller_key, seller_id FROM dwh.dim_seller WHERE is_current = TRUE', connection)


                # --- 2. Xử lý và Tổng hợp Order Items ---
                logging.info("Tổng hợp dữ liệu Order Items...")
                df_items['price'] = pd.to_numeric(df_items['price'], errors='coerce').fillna(0)
                df_items['freight_value'] = pd.to_numeric(df_items['freight_value'], errors='coerce').fillna(0)
                df_items_agg = df_items.groupby('order_id').agg(
                    item_count=('order_item_id', 'count'),
                    total_freight_value=('freight_value', 'sum'),
                    total_price=('price', 'sum'),
                    seller_id=('seller_id', 'first')
                ).reset_index()

                # --- 3. Kết hợp Orders và Items Aggregated ---
                logging.info("Kết hợp Orders và Items Aggregated...")
                df_fact = pd.merge(df_orders, df_items_agg, on='order_id', how='inner')

                # --- 4. Chuyển đổi kiểu dữ liệu Ngày tháng trong Orders ---
                logging.info("Chuyển đổi kiểu dữ liệu ngày tháng...")
                date_cols_ts = [
                    'order_purchase_timestamp', 'order_approved_at',
                    'order_delivered_carrier_date', 'order_delivered_customer_date',
                    'order_estimated_delivery_date'
                ]
                for col in date_cols_ts:
                    df_fact[col] = pd.to_datetime(df_fact[col], errors='coerce')

                date_cols_date = {
                    'order_purchase_timestamp': 'purchase_date',
                    'order_approved_at': 'approved_date',
                    'order_delivered_carrier_date': 'delivered_carrier_date',
                    'order_delivered_customer_date': 'delivered_customer_date',
                    'order_estimated_delivery_date': 'estimated_delivery_date'
                }
                for ts_col, date_col in date_cols_date.items():
                     df_fact[date_col] = df_fact[ts_col].dt.date

                # --- 5. Tính toán các Measures ---
                logging.info("Tính toán các Measures...")
                df_fact['delivery_time_days'] = (pd.to_datetime(df_fact['delivered_customer_date'], errors='coerce') - pd.to_datetime(df_fact['approved_date'], errors='coerce')).dt.days
                df_fact['estimated_delivery_time_days'] = (pd.to_datetime(df_fact['estimated_delivery_date'], errors='coerce') - pd.to_datetime(df_fact['approved_date'], errors='coerce')).dt.days
                df_fact['delivery_time_difference_days'] = (pd.to_datetime(df_fact['delivered_customer_date'], errors='coerce') - pd.to_datetime(df_fact['estimated_delivery_date'], errors='coerce')).dt.days
                df_fact['time_to_approve_hours'] = (df_fact['order_approved_at'] - df_fact['order_purchase_timestamp']) / pd.Timedelta(hours=1)
                df_fact['seller_processing_hours'] = (df_fact['order_delivered_carrier_date'] - df_fact['order_approved_at']) / pd.Timedelta(hours=1)
                df_fact['carrier_shipping_hours'] = (df_fact['order_delivered_customer_date'] - df_fact['order_delivered_carrier_date']) / pd.Timedelta(hours=1)
                hour_cols = ['time_to_approve_hours', 'seller_processing_hours', 'carrier_shipping_hours']
                for col in hour_cols:
                    df_fact[col] = df_fact[col].round(2)
                df_fact['is_late_delivery_flag'] = (df_fact['delivery_time_difference_days'] > 0) & (df_fact['delivered_customer_date'].notna())
                df_fact['is_late_delivery_flag'] = df_fact['is_late_delivery_flag'].fillna(False).astype(bool)

                # ----  CHECK các giá trị ÂM -------
                logging.info("Setting negative time measures to None...")
                time_measure_cols = [
                    'delivery_time_days', 'estimated_delivery_time_days', 'delivery_time_difference_days',
                    'time_to_approve_hours', 'seller_processing_hours', 'carrier_shipping_hours'
                ]
                for col in time_measure_cols:
                    if col in df_fact.columns:
                        df_fact.loc[df_fact[col] < 0, col] = None
                    else:
                         logging.warning(f"Column {col} not found for negative check.")

                # --- 6. Lookup Dimension Keys ---
                logging.info("Lookup Dimension Keys...")
                date_lookup_cols = {
                    'purchase_date': 'purchase_date_key',
                    'approved_date': 'approved_date_key',
                    'delivered_carrier_date': 'delivered_carrier_date_key',
                    'delivered_customer_date': 'delivered_customer_date_key',
                    'estimated_delivery_date': 'estimated_delivery_date_key'
                }
                # Chuyển cột date trong fact sang datetime để join
                for date_col_fact in date_lookup_cols.keys():
                     df_fact[date_col_fact] = pd.to_datetime(df_fact[date_col_fact], errors='coerce')

                # Join với DimDate cho từng cột ngày
                for date_col_fact, date_key_col in date_lookup_cols.items():
                    temp_dim_date = df_dim_date.rename(columns={'date_key': date_key_col})
                    df_fact = pd.merge(
                        df_fact,
                        temp_dim_date[['full_date', date_key_col]],
                        left_on=date_col_fact,
                        right_on='full_date',
                        how='left'
                    )
                    df_fact = df_fact.drop(columns=['full_date']) 

                # Join với dim_customer
                df_fact = pd.merge(
                    df_fact,
                    df_dim_cust[['customer_key', 'customer_id']],
                    on='customer_id',
                    how='left'
                )

                # Join với dim_seller
                df_fact = pd.merge(
                    df_fact,
                    df_dim_seller[['seller_key', 'seller_id']],
                    on='seller_id',
                    how='left'
                )

                logging.info("Handling failed lookups and preparing key data types...")
                date_key_cols_list = list(date_lookup_cols.values())
                dim_key_cols_list = ['customer_key', 'seller_key']
                all_key_cols = date_key_cols_list + dim_key_cols_list

                for col in all_key_cols:
                    if col not in df_fact.columns:
                        logging.warning(f"Key column '{col}' missing after merges. Adding as pd.NA.")
                        df_fact[col] = pd.NA
                    else:
                        # QUAN TRỌNG: KHÔNG fillna(-1) một cách mù quáng nữa.
                        # Chỉ fillna(-1) cho customer/seller keys NẾU bạn đã tạo dòng Unknown=-1 trong Dim.
                        # Nếu không, hãy để NaN/NA để nó thành NULL trong DB.
                        if col in dim_key_cols_list:
                            # Tạm thời vẫn fill -1 cho dimension keys nếu bạn muốn (cần có dòng -1 trong dim)
                            # Hoặc comment dòng fillna này để nó thành NULL
                            df_fact[col] = df_fact[col].fillna(-1)
                            pass

                    # Chuyển đổi sang kiểu số nullable để to_sql xử lý NaN/NA thành NULL
                    # Sử dụng float trước để xử lý các kiểu dữ liệu không đồng nhất có thể có
                    df_fact[col] = pd.to_numeric(df_fact[col], errors='coerce')
                    # Chuyển sang Int64 của Pandas để biểu diễn integer nullable
                    # Điều này giúp to_sql hiểu rõ hơn ý định gửi NULL
                    df_fact[col] = df_fact[col].astype('Int64') # 'Int64' (chữ I viết hoa) là nullable integer type

                # --- 7. Chuẩn bị dữ liệu cuối cùng cho Fact ---
                logging.info("Chuẩn bị dữ liệu cuối cùng cho fact_order_delivery...")
                df_fact = df_fact.rename(columns={'order_id': 'order_id', 'order_status': 'order_status'})
                df_fact['order_count'] = 1
                df_fact['dw_load_timestamp'] = pd.Timestamp.now()
                final_fact_columns = [
                    'order_id', 'purchase_date_key', 'approved_date_key', 'delivered_carrier_date_key',
                    'delivered_customer_date_key', 'estimated_delivery_date_key', 'customer_key', 'seller_key',
                    'order_status', 'delivery_time_days', 'estimated_delivery_time_days', 'delivery_time_difference_days',
                    'is_late_delivery_flag', 'time_to_approve_hours', 'seller_processing_hours', 'carrier_shipping_hours',
                    'item_count', 'total_freight_value', 'total_price', 'order_count', 'dw_load_timestamp'
                ]
                missing_cols = [col for col in final_fact_columns if col not in df_fact.columns]
                if missing_cols:
                    logging.error(f"Thiếu các cột trong Fact DataFrame: {missing_cols}")
                    raise ValueError(f"Missing columns required for fact table: {missing_cols}")
                df_fact_final = df_fact[final_fact_columns]


                # --- 8. Load dữ liệu vào Fact Table ---
                logging.info(f"Load {len(df_fact_final)} dòng vào dwh.fact_order_delivery...")
                start_time = time.time()
                logging.info("Truncating dwh.fact_order_delivery...")
                connection.execute(text("TRUNCATE TABLE dwh.fact_order_delivery;"))
                df_fact_final.to_sql(
                    name='fact_order_delivery',
                    con=connection,
                    schema='dwh',
                    if_exists='append',
                    index=False,
                    chunksize=10000,
                    # method='multi' # Có thể thử method='multi' nếu mặc định chậm
                )
                end_time = time.time()
                logging.info(f"Hoàn thành load fact_order_delivery trong {end_time - start_time:.2f} giây.")

            except Exception as e:
                logging.error(f"Lỗi trong quá trình Transform và Load Fact Table: {e}")
                raise e

    logging.info("Hoàn thành Transform và Load Fact Table.")
