import os
import json
import shutil
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

WORK_DIR = Path("/home/jovyan/work")
KAGGLE_CONFIG_DIR = Path.home() / ".config/kaggle"
KAGGLE_JSON_TARGET_PATH = KAGGLE_CONFIG_DIR / "kaggle.json"

def setup_kaggle_credentials():
    print("--- Bắt đầu thiết lập Kaggle Credentials ---")

    source_file_name = "kaggle.json"
    source_path = WORK_DIR / source_file_name.strip()

    if not source_path.is_file():
        logging.error(f"Lỗi: Không tìm thấy file '{source_path}'.")
        logging.error(f"Hãy đảm bảo bạn đã tải kaggle.json từ web và đặt nó vào thư mục '{WORK_DIR}' (tức là thư mục 'notebooks' trên máy host).")
        return False

    logging.info(f"Tìm thấy file nguồn: {source_path}")

    try:
        # Tạo thư mục ~/.kaggle nếu chưa tồn tại
        KAGGLE_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        logging.info(f"Đã tạo (hoặc tồn tại) thư mục: {KAGGLE_CONFIG_DIR}")

        shutil.copy2(source_path, KAGGLE_JSON_TARGET_PATH)
        logging.info(f"Đã copy file vào: {KAGGLE_JSON_TARGET_PATH}")

        # Thiết lập quyền truy cập (chỉ chủ sở hữu đọc/ghi - 600)
        os.chmod(KAGGLE_JSON_TARGET_PATH, 0o600)
        logging.info(f"Đã thiết lập quyền truy cập (600) cho: {KAGGLE_JSON_TARGET_PATH}")

        # Đọc lại file để xác nhận và lấy thông tin
        with open(KAGGLE_JSON_TARGET_PATH, 'r') as f:
            credentials = json.load(f)

        kaggle_username = credentials.get('username')
        kaggle_key = credentials.get('key')

        if kaggle_username and kaggle_key:
            print("\n--- Thông tin Credentials (đọc từ file đã copy) ---")
            print(f"Kaggle Username: {kaggle_username}")
            # Không nên in key ra màn hình thường xuyên, chỉ để xác nhận lần đầu
            # print(f"Kaggle Key: {kaggle_key[:5]}...{kaggle_key[-5:]}") # Chỉ hiển thị một phần key
            print(f"Đã đọc thành công key.")
            print("-" * 40)
            logging.info("Credentials đã được thiết lập tại ~/.config/kaggle/kaggle.json.")
            logging.info("Bây giờ bạn có thể sử dụng thư viện 'kaggle' (có thể cần restart kernel).")

            return True
        else:
            logging.error("Lỗi: File kaggle.json không chứa 'username' hoặc 'key'.")
            return False

    except Exception as e:
        logging.error(f"Đã xảy ra lỗi trong quá trình thiết lập: {e}")
        if KAGGLE_JSON_TARGET_PATH.exists():
             try:
                 os.remove(KAGGLE_JSON_TARGET_PATH)
             except OSError:
                 pass
        return False


if __name__ == "__main__":
    # Câu lệnh này chỉ chạy nếu bạn lưu code thành file .py và chạy từ terminal
    # Nếu chạy trong notebook, chỉ cần gọi hàm setup_kaggle_credentials()
    pass

# Chạy hàm trong Notebook:
setup_kaggle_credentials()

# Bạn có thể thử import và xác thực ngay sau đó (có thể cần restart kernel)
# import kaggle
# try:
#     kaggle.api.authenticate()
#     print("\nKaggle API authentication successful using the copied token file!")
# except Exception as e:
#     print(f"\nKaggle API authentication failed: {e}")