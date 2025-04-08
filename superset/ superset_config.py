# docker/superset/superset_config.py
import os
from cachelib.redis import RedisCache

# Lấy SECRET_KEY từ biến môi trường đã set trong docker-compose.yml
SECRET_KEY = os.environ.get('SUPERSET_SECRET_KEY')

# Cấu hình Cache sử dụng Redis
CACHE_CONFIG = {
    'CACHE_TYPE': 'RedisCache',
    'CACHE_DEFAULT_TIMEOUT': 300, # 5 phút
    'CACHE_KEY_PREFIX': 'superset_cache_',
    'CACHE_REDIS_HOST': 'redis', 
    'CACHE_REDIS_PORT': 6379,
    'CACHE_REDIS_DB': 1, 
    'CACHE_REDIS_URL': 'redis://redis:6379/1'
}
DATA_CACHE_CONFIG = CACHE_CONFIG


# Cấu hình Celery (nếu muốn chạy worker riêng cho task dài)
# class CeleryConfig:
#     broker_url = 'redis://redis:6379/0'
#     result_backend = 'redis://redis:6379/0'
#     # Các cấu hình Celery khác...
# CELERY_CONFIG = CeleryConfig

# Bật các tính năng thử nghiệm nếu muốn
# FEATURE_FLAGS = { "ENABLE_TEMPLATE_PROCESSING": True }

# Cho phép tải lên file CSV/Excel vào Superset (cần cài thêm package nếu chưa có)
# CSV_EXTENSIONS = {'csv'}
# EXCEL_EXTENSIONS = {'xls', 'xlsx'}
# ALLOW_FULL_CSV_UPLOAD = True # Cẩn thận với dữ liệu lớn