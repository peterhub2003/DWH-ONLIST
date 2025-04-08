#!/bin/bash
set -e 

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SELECT 1 FROM pg_database WHERE datname = '${SUPERSET_DB_NAME}';

    -- Chỉ tạo nếu chưa tồn tại (sử dụng \gexec để thực thi có điều kiện)
    -- Lưu ý: Cú pháp này cần kiểm tra kỹ với phiên bản PSQL cụ thể,
    CREATE DATABASE ${SUPERSET_DB_NAME};
EOSQL

echo "Database ${SUPERSET_DB_NAME} created or already exists."

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "${POSTGRES_DB}" <<-EOSQL
    CREATE SCHEMA IF NOT EXISTS staging;
    CREATE SCHEMA IF NOT EXISTS dwh;
    GRANT ALL ON SCHEMA staging TO ${POSTGRES_USER};
    GRANT ALL ON SCHEMA dwh TO ${POSTGRES_USER};
EOSQL

echo "Schemas staging and dwh created or already exist in ${POSTGRES_DB}."