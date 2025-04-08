#!/usr/bin/env bash
#
# Script entrypoint đơn giản hóa cho Superset
#
set -eo pipefail # Thoát ngay khi có lỗi

echo "######################################################################"
echo "Chạy các bước khởi tạo Superset..."
echo "######################################################################"

# === DEBUGGING: Kiểm tra biến môi trường CONFIG PATH ===
echo "[DEBUG] Kiểm tra biến môi trường CONFIG PATH:"
echo "[DEBUG] SUPERSET_CONFIG_PATH='${SUPERSET_CONFIG_PATH}'"
echo "[DEBUG] Kiểm tra sự tồn tại của file config: $(ls -l ${SUPERSET_CONFIG_PATH})"

# === Bước 1: Áp dụng DB migrations ===
echo "[INIT] Áp dụng DB migrations (superset db upgrade)..."
superset db upgrade
echo "[INIT] DB migrations hoàn thành."

# === Bước 2: Tạo tài khoản admin ===
# Kiểm tra xem admin user đã tồn tại chưa để tránh lỗi nếu chạy lại
if ! superset fab list-users | grep -q "username: ${ADMIN_USERNAME}"; then
  echo "[INIT] Tạo tài khoản admin (${ADMIN_USERNAME})..."
  superset fab create-admin \
      --username "${ADMIN_USERNAME}" \
      --firstname "${ADMIN_FIRSTNAME}" \
      --lastname "${ADMIN_LASTNAME}" \
      --email "${ADMIN_EMAIL}" \
      --password "${ADMIN_PASSWORD}"
  echo "[INIT] Tạo tài khoản admin hoàn thành."
else
  echo "[INIT] Tài khoản admin (${ADMIN_USERNAME}) đã tồn tại, bỏ qua việc tạo mới."
fi

# === Bước 3: Khởi tạo roles và permissions ===
echo "[INIT] Khởi tạo roles và permissions (superset init)..."
superset init
echo "[INIT] Khởi tạo roles và permissions hoàn thành."

# === Bước 4: Load dữ liệu mẫu (nếu được yêu cầu) ===
if [ "$SUPERSET_LOAD_EXAMPLES" = "yes" ]; then
    echo "[INIT] Loading dữ liệu mẫu (superset load_examples)..."
    superset load_examples
    echo "[INIT] Loading dữ liệu mẫu hoàn thành."
else
    echo "[INIT] Bỏ qua việc load dữ liệu mẫu (SUPERSET_LOAD_EXAMPLES != yes)."
fi

echo "######################################################################"
echo "Quá trình khởi tạo Superset hoàn thành."
echo "######################################################################"
echo ""
echo "######################################################################"
echo "Khởi động Superset Web Server..."
echo "######################################################################"

# Thực thi CMD được truyền từ docker-compose.yml
# Ví dụ: /usr/bin/run-server.sh hoặc flask run ...
exec "$@"