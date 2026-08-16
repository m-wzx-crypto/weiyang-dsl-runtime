#!/bin/sh
set -e

CERT_DIR="/certs"
CERT_FILE="${SSL_CERT_PATH:-$CERT_DIR/fullchain.pem}"
KEY_FILE="${SSL_KEY_PATH:-$CERT_DIR/privkey.pem}"

if [ ! -f "$CERT_FILE" ] || [ ! -f "$KEY_FILE" ]; then
    echo "[gateway] No TLS certificates found in $CERT_DIR, generating self-signed certificates..."
    /usr/local/openresty/nginx/ssl/gen_self_signed.sh
else
    echo "[gateway] TLS certificates found, skipping self-signed generation."
fi

echo "[gateway] Starting OpenResty gateway..."

# I15: 启动前校验 nginx 配置语法,失败时输出可读错误并退出,避免容器带病启动
echo "[gateway] Validating nginx configuration..."
if ! openresty -t; then
    echo "[gateway] ERROR: nginx configuration test failed. Aborting startup." >&2
    exit 1
fi

exec openresty -g "daemon off;"
