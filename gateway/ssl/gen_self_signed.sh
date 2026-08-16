#!/bin/sh
set -e

CERT_DIR="/certs"
DOMAIN="${SELF_SIGNED_DOMAIN:-example.com}"

mkdir -p "$CERT_DIR"

if [ -f "$CERT_DIR/fullchain.pem" ] && [ -f "$CERT_DIR/privkey.pem" ]; then
    echo "[ssl] Certificates already exist, skipping generation."
    exit 0
fi

echo "[ssl] Generating self-signed certificate for $DOMAIN..."

openssl req -x509 -nodes \
    -days 365 \
    -newkey rsa:2048 \
    -keyout "$CERT_DIR/privkey.pem" \
    -out "$CERT_DIR/fullchain.pem" \
    -subj "/C=CN/ST=Dev/L=Dev/O=Dev/CN=$DOMAIN" \
    -addext "subjectAltName=DNS:$DOMAIN,DNS:*.$DOMAIN,IP:127.0.0.1"

chmod 644 "$CERT_DIR/fullchain.pem"
chmod 600 "$CERT_DIR/privkey.pem"

echo "[ssl] Self-signed certificate generated successfully."
