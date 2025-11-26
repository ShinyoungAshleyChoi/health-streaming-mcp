#!/bin/bash
# Generate self-signed SSL certificate for development

CERT_DIR="./certs"
mkdir -p "$CERT_DIR"

# Generate private key and certificate
openssl req -x509 -newkey rsa:4096 -nodes \
  -keyout "$CERT_DIR/key.pem" \
  -out "$CERT_DIR/cert.pem" \
  -days 365 \
  -subj "/C=KR/ST=Seoul/L=Seoul/O=HealthStack/CN=localhost"

echo "✓ Self-signed certificate generated in $CERT_DIR/"
echo "  - Certificate: $CERT_DIR/cert.pem"
echo "  - Private Key: $CERT_DIR/key.pem"
