#!/bin/sh
set -e

# Concord Installation Script
# Usage: curl -fsSL https://raw.githubusercontent.com/Faithful001/concord/main/install.sh | sh

echo "==> Installing Concord Distributed KV Store..."

INSTALL_DIR="/usr/local/bin"
BINARY_NAME="concord"

if command -v go >/dev/null 2>&1; then
    echo "==> Go detected. Building Concord from source..."
    TMP_DIR=$(mktemp -d)
    trap 'rm -rf "$TMP_DIR"' EXIT
    git clone --depth 1 https://github.com/Faithful001/concord.git "$TMP_DIR"
    (cd "$TMP_DIR" && go build -o "$BINARY_NAME" ./cmd/concord)
    if [ -w "$INSTALL_DIR" ]; then
        mv "$TMP_DIR/$BINARY_NAME" "$INSTALL_DIR/$BINARY_NAME"
    else
        sudo mv "$TMP_DIR/$BINARY_NAME" "$INSTALL_DIR/$BINARY_NAME"
    fi
else
    echo "==> Fetching binary release..."
    OS="$(uname -s | tr '[:upper:]' '[:lower:]')"
    ARCH="$(uname -m)"

    case "$ARCH" in
        x86_64|amd64) ARCH="amd64" ;;
        aarch64|arm64) ARCH="arm64" ;;
        *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
    esac

    RELEASE_URL="https://github.com/Faithful001/concord/releases/latest/download/concord_${OS}_${ARCH}.tar.gz"
    echo "==> Downloading from $RELEASE_URL..."
    curl -fsSL "$RELEASE_URL" | tar -xz -C /tmp/
    if [ -w "$INSTALL_DIR" ]; then
        mv /tmp/concord "$INSTALL_DIR/$BINARY_NAME"
    else
        sudo mv /tmp/concord "$INSTALL_DIR/$BINARY_NAME"
    fi
fi

if [ -w "$INSTALL_DIR/$BINARY_NAME" ]; then
    chmod +x "$INSTALL_DIR/$BINARY_NAME"
else
    sudo chmod +x "$INSTALL_DIR/$BINARY_NAME"
fi

echo "==> Concord installed successfully to $INSTALL_DIR/$BINARY_NAME!"
echo "==> Run 'concord --help' to verify installation."
