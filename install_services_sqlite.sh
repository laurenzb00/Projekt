#!/usr/bin/env bash
set -euo pipefail

USER_NAME="laurenz2"
PROJECT_DIR="/home/laurenz2/Projekt"
DATA_DIR="/home/laurenz2/datenerfassung"

echo "Erstelle Datenverzeichnis: ${DATA_DIR}"
sudo mkdir -p "${DATA_DIR}"
sudo chown -R "${USER_NAME}:${USER_NAME}" "${DATA_DIR}"

echo "Installiere systemd Services..."
sudo install -m 644 energy-collector.service /etc/systemd/system/energy-collector.service
sudo install -m 644 energy-api.service /etc/systemd/system/energy-api.service

sudo systemctl daemon-reload
sudo systemctl enable --now energy-collector.service
sudo systemctl enable --now energy-api.service

echo ""
echo "Status:"
sudo systemctl --no-pager status energy-collector.service || true
sudo systemctl --no-pager status energy-api.service || true

echo ""
echo "Healthcheck:"
curl -s http://127.0.0.1:5000/api/health | python3 -m json.tool || true
