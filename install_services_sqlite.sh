#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/home/pi/datenerfassungsprojekt"
DATA_DIR="/home/pi/datenerfassung"

echo "Creating data dir: ${DATA_DIR}"
sudo mkdir -p "${DATA_DIR}"
sudo chown -R pi:pi "${DATA_DIR}"

echo "Copying scripts into ${PROJECT_DIR}"
sudo install -m 755 collector_pi3_sqlite.py "${PROJECT_DIR}/collector_pi3_sqlite.py"
sudo install -m 755 api_server_sqlite.py "${PROJECT_DIR}/api_server_sqlite.py"

echo "Installing systemd services..."
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
echo "API health check (local):"
curl -s "http://127.0.0.1:5000/api/health" | python3 -m json.tool || true
echo ""
echo "Latest data (local):"
curl -s "http://127.0.0.1:5000/api/latest" | python3 -m json.tool || true

echo ""
echo "Done."
