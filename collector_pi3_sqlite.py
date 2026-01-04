#!/usr/bin/env python3
"""
Collector-Dienst (SQLite only) für Raspberry Pi 3
- Holt Fronius PowerFlowRealtimeData
- Holt BMK/Heizungs-Daten (daqdata.cgi)
- Speichert Zeitreihen in SQLite (energy.db)

Environment (optional):
    FRONIUS_URL="http://192.168.1.202/solar_api/v1/GetPowerFlowRealtimeData.fcgi"
    BMK_URL="http://192.168.1.201/daqdata.cgi"
    DATA_DIR="/home/pi/datenerfassung"      # default: aktuelles Verzeichnis
    DB_FILE="energy.db"                     # default: energy.db
    FRONIUS_INTERVAL_S="1"                  # default: 1
    BMK_INTERVAL_S="10"                     # default: 10
    HTTP_TIMEOUT_S="5"                      # default: 5

Hinweise:
- SQLite läuft im WAL-Modus (gleichzeitiges Lesen/Schreiben robust).
- Insert erfolgt idempotent (PRIMARY KEY ts + INSERT OR REPLACE).
"""

from __future__ import annotations

import logging
import os
import signal
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

import requests


@dataclass(frozen=True)
class Settings:
    fronius_url: str
    bmk_url: str
    data_dir: Path
    db_file: Path
    fronius_interval_s: float
    bmk_interval_s: float
    http_timeout_s: float


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name)
    if not v:
        return default
    try:
        return float(v)
    except ValueError:
        return default


def load_settings() -> Settings:
    data_dir = Path(os.getenv("DATA_DIR", ".")).expanduser().resolve()
    db_name = os.getenv("DB_FILE", "energy.db")
    return Settings(
        fronius_url=os.getenv(
            "FRONIUS_URL",
            "http://192.168.1.87/solar_api/v1/GetPowerFlowRealtimeData.fcgi",
        ),
        bmk_url=os.getenv("BMK_URL", "http://192.168.1.85/daqdata.cgi"),
        data_dir=data_dir,
        db_file=(data_dir / db_name).resolve(),
        fronius_interval_s=max(0.2, _env_float("FRONIUS_INTERVAL_S", 1.0)),
        bmk_interval_s=max(1.0, _env_float("BMK_INTERVAL_S", 10.0)),
        http_timeout_s=max(0.5, _env_float("HTTP_TIMEOUT_S", 5.0)),
    )


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        str(db_path),
        timeout=30,
        isolation_level=None,  # autocommit
        check_same_thread=False,
    )
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS fronius (
            ts TEXT PRIMARY KEY,
            pv_kw REAL,
            grid_kw REAL,
            battery_kw REAL,
            load_kw REAL,
            soc REAL
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_fronius_ts ON fronius(ts);")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS bmk (
            ts TEXT PRIMARY KEY,
            boiler_temp REAL,
            outside_temp REAL,
            buffer_top REAL,
            buffer_mid REAL,
            buffer_bottom REAL,
            hot_water REAL
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_bmk_ts ON bmk(ts);")


def fetch_fronius(settings: Settings) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(settings.fronius_url, timeout=settings.http_timeout_s)
        r.raise_for_status()
        data = r.json()

        site = data["Body"]["Data"]["Site"]
        inverters = data["Body"]["Data"].get("Inverters", {})

        ts = datetime.now().replace(microsecond=0).isoformat(sep=" ")
        pv_kw = abs((site.get("P_PV") or 0) / 1000)
        grid_kw = abs((site.get("P_Grid") or 0) / 1000)
        battery_kw = abs((site.get("P_Akku") or 0) / 1000)
        load_kw = abs((site.get("P_Load") or 0) / 1000)

        soc = None
        if isinstance(inverters, dict) and inverters:
            inv1 = inverters.get("1") or next(iter(inverters.values()), None)
            if isinstance(inv1, dict):
                soc = inv1.get("SOC")

        # cast soc to float if possible
        try:
            soc = float(soc) if soc is not None else None
        except Exception:
            soc = None

        return {
            "ts": ts,
            "pv_kw": float(pv_kw),
            "grid_kw": float(grid_kw),
            "battery_kw": float(battery_kw),
            "load_kw": float(load_kw),
            "soc": soc,
        }
    except Exception:
        logging.exception("Fronius: Fehler beim Abrufen")
        return None


def _to_float(x: Any) -> Optional[float]:
    if x is None:
        return None
    s = str(x).strip().replace(",", ".")
    if s == "":
        return None
    try:
        return float(s)
    except Exception:
        return None


def fetch_bmk(settings: Settings) -> Optional[Dict[str, Any]]:
    try:
        r = requests.get(settings.bmk_url, timeout=settings.http_timeout_s)
        r.raise_for_status()

        lines = r.text.split("\n")
        values = [line.strip() for line in lines if line.strip()]

        if len(values) <= 12:
            logging.warning("BMK: zu wenige Werte (%s) – Antwort evtl. unvollständig", len(values))
            return None

        ts = datetime.now().replace(microsecond=0).isoformat(sep=" ")
        return {
            "ts": ts,
            "boiler_temp": _to_float(values[1]),
            "outside_temp": _to_float(values[2]),
            "buffer_top": _to_float(values[4]),
            "buffer_mid": _to_float(values[5]),
            "buffer_bottom": _to_float(values[6]),
            "hot_water": _to_float(values[12]),
        }
    except Exception:
        logging.exception("BMK: Fehler beim Abrufen")
        return None


def insert_fronius(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO fronius (ts, pv_kw, grid_kw, battery_kw, load_kw, soc)
        VALUES (:ts, :pv_kw, :grid_kw, :battery_kw, :load_kw, :soc);
        """,
        row,
    )


def insert_bmk(conn: sqlite3.Connection, row: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO bmk (ts, boiler_temp, outside_temp, buffer_top, buffer_mid, buffer_bottom, hot_water)
        VALUES (:ts, :boiler_temp, :outside_temp, :buffer_top, :buffer_mid, :buffer_bottom, :hot_water);
        """,
        row,
    )


class PeriodicWorker(threading.Thread):
    def __init__(self, name: str, interval_s: float, fetch_fn, insert_fn, conn: sqlite3.Connection, settings: Settings, stop_event: threading.Event):
        super().__init__(name=name, daemon=True)
        self.interval_s = float(interval_s)
        self.fetch_fn = fetch_fn
        self.insert_fn = insert_fn
        self.conn = conn
        self.settings = settings
        self.stop_event = stop_event

    def run(self) -> None:
        next_t = time.monotonic()
        while not self.stop_event.is_set():
            now = time.monotonic()
            if now < next_t:
                self.stop_event.wait(next_t - now)
                continue

            row = self.fetch_fn(self.settings)
            if row:
                try:
                    self.insert_fn(self.conn, row)
                except Exception:
                    logging.exception("%s: Fehler beim DB-Insert", self.name)

            next_t += self.interval_s


def main() -> None:
    settings = load_settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)

    log_path = settings.data_dir / "collector.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.FileHandler(log_path, encoding="utf-8"), logging.StreamHandler()],
    )

    logging.info("Collector (SQLite) startet. DATA_DIR=%s DB=%s", settings.data_dir, settings.db_file)
    logging.info("Fronius URL: %s", settings.fronius_url)
    logging.info("BMK URL: %s", settings.bmk_url)

    conn = _connect(settings.db_file)
    init_db(conn)

    stop_event = threading.Event()

    def _handle_sig(signum, frame):
        logging.info("Signal %s erhalten – stoppe...", signum)
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_sig)
    signal.signal(signal.SIGINT, _handle_sig)

    fr_worker = PeriodicWorker(
        name="fronius",
        interval_s=settings.fronius_interval_s,
        fetch_fn=fetch_fronius,
        insert_fn=insert_fronius,
        conn=conn,
        settings=settings,
        stop_event=stop_event,
    )
    bmk_worker = PeriodicWorker(
        name="bmk",
        interval_s=settings.bmk_interval_s,
        fetch_fn=fetch_bmk,
        insert_fn=insert_bmk,
        conn=conn,
        settings=settings,
        stop_event=stop_event,
    )

    fr_worker.start()
    bmk_worker.start()

    while not stop_event.is_set():
        time.sleep(0.5)

    try:
        conn.close()
    except Exception:
        pass
    logging.info("Collector beendet.")


if __name__ == "__main__":
    main()
