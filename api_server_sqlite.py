#!/usr/bin/env python3
"""
Flask API Server (SQLite) for the Collector node (Raspberry Pi 3)

Endpoints:
- GET /api/health
- GET /api/latest
- GET /api/fronius?hours=48 (or days=7) (or since=...&until=...)
- GET /api/bmk?hours=48 (or days=7) (or since=...&until=...)
- GET /api/fronius?limit=5000
- GET /api/bmk?limit=5000

Environment:
    DATA_DIR="/home/pi/datenerfassung"
    DB_FILE="energy.db"
    HOST="0.0.0.0"
    PORT="5000"
    AUTH_TOKEN=""         # optional. If set: require header X-Auth-Token: <token>
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from flask import Flask, abort, jsonify, request

app = Flask(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", ".")).expanduser().resolve()
DB_FILE = DATA_DIR / os.getenv("DB_FILE", "energy.db")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "5000"))
AUTH_TOKEN = os.getenv("AUTH_TOKEN", "").strip()

# keep a single read-only connection per process
_conn: Optional[sqlite3.Connection] = None


def _require_token_if_configured() -> None:
    if not AUTH_TOKEN:
        return
    token = request.headers.get("X-Auth-Token", "")
    if token != AUTH_TOKEN:
        abort(401)


def _connect() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        DB_FILE.parent.mkdir(parents=True, exist_ok=True)
        _conn = sqlite3.connect(
            str(DB_FILE),
            timeout=30,
            isolation_level=None,
            check_same_thread=False,
        )
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL;")
        _conn.execute("PRAGMA synchronous=NORMAL;")
        _conn.execute("PRAGMA busy_timeout=30000;")
    return _conn


def _parse_dt(s: str) -> datetime:
    s = s.strip()
    if "T" in s:
        return datetime.fromisoformat(s)
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def _window_from_args() -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (since_iso, until_iso) as TEXT timestamps (YYYY-MM-DD HH:MM:SS) matching collector.
    """
    since = request.args.get("since")
    until = request.args.get("until")

    if since or until:
        since_dt = _parse_dt(since) if since else None
        until_dt = _parse_dt(until) if until else None
    else:
        hours = request.args.get("hours")
        days = request.args.get("days")
        since_dt = None
        until_dt = None
        now = datetime.now().replace(microsecond=0)
        if hours:
            try:
                h = float(hours)
                since_dt = now - timedelta(hours=h)
                until_dt = now
            except Exception:
                pass
        elif days:
            try:
                d = float(days)
                since_dt = now - timedelta(days=d)
                until_dt = now
            except Exception:
                pass

    since_txt = since_dt.replace(microsecond=0).isoformat(sep=" ") if since_dt else None
    until_txt = until_dt.replace(microsecond=0).isoformat(sep=" ") if until_dt else None
    return since_txt, until_txt


def _query_rows(table: str, since: Optional[str], until: Optional[str], limit: int) -> List[Dict[str, Any]]:
    conn = _connect()
    limit = max(1, min(int(limit), 200000))  # guard
    params: Dict[str, Any] = {"limit": limit}

    where = ""
    if since and until:
        where = "WHERE ts >= :since AND ts <= :until"
        params["since"] = since
        params["until"] = until
    elif since:
        where = "WHERE ts >= :since"
        params["since"] = since
    elif until:
        where = "WHERE ts <= :until"
        params["until"] = until

    sql = f"SELECT * FROM {table} {where} ORDER BY ts ASC LIMIT :limit"
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def _query_latest(table: str) -> Optional[Dict[str, Any]]:
    conn = _connect()
    r = conn.execute(f"SELECT * FROM {table} ORDER BY ts DESC LIMIT 1").fetchone()
    return dict(r) if r else None


@app.get("/api/health")
def health():
    _require_token_if_configured()
    exists = DB_FILE.exists()
    return jsonify(status="ok", data_dir=str(DATA_DIR), db=str(DB_FILE), db_exists=exists)


@app.get("/api/latest")
def latest():
    _require_token_if_configured()
    return jsonify(fronius=_query_latest("fronius"), bmk=_query_latest("bmk"))


@app.get("/api/fronius")
def fronius():
    _require_token_if_configured()
    since, until = _window_from_args()
    limit = int(request.args.get("limit", "5000"))
    data = _query_rows("fronius", since, until, limit)
    return jsonify(rows=data)


@app.get("/api/bmk")
def bmk():
    _require_token_if_configured()
    since, until = _window_from_args()
    limit = int(request.args.get("limit", "5000"))
    data = _query_rows("bmk", since, until, limit)
    return jsonify(rows=data)


if __name__ == "__main__":
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    app.run(host=HOST, port=PORT)
