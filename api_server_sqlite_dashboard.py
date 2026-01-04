#!/usr/bin/env python3
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Optional

from flask import Flask, jsonify, Response

app = Flask(__name__)

DATA_DIR = Path(os.getenv("DATA_DIR", "/home/laurenz2/datenerfassung")).expanduser().resolve()
DB_FILE = DATA_DIR / os.getenv("DB_FILE", "energy.db")
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "5000"))

_conn: Optional[sqlite3.Connection] = None


def _connect() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        _conn = sqlite3.connect(str(DB_FILE), timeout=30, check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL;")
        _conn.execute("PRAGMA synchronous=NORMAL;")
        _conn.execute("PRAGMA busy_timeout=30000;")

        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS fronius(
                ts TEXT PRIMARY KEY,
                pv_kw REAL,
                grid_kw REAL,
                battery_kw REAL,
                load_kw REAL,
                soc REAL
            );
            """
        )
        _conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bmk(
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
    return _conn


@app.get("/api/health")
def health():
    return jsonify(status="ok", data_dir=str(DATA_DIR), db=str(DB_FILE), db_exists=DB_FILE.exists())


@app.get("/api/latest")
def latest():
    c = _connect()
    f = c.execute("SELECT * FROM fronius ORDER BY ts DESC LIMIT 1").fetchone()
    b = c.execute("SELECT * FROM bmk ORDER BY ts DESC LIMIT 1").fetchone()
    return jsonify(fronius=dict(f) if f else None, bmk=dict(b) if b else None)


@app.get("/dashboard")
def dashboard():
    html = """<!doctype html><html><head><meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Energy Dashboard</title>
    <style>
      body{font-family:system-ui;background:#111;color:#eee;margin:0;padding:16px}
      .grid{display:grid;grid-template-columns:repeat(2,1fr);gap:12px}
      .card{background:#222;border-radius:14px;padding:14px}
      .label{color:#9ab;font-size:12px}
      .val{font-size:34px;font-weight:800}
      @media (max-width:700px){.grid{grid-template-columns:1fr}}
    </style></head><body>
    <h2 style="margin:0 0 12px 0">Energy Dashboard</h2>
    <div class="grid">
      <div class="card"><div class="label">PV</div><div class="val" id="pv">–</div></div>
      <div class="card"><div class="label">Load</div><div class="val" id="load">–</div></div>
      <div class="card"><div class="label">Grid</div><div class="val" id="grid">–</div></div>
      <div class="card"><div class="label">SoC</div><div class="val" id="soc">–</div></div>
      <div class="card"><div class="label">Kessel</div><div class="val" id="boiler">–</div></div>
      <div class="card"><div class="label">Außen</div><div class="val" id="outside">–</div></div>
    </div>
    <script>
      async function tick(){
        const r = await fetch('/api/latest',{cache:'no-store'});
        const j = await r.json();
        if(j.fronius){
          pv.textContent = (j.fronius.pv_kw ?? 0).toFixed(1) + " kW";
          load.textContent = (j.fronius.load_kw ?? 0).toFixed(1) + " kW";
          grid.textContent = (j.fronius.grid_kw ?? 0).toFixed(1) + " kW";
          soc.textContent = (j.fronius.soc ?? 0).toFixed(1) + " %";
        }
        if(j.bmk){
          boiler.textContent = (j.bmk.boiler_temp ?? 0).toFixed(1) + " °C";
          outside.textContent = (j.bmk.outside_temp ?? 0).toFixed(1) + " °C";
        }
      }
      setInterval(tick,2000); tick();
    </script></body></html>"""
    return Response(html, mimetype="text/html")


if __name__ == "__main__":
    app.run(host=HOST, port=PORT)
