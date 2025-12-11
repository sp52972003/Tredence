"""
persistence.py
Simple SQLite persistence for graphs and runs.
Stores JSON strings for portability and simplicity.
"""

import sqlite3
import json
from typing import Optional

DB_PATH = "workflow.db"

def _get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn

def init_db():
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS graphs (
        graph_id TEXT PRIMARY KEY,
        graph_json TEXT NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS runs (
        run_id TEXT PRIMARY KEY,
        run_json TEXT NOT NULL,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)
    conn.commit()
    conn.close()

def save_graph(graph_id: str, graph_obj: dict):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("REPLACE INTO graphs (graph_id, graph_json) VALUES (?, ?)",
                (graph_id, json.dumps(graph_obj)))
    conn.commit()
    conn.close()

def load_graph(graph_id: str) -> Optional[dict]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT graph_json FROM graphs WHERE graph_id = ?", (graph_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return json.loads(row[0])

def save_run(run_id: str, run_obj: dict):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("REPLACE INTO runs (run_id, run_json, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)",
                (run_id, json.dumps(run_obj)))
    conn.commit()
    conn.close()

def load_run(run_id: str) -> Optional[dict]:
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT run_json FROM runs WHERE run_id = ?", (run_id,))
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    return json.loads(row[0])

def update_run(run_id: str, run_obj: dict):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE runs SET run_json = ?, updated_at = CURRENT_TIMESTAMP WHERE run_id = ?",
                (json.dumps(run_obj), run_id))
    conn.commit()
    conn.close()

# initialize DB on import
init_db()
