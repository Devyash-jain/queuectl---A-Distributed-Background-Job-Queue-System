import sqlite3
import time
import json
import os
from contextlib import contextmanager
from typing import Optional, List, Dict

_SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,                 -- user-provided unique-job-id
    command TEXT NOT NULL,
    state TEXT NOT NULL,                 -- pending, processing, completed, failed, dead
    attempts INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    eta REAL,                            -- epoch seconds when job becomes eligible
    created_at TEXT NOT NULL,            -- ISO8601
    updated_at TEXT NOT NULL             -- ISO8601
);

CREATE INDEX IF NOT EXISTS idx_jobs_state_eta ON jobs(state, eta);
CREATE INDEX IF NOT EXISTS idx_jobs_eta ON jobs(eta);

CREATE TABLE IF NOT EXISTS dlq (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    payload TEXT NOT NULL,               -- JSON snapshot of the job row
    last_error TEXT,
    failed_at TEXT NOT NULL              -- ISO8601
);

CREATE TABLE IF NOT EXISTS workers (
    pid INTEGER PRIMARY KEY,
    started_at TEXT NOT NULL,
    queues TEXT NOT NULL DEFAULT 'default'
);

CREATE TABLE IF NOT EXISTS control (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS config (
    k TEXT PRIMARY KEY,
    v TEXT NOT NULL
);
"""

def _now_iso() -> str:
    import datetime
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

class Storage:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self):
        with self._connect() as con:
            con.executescript(_SCHEMA)

    @contextmanager
    def _txn(self, con: sqlite3.Connection):
        con.execute("BEGIN IMMEDIATE")
        try:
            yield
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise

    # ---------- Job primitives ----------

    def enqueue(self, job: Dict) -> str:
        # Supply defaults
        job_id = job["id"]
        created_at = job.get("created_at") or _now_iso()
        updated_at = job.get("updated_at") or created_at
        state = job.get("state") or "pending"
        max_retries = int(job.get("max_retries", 3))
        attempts = int(job.get("attempts", 0))
        command = job["command"]
        eta = None  # eligible immediately unless delayed in future iterations

        with self._connect() as con:
            con.execute("""
                INSERT INTO jobs(id, command, state, attempts, max_retries, eta, created_at, updated_at)
                VALUES(?,?,?,?,?,?,?,?)
            """, (job_id, command, state, attempts, max_retries, eta, created_at, updated_at))
        return job_id

    def list_jobs(self, state: Optional[str] = None, limit: int = 100) -> list:
        q = "SELECT * FROM jobs"
        params = []
        if state and state != "all":
            q += " WHERE state = ?"
            params.append(state)
        q += " ORDER BY updated_at DESC LIMIT ?"
        params.append(limit)
        with self._connect() as con:
            return [dict(r) for r in con.execute(q, params)]

    def get_job(self, job_id: str) -> Optional[dict]:
        with self._connect() as con:
            r = con.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
            return dict(r) if r else None

    def delete_job(self, job_id: str) -> bool:
        with self._connect() as con:
            cur = con.execute("DELETE FROM jobs WHERE id=?", (job_id,))
            return cur.rowcount > 0

    def pop_pending_for_run(self) -> Optional[dict]:
        # atomically claim one pending job whose eta is null or due
        now = time.time()
        with self._connect() as con:
            with self._txn(con):
                row = con.execute("""
                    SELECT * FROM jobs
                    WHERE state='pending' AND (eta IS NULL OR eta <= ?)
                    ORDER BY eta IS NOT NULL, created_at ASC
                    LIMIT 1
                """, (now,)).fetchone()
                if not row:
                    return None
                con.execute("UPDATE jobs SET state='processing', updated_at=? WHERE id=?",
                            (_now_iso(), row["id"]))
                return dict(row)

    def mark_completed(self, job_id: str):
        with self._connect() as con:
            con.execute("UPDATE jobs SET state='completed', updated_at=?, eta=NULL WHERE id=?",
                        (_now_iso(), job_id))

    def mark_failed_retry(self, job_id: str, attempts: int, delay_seconds: float):
        next_eta = time.time() + delay_seconds
        with self._connect() as con:
            con.execute("""UPDATE jobs
                           SET state='failed', attempts=?, eta=?, updated_at=?
                           WHERE id=?""",
                        (attempts, next_eta, _now_iso(), job_id))

    def move_to_dlq(self, job_row: dict, last_error: str):
        payload = json.dumps(job_row)
        with self._connect() as con:
            with self._txn(con):
                con.execute("INSERT INTO dlq(job_id, payload, last_error, failed_at) VALUES(?,?,?,?)",
                            (job_row["id"], payload, last_error, _now_iso()))
                con.execute("UPDATE jobs SET state='dead', updated_at=?, eta=NULL WHERE id=?",
                            (_now_iso(), job_row["id"]))

    def retry_from_dlq(self, job_id: str) -> bool:
        with self._connect() as con:
            with self._txn(con):
                dl = con.execute("SELECT * FROM dlq WHERE job_id=?", (job_id,)).fetchone()
                if not dl:
                    return False
                # reset job to pending
                con.execute("""UPDATE jobs
                               SET state='pending', attempts=0, eta=NULL, updated_at=?
                               WHERE id=?""", (_now_iso(), job_id))
                con.execute("DELETE FROM dlq WHERE job_id=?", (job_id,))
                return True

    def list_dlq(self, limit: int = 100) -> list:
        with self._connect() as con:
            rows = con.execute("SELECT * FROM dlq ORDER BY failed_at DESC LIMIT ?", (limit,)).fetchall()
            return [dict(r) for r in rows]

    # ---------- Workers & control ----------

    def register_worker(self, pid: int, queues: str = "default"):
        with self._connect() as con:
            con.execute("INSERT OR REPLACE INTO workers(pid, started_at, queues) VALUES(?, ?, ?)",
                        (pid, _now_iso(), queues))

    def deregister_worker(self, pid: int):
        with self._connect() as con:
            con.execute("DELETE FROM workers WHERE pid=?", (pid,))

    def list_workers(self) -> list:
        with self._connect() as con:
            return [dict(r) for r in con.execute("SELECT * FROM workers", ())]

    def request_stop(self):
        with self._connect() as con:
            con.execute("INSERT OR REPLACE INTO control(k, v) VALUES('stop_requested', '1')")

    def clear_stop(self):
        with self._connect() as con:
            con.execute("DELETE FROM control WHERE k='stop_requested'")

    def is_stop_requested(self) -> bool:
        with self._connect() as con:
            r = con.execute("SELECT v FROM control WHERE k='stop_requested'").fetchone()
            return r is not None

    # ---------- Config ----------

    def config_set(self, key: str, value: str):
        with self._connect() as con:
            con.execute("INSERT OR REPLACE INTO config(k, v) VALUES(?, ?)", (key, value))

    def config_get(self, key: str, default: Optional[str] = None) -> str:
        with self._connect() as con:
            r = con.execute("SELECT v FROM config WHERE k=?", (key,)).fetchone()
            return r["v"] if r else default

    def all_config(self) -> dict:
        with self._connect() as con:
            rows = con.execute("SELECT k,v FROM config").fetchall()
            return {r["k"]: r["v"] for r in rows}

    # ---------- Stats ----------
    def stats(self) -> dict:
        with self._connect() as con:
            by_state = {r["state"]: r["cnt"] for r in con.execute(
                "SELECT state, COUNT(*) as cnt FROM jobs GROUP BY state"
            ).fetchall()}
            total = sum(by_state.values())
            dlq = con.execute("SELECT COUNT(*) FROM dlq").fetchone()[0]
            workers = con.execute("SELECT COUNT(*) FROM workers").fetchone()[0]
            return {"states": by_state, "total": total, "dlq": dlq, "workers": workers}
