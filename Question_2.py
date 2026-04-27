'''
The Coding Task (20 Minutes)
The Problem: Write a Python 'Wrapper' script that every Ultron job must run through. This wrapper needs to prevent the server from dying. Create A Semaphore or Rate Limiter: Even if 1,000 jobs are triggered, the script should ensure only $X$ number of "Heavy" jobs (like the Scribd_cpm_map in your screenshot) actually execute their heavy logic at the exact same time. Use maximum 80 code lines.
'''

import argparse
import sqlite3
import tempfile
import time
import uuid
from pathlib import Path
 
DB = Path(tempfile.gettempdir()) / "ultron_job_limiter.db"
STALE_SECONDS = 60
 
def init_db(conn):
    conn.execute(
        "create table if not exists heavy_jobs(id text primary key, ts real)"
    )
 
 
def acquire_slot(max_workers, timeout=30):
    deadline = time.monotonic() + timeout
    job_id = str(uuid.uuid4())
    with sqlite3.connect(DB, timeout=timeout, isolation_level=None) as conn:
        conn.execute("pragma journal_mode=WAL")
        init_db(conn)
        while time.monotonic() < deadline:
            conn.execute("begin immediate")
            now = time.time()
            conn.execute(
                "delete from heavy_jobs where ts < ?", (now - STALE_SECONDS,)
            )
            active = conn.execute("select count(*) from heavy_jobs").fetchone()[0]
            if active < max_workers:
                conn.execute(
                    "insert into heavy_jobs(id, ts) values(?,?)",
                    (job_id, now),
                )
                conn.commit()
                return job_id
            conn.rollback()
            time.sleep(0.1)
    return None
 
 
def release_slot(job_id):
    with sqlite3.connect(DB, timeout=10, isolation_level=None) as conn:
        conn.execute("pragma journal_mode=WAL")
        init_db(conn)
        conn.execute("begin immediate")
        conn.execute("delete from heavy_jobs where id=?", (job_id,))
        conn.commit()
 
 
def heavy_job():
    print("Heavy job started")
    time.sleep(5)
    print("Heavy job finished")
 
 
def main():
    parser = argparse.ArgumentParser(description="Ultron job wrapper")
    parser.add_argument("--heavy", action="store_true")
    parser.add_argument("--max-heavy", type=int, default=3)
    args = parser.parse_args()
 
    if args.heavy:
        job_id = acquire_slot(args.max_heavy)
        if not job_id:
            print("Rate limit reached, skipping heavy logic")
            return 1
        try:
            heavy_job()
        finally:
            release_slot(job_id)
    else:
        print("Light job executed")
    return 0
 
 
if __name__ == "__main__":
    raise SystemExit(main())
