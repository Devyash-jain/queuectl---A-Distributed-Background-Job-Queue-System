import argparse
import json
import os
import signal
from typing import Optional
from .storage import Storage
from .worker import start_workers
import psutil
import sys
import codecs
import uuid
import datetime
from .dashboard import start_dashboard


def cmd_enqueue(args):
    db = Storage(args.db)

    # 1) If --file was provided
    if args.file:
        with codecs.open(args.file, "r", encoding="utf-8-sig") as f:
            job_json = f.read()

    # 2) If job JSON was piped via STDIN
    elif not sys.stdin.isatty():
        job_json = sys.stdin.read()

    # 3) Otherwise, use the positional argument
    else:
        job_json = args.job_json

    try:
        payload = json.loads(job_json)
    except json.JSONDecodeError as e:
        raise SystemExit(f"Invalid JSON input: {e}")

    # Fill defaults from config
    if "max_retries" not in payload:
        payload["max_retries"] = int(db.config_get("max_retries", "3"))
    if "state" not in payload:
        payload["state"] = "pending"

    job_id = db.enqueue(payload)
    print(job_id)

def cmd_worker_start(args):
    db = Storage(args.db)
    db.clear_stop()
    start_workers(args.db, args.count)

def cmd_worker_stop(args):
    db = Storage(args.db)
    db.request_stop()
    print("Stopping workers...")

    for w in db.list_workers():
        pid = int(w["pid"])
        try:
            p = psutil.Process(pid)
            p.terminate()   # graceful
        except psutil.NoSuchProcess:
            pass

    print("Stop requested. Workers will exit after finishing their current job.")

def cmd_status(args):
    db = Storage(args.db)
    s = db.stats()
    print(json.dumps(s, indent=2))

def cmd_list(args):
    db = Storage(args.db)
    rows = db.list_jobs(state=args.state, limit=args.limit)
    for r in rows:
        print(json.dumps(r))

def cmd_dlq_list(args):
    db = Storage(args.db)
    rows = db.list_dlq(limit=args.limit)
    for r in rows:
        print(json.dumps(r))

def cmd_dlq_retry(args):
    db = Storage(args.db)
    ok = db.retry_from_dlq(args.job_id)
    print("OK" if ok else "NOT_FOUND")

def cmd_config_set(args):
    db = Storage(args.db)
    # supported keys: max_retries (default for enqueue), backoff_base
    db.config_set(args.key, str(args.value))
    print("OK")

def cmd_config_get(args):
    db = Storage(args.db)
    v = db.config_get(args.key)
    if v is None:
        print("NOT_SET")
    else:
        print(v)

def cmd_config_show(args):
    db = Storage(args.db)
    print(json.dumps(db.all_config(), indent=2))

def cmd_run(args):
    db = Storage(args.db)

    job_id = f"job_{datetime.datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    payload = {
        "id": job_id,
        "command": args.command,
        "state": "pending",
        "attempts": 0,
        "max_retries": int(db.config_get("max_retries", "3")),
        "created_at": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z",
        "updated_at": datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    }

    db.enqueue(payload)
    print(job_id)

def build_parser():
    p = argparse.ArgumentParser(prog="queuectl", description="CLI-based background job queue system")
    p.add_argument("--db", default="queuectl.db", help="Path to SQLite DB")
    sub = p.add_subparsers(dest="cmd", required=True)

    # Enqueue
    pe = sub.add_parser("enqueue", help="Add a new job to the queue")
    pe.add_argument("job_json", nargs="?", help="Job JSON string")
    pe.add_argument("--file", "-f", help="Path to job JSON file")
    pe.set_defaults(func=cmd_enqueue)

    # Worker
    pw = sub.add_parser("worker", help="Manage workers")
    pw_sub = pw.add_subparsers(dest="wcmd", required=True)
    pws = pw_sub.add_parser("start", help="Start one or more workers")
    pws.add_argument("--count", type=int, default=1)
    pws.set_defaults(func=cmd_worker_start)

    pwx = pw_sub.add_parser("stop", help="Stop running workers gracefully")
    pwx.set_defaults(func=cmd_worker_stop)

    # Status
    ps = sub.add_parser("status", help="Show job states & active workers")
    ps.set_defaults(func=cmd_status)

    # List
    pl = sub.add_parser("list", help="List jobs by state")
    pl.add_argument("--state", default="pending")
    pl.add_argument("--limit", type=int, default=100)
    pl.set_defaults(func=cmd_list)

    # DLQ
    pd = sub.add_parser("dlq", help="Dead Letter Queue operations")
    pd_sub = pd.add_subparsers(dest="dcmd", required=True)
    pdl = pd_sub.add_parser("list", help="List DLQ entries")
    pdl.add_argument("--limit", type=int, default=100)
    pdl.set_defaults(func=cmd_dlq_list)

    pdr = pd_sub.add_parser("retry", help="Retry a DLQ job by id")
    pdr.add_argument("job_id")
    pdr.set_defaults(func=cmd_dlq_retry)

    # Config
    pc = sub.add_parser("config", help="Manage configuration")
    pc_sub = pc.add_subparsers(dest="ccmd", required=True)

    pcs = pc_sub.add_parser("set", help="Set a config option")
    pcs.add_argument("key", choices=["max_retries", "backoff_base"])
    pcs.add_argument("value")
    pcs.set_defaults(func=cmd_config_set)

    pcg = pc_sub.add_parser("get", help="Get a config value")
    pcg.add_argument("key", choices=["max_retries", "backoff_base"])
    pcg.set_defaults(func=cmd_config_get)

    pca = pc_sub.add_parser("show", help="Show all config")
    pca.set_defaults(func=cmd_config_show)

    # run = enqueue shortcut
    prun = sub.add_parser("run", help="Enqueue a job using a command string")
    prun.add_argument("command", help="Shell command to run")
    prun.set_defaults(func=cmd_run)

    pweb = sub.add_parser("dashboard", help="Start web dashboard")
    pweb.add_argument("--port", type=int, default=8000)
    pweb.set_defaults(func=lambda args: start_dashboard(args.db, args.port))

    return p

def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()
