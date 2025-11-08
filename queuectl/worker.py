import signal
import subprocess
import time
import os
from multiprocessing import Process, Event
from .storage import Storage
from .backoff import compute_delay

class Worker:
    def __init__(self, db_path: str):
        self.db = Storage(db_path)
        self.stop_event = Event()
        self.pid = os.getpid()

    def _handle_signal(self, signum, frame):
        # finish current job gracefully
        self.stop_event.set()

    def run_forever(self):
        # register signals
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)
        # register worker
        self.db.register_worker(os.getpid())
        try:
            while not self.stop_event.is_set() and not self.db.is_stop_requested():
                job = self.db.pop_pending_for_run()
                if not job:
                    print("[worker] waiting for jobs...")
                    time.sleep(1)
                    continue
                print(f"[worker] picked job {job['id']} -> {job['command']}")
                job_id = job["id"]
                command = job["command"]
                attempts = int(job["attempts"])
                max_retries = int(job["max_retries"])

                # Execute the shell command
                try:
                    result = subprocess.run(command, shell=True)
                    if result.returncode == 0:
                        print(f"[worker] ✅ completed {job_id}")
                        self.db.mark_completed(job_id)
                        continue
                    else:
                        # failure path
                        print(f"[worker] ❌ failed (attempt {attempts}/{max_retries}) exit={result.returncode}")
                        attempts += 1
                        if attempts > max_retries:
                            self.db.move_to_dlq(job, f"Exit {result.returncode}")
                        else:
                            base = float(self.db.config_get("backoff_base", "2"))
                            delay = compute_delay(base, attempts)
                            self.db.mark_failed_retry(job_id, attempts, delay)
                except FileNotFoundError as e:
                    attempts += 1
                    if attempts > max_retries:
                        self.db.move_to_dlq(job, f"FileNotFoundError: {e}")
                    else:
                        base = float(self.db.config_get("backoff_base", "2"))
                        delay = compute_delay(base, attempts)
                        self.db.mark_failed_retry(job_id, attempts, delay)
                except Exception as e:
                    attempts += 1
                    if attempts > max_retries:
                        self.db.move_to_dlq(job, f"{type(e).__name__}: {e}")
                    else:
                        base = float(self.db.config_get("backoff_base", "2"))
                        delay = compute_delay(base, attempts)
                        self.db.mark_failed_retry(job_id, attempts, delay)
        finally:
            self.db.deregister_worker(os.getpid())

def start_workers(db_path: str, count: int):
    procs = []
    for _ in range(count):
        w = Worker(db_path)
        p = Process(target=w.run_forever, daemon=False)
        p.start()
        procs.append(p)
    # wait
    try:
        for p in procs:
            p.join()
    except KeyboardInterrupt:
        pass
    finally:
        for p in procs:
            if p.is_alive():
                p.terminate()
                p.join()
