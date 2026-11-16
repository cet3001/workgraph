"""Run Worker.run() in a dedicated process (also exposed by the CLI)."""
from __future__ import annotations

import math
import signal
import threading
from typing import Callable

from .core import WorkGraph


class Worker:
    def __init__(self, name: str, store_path: str, handler: Callable[[dict], str],
                 heartbeat_interval: float = 5.0, claim_ttl: float = 45.0):
        if not name.strip() or not callable(handler):
            raise ValueError("Worker needs a name and callable handler")
        if not (math.isfinite(heartbeat_interval) and math.isfinite(claim_ttl)
                and 0 < heartbeat_interval < claim_ttl):
            raise ValueError("Require 0 < heartbeat_interval < claim_ttl, both finite")
        self.name = name
        self.store_path = store_path
        self.handler = handler
        self.heartbeat_interval = heartbeat_interval
        self.claim_ttl = claim_ttl
        self._stop = threading.Event()

    def stop(self) -> None:
        """Finish the current task, then stop claiming new work."""
        self._stop.set()

    def run(self) -> None:
        if threading.current_thread() is not threading.main_thread():
            raise RuntimeError("Run the worker on the main thread of its process")
        graph = WorkGraph(self.store_path)
        heartbeat_stop = threading.Event()
        active_lock = threading.Lock()
        leases = {}
        errors = []

        def heartbeat():
            try:
                while not heartbeat_stop.wait(self.heartbeat_interval):
                    with active_lock:
                        graph.heartbeat(self.name, self.claim_ttl, dict(leases))
            except Exception as exc:
                errors.append(exc)
                self.stop()

        previous = signal.getsignal(signal.SIGTERM)
        signal.signal(signal.SIGTERM, lambda signum, frame: self.stop())
        thread = threading.Thread(target=heartbeat, name=f"{self.name}-heartbeat", daemon=True)
        started = False
        try:
            graph.register_agent(self.name)
            thread.start()
            started = True
            while not self._stop.is_set():
                graph.recover_orphans(stale_after=max(30.0, self.claim_ttl))
                for candidate in graph.claimable():
                    if self._stop.is_set():
                        break
                    with active_lock:
                        if not graph.claim(self.name, candidate["id"], self.claim_ttl):
                            continue
                        task = graph.get_task(candidate["id"])
                        leases[task["id"]] = task["lease_token"]
                    try:
                        result = self.handler(task)
                        if not isinstance(result, str):
                            raise TypeError("handler(task) must return str")
                    except Exception as exc:
                        graph.fail(self.name, task["id"], str(exc), lease_token=task["lease_token"])
                    else:
                        graph.complete(self.name, task["id"], result, lease_token=task["lease_token"])
                    finally:
                        with active_lock:
                            leases.pop(task["id"], None)
                    break
                self._stop.wait(min(self.heartbeat_interval, 0.25))
            if errors:
                raise RuntimeError("Worker heartbeat failed") from errors[0]
        finally:
            heartbeat_stop.set()
            if started:
                thread.join()
            try:
                graph.mark_dead(self.name)
            finally:
                graph.close()
                signal.signal(signal.SIGTERM, previous)
