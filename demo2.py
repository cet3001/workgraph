"""Real workers, real SIGKILL, real release artifacts. No network required."""
import argparse
from collections import deque
from datetime import datetime, timezone
import fcntl
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import threading
import time

from workgraph.core import WorkGraph
from workgraph.queries import Queries
from workgraph.server import create_server


TASKS = [
    ('fetch', 'Read release fixture', []),
    ('extract', 'Extract release items', ['fetch']),
    ('counts', 'Count changes by kind', ['extract']),
    ('crosscheck', 'Cross-check approved items', ['extract']),
    ('render', 'Render release notes', ['counts', 'crosscheck']),
    ('manifest', 'Hash release artifacts', ['render']),
    ('verify', 'Verify artifact checksums', ['manifest']),
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ui', action='store_true')
    args = parser.parse_args()
    # Serialize demo resets, keeping the lock inode in place across runs.
    with open('.demo2.lock', 'a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            parser.error('demo2 is already running in this directory')
        return run(args.ui)


def run(ui):
    for path in (Path('demo.rlf'), Path('demo_out')):
        if path.is_symlink():
            raise RuntimeError(f'Refusing to reset symlink: {path}')
        if path.is_dir():
            shutil.rmtree(path)
        elif path.exists():
            raise RuntimeError(f'Expected demo directory: {path}')
    graph = WorkGraph('demo.rlf')
    queries = Queries(graph)
    processes, readers = {}, []
    logs = deque(maxlen=200)
    log_lock = threading.Lock()
    killed = set()
    server = None
    deadline = time.monotonic() + 50

    def snapshot():
        state = graph.snapshot()
        for agent in state['agents']:
            agent['process_down'] = agent['name'] in killed
        with log_lock:
            state['worker_output'] = list(logs)
        return state

    def tee(process):
        for line in process.stdout:
            print(line, end='', flush=True)
            with log_lock:
                logs.append(line.rstrip())
        process.stdout.close()

    def spawn(name):
        env = dict(os.environ, PYTHONUNBUFFERED='1')
        root = str(Path(__file__).resolve().parent)
        env['PYTHONPATH'] = root + os.pathsep + env.get('PYTHONPATH', '')
        process = subprocess.Popen(
            [sys.executable, '-m', 'workgraph', 'worker', name,
             '--module', 'examples.pipeline_handlers', '--db', 'demo.rlf',
             '--heartbeat-interval', '.5', '--claim-ttl', '3'],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=env)
        processes[name] = process
        thread = threading.Thread(target=tee, args=(process,), daemon=True)
        readers.append(thread)
        thread.start()
        return process

    def wait_for(predicate):
        while not predicate():
            if time.monotonic() > deadline:
                raise TimeoutError('Pipeline did not finish within 50 seconds')
            for name, process in processes.items():
                if name not in killed and process.poll() is not None:
                    raise RuntimeError(f'{name} exited unexpectedly: {process.returncode}')
            time.sleep(.05)

    try:
        if ui:
            server = create_server(snapshot, get_lineage=queries.lineage,
                                   get_critical_path=queries.critical_path)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            print('WorkGraph UI on http://localhost:8471', flush=True)
        # Stage starts, not claims: all ownership and recovery use Worker.run.
        graph.add_task(TASKS[0][1], TASKS[0][0])
        scout = spawn('scout')
        wait_for(lambda: graph.get_task('fetch')['status'] == 'done')
        scout.terminate()
        scout.wait(timeout=5)
        if scout.returncode != 0:
            raise RuntimeError('scout did not stop cleanly')
        del processes['scout']
        for tid, title, deps in TASKS[1:]:
            graph.add_task(title, tid, deps)
        builder = spawn('builder')
        progress = Path('demo_out/crosscheck-builder.progress')
        wait_for(lambda: progress.exists() and int(progress.read_text().split('/')[0] or 0) >= 2)
        os.kill(builder.pid, signal.SIGKILL)
        builder.wait(timeout=5)
        killed.add('builder')
        print(f'Killed builder pid={builder.pid}; its lease will expire naturally.', flush=True)
        spawn('closer')
        wait_for(lambda: all(t['status'] == 'done' for t in graph.snapshot()['tasks']))
        print('\nAll 7/7 tasks done. Report: demo_out/report.md', flush=True)
        print('lineage(crosscheck):', flush=True)
        labels = {'task_added': 'created', 'completed': 'done'}
        for event in queries.lineage('crosscheck'):
            timestamp = datetime.fromtimestamp(event['ts'], timezone.utc).isoformat(timespec='milliseconds')
            print(f"  {timestamp} {labels.get(event['kind'], event['kind'])}({event['agent']})", flush=True)
        if ui:
            print('Board stays open. Ctrl+C to quit.', flush=True)
            while True:
                time.sleep(1)
        return 0
    finally:
        for process in processes.values():
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        for thread in readers:
            thread.join(timeout=2)
        if server:
            server.shutdown()
            server.server_close()
        graph.close()


if __name__ == '__main__':
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        pass
