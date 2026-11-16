"""Packet 2: real pipeline recovery and board API contracts."""
import io
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from workgraph.core import WorkGraph
from workgraph.queries import Queries
from workgraph.server import PAGE, create_server

ROOT = Path(__file__).resolve().parents[1]


def test_demo2_headless(tmp_path):
    started = time.monotonic()
    # A process group lets a timeout clean up workers as well as the parent.
    process = subprocess.Popen([sys.executable, str(ROOT / 'demo2.py')], cwd=tmp_path,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               text=True, start_new_session=True)
    try:
        out, err = process.communicate(timeout=85)
    finally:
        if process.poll() is None:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
    assert process.returncode == 0, (out, err)
    assert time.monotonic() - started < 90
    report = tmp_path / 'demo_out/report.md'
    assert report.exists()
    assert 'Cross-check: 4 approved items matched.' in report.read_text()
    assert 'fix: 2' in report.read_text()
    assert (tmp_path / 'demo_out/verified.txt').read_text() == '5 artifacts verified\n'
    assert 'Killed builder pid=' in out
    assert out.index('orphaned(builder)') < out.index('reclaimed(closer)') < out.index('done(closer)')
    assert 'All 7/7 tasks done.' in out
    graph = WorkGraph(str(tmp_path / 'demo.rlf'))
    try:
        assert all(t['status'] == 'done' for t in graph.snapshot()['tasks'])
        events = Queries(graph).lineage('crosscheck')
        assert [e['kind'] for e in events] == [
            'task_added', 'claimed', 'agent_died', 'orphaned', 'recovered', 'reclaimed', 'completed']
        assert events[1]['agent'] == 'builder'
        assert events[-1]['agent'] == 'closer'
        assert events[2]['ts'] - events[1]['ts'] >= 3
    finally:
        graph.close()
    interrupted = int((tmp_path / 'demo_out/crosscheck-builder.progress').read_text().split('/')[0])
    assert 1 < interrupted < 16
    assert (tmp_path / 'demo_out/crosscheck-closer.progress').read_text() == '16/16\n'


class Request:
    """Exercise the HTTP handler without making network calls."""
    def __init__(self, path):
        self.input = io.BytesIO(f'GET {path} HTTP/1.0\r\n\r\n'.encode())
        self.output = bytearray()

    def makefile(self, *args):
        return self.input

    def sendall(self, data):
        self.output.extend(data)


def test_board_queries_and_health(tmp_path):
    graph = WorkGraph(str(tmp_path / 'board.rlf'))
    try:
        graph.add_task('First', 'one')
        graph.add_task('Second', 'two', ['one'])
        graph.register_agent('builder', .5)
        graph.claim('builder', 'one', 3)
        q = Queries(graph)
        with create_server(graph.snapshot, 0, get_lineage=q.lineage,
                           get_critical_path=q.critical_path) as server:
            def get(path):
                request = Request(path)
                server.RequestHandlerClass(request, ('127.0.0.1', 0), server)
                headers, body = bytes(request.output).split(b'\r\n\r\n', 1)
                return headers, body

            headers, body = get('/api/critical-path')
            assert b'200 OK' in headers
            assert [t['id'] for t in json.loads(body)] == ['one', 'two']
            _, body = get('/api/lineage/%6fne')
            assert [e['kind'] for e in json.loads(body)] == ['task_added', 'claimed']
            _, body = get('/api/snapshot')
            state = json.loads(body)
            assert state['tasks'][0]['lease_expires'] > time.time()
            agent = next(a for a in state['agents'] if a['name'] == 'builder')
            assert agent['heartbeat_interval'] == .5
            graph.mark_dead('builder')
            _, body = get('/api/snapshot')
            assert next(a for a in json.loads(body)['agents'] if a['name'] == 'builder')['state'] == 'dead'
            assert json.loads(get('/api/lineage/missing')[1]) == []
            assert b'404' in get('/api/unknown')[0]
            assert b'Task lineage' in get('/')[1]
        assert '.innerHTML' not in PAGE
    finally:
        graph.close()
