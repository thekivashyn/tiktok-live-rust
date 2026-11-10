"""Suspend only this harness's own child receiver; never suspend the host or other apps."""
import ctypes as c
from ctypes import wintypes as w
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / 'reports/watchdog-investigation'
kernel = c.WinDLL('kernel32', use_last_error=True)
nt = c.WinDLL('ntdll')
kernel.OpenProcess.argtypes = [w.DWORD, w.BOOL, w.DWORD]
kernel.OpenProcess.restype = w.HANDLE
kernel.CloseHandle.argtypes = [w.HANDLE]
nt.NtSuspendProcess.argtypes = nt.NtResumeProcess.argtypes = [w.HANDLE]
nt.NtSuspendProcess.restype = nt.NtResumeProcess.restype = c.c_long


def run(version, pair):
    binary = (Path(shutil.which('node')) if version == 'connector' else
              OUT / 'baseline/instrumented_receiver.exe' if version == '0.7.1' else
              ROOT / 'target/release/examples/matched_receiver_candidate.exe')
    prefix = OUT / f'pause-{version}-{pair}'
    ready = Path(str(prefix) + '.ready.json')
    assert not ready.exists(), 'Use a fresh pair label'
    child = server = None
    handle = None
    suspended = False
    with open(str(prefix) + '.server.log', 'wb') as server_log, open(str(prefix) + '.receiver.log', 'wb') as receiver_log, open(str(prefix) + '.output.json', 'wb') as output:
        try:
            server = subprocess.Popen([shutil.which('node'), str(ROOT / 'benchmarks/connector/server.mjs'), str(ROOT / 'reports/matched-corpus/mixed.bin'), str(prefix), '4096', '1', '0', '0'], stdout=server_log, stderr=server_log, creationflags=subprocess.CREATE_NO_WINDOW)
            until = time.monotonic() + 15
            while not ready.exists():
                assert time.monotonic() < until and server.poll() is None
                time.sleep(.02)
            port = json.loads(ready.read_text())['port']
            command = ([str(binary), str(ROOT / 'benchmarks/connector/receiver.mjs'), str(port), '60']
                       if version == 'connector' else [str(binary), str(port), '60'])
            child = subprocess.Popen(command, stdout=output, stderr=receiver_log, env=os.environ | {'TLR_BENCH_IDLE_SECONDS': '3'}, creationflags=subprocess.CREATE_NO_WINDOW)
            until = time.monotonic() + 10
            while (('"event":"connected"' not in Path(str(prefix) + '.server.log').read_text()) if version == 'connector' else ('"state":"receiving"' not in Path(str(prefix) + '.receiver.log').read_text())):
                assert time.monotonic() < until and child.poll() is None
                time.sleep(.01)
            if version == 'connector':
                time.sleep(.25)
            # Open by the PID returned by our own Popen, not a name/process search.
            handle = kernel.OpenProcess(0x0800, False, child.pid)
            if not handle:
                raise OSError(c.get_last_error(), 'OpenProcess for own child failed')
            assert nt.NtSuspendProcess(handle) == 0
            suspended = True
            pause_at = time.monotonic()
            try:
                time.sleep(4)
            finally:
                assert nt.NtResumeProcess(handle) == 0
                suspended = False
            pause_seconds = time.monotonic() - pause_at
            child.wait(timeout=30)
            server.wait(timeout=5)
        finally:
            if suspended:
                nt.NtResumeProcess(handle)
            if handle:
                kernel.CloseHandle(handle)
            for process in (child, server):
                if process is not None and process.poll() is None:
                    process.kill()
                    process.wait(timeout=5)
    receiver = json.loads(Path(str(prefix) + '.output.json').read_text())
    engine = receiver.get('report', receiver)
    fixture = json.loads(Path(str(prefix) + '.server.json').read_text())
    checks = {
        'complete_output': receiver['output']['events'] == 32769 and receiver['output']['crc32'] == fixture['expected_crc32'] and receiver['output']['serialized_bytes'] == fixture['expected_bytes'],
        'same_connection': engine['connections'] == 1,
        'no_fixture_failure': not fixture['failures'],
        'cleanup': fixture['active'] == fixture['active_tasks'] == 0,
    }
    report = {'version': version, 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(), 'pause_seconds': pause_seconds, 'watchdog_seconds': 3, 'checks': checks, 'receiver': receiver, 'fixture': fixture}
    Path(str(prefix) + '.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'version': version, 'pair': pair, 'pause_seconds': pause_seconds, 'checks': checks, 'reconnects': engine['reconnects'], 'last_error': engine.get('last_connection_error'), 'watchdog_disconnects':engine.get('watchdog_disconnects')}), flush=True)
    if version == '0.7.2':
        assert all(checks.values()), checks
    return report


if __name__ == '__main__':
    for pair in range(1, 4):
        if '--node' in sys.argv:
            run('connector', pair)
            continue
        for version in (('0.7.1', '0.7.2') if pair % 2 else ('0.7.2', '0.7.1')):
            run(version, pair)
