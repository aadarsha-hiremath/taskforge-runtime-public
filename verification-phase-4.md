# Phase 4 local verification

All checks ran in PowerShell on Windows x64. Both clean local main and freshly fetched origin/main pointed to `f289c9c78d16c26008856796adbeae2900403d65`. The initial live reference check needed Git authentication. The retried fetch succeeded before implementation.

## Unit and regression suites

The base suite passed 55 tests on CPython 3.15.2. Phase 4 adds eight tests and extends the existing deterministic cancellation-versus-claim race to check exact counter values for each winner.

| Windows CPython | Passed | Suite duration |
| --- | ---: | ---: |
| 3.11.18 | 63 | 19.671 s |
| 3.12.14 | 63 | 25.903 s |
| 3.15.2 | 63 | 27.706 s |

All requested interpreters were available. There were no skips. The durations are unittest output from local runs, not throughput measurements.

```powershell
.\.venv\interpreters\cpython-3.11.18-windows-x86_64-none\python.exe -c "import subprocess, sys; print(sys.version, flush=True); subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-q'], check=True, timeout=150)"
& "$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe" -c "import subprocess, sys; print(sys.version, flush=True); subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-q'], check=True, timeout=150)"
.\.venv\Scripts\python.exe -c "import subprocess, sys; print(sys.version, flush=True); subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-q'], check=True, timeout=150)"
```

## Reliability scenarios

- Four seeded producers (731–734) each submitted 32 jobs to three workers with three queue slots. They varied priority and attempted pending cancellation. Two concurrent readers each checked 100 immutable snapshots for conservation of logical jobs, unique worker identities, and bounded capacity including reservations. The same scenario ran on both backends. The scheduler determines the cancellation winner.
- Three separate two-process pools each lost both workers during retry-enabled attempts while 12 jobs waited. Each pool reported 14 final failures, two claims, zero retries and two failed process records with exit code 23. Repeated shutdown left the initial active-child set intact. A separate test verified continued service and retained crash history after losing only one worker.
- A controlled monotonic clock exercised expiry during retry backoff. Gates pinned a running job during shutdown. Other tests covered zero-window expiry, rejected admissions, handler failure, log redaction, stable thread identity and unexpected thread-loop failure.
- Five additional fresh interpreter runs passed **8/8** observability tests each. Each run had a 90-second outer timeout. Task and coordination waits were bounded. Gates opened in finally blocks and pool context managers joined the workers.

```powershell
.\.venv\Scripts\python.exe -c "import subprocess, sys; [subprocess.run([sys.executable, '-m', 'unittest', 'tests.test_observability', '-q'], check=True, timeout=90) for _ in range(5)]"
```

## Static checks

```powershell
.\.venv\Scripts\python.exe -m ruff format --check taskforge tests
.\.venv\Scripts\python.exe -m ruff check taskforge tests
.\.venv\Scripts\python.exe -m mypy --strict taskforge tests
```

Ruff reported 16 formatted files and all lint checks passed. Strict mypy reported zero issues across 16 files. During development, Ruff flagged loop-variable captures in tests. Strict mypy rejected deliberate direct writes to frozen fields in the immutability test. The final tests use scoped helpers and dynamic attribute names to exercise runtime rejection while preserving static read-only types.

## Findings and limits

The initial health implementation used the thread's current name for lookup. A task can rename itself, so each worker now carries its original identifier independent of its display name. A targeted regression renames a thread, submits another job and checks final health. An injected failure immediately after claim verifies that a lost worker's assigned job and residual queue both reach terminal states, the reservation is released and the attempt is never replayed.

Worker health describes what the scheduler has observed. A lost connection can be reported before its process exit code is known. A blocked callable still appears running. Logging uses synchronous application-owned handlers. The tests cover Windows spawn, bounded loads and selected failure interleavings. Linux, macOS, long soaks and throughput benchmarking were outside this run.

## Distribution and installed CLI

Build 1.6.1 produced the source distribution and built the wheel from it. Twine 7.0.0 passed strict metadata checks for both artifacts. The wheel passed its ZIP CRC check. Archive inspection confirmed the runtime module in both artifacts and the Phase 4 tests and verification record in the source distribution.

```powershell
.\.venv\Scripts\python.exe -m build --no-isolation --outdir dist/phase4
.\.venv\Scripts\python.exe -m venv .venv/phase4-wheel
.\.venv\Scripts\python.exe -m twine check --strict dist/phase4/taskforge_runtime-0.4.0.tar.gz dist/phase4/taskforge_runtime-0.4.0-py3-none-any.whl
.\.venv\phase4-wheel\Scripts\python.exe -m pip install --no-index --no-deps dist/phase4/taskforge_runtime-0.4.0-py3-none-any.whl
.\.venv\Scripts\python.exe -c "import tarfile, zipfile; from pathlib import Path; d=Path('dist/phase4'); z=zipfile.ZipFile(next(d.glob('*.whl'))); assert z.testzip() is None; assert 'taskforge/observability.py' in z.namelist(); t=tarfile.open(next(d.glob('*.tar.gz'))); assert all(any(p.endswith('/'+f) for p in t.getnames()) for f in ('tests/test_observability.py','verification-phase-4.md','taskforge/observability.py')); print('archive checks passed')"
```

The independent environment installed the wheel with index access and dependency resolution disabled. From the **parent directory of the checkout**, both CLI backends printed squares 4, 9 and 16. Both JSON snapshots recorded three accepted, started and completed jobs, three attempts, zero failures and two exited workers. Both child processes reported exit code zero. Pip found no broken requirements, and the isolated import resolved version 0.4.0 from the new environment's site-packages.

```powershell
.\taskforge-runtime\.venv\phase4-wheel\Scripts\taskforge.exe --backend thread --workers 2 --queue-capacity 1 --stats 2 3 4
.\taskforge-runtime\.venv\phase4-wheel\Scripts\taskforge.exe --backend process --workers 2 --queue-capacity 1 --stats 2 3 4
.\taskforge-runtime\.venv\phase4-wheel\Scripts\python.exe -m pip check
.\taskforge-runtime\.venv\phase4-wheel\Scripts\python.exe -I -c "import taskforge; from importlib.metadata import version; assert 'site-packages' in taskforge.__file__; print(version('taskforge-runtime'), taskforge.__file__)"
```
