# Phase 3 local verification

The branch started at `91d51d7` after fetching origin and confirming a clean main with zero commits ahead or behind. The original suite passed all 29 tests on Windows CPython 3.12.14 in 2.928 s before implementation.

## Scheduling and regression tests

The final suite contains the original 29 tests and 24 new cases. Both backends run the same scheduling contract tests. Gates establish the order of concurrent operations, a controlled monotonic clock exercises the claim boundary and delayed retries, and a real timer test exercises autonomous expiration. Each suite process has a 90-second outer timeout.

| Windows interpreter | Passed | Suite duration |
| --- | ---: | ---: |
| CPython 3.11.18 | 53 | 6.688 s |
| CPython 3.12.14 | 53 | 6.785 s |
| CPython 3.15.2 | 53 | 9.885 s |

Commands ran in PowerShell from the repository root.

```powershell
.\.venv\interpreters\cpython-3.11.18-windows-x86_64-none\python.exe -c "import subprocess, sys; print(sys.version, flush=True); subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-q'], check=True, timeout=90)"
.\.venv\Scripts\python.exe -c "import subprocess, sys; print(sys.version, flush=True); subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-q'], check=True, timeout=90)"
python -c "import subprocess, sys; print(sys.version, flush=True); subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-q'], check=True, timeout=90)"
.\.venv\Scripts\python.exe -c "import subprocess, sys; [subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-q'], check=True, timeout=90) for _ in range(5)]"
.\.venv\Scripts\python.exe -m ruff format --check taskforge tests
.\.venv\Scripts\python.exe -m ruff check taskforge tests
.\.venv\Scripts\python.exe -m mypy --strict taskforge tests
```

## Findings during development

- A process retry test passed a temporary manager event directly to submission. The server could reclaim the event after serialization and before child reconstruction. Each request now retains its original inputs for the lifetime of its attempts. The test checks that the retry succeeds with the same event.
- Admission now expires overdue entries under the queue lock before checking space. An immediate submission can reuse a due slot even if the timer has not run yet.
- Windows IPC timeouts use millisecond waits. The controller rounds positive submillisecond waits up and splits very long waits. The first long-delay test exceeded the platform's supported duration and correctly failed validation. The corrected test uses 3,000,000 seconds and advances the clock to trigger the retry.

The checks cover real Windows spawn. Other operating systems and scheduling performance under production load were outside this verification.
