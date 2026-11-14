# Phase 1 verification record

All checks ran locally on Windows. Commands below ran in PowerShell from the repository root, except for the installed CLI check.

## Environment

```powershell
& "$HOME/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe" -m venv .venv
.\.venv\Scripts\python.exe -m pip install ruff mypy setuptools wheel
.\.venv\Scripts\python.exe -m pip install uv
$env:UV_CACHE_DIR = '.venv/uv-cache'
.\.venv\Scripts\uv.exe python install 3.11 --install-dir .venv/interpreters --no-bin
```

The development environment used CPython 3.12.14, Ruff 0.17.5 and mypy 2.4.1. The uv command downloaded CPython 3.11.18 and encountered a Windows access error creating its minor-version link. Rerunning the same command reported the interpreter installed. The tests below used its executable directly. The system Python was 3.15.2.

## Complete suites

| Executed command | Result |
| --- | --- |
| `.\.venv\interpreters\cpython-3.11.18-windows-x86_64-none\python.exe -m unittest discover -v` | 15 passed, 0.061 s |
| `.\.venv\Scripts\python.exe -m unittest discover -v` | 15 passed, 0.067 s |
| `python -m unittest discover -v` | 15 passed, 0.077 s |

Repeated the entire suite in 25 fresh Python 3.12 processes with a 15-second timeout per process. All 25 exited successfully.

```powershell
.\.venv\Scripts\python.exe -c "import subprocess, sys; [subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-q'], check=True, timeout=15, capture_output=True) for _ in range(25)]; print('25 full suite runs passed')"
```

## Format, lint and types

| Executed command | Result |
| --- | --- |
| `.\.venv\Scripts\python.exe -m ruff format --check .` | 8 files already formatted |
| `.\.venv\Scripts\python.exe -m ruff check .` | All checks passed |
| `.\.venv\Scripts\python.exe -m mypy taskforge tests` | Strict check passed for 8 source files with Python 3.11 as the target |

During development, Ruff flagged the broad worker exception handler and test style issues. The handler has a line-scoped exemption with a regression test for `SystemExit`. The test style findings were fixed before the final checks.

## Installation and CLI

| Executed command | Result |
| --- | --- |
| `.\.venv\Scripts\python.exe -m pip install .` | Built a wheel and installed taskforge-runtime 0.1.0 |
| `.\.venv\Scripts\python.exe -m pip check` | No broken requirements found |
| `python -m taskforge --workers 2 --queue-capacity 1 2 3 4` | Three completed jobs with squares 4, 9 and 16 |
| `.\.venv\Scripts\python.exe -c "from taskforge import Runtime; pool = Runtime(workers=2, queue_capacity=4); job = pool.submit(pow, 2, exp=5); print(job.result(timeout=2)); pool.shutdown()"` | Printed 32 |

From the parent directory, the installed console entry point also printed three completed jobs with squares 4, 9 and 16 and exited successfully:

```powershell
.\taskforge-runtime\.venv\Scripts\taskforge.exe --workers 2 --queue-capacity 1 2 3 4
```
