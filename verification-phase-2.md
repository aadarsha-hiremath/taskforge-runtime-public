# Phase 2 local verification

All commands ran locally in PowerShell on Windows. The branch base was freshly fetched `origin/main` at `c88b3c1`.

## Thread regression before process development

```powershell
.\.venv\Scripts\python.exe -m unittest tests.test_runtime -v
```

All 16 Phase 1 tests passed in 0.080 s on CPython 3.12.14. The new test places the worker and cancelling caller at their queue operations, releases them in both orders and checks both the terminal state and the exact number of task executions.

## Combined thread and process suites

```powershell
.\.venv\interpreters\cpython-3.11.18-windows-x86_64-none\python.exe -c "import subprocess, sys; print(sys.version, flush=True); subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-v'], check=True, timeout=90)"
.\.venv\Scripts\python.exe -c "import subprocess, sys; print(sys.version, flush=True); subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-v'], check=True, timeout=90)"
python -c "import subprocess, sys; print(sys.version, flush=True); subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-v'], check=True, timeout=90)"
```

| CPython | Tests passed | Suite duration |
| --- | ---: | ---: |
| 3.11.18 | 29 | 3.178 s |
| 3.12.14 | 29 | 3.226 s |
| 3.15.2 | 29 | 4.454 s |

All three runs included 16 thread tests and 13 process tests. The process tests exercise actual spawned children, simultaneous workers, CPU work, memory isolation, loss of a busy or idle worker, blocked producers, shutdown, serialization in both directions, partial startup failure and OS handle cleanup.

```powershell
.\.venv\Scripts\python.exe -c "import subprocess, sys; [subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-q'], check=True, timeout=90) for _ in range(5)]"
```

Five additional fresh Python 3.12 runs passed all 29 tests each. No tests were skipped. Other operating systems were not exercised.

During development, six process tests encountered Windows error 2 when the sandbox could not open the test manager's default named pipe. The same 10-test development suite passed outside the sandbox. The final fixture uses an ephemeral loopback address for its event proxies. The runtime itself uses private pipes. Strict mypy also identified an export and a test fixture annotation that were corrected before the final checks.

## Format, lint and type checks

```powershell
.\.venv\Scripts\python.exe -m ruff format --check taskforge tests
.\.venv\Scripts\python.exe -m ruff check taskforge tests
.\.venv\Scripts\python.exe -m mypy --strict taskforge tests
```

Ruff reported 12 files formatted and all lint checks passed. Strict mypy reported zero issues across the same 12 source and test files.

## Packaging and installed demos

```powershell
.\.venv\Scripts\python.exe -m pip install build
.\.venv\Scripts\python.exe -m build --no-isolation
.\.venv\Scripts\python.exe -m venv .venv/package-check
.\.venv\package-check\Scripts\python.exe -m pip install --no-index --no-deps dist/taskforge_runtime-0.2.0-py3-none-any.whl
```

The build produced a source archive and built a pure-Python wheel from it. The wheel installed into a clean virtual environment without runtime dependencies. Inspection of the first source archive found the test package initializer missing. The source manifest now includes it and the verification records.

After rebuilding, the following commands reinstalled the wheel and ran all 29 tests from the extracted source archive. Both exited successfully. The extracted suite took 2.765 s.

```powershell
.\.venv\package-check\Scripts\python.exe -m pip install --force-reinstall --no-index --no-deps dist/taskforge_runtime-0.2.0-py3-none-any.whl
.\.venv\Scripts\python.exe -c "import tarfile, tempfile, subprocess, sys, pathlib; tmp = tempfile.TemporaryDirectory(); archive = tarfile.open('dist/taskforge_runtime-0.2.0.tar.gz'); archive.extractall(tmp.name, filter='data'); root = pathlib.Path(tmp.name) / 'taskforge_runtime-0.2.0'; assert (root / 'verification-phase-2.md').is_file(); subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-v'], cwd=root, check=True, timeout=90); tmp.cleanup()"
```

The following commands ran from the repository's **parent directory**:

```powershell
.\taskforge-runtime\.venv\package-check\Scripts\taskforge.exe --backend process --workers 2 --queue-capacity 1 2 3 4
.\taskforge-runtime\.venv\package-check\Scripts\taskforge.exe --workers 2 --queue-capacity 1 2 3 4
.\taskforge-runtime\.venv\package-check\Scripts\python.exe -m pip check
```

Both installed CLI backends printed three completed jobs with squares 4, 9 and 16. The process demo reported two distinct worker PIDs. The dependency check reported no broken requirements.
