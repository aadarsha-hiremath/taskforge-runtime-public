# Phase 5 engineering audit

Base `35455c12b0d56159cbdcbff03e0310fa7353af37`, Windows x64, 14 November 2026. Clean main, fetched origin/main and FETCH_HEAD matched before branching. The first fetch required account selection. Fetching with the stored repository-owner account succeeded without a global configuration change.

## Test evidence

| CPython | Base suite | Final suite |
| --- | --- | --- |
| 3.11.18 | 63 passed, 14.191 s | 67 passed, 19.197 s |
| 3.12.14 | 63 passed, 18.700 s | 67 passed, 23.591 s |
| 3.14.7 | 63 passed, 15.724 s | 67 passed, 19.872 s |
| 3.15.2 | 63 passed, 20.418 s | 67 passed, 25.652 s |

No skips. Each interpreter ran the complete suite in a separate process with a 150-second deadline. In PowerShell set `$python` to the desired interpreter executable. The base static checks covered 16 files under taskforge and tests. The final Ruff and strict mypy gates included all 19 source, test and benchmark files.

```powershell
& $python -c "import sys,subprocess; print(sys.version,flush=True); subprocess.run([sys.executable,'-m','unittest','discover','-q'],check=True,timeout=150)"
.\.venv\Scripts\python.exe -m ruff check taskforge tests benchmarks
.\.venv\Scripts\python.exe -m ruff format --check taskforge tests benchmarks
.\.venv\Scripts\python.exe -m mypy --strict taskforge tests benchmarks
.\.venv\Scripts\python.exe -c "import subprocess,sys; [subprocess.run([sys.executable,'-m','unittest','discover','-q'],check=True,timeout=150) for _ in range(3)]"
```

Before the final exception-hook regression, three additional CPython 3.15.2 processes passed 66/66 in 20.658, 20.577 and 21.499 seconds. All lint, formatting and type checks passed. During benchmark development strict mypy identified an incorrectly reused local name for two different job result types, and Ruff flagged dictionary construction style. Both were corrected.

After the exception-hook fix, three further fresh 3.15.2 processes passed all 67 tests in 18.662, 19.364 and 18.943 seconds. The full four-interpreter matrix above and all three static gates were rerun on that fix.

## Correctness and resource audit

- **Locking and outcomes.** Admission, cancellation, claim, retry reservation, closure and snapshot counters use one scheduler condition. The per-job condition protects the outcome. Cancellation releases that lock before calling the scheduler. Task execution and IPC are outside the scheduler lock. Synchronous log handlers run at transition points and must avoid re-entry and blocking. Cyclic application dependencies can still deadlock a saturated pool.
- **Traceback retention defect.** Five reads of a failed thread job grew its traceback from 3 to 11 frames, retaining previous reader frames. The new regression failed on base code and passed after capturing the original failure traceback and restoring it before each raise. Thread exceptions retain identity and original worker frames. A follow-up test used a custom exception that intercepts traceback access. It reproduced a five-second result timeout with a job stuck running on the initial fix. Reading the native exception descriptor under the job lock bypassed the hook. The regression now verifies both original exception identity and continued worker availability. Holding a failed job still retains its original traceback and exception.
- **Queue and timing.** Retry-capable claims reserve a slot. Delayed retries remain pending and cancellable. Eligibility and expiry are recomputed under the scheduler lock. The fairness bound counts claims of eligible work and does not bound time or admission order. Default shutdown drains retries. Cancel-pending shutdown suppresses subsequent retries. Cooperative tasks can finish after their latest claim time.
- **Resource stress.** Both backends ran a 12-job, three-slot retry burst with two workers and four result readers. Each backend finished 12 logical jobs in 24 attempts, with 12 retries and zero reservations left. The inherited scenarios cover 128 submissions per backend, concurrent snapshots, cancellation races, occupied-worker expiry, shutdown during backoff, partial spawn failure, last-worker loss and repeated joins. Tests check actual thread liveness, active child PIDs and closed pipe/process handles. This establishes cleanup for those cases, not a lifetime memory-leak bound. Native heap growth, OS handle counts over long soaks and thermal behavior were not measured.
- **Process supervision.** One private connection and at most one assignment per child preserve attribution on loss. The parent fails uncertain work without replay and closes admission after losing the final worker. Pipe waits include monotonic scheduling deadlines. A slow serialization hook, handler or hung callable can delay progress. A joining shutdown can wait indefinitely.
- **Portability.** Explicit spawn and importable tasks avoid reliance on fork-inherited state. Windows pipe types, wait limits and millisecond conversion have platform-specific handling. POSIX uses Connection and the shared spawn path. Four Windows Python versions exercised real process execution. No 3.13 executable was found. WSL reported it was not installed and macOS was unavailable. POSIX behavior remains unverified.

## Source hygiene and licensing

Reviewed tracked inventory, ignore rules, packaging configuration and all 18 commits reachable from local refs at the branch point. A local Git blob scan covered 66 unique historical blobs for private-key headers, common GitHub/OpenAI/AWS token formats and credential assignments. The only candidate path was `tests/test_observability.py`, containing a deliberately fake log-redaction marker. The largest baseline blob was the 20,238-byte README. No dedicated secret-scanner executable was found on PATH. Pattern matching cannot certify the absence of secrets, and the scan did not inspect unreachable objects, untracked user files or remote services.

Home-specific paths in two old verification records were normalized in the current tree. Prior commits and their Git author identities remain in history. The tracked distribution excludes virtual environments, local build directories and caches. The benchmark JSON is a small intentional measurement artifact containing platform/CPU details and no machine name. Runtime dependencies are empty. Packaging, typing and lint tools are development dependencies and are not vendored in the wheel. The source has no selected project license. License choice and any eventual public version or publication decision belong to the owner. Phase 5 retains 0.4.0.

## Measurement and reproduction

The [benchmark report](docs/benchmarks.md) gives the methodology, variability and interpretation. The [raw run](benchmarks/windows-short.json) includes all 36 backend/worker/workload combinations, three measured samples each and four lifecycle configurations. The optional extended run also passed with 54 configurations, seven measured samples each, six lifecycle configurations and 128 jobs per batch in 29.808 seconds. It was used for bounded stress verification, separately from the recorded short measurements. No comparative claims about third-party systems were tested.

## Distributions and installed examples

The local builder used the development environment's setuptools. Twine strict passed on both artifacts. The wheel has 11 runtime modules and five dist-info files. Its metadata declares version 0.4.0, Python >=3.11 and zero runtime dependencies. The sdist also contains the tests, documentation, benchmark sources, measurement JSON and verification records. Archive inspection checked wheel CRCs, runtime file membership and required source files. Relative links in README, the four technical guides and this record resolved in the checkout.

```powershell
.\.venv\Scripts\python.exe -m build --no-isolation --outdir dist/phase5
.\.venv\Scripts\python.exe -m twine check --strict dist/phase5/taskforge_runtime-0.4.0.tar.gz dist/phase5/taskforge_runtime-0.4.0-py3-none-any.whl
```

Independent checks used temporary directories outside the checkout, empty virtual environments and offline installs. The block below gives the subprocess commands with the generated paths represented by variables. Set `$dev` to the absolute development Python executable, `$base` to the interpreter under test and `$out` to the absolute distribution directory. Run from a temporary working directory. Use a fresh `$dest` for each interpreter and source build.

```powershell
& $base -m venv --without-pip $dest
$target = Join-Path $dest 'Scripts/python.exe'
& $dev -m pip --disable-pip-version-check --python $target install --no-index --no-deps "$out/taskforge_runtime-0.4.0-py3-none-any.whl"
& $dev -m pip --disable-pip-version-check --python $target check
& $target -I -c "from pathlib import Path; import sys,taskforge; from importlib.metadata import version; assert Path(taskforge.__file__).resolve().is_relative_to(Path(sys.prefix).resolve()); assert version('taskforge-runtime')=='0.4.0'"
& $target -I -m taskforge --stats --backend thread --workers 2 2 3 4
& "$dest/Scripts/taskforge.exe" --stats --backend process --workers 2 2 3 4
& $dev -m pip --disable-pip-version-check wheel --no-cache-dir --no-index --no-deps --no-build-isolation "$out/taskforge_runtime-0.4.0.tar.gz" -w rebuilt
```

Wheel installs on 3.11.18, 3.12.14, 3.14.7 and 3.15.2 passed the import-path assertion and pip dependency check. Each CLI produced 4, 9 and 16, three completed jobs and two exited workers. A wheel rebuilt offline from the sdist passed the same checks in a fifth fresh 3.15.2 environment. The initial rebuild encountered a sandbox-denied pip cache and one launcher lookup failed from the temporary directory. Reruns disabled the cache and used the absolute interpreter executable.

Each of the six Python examples from the archived README, architecture and observability guides ran as a separate saved script with the installed Python using `-I` and a 30-second limit. The README printed 32, then 6 and `1 True`. The architecture examples printed 6 and 32. The snapshot example observed one running worker, zero available workers and an expected exit. Adding one job to the logging example emitted structured records. The extracted sdist also ran its complete suite with `python -m unittest discover -q` from its own root.
