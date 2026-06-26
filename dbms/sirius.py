import os
import re
import subprocess
import threading
import time

from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS, Result, DBMSDescription
from util import logger, sql

# Default location of the Sirius-enabled DuckDB binary on this machine. The
# Sirius extension is statically linked, so no LOAD is required and the CUDA/cuDF
# libraries resolve via the binary's RPATH (see INTEGRATE_SIRIUS_OLAPBENCH.md).
DEFAULT_BIN = "/home/jungmair/sirius/cloned_repo/build/legacy-release/duckdb"

# DuckDB CLI prints `Run Time (s): real 0.466 user ... sys ...` per executed
# statement when `.timer on` is set. We use the `real` value as the query time.
_RUNTIME_RE = re.compile(r"Run Time \(s\): real ([0-9.]+)")
# A genuine DuckDB exception renders as "<Kind> Error: <message>" (Catalog
# Error:, Parser Error:, Binder Error:, Out of Memory Error:, ...). A crashing
# process emits the markers below instead. Both mean the statement failed.
_ERROR_RE = re.compile(r"Error:|terminate called|Aborted|Segmentation|bad_alloc|std::|what\(\):", re.IGNORECASE)
# Sirius emits this banner (not a real exception) when it cannot run a query on
# the GPU and transparently re-runs it on DuckDB CPU. The statement still
# completes and reports a valid time, so this is a *fallback*, not a failure.
_FALLBACK_RE = re.compile(r"fallback to duckdb", re.IGNORECASE)
# Subset of error messages that mean we ran out of (GPU/host) memory.
_OOM_PATTERNS = ["out of memory", "bad_alloc", "cannot allocate", "outofmemory", "out_of_memory"]


class Sirius(DBMS):
    """Sirius — the GPU-native SQL engine shipped as a DuckDB extension.

    Sirius is driven through the bundled DuckDB CLI binary. Tables are loaded
    into a DuckDB-native `.duckdb` file with ordinary (CPU) SQL, and every
    benchmark query is executed on the GPU via `call gpu_processing('<SQL>')`.

    A single long-lived CLI session is used so that the GPU column cache built
    up by the harness' warmup runs survives into the measured repetitions
    (Sirius' cache does not persist across processes). `SIRIUS_DISABLE=1` is set
    for the process to keep the transparent "Super Sirius" path from grabbing the
    GPU; we call `gpu_processing` explicitly instead.
    """

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)

        self._bin = params.get("bin", DEFAULT_BIN)
        # GPU buffer-manager sizing (see §8 of the integration notes). `cache`
        # holds the raw cached columns, `proc` the query intermediates.
        self._gpu_cache_size = params.get("gpu_cache_size", "12 GB")
        self._gpu_processing_size = params.get("gpu_processing_size", "16 GB")
        self._pinned_memory_size = params.get("pinned_memory_size", self._gpu_processing_size)
        # Cache columns in pinned host memory instead of GPU memory (lets the
        # cache exceed GPU memory, slower). Must be SET before gpu_buffer_init.
        self._use_pin_memory_for_caching = bool(params.get("use_pin_memory_for_caching", False))
        # When Sirius can't run a query on the GPU it prints a banner and falls
        # back to DuckDB CPU, still returning a correct result. By default we
        # record that (flagged as cpu_fallback); set this to mark such queries as
        # errors instead ("unsupported on GPU").
        self._fail_on_cpu_fallback = bool(params.get("fail_on_cpu_fallback", False))

        # Reuse a single .duckdb file across runs so data is generated once.
        self._db_file = os.path.join(self._db_dir, params.get("dbfile", benchmark.unique_name + ".duckdb"))

        self._query_phase = False
        self._sentinel_seq = 0
        self._timed_out = False
        self.process = None
        self._db_exists = False

    @property
    def name(self) -> str:
        return "sirius"

    @property
    def version(self) -> str:
        return self._version if self._version != "latest" else "gpu_processing"

    # ------------------------------------------------------------------ #
    # Session lifecycle
    # ------------------------------------------------------------------ #
    def __enter__(self):
        os.makedirs(self._db_dir, exist_ok=True)
        # Capture existence *before* starting the CLI: opening a path creates an
        # empty database file, so we must decide whether to (re)load beforehand.
        self._db_exists = os.path.exists(self._db_file) and os.path.getsize(self._db_file) > 0
        logger.log_verbose_dbms(f"Using Sirius DuckDB binary {self._bin}", self)
        logger.log_verbose_dbms(f"{'Reusing' if self._db_exists else 'Creating'} Sirius database {self._db_file}", self)
        self._start_session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop_process()

    def _start_session(self):
        env = dict(os.environ)
        # Mandatory: keep the transparent gpu_execution engine from initializing
        # on extension load and grabbing all GPU/pinned memory.
        env["SIRIUS_DISABLE"] = "1"

        command = [self._bin, self._db_file]
        logger.log_verbose_process(f"Starting command `{' '.join(command)}`")
        self.process = subprocess.Popen(
            command,
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # merge so errors arrive inline, synchronously
            text=True,
            bufsize=1,
        )

        # `.timer on` => per-statement timing; `.mode trash` => discard rendered
        # result sets so parsing only sees timing lines (and any error lines).
        self._send_control(".timer on")
        self._send_control(".mode trash")

        if self._query_phase:
            # Restarted mid-benchmark (after a crash/timeout): re-arm the GPU
            # buffer manager so subsequent gpu_processing calls work again.
            self._init_gpu_buffer()

    def _stop_process(self):
        if self.process is None:
            return
        try:
            self.process.stdin.close()
        except Exception:
            pass
        try:
            self.process.wait(timeout=30)
        except Exception:
            try:
                self.process.kill()
            except Exception:
                pass
        self.process = None

    def _restart_session(self):
        logger.log_verbose_dbms("Restarting Sirius session", self)
        self._stop_process()
        self._start_session()

    # ------------------------------------------------------------------ #
    # GPU buffer manager
    # ------------------------------------------------------------------ #
    def _init_gpu_buffer(self):
        if self._use_pin_memory_for_caching:
            self._run("SET use_pin_memory_for_caching=true;")
        stmt = (f"call gpu_buffer_init('{self._gpu_cache_size}','{self._gpu_processing_size}', "
                f"pinned_memory_size='{self._pinned_memory_size}');")
        result = self._run(stmt)
        if result.state != Result.SUCCESS:
            raise Exception(f"gpu_buffer_init failed: {result.message}")

    # ------------------------------------------------------------------ #
    # Schema / loading (plain DuckDB SQL into the .duckdb file)
    # ------------------------------------------------------------------ #
    def _transform_schema(self, schema: dict) -> dict:
        return sql.transform_schema(schema, escape='"', lowercase=False)

    def _create_table_statements(self, schema: dict) -> list[str]:
        return sql.create_table_statements(schema, alter_table=False)

    def _copy_statements(self, schema: dict) -> list[str]:
        # Local files (Sirius runs natively, not in docker), so resolve against
        # the real data directory; `text` format is mapped to `csv` for DuckDB.
        return sql.copy_statements_postgres(schema, self._data_dir, supports_text=False)

    def load_database(self):
        if self._db_exists:
            logger.log_verbose_dbms("Reusing existing Sirius database " + self._db_file, self)
        else:
            logger.log_verbose_dbms("Loading tables into " + self._db_file, self)
            super().load_database()  # create + copy via _execute (plain SQL)

        # Initialize the GPU buffer manager once, before any gpu_processing, and
        # switch _execute over to wrapping queries in gpu_processing(...).
        self._init_gpu_buffer()
        self._query_phase = True

    # ------------------------------------------------------------------ #
    # Execution
    # ------------------------------------------------------------------ #
    def _execute(self, query: str, fetch_result: bool, timeout: int = 0, fetch_result_limit: int = 0) -> Result:
        if self._query_phase:
            return self._run(f"call gpu_processing('{self._escape(query)}');", timeout=timeout)
        # Loading phase: create/copy statements run as ordinary DuckDB SQL.
        return self._run(query, timeout=timeout)

    @staticmethod
    def _escape(query: str) -> str:
        q = query.strip()
        if q.endswith(";"):
            q = q[:-1]
        # Single-quote the SQL as a string argument to gpu_processing(); double
        # any embedded single quotes (e.g. date literals like '1995-01-01').
        return q.replace("'", "''")

    def _next_sentinel(self) -> str:
        self._sentinel_seq += 1
        return f"OLAPBENCH_SENTINEL_{self._sentinel_seq}"

    def _send_control(self, dot_command: str):
        """Send a CLI dot-command (no Run Time line) and drain until the sentinel."""
        sentinel = self._next_sentinel()
        self.process.stdin.write(dot_command + "\n")
        self.process.stdin.write(f".print {sentinel}\n")
        self.process.stdin.flush()
        while True:
            line = self.process.stdout.readline()
            if line == "":
                raise ChildProcessError("Sirius process closed during startup")
            if line.strip() == sentinel:
                return
            logger.log_verbose_process(line.rstrip("\n"))

    def _run(self, statement: str, timeout: int = 0) -> Result:
        result = Result()

        if self.process is None or self.process.poll() is not None:
            self._restart_session()

        sentinel = self._next_sentinel()

        self._timed_out = False
        killer = None
        if timeout and timeout > 0:
            killer = threading.Timer(timeout, self._kill_process)
            killer.start()

        begin = time.time()
        try:
            self.process.stdin.write(statement + "\n")
            self.process.stdin.write(f".print {sentinel}\n")
            self.process.stdin.flush()
        except (BrokenPipeError, ValueError, OSError):
            if killer is not None:
                killer.cancel()
            return self._crash_result(result, [])

        run_time = None
        error_lines = []
        fell_back = False
        crashed = False
        while True:
            line = self.process.stdout.readline()
            if line == "":  # EOF: process died, was killed (timeout), or crashed
                crashed = True
                break
            line = line.rstrip("\n")
            if line.strip() == sentinel:
                break
            logger.log_verbose_process(line)
            m = _RUNTIME_RE.search(line)
            if m:
                run_time = float(m.group(1))
                continue
            if _FALLBACK_RE.search(line):
                fell_back = True
                continue
            if line.strip() and _ERROR_RE.search(line):
                error_lines.append(line.strip())

        client_total = (time.time() - begin) * 1000
        if killer is not None:
            killer.cancel()
            killer.join()

        if crashed:
            if self._timed_out:
                logger.log_warn_verbose("Sirius query timed out")
                result.state = Result.TIMEOUT
                result.message = "olapbench: query timeout"
                result.client_total.append(timeout * 1000)
                self._restart_session()
                return result
            return self._crash_result(result, error_lines)

        if error_lines:
            message = "\n".join(error_lines)
            logger.log_error_verbose(message)
            result.message = message
            result.state = Result.OOM if self._is_oom(message) else Result.ERROR
            return result

        if run_time is None:
            result.state = Result.ERROR
            result.message = "olapbench: no result from Sirius"
            return result

        if fell_back and self._fail_on_cpu_fallback:
            # Treat "couldn't run on GPU" as a failure (strict GPU-only mode).
            result.state = Result.ERROR
            result.message = "Sirius: unsupported on GPU (fell back to DuckDB CPU)"
            return result

        # Record GPU-vs-CPU per execution. Sirius falls back to DuckDB CPU when a
        # query can't run on the GPU; for some queries this is deterministic, for
        # borderline ones it varies run-to-run with GPU memory pressure that
        # accumulates over the (single, long-lived) session. We emit the flag on
        # *every* execution (0.0 = GPU, 1.0 = CPU) so the recorded value reflects
        # the first measured repetition and does NOT depend on how many
        # repetitions you run (Result.merge keeps the first non-empty extra).
        result.extra["cpu_fallback"] = 1.0 if fell_back else 0.0
        if fell_back:
            logger.log_warn_verbose("Sirius fell back to DuckDB CPU for this query")
            result.message = "Sirius: ran on DuckDB CPU (GPU fallback)"

        # Prefer the DuckDB-reported wall-clock (excludes CLI rendering, which is
        # suppressed by `.mode trash`); fall back to the measured round-trip.
        t = run_time * 1000 if run_time is not None else client_total
        result.client_total.append(t)
        result.total.append(t)
        result.execution.append(t)
        result.rows = -1  # results discarded (.mode trash); row count unavailable
        return result

    def _crash_result(self, result: Result, error_lines: list[str]) -> Result:
        message = "\n".join(error_lines) if error_lines else "Sirius process crashed"
        logger.log_error_verbose(message)
        result.message = message
        result.state = Result.OOM if self._is_oom(message) else Result.ERROR
        # Bring the session back so subsequent queries can still run.
        self._restart_session()
        return result

    @staticmethod
    def _is_oom(message: str) -> bool:
        low = message.lower()
        return any(p in low for p in _OOM_PATTERNS)

    def _kill_process(self):
        self._timed_out = True
        if self.process is not None:
            try:
                self.process.kill()
            except Exception:
                pass


class SiriusDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return 'sirius'

    @staticmethod
    def get_description() -> str:
        return 'Sirius (GPU, DuckDB extension)'

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict) -> DBMS:
        return Sirius(benchmark, db_dir, data_dir, params, settings)
