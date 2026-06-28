import os
import re
import subprocess
import time

from benchmarks.benchmark import Benchmark
from dbms.dbms import DBMS, Result, DBMSDescription
from util import logger, sql

# HeavyDB is a client/server database. We start one long-running `heavydb`
# server, drive it with one `heavysql` (Thrift) client invocation per query, and
# stop the server on exit. The binaries link shared libraries (LLVM, Arrow,
# Boost, CUDA, ...) that live inside a conda (micromamba) env, so every binary is
# launched through `micromamba run -n <env> ...` (see heavydb-harness-integration.md).

# micromamba defaults for the build on this machine.
DEFAULT_MAMBA = "/home/jungmair/heavydb/bin/micromamba"
DEFAULT_MAMBA_ROOT = "/home/jungmair/heavydb/mamba"
DEFAULT_ENV = "hd-master"
# CUDA build binaries (CPU build is .../build/bin). The same harness works for
# both; the CPU build simply ignores `\gpu`.
DEFAULT_BIN = "/home/jungmair/heavydb/heavydb/build-cuda/bin"

# heavysql with `-t` prints one line per successful statement:
#   `Execution time: 22 ms, Total time: 23 ms`
_TIMING_RE = re.compile(r"Execution time:\s*(\d+)\s*ms,\s*Total time:\s*(\d+)\s*ms")
# A failed statement prints `SQL Error: <message>` to stderr (and the process
# exits non-zero). Other stderr lines (CUDA_HOME notice, JVM WARNING, ...) are noise.
_SQL_ERROR_RE = re.compile(r"^\s*(?:SQL\s+)?Error:\s*(.*)", re.IGNORECASE)
# Subset of error messages that mean we ran out of (GPU/host) memory.
_OOM_PATTERNS = ["out of memory", "bad_alloc", "cannot allocate", "outofmemory", "out_of_memory"]
# Stderr noise to drop when surfacing an error message.
_STDERR_NOISE_RE = re.compile(r"CUDA_HOME|WARNING|Unsafe|deprecated|native-access", re.IGNORECASE)


class HeavyDB(DBMS):
    """HeavyDB (formerly OmniSci/MapD) — a GPU-accelerated analytic database.

    A single `heavydb` server process is started against a per-benchmark storage
    directory and kept alive for the whole run; each benchmark query is executed
    by piping it to a fresh `heavysql` client. The server holds the GPU buffer
    pool and code cache, so the column data warmed up by the harness' warmup
    repetitions stays GPU-resident across client invocations.

    Execution is forced onto the GPU (or CPU) with a leading `\\gpu` / `\\cpu`
    client command. Timing is taken from the server-reported `Execution time`
    line; there is no separate compilation time in the protocol (a cold run folds
    codegen + JIT into `Execution time`, which the harness' warmup pass absorbs).
    """

    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)

        self._mamba = params.get("mamba", DEFAULT_MAMBA)
        self._mamba_root = params.get("mamba_root", DEFAULT_MAMBA_ROOT)
        self._env = params.get("env", DEFAULT_ENV)
        bin_dir = params.get("bin", DEFAULT_BIN)
        self._heavydb_bin = os.path.join(bin_dir, "heavydb")
        self._heavysql_bin = os.path.join(bin_dir, "heavysql")
        self._initheavy_bin = os.path.join(bin_dir, "initheavy")

        self._port = int(params.get("port", 6274))
        self._http_port = int(params.get("http_port", 6278))
        self._calcite_port = int(params.get("calcite_port", 6279))
        self._db = params.get("db", "heavyai")
        self._user = params.get("user", "admin")
        self._password = params.get("password", "HyperInteractive")

        # Force GPU execution by default; set false for a CPU baseline on the
        # same binary.
        self._gpu = bool(params.get("gpu", True))
        # Seconds to wait for the server to become query-ready (GPU JIT warmup on
        # first boot can take ~20 s).
        self._startup_timeout = int(params.get("startup_timeout", 180))
        # Allow loop (non-equi) joins. HeavyDB rejects queries that need them by
        # default (e.g. TPC-H Q21), so such queries are recorded as errors. Off by
        # default: enabling lets them run but a loop join can be catastrophically
        # slow (Q21 effectively never finishes), so opt in deliberately.
        self._allow_loop_joins = bool(params.get("allow_loop_joins", False))
        # Extra `heavydb` server flags (e.g. ["--gpu-buffer-mem-bytes", "..."]).
        self._server_args = params.get("server_args", [])

        # Per-benchmark storage directory so different benchmarks/scales (which
        # share table names like `lineitem`) never collide in one catalog.
        self._storage_dir = os.path.join(self._db_dir, "heavydb_" + benchmark.unique_name)
        # Written after a successful load so we can reuse the storage across runs.
        self._loaded_marker = os.path.join(self._storage_dir, ".olapbench_loaded")

        self._query_phase = False
        self._loaded = False
        self._server = None
        self._server_log = None

    @property
    def name(self) -> str:
        return "heavydb"

    @property
    def version(self) -> str:
        if self._version != "latest":
            return self._version
        return "gpu" if self._gpu else "cpu"

    # ------------------------------------------------------------------ #
    # Server lifecycle
    # ------------------------------------------------------------------ #
    def __enter__(self):
        os.makedirs(self._storage_dir, exist_ok=True)
        self._loaded = os.path.exists(self._loaded_marker)
        logger.log_verbose_dbms(f"Using HeavyDB binaries in {os.path.dirname(self._heavydb_bin)}", self)
        logger.log_verbose_dbms(f"{'Reusing' if self._loaded else 'Creating'} HeavyDB storage {self._storage_dir}", self)
        self._init_storage()
        self._start_server()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._stop_server()

    def _mamba_run(self, *args: str) -> list[str]:
        return [self._mamba, "run", "-n", self._env, *args]

    def _mamba_env(self) -> dict:
        env = dict(os.environ)
        env["MAMBA_ROOT_PREFIX"] = self._mamba_root
        return env

    def _init_storage(self):
        # `initheavy` populates catalogs/, data/, ... once per storage dir. It may
        # keep running (it spawns a Calcite helper); once `catalogs/` exists the
        # initialization is done and we can stop it.
        catalogs = os.path.join(self._storage_dir, "catalogs")
        if os.path.isdir(catalogs) and os.listdir(catalogs):
            return
        logger.log_verbose_dbms("Initializing HeavyDB storage directory", self)
        command = self._mamba_run(self._initheavy_bin, self._storage_dir)
        logger.log_verbose_process(f"Starting command `{' '.join(command)}`")
        proc = subprocess.Popen(command, env=self._mamba_env(),
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        try:
            for _ in range(60):
                if os.path.isdir(catalogs) and os.listdir(catalogs):
                    break
                if proc.poll() is not None:
                    break
                time.sleep(1)
        finally:
            self._terminate(proc)
        if not (os.path.isdir(catalogs) and os.listdir(catalogs)):
            raise Exception("HeavyDB storage initialization failed")

    def _start_server(self):
        # HeavyDB refuses `COPY FROM` for paths outside a whitelist; allow the
        # data directory (JSON array, passed as a single argv element so there is
        # no shell quoting to worry about).
        import json
        allowed_paths = json.dumps([os.path.abspath(self._data_dir)])
        command = self._mamba_run(
            self._heavydb_bin, self._storage_dir,
            "--port", str(self._port),
            "--http-port", str(self._http_port),
            "--calcite-port", str(self._calcite_port),
            "--allowed-import-paths", allowed_paths,
            *(["--allow-loop-joins"] if self._allow_loop_joins else []),
            *self._server_args,
        )
        logger.log_verbose_process(f"Starting command `{' '.join(command)}`")
        self._server_log = open(os.path.join(self._storage_dir, "server.log"), "w")
        self._server = subprocess.Popen(command, env=self._mamba_env(),
                                        stdout=self._server_log, stderr=subprocess.STDOUT)
        self._wait_until_ready()

    def _wait_until_ready(self):
        # The server opens the port early but is not query-ready until Calcite has
        # started and (on a GPU build) the one-time JIT warmup finished. Poll with
        # a trivial query rather than just probing the port.
        deadline = time.time() + self._startup_timeout
        while time.time() < deadline:
            if self._server.poll() is not None:
                raise Exception("HeavyDB server exited during startup (see server.log)")
            rc, _, _ = self._heavysql("SELECT 1;")
            if rc == 0:
                logger.log_verbose_dbms("HeavyDB server is ready", self)
                return
            time.sleep(2)
        raise Exception("HeavyDB server did not become ready in time")

    def _stop_server(self):
        if self._server is not None:
            self._terminate(self._server)
            self._server = None
        if self._server_log is not None:
            try:
                self._server_log.close()
            except Exception:
                pass
            self._server_log = None

    @staticmethod
    def _terminate(proc):
        try:
            proc.terminate()
        except Exception:
            pass
        try:
            proc.wait(timeout=30)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    # Schema / loading
    # ------------------------------------------------------------------ #
    def _transform_schema(self, schema: dict) -> dict:
        schema = sql.transform_schema(schema, escape='"', lowercase=False)
        for table in schema["tables"]:
            # HeavyDB does not support PRIMARY KEY / FOREIGN KEY constraints in
            # CREATE TABLE; drop them so the table definitions are accepted.
            table.pop("primary key", None)
            table.pop("foreign keys", None)
            for column in table["columns"]:
                # HeavyDB stores strings as TEXT and dictionary-encodes them by
                # default (its native, GPU-friendly representation). Mapping to a
                # bare TEXT also keeps `not null` in the position HeavyDB expects
                # (`TEXT not null`), since an explicit `ENCODING` clause must come
                # *after* `not null`.
                column["type"] = re.sub(
                    r"\b(?:var)?char\s*\(\s*\d+\s*\)", "TEXT",
                    column["type"], flags=re.IGNORECASE)
        return schema

    def _create_table_statements(self, schema: dict) -> list[str]:
        # Keys are stripped in _transform_schema, so this emits plain
        # `create table <t> (<cols>);` with no constraints and no ALTERs.
        return sql.create_table_statements(schema)

    def _copy_statements(self, schema: dict) -> list[str]:
        # HeavyDB runs natively (not in docker), so resolve files against the real
        # data directory. TPC-H/SSB .tbl files are pipe-delimited, unquoted, with
        # empty fields meaning NULL.
        delimiter = schema["delimiter"]
        header = "true" if schema.get("header") else "false"
        nulls = f", nulls = '{schema['null']}'" if "null" in schema else ""

        statements = []
        for table in schema["tables"]:
            if table.get("initially empty", False):
                continue
            path = os.path.join(self._data_dir, table["file"])
            statements.append(
                f"COPY {table['name']} FROM '{path}' "
                f"WITH (delimiter = '{delimiter}', header = '{header}', quoted = 'false'{nulls});")
        return statements

    def load_database(self):
        if self._loaded:
            logger.log_verbose_dbms("Reusing existing HeavyDB tables in " + self._storage_dir, self)
        else:
            logger.log_verbose_dbms("Loading tables into " + self._storage_dir, self)
            super().load_database()  # create + copy via _execute (plain SQL)
            with open(self._loaded_marker, "w") as f:
                f.write("loaded\n")
            self._loaded = True
        # From here on, _execute serves benchmark queries (forced onto GPU/CPU).
        self._query_phase = True

    # ------------------------------------------------------------------ #
    # Execution
    # ------------------------------------------------------------------ #
    def _execute(self, query: str, fetch_result: bool, timeout: int = 0, fetch_result_limit: int = 0) -> Result:
        statement = self._normalize(query)
        if self._query_phase:
            # A leading client command forces the executor onto GPU (or CPU).
            mode = "\\gpu" if self._gpu else "\\cpu"
            script = f"{mode}\n{statement}\n"
        else:
            script = f"{statement}\n"
        return self._run(script, timeout=timeout)

    @staticmethod
    def _normalize(query: str) -> str:
        # HeavyDB rejects statements that *start* with a comment, so strip any
        # leading comment lines. Inline/trailing comments are fine.
        lines = query.strip().splitlines()
        while lines and (lines[0].lstrip().startswith("--") or lines[0].strip() == ""):
            lines.pop(0)
        statement = "\n".join(lines).strip()
        # Every statement must be terminated by exactly one semicolon.
        if not statement.endswith(";"):
            statement += ";"
        return statement

    def _heavysql(self, script: str, timeout: int = 0):
        """Run a SQL script through a fresh heavysql client. Returns (rc, stdout, stderr)."""
        command = self._mamba_run(
            self._heavysql_bin, "-q", "-t",
            "-p", self._password, "-u", self._user,
            "--db", self._db, "--port", str(self._port),
        )
        logger.log_verbose_process(f"Running command `{' '.join(command)}`")
        try:
            proc = subprocess.run(command, input=script, env=self._mamba_env(),
                                  capture_output=True, text=True,
                                  timeout=timeout if timeout and timeout > 0 else None)
        except subprocess.TimeoutExpired:
            return None, "", ""
        return proc.returncode, proc.stdout, proc.stderr

    def _run(self, script: str, timeout: int = 0) -> Result:
        result = Result()

        begin = time.time()
        rc, stdout, stderr = self._heavysql(script, timeout=timeout)
        client_total = (time.time() - begin) * 1000

        if rc is None:  # client killed by timeout
            logger.log_warn_verbose("HeavyDB query timed out")
            result.state = Result.TIMEOUT
            result.message = "olapbench: query timeout"
            result.client_total.append(timeout * 1000)
            return result

        for line in stdout.splitlines():
            logger.log_verbose_process(line)

        explicit, other = self._stderr_lines(stderr)
        # A query failed if heavysql exited non-zero or printed an `Error:` line.
        if rc != 0 or explicit:
            message = "\n".join(explicit) or "\n".join(other) or "HeavyDB query failed"
            logger.log_error_verbose(message)
            result.message = message
            result.state = Result.OOM if self._is_oom(message) else Result.ERROR
            return result

        match = _TIMING_RE.search(stdout)
        if match:
            exec_ms = float(match.group(1))
            total_ms = float(match.group(2))
        else:
            # No timing line (e.g. CREATE/COPY during loading): fall back to the
            # measured wall-clock so loading still reports a time.
            exec_ms = total_ms = client_total

        result.client_total.append(client_total)
        result.execution.append(exec_ms)
        result.total.append(total_ms)
        result.rows = -1  # heavysql `-q` suppresses the row count
        return result

    @staticmethod
    def _stderr_lines(stderr: str):
        """Split stderr into explicit `Error:` messages and other non-noise lines."""
        explicit, other = [], []
        for line in stderr.splitlines():
            if not line.strip() or _STDERR_NOISE_RE.search(line):
                continue
            m = _SQL_ERROR_RE.match(line)
            if m:
                explicit.append(m.group(1).strip() or line.strip())
            else:
                other.append(line.strip())
        return explicit, other

    @staticmethod
    def _is_oom(message: str) -> bool:
        low = message.lower()
        return any(p in low for p in _OOM_PATTERNS)


class HeavyDBDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return 'heavydb'

    @staticmethod
    def get_description() -> str:
        return 'HeavyDB (GPU)'

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict) -> DBMS:
        return HeavyDB(benchmark, db_dir, data_dir, params, settings)
