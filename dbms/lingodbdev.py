import os
import re
from ast import Index

from dbms.dbms import DBMS, Result, DBMSDescription
from util import logger, sql
from benchmarks.benchmark import Benchmark
from util.process import Process
import time
import math



class LingoDBDev(DBMS):
    def __init__(self, benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict):
        super().__init__(benchmark, db_dir, data_dir, params, settings)
        self._lingodb_db = os.path.join(self._db_dir, params["dbdir"] if "dbdir" in params else "lingodb")
        self._cwd = os.getcwd()
        self._bin_dir = os.path.join(self._cwd, params["bin"] if "bin" in params else "bin")
        self._database_name=benchmark.unique_name
        # DISABLED: previously, when set (e.g. 100), a `limit <n>` was appended to queries
        # whose last clause was an ORDER BY without a LIMIT (GPU mode once supported only
        # `order by ... limit`, not a standalone sort). GPU mode now runs standalone sorts
        # (full sorted-view materialize), so we never append a limit anymore — benchmark the
        # real (unbounded) ORDER BY. The `order_by_limit` config param is ignored on purpose.
        self._order_by_limit = None
        # Run each benchmark query in a fresh `sql` process instead of one
        # long-lived REPL. LingoDB's GPU backend leaks device state across
        # queries in a single process (some query pairs SIGSEGV), so isolating
        # every query keeps a crash from aborting the whole run. Data loading
        # still uses the persistent process (it needs one session for `SET
        # persist=1` + the COPY statements).
        self._process_per_query = params.get("process_per_query", False)
        # Comma-separated identifiers that collide with LingoDB reserved words
        # and must be quoted when used as table/column names (e.g. SSB's `date`
        # table -> "date"). Date literals like `date '1995-01-01'` are left
        # untouched. A plain string (not a list) avoids the config `unfold`
        # treating each entry as a separate benchmark run.
        self._quote_keywords = [k.strip() for k in params.get("quote_keywords", "").split(",") if k.strip()]
        self._query_phase = False
        self._env = None
        self.process = None

    def _rewrite_query(self, query: str) -> str:
        # Quote reserved-word identifiers, but never a date literal (`date '...'`).
        for kw in self._quote_keywords:
            query = re.sub(rf"\b{re.escape(kw)}\b(?!\s*')", f'"{kw}"', query, flags=re.IGNORECASE)
        return self._append_order_by_limit(query)

    def _append_order_by_limit(self, query: str) -> str:
        # No-op: GPU mode runs standalone sorts, so we never append a LIMIT to ORDER BY
        # queries — they are benchmarked unbounded. (Kept as a stub so callers/tests still
        # resolve; `self._order_by_limit` is forced to None in __init__.)
        return query
    def __enter__(self):
        self.sql = os.path.join(self._bin_dir,"sql")
        os.makedirs(self._db_dir, exist_ok=True)
        self.db = os.path.join(self._lingodb_db, self._database_name)
        self.db_exists=os.path.exists(self.db)
        logger.log_verbose_dbms("Using lingodb sql binary " + self.sql, self)
        logger.log_verbose_dbms("Starting lingodb with a new database " + self.db, self)
        os.makedirs(self.db, exist_ok=True)
        self._command = f'{self.sql} {self.db}'
        self._env = {"LINGODB_EXECUTION_MODE": "SPEED", "LINGODB_SQL_PROMPT": "0",
                     "LINGODB_SQL_REPORT_TIMES": "1", **self._settings}
        self.process = Process(self._command, self._env)
        self.process.start()
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.process is not None:
            self.process.stop()
    @property
    def name(self) -> str:
        return "lingodbdev"
    @property
    def version(self) -> str:
        return "dev"
    def _transform_schema(self, schema: dict) -> dict:
        schema = sql.transform_schema(schema, escape='"', lowercase=self._umbra_planner)
        for table in schema['tables']:
            for column in table['columns']:
                # text is limited to 65KB replace with longtext
                column['type'] = column['type'].replace('smallint', 'int')
        return schema

    def _create_table_statements(self, schema: dict) -> [str]:
        return sql.create_table_statements(schema)

    def copy_statements_postgres_adapted(self, schema: dict, data_dir: str, supports_text: bool = True) -> [str]:
        delimiter = schema["delimiter"]
        format = schema["format"] if supports_text or schema["format"] != "text" else "csv"

        null = f" null {sql.escape(schema['null'])}" if "null" in schema else ""
        quote = f" quote {sql.escape(schema['quote'])}" if "quote" in schema else ""
        csv_escape = f" escape '{schema['csv_escape']}'" if format == "csv" and "csv_escape" in schema else ""
        header = " header" if "header" in schema and schema["header"] else ""

        statements = []
        for table in schema["tables"]:
            if table.get("initially empty", False):
                continue
            statements.append(
                f'copy {table["name"]} from \'{os.path.join(data_dir, table["file"])}\' delimiter \'{delimiter}\' {null}{quote}{csv_escape}{header};')

        return statements
    def _copy_statements(self, schema: dict) -> [str]:
        return self.copy_statements_postgres_adapted(schema, self._data_dir, supports_text=False)

    def _execute(self, query: str, fetch_result: bool, timeout: int = 0, fetch_result_limit: int = 0) -> Result:
        # Rewrite benchmark queries only; load statements (create/copy) already
        # quote identifiers via the schema and must not be touched.
        if self._query_phase:
            query = self._rewrite_query(query)

        # Isolate benchmark queries in a fresh process (see __init__). Loading
        # statements (self._query_phase is False) keep using the persistent one.
        if self._process_per_query and self._query_phase:
            process = Process(self._command, self._env)
            process.start()
            try:
                return self._run_on(process, query)
            finally:
                self._shutdown(process)

        return self._run_on(self.process, query)

    def _shutdown(self, process):
        # Close stdin and reap the process without raising, even if it crashed.
        try:
            process.process.stdin.close()
        except Exception:
            pass
        try:
            process.process.wait(timeout=30)
        except Exception:
            try:
                process.process.kill()
            except Exception:
                pass

    def _run_on(self, process, query: str) -> Result:
        result = Result()

        begin = time.time()
        try:
            process.write(str(query))
            output: str = None
            client_total = math.nan
            while output is None:
                output = process.readline_stderr()
                client_total = (time.time() - begin) * 1000

                if "execution:" in output and "compilation:" in output:
                    break
                elif output.startswith("ERROR:"):
                    logger.log_error(output)
                    result.state = Result.ERROR
                    result.message = output
                    return result
                else:
                    logger.log_warn(output)

                output = None
        except ChildProcessError:
            # The process died (e.g. GPU SIGSEGV) before reporting timings.
            logger.log_error(f"lingodb process crashed while executing query")
            result.state = Result.ERROR
            result.message = "lingodb process crashed"
            return result

        try:
            [compilation, execution] = re.findall(r'([0-9.]*) \[ms]', output)
            execution = float(execution)
            compilation = float(compilation)
        except ValueError as e:
            logger.log_error(f"Could not extract execution and compilation time from '{output}'")
            logger.log_error(str(e))
            execution = math.nan
            compilation = math.nan



        result.execution.append(execution)
        result.compilation.append(compilation)
        result.total.append(execution + compilation)
        result.client_total.append(client_total)
        result.extra = {}
        result.rows = -1

        return result
    def load_database(self):
        if self.db_exists:
            logger.log_verbose_dbms("Starting lingodb with existing database " + self.db, self)
        else:
            logger.log_verbose_dbms("Loading tables " + self.db, self)
            self._execute("SET persist=1;\n", False)
            super().load_database()
        # From here on, _execute serves benchmark queries (not load statements).
        self._query_phase = True

class LingoDBDevDescription(DBMSDescription):
    @staticmethod
    def get_name() -> str:
        return 'lingodbdev'

    @staticmethod
    def get_description() -> str:
        return 'LingoDB Dev'

    @staticmethod
    def instantiate(benchmark: Benchmark, db_dir: str, data_dir: str, params: dict, settings: dict) -> DBMS:
        return LingoDBDev(benchmark, db_dir, data_dir, params, settings)

