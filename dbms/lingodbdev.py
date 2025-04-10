import os
import re

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
        self.process = None
    def __enter__(self):
        self.sql = os.path.join(self._bin_dir,"sql")
        os.makedirs(self._db_dir, exist_ok=True)
        self.db = os.path.join(self._lingodb_db, self._database_name)
        logger.log_verbose_dbms("Using lingodb sql binary " + self.sql, self)
        logger.log_verbose_dbms("Starting lingodb with a new database " + self.db, self)
        os.makedirs(self.db, exist_ok=True)
        command = f'{self.sql} {self.db}'
        self.db_exists = False
        self.process = Process(command, {"LINGODB_EXECUTION_MODE": "SPEED", "LINGODB_SQL_PROMPT": "0",
                                         "LINGODB_SQL_REPORT_TIMES": "1"})
        self.process.start()
        self._execute("SET persist=1;\n", False)
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

    def _create_table_statements(self, schema: dict) -> [str]:
        return sql.create_table_statements(schema)

    def _copy_statements(self, schema: dict) -> [str]:
        return sql.copy_statements_postgres(schema, self._data_dir, supports_text=False)

    def _execute(self, query: str, fetch_result: bool, timeout: int = 0, fetch_result_limit: int = 0) -> Result:
        result = Result()

        begin = time.time()
        self.process.write(str(query))
        output: str = None
        client_total = math.nan
        while output is None:
            output = self.process.readline_stderr()
            client_total = (time.time() - begin) * 1000

            if "execution:" in output and "compilation:" in output:
                break
            elif output.startswith("ERROR:"):
                logger.log_error(output)
                result.error = output
                return result
            else:
                logger.log_warn(output)

            output = None

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

