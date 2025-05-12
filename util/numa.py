import os
import sys

import psutil

from util import logger


def set_node(numa_node: int | None):
    pass

def get_cpus(numa_node: int | None) -> str:
    return ""


def get_mems(numa_node: int | None) -> str:
    return ""


def get_thread_count(numa_node: int | None) -> int:
    return psutil.cpu_count(logical=True)


def get_memory_size(numa_node: int | None) -> int:
    return psutil.virtual_memory().total
