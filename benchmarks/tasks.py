"""Importable workloads for Windows spawn."""

import os
from threading import get_ident
from time import perf_counter, sleep


def measure(number: int, rounds: int, delay: float) -> tuple[int, int, int, float]:
    start = perf_counter()
    if delay:
        sleep(delay)
    value = number
    for i in range(rounds):
        value = (value * 33 + i) % 65521
    return value, os.getpid(), get_ident(), perf_counter() - start


def fail(number: int) -> None:
    raise ValueError(f"expected failure {number}")
