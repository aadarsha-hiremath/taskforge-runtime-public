"""Importable task for the spawn-based CLI demonstration."""

from os import getpid


def process_square(number: int) -> tuple[int, str]:
    return number * number, f"pid {getpid()}"
