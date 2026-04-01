"""Demonstration entrypoints for emulator behaviors."""

from .database_demo import (
	create_servers,
	run_database_demo,
	run_lookup_demo,
	run_update_demo,
)

from .corrupt_and_repair_demo import run_demo as run_corrupt_and_repair_demo

__all__ = [
	"create_servers",
	"run_database_demo",
	"run_lookup_demo",
	"run_update_demo",
	"run_corrupt_and_repair_demo",
]
