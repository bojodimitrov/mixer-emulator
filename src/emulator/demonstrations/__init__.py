"""Demonstration entrypoints for emulator behaviors."""

from .database_demo import (
	create_servers,
	run_database_demo,
	run_lookup_demo,
	run_update_demo,
)

__all__ = [
	"create_servers",
	"run_database_demo",
	"run_lookup_demo",
	"run_update_demo",
]
