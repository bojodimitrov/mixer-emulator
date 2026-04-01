"""Storage-oriented package for database and index implementations.

Keep this module import-light.

When running submodules as scripts, e.g. `python -m emulator.storage.database`,
`runpy` first imports the package (`emulator.storage`). If we eagerly import
`emulator.storage.server` here, that in turn imports `emulator.storage.database`
and triggers a `runpy` warning about the module being present in `sys.modules`.

To avoid that, we provide lazy attribute access for the public symbols.
"""

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:  # pragma: no cover
	from .server import DatabaseRequest, DatabaseServer, LookupStrategy

__all__ = ["DatabaseRequest", "DatabaseServer", "LookupStrategy"]


def __getattr__(name: str) -> Any:  # pragma: no cover
	if name in __all__:
		from .server import DatabaseRequest, DatabaseServer, LookupStrategy

		return {
			"DatabaseRequest": DatabaseRequest,
			"DatabaseServer": DatabaseServer,
			"LookupStrategy": LookupStrategy,
		}[name]
	raise AttributeError(name)
