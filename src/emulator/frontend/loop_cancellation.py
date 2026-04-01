import time
from typing import Any


class LoopCancellation:
    """Adapter for loop cancellation checks with optional sleep interruption."""

    def __init__(self, token: Any = None):
        self._token = token

    def is_cancelled(self) -> bool:
        if self._token is None:
            return False

        is_set = getattr(self._token, "is_set", None)
        if callable(is_set):
            return bool(is_set())

        if callable(self._token):
            return bool(self._token())

        return bool(self._token)

    def pause_or_cancel(self, pause_sec: float) -> bool:
        if pause_sec <= 0.0:
            return self.is_cancelled()

        wait = getattr(self._token, "wait", None)
        if callable(wait):
            try:
                return bool(wait(timeout=pause_sec))
            except TypeError:
                # Fallback to sleep if wait() has an unexpected signature.
                pass

        time.sleep(pause_sec)
        return self.is_cancelled()
