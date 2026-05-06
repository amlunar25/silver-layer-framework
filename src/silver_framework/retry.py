import functools
import time
from typing import Callable, Tuple, Type, TypeVar

from silver_framework.logger import get_logger

_log = get_logger("retry")

F = TypeVar("F", bound=Callable)


def with_retry(
    max_retries: int = 3,
    delay_seconds: float = 5.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (Exception,),
) -> Callable[[F], F]:
    """Decorator that retries a function up to max_retries times on failure."""
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc: Exception = RuntimeError("unreachable")
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except retryable_exceptions as exc:
                    last_exc = exc
                    if attempt < max_retries:
                        _log.warning(
                            "Attempt %d/%d for '%s' failed: %s — retrying in %.1fs",
                            attempt, max_retries, func.__name__, exc, delay_seconds,
                        )
                        time.sleep(delay_seconds)
                    else:
                        _log.error(
                            "All %d attempts for '%s' exhausted: %s",
                            max_retries, func.__name__, exc,
                        )
            raise last_exc
        return wrapper  # type: ignore[return-value]
    return decorator
