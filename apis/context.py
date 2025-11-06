"""
Per-request configuration for the SQLGlot transpiler.

This module uses Python's contextvars to provide thread-safe, request-isolated
configuration. Each request can have its own settings without interfering with
concurrent requests.

Per-request configs override system-level defaults from apis/config.py.
"""

from contextvars import ContextVar
from dataclasses import dataclass
from typing import Optional, Literal


@dataclass
class PerRequestConfig:
    """
    Per-request configuration for transpilation.

    This configuration is set at the beginning of each request and automatically
    isolated from other concurrent requests via context variables.

    All fields have defaults that match system configuration, but can be overridden
    on a per-request basis via API options.
    """

    enable_table_alias_qualification: bool = False
    use_two_phase_qualification_scheme: bool = False
    skip_e6_transpilation: bool = False
    pretty_print: bool = True
    error_level: Optional[Literal["IGNORE", "WARN", "RAISE"]] = None

    @classmethod
    def from_system_config(cls):
        """
        Create a PerRequestConfig with defaults from system configuration.

        Returns:
            PerRequestConfig: Instance with system-level defaults
        """
        try:
            from apis.config import get_transpiler_config
            config = get_transpiler_config()

            return cls(
                enable_table_alias_qualification=config.default_enable_table_alias_qualification,
                use_two_phase_qualification_scheme=config.default_use_two_phase_qualification,
                skip_e6_transpilation=config.default_skip_e6_transpilation,
                pretty_print=config.default_pretty_print,
                error_level=config.default_error_level,
            )
        except ImportError:
            # Fallback to hard-coded defaults if config unavailable
            return cls()


# Context variable for per-request config
# Default is None to catch cases where config is not set
_per_request_config: ContextVar[Optional[PerRequestConfig]] = ContextVar(
    'per_request_config',
    default=None
)


def set_per_request_config(config: PerRequestConfig) -> None:
    """
    Set the per-request configuration for the current request context.

    This should be called at the beginning of each request handler.

    Args:
        config: The PerRequestConfig instance to set for this request
    """
    _per_request_config.set(config)


def get_per_request_config() -> PerRequestConfig:
    """
    Get the per-request configuration for the current request context.

    This will raise an error if called outside of a request context where
    the config has not been set.

    Returns:
        PerRequestConfig: The configuration for the current request

    Raises:
        RuntimeError: If per-request config has not been set
    """
    config = _per_request_config.get()
    if config is None:
        raise RuntimeError(
            "Per-request config not set. Ensure set_per_request_config() "
            "is called at the beginning of the request handler."
        )
    return config


def get_per_request_config_safe() -> Optional[PerRequestConfig]:
    """
    Safely get the per-request configuration, returning None if not set.

    Use this when you want to handle the case where config might not be set
    without raising an error.

    Returns:
        Optional[PerRequestConfig]: The configuration for the current request,
            or None if not set
    """
    return _per_request_config.get()
