"""
Prometheus middleware for tracking HTTP-level metrics.

This middleware automatically tracks request counts, durations, status codes,
and in-flight requests for all endpoints.
"""

import logging
import time
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response
from apis.config import get_transpiler_config
from apis.metrics import http_requests_in_progress, record_http_request

logger = logging.getLogger(__name__)


class PrometheusMiddleware(BaseHTTPMiddleware):
    """
    Middleware to track HTTP-level Prometheus metrics.

    Tracks:
    - Request counts by method, endpoint, status code
    - Request duration by method, endpoint
    - In-flight requests by method, endpoint
    """

    async def dispatch(self, request: Request, call_next):
        """Process request and record metrics."""
        config = get_transpiler_config()

        # Skip if metrics disabled
        if not config.enable_metrics:
            return await call_next(request)

        # Normalize endpoint path (remove query params)
        endpoint = request.url.path
        method = request.method

        # Track in-flight requests
        try:
            http_requests_in_progress.labels(
                method=method,
                endpoint=endpoint
            ).inc()
        except Exception as e:
            logger.warning(f"Failed to increment in-flight requests: {e}")

        # Record start time
        start_time = time.time()
        status_code = 500  # Default to error in case of exception

        try:
            # Process request
            response = await call_next(request)
            status_code = response.status_code
            return response
        except Exception as e:
            # Log error but don't suppress it
            logger.error(f"Request failed: {e}", exc_info=True)
            raise
        finally:
            # Calculate duration
            duration_seconds = time.time() - start_time

            # Decrement in-flight requests
            try:
                http_requests_in_progress.labels(
                    method=method,
                    endpoint=endpoint
                ).dec()
            except Exception as e:
                logger.warning(f"Failed to decrement in-flight requests: {e}")

            # Record request metrics
            try:
                record_http_request(
                    method=method,
                    endpoint=endpoint,
                    status_code=status_code,
                    duration_seconds=duration_seconds
                )
            except Exception as e:
                logger.warning(f"Failed to record HTTP metrics: {e}")
