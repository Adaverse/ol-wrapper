"""
OpenLineage Singleton Decorator Module

This module provides a singleton decorator that injects the OpenLineage client
into Python functions.
"""

import functools
from typing import Any, Callable, Optional, TypeVar, cast

try:
    from openlineage.client import OpenLineageClient
except ImportError:
    raise ImportError(
        "OpenLineage client is not installed. Please install it with: pip install openlineage-python"
    )

F = TypeVar('F', bound=Callable[..., Any])


class OpenLineageClientSingleton:
    """
    Singleton class for OpenLineage client.
    Ensures that only one instance of the OpenLineage client is created.
    """
    _instance: Optional['OpenLineageClientSingleton'] = None
    _client: Optional[OpenLineageClient] = None

    def __new__(cls, *args, **kwargs) -> 'OpenLineageClientSingleton':
        if cls._instance is None:
            cls._instance = super(OpenLineageClientSingleton, cls).__new__(cls)
            cls._instance._client = None
        return cls._instance

    def __init__(self, url: str = "http://localhost:5000") -> None:
        if self._client is None:
            self._client = OpenLineageClient(url=url)

    @property
    def client(self) -> OpenLineageClient:
        """Returns the OpenLineage client instance."""
        if self._client is None:
            raise ValueError("OpenLineage client has not been initialized")
        return self._client


def with_openlineage_client(
    func: Optional[F] = None,
    *,
    url: str = "http://localhost:5000",
    client_param_name: str = "ol_client"
) -> Callable:
    """
    Simple decorator that injects the OpenLineage client singleton into the decorated function.
    
    Args:
        func: The function to decorate
        url: URL of the OpenLineage API server
        client_param_name: The parameter name under which to inject the client
        
    Returns:
        Decorated function with OpenLineage client injected
        
    Example:
        @with_openlineage_client
        def my_function(x, y, ol_client=None):
            # Now ol_client is the OpenLineage client instance
            # Do something with the client
            return x + y
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Get the singleton instance of the OpenLineage client
            ol_singleton = OpenLineageClientSingleton(url=url)
            
            # Inject the client into kwargs if not already present
            if client_param_name not in kwargs:
                kwargs[client_param_name] = ol_singleton.client
                
            # Call the original function with the client injected
            return func(*args, **kwargs)
                
        return cast(F, wrapper)
    
    if func is None:
        return decorator
    return decorator(func)
