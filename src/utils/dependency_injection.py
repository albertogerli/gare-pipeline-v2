#!/usr/bin/env python3
"""
Dependency Injection Container for clean architecture and testability.
Implements Inversion of Control pattern with service registration and resolution.
"""

import inspect
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, Optional, Type, TypeVar, Union

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ServiceLifetime(Enum):
    """Service lifetime management options"""

    SINGLETON = "singleton"
    TRANSIENT = "transient"  # New instance every time
    SCOPED = "scoped"  # One instance per scope


@dataclass
class ServiceDescriptor:
    """Describes how a service should be created and managed"""

    service_type: Type
    implementation_type: Optional[Type] = None
    factory: Optional[Callable] = None
    instance: Optional[Any] = None
    lifetime: ServiceLifetime = ServiceLifetime.TRANSIENT

    def __post_init__(self):
        if not self.implementation_type and not self.factory and not self.instance:
            raise ValueError("Must provide implementation_type, factory, or instance")


class ServiceContainer:
    """
    Dependency Injection Container with automatic constructor injection.
    Supports singleton, transient, and scoped lifetimes.
    """

    def __init__(self):
        self._services: Dict[Type, ServiceDescriptor] = {}
        self._singletons: Dict[Type, Any] = {}
        self._scoped_instances: Dict[Type, Any] = {}
        self._scope_active = False

    def register_singleton(
        self, service_type: Type[T], implementation_type: Type[T] = None
    ) -> "ServiceContainer":
        """Register a service as singleton (one instance for lifetime of container)"""
        impl_type = implementation_type or service_type
        self._services[service_type] = ServiceDescriptor(
            service_type=service_type,
            implementation_type=impl_type,
            lifetime=ServiceLifetime.SINGLETON,
        )
        logger.debug(
            f"Registered singleton: {service_type.__name__} -> {impl_type.__name__}"
        )
        return self

    def register_transient(
        self, service_type: Type[T], implementation_type: Type[T] = None
    ) -> "ServiceContainer":
        """Register a service as transient (new instance every time)"""
        impl_type = implementation_type or service_type
        self._services[service_type] = ServiceDescriptor(
            service_type=service_type,
            implementation_type=impl_type,
            lifetime=ServiceLifetime.TRANSIENT,
        )
        logger.debug(
            f"Registered transient: {service_type.__name__} -> {impl_type.__name__}"
        )
        return self

    def register_scoped(
        self, service_type: Type[T], implementation_type: Type[T] = None
    ) -> "ServiceContainer":
        """Register a service as scoped (one instance per scope)"""
        impl_type = implementation_type or service_type
        self._services[service_type] = ServiceDescriptor(
            service_type=service_type,
            implementation_type=impl_type,
            lifetime=ServiceLifetime.SCOPED,
        )
        logger.debug(
            f"Registered scoped: {service_type.__name__} -> {impl_type.__name__}"
        )
        return self

    def register_factory(
        self,
        service_type: Type[T],
        factory: Callable[[], T],
        lifetime: ServiceLifetime = ServiceLifetime.TRANSIENT,
    ) -> "ServiceContainer":
        """Register a service with a custom factory function"""
        self._services[service_type] = ServiceDescriptor(
            service_type=service_type, factory=factory, lifetime=lifetime
        )
        logger.debug(f"Registered factory for: {service_type.__name__}")
        return self

    def register_instance(
        self, service_type: Type[T], instance: T
    ) -> "ServiceContainer":
        """Register a specific instance (always singleton)"""
        self._services[service_type] = ServiceDescriptor(
            service_type=service_type,
            instance=instance,
            lifetime=ServiceLifetime.SINGLETON,
        )
        logger.debug(f"Registered instance: {service_type.__name__}")
        return self

    def resolve(self, service_type: Type[T]) -> T:
        """Resolve a service instance with dependency injection"""
        if service_type not in self._services:
            # Try to auto-register if it's a concrete class
            if inspect.isclass(service_type) and not inspect.isabstract(service_type):
                self.register_transient(service_type)
            else:
                raise ValueError(f"Service not registered: {service_type.__name__}")

        descriptor = self._services[service_type]

        # Handle singleton lifetime
        if descriptor.lifetime == ServiceLifetime.SINGLETON:
            if service_type in self._singletons:
                return self._singletons[service_type]

            instance = self._create_instance(descriptor)
            self._singletons[service_type] = instance
            return instance

        # Handle scoped lifetime
        elif descriptor.lifetime == ServiceLifetime.SCOPED:
            if not self._scope_active:
                logger.warning(
                    f"Resolving scoped service {service_type.__name__} outside of scope"
                )

            if service_type in self._scoped_instances:
                return self._scoped_instances[service_type]

            instance = self._create_instance(descriptor)
            self._scoped_instances[service_type] = instance
            return instance

        # Handle transient lifetime
        else:
            return self._create_instance(descriptor)

    def _create_instance(self, descriptor: ServiceDescriptor) -> Any:
        """Create an instance based on the service descriptor"""
        # Use provided instance
        if descriptor.instance is not None:
            return descriptor.instance

        # Use factory function
        if descriptor.factory is not None:
            return self._inject_factory(descriptor.factory)

        # Use implementation type with constructor injection
        if descriptor.implementation_type is not None:
            return self._inject_constructor(descriptor.implementation_type)

        raise ValueError(
            f"Cannot create instance for {descriptor.service_type.__name__}"
        )

    def _inject_constructor(self, implementation_type: Type) -> Any:
        """Create instance with automatic constructor dependency injection"""
        # Get constructor signature
        init_signature = inspect.signature(implementation_type.__init__)

        # Build arguments dictionary (skip 'self')
        kwargs = {}
        for param_name, param in init_signature.parameters.items():
            if param_name == "self":
                continue

            # Try to resolve parameter type
            if param.annotation != inspect.Parameter.empty:
                try:
                    kwargs[param_name] = self.resolve(param.annotation)
                except ValueError:
                    # If we can't resolve, use default value if available
                    if param.default != inspect.Parameter.empty:
                        kwargs[param_name] = param.default
                    else:
                        raise ValueError(
                            f"Cannot resolve dependency: {param.annotation} for {implementation_type.__name__}"
                        )
            elif param.default != inspect.Parameter.empty:
                kwargs[param_name] = param.default

        logger.debug(
            f"Creating {implementation_type.__name__} with dependencies: {list(kwargs.keys())}"
        )
        return implementation_type(**kwargs)

    def _inject_factory(self, factory: Callable) -> Any:
        """Call factory function with dependency injection"""
        factory_signature = inspect.signature(factory)

        # Build arguments dictionary
        kwargs = {}
        for param_name, param in factory_signature.parameters.items():
            if param.annotation != inspect.Parameter.empty:
                try:
                    kwargs[param_name] = self.resolve(param.annotation)
                except ValueError:
                    if param.default != inspect.Parameter.empty:
                        kwargs[param_name] = param.default
                    else:
                        raise ValueError(
                            f"Cannot resolve dependency: {param.annotation} for factory"
                        )
            elif param.default != inspect.Parameter.empty:
                kwargs[param_name] = param.default

        return factory(**kwargs)

    def create_scope(self) -> "ServiceScope":
        """Create a new scope for scoped services"""
        return ServiceScope(self)

    def _clear_scoped(self):
        """Clear scoped instances (called by ServiceScope)"""
        self._scoped_instances.clear()
        self._scope_active = False

    def _activate_scope(self):
        """Activate scoped lifetime (called by ServiceScope)"""
        self._scope_active = True


class ServiceScope:
    """
    Context manager for scoped service lifetimes.
    All scoped services created within this scope share the same instances.
    """

    def __init__(self, container: ServiceContainer):
        self.container = container

    def __enter__(self):
        self.container._activate_scope()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.container._clear_scoped()


# Global default container instance
_default_container = ServiceContainer()


def get_container() -> ServiceContainer:
    """Get the default service container"""
    return _default_container


def resolve(service_type: Type[T]) -> T:
    """Resolve a service from the default container"""
    return _default_container.resolve(service_type)


# Service registration decorators for convenience
def singleton(service_type: Type = None):
    """Decorator to register a class as singleton in the default container"""

    def decorator(cls):
        _default_container.register_singleton(service_type or cls, cls)
        return cls

    return decorator


def transient(service_type: Type = None):
    """Decorator to register a class as transient in the default container"""

    def decorator(cls):
        _default_container.register_transient(service_type or cls, cls)
        return cls

    return decorator


def scoped(service_type: Type = None):
    """Decorator to register a class as scoped in the default container"""

    def decorator(cls):
        _default_container.register_scoped(service_type or cls, cls)
        return cls

    return decorator


# Example interfaces and implementations
class ILogger(ABC):
    """Abstract logger interface"""

    @abstractmethod
    def info(self, message: str) -> None:
        pass

    @abstractmethod
    def error(self, message: str) -> None:
        pass


@singleton(ILogger)
class ConsoleLogger(ILogger):
    """Console logger implementation"""

    def info(self, message: str) -> None:
        print(f"INFO: {message}")

    def error(self, message: str) -> None:
        print(f"ERROR: {message}")


class IRepository(ABC):
    """Abstract repository interface"""

    @abstractmethod
    async def save(self, data: Any) -> None:
        pass

    @abstractmethod
    async def load(self, key: str) -> Optional[Any]:
        pass


@scoped(IRepository)
class FileRepository(IRepository):
    """File-based repository implementation"""

    def __init__(self, logger: ILogger):
        self.logger = logger
        self.logger.info("FileRepository initialized")

    async def save(self, data: Any) -> None:
        self.logger.info(f"Saving data: {type(data).__name__}")

    async def load(self, key: str) -> Optional[Any]:
        self.logger.info(f"Loading data for key: {key}")
        return None


# Example service that depends on other services
@transient()
class DataService:
    """Example service with injected dependencies"""

    def __init__(self, repository: IRepository, logger: ILogger):
        self.repository = repository
        self.logger = logger
        self.logger.info("DataService initialized with dependencies")

    async def process_data(self, data: Any) -> None:
        self.logger.info("Processing data...")
        await self.repository.save(data)


def configure_services(container: ServiceContainer = None) -> ServiceContainer:
    """Configure all application services"""
    if container is None:
        container = get_container()

    # Services are already registered via decorators
    # Additional manual registrations can be done here

    logger.info("Service container configured")
    return container


async def example_usage():
    """Example of how to use the dependency injection container"""

    # Services are automatically registered via decorators
    configure_services()

    # Resolve services - dependencies are automatically injected
    data_service = resolve(DataService)

    # Use the service
    await data_service.process_data("example data")

    # Example of using scoped services
    with get_container().create_scope():
        repo1 = resolve(IRepository)
        repo2 = resolve(IRepository)
        # repo1 and repo2 are the same instance within this scope
        assert repo1 is repo2

    # Outside scope, new scoped instances are created
    repo3 = resolve(IRepository)
    # repo3 is a different instance


if __name__ == "__main__":
    import asyncio

    asyncio.run(example_usage())
