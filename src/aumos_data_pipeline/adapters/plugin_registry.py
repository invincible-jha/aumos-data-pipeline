"""Pipeline plugin registry for aumos-data-pipeline (GAP-119).

Provides a runtime plugin architecture for extending the pipeline with
custom connectors, transformers, and quality checks. Plugins are registered
by name and loaded on demand via importlib.
"""

from __future__ import annotations

import importlib
import uuid
from typing import Any, Protocol

from aumos_common.observability import get_logger

logger = get_logger(__name__)


class PipelinePluginProtocol(Protocol):
    """Protocol that all pipeline plugins must implement."""

    plugin_name: str
    plugin_version: str
    plugin_type: str  # 'connector' | 'transformer' | 'quality_check' | 'exporter'

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Execute the plugin with the given context.

        Args:
            context: Plugin execution context (input_uri, tenant_id, config, etc.)

        Returns:
            Plugin result dict with at minimum 'status' and 'output'.
        """
        ...


class PluginRegistration:
    """Record of a registered plugin.

    Attributes:
        plugin_id: Unique plugin identifier.
        name: Plugin display name.
        plugin_type: Type category.
        version: Plugin version string.
        module_path: Python import path for the plugin class.
        class_name: Class name within the module.
        config_schema: JSON Schema for plugin configuration.
        registered_by: Tenant ID that registered the plugin.
    """

    def __init__(
        self,
        plugin_id: uuid.UUID,
        name: str,
        plugin_type: str,
        version: str,
        module_path: str,
        class_name: str,
        config_schema: dict[str, Any],
        registered_by: uuid.UUID,
    ) -> None:
        """Initialize PluginRegistration.

        Args:
            plugin_id: Unique plugin ID.
            name: Plugin name.
            plugin_type: 'connector' | 'transformer' | 'quality_check' | 'exporter'.
            version: Semver string.
            module_path: Dotted Python import path.
            class_name: Class to instantiate within the module.
            config_schema: JSON Schema dict for configuration validation.
            registered_by: Tenant UUID that registered this plugin.
        """
        self.plugin_id = plugin_id
        self.name = name
        self.plugin_type = plugin_type
        self.version = version
        self.module_path = module_path
        self.class_name = class_name
        self.config_schema = config_schema
        self.registered_by = registered_by

    def to_dict(self) -> dict[str, Any]:
        """Serialize the registration record to a dict.

        Returns:
            Dict representation of the plugin registration.
        """
        return {
            "plugin_id": str(self.plugin_id),
            "name": self.name,
            "plugin_type": self.plugin_type,
            "version": self.version,
            "module_path": self.module_path,
            "class_name": self.class_name,
            "config_schema": self.config_schema,
            "registered_by": str(self.registered_by),
        }


class PluginRegistry:
    """Runtime registry for pipeline extension plugins (GAP-119).

    Maintains an in-memory catalog of registered plugins. Plugins are
    identified by (tenant_id, name) pairs. Loading is done on demand
    via importlib to avoid importing unused dependencies at startup.

    Plugin types:
        - connector: Custom data source connector (implements IngestorProtocol)
        - transformer: Custom feature transformation step
        - quality_check: Custom Great Expectations-compatible expectation
        - exporter: Custom output format writer
    """

    _VALID_PLUGIN_TYPES = frozenset({"connector", "transformer", "quality_check", "exporter"})

    def __init__(self) -> None:
        """Initialize an empty plugin registry."""
        # {plugin_id_str: PluginRegistration}
        self._registry: dict[str, PluginRegistration] = {}
        # {(tenant_id_str, name): plugin_id_str}
        self._name_index: dict[tuple[str, str], str] = {}

    def register(
        self,
        name: str,
        plugin_type: str,
        version: str,
        module_path: str,
        class_name: str,
        config_schema: dict[str, Any],
        tenant_id: uuid.UUID,
    ) -> PluginRegistration:
        """Register a new plugin.

        Args:
            name: Unique name for this plugin within the tenant.
            plugin_type: One of 'connector', 'transformer', 'quality_check', 'exporter'.
            version: Semver version string.
            module_path: Python dotted import path (e.g. 'my_pkg.connectors.custom').
            class_name: Class to instantiate within the module.
            config_schema: JSON Schema dict describing expected configuration.
            tenant_id: Registering tenant UUID.

        Returns:
            PluginRegistration record.

        Raises:
            ValueError: If plugin_type is invalid or name already registered.
        """
        if plugin_type not in self._VALID_PLUGIN_TYPES:
            raise ValueError(
                f"Invalid plugin_type '{plugin_type}'. "
                f"Must be one of: {sorted(self._VALID_PLUGIN_TYPES)}"
            )

        index_key = (str(tenant_id), name)
        if index_key in self._name_index:
            existing_id = self._name_index[index_key]
            existing = self._registry[existing_id]
            logger.warning(
                "plugin_name_conflict",
                name=name,
                existing_version=existing.version,
                new_version=version,
                tenant_id=str(tenant_id),
            )

        plugin_id = uuid.uuid4()
        registration = PluginRegistration(
            plugin_id=plugin_id,
            name=name,
            plugin_type=plugin_type,
            version=version,
            module_path=module_path,
            class_name=class_name,
            config_schema=config_schema,
            registered_by=tenant_id,
        )

        self._registry[str(plugin_id)] = registration
        self._name_index[index_key] = str(plugin_id)

        logger.info(
            "plugin_registered",
            plugin_id=str(plugin_id),
            name=name,
            plugin_type=plugin_type,
            version=version,
            tenant_id=str(tenant_id),
        )
        return registration

    def get_by_id(self, plugin_id: uuid.UUID) -> PluginRegistration | None:
        """Retrieve a plugin registration by ID.

        Args:
            plugin_id: Plugin UUID.

        Returns:
            PluginRegistration or None if not found.
        """
        return self._registry.get(str(plugin_id))

    def get_by_name(self, name: str, tenant_id: uuid.UUID) -> PluginRegistration | None:
        """Retrieve a plugin registration by tenant-scoped name.

        Args:
            name: Plugin name.
            tenant_id: Tenant UUID.

        Returns:
            PluginRegistration or None if not found.
        """
        plugin_id_str = self._name_index.get((str(tenant_id), name))
        if not plugin_id_str:
            return None
        return self._registry.get(plugin_id_str)

    def list_plugins(
        self,
        tenant_id: uuid.UUID,
        plugin_type: str | None = None,
    ) -> list[PluginRegistration]:
        """List all plugins registered for a tenant.

        Args:
            tenant_id: Tenant UUID.
            plugin_type: Optional type filter.

        Returns:
            List of matching PluginRegistration records.
        """
        tenant_str = str(tenant_id)
        results = [
            reg
            for reg in self._registry.values()
            if str(reg.registered_by) == tenant_str
            and (plugin_type is None or reg.plugin_type == plugin_type)
        ]
        return results

    def load_plugin(self, plugin_id: uuid.UUID) -> Any:
        """Import and instantiate a registered plugin class.

        Args:
            plugin_id: Plugin UUID to load.

        Returns:
            Instantiated plugin object.

        Raises:
            ValueError: If plugin_id is not registered.
            ImportError: If the plugin module cannot be imported.
            AttributeError: If class_name is not found in the module.
        """
        registration = self.get_by_id(plugin_id)
        if not registration:
            raise ValueError(f"Plugin {plugin_id} not found in registry")

        try:
            module = importlib.import_module(registration.module_path)
        except ImportError as exc:
            logger.error(
                "plugin_import_failed",
                plugin_id=str(plugin_id),
                module_path=registration.module_path,
                error=str(exc),
            )
            raise

        plugin_class = getattr(module, registration.class_name)
        plugin_instance = plugin_class()

        logger.info(
            "plugin_loaded",
            plugin_id=str(plugin_id),
            name=registration.name,
            module_path=registration.module_path,
            class_name=registration.class_name,
        )
        return plugin_instance

    def deregister(self, plugin_id: uuid.UUID, tenant_id: uuid.UUID) -> bool:
        """Remove a plugin registration.

        Args:
            plugin_id: Plugin UUID to remove.
            tenant_id: Tenant UUID (must match registered_by).

        Returns:
            True if removed, False if not found or tenant mismatch.
        """
        registration = self.get_by_id(plugin_id)
        if not registration:
            return False
        if str(registration.registered_by) != str(tenant_id):
            logger.warning(
                "plugin_deregister_denied",
                plugin_id=str(plugin_id),
                requesting_tenant=str(tenant_id),
                owner_tenant=str(registration.registered_by),
            )
            return False

        del self._registry[str(plugin_id)]
        index_key = (str(tenant_id), registration.name)
        self._name_index.pop(index_key, None)

        logger.info(
            "plugin_deregistered",
            plugin_id=str(plugin_id),
            name=registration.name,
            tenant_id=str(tenant_id),
        )
        return True


# Singleton registry for use within the application lifetime
_plugin_registry: PluginRegistry | None = None


def get_plugin_registry() -> PluginRegistry:
    """Return the singleton PluginRegistry instance.

    Returns:
        Global PluginRegistry instance.
    """
    global _plugin_registry
    if _plugin_registry is None:
        _plugin_registry = PluginRegistry()
    return _plugin_registry
