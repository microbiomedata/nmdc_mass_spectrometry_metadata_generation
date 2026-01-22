"""
Helpers for deriving NMDC schema metadata at runtime.

This module builds on the nmdc-schema package so downstream code no longer needs
hard-coded mappings between LinkML classes, CURIEs, and NMDC ID typecodes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from functools import lru_cache
from typing import Dict, Mapping, Optional, Tuple, Type

import nmdc_schema.nmdc as nmdc
from linkml_runtime.utils.schemaview import SchemaView
from nmdc_schema import nmdc_data
from nmdc_schema.id_helpers import (
    get_compatible_typecodes,
    get_typecode_for_future_ids,
)

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SchemaTypeMetadata:
    """Lightweight record describing a schema class and its ID constraints."""

    class_name: str
    curie: str
    compatible_typecodes: tuple[str, ...]
    preferred_typecode: str | None

    @property
    def primary_typecode(self) -> str | None:
        """Return the typecode that ID generators should prefer for new IDs."""

        if self.preferred_typecode:
            return self.preferred_typecode
        return self.compatible_typecodes[0] if self.compatible_typecodes else None


@dataclass(frozen=True)
class SchemaTypeRegistry:
    """Indexes schema metadata by class name, CURIE, and ID typecode."""

    by_class_name: Mapping[str, SchemaTypeMetadata]
    by_curie: Mapping[str, SchemaTypeMetadata]
    by_typecode: Mapping[str, SchemaTypeMetadata]


MaterialProcessingCls = Type[nmdc.MaterialProcessing]


@lru_cache(maxsize=1)
def get_schema_view() -> SchemaView:
    """Return a cached SchemaView for the packaged NMDC schema."""

    schema = nmdc_data.get_nmdc_schema_definition()
    return SchemaView(schema)


@lru_cache(maxsize=1)
def get_schema_type_registry() -> SchemaTypeRegistry:
    """Build and cache indexes that describe NMDC classes and their ID patterns."""

    view = get_schema_view()
    by_class: dict[str, SchemaTypeMetadata] = {}
    by_curie: dict[str, SchemaTypeMetadata] = {}
    by_typecode: dict[str, SchemaTypeMetadata] = {}

    for class_name, class_def in view.all_classes().items():
        curie = class_def.class_uri or f"nmdc:{class_name}"

        compatible_codes: tuple[str, ...] = tuple()
        preferred_code: str | None = None
        id_slot = view.induced_slot("id", class_name)
        if id_slot and id_slot.pattern:
            compatible_codes = tuple(get_compatible_typecodes(id_slot.pattern))
            preferred_code = get_typecode_for_future_ids(id_slot.pattern)

        metadata = SchemaTypeMetadata(
            class_name=class_name,
            curie=curie,
            compatible_typecodes=compatible_codes,
            preferred_typecode=preferred_code,
        )

        by_class[class_name] = metadata
        by_curie[curie] = metadata

        primary_code = metadata.primary_typecode
        if primary_code:
            existing = by_typecode.get(primary_code)
            if existing and existing != metadata:
                LOGGER.warning(
                    "Typecode conflict for '%s': '%s' vs '%s'",
                    primary_code,
                    existing.class_name,
                    metadata.class_name,
                )
            else:
                by_typecode[primary_code] = metadata

    return SchemaTypeRegistry(by_class, by_curie, by_typecode)


def get_metadata_for_class(class_name: str) -> SchemaTypeMetadata:
    """Look up schema metadata by LinkML class name."""

    registry = get_schema_type_registry()
    try:
        return registry.by_class_name[class_name]
    except KeyError as exc:
        raise KeyError(
            f"Class '{class_name}' is not defined in the NMDC schema"
        ) from exc


def get_metadata_for_typecode(typecode: str) -> SchemaTypeMetadata:
    """Look up schema metadata by NMDC ID typecode (e.g., 'bsm')."""

    registry = get_schema_type_registry()
    try:
        return registry.by_typecode[typecode]
    except KeyError as exc:
        raise KeyError(
            f"Typecode '{typecode}' is not defined in the NMDC schema"
        ) from exc


def get_curie_for_class(class_name: str) -> str:
    """Return the CURIE for a schema class (e.g., 'nmdc:Biosample')."""

    return get_metadata_for_class(class_name).curie


def get_typecode_for_curie(curie: str) -> str | None:
    """Return the preferred typecode for a schema class identified by CURIE."""

    registry = get_schema_type_registry()
    try:
        return registry.by_curie[curie]
    except KeyError as exc:
        raise KeyError(f"CURIE '{curie}' is not defined in the NMDC schema") from exc


@lru_cache(maxsize=1)
def _get_material_processing_registry() -> dict[str, MaterialProcessingCls]:
    """Return a mapping of material-processing class names to runtime classes."""

    view = get_schema_view()
    registry: dict[str, MaterialProcessingCls] = {}
    descendants = view.class_descendants("MaterialProcessing") or []
    for class_name in descendants:
        if class_name == "MaterialProcessing":
            continue

        cls = getattr(nmdc, class_name, None)
        if cls is None:
            continue

        if not issubclass(cls, nmdc.MaterialProcessing):
            continue

        registry[class_name] = cls

    return registry


def list_material_processing_types() -> tuple[str, ...]:
    """Return all schema class names that implement MaterialProcessing."""

    return tuple(sorted(_get_material_processing_registry().keys()))


def get_material_processing_class(process_type: str) -> MaterialProcessingCls:
    """Return the runtime NMDC class for a material-processing type name."""

    registry = _get_material_processing_registry()
    try:
        return registry[process_type]
    except KeyError as exc:
        available = ", ".join(list_material_processing_types())
        raise KeyError(
            f"'{process_type}' is not a MaterialProcessing class. Available: {available}"
        ) from exc
