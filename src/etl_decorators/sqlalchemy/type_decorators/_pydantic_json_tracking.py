from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from pydantic import BaseModel

from ._pydantic_json_core import _MutablePydanticModelList, _attach_parent, _attach_parent_list
from .pydantic_json_types import PydanticJSON, PydanticJSONList


def _attach_parents_for_instance(target: Any) -> None:
    """Attach parent pointers for all PydanticJSON(/List) attrs on an instance."""

    mapper = sa.inspect(target.__class__)
    for prop in mapper.column_attrs:
        key = prop.key
        try:
            col = prop.columns[0]
            typ = col.type
        except Exception:
            continue

        if isinstance(typ, PydanticJSON):
            v = getattr(target, key, None)
            if isinstance(v, BaseModel):
                _attach_parent(v, target, key)
        elif isinstance(typ, PydanticJSONList):
            v = getattr(target, key, None)
            if isinstance(v, list):
                _attach_parent_list(v, target, key)


def _setup_attribute_set_listeners() -> None:
    """Attach parent pointers on attribute set operations.

    This is required so that newly assigned Pydantic models (and list items)
    have a parent pointer immediately, enabling dirty tracking for in-place
    mutation.
    """

    def on_mapper_configured(mapper, class_):  # noqa: ANN001
        # ``Mapper.non_primary`` was removed in SQLAlchemy 2.0; the attribute is
        # absent (not merely False) there, so use a defaulted access.
        if getattr(mapper, "non_primary", False):
            return

        for prop in mapper.column_attrs:
            key = prop.key
            try:
                col = prop.columns[0]
                typ = col.type
            except Exception:
                continue

            if not isinstance(typ, (PydanticJSON, PydanticJSONList)):
                continue

            attr = getattr(class_, key)

            def _set(target, value, oldvalue, initiator, key=key, typ=typ):  # noqa: ANN001,B023,E501
                if isinstance(typ, PydanticJSON):
                    if isinstance(value, BaseModel):
                        _attach_parent(value, target.obj(), key)
                else:
                    if isinstance(value, list):
                        _attach_parent_list(value, target.obj(), key)
                return value

            sa.event.listen(attr, "set", _set, raw=True, retval=True)

    sa.event.listen(sa.orm.Mapper, "mapper_configured", on_mapper_configured)


_TRACKING_SETUP_DONE = False


def setup_pydantic_json_tracking() -> None:
    """Register SQLAlchemy listeners and mutable associations (idempotent)."""

    global _TRACKING_SETUP_DONE
    if _TRACKING_SETUP_DONE:
        return

    # Mapper events: attach parent pointers after load/refresh.
    @sa.event.listens_for(sa.orm.Mapper, "load")
    def _pydanticjson_attach_parent_on_load(target, context):  # noqa: ANN001
        _attach_parents_for_instance(target)

    @sa.event.listens_for(sa.orm.Mapper, "refresh")
    def _pydanticjson_attach_parent_on_refresh(
        target, context, attrs
    ):  # noqa: ANN001
        # After Session.expire() a refresh will replace attribute values; re-attach.
        _attach_parents_for_instance(target)

    # Associate our mutable container with the list type decorator so SQLAlchemy will
    # automatically coerce loaded values and track list-level changes.
    _MutablePydanticModelList.associate_with(PydanticJSONList)
    _setup_attribute_set_listeners()

    _TRACKING_SETUP_DONE = True
