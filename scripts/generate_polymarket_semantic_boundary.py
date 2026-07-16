#!/usr/bin/env python3

from __future__ import annotations

import argparse
import re
import sys
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_REGISTRY = (
    ROOT / "crates" / "adapters" / "polymarket" / "provider-evidence" / "semantic-boundary.toml"
)
DEFAULT_OUTPUT = ROOT / "crates" / "adapters" / "polymarket" / "src" / "semantic" / "generated.rs"

REQUIRED_ROUTES = {"post_order", "get_exact_order", "get_associated_trades"}
REQUIRED_VOCABULARIES = {
    "side": ["BUY", "SELL"],
    "order_type": ["GTC", "FOK", "GTD", "FAK"],
    "trader_side": ["TAKER", "MAKER"],
}
REQUIRED_LIMITS = {
    "request_body_bytes",
    "request_items",
    "response_body_bytes",
    "response_items",
    "transaction_hashes",
    "trade_ids",
    "associated_trades",
    "string_bytes",
    "decimal_bytes",
    "log_items",
}
REQUIRED_CAPABILITIES = {
    "permanent_terminality",
    "complete_capture",
    "competing_work_absence",
}
HEX_40 = re.compile(r"^[0-9a-f]{40}$")
IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]*$")


class RegistryError(ValueError):
    pass


@dataclass(frozen=True)
class Registry:
    data: dict[str, Any]


def _reject_unknown(row: dict[str, Any], allowed: set[str], context: str) -> None:
    unknown = sorted(set(row) - allowed)
    if unknown:
        raise RegistryError(f"{context}: unknown key {unknown[0]!r}")


def _validate_unique_ids(rows: list[dict[str, Any]], context: str) -> None:
    seen: set[str] = set()
    for row in rows:
        identifier = row.get("id")
        if not isinstance(identifier, str) or not IDENTIFIER.fullmatch(identifier):
            raise RegistryError(f"{context}: invalid id {identifier!r}")
        if identifier in seen:
            raise RegistryError(f"{context}: duplicate id {identifier!r}")
        seen.add(identifier)


def _require_rows(data: dict[str, Any], key: str) -> list[dict[str, Any]]:
    rows = data.get(key)
    if not isinstance(rows, list) or not rows or not all(isinstance(row, dict) for row in rows):
        raise RegistryError(f"{key}: expected a non-empty table array")
    return rows


def _validate_sources(data: dict[str, Any]) -> list[dict[str, Any]]:
    sources = _require_rows(data, "sources")
    _validate_unique_ids(sources, "sources")
    for row in sources:
        _reject_unknown(
            row,
            {"id", "repository", "commit", "path", "blob", "authority"},
            f"source {row['id']}",
        )
        for field in ("repository", "path", "authority"):
            if not isinstance(row.get(field), str) or not row[field]:
                raise RegistryError(f"source {row['id']}: invalid {field}")
        for field in ("commit", "blob"):
            if not isinstance(row.get(field), str) or not HEX_40.fullmatch(row[field]):
                raise RegistryError(f"source {row['id']}: invalid {field}")
    return sources


def _validate_routes(data: dict[str, Any]) -> None:
    routes = _require_rows(data, "routes")
    _validate_unique_ids(routes, "routes")
    if {row["id"] for row in routes} != REQUIRED_ROUTES:
        raise RegistryError(f"routes: required routes are {sorted(REQUIRED_ROUTES)}")
    paths: set[str] = set()
    statuses: set[str] = set()
    for row in routes:
        _reject_unknown(row, {"id", "method", "path", "statuses"}, f"route {row['id']}")
        if row.get("method") not in {"GET", "POST"}:
            raise RegistryError(f"route {row['id']}: invalid method")
        route_path = row.get("path")
        if not isinstance(route_path, str) or not route_path.startswith("/"):
            raise RegistryError(f"route {row['id']}: invalid path")
        if route_path in paths:
            raise RegistryError(f"route {row['id']}: duplicate path")
        paths.add(route_path)
        _validate_statuses(row, statuses)


def _validate_statuses(row: dict[str, Any], statuses: set[str]) -> None:
    wire_values = row.get("statuses")
    if not isinstance(wire_values, list) or not wire_values:
        raise RegistryError(f"route {row['id']}: statuses must be non-empty")
    local: set[str] = set()
    for wire in wire_values:
        if not isinstance(wire, str) or not wire:
            raise RegistryError(f"route {row['id']}: invalid status")
        if wire in local:
            raise RegistryError(f"route {row['id']}: duplicate status {wire!r}")
        if wire in statuses:
            raise RegistryError(f"route {row['id']}: status reused across routes: {wire!r}")
        local.add(wire)
        statuses.add(wire)


def _validate_vocabularies(data: dict[str, Any], sources: list[dict[str, Any]]) -> None:
    vocabularies = _require_rows(data, "vocabularies")
    _validate_unique_ids(vocabularies, "vocabularies")
    if {row["id"] for row in vocabularies} != set(REQUIRED_VOCABULARIES):
        raise RegistryError(
            f"vocabularies: required vocabularies are {sorted(REQUIRED_VOCABULARIES)}"
        )
    source_ids = {row["id"] for row in sources}
    for row in vocabularies:
        _reject_unknown(row, {"id", "source", "values"}, f"vocabulary {row['id']}")
        if row.get("source") not in source_ids:
            raise RegistryError(f"vocabulary {row['id']}: source is not registered")
        if row.get("values") != REQUIRED_VOCABULARIES[row["id"]]:
            raise RegistryError(
                f"vocabulary {row['id']}: values must match registered provider evidence"
            )


def _validate_limits(data: dict[str, Any]) -> None:
    limits = _require_rows(data, "limits")
    _validate_unique_ids(limits, "limits")
    for row in limits:
        _reject_unknown(row, {"id", "authority"}, f"limit {row['id']}")
        if row.get("authority") != "caller":
            raise RegistryError(f"limit {row['id']}: authority must be caller")
    if {row["id"] for row in limits} != REQUIRED_LIMITS:
        raise RegistryError(f"limits: required limits are {sorted(REQUIRED_LIMITS)}")


def _validate_widths(data: dict[str, Any]) -> None:
    widths = _require_rows(data, "protocol_widths")
    _validate_unique_ids(widths, "protocol_widths")
    if widths != [{"id": "transaction_hash_bytes", "value": 32}]:
        raise RegistryError("protocol_widths: expected transaction_hash_bytes = 32")


def _validate_capabilities(data: dict[str, Any]) -> None:
    capabilities = _require_rows(data, "capabilities")
    _validate_unique_ids(capabilities, "capabilities")
    if {row["id"] for row in capabilities} != REQUIRED_CAPABILITIES:
        raise RegistryError(
            f"capabilities: required capabilities are {sorted(REQUIRED_CAPABILITIES)}"
        )
    for row in capabilities:
        _reject_unknown(row, {"id", "state", "reason"}, f"capability {row['id']}")
        if row.get("state") != "unavailable":
            raise RegistryError(f"capability {row['id']}: current V2 must be unavailable")
        if not isinstance(row.get("reason"), str) or not IDENTIFIER.fullmatch(row["reason"]):
            raise RegistryError(f"capability {row['id']}: invalid reason")


def load_registry(path: Path) -> Registry:
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as e:
        raise RegistryError(f"unable to load registry: {e}") from e

    _reject_unknown(
        data,
        {
            "schema_version",
            "issue_url",
            "sources",
            "routes",
            "vocabularies",
            "limits",
            "protocol_widths",
            "capabilities",
        },
        "registry",
    )
    if data.get("schema_version") != 1:
        raise RegistryError("registry: schema_version must be 1")

    sources = _validate_sources(data)
    _validate_routes(data)
    _validate_vocabularies(data, sources)
    _validate_limits(data)
    _validate_widths(data)
    _validate_capabilities(data)

    return Registry(data)


def _camel(identifier: str) -> str:
    return "".join(part.capitalize() for part in identifier.split("_"))


def _status_variant(wire: str) -> str:
    normalized = wire.removeprefix("ORDER_STATUS_").lower()
    return _camel(normalized)


def _render_limit_check(name: str) -> str:
    kind = _camel(name)
    compact_none = (
        f"                None => return Err(SemanticLimitError::Zero(SemanticLimitKind::{kind})),"
    )
    if len(kind) < 16:
        none_arm = f"{compact_none}\n"
    else:
        none_arm = (
            "                None => {\n"
            "                    return Err(SemanticLimitError::Zero(\n"
            f"                        SemanticLimitKind::{kind},\n"
            "                    ));\n"
            "                }\n"
        )
    return (
        f"            {name}: match NonZeroUsize::new(values.{name}) {{\n"
        "                Some(value) => value,\n"
        f"{none_arm}"
        "            },"
    )


def _render_status_enum(name: str, route_variant: str, statuses: list[str]) -> str:
    variants = "\n".join(f"    {_status_variant(wire)}," for wire in statuses)
    matches = "\n".join(
        f'            "{wire}" => Ok(Self::{_status_variant(wire)}),' for wire in statuses
    )
    return f"""#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum {name} {{
{variants}
}}

impl TryFrom<&str> for {name} {{
    type Error = WireValueError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {{
        match value {{
{matches}
            _ => Err(WireValueError {{
                route: SemanticRoute::{route_variant},
            }}),
        }}
    }}
}}
"""


def _render_vocabulary_enum(name: str, vocabulary_variant: str, values: list[str]) -> str:
    variants = "\n".join(f"    {_camel(wire.lower())}," for wire in values)
    matches = "\n".join(
        f'            "{wire}" => Ok(Self::{_camel(wire.lower())}),' for wire in values
    )
    return f"""#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum {name} {{
{variants}
}}

impl TryFrom<&str> for {name} {{
    type Error = VocabularyValueError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {{
        match value {{
{matches}
            _ => Err(VocabularyValueError {{
                vocabulary: SemanticVocabulary::{vocabulary_variant},
            }}),
        }}
    }}
}}
"""


def render_rust(registry: Registry) -> str:
    data = registry.data
    sources = data["sources"]
    routes = {row["id"]: row for row in data["routes"]}
    vocabularies = {row["id"]: row for row in data["vocabularies"]}
    limits = [row["id"] for row in data["limits"]]
    capabilities = data["capabilities"]

    source_rows = "\n".join(
        "    RegisteredSource {\n"
        f'        id: "{row["id"]}",\n'
        f'        repository: "{row["repository"]}",\n'
        f'        commit: "{row["commit"]}",\n'
        f'        path: "{row["path"]}",\n'
        f'        blob: "{row["blob"]}",\n'
        f'        authority: "{row["authority"]}",\n'
        "    },"
        for row in sources
    )
    limit_value_fields = "\n".join(f"    pub {name}: usize," for name in limits)
    limit_private_fields = "\n".join(f"    {name}: NonZeroUsize," for name in limits)
    limit_checks = "\n".join(_render_limit_check(name) for name in limits)
    limit_accessors = "\n\n".join(
        f"    #[must_use]\n"
        f"    pub const fn {name}(self) -> usize {{\n"
        f"        self.{name}.get()\n"
        f"    }}"
        for name in limits
    )
    limit_kinds = "\n".join(f"    {_camel(name)}," for name in limits)
    cap_variants = "\n".join(f"    {_camel(row['id'])}," for row in capabilities)
    cap_rows = "\n".join(
        "    CapabilityEvidence {\n"
        f"        capability: UnavailableCapability::{_camel(row['id'])},\n"
        f'        reason: "{row["reason"]}",\n'
        "    },"
        for row in capabilities
    )

    status_blocks = "\n".join(
        [
            _render_status_enum("PostOrderStatus", "PostOrder", routes["post_order"]["statuses"]),
            _render_status_enum(
                "ExactOrderStatus", "GetExactOrder", routes["get_exact_order"]["statuses"]
            ),
            _render_status_enum(
                "AssociatedTradeStatus",
                "GetAssociatedTrades",
                routes["get_associated_trades"]["statuses"],
            ),
        ]
    )
    vocabulary_blocks = "\n".join(
        [
            _render_vocabulary_enum("ProviderSide", "Side", vocabularies["side"]["values"]),
            _render_vocabulary_enum(
                "ProviderOrderType", "OrderType", vocabularies["order_type"]["values"]
            ),
            _render_vocabulary_enum(
                "ProviderTraderSide", "TraderSide", vocabularies["trader_side"]["values"]
            ),
        ]
    )

    return f"""// This file is generated by scripts/generate_polymarket_semantic_boundary.py.
// Do not edit by hand.

use std::num::NonZeroUsize;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SemanticRoute {{
    PostOrder,
    GetExactOrder,
    GetAssociatedTrades,
}}

impl SemanticRoute {{
    #[must_use]
    pub const fn method(self) -> &'static str {{
        match self {{
            Self::PostOrder => "{routes["post_order"]["method"]}",
            Self::GetExactOrder => "{routes["get_exact_order"]["method"]}",
            Self::GetAssociatedTrades => "{routes["get_associated_trades"]["method"]}",
        }}
    }}

    #[must_use]
    pub const fn path(self) -> &'static str {{
        match self {{
            Self::PostOrder => "{routes["post_order"]["path"]}",
            Self::GetExactOrder => "{routes["get_exact_order"]["path"]}",
            Self::GetAssociatedTrades => "{routes["get_associated_trades"]["path"]}",
        }}
    }}
}}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WireValueError {{
    pub route: SemanticRoute,
}}

{status_blocks}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SemanticVocabulary {{
    Side,
    OrderType,
    TraderSide,
}}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VocabularyValueError {{
    pub vocabulary: SemanticVocabulary,
}}

{vocabulary_blocks}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SemanticLimitKind {{
{limit_kinds}
}}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SemanticLimitError {{
    Zero(SemanticLimitKind),
}}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SemanticLimitValues {{
{limit_value_fields}
}}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SemanticLimits {{
{limit_private_fields}
}}

impl SemanticLimits {{
    pub const fn checked(values: SemanticLimitValues) -> Result<Self, SemanticLimitError> {{
        Ok(Self {{
{limit_checks}
        }})
    }}

{limit_accessors}
}}

pub const TRANSACTION_HASH_BYTES: usize = {data["protocol_widths"][0]["value"]};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UnavailableCapability {{
{cap_variants}
}}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityEvidence {{
    pub capability: UnavailableCapability,
    pub reason: &'static str,
}}

pub const CURRENT_V2_UNAVAILABLE: [CapabilityEvidence; {len(capabilities)}] = [
{cap_rows}
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegisteredSource {{
    pub id: &'static str,
    pub repository: &'static str,
    pub commit: &'static str,
    pub path: &'static str,
    pub blob: &'static str,
    pub authority: &'static str,
}}

pub const REGISTERED_SOURCES: [RegisteredSource; {len(sources)}] = [
{source_rows}
];
"""


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        rendered = render_rust(load_registry(args.registry))
    except RegistryError as e:
        print(f"semantic boundary registry error: {e}", file=sys.stderr)
        return 1

    if args.check:
        try:
            current = args.output.read_text(encoding="utf-8")
        except OSError as e:
            print(f"generated semantic boundary missing: {e}", file=sys.stderr)
            return 1
        if current != rendered:
            print("generated semantic boundary is stale", file=sys.stderr)
            return 1
        print("generated semantic boundary matches registered evidence")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    print(f"generated {args.output.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
