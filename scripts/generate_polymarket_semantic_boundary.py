#!/usr/bin/env python3

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
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
REQUIRED_VOCABULARIES = {"side", "order_type", "trader_side"}
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
REQUIRED_NUMERIC_CONSTRAINTS = {
    "matched_not_above_original",
    "non_negative_provider_decimals",
}


class RegistryError(ValueError):
    pass


@dataclass(frozen=True)
class Registry:
    data: dict[str, Any]
    source_text: dict[str, str]


def _is_hex_40(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 40
        and all(character in "0123456789abcdef" for character in value)
    )


def _is_identifier(value: object) -> bool:
    return (
        isinstance(value, str)
        and bool(value)
        and value[0].islower()
        and value[0].isascii()
        and all(
            character.isascii() and (character.islower() or character.isdigit() or character == "_")
            for character in value[1:]
        )
    )


def _typescript_string_constant(source: str, symbol: str) -> str | None:
    prefix = f'export const {symbol} = "'
    suffix = '";'
    matches = [
        line[len(prefix) : -len(suffix)]
        for line in source.splitlines()
        if line.startswith(prefix) and line.endswith(suffix)
    ]
    if len(matches) != 1 or not matches[0].startswith("/"):
        return None
    return matches[0]


def _typescript_tokens(source: str) -> list[tuple[str, str]]:
    tokens: list[tuple[str, str]] = []
    index = 0
    while index < len(source):
        index = _skip_typescript_whitespace(source, index)
        if index >= len(source):
            break
        comment_end = _scan_typescript_comment(source, index)
        if comment_end is not None:
            index = comment_end
            continue
        string = _scan_typescript_string(source, index)
        if string is not None:
            token, index = string
            tokens.append(token)
            continue
        identifier = _scan_typescript_identifier(source, index)
        if identifier is not None:
            token, index = identifier
            tokens.append(token)
            continue
        tokens.append(("punctuation", source[index]))
        index += 1
    return tokens


def _skip_typescript_whitespace(source: str, index: int) -> int:
    while index < len(source) and source[index].isspace():
        index += 1
    return index


def _scan_typescript_comment(source: str, index: int) -> int | None:
    if source.startswith("//", index):
        newline = source.find("\n", index + 2)
        return len(source) if newline < 0 else newline + 1
    if not source.startswith("/*", index):
        return None
    end = source.find("*/", index + 2)
    if end < 0:
        raise RegistryError("unterminated TypeScript block comment")
    return end + 2


def _scan_typescript_string(source: str, index: int) -> tuple[tuple[str, str], int] | None:
    if source[index] not in {'"', "'"}:
        return None
    quote = source[index]
    cursor = index + 1
    value: list[str] = []
    while cursor < len(source) and source[cursor] != quote:
        if source[cursor] == "\\":
            raise RegistryError("escaped TypeScript evidence strings are unsupported")
        value.append(source[cursor])
        cursor += 1
    if cursor >= len(source):
        raise RegistryError("unterminated TypeScript string")
    return ("string", "".join(value)), cursor + 1


def _scan_typescript_identifier(source: str, index: int) -> tuple[tuple[str, str], int] | None:
    if not source[index].isascii() or not (source[index].isalpha() or source[index] in "_$"):
        return None
    end = index + 1
    while (
        end < len(source)
        and source[end].isascii()
        and (source[end].isalnum() or source[end] in "_$")
    ):
        end += 1
    return ("identifier", source[index:end]), end


def _typescript_enum_values(source: str, symbol: str) -> list[str]:
    tokens = _typescript_tokens(source)
    values = [value for _, value in tokens]
    prefix = ["export", "enum", symbol, "{"]
    starts = [
        index + len(prefix)
        for index in range(len(values) - len(prefix) + 1)
        if values[index : index + len(prefix)] == prefix
    ]
    if len(starts) != 1:
        raise RegistryError(f"TypeScript enum {symbol}: expected one exact declaration")
    index = starts[0]
    members: list[str] = []
    while index < len(tokens) and tokens[index][1] != "}":
        if index + 3 >= len(tokens):
            raise RegistryError(f"TypeScript enum {symbol}: incomplete member")
        name = tokens[index]
        equals = tokens[index + 1][1]
        wire = tokens[index + 2]
        separator = tokens[index + 3][1]
        if name[0] != "identifier" or equals != "=" or wire[0] != "string":
            raise RegistryError(f"TypeScript enum {symbol}: unsupported member syntax")
        if name[1] != wire[1]:
            raise RegistryError(f"TypeScript enum {symbol}: enum member must equal its wire value")
        if separator not in {",", "}"}:
            raise RegistryError(f"TypeScript enum {symbol}: missing member separator")
        members.append(wire[1])
        index += 3 if separator == "}" else 4
    if not members or index >= len(tokens) or tokens[index][1] != "}":
        raise RegistryError(f"TypeScript enum {symbol}: unterminated declaration")
    return members


def _typescript_string_union_values(source: str, symbol: str) -> list[str]:
    tokens = _typescript_tokens(source)
    candidates: list[list[str]] = []
    for start in range(len(tokens) - 3):
        if tokens[start] != ("identifier", symbol) or tokens[start + 1][1] != ":":
            continue
        index = start + 2
        members: list[str] = []
        while index < len(tokens) and tokens[index][0] == "string":
            members.append(tokens[index][1])
            index += 1
            if index < len(tokens) and tokens[index][1] == "|":
                index += 1
                continue
            break
        if members and index < len(tokens) and tokens[index][1] == ";":
            candidates.append(members)
    if len(candidates) != 1:
        raise RegistryError(f"TypeScript string union {symbol}: expected one exact declaration")
    return candidates[0]


def _reject_unknown(row: dict[str, Any], allowed: set[str], context: str) -> None:
    unknown = sorted(set(row) - allowed)
    if unknown:
        raise RegistryError(f"{context}: unknown key {unknown[0]!r}")


def _validate_unique_ids(rows: list[dict[str, Any]], context: str) -> None:
    seen: set[str] = set()
    for row in rows:
        identifier = row.get("id")
        if not _is_identifier(identifier):
            raise RegistryError(f"{context}: invalid id {identifier!r}")
        if identifier in seen:
            raise RegistryError(f"{context}: duplicate id {identifier!r}")
        seen.add(identifier)


def _require_rows(data: dict[str, Any], key: str) -> list[dict[str, Any]]:
    rows = data.get(key)
    if not isinstance(rows, list) or not rows or not all(isinstance(row, dict) for row in rows):
        raise RegistryError(f"{key}: expected a non-empty table array")
    return rows


def _git_blob_oid(content: bytes) -> str:
    header = f"blob {len(content)}\0".encode()
    return hashlib.sha1(header + content, usedforsecurity=False).hexdigest()


def _validate_sources(data: dict[str, Any], registry_path: Path) -> dict[str, str]:
    sources = _require_rows(data, "sources")
    _validate_unique_ids(sources, "sources")
    source_text: dict[str, str] = {}
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
            if not _is_hex_40(row.get(field)):
                raise RegistryError(f"source {row['id']}: invalid {field}")
        cache = registry_path.parent / "blobs" / f"{row['blob']}.b64"
        try:
            encoded = cache.read_bytes()
        except OSError as e:
            raise RegistryError(f"source {row['id']}: source cache unavailable") from e
        try:
            content = base64.b64decode(b"".join(encoded.split()), validate=True)
        except (binascii.Error, ValueError) as e:
            raise RegistryError(f"source {row['id']}: invalid source cache encoding") from e
        if _git_blob_oid(content) != row["blob"]:
            raise RegistryError(f"source {row['id']}: blob digest mismatch")
        try:
            source_text[row["id"]] = content.decode("utf-8")
        except UnicodeDecodeError as e:
            raise RegistryError(f"source {row['id']}: source cache is not UTF-8") from e
    return source_text


def _validate_routes(data: dict[str, Any], source_text: dict[str, str]) -> None:
    routes = _require_rows(data, "routes")
    _validate_unique_ids(routes, "routes")
    if {row["id"] for row in routes} != REQUIRED_ROUTES:
        raise RegistryError(f"routes: required routes are {sorted(REQUIRED_ROUTES)}")
    paths: set[str] = set()
    statuses: set[str] = set()
    for row in routes:
        _reject_unknown(
            row,
            {"id", "source", "symbol", "parameter", "status_source", "statuses"},
            f"route {row['id']}",
        )
        source = row.get("source")
        if source not in source_text:
            raise RegistryError(f"route {row['id']}: source is not registered")
        symbol = row.get("symbol")
        if (
            not isinstance(symbol, str)
            or not symbol.startswith(("GET_", "POST_"))
            or any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZ_" for character in symbol)
        ):
            raise RegistryError(f"route {row['id']}: invalid source symbol")
        route_path = _typescript_string_constant(source_text[source], symbol)
        if route_path is None:
            raise RegistryError(f"route {row['id']}: source-bound route mismatch")
        parameter = row.get("parameter")
        if parameter is not None:
            if not _is_identifier(parameter):
                raise RegistryError(f"route {row['id']}: invalid path parameter")
            if not route_path.endswith("/"):
                raise RegistryError(f"route {row['id']}: parameterized source path lacks slash")
            route_path += f"{{{parameter}}}"
        row["method"] = symbol.partition("_")[0]
        row["path"] = route_path
        if route_path in paths:
            raise RegistryError(f"route {row['id']}: duplicate path")
        paths.add(route_path)
        _validate_statuses(row, statuses, source_text)


def _validate_statuses(
    row: dict[str, Any], statuses: set[str], source_text: dict[str, str]
) -> None:
    status_source = row.get("status_source")
    if status_source not in source_text:
        raise RegistryError(f"route {row['id']}: status source is not registered")
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
        if f"`{wire}`" not in source_text[status_source]:
            raise RegistryError(f"route {row['id']}: source-bound status mismatch")
        local.add(wire)
        statuses.add(wire)


def _validate_vocabularies(data: dict[str, Any], source_text: dict[str, str]) -> None:
    vocabularies = _require_rows(data, "vocabularies")
    _validate_unique_ids(vocabularies, "vocabularies")
    if {row["id"] for row in vocabularies} != REQUIRED_VOCABULARIES:
        raise RegistryError(
            f"vocabularies: required vocabularies are {sorted(REQUIRED_VOCABULARIES)}"
        )
    for row in vocabularies:
        _reject_unknown(row, {"id", "source", "declaration", "symbol"}, f"vocabulary {row['id']}")
        source = row.get("source")
        if source not in source_text:
            raise RegistryError(f"vocabulary {row['id']}: source is not registered")
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or not symbol:
            raise RegistryError(f"vocabulary {row['id']}: invalid symbol")
        declaration = row.get("declaration")
        if declaration == "enum":
            values = _typescript_enum_values(source_text[source], symbol)
        elif declaration == "string_union":
            values = _typescript_string_union_values(source_text[source], symbol)
        else:
            raise RegistryError(f"vocabulary {row['id']}: unsupported declaration")
        if len(values) != len(set(values)):
            raise RegistryError(f"vocabulary {row['id']}: duplicate source value")
        row["values"] = values


def _validate_limits(data: dict[str, Any]) -> None:
    limits = _require_rows(data, "limits")
    _validate_unique_ids(limits, "limits")
    for row in limits:
        _reject_unknown(row, {"id", "authority"}, f"limit {row['id']}")
        if row.get("authority") != "caller":
            raise RegistryError(f"limit {row['id']}: authority must be caller")
    if {row["id"] for row in limits} != REQUIRED_LIMITS:
        raise RegistryError(f"limits: required limits are {sorted(REQUIRED_LIMITS)}")


def _validate_widths(data: dict[str, Any], source_text: dict[str, str]) -> None:
    widths = _require_rows(data, "protocol_widths")
    _validate_unique_ids(widths, "protocol_widths")
    if len(widths) != 1:
        raise RegistryError("protocol_widths: expected one transaction hash width")
    row = widths[0]
    _reject_unknown(row, {"id", "value", "source"}, "protocol width")
    if row.get("id") != "transaction_hash_bytes" or not isinstance(row.get("value"), int):
        raise RegistryError("protocol_widths: invalid transaction hash width")
    source = row.get("source")
    if source not in source_text:
        raise RegistryError("protocol_widths: source is not registered")
    if f"canonical {row['value']}-byte transaction hashes" not in source_text[source]:
        raise RegistryError("protocol_widths: source-bound width mismatch")


def _validate_capabilities(data: dict[str, Any], source_text: dict[str, str]) -> None:
    capabilities = _require_rows(data, "capabilities")
    _validate_unique_ids(capabilities, "capabilities")
    if {row["id"] for row in capabilities} != REQUIRED_CAPABILITIES:
        raise RegistryError(
            f"capabilities: required capabilities are {sorted(REQUIRED_CAPABILITIES)}"
        )
    for row in capabilities:
        _reject_unknown(
            row, {"id", "state", "reason", "source", "evidence"}, f"capability {row['id']}"
        )
        if row.get("state") != "unavailable":
            raise RegistryError(f"capability {row['id']}: current V2 must be unavailable")
        if not _is_identifier(row.get("reason")):
            raise RegistryError(f"capability {row['id']}: invalid reason")
        source = row.get("source")
        evidence = row.get("evidence")
        if source not in source_text or not isinstance(evidence, str) or not evidence:
            raise RegistryError(f"capability {row['id']}: invalid source evidence")
        if evidence not in source_text[source]:
            raise RegistryError(f"capability {row['id']}: source evidence mismatch")


def _validate_numeric_constraints(data: dict[str, Any], source_text: dict[str, str]) -> None:
    constraints = _require_rows(data, "numeric_constraints")
    _validate_unique_ids(constraints, "numeric_constraints")
    if {row["id"] for row in constraints} != REQUIRED_NUMERIC_CONSTRAINTS:
        raise RegistryError(
            f"numeric_constraints: required constraints are {sorted(REQUIRED_NUMERIC_CONSTRAINTS)}"
        )
    for row in constraints:
        _reject_unknown(row, {"id", "source", "evidence"}, f"constraint {row['id']}")
        source = row.get("source")
        evidence = row.get("evidence")
        if source not in source_text or not isinstance(evidence, list) or not evidence:
            raise RegistryError(f"constraint {row['id']}: invalid source evidence")
        for excerpt in evidence:
            if not isinstance(excerpt, str) or excerpt not in source_text[source]:
                raise RegistryError(f"constraint {row['id']}: source evidence mismatch")


def load_registry(path: Path) -> Registry:
    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as e:
        raise RegistryError(f"unable to load registry: {e}") from e

    _reject_unknown(
        data,
        {
            "schema_version",
            "base_revision",
            "issue_url",
            "sources",
            "routes",
            "vocabularies",
            "limits",
            "protocol_widths",
            "capabilities",
            "numeric_constraints",
        },
        "registry",
    )
    if data.get("schema_version") != 1:
        raise RegistryError("registry: schema_version must be 1")
    if not _is_hex_40(data.get("base_revision")):
        raise RegistryError("registry: invalid base_revision")

    source_text = _validate_sources(data, path)
    _validate_routes(data, source_text)
    _validate_vocabularies(data, source_text)
    _validate_limits(data)
    _validate_widths(data, source_text)
    _validate_capabilities(data, source_text)
    _validate_numeric_constraints(data, source_text)

    return Registry(data, source_text)


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
    numeric_constraints = data["numeric_constraints"]

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
    numeric_variants = "\n".join(f"    {_camel(row['id'])}," for row in numeric_constraints)
    numeric_rows = "\n".join(
        f"    NumericConstraint::{_camel(row['id'])}," for row in numeric_constraints
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
pub enum NumericConstraint {{
{numeric_variants}
}}

pub const NUMERIC_CONSTRAINTS: [NumericConstraint; {len(numeric_constraints)}] = [
{numeric_rows}
];

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
