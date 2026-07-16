#!/usr/bin/env python3
"""Fail-closed source fence for the Polymarket semantic boundary."""

from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

from generate_polymarket_semantic_boundary import load_registry
from generate_polymarket_semantic_boundary import render_rust


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "crates/adapters/polymarket/provider-evidence/semantic-boundary.toml"
SEMANTIC = ROOT / "crates/adapters/polymarket/src/semantic"

SENSITIVE_TYPES = (
    "SensitiveProviderBytes",
    "SensitiveSignedRequest",
    "SemanticCredential",
)
FORBIDDEN_TRAITS = ("Debug", "Display", "Serialize", "Deserialize")
ALLOWED_QUALIFIED_ROOTS = {
    "aws_lc_rs",
    "capabilities",
    "collector",
    "de",
    "decode",
    "digest",
    "generated",
    "hooks",
    "nautilus_polymarket",
    "rust_decimal",
    "self",
    "serde",
    "serde_json",
    "sensitive",
    "std",
    "super",
    "zeroize",
}
ALLOWED_STD_MODULES = {"fmt", "num", "str"}
FORBIDDEN_OUTPUT_MACROS = ("dbg!", "eprint!", "eprintln!", "print!", "println!")
FORBIDDEN_OUTPUT_MACRO_NAMES = {macro.removesuffix("!") for macro in FORBIDDEN_OUTPUT_MACROS}
ALLOWED_INTERNAL_CALLBACK_COUNTS = {"decode.rs": {"FnMut": 3}}
SENSITIVE_PROVIDER_METHODS = {
    "checked",
    "metadata",
    "decode_post_order",
    "decode_exact_order",
    "decode_associated_trades",
}
GIT = shutil.which("git")


class FenceError(ValueError):
    pass


@dataclass(frozen=True)
class RustToken:
    kind: str
    value: str


def rust_tokens(source: str) -> list[RustToken]:
    tokens: list[RustToken] = []
    index = 0
    while index < len(source):
        index = _skip_rust_whitespace(source, index)
        if index >= len(source):
            break
        matched, token, index = _scan_rust_item(source, index)
        if not matched:
            token = RustToken("punctuation", source[index])
            index += 1
        if token is not None:
            tokens.append(token)
    return tokens


def _skip_rust_whitespace(source: str, index: int) -> int:
    while index < len(source) and source[index].isspace():
        index += 1
    return index


def _scan_rust_item(source: str, index: int) -> tuple[bool, RustToken | None, int]:
    for scanner in (
        _scan_rust_line_comment,
        _scan_rust_block_comment,
        _scan_rust_raw_string,
        _scan_rust_string,
        _scan_rust_identifier,
        _scan_rust_double_colon,
    ):
        result = scanner(source, index)
        if result is not None:
            token, end = result
            return True, token, end
    return False, None, index


def _scan_rust_line_comment(source: str, index: int) -> tuple[None, int] | None:
    if not source.startswith("//", index):
        return None
    newline = source.find("\n", index + 2)
    return None, len(source) if newline < 0 else newline + 1


def _scan_rust_block_comment(source: str, index: int) -> tuple[None, int] | None:
    if not source.startswith("/*", index):
        return None
    depth = 1
    cursor = index + 2
    while cursor < len(source) and depth:
        if source.startswith("/*", cursor):
            depth += 1
            cursor += 2
        elif source.startswith("*/", cursor):
            depth -= 1
            cursor += 2
        else:
            cursor += 1
    if depth:
        raise FenceError("unterminated Rust block comment")
    return None, cursor


def _scan_rust_raw_string(source: str, index: int) -> tuple[RustToken, int] | None:
    if source[index] != "r" or index + 1 >= len(source):
        return None
    marker = index + 1
    while marker < len(source) and source[marker] == "#":
        marker += 1
    if marker >= len(source) or source[marker] != '"':
        return None
    hashes = source[index + 1 : marker]
    end_marker = '"' + hashes
    end = source.find(end_marker, marker + 1)
    if end < 0:
        raise FenceError("unterminated Rust raw string")
    return RustToken("string", source[marker + 1 : end]), end + len(end_marker)


def _scan_rust_string(source: str, index: int) -> tuple[RustToken, int] | None:
    if source[index] != '"':
        return None
    value: list[str] = []
    cursor = index + 1
    while cursor < len(source) and source[cursor] != '"':
        if source[cursor] == "\\":
            if cursor + 1 >= len(source):
                raise FenceError("unterminated Rust string escape")
            value.extend((source[cursor], source[cursor + 1]))
            cursor += 2
        else:
            value.append(source[cursor])
            cursor += 1
    if cursor >= len(source):
        raise FenceError("unterminated Rust string")
    return RustToken("string", "".join(value)), cursor + 1


def _scan_rust_identifier(source: str, index: int) -> tuple[RustToken, int] | None:
    character = source[index]
    if not character.isascii() or not (character.isalpha() or character == "_"):
        return None
    end = index + 1
    while (
        end < len(source)
        and source[end].isascii()
        and (source[end].isalnum() or source[end] == "_")
    ):
        end += 1
    return RustToken("identifier", source[index:end]), end


def _scan_rust_double_colon(source: str, index: int) -> tuple[RustToken, int] | None:
    if not source.startswith("::", index):
        return None
    return RustToken("punctuation", "::"), index + 2


def _token_values(tokens: list[RustToken]) -> list[str]:
    return [token.value for token in tokens]


def registered_literals() -> tuple[set[str], set[str]]:
    data = load_registry(REGISTRY).data
    routes = {row["path"] for row in data["routes"]}
    statuses = {status for row in data["routes"] for status in row.get("statuses", [])}
    vocabulary_values = {value for row in data["vocabularies"] for value in row.get("values", [])}
    return routes, statuses | vocabulary_values


def _check_effects(name: str, source: str) -> None:
    values = _token_values(rust_tokens(source))
    _check_use_roots(name, values)
    _check_qualified_roots(name, values)
    _check_effect_calls(name, values)
    _check_effect_capabilities(name, values)


def _check_qualified_roots(name: str, values: list[str]) -> None:
    for index, value in enumerate(values[:-1]):
        nested = (
            index > 1
            and values[index - 1] == "::"
            and values[index - 2]
            and (values[index - 2][0].isalpha() or values[index - 2][0] == "_")
        )
        if value == "::" or values[index + 1] != "::" or nested:
            continue
        if index + 2 < len(values) and values[index + 2] == "<":
            continue
        if not value or not value[0].islower():
            continue
        if value not in ALLOWED_QUALIFIED_ROOTS:
            raise FenceError(f"{name}: unregistered qualified effect root: {value}")
        if value == "std" and index + 2 < len(values):
            module = values[index + 2]
            if module not in ALLOWED_STD_MODULES:
                raise FenceError(f"{name}: unregistered std effect module: {module}")


def _check_use_roots(name: str, values: list[str]) -> None:
    for index, value in enumerate(values[:-1]):
        if value != "use":
            continue
        root_index = index + 1
        if values[root_index] == "::":
            root_index += 1
        if root_index >= len(values) or values[root_index] not in ALLOWED_QUALIFIED_ROOTS:
            root = values[root_index] if root_index < len(values) else "<missing>"
            raise FenceError(f"{name}: unregistered import root: {root}")


def _check_effect_calls(name: str, values: list[str]) -> None:
    for index, value in enumerate(values[:-1]):
        if value in FORBIDDEN_OUTPUT_MACRO_NAMES and values[index + 1] == "!":
            raise FenceError(f"{name}: forbidden output sink: {value}!")
        if value == "spawn" and values[index + 1] == "(":
            raise FenceError(f"{name}: unregistered spawn call")


def _check_effect_capabilities(name: str, values: list[str]) -> None:
    for forbidden in ("unsafe", "extern"):
        if forbidden in values:
            raise FenceError(f"{name}: forbidden {forbidden} capability")
    allowed = ALLOWED_INTERNAL_CALLBACK_COUNTS.get(Path(name).name, {})
    for callback in ("Fn", "FnMut", "FnOnce"):
        if values.count(callback) != allowed.get(callback, 0):
            raise FenceError(f"{name}: unregistered callback capability: {callback}")
    if _contains_token_sequence(values, [":", "fn", "("]):
        raise FenceError(f"{name}: unregistered function-pointer callback capability")


def _check_literals(name: str, source: str, routes: set[str], statuses: set[str]) -> None:
    strings = {token.value for token in rust_tokens(source) if token.kind == "string"}
    for literal in sorted((routes | statuses) & strings):
        raise FenceError(f"{name}: registered provider literal must remain generated: {literal}")


def _check_sensitive_traits(name: str, source: str) -> None:
    values = _token_values(rust_tokens(source))
    for sensitive in SENSITIVE_TYPES:
        for trait in FORBIDDEN_TRAITS:
            if _contains_token_sequence(values, ["impl", trait, "for", sensitive]):
                raise FenceError(f"{name}: forbidden {trait} implementation for {sensitive}")
        struct_index = _sequence_index(values, ["pub", "struct", sensitive])
        if struct_index is not None:
            attributes = _attribute_tokens_before(values, struct_index)
            for trait in FORBIDDEN_TRAITS:
                if any("derive" in attribute and trait in attribute for attribute in attributes):
                    raise FenceError(f"{name}: forbidden {trait} derive for {sensitive}")


def _sequence_index(values: list[str], sequence: list[str]) -> int | None:
    width = len(sequence)
    for index in range(len(values) - width + 1):
        if values[index : index + width] == sequence:
            return index
    return None


def _contains_token_sequence(values: list[str], sequence: list[str]) -> bool:
    return _sequence_index(values, sequence) is not None


def _sequence_count(values: list[str], sequence: list[str]) -> int:
    width = len(sequence)
    return sum(
        values[index : index + width] == sequence for index in range(len(values) - width + 1)
    )


def _attribute_tokens_before(values: list[str], index: int) -> list[list[str]]:
    attributes: list[list[str]] = []
    cursor = index - 1
    while cursor >= 0 and values[cursor] == "]":
        depth = 1
        start = cursor - 1
        while start >= 0 and depth:
            if values[start] == "]":
                depth += 1
            elif values[start] == "[":
                depth -= 1
            start -= 1
        if depth or start < 0 or values[start] != "#":
            break
        attributes.append(values[start + 2 : cursor])
        cursor = start - 1
    return attributes


def check_source(name: str, source: str, routes: set[str], statuses: set[str]) -> None:
    _check_effects(name, source)
    _check_literals(name, source, routes, statuses)
    _check_sensitive_traits(name, source)
    values = _token_values(rust_tokens(source))
    if name.endswith("sensitive.rs"):
        _check_sensitive_provider_impl(name, values)


def _check_sensitive_provider_impl(name: str, values: list[str]) -> None:
    expected_layout = [
        "pub",
        "struct",
        "SensitiveProviderBytes",
        "{",
        "bytes",
        ":",
        "Zeroizing",
        "<",
        "Vec",
        "<",
        "u8",
        ">",
        ">",
        ",",
        "limits",
        ":",
        "SemanticLimits",
        ",",
        "metadata",
        ":",
        "RedactedMetadata",
        ",",
        "}",
    ]
    if _sequence_count(values, expected_layout) != 1:
        raise FenceError(f"{name}: sensitive provider field layout or visibility drifted")
    methods = _impl_method_names(values, "SensitiveProviderBytes")
    if methods != SENSITIVE_PROVIDER_METHODS:
        raise FenceError(f"{name}: sensitive provider method surface drifted")
    raw_access = ["self", ".", "bytes", ".", "as_slice", "(", ")"]
    allowed_calls = (
        ["decode_post_order_borrowed", "(", *raw_access, ","],
        ["decode_exact_order_borrowed", "(", *raw_access, ","],
        ["decode_associated_trades_borrowed", "(", *raw_access, ","],
    )
    for call in allowed_calls:
        if not _contains_token_sequence(values, call):
            raise FenceError(f"{name}: route-specific sensitive decode call drifted")
    if _sequence_count(values, ["self", ".", "bytes"]) != len(allowed_calls):
        raise FenceError(f"{name}: sensitive byte field access drifted")


def _impl_method_names(values: list[str], type_name: str) -> set[str]:
    start = _sequence_index(values, ["impl", type_name, "{"])
    if start is None:
        return set()
    cursor = start + 3
    depth = 1
    methods: set[str] = set()
    while cursor < len(values) and depth:
        if values[cursor] == "{":
            depth += 1
        elif values[cursor] == "}":
            depth -= 1
        elif depth == 1 and values[cursor] == "fn" and cursor + 1 < len(values):
            methods.add(values[cursor + 1])
        cursor += 1
    return methods


def check_generated() -> None:
    registry = load_registry(REGISTRY)
    expected = render_rust(registry)
    generated = SEMANTIC / "generated.rs"
    if generated.read_text() != expected:
        raise FenceError("generated semantic boundary is stale")


def check_tree(base_revision: str) -> None:
    routes, statuses = registered_literals()
    for path in sorted(SEMANTIC.glob("*.rs")):
        if path.name == "generated.rs":
            continue
        check_source(str(path.relative_to(ROOT)), path.read_text(), routes, statuses)

    generated = SEMANTIC / "generated.rs"
    generated_source = generated.read_text()
    for literal in sorted(routes | statuses):
        if f'"{literal}"' not in generated_source:
            raise FenceError(f"generated artifact missing registered literal: {literal}")

    capability_source = (SEMANTIC / "capabilities.rs").read_text()
    if "Ok(AutonomousEntryCapability" in capability_source:
        raise FenceError("capabilities.rs: autonomous entry became constructable")

    check_diff_confinement(base_revision)


def check_diff_confinement(base_revision: str) -> None:
    diff = _run_git(["diff", "--name-only", base_revision])
    untracked = _run_git(["ls-files", "--others", "--exclude-standard"])
    if diff.returncode != 0 or untracked.returncode != 0:
        raise FenceError("unable to inspect issue-bound diff confinement")
    allowed_exact = {
        "crates/adapters/polymarket/src/lib.rs",
    }
    semantic_prefix = "crates/adapters/polymarket/src/semantic/"
    changed_paths = set(diff.stdout.splitlines()) | set(untracked.stdout.splitlines())
    for changed in sorted(changed_paths):
        if not changed.startswith("crates/adapters/polymarket/src/"):
            continue
        if changed in allowed_exact or changed.startswith(semantic_prefix):
            continue
        raise FenceError(f"unregistered Polymarket runtime source in issue-bound diff: {changed}")
    lib_diff = _run_git(
        [
            "diff",
            "--unified=0",
            base_revision,
            "--",
            "crates/adapters/polymarket/src/lib.rs",
        ],
    )
    if lib_diff.returncode != 0:
        raise FenceError("unable to inspect the Polymarket module export")
    additions = [
        line
        for line in lib_diff.stdout.splitlines()
        if line.startswith("+") and not line.startswith("+++")
    ]
    if additions != ["+pub mod semantic;"]:
        raise FenceError("Polymarket module export drifted beyond the registered semantic module")


def _run_git(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    if GIT is None:
        raise FenceError("git executable is unavailable")
    return subprocess.run(  # noqa: S603 -- fixed executable; revisions are validated hex.
        [GIT, *arguments],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )


def run_self_test() -> None:
    routes, statuses = registered_literals()
    rejected = {
        "registered route duplication": f'const ROUTE: &str = "{sorted(routes)[0]}";',
        "registered status duplication": f'const STATUS: &str = "{sorted(statuses)[0]}";',
        "network effect": "fn effect() { let _ = reqwest::Client::new(); }",
        "absolute network effect": "fn effect() { let _ = ::reqwest::Client::new(); }",
        "crate indirection": "fn effect() { crate::client::Client::new(); }",
        "aliased import": "use reqwest as de; fn effect() { de::Client::new(); }",
        "task spawn": "fn effect() { tokio::spawn(async {}); }",
        "alternate task spawn": "fn effect() { tokio::task::spawn(async {}); }",
        "thread spawn": "fn effect() { std::thread::spawn(effect); }",
        "alternate socket": "fn effect() { socket2::Socket::new(domain, kind, protocol); }",
        "alternate client": "fn effect() { ureq::get(endpoint).call(); }",
        "logging sink": 'fn effect() { println!("provider bytes"); }',
        "public callback": "pub fn run_effect(f: impl FnOnce()) { f(); }",
        "function pointer callback": "pub fn run_effect(f: fn()) { f(); }",
        "foreign effect": 'unsafe extern "C" { fn connect(fd: i32) -> i32; }',
        "sensitive trait": "impl Debug for SensitiveProviderBytes {}",
    }
    for label, source in rejected.items():
        try:
            check_source(f"self-test/{label}", source, routes, statuses)
        except FenceError:
            continue
        raise FenceError(f"self-test failed to reject {label}")
    sensitive_source = (SEMANTIC / "sensitive.rs").read_text()
    sensitive_mutations = {
        "public sensitive field": sensitive_source.replace(
            "    bytes: Zeroizing<Vec<u8>>,",
            "    pub(crate) bytes: Zeroizing<Vec<u8>>,",
            1,
        ),
        "alternate sensitive projection": sensitive_source.replace(
            "self.bytes.as_slice()",
            "self.bytes.as_ref()",
            1,
        ),
    }
    for label, mutation in sensitive_mutations.items():
        try:
            check_source("self-test/sensitive.rs", mutation, routes, statuses)
        except FenceError:
            continue
        raise FenceError(f"self-test failed to reject {label}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="verify the real semantic tree")
    parser.add_argument("--self-test", action="store_true", help="run negative fence fixtures")
    parser.add_argument("--base-revision", help="trusted issue-bound merge base")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.check and not args.self_test:
        raise FenceError("at least one of --check or --self-test is required")
    if args.check:
        declared_base = load_registry(REGISTRY).data["base_revision"]
        trusted_base = args.base_revision or os.environ.get("CHANGED_BASE_SHA") or declared_base
        if (
            args.base_revision or os.environ.get("CHANGED_BASE_SHA")
        ) and trusted_base != declared_base:
            raise FenceError("trusted merge base disagrees with the registered base revision")
        check_generated()
        check_tree(trusted_base)
        print("semantic boundary source fence passed")
    if args.self_test:
        run_self_test()
        print("semantic boundary source fence self-tests passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except FenceError as e:
        print(f"semantic boundary source fence failed: {e}", file=sys.stderr)
        raise SystemExit(1)
