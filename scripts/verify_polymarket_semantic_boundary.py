#!/usr/bin/env python3
"""Fail-closed source fence for the Polymarket semantic boundary."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tomllib
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
REGISTRY = ROOT / "crates/adapters/polymarket/provider-evidence/semantic-boundary.toml"
SEMANTIC = ROOT / "crates/adapters/polymarket/src/semantic"
GENERATOR = ROOT / "scripts/generate_polymarket_semantic_boundary.py"

ROUTE_LITERAL = re.compile(r'"/[A-Za-z0-9_{}?=&./-]+"')
STATUS_LIKE = re.compile(r'"(?:ORDER_STATUS_[A-Z_]+|[A-Z][A-Z_]{3,})"')
FORBIDDEN_EFFECTS = {
    "reqwest": "HTTP client",
    "hyper::": "HTTP runtime",
    "tokio::spawn": "detached task",
    "spawn_blocking": "detached blocking task",
    "std::net": "network socket",
    "TcpStream": "network socket",
    "UdpSocket": "network socket",
    "lookup_host": "DNS lookup",
    "rustls": "TLS runtime",
    "native_tls": "TLS runtime",
}
SENSITIVE_TYPES = (
    "SensitiveProviderBytes",
    "SensitiveSignedRequest",
    "SemanticCredential",
)
FORBIDDEN_TRAITS = ("Debug", "Display", "Serialize", "Deserialize")


class FenceError(ValueError):
    pass


def registered_literals() -> tuple[set[str], set[str]]:
    with REGISTRY.open("rb") as handle:
        data = tomllib.load(handle)
    routes = {row["path"] for row in data["routes"]}
    statuses = {
        status
        for row in data["routes"]
        for status in row.get("statuses", [])
    }
    return routes, statuses


def check_source(name: str, source: str, routes: set[str], statuses: set[str]) -> None:
    for token, description in FORBIDDEN_EFFECTS.items():
        if token in source:
            raise FenceError(f"{name}: forbidden {description}: {token}")

    route_literals = {match.group(0)[1:-1] for match in ROUTE_LITERAL.finditer(source)}
    if route_literals:
        literal = sorted(route_literals)[0]
        if literal in routes:
            raise FenceError(f"{name}: registered route literal must remain generated: {literal}")
        raise FenceError(f"{name}: unregistered route literal: {literal}")

    quoted_statuses = {f'"{status}"' for status in statuses}
    for status in sorted(quoted_statuses):
        if status in source:
            raise FenceError(f"{name}: registered status literal must remain generated: {status}")
    match = STATUS_LIKE.search(source)
    if match:
        raise FenceError(f"{name}: unregistered status-like literal: {match.group(0)}")

    for sensitive in SENSITIVE_TYPES:
        for trait in FORBIDDEN_TRAITS:
            impl_pattern = re.compile(rf"impl(?:<[^>]+>)?\s+{trait}\s+for\s+{sensitive}\b")
            if impl_pattern.search(source):
                raise FenceError(f"{name}: forbidden {trait} implementation for {sensitive}")
        struct_match = re.search(rf"pub struct {sensitive}\b", source)
        if struct_match:
            prefix = source[max(0, struct_match.start() - 300) : struct_match.start()]
            derive_blocks = re.findall(r"#\[derive\(([^)]*)\)\]", prefix)
            for block in derive_blocks:
                traits = {part.strip() for part in block.split(",")}
                forbidden = traits.intersection(FORBIDDEN_TRAITS)
                if forbidden:
                    found = sorted(forbidden)[0]
                    raise FenceError(f"{name}: forbidden {found} derive for {sensitive}")


def check_tree() -> None:
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


def run_self_test() -> None:
    routes, statuses = registered_literals()
    rejected = {
        "unregistered route": 'const ROUTE: &str = "/unregistered-effect";',
        "unregistered status": 'const STATUS: &str = "ORDER_STATUS_GUESSED";',
        "network effect": "fn effect() { let _ = reqwest::Client::new(); }",
        "task spawn": "fn effect() { tokio::spawn(async {}); }",
        "sensitive trait": "impl Debug for SensitiveProviderBytes {}",
    }
    for label, source in rejected.items():
        try:
            check_source(f"self-test/{label}", source, routes, statuses)
        except FenceError:
            continue
        raise FenceError(f"self-test failed to reject {label}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="verify the real semantic tree")
    parser.add_argument("--self-test", action="store_true", help="run negative fence fixtures")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.check and not args.self_test:
        raise FenceError("at least one of --check or --self-test is required")
    if args.check:
        subprocess.run(
            [sys.executable, str(GENERATOR), "--check"],
            cwd=ROOT,
            check=True,
        )
        check_tree()
        print("semantic boundary source fence passed")
    if args.self_test:
        run_self_test()
        print("semantic boundary source fence self-tests passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FenceError, subprocess.CalledProcessError) as error:
        print(f"semantic boundary source fence failed: {error}", file=sys.stderr)
        raise SystemExit(1)
