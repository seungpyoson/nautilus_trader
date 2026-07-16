#!/usr/bin/env python3

from __future__ import annotations

import tempfile
import textwrap
from pathlib import Path

from generate_polymarket_semantic_boundary import RegistryError
from generate_polymarket_semantic_boundary import load_registry
from generate_polymarket_semantic_boundary import render_rust


VALID_REGISTRY = """
schema_version = 1

[[sources]]
id = "architecture"
repository = "seungpyoson/bolt-v2"
commit = "fe368f8510000000000000000000000000000000"
path = "docs/architecture.md"
blob = "1111111111111111111111111111111111111111"
authority = "semantic_contract"

[[routes]]
id = "post_order"
method = "POST"
path = "/order"
statuses = ["live"]

[[routes]]
id = "get_exact_order"
method = "GET"
path = "/data/order/{order_id}"
statuses = ["ORDER_STATUS_LIVE"]

[[routes]]
id = "get_associated_trades"
method = "GET"
path = "/data/trades"
statuses = ["MATCHED"]

[[vocabularies]]
id = "side"
source = "architecture"
values = ["BUY", "SELL"]
[[vocabularies]]
id = "order_type"
source = "architecture"
values = ["GTC", "FOK", "GTD", "FAK"]
[[vocabularies]]
id = "trader_side"
source = "architecture"
values = ["TAKER", "MAKER"]

[[limits]]
id = "request_body_bytes"
authority = "caller"
[[limits]]
id = "request_items"
authority = "caller"
[[limits]]
id = "response_body_bytes"
authority = "caller"
[[limits]]
id = "response_items"
authority = "caller"
[[limits]]
id = "transaction_hashes"
authority = "caller"
[[limits]]
id = "trade_ids"
authority = "caller"
[[limits]]
id = "associated_trades"
authority = "caller"
[[limits]]
id = "string_bytes"
authority = "caller"
[[limits]]
id = "decimal_bytes"
authority = "caller"
[[limits]]
id = "log_items"
authority = "caller"

[[protocol_widths]]
id = "transaction_hash_bytes"
value = 32

[[capabilities]]
id = "permanent_terminality"
state = "unavailable"
reason = "no_exact_hash_tombstone"
[[capabilities]]
id = "complete_capture"
state = "unavailable"
reason = "no_complete_hash_contract"
[[capabilities]]
id = "competing_work_absence"
state = "unavailable"
reason = "no_competing_work_exclusion"
"""


def load(text: str = VALID_REGISTRY):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "registry.toml"
        path.write_text(textwrap.dedent(text), encoding="utf-8")
        return load_registry(path)


def assert_rejected(text: str, message: str) -> None:
    try:
        load(text)
    except RegistryError as e:
        assert message in str(e)
        return
    raise AssertionError(f"registry unexpectedly accepted; expected {message}")


def test_valid_registry_renders_caller_supplied_limits() -> None:
    rendered = render_rust(load())
    assert "pub struct SemanticLimitValues" in rendered
    assert "pub struct SemanticLimits" in rendered
    assert "impl Default for SemanticLimits" not in rendered
    assert '"ORDER_STATUS_LIVE"' in rendered
    assert "pub enum ProviderSide" in rendered
    assert '"TAKER"' in rendered
    assert "CURRENT_V2_UNAVAILABLE" in rendered


def test_numeric_operational_default_is_rejected() -> None:
    invalid = VALID_REGISTRY.replace(
        'id = "request_body_bytes"\nauthority = "caller"',
        'id = "request_body_bytes"\nauthority = "caller"\ndefault = 64',
    )
    assert_rejected(invalid, "unknown key")


def test_cross_route_status_reuse_is_rejected() -> None:
    invalid = VALID_REGISTRY.replace('statuses = ["ORDER_STATUS_LIVE"]', 'statuses = ["live"]')
    assert_rejected(invalid, "status reused")


def test_available_current_v2_capability_is_rejected() -> None:
    invalid = VALID_REGISTRY.replace('state = "unavailable"', 'state = "available"', 1)
    assert_rejected(invalid, "must be unavailable")


def test_missing_required_limit_is_rejected() -> None:
    invalid = VALID_REGISTRY.replace('[[limits]]\nid = "request_items"\nauthority = "caller"\n', "")
    assert_rejected(invalid, "required limits")


def test_provider_vocabulary_drift_is_rejected() -> None:
    invalid = VALID_REGISTRY.replace('values = ["BUY", "SELL"]', 'values = ["BUY", "SIDEWAYS"]')
    assert_rejected(invalid, "registered provider evidence")


if __name__ == "__main__":
    tests = [
        test_valid_registry_renders_caller_supplied_limits,
        test_numeric_operational_default_is_rejected,
        test_cross_route_status_reuse_is_rejected,
        test_available_current_v2_capability_is_rejected,
        test_missing_required_limit_is_rejected,
        test_provider_vocabulary_drift_is_rejected,
    ]
    for test in tests:
        test()
    print(f"{len(tests)} generator tests passed")
