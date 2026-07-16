#!/usr/bin/env python3

from __future__ import annotations

import sys
import tempfile
import textwrap
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from generate_polymarket_semantic_boundary import (  # noqa: E402
    RegistryError,
    load_registry,
    render_rust,
)


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

[[limits]]
id = "request_body_bytes"
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


class GeneratorTests(unittest.TestCase):
    def load(self, text: str = VALID_REGISTRY):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "registry.toml"
            path.write_text(textwrap.dedent(text), encoding="utf-8")
            return load_registry(path)

    def test_valid_registry_renders_caller_supplied_limits(self) -> None:
        rendered = render_rust(self.load())
        self.assertIn("pub struct SemanticLimitValues", rendered)
        self.assertIn("pub struct SemanticLimits", rendered)
        self.assertNotIn("impl Default for SemanticLimits", rendered)
        self.assertIn('"ORDER_STATUS_LIVE"', rendered)
        self.assertIn("CURRENT_V2_UNAVAILABLE", rendered)

    def test_numeric_operational_default_is_rejected(self) -> None:
        invalid = VALID_REGISTRY.replace(
            'id = "request_body_bytes"\nauthority = "caller"',
            'id = "request_body_bytes"\nauthority = "caller"\ndefault = 64',
        )
        with self.assertRaisesRegex(RegistryError, "unknown key"):
            self.load(invalid)

    def test_cross_route_status_reuse_is_rejected(self) -> None:
        invalid = VALID_REGISTRY.replace(
            'statuses = ["ORDER_STATUS_LIVE"]', 'statuses = ["live"]'
        )
        with self.assertRaisesRegex(RegistryError, "status reused"):
            self.load(invalid)

    def test_available_current_v2_capability_is_rejected(self) -> None:
        invalid = VALID_REGISTRY.replace(
            'state = "unavailable"', 'state = "available"', 1
        )
        with self.assertRaisesRegex(RegistryError, "must be unavailable"):
            self.load(invalid)

    def test_missing_required_limit_is_rejected(self) -> None:
        invalid = VALID_REGISTRY.replace(
            '[[limits]]\nid = "log_items"\nauthority = "caller"\n', ""
        )
        with self.assertRaisesRegex(RegistryError, "required limits"):
            self.load(invalid)


if __name__ == "__main__":
    unittest.main()
