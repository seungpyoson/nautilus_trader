#!/usr/bin/env python3

from __future__ import annotations

import base64
import hashlib
import tempfile
import textwrap
from pathlib import Path

from generate_polymarket_semantic_boundary import RegistryError
from generate_polymarket_semantic_boundary import load_registry
from generate_polymarket_semantic_boundary import render_rust


VALID_REGISTRY = """
schema_version = 1
base_revision = "d636f17604cdbddc28ad40e0e15720e2d19bf860"

[[sources]]
id = "architecture"
repository = "seungpyoson/bolt-v2"
commit = "2222222222222222222222222222222222222222"
path = "docs/architecture.md"
blob = "1111111111111111111111111111111111111111"
authority = "semantic_contract"

[[routes]]
id = "post_order"
source = "architecture"
symbol = "POST_ORDER"
status_source = "architecture"
statuses = ["live"]

[[routes]]
id = "get_exact_order"
source = "architecture"
symbol = "GET_ORDER"
parameter = "order_id"
status_source = "architecture"
statuses = ["ORDER_STATUS_LIVE"]

[[routes]]
id = "get_associated_trades"
source = "architecture"
symbol = "GET_TRADES"
status_source = "architecture"
statuses = ["MATCHED"]

[[vocabularies]]
id = "side"
source = "architecture"
declaration = "enum"
symbol = "Side"
[[vocabularies]]
id = "order_type"
source = "architecture"
declaration = "enum"
symbol = "OrderType"
[[vocabularies]]
id = "trader_side"
source = "architecture"
declaration = "string_union"
symbol = "trader_side"

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
source = "architecture"

[[capabilities]]
id = "permanent_terminality"
state = "unavailable"
reason = "no_exact_hash_tombstone"
source = "architecture"
evidence = "no permanent maker-controlled tombstone"
[[capabilities]]
id = "complete_capture"
state = "unavailable"
reason = "no_complete_hash_contract"
source = "architecture"
evidence = "no reviewed complete-at-most-64 hash-set contract"
[[capabilities]]
id = "competing_work_absence"
state = "unavailable"
reason = "no_competing_work_exclusion"
source = "architecture"
evidence = "submit/delay/retry/match/duplicate/preapproval work"
"""


SOURCE_BYTES = b"""export const POST_ORDER = "/order";
export const GET_ORDER = "/data/order/";
export const GET_TRADES = "/data/trades";
export enum Side { BUY = "BUY", SELL = "SELL" }
export enum OrderType { GTC = "GTC", FOK = "FOK", GTD = "GTD", FAK = "FAK" }
export interface Trade { trader_side: "TAKER" | "MAKER"; }
original_size: string; size_matched: string;
uint256 makerAmount; uint256 takerAmount; uint248 remaining;
POST admits `live`; GET admits `ORDER_STATUS_LIVE`; trades admit `MATCHED`.
canonical 32-byte transaction hashes
no permanent maker-controlled tombstone
no reviewed complete-at-most-64 hash-set contract
submit/delay/retry/match/duplicate/preapproval work
"""


def git_object_oid(kind: str, content: bytes) -> str:
    header = f"{kind} {len(content)}\0".encode()
    return hashlib.sha1(header + content, usedforsecurity=False).hexdigest()


def git_tree(entries: list[tuple[str, str, str]]) -> tuple[str, bytes]:
    content = b"".join(
        f"{mode} {name}\0".encode() + bytes.fromhex(oid) for mode, name, oid in entries
    )
    return git_object_oid("tree", content), content


def load(
    text: str = VALID_REGISTRY,
    *,
    source_bytes: bytes = SOURCE_BYTES,
    cache_bytes: bytes | None = None,
    commit_cache_bytes: bytes | None = None,
    root_tree_cache_bytes: bytes | None = None,
    tree_blob_oid: str | None = None,
    write_cache: bool = True,
):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "registry.toml"
        blob = git_object_oid("blob", source_bytes)
        docs_tree, docs_content = git_tree(
            [("100644", "architecture.md", tree_blob_oid or blob)],
        )
        root_tree, root_content = git_tree([("40000", "docs", docs_tree)])
        commit_content = f"tree {root_tree}\n\nsource proof fixture\n".encode()
        commit = git_object_oid("commit", commit_content)
        text = text.replace("1111111111111111111111111111111111111111", blob)
        text = text.replace("2222222222222222222222222222222222222222", commit)
        path.write_text(textwrap.dedent(text), encoding="utf-8")
        objects = path.parent / "objects"
        objects.mkdir()
        for oid, kind, content in (
            (commit, "commit", commit_cache_bytes or commit_content),
            (root_tree, "tree", root_tree_cache_bytes or root_content),
            (docs_tree, "tree", docs_content),
        ):
            (objects / f"{oid}.{kind}.b64").write_bytes(base64.b64encode(content))
        if write_cache:
            cache = path.parent / "blobs" / f"{blob}.b64"
            cache.parent.mkdir()
            cache.write_bytes(base64.b64encode(cache_bytes or source_bytes))
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
    invalid_source = SOURCE_BYTES.replace(b'SELL = "SELL"', b'SELL = "SIDEWAYS"')
    try:
        load(source_bytes=invalid_source)
    except RegistryError as e:
        assert "enum member must equal its wire value" in str(e)
        return
    raise AssertionError("registry unexpectedly accepted provider vocabulary drift")


def test_missing_source_cache_is_rejected() -> None:
    try:
        load(write_cache=False)
    except RegistryError as e:
        assert "source cache" in str(e)
        return
    raise AssertionError("registry unexpectedly accepted a missing source cache")


def test_tampered_source_cache_is_rejected() -> None:
    try:
        load(cache_bytes=SOURCE_BYTES + b"tampered")
    except RegistryError as e:
        assert "blob digest" in str(e)
        return
    raise AssertionError("registry unexpectedly accepted a tampered source cache")


def test_self_consistent_blob_with_false_path_is_rejected() -> None:
    invalid = VALID_REGISTRY.replace('path = "docs/architecture.md"', 'path = "docs/false.md"')
    assert_rejected(invalid, "path is absent from commit proof")


def test_missing_commit_proof_is_rejected() -> None:
    invalid = VALID_REGISTRY.replace(
        'commit = "2222222222222222222222222222222222222222"',
        'commit = "3333333333333333333333333333333333333333"',
    )
    assert_rejected(invalid, "commit proof unavailable")


def test_tampered_commit_proof_is_rejected() -> None:
    try:
        load(commit_cache_bytes=b"tampered commit")
    except RegistryError as e:
        assert "commit proof digest mismatch" in str(e)
        return
    raise AssertionError("registry unexpectedly accepted a tampered commit proof")


def test_tampered_tree_proof_is_rejected() -> None:
    try:
        load(root_tree_cache_bytes=b"tampered tree")
    except RegistryError as e:
        assert "tree proof digest mismatch" in str(e)
        return
    raise AssertionError("registry unexpectedly accepted a tampered tree proof")


def test_self_consistent_tree_blob_substitution_is_rejected() -> None:
    try:
        load(tree_blob_oid="4444444444444444444444444444444444444444")
    except RegistryError as e:
        assert "commit/path/blob proof mismatch" in str(e)
        return
    raise AssertionError("registry unexpectedly accepted a substituted path blob")


def test_route_drift_from_source_is_rejected() -> None:
    invalid = VALID_REGISTRY.replace('symbol = "POST_ORDER"', 'symbol = "POST_GUESSED"')
    assert_rejected(invalid, "source-bound route")


def test_status_drift_from_source_is_rejected() -> None:
    invalid = VALID_REGISTRY.replace('statuses = ["live"]', 'statuses = ["guessed"]')
    assert_rejected(invalid, "source-bound status")


def test_protocol_width_drift_from_source_is_rejected() -> None:
    invalid = VALID_REGISTRY.replace("value = 32", "value = 31")
    assert_rejected(invalid, "source-bound width")


def test_capability_evidence_drift_is_rejected() -> None:
    invalid = VALID_REGISTRY.replace(
        'evidence = "no permanent maker-controlled tombstone"',
        'evidence = "guessed permanent terminality"',
    )
    assert_rejected(invalid, "source evidence mismatch")


def test_generator_contains_no_duplicate_provider_vocabulary() -> None:
    generator = Path(__file__).with_name("generate_polymarket_semantic_boundary.py").read_text()
    for value in ('"BUY"', '"SELL"', '"TAKER"', '"MAKER"'):
        assert value not in generator


if __name__ == "__main__":
    tests = [
        test_valid_registry_renders_caller_supplied_limits,
        test_numeric_operational_default_is_rejected,
        test_cross_route_status_reuse_is_rejected,
        test_available_current_v2_capability_is_rejected,
        test_missing_required_limit_is_rejected,
        test_provider_vocabulary_drift_is_rejected,
        test_missing_source_cache_is_rejected,
        test_tampered_source_cache_is_rejected,
        test_self_consistent_blob_with_false_path_is_rejected,
        test_missing_commit_proof_is_rejected,
        test_tampered_commit_proof_is_rejected,
        test_tampered_tree_proof_is_rejected,
        test_self_consistent_tree_blob_substitution_is_rejected,
        test_route_drift_from_source_is_rejected,
        test_status_drift_from_source_is_rejected,
        test_protocol_width_drift_from_source_is_rejected,
        test_capability_evidence_drift_is_rejected,
        test_generator_contains_no_duplicate_provider_vocabulary,
    ]
    for test in tests:
        test()
    print(f"{len(tests)} generator tests passed")
