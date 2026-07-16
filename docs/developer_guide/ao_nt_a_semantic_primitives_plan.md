# AO-NT.a Semantic Primitives Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> `superpowers:subagent-driven-development` (recommended) or
> `superpowers:executing-plans` to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking. This session executes inline because
> repository-session policy does not authorize sub-agent dispatch.

**Goal:** Add bounded, fail-closed Polymarket semantic primitives at the exact
Nautilus revision pinned by bolt-v2 without adding a physical network runtime.

**Architecture:** A commit-pinned TOML evidence registry deterministically
generates route vocabularies, required branded limit fields, and unavailable V2
capability facts. Handwritten pure Rust modules consume those generated types for
checked collectors, opaque sensitive hooks, and strict route decoders; a static
source fence prevents provider effects or duplicate protocol literals outside the
generated artifact.

**Tech Stack:** Rust 2024, serde/serde_json, rust_decimal, aws-lc-rs, zeroize,
Python 3 `tomllib`, Cargo integration tests.

## Global Constraints

- Start from Nautilus commit `d636f17604cdbddc28ad40e0e15720e2d19bf860`.
- Treat `.worktrees/nt-1383-a-main` and `9610de268` as rejected evidence only.
- Operational capacities are caller-supplied branded limits with no defaults or
  fixed maximum; values above 64 must work.
- Unknown, oversized, incomplete, contradictory, or cross-route input fails
  closed without retaining raw provider data.
- Credentials, signed requests, and raw provider bytes implement no formatting
  or serialization traits.
- No reqwest, Tokio task, DNS, TLS, Hyper, WebSocket, socket, retry, durability,
  Bolt integration, or dependency-pin change enters this slice.
- Every changed requirement has focused behavior evidence, fail-closed evidence,
  generated/static evidence, or exact-head remote CI evidence.

---

## File map

- `crates/adapters/polymarket/provider-evidence/semantic-boundary.toml` — sole
  registered source identities, route/status facts, required limit categories,
  and unavailable capability reasons.
- `scripts/generate_polymarket_semantic_boundary.py` — deterministic network-free
  TOML-to-Rust generator with `--check`.
- `scripts/verify_polymarket_semantic_boundary.py` — generated-drift, duplicate
  vocabulary, forbidden trait, and provider-effect source fence with negative
  self-tests.
- `crates/adapters/polymarket/src/semantic/generated.rs` — checked-in generated
  routes, route-specific statuses, branded caller limit types, and capability
  evidence identifiers.
- `crates/adapters/polymarket/src/semantic/collector.rs` — allocation-after-check
  fixed-capacity exact and bounded collectors.
- `crates/adapters/polymarket/src/semantic/sensitive.rs` — zeroizing opaque bytes,
  credentials, and safe redacted metadata.
- `crates/adapters/polymarket/src/semantic/hooks.rs` — synchronous fail-closed
  pre-send and pre-dispatch traits.
- `crates/adapters/polymarket/src/semantic/decode.rs` — strict POST, GET exact
  order, and associated-trade decoders.
- `crates/adapters/polymarket/src/semantic/capabilities.rs` — current V2 negative
  capability fixture and unconstructable autonomous-entry success type.
- `crates/adapters/polymarket/src/semantic/mod.rs` — public semantic API and
  compile-fail observability examples.
- `crates/adapters/polymarket/src/lib.rs` — exports `semantic`.
- `crates/adapters/polymarket/tests/semantic_boundary.rs` — behavior and
  fail-closed boundary tests.
- `crates/adapters/polymarket/tests/semantic_no_allocation.rs` — isolated counting
  allocator proof.
- `crates/adapters/polymarket/tests/semantic_source_fence.rs` — invokes generator
  check and static verifier self-tests.

### Task 1: Registered evidence and deterministic generated artifact

**Files:**

- Create: `crates/adapters/polymarket/provider-evidence/semantic-boundary.toml`
- Create: `scripts/generate_polymarket_semantic_boundary.py`
- Create: `crates/adapters/polymarket/src/semantic/generated.rs`

**Interfaces:**

- Produces `SemanticRoute`, `PostOrderStatus`, `ExactOrderStatus`,
  `AssociatedTradeStatus`, `ProviderSide`, `ProviderOrderType`,
  `ProviderTraderSide`, `SemanticLimitValues`, `SemanticLimits`,
  `SemanticLimitError`, `UnavailableCapability`, and
  `CURRENT_V2_UNAVAILABLE`.
- `SemanticLimits::checked(SemanticLimitValues) -> Result<SemanticLimits,
  SemanticLimitError>` is the sole limits constructor.

- [ ] **Step 1: Write the registry before the generator**

Register the exact repositories, revisions, paths, and blob SHAs documented by
the design. Define the three routes, separate status arrays, the ten required
limit fields, protocol-fixed 32-byte hash width, and all three V2 capability
states as unavailable. The operational limit rows contain names and authority,
not numeric defaults.

- [ ] **Step 2: Write generator-failure fixtures into the generator self-test**

The generator must reject a duplicate source id, duplicate route path, duplicate
wire status, a status reused across routes, missing required limit category,
numeric operational default, non-`unavailable` current-V2 capability, or unknown
TOML key.

- [ ] **Step 3: Implement deterministic generation**

Generate types equivalent to:

```rust
pub struct SemanticLimitValues {
    pub request_body_bytes: usize,
    pub response_body_bytes: usize,
    pub response_items: usize,
    pub transaction_hashes: usize,
    pub trade_ids: usize,
    pub associated_trades: usize,
    pub string_bytes: usize,
    pub decimal_bytes: usize,
    pub log_items: usize,
}

impl SemanticLimits {
    pub const fn checked(values: SemanticLimitValues) -> Result<Self, SemanticLimitError>;
}

impl TryFrom<&str> for PostOrderStatus { /* generated exact lower-case table */ }
impl TryFrom<&str> for ExactOrderStatus { /* generated exact route table */ }
impl TryFrom<&str> for AssociatedTradeStatus { /* generated exact upper-case table */ }
```

All `SemanticLimits` fields are private `NonZeroUsize`; no `Default`, fallback,
maximum, or fixed 64 ceiling is generated. Unknown wire-value errors carry only
the safe route classification, never the rejected raw string.

- [ ] **Step 4: Generate and check the artifact**

Run:

```bash
python3 scripts/generate_polymarket_semantic_boundary.py
python3 scripts/generate_polymarket_semantic_boundary.py --check
```

Expected: both commands exit 0; the second reports that the checked-in artifact
matches the registry.

- [ ] **Step 5: Commit the provenance unit**

```bash
git add crates/adapters/polymarket/provider-evidence/semantic-boundary.toml \
  crates/adapters/polymarket/src/semantic/generated.rs \
  scripts/generate_polymarket_semantic_boundary.py
git commit -m "feat(polymarket): generate semantic boundary evidence"
```

### Task 2: Branded limits and fixed-capacity collectors

**Files:**

- Create: `crates/adapters/polymarket/src/semantic/collector.rs`
- Create: `crates/adapters/polymarket/src/semantic/mod.rs`
- Modify: `crates/adapters/polymarket/src/lib.rs`
- Create: `crates/adapters/polymarket/tests/semantic_boundary.rs`
- Create: `crates/adapters/polymarket/tests/semantic_no_allocation.rs`

**Interfaces:**

- Produces `CollectorPlan::bounded`, `CollectorPlan::exact`,
  `CollectorPlan::allocate`, `FixedCollector::try_push`, and
  `FixedCollector::finish`.
- `CollectorError` is a closed allocation-free enum: `ZeroCapacity`,
  `ItemCapacity`, `ByteCapacity`, `ArithmeticOverflow`, `Incomplete`, and
  `Contradictory`.

- [ ] **Step 1: Add failing caller-limit boundary tests**

Use a helper that constructs every required limit explicitly. Test zero rejection,
ordinary capacities at cap−1/cap/cap+1, and a transaction-hash capacity of 96
with 95/96/97 items. Assert there is no `Default` use in any example or test.

- [ ] **Step 2: Add failing collector tests**

Cover exact completion, incomplete exact completion, bounded completion below
capacity, item overflow, byte overflow, checked-add overflow, and contradictory
declared versus observed byte totals. Verify rejected items are not retained.

- [ ] **Step 3: Add the allocation-order proof**

Define one counting global allocator in the isolated integration-test binary.
Allocate all fixture inputs before enabling the counter, then assert zero
allocations for invalid `SemanticLimits`, rejected `CollectorPlan` construction,
and oversized sensitive-byte copies. Keep this binary to one test so unrelated
harness work cannot contaminate the count.

- [ ] **Step 4: Implement the minimal collector**

```rust
pub struct CollectorPlan {
    item_capacity: usize,
    byte_capacity: usize,
    exact: Option<(usize, usize)>,
}

pub struct FixedCollector<T> {
    plan: CollectorPlan,
    items: Vec<T>,
    observed_bytes: usize,
}

impl CollectorPlan {
    pub fn allocate<T>(self) -> Result<FixedCollector<T>, CollectorError> {
        let mut items = Vec::new();
        items
            .try_reserve_exact(self.item_capacity)
            .map_err(|_| CollectorError::AllocationCapacity)?;
        Ok(FixedCollector {
            items,
            plan: self,
            observed_bytes: 0,
        })
    }
}
```

Every semantic check precedes reservation or `Vec::push`; reservation failure is
a typed error. Route decoders perform a borrowed array-count preflight and reject
capacity-plus-one before reservation. `finish` returns the owned vector only
after the exact-mode invariants pass.

- [ ] **Step 5: Export and commit**

```bash
git add crates/adapters/polymarket/src/lib.rs \
  crates/adapters/polymarket/src/semantic \
  crates/adapters/polymarket/tests/semantic_boundary.rs \
  crates/adapters/polymarket/tests/semantic_no_allocation.rs
git commit -m "feat(polymarket): add fixed semantic collectors"
```

### Task 3: Non-observable values and opaque semantic hooks

**Files:**

- Create: `crates/adapters/polymarket/src/semantic/sensitive.rs`
- Create: `crates/adapters/polymarket/src/semantic/hooks.rs`
- Modify: `crates/adapters/polymarket/src/semantic/mod.rs`
- Modify: `crates/adapters/polymarket/tests/semantic_boundary.rs`
- Modify: `crates/adapters/polymarket/tests/semantic_no_allocation.rs`

**Interfaces:**

- Produces `SensitiveProviderBytes`, `SensitiveSignedRequest`,
  `SemanticCredential`, `RedactedMetadata`, `FinalizedBlockRef`, `PreSendHook`,
  `PreDispatchHook`, and `SemanticHookError`.
- Only crate-private decode closures can borrow raw bytes; public callers receive
  safe metadata containing route, validated length, and `[u8; 32]` SHA-256.

- [ ] **Step 1: Add compile-fail documentation examples**

Add examples that fail to compile when attempting `format!("{value:?}")`,
`format!("{value}")`, or `serde_json::to_vec(&value)` for each sensitive type.

- [ ] **Step 2: Add runtime sentinel and oversize tests**

Construct credential, signed-request, success, failure, malformed, and oversized
sentinels. Exercise every public metadata projection and hook input, asserting no
sentinel substring appears. Verify oversize checks return before allocation.

- [ ] **Step 3: Implement zeroizing wrappers**

```rust
pub struct SensitiveProviderBytes {
    bytes: Zeroizing<Vec<u8>>,
    metadata: RedactedMetadata,
}

pub struct SensitiveSignedRequest {
    bytes: Zeroizing<Vec<u8>>,
    metadata: RedactedMetadata,
}

pub struct SemanticCredential {
    api_key: Zeroizing<Vec<u8>>,
    secret: Zeroizing<Vec<u8>>,
    passphrase: Zeroizing<Vec<u8>>,
}
```

Do not derive or implement formatting, serde, broad byte conversion, cloning, or
string conversion. Compute SHA-256 with the existing `aws-lc-rs` dependency.

- [ ] **Step 4: Implement synchronous fail-closed hooks**

```rust
pub trait PreSendHook: Send + Sync {
    fn before_send(&self, request: &SensitiveSignedRequest)
        -> Result<(), SemanticHookError>;
}

pub trait PreDispatchHook: Send + Sync {
    fn before_dispatch(
        &self,
        request: &RedactedMetadata,
        block: FinalizedBlockRef,
    ) -> Result<(), SemanticHookError>;
}
```

The traits expose no effect implementation and return no authority token.

- [ ] **Step 5: Commit**

```bash
git add crates/adapters/polymarket/src/semantic \
  crates/adapters/polymarket/tests/semantic_boundary.rs \
  crates/adapters/polymarket/tests/semantic_no_allocation.rs
git commit -m "feat(polymarket): protect semantic boundary values"
```

### Task 4: Strict route-specific decoding

**Files:**

- Create: `crates/adapters/polymarket/src/semantic/decode.rs`
- Modify: `crates/adapters/polymarket/src/semantic/mod.rs`
- Modify: `crates/adapters/polymarket/tests/semantic_boundary.rs`

**Interfaces:**

- Produces `decode_post_order`, `decode_exact_order`,
  `decode_associated_trades`, `PostOrderObservation`, `ExactOrderObservation`,
  `AssociatedTradesObservation`, `SemanticDiagnostic`, and `DiagnosticClass`.
- Observations implement neither formatting nor serialization.

- [ ] **Step 1: Add one positive fixture for every generated wire value**

POST fixtures use only generated lower-case statuses. Exact-order fixtures use
only the generated route contract. Trade fixtures use only generated bare
uppercase statuses. Include an exact-order expected-id argument and require the
response id to match it.

- [ ] **Step 2: Add negative fixtures**

For every route test unknown status, status from another route, missing required
field, unknown extra field, malformed JSON, invalid decimal, invalid canonical
hash, duplicate hash, duplicate trade id with conflicting content, response-id
mismatch, cap+1 body, cap+1 array, cap+1 string, and cap+1 decimal. All failures
must expose only safe `SemanticDiagnostic` fields.

- [ ] **Step 3: Implement strict borrowed wire structs**

Use `#[serde(deny_unknown_fields)]` route-specific private structs. Borrow string
fields from the already bounded body where possible. Convert statuses only
through generated `TryFrom<&str>` implementations. Parse exact decimals with
`rust_decimal`; parse transaction hashes to fixed 32-byte values.

- [ ] **Step 4: Route variable-length arrays through `FixedCollector`**

Construct each collector from the relevant caller-supplied branded capacity
before retaining any element. Check each borrowed string length before copying.
Reject duplicate or contradictory records rather than truncating or overwriting.

- [ ] **Step 5: Commit**

```bash
git add crates/adapters/polymarket/src/semantic \
  crates/adapters/polymarket/tests/semantic_boundary.rs
git commit -m "feat(polymarket): decode bounded route observations"
```

### Task 5: Explicit unavailable V2 capability gates

**Files:**

- Create: `crates/adapters/polymarket/src/semantic/capabilities.rs`
- Modify: `crates/adapters/polymarket/src/semantic/mod.rs`
- Modify: `crates/adapters/polymarket/tests/semantic_boundary.rs`

**Interfaces:**

- Produces `CurrentV2Capabilities::current_v2`,
  `CurrentV2Capabilities::unavailable`, and
  `CurrentV2Capabilities::require_autonomous_entry`.
- `AutonomousEntryCapability` has private fields, no public constructor, no
  cloning, and no serde representation.

- [ ] **Step 1: Add failing capability fixture tests**

Assert permanent terminality, complete capture, and competing-work/nonce absence
are all unavailable. Test that cancellation, 404, elapsed time, unsigned expiry,
FOK text, quiet chain, sequential response hashes, status observations, and a
caller capacity above 64 cannot change the result.

- [ ] **Step 2: Implement the closed gate**

```rust
pub struct AutonomousEntryCapability {
    _private: (),
}

impl CurrentV2Capabilities {
    pub const fn require_autonomous_entry(
        &self,
    ) -> Result<AutonomousEntryCapability, CapabilityUnavailable> {
        Err(CapabilityUnavailable::current_v2())
    }
}
```

Return the complete generated unavailable set. Do not add an override or test-only
available constructor.

- [ ] **Step 3: Commit**

```bash
git add crates/adapters/polymarket/src/semantic \
  crates/adapters/polymarket/tests/semantic_boundary.rs
git commit -m "feat(polymarket): block unavailable autonomous capability"
```

### Task 6: Static source fence and exact-head evidence

**Files:**

- Create: `scripts/verify_polymarket_semantic_boundary.py`
- Create: `crates/adapters/polymarket/tests/semantic_source_fence.rs`
- Modify: `docs/developer_guide/ao_nt_a_semantic_primitives_design.md` only if
  implementation revealed a corrected fact.

**Interfaces:**

- `python3 scripts/verify_polymarket_semantic_boundary.py --check` verifies the
  real tree.
- `--self-test` injects an unregistered route, unregistered status, network
  effect, task spawn, and forbidden sensitive trait implementation and requires
  each injected case to be rejected.

- [ ] **Step 1: Implement the static verifier and its negative self-tests**

Scan `src/semantic` while treating `generated.rs` as the sole protocol-literal
source. Reject route-like literals and registered-status duplicates elsewhere.
Reject `reqwest`, `hyper`, DNS/TLS/socket/client construction, Tokio spawning,
detached work, and `Debug`/`Display`/serde implementations for sensitive types.

- [ ] **Step 2: Wire the verifier into a focused Cargo integration test**

Use `env!("CARGO_MANIFEST_DIR")` to find the repository root and run both
generator `--check` and verifier `--check --self-test`. Assert successful exit.

- [ ] **Step 3: Run permitted local checks**

```bash
python3 scripts/generate_polymarket_semantic_boundary.py --check
python3 scripts/verify_polymarket_semantic_boundary.py --check --self-test
cargo fmt --all -- --check
git diff --check
```

Expected: all exit 0. Do not run a local compile-heavy Rust command under the
bolt-v2 remote-first verification policy.

- [ ] **Step 4: Perform the completion audit**

Map every pasted-prompt requirement and every current issue amendment to a test,
static check, generated artifact, source identity, or explicit remaining-scope
disclosure. Confirm bolt-v2 still pins `d636f176...` and this fork diff contains
no Bolt files or dependency-pin changes.

- [ ] **Step 5: Commit, publish, and open the issue-bound draft PR**

```bash
git add scripts/verify_polymarket_semantic_boundary.py \
  crates/adapters/polymarket/tests/semantic_source_fence.rs
git commit -m "test(polymarket): fence semantic provider effects"
```

Push the exact branch through the credential-helper-backed URL, verify remote
HEAD equals local HEAD, and open one draft PR in `seungpyoson/nautilus_trader`.
The stable PR body references bolt-v2 #1383 without closing it, states all
remaining AO-NT.a and AO-NT.b work, and says no physical runtime is claimed.
Put exact-head SHA and verification receipts in a PR comment rather than the
stable body.

- [ ] **Step 6: Report**

Report the worktree, branch, exact head SHA, caller-supplied bounds, local/static
evidence, remote evidence state, draft PR URL, and unresolved permanent
terminality, complete-capture, and competing-work/nonce capabilities. Do not
merge, deploy, start EC2, or trade.
