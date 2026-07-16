# AO-NT.a Semantic Primitives Design

## Scope and authority

This design implements the narrow AO-NT.a semantic-primitives slice of bolt-v2
issue #1383 in the `seungpyoson/nautilus_trader` fork. The branch starts at the
exact Nautilus revision currently pinned by bolt-v2:
`d636f17604cdbddc28ad40e0e15720e2d19bf860`.

The design follows the Claude-approved autonomous-operation architecture at
bolt-v2 commit `fe368f851`, as amended by the current issue #1383
runtime-policy authority acceptance. That later acceptance supersedes the
architecture's retired fixed operational ceilings: capacities are caller-supplied
branded limits, have no defaults, and must support values above 64. The rejected worktree
`.worktrees/nt-1383-a-main` and rejected commit `9610de268` are not inputs.

This slice provides pure, bounded semantic primitives only. It does not provide
a physical network runtime and does not change bolt-v2's dependency pin.

## Deliverables

The Polymarket Rust adapter gains five focused surfaces:

1. Fixed-capacity request and response collectors whose item and byte limits are
   validated before storage allocation.
2. Fail-closed pre-send and pre-dispatch semantic hook interfaces with opaque
   sensitive inputs and no transport implementation.
3. Strict, route-specific protocol decoders generated from a registered,
   commit-pinned evidence manifest.
4. Non-observable wrappers for credentials, signed request bytes, and raw
   provider bytes.
5. Explicit capability gates proving that the reviewed Polymarket V2 contract
   cannot authorize autonomous entry.

The diff remains confined to these primitives, generated provenance, generator
and static-verifier tooling, and focused tests.

## Registered evidence and generation

`crates/adapters/polymarket/provider-evidence/semantic-boundary.toml` is the
single semantic registry. Each source row records repository, commit, path, Git
blob SHA, authority class, and the exact protocol facts used by this slice.

The provider facts are registered from these reviewed sources:

- `Polymarket/clob-client-v2` at
  `ff5913f83132a141e01d403e505b6ccc003aa0f7`, including
  `src/endpoints.ts`, `src/types/clob.ts`, and `src/order-utils/model/side.ts`;
- the accepted bolt-v2 architecture at `fe368f851`, for operational-limit and
  capability authority.

Each source row points to a checked-in base64 encoding of the complete Git blob.
Checked-in commit and tree objects cryptographically bind each declared commit,
path, and blob. The repository name is a locator because Git object identity is
repository-independent. Generation recomputes every Git object SHA-1 before
following the commit-to-path chain. Routes and vocabulary are derived from exact
TypeScript declarations without a second handwritten value list.

Required operational-limit categories are registered separately as issue
authority from bolt-v2 issue #1383. Provider sources prove vocabulary and
schema; they do not invent operational safety capacities. The registry emits
the complete set of required branded limit fields, not numeric runtime values.

The registry defines exactly three supported semantic routes:

- POST order insertion at `/order`;
- GET exact order at `/data/order/{order_id}`;
- GET associated trades at `/data/trades` with an exact order selector.

It defines separate wire vocabularies for each route:

- POST status: `live`, `matched`, `delayed`, `unmatched`;
- GET exact-order status: `ORDER_STATUS_LIVE`, `ORDER_STATUS_INVALID`,
  `ORDER_STATUS_CANCELED_MARKET_RESOLVED`, `ORDER_STATUS_CANCELED`,
  `ORDER_STATUS_MATCHED`;
- associated-trade status: `MATCHED`, `MINED`, `CONFIRMED`, `RETRYING`,
  `FAILED`.

No shared catch-all provider enum is used by the semantic boundary. A status
accepted for one route is rejected on every other route.

The registered TypeScript schema also generates the closed `BUY`/`SELL`,
`GTC`/`FOK`/`GTD`/`FAK`, and `TAKER`/`MAKER` vocabularies. Strict decoders do
not accept handwritten or open-ended substitutes for those values.

The registry also owns the applicable body, item, string, decimal, transaction
hash, trade-id, and log limit categories. Every numeric operational capacity is
supplied by the caller through `SemanticLimits::checked`; there is no `Default`
implementation, fallback constructor, or retired fixed ceiling. Protocol-fixed
widths such as the 32-byte canonical transaction-hash width remain generated
schema facts rather than operational capacities.

`scripts/generate_polymarket_semantic_boundary.py` reads the registry and emits
`src/semantic/generated.rs`. The generated file is checked in so reviewers can
inspect vocabulary and bound changes directly. The generator is deterministic,
network-free, validates every cached Git blob, and supports a check mode that
fails on drift.

`scripts/verify_polymarket_semantic_boundary.py` performs the static source
fence. It verifies that:

- every generated route, status, limit, and capability reason comes from the
  registry;
- the registered source identities are unique and complete;
- semantic code does not duplicate any registered provider route or vocabulary
  literal;
- the public semantic surface contains no unsafe/FFI capability, effect-capable
  callback trait, function-pointer type, unapproved qualified root, output
  macro, or task/thread spawn, and every complete import statement is
  exact-allowlisted;
- sensitive wrappers do not gain formatting or serialization implementations;
- generated output exactly matches a fresh generator run.

The verifier tokenizes Rust deterministically, ignoring comments and literal
contents when classifying structural capabilities. It uses the CI-supplied merge
base when available, requires it to equal the declared base, and confines that
diff to the semantic subtree and exact module export. Local runs without a
trusted base are diagnostic only. The verifier checks exact registered literals;
it does not guess semantic meaning from spelling, case, prefixes, or regex.

This is a structural guard, not a proof of arbitrary Rust call-graph purity.
Review and compiler evidence remain required. The sensitive provider wrapper is
tighter: its private field layout, complete method set, and each raw-field access
are exact-allowlisted to the three route-specific decoder calls.

The static verifier never treats a current network fetch as build authority.
Updating a source revision requires an explicit registry and generated-artifact
diff.

## Fixed-capacity collectors

The collector API separates a checked plan from allocated storage.

`SemanticLimits::checked` validates a complete caller-supplied set of nonzero
operational capacities and produces the only branded limits value accepted by
the semantic boundary. `CollectorPlan::checked(expected_items, expected_bytes,
limits)` validates both declared dimensions using allocation-free arithmetic and
static error variants.
Only a valid plan can construct `FixedCollector<T>`. Storage reservation uses
`try_reserve_exact`; an unrepresentable or unavailable allocation returns the
typed `AllocationCapacity` error rather than panicking. The plan fields are
private, preventing unchecked construction.

Each pushed item supplies its exact encoded byte length. Before insertion, the
collector checks:

- another item fits;
- the byte addition does not overflow;
- the new byte total does not exceed the planned total;
- the new byte total does not exceed the caller-supplied branded route limit.

`finish()` succeeds only when observed item and byte totals exactly match the
checked plan. Too few items or bytes is incomplete; too many is overflow; a
mismatched exact total is contradictory. All failures return closed error enums
without allocating diagnostic strings or retaining rejected raw bytes.

For a raw response body, `SensitiveProviderBytes::checked` compares the source
slice length with the caller-supplied route limit before allocating or copying.
Its only decoding operations are route-specific methods owned by the sensitive
module; there is no generic raw-byte callback or projection. For a prepared
request, the same rule applies to signed request bytes. A failed size check never
constructs the sensitive wrapper.

Array decoders first count borrowed raw elements without converting or storing
them. They reject capacity-plus-one before collector reservation, then reserve
only the observed in-limit count. Element validation checks collector capacity
before conversion so rejected elements cannot trigger conversion allocations.

Boundary tests exercise capacity minus one, capacity, and capacity plus one for
both item and byte dimensions, and separately use a transaction-hash capacity
above 64 to prove that no retired ceiling remains. A dedicated integration-test counting allocator
proves rejected collector plans and oversized byte copies perform zero
allocations before returning. It also covers capacity-plus-one arrays and an
unrepresentable reservation request.

## Sensitive values

`SensitiveProviderBytes`, `SensitiveSignedRequest`, and `SemanticCredential`
own their values in zeroizing storage. Their fields are private and their public
interfaces expose only deliberately bounded operations needed by the semantic
hooks. They do not implement `Debug`, `Display`, `Serialize`, `Deserialize`,
`AsRef<[u8]>`, or broad conversion traits.

The only safe projection is `RedactedMetadata`, containing generated route id,
validated length, and SHA-256 digest. It contains no raw bytes, signature,
authorization header, API key, secret, passphrase, or provider response text.

Compile-fail doctests demonstrate that formatting and serde serialization are
unavailable. Runtime sentinel tests pass representative secrets, signed request
bytes, success responses, failure responses, malformed responses, and oversized
responses through every provided metadata surface and assert that no sentinel
substring is observable.

Existing adapter credential types remain outside this new semantic API. This
slice does not claim that the entire existing adapter is already migrated to the
new wrappers; that integration belongs to the remaining AO-NT.a work.

## Semantic hooks

`PreSendHook` receives an opaque borrow of a validated
`SensitiveSignedRequest` plus safe redacted metadata. It returns `Result<(),
SemanticHookError>`. The signed bytes are inaccessible to formatting and
serialization sinks.

`PreDispatchHook` receives a validated finalized block reference and the same
request digest identity. The block number and block hash are checked semantic
values, not proof that any provider call occurred.

Both hooks are synchronous, pure interfaces. A hook error is fail-closed. The
interfaces provide no reqwest, Tokio, DNS, TLS, Hyper, socket, retry, task, or
durability behavior. They do not return send authority, replay authority,
capacity-release authority, or a serializable permit.

The ordering contract is explicit: a future caller prepares a bounded signed
request, invokes the pre-send hook, obtains and validates the finalized block,
then invokes the pre-dispatch hook immediately before its own provider effect.
This slice only makes those semantic interception points possible.

## Route-specific decoding

Each decoder accepts a `SensitiveProviderBytes` value that has already passed
the caller-supplied branded route body limit. Deserialization uses strict route-specific wire
structures with unknown fields denied.

POST insertion accepts only `success=true`, an absent or empty `errorMsg`, all
required bounded fields, canonical transaction hashes, and its generated
lower-case status vocabulary. A transport failure, non-2xx response,
`success=false`, non-empty error, malformed envelope, unknown field, unknown
status, cross-route status, duplicate hash, oversized collection, or invalid
canonical hash returns a bounded nonterminal diagnostic class.

GET exact order accepts only its generated prefixed statuses and exact required
schema. Associated trades accept only their generated bare uppercase statuses.
Missing fields, extra fields, malformed decimals, invalid identifiers,
incomplete collections, contradictory duplicate records, or any unknown value
fail closed.

Associated trades must reference the requested order as either the taker id or a
maker-order id. The pinned sources define the `TAKER`/`MAKER` vocabulary but do
not establish a complete correlation contract, so this slice does not invent
one. Likewise, decimals are checked for syntax and caller-supplied byte bounds;
sign, price ceilings, matched-size relations, and tick policy remain unenforced
until authoritative field-level semantics are registered.

No decoder infers terminality from a status. Decoded observations are semantic
facts only.

## Capability gates

The generated capability fixture represents three independent requirements:

- permanent terminality through an indefinite, linearizable exact-hash
  tombstone covering delayed, retried, duplicated, and preapproved work;
- complete capture of every transaction hash and associated trade needed for
  fill verification within the caller-supplied operational capacities;
- proof that no competing provider work or nonce can later create an effect.

At the reviewed V2 revisions all three are `Unavailable`, with generated reason
identifiers tied to registered evidence. Existing statuses, cancellation, 404,
elapsed time, unsigned expiry, FOK wording, quiet chain, response hashing, or a
sequential multi-service snapshot cannot satisfy any capability.

Increasing a caller-supplied capacity, including above 64, does not manufacture
a provider completeness guarantee. Capacity and provider capability remain
separate types and gates.

`AutonomousEntryCapabilities::current_v2()` therefore cannot construct an
enabled gate. `require_autonomous_entry()` returns the complete unavailable set
and has no override, fallback, or compatibility path. This slice exposes no
public constructor for an available capability. A later provider revision must
update registered evidence and generated artifacts under a separate review.

## Error handling

All errors are closed enums with static classifications. Errors never retain raw
provider bytes or credentials. Unknown, oversized, incomplete, or contradictory
input is rejected. Integer overflow is treated as capacity overflow. Capability
absence is an ordinary fail-closed result, not a panic and not a warning.

## Verification map

| Requirement | Evidence |
|---|---|
| Item and byte bounds | Unit tests at caller limit minus one, limit, and limit plus one for request and response collectors, including a transaction‑hash limit above 64 |
| Check before allocation | Counting‑allocator integration tests on invalid plans, oversized byte copies, capacity‑plus‑one arrays, and unrepresentable reservations |
| Strict provider decoding | Route tests for every generated value plus unknown, malformed, extra‑field, cross‑route, oversized, incomplete, and contradictory fixtures |
| Non‑observable sensitive values | Compile‑fail doctests, forbidden‑trait static checks, zeroization tests, and sentinel projection tests |
| Generated provenance | Cached commit/tree/blob object‑chain verification, source‑drift tests, and registry‑to‑generated equality |
| Structural capability guard | Deterministic Rust‑token checks, exact sensitive access allowlists, exact registered‑literal checks, and trusted‑base diff confinement |
| V2 capability absence | Generated negative fixtures for all three unavailable contracts and autonomous‑entry rejection tests |
| No physical runtime | Static rejection of network/task/runtime imports and direct inspection of the confined diff |

Formatting, generator check mode, the static verifier, focused Rust tests, and
the fork's remote CI provide exact-head evidence. Bolt's dependency pin remains
unchanged and is reported as external inspection evidence from bolt-v2.

## Explicit non-goals and remaining work

This PR does not implement concrete HTTP, reqwest, Tokio, DNS, TLS, Hyper,
WebSocket, socket, retry, task, cache-lifecycle, subscription-generation,
transport-tracking, current-account capture, exact balance/position, safe purge,
or durable recovery behavior. It does not add dummy ballast, detached cleanup,
accounting-only sockets, or sequential-response snapshots.

The remaining AO-NT.a work includes the broader bounded runtime primitives from
issue #1383 that are intentionally excluded from this semantic slice. AO-NT.b
still owns the bolt-v2 pin update, TOML-supplied runtime limits, disabled Bolt
adapters, registered physical provider effects, and integration source fences.
Neither slice closes bolt-v2 issue #1383 by itself.
