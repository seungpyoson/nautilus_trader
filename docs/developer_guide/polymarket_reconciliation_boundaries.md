# Polymarket reconciliation boundary closure

## Goal

Close the remaining reconciliation and settlement gaps without adding another identity store,
fallback route, or compatibility path. The execution engine remains the generic reconciliation
boundary; the Polymarket adapter remains the sole owner of Polymarket protocol and settlement
semantics.

## Generic reconciliation identity

The execution engine and live startup manager must call one pure order-resolution function. It
resolves both client-order and venue-order indexes, rejects split ownership, and validates
instrument, side, client, and venue identity before returning a cached order.

`account_id` is deliberately not part of this identity. Reconciliation reports are authoritative
for account attribution, including position ownership, and existing engine behavior requires a
report account to differ from the account previously attached to an order. Tests must preserve
that contract while proving the live manager cannot bypass the shared resolver.

## Maker ownership and incomplete-pass reporting

Ethereum addresses are canonicalized at input boundaries. Configured signer/funder addresses and
maker addresses decoded from REST or WebSocket payloads use the same lowercase representation;
runtime ownership checks then use one shared predicate with exact address and API-key equality.
Python schemas expose the same shared ownership predicate so their behavior cannot drift from Rust.

Trade conversion returns a typed discard summary rather than a single anonymous count. It records
at least unmapped instruments and confirmed maker trades for which no owned maker order can be
identified. Every caller reports that summary at its owned severity. A successful empty report set
must therefore remain distinguishable from a pass that silently lost fill evidence.

## Crash-consistent trade-group finality

Economic fills and settlement markers remain per order fill. No new generic group event or second
settlement store is introduced.

On restoration, each fill in a Polymarket trade group is classified independently as provisional,
confirmed, or voided using its full persisted identity. A group is accepted as complete only when
every member has the same terminal finality. A partially finalized group is revalidated against the
exact venue trade. The adapter validates the complete venue fill set, emits only the missing member
transitions, and fails closed on mixed or contradictory persisted finality.

This makes a crash between member writes recoverable without treating one member as proof for its
siblings.

## Superseded-lineage parity

The rebuilt adapter is compared against every semantic fix in the superseded fork PR lineage.
Mechanisms replaced by the coordinator need not be copied, but each behavior contract must be
classified as preserved, superseded by a stronger invariant, or missing and restored. Unclassified
fixes block completion.

## Verification

- A live-manager test fails when client and venue indexes resolve different orders and passes only
  through the shared generic resolver.
- Existing report-account-authority behavior remains green.
- Rust and Python ownership tests use addresses differing only by case while the API-key arm cannot
  satisfy ownership.
- Trade conversion tests distinguish a truly empty pass from unmapped and unowned-maker losses.
- Restoration tests model crashes after only one member confirmation and one member void, requiring
  revalidation and emission of only the missing sibling transition.
- Existing focused Polymarket, execution-engine, live-manager, formatting, and lint checks remain
  green at the exact head.
