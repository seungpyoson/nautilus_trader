# Kalshi data contracts

This Rust slice provides bounded public REST discovery, metadata decoding, native instrument conversion,
GET authentication, a fixed-selection WebSocket client, the orderbook snapshot/delta wire decoder,
and subscription continuity checks for
the fork's Kalshi integration. It implements
the official AsyncAPI contract for omitted empty sides, required identities,
decimal strings, and level pairs.
It contains no market-specific selection, credential source,
execution client, or live-readiness authority.

`KalshiHttpClient` fetches single-market metadata or discovers all markets matching
optional event, series, and status filters. Its configuration requires explicit
response-byte, page, market-count, per-attempt and total elapsed-time, request-spacing,
and retry budgets.
It uses NT's shared HTTP transport, quota, and retry machinery. Cloned clients share
their quota and connection pool. The request-spacing budget permits a burst of one;
it does not account for other processes or claim a particular venue account tier.
Use the production API root `https://external-api.kalshi.com/trade-api/v2` or an
explicit demo or test root. Requests use no credentials, system proxy, or redirects.

Discovery returns a complete result or an error, with no partial catalog retained.
Repeated cursors, duplicate markets across pages, and exhausted budgets are errors.
The elapsed-time budget includes pagination, quota waits, and retry delays. Only
transient HTTP statuses and timeouts retry; malformed metadata and authorization
failures do not. Integer `Retry-After` delays are honored; an unrecognized delay
(including an HTTP-date) stops automatic retries. Dropping the operation future
cancels its pending requests and retries. The HTTP tests use local fixture servers;
they do not establish authenticated WebSocket or NT publication behavior.

`KalshiCredential` accepts a caller-injected key ID and an unencrypted RSA private
key in PKCS#1 or PKCS#8 PEM format. It parses the key once and signs GET requests
using RSA-PSS with SHA-256, MGF1-SHA256 and a 32-byte salt. The signed message is
the millisecond timestamp followed by `GET` and the encoded URL path; query
parameters are excluded. Credential debug output contains no key material.
There is no file, environment-variable, or credential-store lookup.

Its WebSocket header provider generates a complete signed header set for each
initial connection and reconnect attempt. NT invokes it after connection quota
waits and reconnect backoff; a provider error prevents that attempt and cannot
reuse earlier headers. Providers and stored headers are mutually exclusive.
Authenticated connections require TLS, with a direct literal-loopback exception for fixtures.
Configuring any proxy requires a `wss://` target, including when the target is loopback.
Authentication tests use generated keys and verify signatures locally. They do
not establish venue acceptance or authenticated data publication.
The wire contract follows the [Kalshi API key guide](https://docs.kalshi.com/getting_started/api_keys)
and [WebSocket connection guide](https://docs.kalshi.com/getting_started/quick_start_websockets).

`KalshiWebSocketClient` connects with that credential and queues its initial subscription
before returning. The caller supplies a fixed set of metadata-derived tickers and continuously
polls `next_event`. Every subscribe explicitly sets `use_yes_price: true`; NO-side prices
therefore use the YES-leg scale. A successful connection or send does not confirm the
subscription. The handler checks the venue acknowledgement's channel and command ID,
when present, before confirming NT's shared `SubscriptionState`. The schema permits an
omitted command ID; this is unambiguous because the client has one outstanding request
and never resends it on the same connection.

The bootstrap deadline includes the command quota wait, acknowledgement and every selected
market's first snapshot. Reconnects discard all sequence and UUID bindings, sign a fresh
handshake, replay subscription intent with a new command ID, and await fresh snapshots.
Frames from retired connections are discarded before parsing, even if the venue reuses a
subscription ID. Errors invalidate protocol progress and request recovery through NT's
existing controller. Consumers must reset their downstream book state after an error,
`Disconnected` event, or end of stream; every returned book message also carries its transport epoch.

The shared transport owns heartbeats, backoff and shutdown. Its terminal-state wait wakes
the consumer when reconnect attempts are exhausted. Repeated `disconnect` calls are safe,
clear all intent and queued frames, and cannot start another subscription. Dropping the
client stops its owned transport. The example uses continuous reconnect attempts with backoff
capped at 30 seconds. An explicit `reconnect_max_attempts` limit makes exhaustion terminal;
the data client clears published books, reports disconnected, and logs its client ID and selection.
Recovery after exhaustion requires an explicit data-client `connect`. Configuration requires
an initial connection deadline, a bootstrap deadline, finite heartbeat or idle detection,
and per-client connection/command spacing. The application frame limit is checked before
retention. `max_pending_frames` bounds queued frames; overflow discards the backlog and
requires a fresh connection and snapshots. Shared transport wire limits remain unchanged.
Interrupted event polls retain the original subscription send through quota and writer waits.

This client returns validated wire events. The public WebSocket tests run the same scenarios
on OS sockets and, with `--features turmoil`, NT's existing network simulator.

## Native data client

`KalshiDataClientFactory` implements NT's native factory trait and accepts an injected
`KalshiCredential`. It consumes `KalshiDataClientConfig` directly. The config selects an
explicit set of metadata tickers, transport policies, and bootstrap/shutdown deadlines.
It performs no credential lookup and creates no execution client.

Bootstrap fetches every selected definition and queues the definitions and initial snapshots.
`LiveNode` drains those inputs after `connect` returns and continues processing bounded batches
while waiting for readiness. `is_connected` remains false until the engine has applied every
selected snapshot and its cache contains the exact definitions. Construction performs no I/O.

Full `L2_MBP` subscriptions send typed `DataEvent::BookFeed` inputs to the data engine, which
applies them and publishes absolute NT deltas. A late subscriber receives a complete snapshot
of the canonical cached book without reapplying it. The fixed venue
selection remains subscribed when an NT consumer unsubscribes; only local publication stops.
Explicit instrument requests fetch current definitions from REST. A single request is restricted
to the configured selection; a request for all instruments fetches that complete selection.
Requests retain their correlation IDs, routing, and supported NT parameters:
`force_instrument_update` and `update_catalog` booleans, and `only_last: true`.
Historical bounds and other parameters are rejected before I/O.

One stream owner processes metadata requests in arrival order while continuing to process books
and local subscriptions. Each selected market has its own HTTP operation deadline, including
quota waits and retries; an N-market refresh can use up to N such budgets. Definitions are published
before their response and subsequent book events. Readiness checks the latest published definitions
in the engine cache.
Changes to the grid, price or quantity precision, contract increment, or exchange index
clear every published book and require fresh WebSocket snapshots. Changes to descriptive metadata
or opening, closing and receipt timestamps preserve current liquidity while publishing the new
definition.

A failed or timed-out refresh is logged and leaves the last validated definitions and books intact.
The failed request produces no success response; later requests and book frames continue.
Definitions are replaced only after the complete requested selection validates. Stop and drop cancel
pending HTTP work and discard queued requests; late responses cannot publish. The consumer subscription
intent survives successful refresh and stream recovery. Changing the selection requires recreating
the client. Historical data, quotes, trades, depth-limited books, and Python projection are unsupported.

Only the engine cache retains mutable book levels. The adapter retains interpretation rules,
protocol progress and subscription intent. Signed quantity conversion, integrity checks and
publication run together on the engine thread. Its book remains current without a local
subscriber. Retired connection generations cannot apply queued frames or clear replacement
books. A client-wide input budget spans reconnects, with bounded bootstrap room for the fixed
selection; overflow invalidates the generation and requires new snapshots. This also applies
to local subscribe/unsubscribe publication, preserving the final subscription intent through
recovery. Closing the engine receiver or stopping the client remains terminal.
YES is the bid side and NO is the
ask side on the explicitly requested YES price scale. Every price must satisfy the full
metadata grid, and quantities must remain exact at the instrument's contract increment.
Additive changes produce absolute NT quantities; zero deletes a level. Snapshot levels with
zero liquidity are omitted. Duplicate levels, negative results, overflow, off-grid values,
and crossed books invalidate the selection before further publication.

Snapshots begin with `Clear`, carry `F_SNAPSHOT`, and end with `F_LAST`, including empty
snapshots. Source milliseconds become exact nanoseconds. Where the venue provides no timestamp,
the shared real-time clock's receipt time supplies both event and initialization time.
Recovery, terminal closure, stop, and task drop clear published liquidity and require fresh
snapshots. A local invalidation clear carries `F_LAST` without claiming a venue snapshot.
The shared transport owns reconnects. One adapter task supplies ordered inputs; the engine owns
book application and publication. Engine application failures invalidate the fixed selection
and signal the adapter to recover. Native lifecycle tests exercise both transport backends
and the real LiveNode. The shared ownership changes still require independent review.

These boundaries do not establish authenticated venue acceptance or live readiness. Factory
lifecycle tests exercise ordinary OS sockets and NT event/cache boundaries; they do not run
inside Turmoil because the native client uses NT's global runtime.
The `node` test target drives both transport backends sequentially through a real `LiveNode`,
its data engine and a registered actor. It checks bootstrap definitions in the engine cache,
managed book updates, metadata-driven invalidation and fresh snapshots, and node shutdown
while an HTTP metadata response is withheld. It uses local fixture servers and generated keys.

### Bounded venue reader

The `kalshi-data-reader` example drives the native factory through `LiveNode` with one data actor.
Copy `examples/data_reader.json`, select currently open tickers, and set the REST and WebSocket
endpoints for the same environment. Its `client` object is the existing `KalshiDataClientConfig`;
`observation_seconds` selects a period from 1 through 300 seconds after actor startup.

Run `cargo run --locked --offline -p nautilus-kalshi --example kalshi-data-reader -- inspect CONFIG.json`
to fetch public metadata and validate active status, dates and exact native instrument conversion.
This command does not read credentials. For an authenticated observation, set `KALSHI_KEY_ID` and
`KALSHI_PRIVATE_KEY_FILE` locally and replace `inspect` with `read`. The file must contain the
unencrypted RSA PEM for that environment; the example injects it into the factory and zeroizes
the input buffer. It does not load dotenv files or access a credential store.

The reader registers no execution client, loads or saves no node state, and requests only full
L2 book deltas. It reports snapshot, update and invalidation counts from events received through
the real data engine, using the engine-managed book. Success requires a complete observation
period, at least one snapshot and subsequent update per market, and completed node shutdown.
A quiet market without an update yields an incomplete observation. The report is a bounded
sample, not a claim of continuous feed availability or trading readiness.

`decode_markets_response` and `decode_market_response` enforce caller-supplied
response bounds, required metadata, exact decimals, explicit exchange routing,
and valid lifecycle timestamps. The single-market decoder binds the response to
the requested ticker. The complete original market JSON is retained, including
settlement rules and fields outside the definition projection.

`parse_instrument` creates a native `BinaryOption` for binary, one-dollar
contracts. The ticker becomes the symbol on `KALSHI`; `exchange_index` is retained
in `info`, independently of ticker spelling. Activation and trading expiration
use source open and close times. Latest expiration and settlement details remain
in the original JSON under `info.kalshi_market_json`, stored as a string to avoid
rounding unknown numeric fields. Fee and margin defaults are not venue economics;
this definition is for the data slice and does not enable execution or settlement.

Each instrument owns a shared NT `PriceGrid`. Its inclusive `(first, last, step)`
ranges survive JSON, Python-dictionary, and Arrow serialization. The adapter
requires contiguous source bands with aligned boundaries and represents each
shared boundary once. It currently supports prices strictly between zero and one;
the captured schema does not establish outer endpoint eligibility. Unsupported
products, unknown lifecycle states, and unrepresentable grids fail explicitly.
Use `PriceGrid::price_from_decimal` when ingesting prices: it rejects off-grid and
inexact values instead of rounding. The structure label never selects behavior.
The [public metadata fixture](test_data/README.md) records the capture and OpenAPI
provenance used by the conversion tests.

`decode_orderbook_message` performs stateless book-message decoding. Missing snapshot
sides become empty vectors because the protocol defines their absence as an
empty side. Explicit nulls, malformed levels, missing identities, and decimal
values that cannot be represented exactly are errors. Optional source timestamps
remain optional; the decoder does not invent or substitute event time.
Unknown fields are rejected so an obsolete side representation cannot silently
decode as an empty book. Protocol changes require updating this contract.
Subscription IDs and sequence numbers use nonzero integers, enforcing the
schema's minimum value of one at the decoding boundary.

The caller must bound frame size before stateless decoding.

`KalshiOrderbookStream` accepts raw frames with a configured byte limit for one
fixed subscription in one transport connection. The caller supplies a confirmed
subscription ID and selected tickers derived from market metadata. REST metadata
does not provide market UUIDs; each market's first snapshot pins its UUID. UUIDs
must remain stable and cannot be shared by two selected tickers.
The stream checks those bindings and tracks one sequence across all selected
markets, including sequenced `ok` and `unsubscribed` responses. Each market needs
its own snapshot before its deltas can pass. No initial sequence of one is assumed.
Membership lists in `ok` responses must match the complete selection without
duplicates; reduced responses can omit those lists. This boundary requires both
`sid` and `seq` on subscription `ok` responses. Connection-level responses,
including subscription-list results, belong in the transport handler.

A gap, duplicate, reordered or malformed frame, mismatched identity, changed
membership, oversized frame, or venue error invalidates the entire subscription.
Unknown error codes also invalidate it. `invalidate()` handles disconnect or
downstream processing failure, and `stop()` handles shutdown. These states are
terminal: a later snapshot cannot revive the instance. Recovery requires a new
transport subscription and fresh snapshots. The handler must keep frames from
old connections away from replacement instances, even if the venue reuses IDs.

This layer stores only sequence and snapshot progress, never book levels. Its
`Streaming` state establishes protocol continuity across the selection, not book
validity or live readiness. The native data client enforces price grids and publishes NT events. The WebSocket client selects the price
convention explicitly; standalone decoder callers must also bind interpretation to their
subscription's `use_yes_price` setting.

Contract source: [Kalshi AsyncAPI](https://docs.kalshi.com/asyncapi.yaml), retrieved
September 5, 2026; SHA-256
`e6d163a464bd4ef35657c101142f79c290047657036e5de7870b26e359a6bc2c`.
The schema declares both snapshot side arrays optional and each present level
an array of exactly two strings. This also matches the omitted YES side observed
in an authenticated operator probe; the fixtures here are schema examples,
not a retained live-session capture.
The stream tests replay schema-shaped frames and local lifecycle calls; they do
not establish authenticated transport, reconnect, or NT publication behavior.

Related scope: [Bolt integration issue #1765](https://github.com/seungpyoson/bolt-v2/issues/1765).
Development and review stay in `seungpyoson/nautilus_trader`. Upstream submission
requires unanimous approval of the exact fork head by the agreed reviewers.
