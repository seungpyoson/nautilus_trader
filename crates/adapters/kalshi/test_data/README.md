# Kalshi public metadata fixture

`markets.json` is an unmodified public response captured on September 5, 2026 UTC
from [Get Markets](https://external-api.kalshi.com/trade-api/v2/markets?status=open&limit=2).
The request used no credentials. It sampled two open markets without a ticker,
asset, category or exchange-index filter; it is not a complete market census.

SHA-256: `8a0a878aaed54fe43fcae94d50bfb436940b4a8b5aec03986a1316042543b1f5`.

Both captured markets provide explicit exchange routing and three price bands.
The tests use their metadata, never ticker spelling or the grid label, to select
the resulting fields. Mutated test cases are synthetic negative or precision
cases derived from this public response.

The REST schema is [Kalshi OpenAPI](https://docs.kalshi.com/openapi.yaml), retrieved
September 5, 2026 UTC; SHA-256
`1aea311542d8587e386deb40f8384b0d8a6d996845a98ffa8119f1df0c3c234f`.
The schema's `FixedPointCount` defines 0.01-contract granularity. The
[fixed-point reference](https://docs.kalshi.com/getting_started/fixed_point_migration)
identifies `price_ranges` as the source of truth and the structure name as a label.

These fixtures establish public metadata parsing only. Authenticated WebSocket
captures and the actual NT entrypoint remain separate integration evidence.

## WebSocket protocol examples

`ws_subscribed.json`, `ws_orderbook_snapshot.json`, and `ws_orderbook_delta.json`
are the first examples from `subscribedResponse`, `orderbookSnapshot`, and
`orderbookDelta` in the pinned [Kalshi AsyncAPI](https://docs.kalshi.com/asyncapi.yaml).
The schema was retrieved September 5, 2026; SHA-256
`e6d163a464bd4ef35657c101142f79c290047657036e5de7870b26e359a6bc2c`.
Payloads were extracted without changing field values. Lifecycle scenarios explicitly
mutate IDs, tickers and sequences or omit frames; authentication tests generate synthetic
keys in memory. These examples are not authenticated venue captures and do not prove
native book application or publication by themselves. Native book and factory tests correlate
the metadata ticker with the documented stream ticker and convert the example's NO prices
to YES-leg prices (`1 - no_price`) to match the explicit `use_yes_price: true` subscription.
Those derived scenarios also mutate quantities and sequences to test native state transitions;
they remain local fixture evidence rather than authenticated venue captures.
