// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
// -------------------------------------------------------------------------------------------------

use std::str::FromStr;

use rust_decimal::Decimal;
use serde::{
    Deserialize, Deserializer,
    de::{DeserializeSeed, Error as _, SeqAccess, Visitor},
};
use serde_json::value::RawValue;

use super::{
    AssociatedTradeStatus, CollectorError, CollectorKind, CollectorPlan, ExactOrderStatus,
    FixedCollector, PostOrderStatus, ProviderOrderType, ProviderSide, ProviderTraderSide,
    SemanticLimits, SemanticRoute, SensitiveProviderBytes, TRANSACTION_HASH_BYTES,
};

/// Safe classification for rejected provider input.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticClass {
    WrongRoute,
    Malformed,
    UnknownStatus,
    Oversized,
    Incomplete,
    Contradictory,
    InvalidDecimal,
    InvalidHash,
    ProviderRejected,
}

/// A bounded diagnostic which never retains provider bytes or values.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SemanticDiagnostic {
    route: SemanticRoute,
    class: DiagnosticClass,
}

impl SemanticDiagnostic {
    const fn new(route: SemanticRoute, class: DiagnosticClass) -> Self {
        Self { route, class }
    }

    #[must_use]
    pub const fn route(self) -> SemanticRoute {
        self.route
    }

    #[must_use]
    pub const fn class(self) -> DiagnosticClass {
        self.class
    }
}

/// A strict semantic observation from order insertion.
#[allow(missing_debug_implementations)]
pub struct PostOrderObservation {
    order_id: String,
    status: PostOrderStatus,
    taking_amount: Decimal,
    making_amount: Decimal,
    transaction_hashes: Vec<[u8; TRANSACTION_HASH_BYTES]>,
    trade_ids: Vec<String>,
}

impl PostOrderObservation {
    #[must_use]
    pub const fn status(&self) -> PostOrderStatus {
        self.status
    }

    #[must_use]
    pub fn order_id(&self) -> &str {
        &self.order_id
    }

    #[must_use]
    pub const fn taking_amount(&self) -> Decimal {
        self.taking_amount
    }

    #[must_use]
    pub const fn making_amount(&self) -> Decimal {
        self.making_amount
    }

    #[must_use]
    pub fn transaction_hashes(&self) -> &[[u8; TRANSACTION_HASH_BYTES]] {
        &self.transaction_hashes
    }

    #[must_use]
    pub fn trade_ids(&self) -> &[String] {
        &self.trade_ids
    }
}

/// A strict semantic observation from exact-order lookup.
#[allow(missing_debug_implementations)]
pub struct ExactOrderObservation {
    order_id: String,
    status: ExactOrderStatus,
    original_size: Decimal,
    size_matched: Decimal,
    price: Decimal,
    associated_trade_ids: Vec<String>,
}

impl ExactOrderObservation {
    #[must_use]
    pub const fn status(&self) -> ExactOrderStatus {
        self.status
    }

    #[must_use]
    pub fn order_id(&self) -> &str {
        &self.order_id
    }

    #[must_use]
    pub const fn original_size(&self) -> Decimal {
        self.original_size
    }

    #[must_use]
    pub const fn size_matched(&self) -> Decimal {
        self.size_matched
    }

    #[must_use]
    pub const fn price(&self) -> Decimal {
        self.price
    }

    #[must_use]
    pub fn associated_trade_ids(&self) -> &[String] {
        &self.associated_trade_ids
    }
}

/// One strict associated-trade observation.
#[allow(missing_debug_implementations)]
pub struct AssociatedTradeObservation {
    id: String,
    status: AssociatedTradeStatus,
    size: Decimal,
    price: Decimal,
    transaction_hash: Option<[u8; TRANSACTION_HASH_BYTES]>,
}

impl AssociatedTradeObservation {
    #[must_use]
    pub fn id(&self) -> &str {
        &self.id
    }

    #[must_use]
    pub const fn status(&self) -> AssociatedTradeStatus {
        self.status
    }

    #[must_use]
    pub const fn size(&self) -> Decimal {
        self.size
    }

    #[must_use]
    pub const fn price(&self) -> Decimal {
        self.price
    }

    #[must_use]
    pub const fn transaction_hash(&self) -> Option<[u8; TRANSACTION_HASH_BYTES]> {
        self.transaction_hash
    }
}

/// Strict observations from the associated-trades route.
#[allow(missing_debug_implementations)]
pub struct AssociatedTradesObservation {
    trades: Vec<AssociatedTradeObservation>,
}

impl AssociatedTradesObservation {
    #[must_use]
    pub fn trades(&self) -> &[AssociatedTradeObservation] {
        &self.trades
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WirePostOrder<'a> {
    success: bool,
    #[serde(rename = "errorMsg")]
    error_msg: Option<&'a str>,
    #[serde(rename = "orderID")]
    order_id: &'a str,
    #[serde(default, rename = "transactionsHashes", borrow)]
    transaction_hashes: Option<&'a RawValue>,
    #[serde(default, rename = "tradeIDs", borrow)]
    trade_ids: Option<&'a RawValue>,
    status: &'a str,
    #[serde(rename = "takingAmount")]
    taking_amount: &'a str,
    #[serde(rename = "makingAmount")]
    making_amount: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireExactOrder<'a> {
    #[serde(borrow)]
    associate_trades: &'a RawValue,
    id: &'a str,
    status: &'a str,
    owner: &'a str,
    maker_address: &'a str,
    market: &'a str,
    asset_id: &'a str,
    side: &'a str,
    original_size: &'a str,
    size_matched: &'a str,
    price: &'a str,
    outcome: &'a str,
    created_at: u64,
    expiration: &'a str,
    order_type: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireTrade<'a> {
    id: &'a str,
    taker_order_id: &'a str,
    market: &'a str,
    asset_id: &'a str,
    side: &'a str,
    size: &'a str,
    fee_rate_bps: &'a str,
    price: &'a str,
    status: &'a str,
    match_time: &'a str,
    #[serde(default)]
    match_time_nano: Option<&'a str>,
    last_update: &'a str,
    outcome: &'a str,
    bucket_index: u64,
    owner: &'a str,
    maker_address: &'a str,
    #[serde(borrow)]
    maker_orders: &'a RawValue,
    #[serde(default)]
    transaction_hash: Option<&'a str>,
    #[serde(default)]
    err_msg: Option<&'a str>,
    trader_side: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireMakerOrder<'a> {
    order_id: &'a str,
    owner: &'a str,
    maker_address: &'a str,
    matched_amount: &'a str,
    price: &'a str,
    fee_rate_bps: &'a str,
    asset_id: &'a str,
    outcome: &'a str,
    #[serde(default)]
    side: Option<&'a str>,
    #[serde(default)]
    builder_fee: Option<&'a str>,
    #[serde(default)]
    builder_code: Option<&'a str>,
}

pub fn decode_post_order(
    bytes: SensitiveProviderBytes,
) -> Result<PostOrderObservation, SemanticDiagnostic> {
    let route = SemanticRoute::PostOrder;
    if bytes.metadata().route() != route {
        return Err(SemanticDiagnostic::new(route, DiagnosticClass::WrongRoute));
    }
    let limits = bytes.limits();
    bytes.decode_with(|raw| decode_post_order_borrowed(raw, route, limits))
}

fn decode_post_order_borrowed(
    raw: &[u8],
    route: SemanticRoute,
    limits: SemanticLimits,
) -> Result<PostOrderObservation, SemanticDiagnostic> {
    let wire: WirePostOrder = serde_json::from_slice(raw)
        .map_err(|_| SemanticDiagnostic::new(route, DiagnosticClass::Malformed))?;
    decode_post_wire(wire, limits).map_err(|class| SemanticDiagnostic::new(route, class))
}

pub fn decode_exact_order(
    bytes: SensitiveProviderBytes,
    expected_id: &str,
) -> Result<ExactOrderObservation, SemanticDiagnostic> {
    let route = SemanticRoute::GetExactOrder;
    if bytes.metadata().route() != route {
        return Err(SemanticDiagnostic::new(route, DiagnosticClass::WrongRoute));
    }
    let limits = bytes.limits();
    bytes.decode_with(|raw| {
        let wire: WireExactOrder = serde_json::from_slice(raw)
            .map_err(|_| SemanticDiagnostic::new(route, DiagnosticClass::Malformed))?;
        decode_exact_wire(wire, expected_id, limits)
            .map_err(|class| SemanticDiagnostic::new(route, class))
    })
}

pub fn decode_associated_trades(
    bytes: SensitiveProviderBytes,
    expected_order_id: &str,
) -> Result<AssociatedTradesObservation, SemanticDiagnostic> {
    let route = SemanticRoute::GetAssociatedTrades;
    if bytes.metadata().route() != route {
        return Err(SemanticDiagnostic::new(route, DiagnosticClass::WrongRoute));
    }
    let limits = bytes.limits();
    bytes.decode_with(|raw| {
        collect_trades(raw, expected_order_id, limits)
            .map(|trades| AssociatedTradesObservation { trades })
            .map_err(|class| SemanticDiagnostic::new(route, class))
    })
}

fn decode_post_wire(
    wire: WirePostOrder<'_>,
    limits: SemanticLimits,
) -> Result<PostOrderObservation, DiagnosticClass> {
    checked_string(wire.order_id, limits)?;
    checked_optional_string(wire.error_msg, limits)?;
    if !wire.success || wire.error_msg.is_some_and(|message| !message.is_empty()) {
        return Err(DiagnosticClass::ProviderRejected);
    }
    let status =
        PostOrderStatus::try_from(wire.status).map_err(|_| DiagnosticClass::UnknownStatus)?;
    let taking_amount = checked_decimal(wire.taking_amount, limits)?;
    let making_amount = checked_decimal(wire.making_amount, limits)?;
    let transaction_hashes = match wire.transaction_hashes {
        Some(raw) => collect_strings(raw, CollectorKind::TransactionHashes, limits, |value| {
            parse_hash(value, limits)
        })?,
        None => Vec::new(),
    };

    if has_duplicates(&transaction_hashes) {
        return Err(DiagnosticClass::Contradictory);
    }
    let trade_ids = match wire.trade_ids {
        Some(raw) => collect_strings(raw, CollectorKind::TradeIds, limits, |value| {
            checked_string(value, limits)?;
            Ok(value.to_owned())
        })?,
        None => Vec::new(),
    };

    if has_duplicates(&trade_ids) {
        return Err(DiagnosticClass::Contradictory);
    }
    Ok(PostOrderObservation {
        order_id: wire.order_id.to_owned(),
        status,
        taking_amount,
        making_amount,
        transaction_hashes,
        trade_ids,
    })
}

fn decode_exact_wire(
    wire: WireExactOrder<'_>,
    expected_id: &str,
    limits: SemanticLimits,
) -> Result<ExactOrderObservation, DiagnosticClass> {
    checked_string(expected_id, limits)?;

    for value in [
        wire.id,
        wire.owner,
        wire.maker_address,
        wire.market,
        wire.asset_id,
        wire.side,
        wire.outcome,
        wire.expiration,
        wire.order_type,
    ] {
        checked_string(value, limits)?;
    }
    let _ = wire.created_at;
    ProviderSide::try_from(wire.side).map_err(|_| DiagnosticClass::UnknownStatus)?;
    ProviderOrderType::try_from(wire.order_type).map_err(|_| DiagnosticClass::UnknownStatus)?;
    if wire.id != expected_id {
        return Err(DiagnosticClass::Contradictory);
    }
    let status =
        ExactOrderStatus::try_from(wire.status).map_err(|_| DiagnosticClass::UnknownStatus)?;
    let original_size = checked_decimal(wire.original_size, limits)?;
    let size_matched = checked_decimal(wire.size_matched, limits)?;
    let price = checked_decimal(wire.price, limits)?;
    let associated_trade_ids = collect_strings(
        wire.associate_trades,
        CollectorKind::AssociatedTrades,
        limits,
        |value| {
            checked_string(value, limits)?;
            Ok(value.to_owned())
        },
    )?;

    if has_duplicates(&associated_trade_ids) {
        return Err(DiagnosticClass::Contradictory);
    }
    Ok(ExactOrderObservation {
        order_id: wire.id.to_owned(),
        status,
        original_size,
        size_matched,
        price,
        associated_trade_ids,
    })
}

fn convert_trade(
    wire: WireTrade<'_>,
    limits: SemanticLimits,
) -> Result<AssociatedTradeObservation, DiagnosticClass> {
    for value in [
        wire.id,
        wire.taker_order_id,
        wire.market,
        wire.asset_id,
        wire.side,
        wire.match_time,
        wire.last_update,
        wire.outcome,
        wire.owner,
        wire.maker_address,
        wire.trader_side,
    ] {
        checked_string(value, limits)?;
    }
    checked_optional_string(wire.match_time_nano, limits)?;
    checked_optional_string(wire.err_msg, limits)?;
    let _ = wire.bucket_index;
    ProviderSide::try_from(wire.side).map_err(|_| DiagnosticClass::UnknownStatus)?;
    ProviderTraderSide::try_from(wire.trader_side).map_err(|_| DiagnosticClass::UnknownStatus)?;
    checked_decimal(wire.fee_rate_bps, limits)?;
    let status =
        AssociatedTradeStatus::try_from(wire.status).map_err(|_| DiagnosticClass::UnknownStatus)?;
    let size = checked_decimal(wire.size, limits)?;
    let price = checked_decimal(wire.price, limits)?;
    let transaction_hash = wire
        .transaction_hash
        .map(|value| parse_hash(value, limits))
        .transpose()?;
    Ok(AssociatedTradeObservation {
        id: wire.id.to_owned(),
        status,
        size,
        price,
        transaction_hash,
    })
}

fn checked_string(value: &str, limits: SemanticLimits) -> Result<(), DiagnosticClass> {
    if value.is_empty() {
        return Err(DiagnosticClass::Incomplete);
    }

    if value.len() > limits.string_bytes() {
        return Err(DiagnosticClass::Oversized);
    }
    Ok(())
}

fn checked_optional_string(
    value: Option<&str>,
    limits: SemanticLimits,
) -> Result<(), DiagnosticClass> {
    if let Some(value) = value
        && value.len() > limits.string_bytes()
    {
        return Err(DiagnosticClass::Oversized);
    }
    Ok(())
}

fn checked_decimal(value: &str, limits: SemanticLimits) -> Result<Decimal, DiagnosticClass> {
    if value.is_empty() {
        return Err(DiagnosticClass::Incomplete);
    }

    if value.len() > limits.decimal_bytes() {
        return Err(DiagnosticClass::Oversized);
    }
    Decimal::from_str(value).map_err(|_| DiagnosticClass::InvalidDecimal)
}

fn parse_hash(
    value: &str,
    limits: SemanticLimits,
) -> Result<[u8; TRANSACTION_HASH_BYTES], DiagnosticClass> {
    checked_string(value, limits)?;
    if value.len() != 2 + 2 * TRANSACTION_HASH_BYTES || !value.starts_with("0x") {
        return Err(DiagnosticClass::InvalidHash);
    }
    let mut hash = [0_u8; TRANSACTION_HASH_BYTES];

    for (index, pair) in value.as_bytes()[2..].chunks_exact(2).enumerate() {
        let high = hex_nibble(pair[0]).ok_or(DiagnosticClass::InvalidHash)?;
        let low = hex_nibble(pair[1]).ok_or(DiagnosticClass::InvalidHash)?;
        hash[index] = (high << 4) | low;
    }
    Ok(hash)
}

const fn hex_nibble(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn has_duplicates<T: Eq>(values: &[T]) -> bool {
    values
        .iter()
        .enumerate()
        .any(|(index, value)| values[..index].contains(value))
}

fn collector_class(error: CollectorError) -> DiagnosticClass {
    match error {
        CollectorError::ZeroCapacity | CollectorError::Incomplete => DiagnosticClass::Incomplete,
        CollectorError::ItemCapacity
        | CollectorError::ByteCapacity
        | CollectorError::ArithmeticOverflow => DiagnosticClass::Oversized,
        CollectorError::Contradictory => DiagnosticClass::Contradictory,
    }
}

fn collect_strings<T, F>(
    raw: &RawValue,
    kind: CollectorKind,
    limits: SemanticLimits,
    mut convert: F,
) -> Result<Vec<T>, DiagnosticClass>
where
    F: FnMut(&str) -> Result<T, DiagnosticClass>,
{
    let item_capacity = match kind {
        CollectorKind::TransactionHashes => limits.transaction_hashes(),
        CollectorKind::TradeIds => limits.trade_ids(),
        CollectorKind::AssociatedTrades => limits.associated_trades(),
        CollectorKind::RequestItems => limits.request_items(),
        CollectorKind::ResponseItems => limits.response_items(),
    };
    let plan = CollectorPlan::bounded(kind, item_capacity, limits.response_body_bytes(), limits)
        .map_err(collector_class)?;
    let mut failure = None;
    let seed = StringArraySeed {
        collector: plan.allocate(),
        convert: &mut convert,
        failure: &mut failure,
    };
    let mut deserializer = serde_json::Deserializer::from_str(raw.get());
    match seed.deserialize(&mut deserializer) {
        Ok(values) if deserializer.end().is_ok() => Ok(values),
        Ok(_) => Err(DiagnosticClass::Malformed),
        Err(_) => Err(failure.unwrap_or(DiagnosticClass::Malformed)),
    }
}

struct StringArraySeed<'a, T, F> {
    collector: FixedCollector<T>,
    convert: &'a mut F,
    failure: &'a mut Option<DiagnosticClass>,
}

impl<'de, T, F> DeserializeSeed<'de> for StringArraySeed<'_, T, F>
where
    F: FnMut(&str) -> Result<T, DiagnosticClass>,
{
    type Value = Vec<T>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(StringArrayVisitor {
            collector: self.collector,
            convert: self.convert,
            failure: self.failure,
        })
    }
}

struct StringArrayVisitor<'a, T, F> {
    collector: FixedCollector<T>,
    convert: &'a mut F,
    failure: &'a mut Option<DiagnosticClass>,
}

impl<'de, T, F> Visitor<'de> for StringArrayVisitor<'_, T, F>
where
    F: FnMut(&str) -> Result<T, DiagnosticClass>,
{
    type Value = Vec<T>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a bounded semantic string array")
    }

    fn visit_seq<A>(mut self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(value) = sequence.next_element::<&str>()? {
            let item = match (self.convert)(value) {
                Ok(item) => item,
                Err(class) => {
                    *self.failure = Some(class);
                    return Err(A::Error::custom("semantic value rejected"));
                }
            };

            if let Err(e) = self.collector.try_push(item, value.len()) {
                *self.failure = Some(collector_class(e));
                return Err(A::Error::custom("semantic capacity rejected"));
            }
        }
        self.collector.finish().map_err(|e| {
            *self.failure = Some(collector_class(e));
            A::Error::custom("semantic collection rejected")
        })
    }
}

fn collect_trades(
    raw: &[u8],
    expected_order_id: &str,
    limits: SemanticLimits,
) -> Result<Vec<AssociatedTradeObservation>, DiagnosticClass> {
    checked_string(expected_order_id, limits)?;
    let plan = CollectorPlan::bounded(
        CollectorKind::AssociatedTrades,
        limits.associated_trades(),
        limits.response_body_bytes(),
        limits,
    )
    .map_err(collector_class)?;
    let mut failure = None;
    let seed = TradeArraySeed {
        collector: plan.allocate(),
        expected_order_id,
        limits,
        failure: &mut failure,
    };
    let mut deserializer = serde_json::Deserializer::from_slice(raw);
    let trades = match seed.deserialize(&mut deserializer) {
        Ok(trades) if deserializer.end().is_ok() => trades,
        Ok(_) => return Err(DiagnosticClass::Malformed),
        Err(_) => return Err(failure.unwrap_or(DiagnosticClass::Malformed)),
    };

    for (index, trade) in trades.iter().enumerate() {
        if trades[..index].iter().any(|other| other.id == trade.id) {
            return Err(DiagnosticClass::Contradictory);
        }
    }
    Ok(trades)
}

struct TradeArraySeed<'a> {
    collector: FixedCollector<AssociatedTradeObservation>,
    expected_order_id: &'a str,
    limits: SemanticLimits,
    failure: &'a mut Option<DiagnosticClass>,
}

impl<'de> DeserializeSeed<'de> for TradeArraySeed<'_> {
    type Value = Vec<AssociatedTradeObservation>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(TradeArrayVisitor {
            collector: self.collector,
            expected_order_id: self.expected_order_id,
            limits: self.limits,
            failure: self.failure,
        })
    }
}

struct TradeArrayVisitor<'a> {
    collector: FixedCollector<AssociatedTradeObservation>,
    expected_order_id: &'a str,
    limits: SemanticLimits,
    failure: &'a mut Option<DiagnosticClass>,
}

impl<'de> Visitor<'de> for TradeArrayVisitor<'_> {
    type Value = Vec<AssociatedTradeObservation>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a bounded associated-trade array")
    }

    fn visit_seq<A>(mut self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(wire) = sequence.next_element::<WireTrade<'de>>()? {
            let encoded_bytes = wire.id.len();
            let maker_matches =
                match validate_maker_orders(wire.maker_orders, self.expected_order_id, self.limits)
                {
                    Ok(matches) => matches,
                    Err(class) => {
                        *self.failure = Some(class);
                        return Err(A::Error::custom("semantic maker orders rejected"));
                    }
                };

            if wire.taker_order_id != self.expected_order_id && !maker_matches {
                *self.failure = Some(DiagnosticClass::Contradictory);
                return Err(A::Error::custom("trade does not reference requested order"));
            }
            let trade = match convert_trade(wire, self.limits) {
                Ok(trade) => trade,
                Err(class) => {
                    *self.failure = Some(class);
                    return Err(A::Error::custom("semantic trade rejected"));
                }
            };

            if let Err(e) = self.collector.try_push(trade, encoded_bytes) {
                *self.failure = Some(collector_class(e));
                return Err(A::Error::custom("semantic trade capacity rejected"));
            }
        }
        self.collector.finish().map_err(|e| {
            *self.failure = Some(collector_class(e));
            A::Error::custom("semantic trade collection rejected")
        })
    }
}

fn validate_maker_orders(
    raw: &RawValue,
    expected_order_id: &str,
    limits: SemanticLimits,
) -> Result<bool, DiagnosticClass> {
    let plan = CollectorPlan::bounded(
        CollectorKind::ResponseItems,
        limits.response_items(),
        limits.response_body_bytes(),
        limits,
    )
    .map_err(collector_class)?;
    let mut failure = None;
    let seed = MakerArraySeed {
        collector: plan.allocate(),
        expected_order_id,
        limits,
        failure: &mut failure,
    };
    let mut deserializer = serde_json::Deserializer::from_str(raw.get());
    match seed.deserialize(&mut deserializer) {
        Ok(matches) if deserializer.end().is_ok() => Ok(matches),
        Ok(_) => Err(DiagnosticClass::Malformed),
        Err(_) => Err(failure.unwrap_or(DiagnosticClass::Malformed)),
    }
}

fn validate_maker(
    maker: WireMakerOrder<'_>,
    limits: SemanticLimits,
) -> Result<(), DiagnosticClass> {
    for value in [
        maker.order_id,
        maker.owner,
        maker.maker_address,
        maker.asset_id,
        maker.outcome,
    ] {
        checked_string(value, limits)?;
    }
    checked_optional_string(maker.side, limits)?;
    if let Some(side) = maker.side {
        ProviderSide::try_from(side).map_err(|_| DiagnosticClass::UnknownStatus)?;
    }
    checked_optional_string(maker.builder_code, limits)?;
    checked_decimal(maker.matched_amount, limits)?;
    checked_decimal(maker.price, limits)?;
    checked_decimal(maker.fee_rate_bps, limits)?;
    if let Some(builder_fee) = maker.builder_fee {
        checked_decimal(builder_fee, limits)?;
    }
    Ok(())
}

struct MakerArraySeed<'a> {
    collector: FixedCollector<()>,
    expected_order_id: &'a str,
    limits: SemanticLimits,
    failure: &'a mut Option<DiagnosticClass>,
}

impl<'de> DeserializeSeed<'de> for MakerArraySeed<'_> {
    type Value = bool;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(MakerArrayVisitor {
            collector: self.collector,
            expected_order_id: self.expected_order_id,
            limits: self.limits,
            failure: self.failure,
            matched_expected_order: false,
        })
    }
}

struct MakerArrayVisitor<'a> {
    collector: FixedCollector<()>,
    expected_order_id: &'a str,
    limits: SemanticLimits,
    failure: &'a mut Option<DiagnosticClass>,
    matched_expected_order: bool,
}

impl<'de> Visitor<'de> for MakerArrayVisitor<'_> {
    type Value = bool;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("a bounded maker-order array")
    }

    fn visit_seq<A>(mut self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while let Some(maker) = sequence.next_element::<WireMakerOrder<'de>>()? {
            let encoded_bytes = maker.order_id.len();
            self.matched_expected_order |= maker.order_id == self.expected_order_id;
            if let Err(class) = validate_maker(maker, self.limits) {
                *self.failure = Some(class);
                return Err(A::Error::custom("semantic maker order rejected"));
            }

            if let Err(e) = self.collector.try_push((), encoded_bytes) {
                *self.failure = Some(collector_class(e));
                return Err(A::Error::custom("semantic maker capacity rejected"));
            }
        }
        self.collector.finish().map_err(|e| {
            *self.failure = Some(collector_class(e));
            A::Error::custom("semantic maker collection rejected")
        })?;
        Ok(self.matched_expected_order)
    }
}
