// -------------------------------------------------------------------------------------------------
//  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
//  https://nautechsystems.io
//
//  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
//  You may not use this file except in compliance with the License.
//  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
// -------------------------------------------------------------------------------------------------

//! Identity compatibility rule shared by every reconciliation evidence check.

/// Returns whether two optional identity values can describe the same venue entity.
///
/// Venues routinely carry an identity on one report of a pair and omit it on the other.
/// A hedging venue reports the position ID on order status but not on execution data,
/// and some venues echo the client order ID on fills but not on the order snapshot. An
/// omitted value is absence of evidence rather than a contradiction, so only two
/// explicit and differing values conflict.
///
/// This is the single rule for comparing optional reconciliation identities. Attribution
/// decisions that need an explicit identity, such as choosing which hedge position a fill
/// belongs to, must be made by the caller rather than by tightening this rule.
#[must_use]
pub fn optional_identities_compatible<T: PartialEq>(left: Option<T>, right: Option<T>) -> bool {
    left.zip(right).is_none_or(|(left, right)| left == right)
}
