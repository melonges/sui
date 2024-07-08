// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    data::{Conn, DbConnection},
    filter, inner_join, max_option, min_option, query,
    raw_query::RawQuery,
    types::{
        cursor::Page,
        digest::Digest,
        sui_address::SuiAddress,
        transaction_block::TransactionBlockKindInput,
        type_filter::{FqNameFilter, ModuleFilter},
    },
};
use diesel::{
    backend::Backend,
    deserialize::{self, FromSql, QueryableByName},
    row::NamedRow,
};
use std::fmt::{self, Write};
use sui_types::base_types::SuiAddress as NativeSuiAddress;

use super::{Cursor, TransactionBlockFilter};

/// The `tx_sequence_number` range of the transactions to be queried.
#[derive(Clone, Debug, Copy)]
pub(crate) struct StoredTxBounds {
    pub lo: i64,
    pub hi: i64,
}

#[derive(Clone, Debug, Copy)]
pub(crate) struct TxBounds {
    /// The unmodified lower bound tx_sequence_number corresponding to the first tx_sequence_number
    /// of the lower checkpoint bound.
    pub lo: u64,
    /// The unmodified upper bound tx_sequence_number corresponding to the last tx_sequence_number
    /// of the upper checkpoint bound.
    pub hi: u64,
    pub after: Option<u64>,
    pub before: Option<u64>,
    pub scan_limit: Option<u64>,
    pub is_from_front: bool,
}

impl TxBounds {
    pub(crate) fn new(
        lo: u64,
        hi: u64,
        after: Option<u64>,
        before: Option<u64>,
        scan_limit: Option<u64>,
        is_from_front: bool,
    ) -> Self {
        Self {
            lo,
            hi,
            after,
            before,
            scan_limit,
            is_from_front,
        }
    }

    /// The default checkpoint lower bound is 0 and the default checkpoint upper bound is
    /// `checkpoint_viewed_at`. The two ends are then further adjusted, selecting the greatest
    /// between `after_cp` and `at_cp`, and the smallest among `before_cp`, `at_cp`, and
    /// `checkpoint_viewed_at`. By incrementing `after` by 1 and decrementing `before` by 1, we can
    /// construct the tx_sequence_number equivalent by selecting the smallest `tx_sequence_number`
    /// from `lo_cp` and the largest `tx_sequence_number` from `hi_cp`. Finally, cursors and the
    /// scan limit are applied. If the after cursor exceeds rhs, or before cursor is below lhs, or
    /// other inconsistency, return None.
    pub(crate) fn query(
        conn: &mut Conn,
        after_cp: Option<u64>,
        at_cp: Option<u64>,
        before_cp: Option<u64>,
        checkpoint_viewed_at: u64,
        scan_limit: Option<u64>,
        page: &Page<Cursor>,
    ) -> Result<Option<Self>, diesel::result::Error> {
        let lo_cp = max_option!(after_cp.map(|x| x.saturating_add(1)), at_cp).unwrap_or(0);
        let hi_cp = min_option!(
            before_cp.map(|x| x.saturating_sub(1)),
            at_cp,
            Some(checkpoint_viewed_at)
        )
        .unwrap();
        let from_db: StoredTxBounds =
            conn.result(move || tx_bounds_query(lo_cp, hi_cp).into_boxed())?;

        println!("StoredTxBounds: {:?}", from_db);

        let lo = from_db.lo as u64;
        let hi = from_db.hi as u64;

        println!("checkpoint_viewed_at: {}", checkpoint_viewed_at);

        println!("before: {:?}", page.before().map(|x| x.tx_sequence_number));

        println!(
            "TxBounds::Query: lo: {}, hi: {}, scan_limit: {}, is_from_front: {}",
            lo,
            hi,
            scan_limit.unwrap_or(0),
            page.is_from_front()
        );

        if page.after().map_or(false, |x| x.tx_sequence_number >= hi)
            || page.before().map_or(false, |x| x.tx_sequence_number <= lo)
        {
            return Ok(None);
        }

        Ok(Some(Self::new(
            lo,
            hi,
            page.after().map(|x| x.tx_sequence_number),
            page.before().map(|x| x.tx_sequence_number),
            scan_limit,
            page.is_from_front(),
        )))
    }

    /// The lower bound tx_sequence_number to scan within. This defaults to the min
    /// tx_sequence_number of the checkpoint bound. If a cursor is provided, the lower bound is
    /// adjusted to the larger of the two, and this gets added to the scan limit if provided.
    pub(crate) fn scan_lo(&self) -> u64 {
        // if the cursor exceeds scan_hi then we probably don't even need to run the query?
        let adjusted_lo = self.after.map_or(self.lo, |a| std::cmp::max(self.lo, a));

        println!("true lo: {}, adjusted_lo: {}", self.lo, adjusted_lo);

        let final_lo = if self.is_from_front {
            adjusted_lo
        } else {
            std::cmp::max(
                adjusted_lo,
                self.scan_hi()
                    .saturating_sub(self.scan_limit.unwrap_or(self.hi)),
            )
        };

        println!("after applying scan_limit: {}", final_lo);

        final_lo
    }

    /// The upper bound tx_sequence_number to scan within. This defaults to the max
    /// tx_sequence_number of the checkpoint bound. If a cursor is provided, the upper bound is
    /// adjusted to the smaller of the two, and this gets added to the scan limit if provided.
    pub(crate) fn scan_hi(&self) -> u64 {
        let adjusted_hi = self.before.map_or(self.hi, |b| std::cmp::min(self.hi, b));

        println!("true hi: {}, adjusted_hi: {}", self.hi, adjusted_hi);

        let final_hi = if self.is_from_front {
            std::cmp::min(
                adjusted_hi,
                self.scan_lo()
                    .saturating_add(self.scan_limit.unwrap_or(adjusted_hi)),
            )
        } else {
            adjusted_hi
        };

        println!("after applying scan_limit: {}", final_hi);

        final_hi
    }

    /// If the query result does not have a previous page, check whether the scan limit is within
    /// the initial tx_sequence_number range.
    pub(crate) fn scan_has_prev_page(&self) -> bool {
        if self.after.unwrap_or(0) >= self.hi {
            return false;
        }

        self.scan_lo() > self.lo
    }

    /// If the query result does not have a next page, check whether the scan limit is within the
    /// initial tx_sequence_number range.
    pub(crate) fn scan_has_next_page(&self) -> bool {
        if self.before.unwrap_or(self.hi) <= self.lo {
            return false;
        }

        self.scan_hi() < self.hi
    }
}

impl TransactionBlockFilter {
    /// Returns the count of `function`, `kind`, `recv_address`, `input_object`, and
    /// `changed_object`.
    pub(crate) fn complex_filters(&self) -> usize {
        [
            self.function.is_some(),
            self.kind.is_some(),
            self.recv_address.is_some(),
            self.input_object.is_some(),
            self.changed_object.is_some(),
        ]
        .iter()
        .filter(|&is_set| *is_set)
        .count()
    }

    /// A TransactionBlockFilter is considered not to have any filters if no filters are specified,
    /// or if the only filters are on `checkpoint`.
    pub(crate) fn has_filters(&self) -> bool {
        self.function.is_some()
            || self.kind.is_some()
            || self.sign_address.is_some()
            || self.recv_address.is_some()
            || self.input_object.is_some()
            || self.changed_object.is_some()
            || self.transaction_ids.is_some()
    }

    pub(crate) fn is_empty(&self) -> bool {
        matches!(self.before_checkpoint, Some(0))
            || matches!((self.after_checkpoint, self.before_checkpoint), (Some(after), Some(before)) if after >= before)
            || matches!((self.after_checkpoint, self.at_checkpoint), (Some(after), Some(at)) if after >= at)
            || matches!((self.at_checkpoint, self.before_checkpoint), (Some(at), Some(before)) if at >= before)
            // If SystemTx, sender if specified must be 0x0. Conversely, if sender is 0x0, kind must be SystemTx.
            || matches!((self.kind, self.sign_address), (Some(kind), Some(signer)) if (kind == TransactionBlockKindInput::SystemTx) != (signer == SuiAddress::from(NativeSuiAddress::ZERO)))
    }
}

/// `sql_query` raw queries require `QueryableByName`. The default implementation looks for a table
/// based on the struct name, and it also expects the struct's fields to reflect the table's
/// columns. We can override this behavior by implementing `QueryableByName` for our struct. For
/// `TxBounds`, its fields are derived from `checkpoints`, so we can't leverage the default
/// implementation directly.
impl<DB> QueryableByName<DB> for StoredTxBounds
where
    DB: Backend,
    i64: FromSql<diesel::sql_types::BigInt, DB>,
{
    fn build<'a>(row: &impl NamedRow<'a, DB>) -> deserialize::Result<Self> {
        let lo = NamedRow::get::<diesel::sql_types::BigInt, _>(row, "lo")?;
        let hi = NamedRow::get::<diesel::sql_types::BigInt, _>(row, "hi")?;

        Ok(Self { lo, hi })
    }
}

/// Constructs a query that selects the first tx_sequence_number of lo_cp and the last
/// tx_sequence_number of hi_cp. The first tx_sequence_number of lo_cp is the
/// `network_total_transactions` of lo_cp - 1, and the last tx_sequence_number is the
/// `network_total_transactions` - 1 of `hi_cp`.
pub(crate) fn tx_bounds_query(lo_cp: u64, hi_cp: u64) -> RawQuery {
    let lo = match lo_cp {
        0 => query!("SELECT 0"),
        _ => query!(format!(
            r#"SELECT network_total_transactions
            FROM checkpoints
            WHERE sequence_number = {}"#,
            lo_cp.saturating_sub(1)
        )),
    };

    let hi = query!(format!(
        r#"SELECT network_total_transactions - 1
        FROM checkpoints
        WHERE sequence_number = {}"#,
        hi_cp
    ));

    query!(
        "SELECT CAST(({}) AS BIGINT) AS lo, CAST(({}) AS BIGINT) AS hi",
        lo,
        hi
    )
}

/// Determines the maximum value in an arbitrary number of Option<u64>.
#[macro_export]
macro_rules! max_option {
    ($($x:expr),+ $(,)?) => {{
        [$($x),*].iter()
            .filter_map(|&x| x)
            .max()
    }};
}

/// Determines the minimum value in an arbitrary number of Option<u64>.
#[macro_export]
macro_rules! min_option {
    ($($x:expr),+ $(,)?) => {{
        [$($x),*].iter()
            .filter_map(|&x| x)
            .min()
    }};
}

/// Constructs a `RawQuery` as a join over all relevant side tables, filtered on their own filter
/// condition, plus optionally a sender, plus optionally tx/cp bounds.
pub(crate) fn subqueries(filter: &TransactionBlockFilter, tx_bounds: TxBounds) -> Option<RawQuery> {
    let sender = filter.sign_address;

    let mut subqueries = vec![];

    if let Some(f) = &filter.function {
        subqueries.push(match f {
            FqNameFilter::ByModule(filter) => match filter {
                ModuleFilter::ByPackage(p) => (select_pkg(p, sender, tx_bounds), "tx_calls_pkg"),
                ModuleFilter::ByModule(p, m) => {
                    (select_mod(p, m.clone(), sender, tx_bounds), "tx_calls_mod")
                }
            },
            FqNameFilter::ByFqName(p, m, n) => (
                select_fun(p, m.clone(), n.clone(), sender, tx_bounds),
                "tx_calls_fun",
            ),
        });
    }
    if let Some(kind) = &filter.kind {
        subqueries.push((select_kind(*kind, tx_bounds), "tx_kinds"));
    }
    if let Some(recv) = &filter.recv_address {
        subqueries.push((select_recipient(recv, sender, tx_bounds), "tx_recipients"));
    }
    if let Some(input) = &filter.input_object {
        subqueries.push((select_input(input, sender, tx_bounds), "tx_input_objects"));
    }
    if let Some(changed) = &filter.changed_object {
        subqueries.push((
            select_changed(changed, sender, tx_bounds),
            "tx_changed_objects",
        ));
    }
    if let Some(sender) = &sender {
        if filter.complex_filters() == 0 || filter.kind.is_some() {
            subqueries.push((select_sender(sender, tx_bounds), "tx_senders"));
        }
    }
    if let Some(txs) = &filter.transaction_ids {
        subqueries.push((select_ids(txs, tx_bounds), "tx_digests"));
    }

    if subqueries.is_empty() {
        return None;
    }

    let mut subquery = subqueries.pop().unwrap().0;

    if !subqueries.is_empty() {
        subquery = query!("SELECT tx_sequence_number FROM ({}) AS initial", subquery);
        while let Some((subselect, alias)) = subqueries.pop() {
            subquery = inner_join!(subquery, alias => subselect, using: ["tx_sequence_number"]);
        }
    }

    Some(subquery)
}

pub(crate) fn select_tx(sender: Option<SuiAddress>, bound: TxBounds, from: &str) -> RawQuery {
    let mut query = query!(format!("SELECT tx_sequence_number FROM {}", from));

    if let Some(sender) = sender {
        query = filter!(query, format!("sender = {}", bytea_literal(&sender)));
    }

    filter!(
        query,
        format!(
            "tx_sequence_number >= {} AND tx_sequence_number <= {}",
            bound.lo, bound.hi
        )
    )
}

pub(crate) fn select_pkg(
    pkg: &SuiAddress,
    sender: Option<SuiAddress>,
    bound: TxBounds,
) -> RawQuery {
    filter!(
        select_tx(sender, bound, "tx_calls_pkg"),
        format!("package = {}", bytea_literal(pkg))
    )
}

pub(crate) fn select_mod(
    pkg: &SuiAddress,
    mod_: String,
    sender: Option<SuiAddress>,
    bound: TxBounds,
) -> RawQuery {
    filter!(
        select_tx(sender, bound, "tx_calls_mod"),
        format!("package = {} and module = {{}}", bytea_literal(pkg)),
        mod_
    )
}

pub(crate) fn select_fun(
    pkg: &SuiAddress,
    mod_: String,
    fun: String,
    sender: Option<SuiAddress>,
    bound: TxBounds,
) -> RawQuery {
    filter!(
        select_tx(sender, bound, "tx_calls_fun"),
        format!(
            "package = {} AND module = {{}} AND func = {{}}",
            bytea_literal(pkg),
        ),
        mod_,
        fun
    )
}

pub(crate) fn select_kind(kind: TransactionBlockKindInput, bound: TxBounds) -> RawQuery {
    filter!(
        select_tx(None, bound, "tx_kinds"),
        format!("tx_kind = {}", kind as i16)
    )
}

pub(crate) fn select_sender(sender: &SuiAddress, bound: TxBounds) -> RawQuery {
    select_tx(Some(*sender), bound, "tx_senders")
}

pub(crate) fn select_recipient(
    recv: &SuiAddress,
    sender: Option<SuiAddress>,
    bound: TxBounds,
) -> RawQuery {
    filter!(
        select_tx(sender, bound, "tx_recipients"),
        format!("recipient = '\\x{}'::bytea", hex::encode(recv.into_vec()))
    )
}

pub(crate) fn select_input(
    input: &SuiAddress,
    sender: Option<SuiAddress>,
    bound: TxBounds,
) -> RawQuery {
    filter!(
        select_tx(sender, bound, "tx_input_objects"),
        format!("object_id = '\\x{}'::bytea", hex::encode(input.into_vec()))
    )
}

pub(crate) fn select_changed(
    changed: &SuiAddress,
    sender: Option<SuiAddress>,
    bound: TxBounds,
) -> RawQuery {
    filter!(
        select_tx(sender, bound, "tx_changed_objects"),
        format!(
            "object_id = '\\x{}'::bytea",
            hex::encode(changed.into_vec())
        )
    )
}

pub(crate) fn select_ids(ids: &Vec<Digest>, bound: TxBounds) -> RawQuery {
    let query = select_tx(None, bound, "tx_digests");
    if ids.is_empty() {
        filter!(query, "1=0")
    } else {
        let mut inner = String::new();
        let mut prefix = "tx_digest IN (";
        for id in ids {
            write!(
                &mut inner,
                "{prefix}'\\x{}'::bytea",
                hex::encode(id.to_vec())
            )
            .unwrap();
            prefix = ", ";
        }
        inner.push(')');
        filter!(query, inner)
    }
}

pub(crate) fn bytea_literal(addr: &SuiAddress) -> impl fmt::Display + '_ {
    struct ByteaLiteral<'a>(&'a [u8]);

    impl fmt::Display for ByteaLiteral<'_> {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "'\\x{}'::bytea", hex::encode(self.0))
        }
    }

    ByteaLiteral(addr.as_slice())
}
