"""P0-9: venue decimals are never silently rounded through binary float.

The contract (see ``docs/NUMERIC_PRECISION.md``): exact venue decimal text is
parsed to ``Decimal`` (never via ``float``), persisted losslessly as
``<field>_exact`` text, and float64 columns are DERIVED approximations.

Two kinds of values are used deliberately:

* venue-realistic values (13-16 significant digits, e.g. OKX funding rates)
  that binary float happens to survive but only as an approximation of the
  decimal (``Decimal(float(x)) != Decimal(x)``), and
* values beyond float64's ~17 significant digits, where float provably
  collapses DISTINCT decimals into one. No supported venue sends these
  today; they exist to make the loss observable rather than argued.
"""
import glob
import json
import os
from decimal import Decimal

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.collector.adapters.binance import BinanceAdapter
from collector.collector.adapters.bybit import BybitAdapter
from collector.collector.adapters.okx import OKXAdapter
from collector.collector.binance_oi import normalize_binance_oi
from collector.collector.book_engine import LocalBook  # noqa: F401  (book keys are Decimal)
from collector.collector.canonical import OIUnit
from collector.collector.config import (
    LIQUIDATION_SCHEMA, MARKPRICE_SCHEMA, OPENINTEREST_SCHEMA, ORDERBOOK_SCHEMA, TRADES_SCHEMA,
)
from collector.collector.feature_computer import (
    compute_liquidation_features, compute_markprice_features, compute_openinterest_features,
    compute_orderbook_features, compute_trades_features,
)
from collector.collector.numeric import column_value, dec, exact_text
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.replay import FrameKind, ReplayEngine, ReplayFrame, ReplaySource
from collector.pipeline.dataset_assembler import assemble_dataset

# Values chosen from repository evidence: OKX funding-rate fixtures carry 16
# significant digits ("0.0001875391284828"); BTCUSDT prices carry up to 8
# fractional digits.
PRICE = "12345.67890123"
QTY = "0.00012345"
NEAR_TICK = "99999.99999999"
FUNDING = "0.0001875391284828"
# Beyond float64: 21 significant digits, and two DISTINCT decimals that are the
# same float. (Asserted below, so the test suite itself proves the premise.)
WIDE_A = "100000.000000000000001"
WIDE_B = "100000.000000000000002"


def test_the_premise_binary_float_really_loses_these_values():
    """Guard against a vacuous suite: prove float loses what we test."""
    assert float(WIDE_A) == float(WIDE_B)           # distinct decimals collapse
    assert Decimal(float(WIDE_A)) != Decimal(WIDE_A)
    assert Decimal(float(PRICE)) != Decimal(PRICE)  # even realistic values are not exact
    assert Decimal(float(FUNDING)) != Decimal(FUNDING)
    assert Decimal(float("0.1")) != Decimal("0.1")


# ---------------------------------------------------------------------------
# The parse / format primitives
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text", [PRICE, QTY, NEAR_TICK, FUNDING, WIDE_A, WIDE_B, "0.1", "0.00000001", "100.50", "0", "1E-8"])
def test_dec_is_exact_and_exact_text_round_trips(text):
    value = dec(text)
    assert value == Decimal(text)
    assert dec(exact_text(value)) == value
    assert "e" not in exact_text(value).lower()


def test_exact_text_preserves_trailing_zeros():
    assert exact_text(dec("100.50")) == "100.50"
    assert exact_text(dec("0.00000001")) == "0.00000001"


@pytest.mark.parametrize("bad", ["abc", "", "NaN", "Infinity", "1,5", True])
def test_malformed_numeric_raises_never_becomes_a_number(bad):
    with pytest.raises((ValueError, TypeError)):
        dec(bad)


def test_missing_is_none_not_zero():
    assert dec(None) is None


def test_a_json_number_uses_its_shortest_repr_not_the_binary_expansion():
    """A venue that sent a JSON *number* has already lost digits; we must not
    then invent the float's 50-digit binary expansion."""
    assert dec(0.1) == Decimal("0.1")


# ---------------------------------------------------------------------------
# 1-4. Feature computers keep exact evidence (trade / orderbook / mark / liq / OI)
# ---------------------------------------------------------------------------

def test_trade_price_and_quantity_are_exact():
    f = compute_trades_features({"a": 1, "p": PRICE, "q": QTY, "m": False, "T": 1})
    assert f["price"] == Decimal(PRICE) and isinstance(f["price"], Decimal)
    assert f["quantity"] == Decimal(QTY) and isinstance(f["quantity"], Decimal)


def test_two_prices_that_collide_as_floats_stay_distinct_in_trades():
    a = compute_trades_features({"a": 1, "p": WIDE_A, "q": "1", "m": False})
    b = compute_trades_features({"a": 2, "p": WIDE_B, "q": "1", "m": False})
    assert a["price"] != b["price"]


def test_orderbook_evidence_arrays_are_exact_and_distinct_levels_never_merge():
    msg = {"E": 1, "b": [[WIDE_B, QTY], [WIDE_A, "2"]], "a": [["100001.5", QTY]]}
    f = compute_orderbook_features(msg)
    assert f["bids_price"] == [Decimal(WIDE_B), Decimal(WIDE_A)]
    assert len(set(f["bids_price"])) == 2  # float would have made them equal
    assert f["bids_qty"][0] == Decimal(QTY)
    assert all(isinstance(x, Decimal) for x in f["bids_price"] + f["bids_qty"] + f["asks_price"] + f["asks_qty"])


def test_markprice_and_funding_rate_are_exact():
    f = compute_markprice_features({"E": 1, "p": NEAR_TICK, "r": FUNDING, "T": 3_600_001})
    assert f["mark_price"] == Decimal(NEAR_TICK)
    assert f["funding_rate"] == Decimal(FUNDING)
    # the derived bps figure is explicitly float64, and consistent with the exact rate
    assert isinstance(f["funding_rate_bps"], float)
    assert f["funding_rate_bps"] == pytest.approx(float(Decimal(FUNDING) * 10000))


def test_liquidation_price_and_quantity_are_exact():
    f = compute_liquidation_features({"o": {"S": "SELL", "p": PRICE, "q": QTY, "T": 5}})
    assert f["price"] == Decimal(PRICE) and f["quantity"] == Decimal(QTY)
    assert isinstance(f["signed_qty"], float)  # derived analytic, documented


def test_open_interest_value_is_exact_and_unit_stays_unknown():
    assert compute_openinterest_features({"openInterest": "123456.789012345", "time": 1})["open_interest"] == Decimal("123456.789012345")
    event = normalize_binance_oi(json.dumps({"openInterest": "123456.789012345", "time": 1}),
                                 response_receive_ts=10, symbol="BTCUSDT")
    assert event.open_interest == Decimal("123456.789012345")
    assert event.unit is OIUnit.UNKNOWN  # P0-9 must not guess/convert the unit


# ---------------------------------------------------------------------------
# 9. Integer identifiers never pass through float
# ---------------------------------------------------------------------------

BIG_ID = 9007199254740993  # 2**53 + 1: float() would return ...992


def test_premise_float_corrupts_large_ids():
    assert int(float(BIG_ID)) != BIG_ID


def test_trade_id_stays_an_exact_integer_in_features():
    f = compute_trades_features({"a": BIG_ID, "p": "1", "q": "1", "m": False})
    assert f["trade_id"] == BIG_ID and isinstance(f["trade_id"], int)


def test_trade_id_stays_exact_through_the_adapter():
    events = BinanceAdapter().normalize(
        {"stream": "btcusdt@aggTrade",
         "data": {"e": "aggTrade", "E": 1, "T": 1, "a": BIG_ID, "p": "1", "q": "1", "m": False}},
        local_receive_ts=5)
    assert events[0].trade_id == str(BIG_ID)


# ---------------------------------------------------------------------------
# 15. Missing / malformed numerics never become a fake zero
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("fn,msg", [
    (compute_trades_features, {"a": 1, "p": "1"}),                       # no quantity
    (compute_trades_features, {"a": 1, "q": "1"}),                       # no price
    (compute_trades_features, {"a": 1, "p": "abc", "q": "1"}),           # malformed
    (compute_markprice_features, {"E": 1, "p": "1", "T": 2}),            # no funding rate
    (compute_markprice_features, {"E": 1, "r": "0.0001", "T": 2}),       # no mark price
    (compute_openinterest_features, {"time": 1}),                        # no OI
    (compute_openinterest_features, {"openInterest": "x", "time": 1}),
    (compute_liquidation_features, {"o": {"S": "BUY", "q": "1"}}),       # no price
    (compute_liquidation_features, {"o": {"S": "BUY", "p": "1"}}),       # no quantity
])
def test_missing_or_malformed_numeric_yields_no_row_not_a_zero(fn, msg):
    assert fn(msg) == {}


# ---------------------------------------------------------------------------
# Cross-venue isolation: every adapter parses the same text to the same exact value
# ---------------------------------------------------------------------------

def test_all_venues_parse_the_same_decimal_text_to_the_same_exact_value():
    binance = BinanceAdapter().normalize(
        {"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "E": 1, "T": 1, "a": 1, "p": PRICE, "q": QTY, "m": False}},
        local_receive_ts=5)[0]
    bybit = BybitAdapter().normalize(
        {"topic": "publicTrade.BTCUSDT", "type": "snapshot", "ts": 1,
         "data": [{"T": 1, "s": "BTCUSDT", "S": "Buy", "v": QTY, "p": PRICE, "i": "1"}]},
        local_receive_ts=5)[0]
    okx = OKXAdapter().normalize(
        {"arg": {"channel": "trades", "instId": "BTC-USDT-SWAP"},
         "data": [{"instId": "BTC-USDT-SWAP", "tradeId": "1", "px": PRICE, "sz": QTY, "side": "buy", "ts": "1", "seqId": 1}]},
        local_receive_ts=5)[0]
    for event in (binance, bybit, okx):
        assert event.price == Decimal(PRICE) and event.quantity == Decimal(QTY)
        assert isinstance(event.price, Decimal)


# ---------------------------------------------------------------------------
# 11. Parquet round trip (the exact -> persisted boundary)
# ---------------------------------------------------------------------------

def _read_rows(tmp_path, stream):
    path = glob.glob(str(tmp_path / "raw" / stream / "*.seg"))[0]
    return pq.read_table(path).to_pylist()


def test_trade_round_trips_exactly_through_the_real_writer(tmp_path):
    features = compute_trades_features({"a": BIG_ID, "p": WIDE_A, "q": QTY, "m": False, "T": 7})
    w = ParquetWriter("trades", TRADES_SCHEMA, base_dir=str(tmp_path))
    w.write(features)
    w.close()
    row = _read_rows(tmp_path, "trades")[0]
    assert row["price_exact"] == WIDE_A                # exact text survives
    assert row["quantity_exact"] == QTY
    assert Decimal(row["price_exact"]) == Decimal(WIDE_A)
    assert row["price"] == float(WIDE_A)               # float64 is the DERIVED value
    assert row["trade_id"] == BIG_ID                   # integer column, exact


def test_orderbook_round_trips_exact_level_text(tmp_path):
    features = compute_orderbook_features({"E": 1, "b": [[WIDE_B, QTY], [WIDE_A, "2"]], "a": [["100001.5", QTY]]})
    w = ParquetWriter("orderbook", ORDERBOOK_SCHEMA, base_dir=str(tmp_path))
    w.write(features)
    w.close()
    row = _read_rows(tmp_path, "orderbook")[0]
    assert row["bids_price_exact"] == [WIDE_B, WIDE_A]          # distinct, exact
    assert row["bids_price"][0] == row["bids_price"][1]         # the float64 column collapsed them
    assert row["bids_qty_exact"] == [QTY, "2"]
    assert row["bid_depth"] == 2 and row["ask_depth"] == 1      # P0-8 depth semantics intact


def test_markprice_liquidation_oi_round_trip(tmp_path):
    cases = (
        ("markprice", MARKPRICE_SCHEMA, compute_markprice_features({"E": 1, "p": NEAR_TICK, "r": FUNDING, "T": 3_600_001}),
         {"mark_price_exact": NEAR_TICK, "funding_rate_exact": FUNDING}),
        ("liquidation", LIQUIDATION_SCHEMA, compute_liquidation_features({"o": {"S": "SELL", "p": PRICE, "q": QTY, "T": 5}}),
         {"price_exact": PRICE, "quantity_exact": QTY}),
        ("openinterest", OPENINTEREST_SCHEMA, compute_openinterest_features({"openInterest": "123456.789012345", "time": 1}),
         {"open_interest_exact": "123456.789012345"}),
    )
    for stream, schema, features, expected in cases:
        w = ParquetWriter(stream, schema, base_dir=str(tmp_path))
        w.write(features)
        w.close()
        row = _read_rows(tmp_path, stream)[0]
        for column, text in expected.items():
            assert row[column] == text, (stream, column)


def test_column_value_never_reconstructs_exactness_from_a_float():
    """A caller that already has a float has already lost the digits; the
    boundary must record 'no exact evidence' (null) rather than mint an
    exact-looking companion from the float."""
    assert column_value("price_exact", {"price": 0.1}) is None
    assert column_value("price_exact", {"price": Decimal("0.1")}) == "0.1"
    assert column_value("price_exact", {}) is None
    assert column_value("price", {"price": Decimal("0.1")}) == 0.1
    assert column_value("bids_price_exact", {"bids_price": [0.1, 0.2]}) is None
    assert column_value("bids_price_exact", {"bids_price": [Decimal("0.1"), Decimal("0.2")]}) == ["0.1", "0.2"]


def test_explicit_exact_value_supplied_by_caller_wins():
    assert column_value("price_exact", {"price": Decimal("1"), "price_exact": "1.000"}) == "1.000"


def test_schemas_declare_exact_companions_as_nullable_strings():
    for schema, names in ((TRADES_SCHEMA, ("price_exact", "quantity_exact")),
                          (ORDERBOOK_SCHEMA, ("bids_price_exact", "asks_qty_exact")),
                          (MARKPRICE_SCHEMA, ("mark_price_exact", "funding_rate_exact")),
                          (OPENINTEREST_SCHEMA, ("open_interest_exact",))):
        for name in names:
            field = schema.field(name)
            assert field.nullable
            assert pa.types.is_string(field.type) or pa.types.is_string(field.type.value_type)
    # the derived float64 columns are still present and unchanged in type
    assert pa.types.is_float64(TRADES_SCHEMA.field("price").type)


# ---------------------------------------------------------------------------
# 12. Replay: same exact value as live, never through float
# ---------------------------------------------------------------------------

def _wire(ts, payload):
    return ReplayFrame(timestamp_ms=ts, kind=FrameKind.WIRE, source_index=0, payload=payload)


def test_replayed_trade_carries_the_exact_decimal():
    payload = json.dumps({"stream": "btcusdt@aggTrade",
                          "data": {"e": "aggTrade", "E": 5, "T": 5, "a": BIG_ID, "p": WIDE_A, "q": QTY, "m": False}})
    result = ReplayEngine().run(ReplaySource([_wire(5, payload)]))
    event = result.non_book_events[0]
    assert event.price == Decimal(WIDE_A) and event.quantity == Decimal(QTY)
    assert event.trade_id == str(BIG_ID)


def test_live_and_replay_normalize_a_trade_identically():
    data = {"e": "aggTrade", "E": 5, "T": 5, "a": 1, "p": WIDE_A, "q": QTY, "m": False}
    live = BinanceAdapter().normalize({"stream": "btcusdt@aggTrade", "data": data}, local_receive_ts=5)[0]
    replayed = ReplayEngine().run(ReplaySource([_wire(5, json.dumps({"stream": "btcusdt@aggTrade", "data": data}))])).non_book_events[0]
    assert (live.price, live.quantity) == (replayed.price, replayed.quantity)


def test_replayed_open_interest_is_exact_and_unit_unknown():
    body = json.dumps({"openInterest": "123456.789012345", "time": 1})
    live = normalize_binance_oi(body, response_receive_ts=10, symbol="BTCUSDT")
    source = ReplaySource.from_records(rest_rows=[{"purpose": "open_interest", "response_receive_ts": 10,
                                                   "payload": body, "ok": True}])
    replayed = ReplayEngine().run(source).non_book_events[0]
    assert replayed.open_interest == Decimal("123456.789012345") == live.open_interest
    assert replayed.unit is OIUnit.UNKNOWN


# ---------------------------------------------------------------------------
# 10. Book keys are exact Decimals (distinct venue prices cannot merge)
# ---------------------------------------------------------------------------

def test_book_engine_keys_are_decimal_and_distinct_prices_do_not_merge():
    from collector.collector.canonical import CanonicalOrderBookEvent
    from collector.collector.book_engine import LocalBook
    book = LocalBook("BINANCE")
    snap = CanonicalOrderBookEvent(
        "BINANCE", "orderbook", None, None, 0,
        bids=((Decimal(WIDE_A), Decimal("1")), (Decimal(WIDE_B), Decimal("2"))),
        asks=((Decimal("100001"), Decimal("1")),), update_id=1, is_snapshot=True)
    book.snapshot(snap)
    assert len(book.bids) == 2
    assert all(isinstance(k, Decimal) for k in book.bids)


# ---------------------------------------------------------------------------
# 13. Research dataset round trip
# ---------------------------------------------------------------------------

def test_research_dataset_carries_exact_scalar_evidence_and_derived_floats(tmp_path):
    date_str = "2026-06-06"
    start = int(pd.Timestamp(f"{date_str} 00:00:00", tz="UTC").timestamp() * 1000)
    data_dir = str(tmp_path)
    for stream in ("orderbook", "markprice"):
        os.makedirs(os.path.join(data_dir, "raw", stream), exist_ok=True)

    ob = compute_orderbook_features({"E": start, "b": [["100.5", "1"], ["100.4", "1"]], "a": [["100.6", "1"], ["100.7", "1"]]})
    mk = compute_markprice_features({"E": start, "p": NEAR_TICK, "r": FUNDING, "T": start + 3_600_000})
    for stream, schema, rec in (("orderbook", ORDERBOOK_SCHEMA, ob), ("markprice", MARKPRICE_SCHEMA, mk)):
        rec = dict(rec, timestamp=start, exchange_timestamp=start, local_timestamp=start)
        w = ParquetWriter(stream, schema, base_dir=data_dir)
        w.write(rec)
        w.close()
    # the writer names segments by wall clock; move them to the assembler's date
    for stream in ("orderbook", "markprice"):
        for src in glob.glob(os.path.join(data_dir, "raw", stream, "*.seg")):
            os.replace(src, os.path.join(data_dir, "raw", stream, f"{date_str}-00-000000.seg"))

    assemble_dataset(date_str, grid_ms=100, data_dir=data_dir)
    df = pd.read_parquet(os.path.join(data_dir, "aligned", f"{date_str}.parquet"))
    assert df.loc[0, "mark_price_exact"] == NEAR_TICK
    assert df.loc[0, "funding_rate_exact"] == FUNDING
    assert df.loc[0, "mark_price"] == float(NEAR_TICK)          # derived float64
    assert "bids_price_exact" not in df.columns                 # raw depth arrays stay out of the dataset
    assert df.loc[0, "bid_depth"] == 2                          # P0-8 semantics intact


# ---------------------------------------------------------------------------
# Legacy data boundary: old segments lack the new nullable columns
# ---------------------------------------------------------------------------

from collector.tests.test_daily_compaction import DATE, SCHEMAS, _base_ms, _daily_parquet, _table  # noqa: E402
from collector.scripts import compact_daily as cd  # noqa: E402


def _legacy_hour(data_dir, stream, hour, *, drop):
    """An hourly segment exactly as an OLDER collector wrote it: the current
    schema minus the columns added since (so no ``*_exact``, no depth)."""
    ts = [_base_ms(hour) + i * 1000 for i in range(2)]
    table = _table(stream, ts, offset=hour)
    table = table.drop_columns([c for c in table.column_names if c in drop or c.endswith("_exact")])
    raw = data_dir / "raw" / stream
    raw.mkdir(parents=True, exist_ok=True)
    path = raw / f"{DATE}-{hour:02d}.parquet"
    pq.write_table(table, path, compression="snappy")
    old = __import__("time").time() - 3600
    os.utime(path, (old, old))


@pytest.mark.parametrize("stream,drop", [
    ("trades", ()), ("markprice", ()), ("openinterest", ()), ("liquidation", ()),
    ("orderbook", ("bid_depth", "ask_depth")),
])
def test_legacy_hourly_segments_still_compact_with_null_new_columns(tmp_path, stream, drop):
    for hour in range(24):
        _legacy_hour(tmp_path, stream, hour, drop=drop)
    assert cd.compact_daily(DATE, stream, tmp_path)
    table = pq.read_table(_daily_parquet(tmp_path, stream))
    exact_cols = [c for c in table.column_names if c.endswith("_exact")]
    assert exact_cols, "current schema should carry exact companions"
    for column in exact_cols + list(drop):
        # Absent in the legacy rows -> null. NEVER a value reconstructed from
        # the (possibly already lossy) float64 column.
        assert table.column(column).null_count == table.num_rows, column


def test_a_genuinely_required_column_is_still_a_hard_compaction_error(tmp_path):
    """The legacy allowance is narrow: only nullable columns added by later
    migrations may be absent. A missing core column (here the float64 price)
    must still fail loudly, not be filled with nulls."""
    for hour in range(24):
        _legacy_hour(tmp_path, "trades", hour, drop=("price",))
    with pytest.raises(cd.CompactionError, match="missing required column: price"):
        cd.compact_daily(DATE, "trades", tmp_path)
