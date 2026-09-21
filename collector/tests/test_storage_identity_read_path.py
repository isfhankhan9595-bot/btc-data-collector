"""Phase D -- canonical storage read-path identity contract.

The contract (see ``instrument.resolve_canonical_instrument_key``):

    LEGACY / MISSING column -> compatibility path (no identity, no error)
    explicit NULL           -> unidentified where the schema allows it
    VALID                   -> resolves to the exact InstrumentId
    MALFORMED               -> explicit error
    CONTRADICTORY           -> explicit error

Nothing here mocks storage: rows go through the real ``ParquetWriter`` and the
real ``compact_daily``, and are read back through both readers this repository
uses (pyarrow and pandas -- which disagree on how a null string is spelled).
"""
import datetime as dt
import itertools
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from collector.collector.config import ORDERBOOK_SCHEMA
from collector.collector.instrument import (
    BINANCE_SPOT_BTCUSDT,
    BINANCE_USDM_BTCUSDT,
    BYBIT_LINEAR_BTCUSDT,
    OKX_SWAP_BTCUSDT,
    SUPPORTED_INSTRUMENTS,
    InstrumentId,
    InstrumentIdError,
    resolve_canonical_instrument_key,
)
from collector.collector.parquet_writer import ParquetWriter
from collector.collector.storage_layout import iter_segments
from collector.scripts import compact_daily as cd
from collector.tests.test_daily_compaction import DATE, _daily_parquet, _write_hour

USDM = BINANCE_USDM_BTCUSDT


def _resolve(row, expected=USDM):
    return resolve_canonical_instrument_key(row, expected=expected)


def _record(**overrides):
    """One schema-valid orderbook record, built from the schema's own types."""
    values = {}
    for field in ORDERBOOK_SCHEMA:
        if pa.types.is_timestamp(field.type):
            values[field.name] = dt.datetime(2026, 6, 10, 0, 0, 1, tzinfo=dt.UTC)
        elif pa.types.is_list(field.type):
            values[field.name] = [1.0]
        elif pa.types.is_boolean(field.type):
            values[field.name] = False
        elif pa.types.is_string(field.type):
            values[field.name] = "x"
        elif pa.types.is_floating(field.type):
            values[field.name] = 1.0
        else:
            values[field.name] = 1
    values.update(overrides)
    return values


def _both_readers(path):
    """The same file as rows, via pyarrow (null -> None) and pandas (null -> NaN)."""
    return {
        "pyarrow": pq.read_table(path).to_pylist(),
        "pandas": pd.read_parquet(path).to_dict("records"),
    }


def _drop_column(path, column):
    pq.write_table(pq.read_table(path).drop_columns([column]), path)


# --------------------------------------------------------------------------
# Resolver contract: the five states, and identity separation
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "row",
    [
        pytest.param({}, id="legacy-column-absent"),
        pytest.param({"instrument_key": None}, id="null-pyarrow"),
        pytest.param({"instrument_key": float("nan")}, id="null-pandas-nan"),
        pytest.param({"instrument_key": pd.NA}, id="null-pandas-NA"),
    ],
)
def test_missing_or_null_key_is_unidentified_never_fabricated(row):
    assert _resolve(row) is None


def test_valid_key_resolves_to_the_complete_identity():
    got = _resolve({"instrument_key": USDM.key})
    assert got == USDM
    # Compare every component, not just the string form.
    assert (got.exchange, got.market_type, got.instrument, got.native_symbol) == (
        "BINANCE", "linear_perpetual", "BTC-USDT", "BTCUSDT")


@pytest.mark.parametrize(
    "bad",
    ["", "nope", "binance|linear_perpetual|BTC-USDT|BTCUSDT", USDM.key + " ",
     "BINANCE|futures|BTC-USDT|BTCUSDT", "A|B|C", 5, 1.5, ["BINANCE"], b"BINANCE"],
)
def test_malformed_key_is_an_explicit_typed_error(bad):
    with pytest.raises(InstrumentIdError):
        _resolve({"instrument_key": bad})


@pytest.mark.parametrize(
    "expected,stored",
    list(itertools.permutations(SUPPORTED_INSTRUMENTS, 2)),
    ids=lambda i: getattr(i, "key", str(i)),
)
def test_valid_key_of_another_instrument_is_contradictory(expected, stored):
    """All 12 ordered pairs: Spot vs USD-M, Bybit vs Binance, OKX vs the rest."""
    with pytest.raises(InstrumentIdError, match="contradicts"):
        _resolve({"instrument_key": stored.key}, expected=expected)


def test_same_native_symbol_never_collides_across_venues_and_markets():
    same_native = [i for i in SUPPORTED_INSTRUMENTS if i.native_symbol == "BTCUSDT"]
    assert set(same_native) == {BINANCE_SPOT_BTCUSDT, BINANCE_USDM_BTCUSDT, BYBIT_LINEAR_BTCUSDT}
    assert len({i.key for i in SUPPORTED_INSTRUMENTS}) == len(SUPPORTED_INSTRUMENTS) == 4
    assert OKX_SWAP_BTCUSDT.native_symbol == "BTC-USDT-SWAP"
    for ident in SUPPORTED_INSTRUMENTS:
        assert InstrumentId.from_key(ident.key) == ident


def test_okx_unidentified_row_stays_unidentified_in_every_null_spelling():
    """Cross-instrument OKX liquidation rows carry a genuine null: it is not corruption."""
    for null in (None, float("nan"), pd.NA):
        assert _resolve({"instrument_key": null}, expected=OKX_SWAP_BTCUSDT) is None
    assert _resolve({"instrument_key": OKX_SWAP_BTCUSDT.key}, expected=OKX_SWAP_BTCUSDT) == OKX_SWAP_BTCUSDT


# --------------------------------------------------------------------------
# Real Parquet: write -> read -> resolve
# --------------------------------------------------------------------------


def _write_segment(tmp_path, records):
    writer = ParquetWriter("orderbook", ORDERBOOK_SCHEMA, base_dir=str(tmp_path))
    for record in records:
        writer.write(record)
    writer.close()
    (path,) = list(iter_segments(str(tmp_path), "orderbook"))
    return path


def test_writer_to_reader_roundtrip_preserves_identity_through_both_readers(tmp_path):
    path = _write_segment(tmp_path, [
        _record(instrument_key=USDM.key),
        _record(instrument_key=None),
        _record(instrument_key=USDM.key),
    ])
    for reader, rows in _both_readers(path).items():
        got = [_resolve(row) for row in rows]
        assert got == [USDM, None, USDM], reader


def test_writer_omitting_the_key_stores_null_not_a_default_identity(tmp_path):
    record = _record()
    del record["instrument_key"]
    path = _write_segment(tmp_path, [record])
    for reader, rows in _both_readers(path).items():
        assert _resolve(rows[0]) is None, reader


def test_legacy_file_without_the_column_reads_with_no_identity(tmp_path):
    path = _write_segment(tmp_path, [_record(instrument_key=USDM.key)])
    _drop_column(path, "instrument_key")
    for reader, rows in _both_readers(path).items():
        assert "instrument_key" not in rows[0], reader
        assert _resolve(rows[0]) is None, reader


@pytest.mark.parametrize("stored", ["nope", BINANCE_SPOT_BTCUSDT.key, BYBIT_LINEAR_BTCUSDT.key])
def test_corrupt_identity_written_to_parquet_fails_explicitly_on_read(tmp_path, stored):
    path = _write_segment(tmp_path, [_record(instrument_key=USDM.key), _record(instrument_key=stored)])
    for reader, rows in _both_readers(path).items():
        assert _resolve(rows[0]) == USDM, reader
        with pytest.raises(InstrumentIdError):
            _resolve(rows[1])


# --------------------------------------------------------------------------
# Compaction: the repository's canonical Parquet read path
# --------------------------------------------------------------------------


def _compact(tmp_path):
    assert cd.compact_daily(DATE, "orderbook", tmp_path, guard_seconds=0) is True
    return pq.read_table(_daily_parquet(tmp_path, "orderbook"))


def _write_legacy_hour(tmp_path, hour):
    _write_hour(tmp_path, "orderbook", hour)
    _drop_column(tmp_path / "raw" / "orderbook" / f"{DATE}-{hour:02d}.parquet", "instrument_key")


def test_compaction_accepts_a_legacy_day_and_fabricates_no_identity(tmp_path):
    _write_legacy_hour(tmp_path, 0)
    daily = _compact(tmp_path)
    assert daily.schema.names == ORDERBOOK_SCHEMA.names
    assert daily.num_rows == 2
    assert daily.column("instrument_key").null_count == daily.num_rows


def test_compaction_accepts_explicit_null_keys(tmp_path):
    _write_hour(tmp_path, "orderbook", 0, overrides={"instrument_key": [None, None]})
    daily = _compact(tmp_path)
    assert daily.column("instrument_key").null_count == 2


def test_compaction_of_a_mixed_day_keeps_each_row_own_identity_state(tmp_path):
    """Deploy-boundary day: hour 0 written before the column existed, hour 1 after."""
    _write_legacy_hour(tmp_path, 0)
    _write_hour(tmp_path, "orderbook", 1)
    keys = _compact(tmp_path).column("instrument_key").to_pylist()
    assert keys == [None, None, USDM.key, USDM.key]


@pytest.mark.parametrize("bad", ["nope", BINANCE_SPOT_BTCUSDT.key, BYBIT_LINEAR_BTCUSDT.key, OKX_SWAP_BTCUSDT.key])
def test_compaction_rejects_malformed_and_contradictory_keys_even_among_nulls(tmp_path, bad):
    _write_hour(tmp_path, "orderbook", 0, overrides={"instrument_key": [USDM.key, None]})
    _write_hour(tmp_path, "orderbook", 1, overrides={"instrument_key": [None, bad]})
    with pytest.raises(cd.CompactionError, match="instrument_key"):
        cd.compact_daily(DATE, "orderbook", tmp_path, guard_seconds=0)
    assert not _daily_parquet(tmp_path, "orderbook").exists()


def test_legacy_allowance_is_only_for_instrument_key(tmp_path):
    """Widening the compatibility path to other columns would hide real corruption."""
    _write_hour(tmp_path, "orderbook", 0)
    _drop_column(tmp_path / "raw" / "orderbook" / f"{DATE}-00.parquet", "best_bid")
    with pytest.raises(cd.CompactionError, match="missing required column: best_bid"):
        cd.compact_daily(DATE, "orderbook", tmp_path, guard_seconds=0)


def test_compaction_and_write_paths_agree_on_the_stream_identity():
    assert cd.STREAM_INSTRUMENT == BINANCE_USDM_BTCUSDT
    assert set(cd.STREAM_SCHEMAS) == {"orderbook", "trades", "markprice", "openinterest", "liquidation"}
    assert all("instrument_key" in schema.names for schema in cd.STREAM_SCHEMAS.values())
