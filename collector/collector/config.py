import pyarrow as pa

# Constants
SYMBOL = "BTCUSDT"

#: Bybit v5 public linear endpoint. Verified 2026-09-19 against
#: https://bybit-exchange.github.io/docs/v5/ws/connect (USDT/USDC perpetual
#: & USDT Futures -> wss://stream.bybit.com/v5/public/linear).
BYBIT_PUBLIC_WS_URL = "wss://stream.bybit.com/v5/public/linear"

#: Orderbook depth level to subscribe at. Valid linear depths, per
#: https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook,
#: are 1, 50, 200, 1000 (push frequency 10/20/100/200ms respectively).
#: 50 is chosen as an operational default -- deep enough for multi-level
#: imbalance features, far short of the 1000-level, 200ms-cadence table.
#: This is a configuration choice, not a protocol fact: any of the four
#: values is valid.
BYBIT_ORDERBOOK_DEPTH = 50
BINANCE_PUBLIC_WS_URL = "wss://fstream.binance.com/public/stream?streams=btcusdt@depth@100ms"
BINANCE_MARKET_WS_URL = "wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade/btcusdt@markPrice@1s/btcusdt@forceOrder"

# Binance Spot (P5) -- a distinct API surface from USD-M futures above.
BINANCE_SPOT_WS_URL = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@depth@100ms"
BINANCE_SPOT_DEPTH_SNAPSHOT_URL = "https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=1000"
# Intervals and Thresholds
ORDERBOOK_STALE_MS = 500
TRADES_STALE_MS = 5000   # was 30000
MARKPRICE_STALE_MS = 5000
OI_STALE_MS = 30000  # OI updates ~every 3s via REST or ~5s via stream
LIQUIDATION_STALE_MS = 1800000  # Liquidations are event-sparse; alert after 30 minutes silent.

# File Paths
DATA_DIR = "data"
LOGS_DIR = "logs"

# Schemas
# All timestamp columns: Unix epoch, milliseconds, UTC
ORDERBOOK_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("bids_price", pa.list_(pa.float64())),
    ("bids_qty", pa.list_(pa.float64())),
    ("asks_price", pa.list_(pa.float64())),
    ("asks_qty", pa.list_(pa.float64())),
    ("best_bid", pa.float64()),
    ("best_ask", pa.float64()),
    ("mid_price", pa.float64()),
    ("micro_price", pa.float64()),
    ("spread", pa.float64()),
    ("spread_bps", pa.float64()),
    ("total_bid_qty", pa.float64()),
    ("total_ask_qty", pa.float64()),
    ("obi", pa.float64()),
    ("obi_level_1", pa.float64()),
    ("obi_level_3", pa.float64()),
    ("obi_level_5", pa.float64()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "orderbook", "symbol": SYMBOL,
             "migration": "v1.1 adds nullable instrument_key (InstrumentId.key, e.g. "
                          "'BINANCE|linear_perpetual|BTC-USDT|BTCUSDT'); missing on rows "
                          "written before this version, never fabricated for them. This "
                          "stream is per-event resolved from BinanceAdapter.normalize()'s "
                          "own instrument stamp (see adapters/base.py __init_subclass__), "
                          "not a blind constant -- see instrument.py"})

TRADES_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("trade_id", pa.int64()),
    ("price", pa.float64()),
    ("quantity", pa.float64()),
    ("is_buyer_maker", pa.bool_()),
    ("side_sign", pa.int8()),
    ("signed_qty", pa.float64()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "trades", "symbol": SYMBOL,
             "migration": "v1.1 adds nullable instrument_key; see orderbook v1.1 note -- "
                          "same per-event BinanceAdapter.normalize() resolution"})

BINANCE_TRADES_RAW_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")), ("local_receive_ts", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")), ("trade_id", pa.int64()),
    ("native_trade_id", pa.string()), ("price", pa.float64()), ("quantity", pa.float64()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "2.1", "migration": "v2: native_trade_id is authoritative; legacy trade_id is nullable. "
                                                     "v2.1 adds nullable instrument_key; see orderbook v1.1 note",
             "stream_name": "binance_trades_raw", "symbol": SYMBOL})

MARKPRICE_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("mark_price", pa.float64()),
    ("funding_rate", pa.float64()),
    ("next_funding_time", pa.int64()),
    ("funding_rate_bps", pa.float64()),
    ("hours_to_funding", pa.float64()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "markprice", "symbol": SYMBOL,
             "migration": "v1.1 adds nullable instrument_key. This stream bypasses "
                          "BinanceAdapter (legacy raw-dict handler, see run_collector."
                          "_handle_markprice): the key is the validated BINANCE_USDM_BTCUSDT "
                          "constant, stamped only after the payload's own 's' symbol field "
                          "(when present) is checked against config.SYMBOL -- a mismatch is "
                          "rejected, never silently stamped"})

OPENINTEREST_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("open_interest", pa.float64()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "openinterest", "symbol": SYMBOL,
             "migration": "v1.1 adds nullable instrument_key, resolved in binance_oi."
                          "normalize_binance_oi from the REST response's own 'symbol' field "
                          "via instrument.resolve_instrument -- never fabricated for a "
                          "response whose symbol does not match what was requested"})

LIQUIDATION_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("side", pa.int8()),
    ("price", pa.float64()),
    ("quantity", pa.float64()),
    ("signed_qty", pa.float64()),
    ("order_status", pa.string()),
    ("time_in_force", pa.string()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "liquidation", "symbol": SYMBOL,
             "migration": "v1.1 adds nullable instrument_key. forceOrder is a symbol-scoped "
                          "subscription (btcusdt@forceOrder), unlike OKX's instType-scoped "
                          "liquidation-orders channel -- see run_collector._handle_liquidation "
                          "for the same payload-symbol contradiction check as markprice"})

# Bybit canonical schemas (Phase 8).
#
# Deliberately not the Binance-shaped ORDERBOOK_SCHEMA/TRADES_SCHEMA/etc.
# above, for two reasons:
#
# 1. Those schemas have no exchange column. Bybit rows would be silently
#    indistinguishable from Binance rows in the same table -- exactly what
#    "do not force incompatible exchange systems into one" forbids.
# 2. No derived microstructure features (obi, spread, micro_price, ...) are
#    computed here. `feature_computer.compute_orderbook_features` reads raw
#    Binance-message keys directly (`msg.get("E", ...)` for exchange
#    timestamp, no persistent-book state, only the levels present in one
#    message) -- reusing it on Bybit's differently-shaped raw payload would
#    silently produce wrong or locally-substituted values rather than fail.
#    Feature computation belongs in its own verified, causal, cross-venue
#    phase, not bolted onto ingestion wiring for one exchange.
#
# Own stream names, not shared ones: segment sequence numbers, `.tmp` files
# and orphan recovery are all scoped to a stream directory, so two writers on
# one stream name share all three (Binance, Bybit and OKX are separate runner
# processes). Sequence allocation is scan-then-create with nothing reserved in
# between, so both pick the same number and open the same `.tmp` path. The
# publish-time `FileExistsError` guard does NOT make that safe: it fires only
# after the damage, and the second writer's orphan recovery deletes the first
# writer's live segment and reports it as a crash. PR #13's OKX capture reused
# "raw_wire"/"quality_events" with Binance's names.
#
# Resolved in the storage-namespace phase: every venue's stream names come
# from `storage_layout.venue_stream`, `ParquetWriter` refuses a stream that
# contradicts its declared venue, and it holds a single-writer lock per stream
# directory. See docs/STORAGE_NAMESPACES.md. Every Bybit stream below is named
# uniquely.
BYBIT_ORDERBOOK_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("bids_price", pa.list_(pa.float64())),
    ("bids_qty", pa.list_(pa.float64())),
    ("asks_price", pa.list_(pa.float64())),
    ("asks_qty", pa.list_(pa.float64())),
    ("update_id", pa.int64()),
    ("sequence", pa.int64()),
    ("is_snapshot", pa.bool_()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "bybit_orderbook", "symbol": SYMBOL,
             "migration": "v1.1 adds nullable instrument_key (InstrumentId.key, e.g. "
                          "'BYBIT|linear_perpetual|BTC-USDT|BTCUSDT'); missing on rows "
                          "written before this version, never fabricated for them"})

BYBIT_TRADES_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("trade_id", pa.string()),
    ("price", pa.float64()),
    ("quantity", pa.float64()),
    ("side", pa.string()),
    ("venue_sequence", pa.int64()),
    ("block_trade", pa.bool_()),
    ("rpi", pa.bool_()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "bybit_trades", "symbol": SYMBOL,
             "migration": "v1.1 adds nullable instrument_key; see bybit_orderbook v1.1 note"})

BYBIT_MARKPRICE_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("mark_price", pa.float64()),
    ("index_price", pa.float64()),
    ("funding_rate", pa.float64()),
    ("next_funding_time", pa.int64()),
    ("carried_forward", pa.list_(pa.string())),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "bybit_markprice", "symbol": SYMBOL,
             "migration": "v1.1 adds nullable instrument_key; see bybit_orderbook v1.1 note"})

BYBIT_OPENINTEREST_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("open_interest", pa.float64()),
    # Unit of open_interest (OIUnit value). UNKNOWN for Bybit: see the adapter.
    ("oi_unit", pa.string()),
    ("carried_forward", pa.list_(pa.string())),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.2", "stream_name": "bybit_openinterest", "symbol": SYMBOL,
             "note": "1.1 adds oi_unit; open_interest unit is UNKNOWN and must not be compared across venues",
             "migration": "v1.2 adds nullable instrument_key; see bybit_orderbook v1.1 note "
                          "(this schema was already at 1.1, so this is 1.1 -> 1.2, not 1.0 -> 1.1)"})

BYBIT_LIQUIDATION_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("side", pa.string()),
    ("price", pa.float64()),
    ("quantity", pa.float64()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "bybit_liquidation", "symbol": SYMBOL,
             "migration": "v1.1 adds nullable instrument_key; see bybit_orderbook v1.1 note"})

BINANCE_ORDERBOOK_RAW_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),  # Canonical local processing timestamp.
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_receive_ts", pa.timestamp("ms", tz="UTC")), ("local_process_ts", pa.timestamp("ms", tz="UTC")),
    ("bids", pa.list_(pa.list_(pa.string()))), ("asks", pa.list_(pa.list_(pa.string()))),
    ("update_id", pa.int64()), ("first_update_id", pa.int64()), ("previous_update_id", pa.int64()),
    ("book_source", pa.string()), ("event_kind", pa.string()), ("recovery_generation", pa.int64()), ("quality_state", pa.string()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "2.1", "migration": "v2: book levels are canonical decimal strings; timestamp is canonical local event "
                                                     "processing timestamp. v2.1 adds nullable instrument_key; see orderbook v1.1 note -- "
                                                     "every row here is a committed book event carried through from an adapter-normalized "
                                                     "diff, so it carries the same per-event instrument stamp",
             "stream_name": "binance_orderbook_raw", "symbol": SYMBOL})

QUALITY_EVENTS_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange", pa.string()),
    ("stream", pa.string()),
    ("event_type", pa.string()),
    ("reason", pa.string()),
    ("gap_size_ms", pa.int64()),
    ("rows_lost", pa.string()),
    ("quality_state", pa.string()),
    ("connection_id", pa.string()),
    ("previous_state", pa.string()), ("new_state", pa.string()),
    ("expected_previous_update_id", pa.int64()), ("actual_previous_update_id", pa.int64()),
    ("update_id", pa.int64()), ("first_update_id", pa.int64()), ("previous_update_id", pa.int64()), ("local_receive_ts", pa.timestamp("ms", tz="UTC")),
    ("local_process_ts", pa.timestamp("ms", tz="UTC")),
], metadata={"schema_version": "1.1", "stream_name": "quality_events", "symbol": SYMBOL})

# Raw wire capture (Phase 2). Defined in collector.collector.raw_capture so the
# capture contract lives beside the records it describes; re-exported here so
# every stream schema remains discoverable from one module.
from .raw_capture import RAW_REST_SCHEMA, RAW_WIRE_SCHEMA  # noqa: E402,F401

# OKX D11 canonical schemas (this phase). Same reasoning as the Bybit block
# above: no exchange column (own okx_-prefixed streams via storage_layout's
# venue namespace, so cross-venue confusion is a storage-layer refusal, not a
# schema concern), and separate from Binance/Bybit shapes wherever the
# semantics actually differ (funding's current/next/settled split, OI's
# three units, liquidation's bkLoss/ccy/posSide) rather than force-fit.
# Fields with no slot here are not lost -- ``okx_raw_wire`` is the lossless
# copy; see collector/collector/adapters/okx.py's module docstring.
OKX_TRADES_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("trade_id", pa.string()),
    ("price", pa.float64()),
    ("quantity", pa.float64()),
    ("side", pa.string()),
    ("venue_sequence", pa.int64()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "okx_trades", "symbol": SYMBOL,
             "migration": "v1.1 adds nullable instrument_key (OKX_SWAP_BTCUSDT.key); this channel is always instId-scoped, so a null here means legacy row, never a genuine unidentified trade"})

OKX_TRADES_ALL_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("trade_id", pa.string()),
    ("price", pa.float64()),
    ("quantity", pa.float64()),
    ("side", pa.string()),
    ("venue_sequence", pa.int64()),
    ("source", pa.string()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "okx_trades_all", "symbol": SYMBOL,
             "note": "Distinct channel from okx_trades -- see docs/OKX_D11_CHANNEL_SCHEMAS.md",
             "migration": "v1.1 adds nullable instrument_key; see okx_trades v1.1 note"})

OKX_MARKPRICE_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("mark_price", pa.float64()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "okx_markprice", "symbol": SYMBOL,
             "note": "mark-price channel only; never populated from index-tickers or funding-rate",
             "migration": "v1.1 adds nullable instrument_key; see okx_trades v1.1 note"})

OKX_INDEXTICKERS_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("index_price", pa.float64()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "okx_indextickers", "symbol": SYMBOL,
             "note": "instrument_key is ALWAYS null here, deliberately: this channel is keyed by the index pair, not the swap instrument (OKX_D11 open question #3) -- a value here would be a fabricated identity, not a resolved one"})

OKX_FUNDINGRATE_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("funding_rate", pa.float64()),
    ("funding_time", pa.int64()),
    ("next_funding_rate", pa.float64()),
    ("next_funding_time", pa.int64()),
    ("sett_funding_rate", pa.float64()),
    ("sett_state", pa.string()),
    ("premium", pa.float64()),
    ("interest_rate", pa.float64()),
    ("max_funding_rate", pa.float64()),
    ("min_funding_rate", pa.float64()),
    ("formula_type", pa.string()),
    ("method", pa.string()),
    ("impact_value", pa.float64()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "okx_fundingrate", "symbol": SYMBOL,
             "note": "current(funding_rate/funding_time), next, and settled are three distinct observations, never merged",
             "migration": "v1.1 adds nullable instrument_key; see okx_trades v1.1 note"})

OKX_OPENINTEREST_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("open_interest", pa.float64()),
    ("oi_ccy", pa.float64()),
    ("oi_usd", pa.float64()),
    # Unit of open_interest (OIUnit value): CONTRACTS for OKX.
    ("oi_unit", pa.string()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.2", "stream_name": "okx_openinterest", "symbol": SYMBOL,
             "note": "open_interest is contracts (oi field); oi_ccy (base currency)/oi_usd preserved alongside, not merged; 1.1 adds oi_unit",
             "migration": "v1.2 adds nullable instrument_key (already at 1.1, so 1.1 -> 1.2, not 1.0 -> 1.1); see okx_trades v1.1 note"})

OKX_LIQUIDATION_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("inst_id", pa.string()),
    ("side", pa.string()),
    ("price", pa.float64()),
    ("quantity", pa.float64()),
    ("bk_loss", pa.float64()),
    ("ccy", pa.string()),
    ("pos_side", pa.string()),
    ("inst_family", pa.string()),
    ("uly", pa.string()),
    ("instrument_key", pa.string()),
], metadata={"schema_version": "1.1", "stream_name": "okx_liquidation", "symbol": SYMBOL,
             "note": "subscription is instType-scoped; inst_id column lets downstream filter to BTC-USDT-SWAP",
             "migration": "v1.1 adds instrument_key, resolved per-row from inst_id: BTC-USDT-SWAP rows "
                          "get OKX_SWAP_BTCUSDT.key, every other instrument's rows get null -- inst_id "
                          "remains the raw column (never removed), instrument_key is the resolved one; "
                          "a null here is a genuine other-instrument liquidation, not a legacy row, since "
                          "this stream is multi-instrument by design"})


# Binance Spot canonical schemas (P5). Own spot_-prefixed streams via
# storage_layout.venue_stream("BINANCE_SPOT", ...) -- never USD-M futures'
# unprefixed "orderbook"/"trades". Mirrors BINANCE_ORDERBOOK_RAW_SCHEMA's
# raw-canonical shape (decimal-string levels, no derived features) rather
# than the older feature-computed ORDERBOOK_SCHEMA, since Spot has no
# feature_computer path yet and inventing one is out of P5's scope.
SPOT_ORDERBOOK_RAW_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_receive_ts", pa.timestamp("ms", tz="UTC")), ("local_process_ts", pa.timestamp("ms", tz="UTC")),
    ("bids", pa.list_(pa.list_(pa.string()))), ("asks", pa.list_(pa.list_(pa.string()))),
    ("update_id", pa.int64()), ("first_update_id", pa.int64()),
    # Always NULL for Spot -- the official depthUpdate payload has no `pu`
    # field (see adapters/binance_spot.py); the column is kept for schema
    # symmetry with BINANCE_ORDERBOOK_RAW_SCHEMA, not because Spot has one.
    ("previous_update_id", pa.int64()),
    ("book_source", pa.string()), ("event_kind", pa.string()),
    ("recovery_generation", pa.int64()), ("quality_state", pa.string()),
], metadata={"schema_version": "1.0", "stream_name": "spot_orderbook_raw", "symbol": SYMBOL,
             "note": "previous_update_id always NULL: Spot depthUpdate has no pu field"})

SPOT_TRADES_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("ms", tz="UTC")),
    ("exchange_timestamp", pa.timestamp("ms", tz="UTC")),
    ("local_timestamp", pa.timestamp("ms", tz="UTC")),
    ("trade_id", pa.string()),
    ("price", pa.float64()),
    ("quantity", pa.float64()),
    ("side", pa.string()),
], metadata={"schema_version": "1.0", "stream_name": "spot_trades", "symbol": SYMBOL,
             "note": "trade_id is the raw per-execution `t`, never aggTrade's `a` -- see adapters/binance_spot.py"})
