import pandas as pd

from collector.pipeline.dataset_assembler import _read_stream


def test_assembler_does_not_double_count_on_migration_hour(tmp_path):
    raw = tmp_path / "raw" / "trades"
    raw.mkdir(parents=True)
    legacy = raw / "2026-06-03-01.parquet"
    segment = raw / "2026-06-03-01-000001.seg"
    frame = pd.DataFrame({"timestamp": [1], "trade_id": [1], "quantity": [7.0]})
    frame.to_parquet(legacy)
    frame.to_parquet(segment)

    frames = _read_stream(str(tmp_path), "trades", "2026-06-03")

    assert len(frames) == 1
    assert float(frames[0]["quantity"].sum()) == 7.0
