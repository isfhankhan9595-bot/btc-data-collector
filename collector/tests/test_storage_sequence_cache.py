from collector.collector.parquet_writer import ParquetWriter


def test_next_sequence_is_hour_scoped_and_cached(tmp_path, monkeypatch):
    raw = tmp_path / "raw" / "trades"
    raw.mkdir(parents=True)
    (raw / "2026-06-03-01-000001.seg").touch()
    (raw / "2026-06-03-01-000002.seg").touch()
    (raw / "2026-06-03-02-000099.seg").touch()

    writer = object.__new__(ParquetWriter)
    writer.stream_name = "trades"
    writer.base_dir = str(tmp_path)
    writer._sequence_cache = {}

    assert writer._next_sequence("2026-06-03-01") == 3
    assert writer._sequence_cache["2026-06-03-01"] == 3

    calls = 0

    def fail_if_scanned(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise AssertionError("sequence cache was not used")

    monkeypatch.setattr("collector.collector.parquet_writer.iter_segments", fail_if_scanned)
    assert writer._next_sequence("2026-06-03-01") == 3
    assert calls == 0

    # A different logical hour must not reuse the first hour's cached sequence.
    assert writer._next_sequence("2026-06-03-02") == 100
    assert writer._sequence_cache["2026-06-03-02"] == 100
