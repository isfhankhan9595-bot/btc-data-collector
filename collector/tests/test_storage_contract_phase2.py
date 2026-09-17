from collector.scripts.compact_daily import _discover_hourly_files


def test_compact_daily_prefers_segments_over_legacy(tmp_path):
    raw = tmp_path / "raw" / "trades"
    raw.mkdir(parents=True)
    legacy = raw / "2026-06-03-01.parquet"
    segment = raw / "2026-06-03-01-000001.seg"
    legacy.touch()
    segment.touch()

    discovered = _discover_hourly_files(tmp_path, raw, "2026-06-03", guard_seconds=0)

    assert discovered.present_files == [segment]
    assert legacy not in discovered.present_files


def test_iter_segments_hour_scope_avoids_unrelated_hours(tmp_path):
    raw = tmp_path / "raw" / "trades"
    raw.mkdir(parents=True)
    wanted = raw / "2026-06-03-01-000001.seg"
    unrelated = raw / "2026-06-03-02-000001.seg"
    wanted.touch()
    unrelated.touch()

    from collector.collector.storage_layout import iter_segments

    assert list(iter_segments(tmp_path, "trades", date="2026-06-03", hour=1)) == [wanted]
