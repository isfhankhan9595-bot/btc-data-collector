# F1 — Durable publication boundary

**Problem (P0).** A visible `*.seg` is not evidence that its rename is durable: the directory
entry stays in the page cache until a *successful* `fsync` of the parent directory. Startup
reconciliation used to treat "a file named `*.seg` exists" as dedup authority. A crash between
`os.replace` and a completed directory fsync could therefore leave an indexed identity whose
segment later disappeared after power loss, so the exchange's redelivery was suppressed forever
(silent, permanent loss); and because `reconciled_segments` was keyed by file *name* only, a
reused sequence number then became a silent no-op.

**Invariant.** `seen(K)` ⇒ a confirmed segment `S` contains `K` **and** the index evidence for `S`
equals `S`'s publication marker (sha256 + size). Unmarked, invalid or mismatching ⇒ not authority.

## Publication marker v2 (`<segment>.meta.json`)

`<seg>.meta.json` is no longer an optional hint. Deterministic JSON (`sort_keys`, compact
separators, one trailing newline); legacy top-level keys are preserved:

```json
{"first_record_ts":1,"last_record_ts":1,"publication":{"boot_id":null,"confirmed_at_utc":"2026-10-07T12:34:56Z","confirmed_by":"writer","legacy":false,"schema":2,"segment":"2026-10-07-12-000003.seg","sha256":"<64 lowercase hex>","size_bytes":123456,"state":"PUBLICATION_CONFIRMED","writer_schema":"parquet_writer/2"},"record_count":5000}
```

Valid ⇔ JSON object, `publication.schema == 2`, `state == PUBLICATION_CONFIRMED`, `segment`
equals the actual basename, `size_bytes` int ≥ 0, `sha256` 64-char lowercase hex, `confirmed_by` ∈
{`writer`,`startup_refsync`}, `legacy` bool, `record_count` int ≥ 0. At classification time the
file's real size must equal `size_bytes`; at index time its sha256 and decoded row count must also
match. A pre-F1 meta (no `publication` key) is "no marker". `boot_id` and `confirmed_at_utc` are
provenance only, never trust conditions.

## Exact writer order

`flush → pyarrow close → fsync(tmp) → sha256+size of tmp → os.replace(tmp, seg) → fsync(dir)`
(**segment durable**) `→ unlink .count.json → on_segment_durable → write marker tmp → fsync(marker
tmp) → os.replace(marker) → fsync(dir)` (**marker durable**) `→ on_segment_published` (dedup commit).

Marker visibility implies the segment rename is durable (it is created only after a successful
directory fsync; journal commits are ordered). The normal path adds **no** fsync over the pre-F1
sequence (two directory fsyncs, one marker-tmp fsync); the new cost is one SHA-256 per segment.

| Failure | Segment | Behaviour |
|---|---|---|
| before rename (flush, close, tmp fsync, digest) | not published | writer FAILED, `.tmp` kept, `DATA_DROP` accounting unchanged |
| rename ok, directory fsync fails | visible, **unconfirmed** | writer FAILED, no marker, no hook, nothing indexed, `.seg` left in place |
| marker write / fsync / replace / dir-fsync fails | durable | **no hook**, writer latched (`_publication_failure`), `STORAGE_METADATA_FAILED` (+ `DEDUP_STATE_FAILED` if a hook is wired), **no `DATA_DROP`**; restart confirms and indexes |
| dedup hook / SQLite fails | durable, marker valid | writer latched, `DEDUP_STATE_FAILED`; restart re-indexes |

This **changes** the previous contract "metadata sidecar failure leaves the writer healthy": a
marker failure now latches every `ParquetWriter`, hooked or not.

## State machine (derived from disk, never from memory)

| State | Evidence | Trusted | Indexable | Reader-visible | Restart action | Redelivery |
|---|---|---|---|---|---|---|
| before tmp fsync | `X.seg.tmp`, maybe partial | no | no | no | orphan recovery deletes tmp, `DATA_DROP` | accepted |
| after tmp fsync | `X.seg.tmp` complete | no | no | no | same | accepted |
| after rename, before dir fsync | `X.seg`, no marker | **no** | no | yes | startup refsync → marker → index | accepted until indexed (not ingesting) |
| after dir fsync, before marker | `X.seg`, no marker (maybe stray `.meta.json.tmp`) | no | no | yes | tmp discarded; refsync → marker → index | accepted until indexed |
| marker confirmed | `X.seg` + valid marker | yes | yes | yes | index if not reconciled | accepted until indexed |
| after SQLite commit | + index row with equal evidence | yes | already | yes | no-op | **suppressed** |
| SQLite txn interrupted | marker, no row | yes | yes | yes | re-index | accepted until re-indexed |
| corrupt marker | non-JSON / wrong fields | no | no | yes | preserved as `.meta.json.invalid.N`; re-confirm | as unmarked |
| missing segment | marker, no `.seg` | no | no | n/a | renamed `.meta.json.orphan.<utc>`; index **rebuilt** if it held the segment | accepted |
| sequence reuse | stale row + new bytes | no | — | yes | `EVIDENCE_CONFLICT` / rebuild | accepted |
| legacy segment | `.seg`, v1 or no meta | no | no until promoted | yes | refsync → marker `legacy:true` → index | as unmarked |
| size/sha mismatch | valid marker, bytes differ | no | **never** | yes | `DedupStateError`, evidence untouched | — |

## Startup reconciliation (`SegmentDedupCoordinator.startup_reconcile`)

Runs before ingestion; deterministic and idempotent.
1. Filesystem guard on the stream directory. 2. Scan `*.seg`, read markers; classify. 3. Invalid
markers are preserved (`.invalid.N`), never deleted. 4. Unmarked set `U`: `fsync(file)` each →
`fsync(dir)` once → read, sha256, size, full parquet parse and footer rows for **all** (no writes
yet) → write marker v2 for each (`confirmed_by=startup_refsync`) → `fsync(dir)` once. Any failure
raises `DedupStateError`; nothing is indexed and no marker precedes the first successful directory
fsync. 5. Dangling markers are renamed `.orphan.<utc>`. 6. Audit the index (C1, below) and rebuild
on any divergence. 7. Index every confirmed, unreconciled segment from the exact bytes whose
sha256/size match the marker; an identity already owned by another segment raises
`CROSS_SEGMENT_DUPLICATE`. `DEDUP_DEEP_VERIFY=1` additionally re-derives every segment's identity
set from its bytes (O(dataset); off by default).

A crash at any step re-runs safely. A previously failed journal commit cannot be laundered into a
successful directory fsync on ext4/xfs, so a successful startup directory fsync after `X.seg` is
visible proves its rename durable. It does **not** prove data durability (a re-read hits the page
cache): that rests on program order — `fsync(tmp)` strictly preceded the rename in every writer
version since `dd9c6be8`.

## Dedup index v3 and the identity audit (C1)

Chain of evidence: segment bytes (authority) → marker (sha256, size) → **identity evidence file**
`<index stem>.identity_evidence/<segment>.ids.json` (distinct identity count + order-independent
digest = sha256 over the byte-sorted, length-framed distinct keys; bound to the marker's
sha256/size and to the identity-key encoding; a pure function of the segment bytes) → SQLite.

Schema (`user_version=3`): `reconciled_segments(segment_id, segment_key, identity_count,
identity_digest, identity_encoding, evidence_sha256, evidence_size, confirmed_by)` and
`seen(identity_key, segment_id)` clustered on `(identity_key, segment_id)` — every identity row is
owned by the segment it was read from. `commit_segment` is one transaction; equal evidence and
identity set → no-op; different evidence → `EVIDENCE_CONFLICT`; same evidence, different set →
`IDENTITY_CONFLICT`; an identity owned by another segment → `CROSS_SEGMENT_DUPLICATE`.

Startup audit (every start, no segment bytes read): per confirmed segment, marker = row evidence,
row encoding = current, row (count, digest) = evidence file; then ONE pass over `seen` recomputes each
segment's (count, digest) and must equal the evidence file; unknown owners, segments with no rows
though the evidence says otherwise, and identities owned by several segments are divergence.
Missing/stale/unreadable evidence files are re-derived from the segment bytes first (old file
preserved `.invalid.N`). Divergence ⇒ the index is rebuilt in one transaction from the bytes.
Identity-key encoding drift is caught by a version constant **and** a canary computed through the live
`dedup_identity_key`. Old indexes (`user_version < 3` or a different table shape, including the
F1 `user_version=2` index) are never trusted: they are dropped and rebuilt once, atomically. A NEWER
`user_version` (> 3) is the opposite case and is **never** rebuilt or downgraded: startup raises
`DedupStateError(UNSUPPORTED_FUTURE_SCHEMA)` before any marker is renamed or written, and
`rebuild_reset` refuses it too. A persisted `identity_key` whose SQLite storage class is not TEXT
(`contains()` compares TEXT and would never match it) is divergence, not normalised: the storage type is
folded into the membership digest.

Duplicate semantics (unchanged by the rehearsal tooling): an identity repeated **inside one segment**
is recorded once (the segment bytes keep every row; nothing is deleted or collapsed). The same identity
in **two or more segments** is `CROSS_SEGMENT_DUPLICATE`: indexing and startup fail closed, no winner is
chosen and no history is deduplicated; the rehearsal tool reports such identities (and
`cross_only_extra_rows`) so the operator decides before the real run.

Trust level: the evidence file is stored separately from SQLite (so losing or distrusting the index does
not lose it), but it is derived from the same bytes by the same identity code — storage independence, not
independent derivation: it catches SQLite loss/corruption/tampering, not a bug in identity derivation that
both would share. It is trusted like the marker — both are re-checked against the bytes only
by `DEDUP_DEEP_VERIFY=1`. A forgery of evidence **and** SQLite together is invisible to the default
audit and caught by deep verify. Evidence files are not fsynced (derived; loss costs one segment read).

## Legacy migration

Existing segments without a valid v2 marker are **not** grandfathered: they are verified by the
same refsync protocol and marked `confirmed_by=startup_refsync`, `legacy=true` (a post-F1 crash
window and a pre-F1 segment are indistinguishable on disk, so `legacy=true` means only "no valid v2
marker existed"). A consistent v1 hint's timestamps are carried; an inconsistent one is dropped.
The F1-era figure for this step (about 33 ms/segment, ≈ 5.5 min per 10 000 segments) predates C1 and no longer
describes startup: a cold index now also derives every identity and writes one evidence file per segment, and the
normal start-up audit is one pass over `seen`, i.e. it scales with the number of **identities**, not segments.
Current measurement (`collector/scripts/c1_dedup_bench.py --segments 300 --rows 2000`, 600 000 identities, Linux
container, Python 3.12, pyarrow 24; a dev container, not target hardware): cold index (read + derive + index every
segment) 4.9 s (≈ 16 ms/segment); **normal startup 0.72 s**; deep verify 3.0 s; index file 66 MB (≈ 110 B per
identity). Re-measure on the real machine and volume before relying on any extrapolation.

## Migration rehearsal tool (read-only)

`collector/scripts/c1_migration_rehearsal.py` audits a **copy** of production data before the real start-up. It reads
segment bytes, markers, evidence and the SQLite index (the latter only through a private copy in a scratch
directory) and writes nothing next to them: no markers, no evidence, no index rebuild, no rename/delete/quarantine,
no duplicate winner is chosen and no history is deduplicated. `--preset` selects one of the five production trade
streams (the tests derive every preset from the real runners' wiring); `--json` prints a report whose `report_digest`
covers only deterministic content (no absolute paths, timings or free space; identical datasets at different
filesystem paths give the same digest).

What it reports, beyond counts and rebuild estimates: `cross_only_extra_rows` (occurrences beyond the first that exist
only because an identity crosses a segment boundary = sum over identities of segments − 1; intra-segment repetitions
are reported separately), the hours present as BOTH legacy hourly `.parquet` and `.seg` (reported only: legacy
parquet is outside start-up's `*.seg` reconciliation, is **not** merged into the dedup index, and whole-dataset
claims must account for the overlap), an unsupported future index schema, and a non-TEXT stored `identity_key`.

Safety rails: the scratch directory (default: system temp, honouring `TMPDIR`) and the `--json-out`/`--detail-out`
files are resolved through symlinks and refused if they lie inside the stream, evidence or index directories (or
`--data-root`); the tool stops rather than contaminate its own input. A before/after `lstat` snapshot (inode, mode,
size, mtime, ctime, symlink target; directories included; index sidecars watched) flags added, removed, modified or
replaced input — it is **not** tamper-proofing: contents are not re-hashed and a privileged actor or clock change can
defeat metadata checks.

Scratch volume: the scratch SQLite holds one row per identity occurrence, plus a copy of the index. A preflight
estimates the need (footer row counts × SQLite bytes per row **measured on a sample of the dataset's own
identities**; no fixed constant) and refuses when even the expected figure does not fit (`--allow-low-scratch`
downgrades this to a warning, `--no-scratch-preflight` skips it). Measured on the benchmark above (identity keys
≈ 89 bytes): ≈ 113 B per identity (estimate 134.8–135.7 MB, actual 134.2 MB for 600 000 identities) plus the index
copy (≈ 110 B per identity), so a **billion-identity dataset can need on the order of 100 GB of scratch plus the
index copy**; the figure scales with key length. Use `--scratch-dir` on a volume that has the space.

## Filesystem requirements and operator override

Promotion of unmarked segments requires ext4 or xfs (read from `/proc/self/mountinfo`; a
`nobarrier` mount, an unreadable mountinfo or any other filesystem is **unverified**). On an
unverified filesystem startup raises `DedupStateError` if any segment is unmarked; already-marked
segments are unaffected. Override (explicit, logged at ERROR): `COLLECTOR_ALLOW_UNVERIFIED_FS=1`;
only that exact value enables it. Use it only after verifying the storage out of band.

## Operator notes

* `quarantine_segment(path, reason, operator)` moves a `.seg` (and marker) into `quarantine/` with a
  `.quarantine.json` provenance note. Never automatic, never deletes; its index rows are then
  detected as divergence and rebuilt.
* **Retention:** `DAILY_COMPACTION.md` allows archiving raw `.seg` files after compaction. Deleting
  a reconciled segment is detected as divergence and triggers a full index rebuild at the next
  start; identities of deleted segments leave the index.
* Reader code (`storage_layout`, `replay`, `compact_daily`, `pipeline/*`) is unchanged: sidecars and
  the `quarantine/` directory are invisible to `iter_segments`.

## F5: how F1 failures propagate

F1 publication semantics and ordering are unchanged. What changed is the exception type: a failed
publication step (flush, close, fsync, rename, directory fsync, opening the next segment), a failed
marker, or a failed dedup hook now surfaces as `FatalStorageError` (original exception on
`__cause__`, plus `stage` / `durability`) instead of a bare `OSError` / `RuntimeError`, and the
application maps the owning writer's stream to a verdict (terminate for `raw_wire`/`raw_rest`,
isolate the route otherwise). A latched marker/hook failure is also visible through
`ParquetWriter.failure_snapshot()` before any further write. See `F5_FATAL_STORAGE_TOPOLOGY.md`.

## Known UNKNOWN / out of scope

* Real power-loss behaviour of the production filesystem is **not tested**; tests simulate loss by
  renaming/deleting files and kill processes with real `SIGKILL`.
* Production filesystem and mount options are not established by the repository (U1).
* A pre-existing behaviour is unchanged: `_recover_orphans` deletes a complete, fsynced
  `.seg.tmp` (emitting `DATA_DROP`); recommended follow-up is to rename it instead.
* SQLite directory-entry durability (`dedup_state/` creation) is not fsynced; loss means a derived
  index rebuild (safe direction).
