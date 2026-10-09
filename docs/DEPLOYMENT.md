# Deployment (systemd on EC2)

Unit: `collector/btc-collector.service`. The only deployment file in the repo.

## Verification status

| Claim | Status |
|---|---|
| Unit syntax and directive validity | Checked offline (`systemd-analyze verify` on a path-remapped copy, plus static tests) |
| Paths/command match the repo's runner contract (`python -m collector.run_collector` from repo root) | Checked offline against source |
| EC2 user is `ec2-user`, repo at `/home/ec2-user/btc-data-collector`, venv at `.venv` | **NOT VERIFIED** -- assumption from project context; no live host was accessible |
| Service actually starts, restarts, and stops cleanly on EC2 | **NOT VERIFIED** |

## Install (on the host, after checking `id` and the clone path)

```
cd /home/ec2-user/btc-data-collector
python3 -m venv .venv && .venv/bin/pip install -r collector/requirements.txt
sudo install -m 0600 -o root -g root /dev/null /etc/btc-collector.env   # add TELEGRAM_BOT_TOKEN=... / TELEGRAM_CHAT_ID=... if wanted
sudo cp collector/btc-collector.service /etc/systemd/system/
sudo systemd-analyze verify /etc/systemd/system/btc-collector.service
sudo systemctl daemon-reload && sudo systemctl enable --now btc-collector
```

If the user or path differs, edit `User`, `Group`, `WorkingDirectory` and `ExecStart` together.
Use `sudo chown -R ec2-user:ec2-user` on the repo so `data/` and `logs/` are writable.

Operate: `systemctl status|stop|restart btc-collector`, `journalctl -u btc-collector -f`.

## Secrets

Not in the unit. `EnvironmentFile=/etc/btc-collector.env` (root-owned, 0600) supplies
`TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`. The previous unit set
`TELEGRAM_BOT_TOKEN=YOUR_BOT_TOKEN`, which the notifier would have read as a real token.

## Failure behaviour

- Missing/unreadable env file, bad WorkingDirectory/ExecStart/user: start fails immediately.
- Repeated failure: after 5 starts in 300 s the unit enters `failed` (no infinite crash loop).
- Stop: SIGTERM, up to 60 s (must exceed the 30 s websocket queue drain).
- **F5 terminal storage failure:** if the `raw_wire` or `raw_rest` writer fails (raw evidence is
  irrecoverable), the collector stops ingesting, runs the normal shutdown (closing the healthy
  writers) and exits with status **70**. Under the existing `Restart=always` that is a failure exit
  and is restarted after `RestartSec`; F1/P0-4 startup reconciliation then recovers the stream. It
  counts toward `StartLimitBurst` like any other failed start. A *derived* writer failure does not
  exit: only that route is isolated and the operator is alerted. The unit file is unchanged;
  behaviour under a real systemd/disk fault is not verified live. See `F5_FATAL_STORAGE_TOPOLOGY.md`.

## Runner coverage (repository evidence, P0-12 finalization)

Only `collector.run_collector` (Binance USD-M) has a systemd unit. `run_bybit_collector.py`,
`run_okx_collector.py` and `run_binance_spot_collector.py` exist in the repository but have
no deployment artifact. This is scope, not a limitation of this unit: nothing in the repo
indicates one unit should cover every runner, and no second unit was added speculatively.
Add one per runner, following this file's pattern, if/when those need deployment.

## Shutdown contract (offline analysis only -- NOT VERIFIED LIVE)

Traced from source, not assumed:

```
SIGTERM -> CollectorApp._async_shutdown()
  -> self.running = False
  -> cancel + await the recovery task
  -> drain integrity quality events, await the quality-queue task
  -> self.shutdown(): ws_client.stop() for each client, then cancel every task in self.tasks
```

`self.tasks` includes each `WebSocketClient.start()` coroutine -- the one that contains
P0-1's own graceful drain (`await asyncio.wait_for(self._ingest_queue.join(), timeout=30.0)`).
`shutdown()` cancels that task immediately after calling `ws_client.stop()`, so static
reading does not show the 30s drain being awaited to completion in this path before
cancellation reaches it. `TimeoutStopSec=60` is kept as generous margin, not as a
proven wait for that drain -- whether it completes in practice depends on asyncio task
scheduling this analysis cannot settle without running it. **NOT VERIFIED LIVE.**

## Telegram credentials

`telegram_bot.py` reads `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` from the environment only;
there is no source-code default. Telegram stays optional: unset or malformed values disable it
and the collector runs normally. An explicit request (`load_telegram_config(validate=True)`,
`send_test_telegram_alert()`) raises `TelegramConfigError` instead.

**ROTATION REQUIRED.** A bot token and chat id were previously committed to `telegram_bot.py`
and remain in Git history. Treat that token as compromised: revoke it via BotFather, issue a new
one, and supply it only through `/etc/btc-collector.env` on the host. Never commit it.
