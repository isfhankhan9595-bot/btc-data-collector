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

## Known issue outside this change

`telegram_bot.py` at the repo root contains a hard-coded Telegram bot token and chat id
in Git history. Treat as compromised: rotate it, and remove the fallback default. Not
changed here (out of P0-12 scope).
