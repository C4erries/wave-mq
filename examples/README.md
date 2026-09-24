# wave-mq blackbox suites

This folder contains the release-gate and blackbox runners for `wave-mq`.

## What is here

- `examples/python/blackbox_suite.py` - single-node control plane, HTTP, MQTT, restart, and metrics coverage.
- `examples/python/blackbox_multi_suite.py` - multi-broker Raft / replication coverage.

These scripts are the main end-to-end checks used while previewing the broker.

## Single-node blackbox

From `wave-mq/examples/python`:

```powershell
python blackbox_suite.py --profile full
```

Useful flags:

```powershell
python blackbox_suite.py --profile smoke --keep-stack
python blackbox_suite.py --profile full --skip-restart
python blackbox_suite.py --profile massive --keep-stack
```

## Multi-broker blackbox

From `wave-mq/examples/python`:

```powershell
python blackbox_multi_suite.py --profile full
```

Useful flags:

```powershell
python blackbox_multi_suite.py --profile smoke --skip-restart
python blackbox_multi_suite.py --profile full --keep-stack
python blackbox_multi_suite.py --profile massive --keep-stack
```

## Notes

- The blackbox suites are separate from the top-level `examples/` README, which focuses on runnable demo scripts.
- For the broker preview quickstart, use the root [README.md](../../README.md).
