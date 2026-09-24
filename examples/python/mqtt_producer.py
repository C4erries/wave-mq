#!/usr/bin/env python3
"""Lightweight MQTT producer for wave-mq single-node mode."""

from __future__ import annotations

import argparse
import json
import os
import time
import urllib.error
import urllib.request

from mqtt_minimal import connect, disconnect, publish


def ensure_topic(api_host: str, api_port: int, topic: str, partitions: int) -> None:
    body = json.dumps(
        {
            "name": topic,
            "partitions": partitions,
            "replicationFactor": 1,
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        f"http://{api_host}:{api_port}/api/topics",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5):
            pass
    except urllib.error.HTTPError as exc:
        if exc.code != 409:
            raise RuntimeError(f"topic create failed: HTTP {exc.code}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="MQTT producer for wave-mq")
    parser.add_argument("--host", default="127.0.0.1", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--topic", default="demo.mqtt", help="MQTT topic name")
    parser.add_argument("--count", type=int, default=10, help="messages to publish")
    parser.add_argument("--interval", type=float, default=0.2, help="seconds between messages")
    parser.add_argument("--qos", type=int, default=0, choices=[0, 1], help="MQTT QoS")
    parser.add_argument("--client-id", default=f"py-producer-{os.getpid()}", help="MQTT client id")
    parser.add_argument("--api-host", default="127.0.0.1", help="HTTP API host for auto topic create")
    parser.add_argument("--api-port", type=int, default=8090, help="HTTP API port for auto topic create")
    parser.add_argument("--partitions", type=int, default=1, help="partitions for auto-created topic")
    parser.add_argument("--no-auto-create", action="store_true", help="skip topic create via HTTP API")
    args = parser.parse_args()

    if not args.no_auto_create:
        ensure_topic(args.api_host, args.api_port, args.topic, args.partitions)

    sock = connect(args.host, args.port, args.client_id, clean_session=True)
    packet_id = 1
    try:
        for i in range(args.count):
            payload = json.dumps(
                {
                    "n": i,
                    "ts": int(time.time() * 1000),
                    "from": args.client_id,
                }
            )
            publish(sock, args.topic, payload, qos=args.qos, packet_id=packet_id)
            print(f"sent {i}: {payload}")
            packet_id = (packet_id % 65535) + 1
            if args.interval > 0 and i + 1 < args.count:
                time.sleep(args.interval)
    finally:
        disconnect(sock)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
