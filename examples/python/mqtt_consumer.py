#!/usr/bin/env python3
"""Lightweight MQTT consumer for wave-mq single-node mode."""

from __future__ import annotations

import argparse
import json
import os
import socket
import time
import urllib.error
import urllib.request

from mqtt_minimal import MQTTError, connect, disconnect, recv_publish, subscribe


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
    parser = argparse.ArgumentParser(description="MQTT consumer for wave-mq")
    parser.add_argument("--host", default="127.0.0.1", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=1883, help="MQTT broker port")
    parser.add_argument("--topic", default="demo.mqtt", help="MQTT topic name")
    parser.add_argument("--qos", type=int, default=0, choices=[0, 1], help="MQTT QoS")
    parser.add_argument("--client-id", default=f"py-consumer-{os.getpid()}", help="MQTT client id")
    parser.add_argument("--max-messages", type=int, default=5, help="messages to read; 0 means infinite")
    parser.add_argument("--timeout", type=float, default=30.0, help="seconds to wait without messages before exit")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="resume from committed offsets (clean_session=false); default is tail-only",
    )
    parser.add_argument("--api-host", default="127.0.0.1", help="HTTP API host for auto topic create")
    parser.add_argument("--api-port", type=int, default=8090, help="HTTP API port for auto topic create")
    parser.add_argument("--partitions", type=int, default=1, help="partitions for auto-created topic")
    parser.add_argument("--no-auto-create", action="store_true", help="skip topic create via HTTP API")
    args = parser.parse_args()

    if not args.no_auto_create:
        ensure_topic(args.api_host, args.api_port, args.topic, args.partitions)

    sock = connect(args.host, args.port, args.client_id, clean_session=not args.resume)
    subscribe(sock, args.topic, qos=args.qos, packet_id=1)
    print(f"subscribed to {args.topic} as {args.client_id}")

    received = 0
    last_recv = time.monotonic()
    try:
        while args.max_messages == 0 or received < args.max_messages:
            if args.timeout > 0 and time.monotonic() - last_recv > args.timeout:
                print("timeout reached without new messages")
                break
            try:
                msg = recv_publish(sock, timeout_sec=1.0)
            except socket.timeout:
                continue
            except MQTTError as exc:
                print(f"mqtt error: {exc}")
                break

            last_recv = time.monotonic()
            received += 1
            print(f"recv {received}: topic={msg.topic} qos={msg.qos} payload={msg.payload.decode('utf-8', 'replace')}")
    finally:
        disconnect(sock)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
