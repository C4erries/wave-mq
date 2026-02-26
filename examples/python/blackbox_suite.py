#!/usr/bin/env python3
"""Large blackbox suite for wave-mq single-node mode.

The suite can manage a dedicated Docker stack and exercises:
- control-plane HTTP endpoints;
- heavy HTTP produce/fetch flows with partition checks;
- MQTT pub/sub for QoS0 and QoS1;
- consumer group offset commits and lag exposure;
- restart persistence (data + offsets);
- stress load from parallel producers;
- metrics/summary consistency.

No third-party Python dependencies are required.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from mqtt_minimal import MQTTError, connect, disconnect, publish, recv_publish, subscribe


@dataclass(frozen=True)
class ProfileConfig:
    http_partitions: int
    http_messages_per_partition: int
    mqtt_live_messages: int
    stress_partitions: int
    stress_workers: int
    stress_messages_per_worker: int
    restart_initial_messages: int
    restart_new_messages: int


PROFILES: dict[str, ProfileConfig] = {
    "smoke": ProfileConfig(
        http_partitions=3,
        http_messages_per_partition=20,
        mqtt_live_messages=20,
        stress_partitions=4,
        stress_workers=6,
        stress_messages_per_worker=60,
        restart_initial_messages=20,
        restart_new_messages=10,
    ),
    "full": ProfileConfig(
        http_partitions=6,
        http_messages_per_partition=80,
        mqtt_live_messages=80,
        stress_partitions=8,
        stress_workers=12,
        stress_messages_per_worker=300,
        restart_initial_messages=100,
        restart_new_messages=40,
    ),
    "massive": ProfileConfig(
        http_partitions=8,
        http_messages_per_partition=200,
        mqtt_live_messages=200,
        stress_partitions=10,
        stress_workers=24,
        stress_messages_per_worker=1000,
        restart_initial_messages=300,
        restart_new_messages=120,
    ),
}


class BlackboxError(RuntimeError):
    """Suite assertion error."""


def log(msg: str) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


class HTTPClient:
    """Tiny HTTP helper with strict status assertions."""

    def __init__(self, host: str, port: int, timeout: float) -> None:
        self.base = f"http://{host}:{port}"
        self.timeout = timeout

    def request(
        self,
        method: str,
        path: str,
        *,
        json_body: Any | None = None,
        expected_status: int | None = 200,
        params: dict[str, Any] | None = None,
    ) -> tuple[int, str]:
        url = self.base + path
        if params:
            query = urllib.parse.urlencode(params, doseq=True)
            url = f"{url}?{query}"

        headers: dict[str, str] = {}
        payload: bytes | None = None
        if json_body is not None:
            payload = json.dumps(json_body).encode("utf-8")
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url=url, data=payload, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                status = resp.status
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            status = exc.code
        except urllib.error.URLError as exc:
            raise BlackboxError(f"{method} {url} failed: {exc}") from exc

        if expected_status is not None and status != expected_status:
            raise BlackboxError(f"{method} {url} -> {status}, expected {expected_status}, body={body!r}")

        return status, body

    def request_json(
        self,
        method: str,
        path: str,
        *,
        json_body: Any | None = None,
        expected_status: int | None = 200,
        params: dict[str, Any] | None = None,
    ) -> tuple[int, Any]:
        status, body = self.request(
            method,
            path,
            json_body=json_body,
            expected_status=expected_status,
            params=params,
        )
        if not body.strip():
            return status, None
        try:
            return status, json.loads(body)
        except json.JSONDecodeError as exc:
            raise BlackboxError(f"{method} {path} returned invalid JSON: {body!r}") from exc

    def get_json(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        _, data = self.request_json("GET", path, expected_status=200, params=params)
        return data

    def post_json(self, path: str, body: Any, *, expected_status: int | None = 200) -> tuple[int, Any]:
        return self.request_json("POST", path, json_body=body, expected_status=expected_status)


class DockerStack:
    """Controls docker compose for the blackbox stack."""

    def __init__(self, compose_file: Path, env: dict[str, str] | None = None) -> None:
        self.compose_file = compose_file
        self.workdir = compose_file.parent
        self.env = env or {}

    def run(self, args: list[str], *, capture: bool = False, check: bool = True) -> subprocess.CompletedProcess[str]:
        cmd = ["docker", "compose", "-f", str(self.compose_file)] + args
        run_env = os.environ.copy()
        run_env.update(self.env)
        try:
            return subprocess.run(
                cmd,
                cwd=self.workdir,
                env=run_env,
                check=check,
                capture_output=capture,
                text=True,
            )
        except FileNotFoundError as exc:
            raise BlackboxError("docker not found. Install Docker Desktop or use --no-docker.") from exc
        except subprocess.CalledProcessError as exc:
            out = (exc.stdout or "") + (exc.stderr or "")
            raise BlackboxError(f"docker compose failed: {' '.join(args)}\n{out}") from exc

    def up(self) -> None:
        self.run(["up", "-d", "--build", "--force-recreate", "--remove-orphans"])

    def down(self) -> None:
        self.run(["down", "--volumes", "--remove-orphans"])

    def restart_broker(self) -> None:
        self.run(["restart", "broker"])

    def logs(self, tail: int = 200) -> str:
        result = self.run(["logs", "broker", "--tail", str(tail)], capture=True, check=False)
        return (result.stdout or "") + (result.stderr or "")


def wait_for(
    name: str,
    fn: Callable[[], Any],
    *,
    timeout_sec: float,
    interval_sec: float = 0.5,
) -> Any:
    deadline = time.monotonic() + timeout_sec
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = fn()
            if result:
                return result
        except Exception as exc:  # noqa: BLE001
            last_err = exc
        time.sleep(interval_sec)
    if last_err is not None:
        raise BlackboxError(f"timeout while waiting for {name}: {last_err}") from last_err
    raise BlackboxError(f"timeout while waiting for {name}")


def sum_metric(metrics_text: str, metric_name: str) -> float:
    total = 0.0
    for line in metrics_text.splitlines():
        if not line or line.startswith("#"):
            continue
        if not line.startswith(metric_name):
            continue
        fields = line.split()
        if len(fields) < 2:
            continue
        try:
            total += float(fields[-1])
        except ValueError:
            continue
    return total


@dataclass
class SuiteSettings:
    http_host: str
    http_port: int
    mqtt_host: str
    mqtt_port: int
    profile: ProfileConfig
    restart_enabled: bool
    timeout_sec: float


class BlackboxSuite:
    def __init__(self, settings: SuiteSettings) -> None:
        self.settings = settings
        self.http = HTTPClient(settings.http_host, settings.http_port, settings.timeout_sec)
        self.run_id = f"{int(time.time() * 1000)}-{os.getpid()}"
        self.created_topics: set[str] = set()
        self.total_produced = 0
        self.restart_hook: Callable[[], None] | None = None

    def run(self) -> None:
        self.run_scenario("control plane", self.scenario_control_plane)
        self.run_scenario("topic + HTTP flow", self.scenario_http_flow)
        self.run_scenario("MQTT flow + groups", self.scenario_mqtt_flow)
        if self.settings.restart_enabled:
            self.run_scenario("restart persistence", self.scenario_restart_persistence)
        self.run_scenario("parallel stress", self.scenario_parallel_stress)
        self.run_scenario("metrics + summary", self.scenario_metrics_summary)

    def run_scenario(self, name: str, fn: Callable[[], None]) -> None:
        log(f"scenario start: {name}")
        started = time.perf_counter()
        fn()
        elapsed = time.perf_counter() - started
        log(f"scenario ok: {name} ({elapsed:.2f}s)")

    def topic(self, suffix: str) -> str:
        return f"bb_{self.run_id}_{suffix}"

    def create_topic(self, name: str, partitions: int, rf: int = 1) -> dict[str, Any]:
        status, body = self.http.request(
            "POST",
            "/api/topics",
            json_body={"name": name, "partitions": partitions, "replicationFactor": rf},
            expected_status=None,
        )
        if status not in (201, 409):
            raise BlackboxError(f"create topic {name} failed with status {status}: {body!r}")
        if status == 201:
            self.created_topics.add(name)
            if body.strip():
                try:
                    parsed = json.loads(body)
                except json.JSONDecodeError:
                    return {}
                if isinstance(parsed, dict):
                    return parsed
        return {}

    def produce_http(self, topic: str, partition: int, value: str, *, retries: int = 2) -> dict[str, Any]:
        path = f"/api/topics/{topic}/partitions/{partition}/messages"
        last_err: Exception | None = None
        for attempt in range(retries + 1):
            try:
                _, data = self.http.post_json(path, {"value": value}, expected_status=200)
                if not isinstance(data, dict):
                    raise BlackboxError(f"produce response is not JSON object: {data!r}")
                self.total_produced += 1
                return data
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                if attempt == retries:
                    break
                time.sleep(0.05 * (attempt + 1))
        assert last_err is not None
        raise BlackboxError(f"produce failed topic={topic} partition={partition}: {last_err}") from last_err

    def fetch_messages(self, topic: str, partition: int, *, limit: int, offset: int | None = None) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"limit": limit}
        if offset is not None:
            params["offset"] = offset
        data = self.http.get_json(f"/api/topics/{topic}/partitions/{partition}/messages", params=params)
        if not isinstance(data, list):
            raise BlackboxError(f"fetch response must be list, got {type(data)}")
        result: list[dict[str, Any]] = []
        for item in data:
            if isinstance(item, dict):
                result.append(item)
        return result

    def consume_exact(
        self,
        *,
        client_id: str,
        topic: str,
        qos: int,
        count: int,
        clean_session: bool,
        timeout_sec: float,
    ) -> list[str]:
        sock = connect(self.settings.mqtt_host, self.settings.mqtt_port, client_id, clean_session=clean_session)
        try:
            subscribe(sock, topic, qos=qos, packet_id=1)
            payloads: list[str] = []
            deadline = time.monotonic() + timeout_sec
            while len(payloads) < count:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise BlackboxError(
                        f"timeout waiting mqtt messages for client={client_id}: "
                        f"{len(payloads)}/{count} received"
                    )
                try:
                    msg = recv_publish(sock, timeout_sec=min(1.0, remaining))
                except socket.timeout:
                    continue
                except MQTTError as exc:
                    raise BlackboxError(f"mqtt error for client={client_id}: {exc}") from exc
                payloads.append(msg.payload.decode("utf-8", "replace"))
            return payloads
        finally:
            disconnect(sock)

    def wait_health(self, timeout_sec: float = 60.0) -> None:
        def check() -> bool:
            status, body = self.http.request("GET", "/healthz", expected_status=None)
            return status == 200 and body.strip() == "ok"

        wait_for("broker healthz", check, timeout_sec=timeout_sec, interval_sec=0.5)

    def wait_for_committed(self, group: str, topic: str, partition: int, min_offset: int, timeout_sec: float = 30.0) -> None:
        def check() -> bool:
            groups = self.http.get_json("/api/consumers")
            if not isinstance(groups, list):
                return False
            for g in groups:
                if not isinstance(g, dict):
                    continue
                if g.get("name") != group:
                    continue
                assignments = g.get("assignments", [])
                if not isinstance(assignments, list):
                    continue
                for a in assignments:
                    if not isinstance(a, dict):
                        continue
                    if a.get("topic") == topic and int(a.get("partition", -1)) == partition:
                        committed = int(a.get("committedOffset", -1))
                        return committed >= min_offset
            return False

        wait_for(
            f"group {group} commit >= {min_offset}",
            check,
            timeout_sec=timeout_sec,
            interval_sec=0.5,
        )

    def scenario_control_plane(self) -> None:
        self.wait_health()

        broker = self.http.get_json("/api/broker")
        if not isinstance(broker, dict):
            raise BlackboxError("/api/broker returned invalid payload")
        if broker.get("controllerMode") != "single":
            raise BlackboxError(f"expected controllerMode=single, got {broker.get('controllerMode')!r}")

        ctrl = self.http.get_json("/api/controller")
        if not isinstance(ctrl, dict):
            raise BlackboxError("/api/controller returned invalid payload")
        if ctrl.get("mode") != "single":
            raise BlackboxError(f"expected controller mode=single, got {ctrl.get('mode')!r}")

        cluster = self.http.get_json("/api/cluster")
        if not isinstance(cluster, dict):
            raise BlackboxError("/api/cluster returned invalid payload")
        brokers = cluster.get("brokers", [])
        if not isinstance(brokers, list) or len(brokers) < 1:
            raise BlackboxError("cluster must contain at least one broker")

        summary = self.http.get_json("/api/summary")
        if not isinstance(summary, dict):
            raise BlackboxError("/api/summary returned invalid payload")
        for key in ("topics", "partitions", "produced", "consumed", "errors"):
            if key not in summary:
                raise BlackboxError(f"summary missing key {key!r}")

    def scenario_http_flow(self) -> None:
        cfg = self.settings.profile
        topic = self.topic("http")
        self.create_topic(topic, cfg.http_partitions)

        dup_status, _ = self.http.post_json(
            "/api/topics",
            {"name": topic, "partitions": cfg.http_partitions, "replicationFactor": 1},
            expected_status=None,
        )
        if dup_status != 409:
            raise BlackboxError(f"duplicate topic create should return 409, got {dup_status}")

        bad_status, _ = self.http.post_json(
            "/api/topics",
            {"name": "", "partitions": 0, "replicationFactor": 1},
            expected_status=None,
        )
        if bad_status != 400:
            raise BlackboxError(f"invalid topic create should return 400, got {bad_status}")

        topics = self.http.get_json("/api/topics")
        if not isinstance(topics, list):
            raise BlackboxError("/api/topics should return list")
        names = {str(item.get("name")) for item in topics if isinstance(item, dict)}
        if topic not in names:
            raise BlackboxError(f"topic {topic} not found in /api/topics")

        detail = self.http.get_json(f"/api/topics/{topic}")
        if not isinstance(detail, dict):
            raise BlackboxError("topic detail must be an object")
        if int(detail.get("partitionCount", -1)) != cfg.http_partitions:
            raise BlackboxError("unexpected partitionCount in topic detail")

        for partition in range(cfg.http_partitions):
            for i in range(cfg.http_messages_per_partition):
                value = f"http|run={self.run_id}|p={partition}|i={i}"
                self.produce_http(topic, partition, value)

        expected_offsets = list(range(cfg.http_messages_per_partition - 1, -1, -1))
        for partition in range(cfg.http_partitions):
            messages = self.fetch_messages(
                topic,
                partition,
                limit=cfg.http_messages_per_partition + 5,
            )
            if len(messages) != cfg.http_messages_per_partition:
                raise BlackboxError(
                    f"partition {partition} expected {cfg.http_messages_per_partition} messages, got {len(messages)}"
                )

            offsets = [int(m.get("offset", -1)) for m in messages]
            if offsets != expected_offsets:
                raise BlackboxError(f"partition {partition} offsets mismatch: {offsets[:10]} ...")

        target = min(10, cfg.http_messages_per_partition - 1)
        limit = 5
        window = self.fetch_messages(topic, 0, limit=limit, offset=target)
        want = list(range(target, max(-1, target-limit), -1))
        got = [int(m.get("offset", -1)) for m in window]
        if got != want:
            raise BlackboxError(f"offset window mismatch: want {want}, got {got}")

        bad_limit_status, _ = self.http.request("GET", f"/api/topics/{topic}/partitions/0/messages", params={"limit": 0}, expected_status=None)
        if bad_limit_status != 400:
            raise BlackboxError(f"invalid limit should return 400, got {bad_limit_status}")

        miss_status, _ = self.http.request(
            "POST",
            f"/api/topics/{topic}/partitions/{cfg.http_partitions+99}/messages",
            json_body={"value": "x"},
            expected_status=None,
        )
        if miss_status != 404:
            raise BlackboxError(f"unknown partition produce should return 404, got {miss_status}")

    def scenario_mqtt_flow(self) -> None:
        cfg = self.settings.profile
        topic = self.topic("mqtt")
        self.create_topic(topic, 1)

        backlog_count = max(10, cfg.mqtt_live_messages // 5)
        backlog_payloads: list[str] = []
        for i in range(backlog_count):
            value = f"mqtt-backlog|run={self.run_id}|i={i}"
            self.produce_http(topic, 0, value)
            backlog_payloads.append(value)

        resume_group = f"bb-resume-{self.run_id}"
        got_backlog = self.consume_exact(
            client_id=resume_group,
            topic=topic,
            qos=1,
            count=backlog_count,
            clean_session=False,
            timeout_sec=60.0,
        )
        if sorted(got_backlog) != sorted(backlog_payloads):
            raise BlackboxError("resume consumer did not receive expected backlog")
        self.wait_for_committed(resume_group, topic, 0, backlog_count - 1, timeout_sec=20.0)

        tail_consumer_id = f"bb-tail-{self.run_id}"
        tail_sock = connect(self.settings.mqtt_host, self.settings.mqtt_port, tail_consumer_id, clean_session=True)
        try:
            subscribe(tail_sock, topic, qos=1, packet_id=1)

            producer_id = f"bb-pub-{self.run_id}"
            pub_sock = connect(self.settings.mqtt_host, self.settings.mqtt_port, producer_id, clean_session=True)
            try:
                live_payloads: list[str] = []
                packet_id = 1
                for i in range(cfg.mqtt_live_messages):
                    payload = f"mqtt-live|run={self.run_id}|i={i}"
                    qos = 1 if i % 2 else 0
                    publish(pub_sock, topic, payload, qos=qos, packet_id=packet_id)
                    self.total_produced += 1
                    live_payloads.append(payload)
                    packet_id = 1 if packet_id >= 65535 else packet_id + 1
            finally:
                disconnect(pub_sock)

            received_live: list[str] = []
            deadline = time.monotonic() + 90.0
            while len(received_live) < cfg.mqtt_live_messages:
                if time.monotonic() > deadline:
                    raise BlackboxError(
                        f"tail consumer timeout: {len(received_live)}/{cfg.mqtt_live_messages}"
                    )
                try:
                    msg = recv_publish(tail_sock, timeout_sec=1.0)
                except socket.timeout:
                    continue
                received_live.append(msg.payload.decode("utf-8", "replace"))

            if sorted(received_live) != sorted(live_payloads):
                raise BlackboxError("tail consumer did not receive expected live payloads")
        finally:
            disconnect(tail_sock)

        self.wait_for_committed(
            tail_consumer_id,
            topic,
            0,
            backlog_count + cfg.mqtt_live_messages - 1,
            timeout_sec=20.0,
        )

        groups = self.http.get_json("/api/consumers")
        if not isinstance(groups, list):
            raise BlackboxError("/api/consumers must return list")
        group_names = {str(g.get("name")) for g in groups if isinstance(g, dict)}
        if resume_group not in group_names:
            raise BlackboxError(f"expected group {resume_group} in /api/consumers")
        if tail_consumer_id not in group_names:
            raise BlackboxError(f"expected group {tail_consumer_id} in /api/consumers")

    def scenario_restart_persistence(self) -> None:
        if self.restart_hook is None:
            raise BlackboxError("restart hook is not configured")

        cfg = self.settings.profile
        topic = self.topic("restart")
        self.create_topic(topic, 1)

        initial_payloads: list[str] = []
        for i in range(cfg.restart_initial_messages):
            payload = f"restart-before|run={self.run_id}|i={i}"
            self.produce_http(topic, 0, payload)
            initial_payloads.append(payload)

        client_id = f"bb-restart-{self.run_id}"
        first_batch = self.consume_exact(
            client_id=client_id,
            topic=topic,
            qos=1,
            count=cfg.restart_initial_messages,
            clean_session=False,
            timeout_sec=120.0,
        )
        if sorted(first_batch) != sorted(initial_payloads):
            raise BlackboxError("restart scenario: first batch mismatch")

        self.wait_for_committed(client_id, topic, 0, cfg.restart_initial_messages - 1, timeout_sec=30.0)

        self.restart_hook()
        self.wait_health(timeout_sec=90.0)

        new_payloads: list[str] = []
        for i in range(cfg.restart_new_messages):
            payload = f"restart-after|run={self.run_id}|i={i}"
            self.produce_http(topic, 0, payload)
            new_payloads.append(payload)

        second_batch = self.consume_exact(
            client_id=client_id,
            topic=topic,
            qos=1,
            count=cfg.restart_new_messages,
            clean_session=False,
            timeout_sec=120.0,
        )
        if sorted(second_batch) != sorted(new_payloads):
            raise BlackboxError(
                "restart scenario: resume consumer received unexpected payloads (offset persistence broken)"
            )

        last_offset = cfg.restart_initial_messages + cfg.restart_new_messages - 1
        self.wait_for_committed(client_id, topic, 0, last_offset, timeout_sec=30.0)

        messages = self.fetch_messages(
            topic,
            0,
            limit=cfg.restart_initial_messages + cfg.restart_new_messages + 10,
        )
        if len(messages) != cfg.restart_initial_messages + cfg.restart_new_messages:
            raise BlackboxError("restart scenario: unexpected message count after restart")

    def scenario_parallel_stress(self) -> None:
        cfg = self.settings.profile
        topic = self.topic("stress")
        self.create_topic(topic, cfg.stress_partitions)

        def worker(worker_id: int) -> list[int]:
            local = [0] * cfg.stress_partitions
            for i in range(cfg.stress_messages_per_worker):
                partition = (worker_id + i) % cfg.stress_partitions
                value = f"stress|run={self.run_id}|w={worker_id}|i={i}|p={partition}"
                self.produce_http(topic, partition, value, retries=3)
                local[partition] += 1
            return local

        per_partition = [0] * cfg.stress_partitions
        with ThreadPoolExecutor(max_workers=cfg.stress_workers) as pool:
            futures = [pool.submit(worker, wid) for wid in range(cfg.stress_workers)]
            for fut in as_completed(futures):
                counts = fut.result()
                for idx, count in enumerate(counts):
                    per_partition[idx] += count

        for partition, expected in enumerate(per_partition):
            messages = self.fetch_messages(topic, partition, limit=expected + 10)
            if len(messages) != expected:
                raise BlackboxError(
                    f"stress partition {partition}: expected {expected}, got {len(messages)}"
                )
            offsets = [int(m.get("offset", -1)) for m in messages]
            expected_offsets = list(range(expected - 1, -1, -1))
            if offsets != expected_offsets:
                raise BlackboxError(f"stress partition {partition}: offsets are not contiguous")

    def scenario_metrics_summary(self) -> None:
        summary = self.http.get_json("/api/summary")
        if not isinstance(summary, dict):
            raise BlackboxError("summary payload is invalid")
        topics_count = int(summary.get("topics", -1))
        if topics_count < len(self.created_topics):
            raise BlackboxError(
                f"summary topics {topics_count} < created {len(self.created_topics)}"
            )
        produced_summary = float(summary.get("produced", -1))
        if produced_summary < self.total_produced:
            raise BlackboxError(
                f"summary produced {produced_summary} < expected at least {self.total_produced}"
            )

        _, metrics = self.http.request("GET", "/metrics", expected_status=200)
        produced_metric = sum_metric(metrics, "wavemq_messages_produced_total")
        consumed_metric = sum_metric(metrics, "wavemq_messages_consumed_total")
        if produced_metric < self.total_produced:
            raise BlackboxError(
                f"metrics produced {produced_metric} < expected at least {self.total_produced}"
            )
        if consumed_metric <= 0:
            raise BlackboxError("consumed metric must be > 0 after test flow")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="wave-mq blackbox suite (single-node)")
    parser.add_argument("--profile", choices=sorted(PROFILES.keys()), default="full", help="test load profile")
    parser.add_argument("--http-host", default="127.0.0.1", help="HTTP host")
    parser.add_argument("--http-port", type=int, default=18090, help="HTTP port")
    parser.add_argument("--mqtt-host", default="127.0.0.1", help="MQTT host")
    parser.add_argument("--mqtt-port", type=int, default=11883, help="MQTT port")
    parser.add_argument("--binary-port", type=int, default=17912, help="binary protocol host port for Docker mapping")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout (sec)")
    parser.add_argument("--no-docker", action="store_true", help="do not manage Docker stack")
    parser.add_argument("--keep-stack", action="store_true", help="keep docker stack running after suite")
    parser.add_argument("--skip-restart", action="store_true", help="skip restart persistence scenario")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    compose_file = script_dir / "docker-compose.blackbox.yml"
    profile = PROFILES[args.profile]

    settings = SuiteSettings(
        http_host=args.http_host,
        http_port=args.http_port,
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        profile=profile,
        restart_enabled=(not args.no_docker and not args.skip_restart),
        timeout_sec=args.timeout,
    )
    suite = BlackboxSuite(settings)

    stack: DockerStack | None = None
    if not args.no_docker:
        env = {
            "WAVEMQ_BB_BINARY_PORT": str(args.binary_port),
            "WAVEMQ_BB_HTTP_PORT": str(args.http_port),
            "WAVEMQ_BB_MQTT_PORT": str(args.mqtt_port),
        }
        stack = DockerStack(compose_file, env=env)
        try:
            log("docker stack: down (cleanup)")
            stack.down()
        except Exception as exc:  # noqa: BLE001
            log(f"cleanup warning: {exc}")
        try:
            log("docker stack: up")
            stack.up()
        except Exception as exc:  # noqa: BLE001
            log(f"docker setup failed: {exc}")
            if not args.keep_stack:
                try:
                    stack.down()
                except Exception:  # noqa: BLE001
                    pass
            return 1
        suite.restart_hook = stack.restart_broker

    try:
        suite.wait_health(timeout_sec=90.0)
        suite.run()
        log("all scenarios passed")
        return 0
    except Exception as exc:  # noqa: BLE001
        log(f"blackbox failed: {exc}")
        if stack is not None:
            try:
                logs = stack.logs(tail=200)
            except Exception as logs_exc:  # noqa: BLE001
                log(f"cannot fetch broker logs: {logs_exc}")
            else:
                print("\n===== broker logs (tail) =====")
                print(logs)
                print("===== end logs =====\n")
        return 1
    finally:
        if stack is not None and not args.keep_stack:
            log("docker stack: down")
            try:
                stack.down()
            except Exception as exc:  # noqa: BLE001
                log(f"down warning: {exc}")


if __name__ == "__main__":
    raise SystemExit(main())
