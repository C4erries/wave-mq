#!/usr/bin/env python3
"""Blackbox suite for wave-mq multi-broker (2-node Raft) mode.

The suite is focused on distributed blackbox behavior:
- both brokers become healthy and report raft mode;
- cluster metadata converges to two brokers;
- topic with RF=2 is created and partition leadership is distributed;
- produce/fetch workload goes to actual partition leaders;
- wrong-broker produce/fetch returns not_leader;
- ISR converges to both replicas for all topic partitions;
- after broker restart, cluster recovers and keeps serving traffic.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable


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
            url = f"{url}?{urllib.parse.urlencode(params, doseq=True)}"

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
    """Controls docker compose for the multi-broker blackbox stack."""

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

    def restart(self, service: str) -> None:
        self.run(["restart", service])

    def logs(self, service: str, tail: int = 200) -> str:
        result = self.run(["logs", service, "--tail", str(tail)], capture=True, check=False)
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


def find_log_marker_lines(logs: str, marker: str, limit: int = 20) -> list[str]:
    marker_lower = marker.lower()
    matches: list[str] = []
    for line in logs.splitlines():
        if marker_lower in line.lower():
            matches.append(line.strip())
            if len(matches) >= limit:
                break
    return matches


@dataclass(frozen=True)
class ProfileConfig:
    partitions: int
    workers: int
    messages_per_worker: int
    restart_messages_per_partition: int


PROFILES: dict[str, ProfileConfig] = {
    "smoke": ProfileConfig(
        partitions=4,
        workers=8,
        messages_per_worker=40,
        restart_messages_per_partition=20,
    ),
    "full": ProfileConfig(
        partitions=8,
        workers=16,
        messages_per_worker=200,
        restart_messages_per_partition=60,
    ),
    "massive": ProfileConfig(
        partitions=12,
        workers=32,
        messages_per_worker=500,
        restart_messages_per_partition=150,
    ),
}


@dataclass
class SuiteSettings:
    http_host: str
    http_port_1: int
    http_port_2: int
    profile: ProfileConfig
    restart_enabled: bool
    timeout_sec: float


class MultiBlackboxSuite:
    def __init__(self, settings: SuiteSettings) -> None:
        self.settings = settings
        self.clients: dict[int, HTTPClient] = {
            1: HTTPClient(settings.http_host, settings.http_port_1, settings.timeout_sec),
            2: HTTPClient(settings.http_host, settings.http_port_2, settings.timeout_sec),
        }
        self.run_id = f"{int(time.time() * 1000)}-{os.getpid()}"
        self.topic = f"bbm_{self.run_id}"
        self.expected_per_partition: dict[int, int] = {}

    def run(self) -> None:
        self.run_scenario("control plane", self.scenario_control_plane)
        self.run_scenario("distributed data flow", self.scenario_distributed_flow)
        if self.settings.restart_enabled:
            self.run_scenario("broker restart", self.scenario_restart)
        self.run_scenario("metrics and summary", self.scenario_metrics)

    def run_scenario(self, name: str, fn: Callable[[], None]) -> None:
        log(f"scenario start: {name}")
        started = time.perf_counter()
        fn()
        elapsed = time.perf_counter() - started
        log(f"scenario ok: {name} ({elapsed:.2f}s)")

    def health_ok(self, broker_id: int) -> bool:
        status, body = self.clients[broker_id].request("GET", "/healthz", expected_status=None)
        return status == 200 and body.strip() == "ok"

    def wait_broker_health(self, broker_id: int, timeout_sec: float = 90.0) -> None:
        wait_for(
            f"broker{broker_id} health",
            lambda: self.health_ok(broker_id),
            timeout_sec=timeout_sec,
            interval_sec=0.5,
        )

    def get_cluster(self) -> dict[str, Any]:
        cluster = self.clients[1].get_json("/api/cluster")
        if not isinstance(cluster, dict):
            raise BlackboxError("/api/cluster returned invalid payload")
        return cluster

    def wait_cluster_brokers(self) -> dict[str, Any]:
        def ready() -> dict[str, Any] | None:
            cluster = self.get_cluster()
            brokers = cluster.get("brokers", [])
            if not isinstance(brokers, list):
                return None
            ids = {int(b.get("brokerID", -1)) for b in brokers if isinstance(b, dict)}
            if ids == {1, 2}:
                return cluster
            return None

        return wait_for("cluster brokers 1+2", ready, timeout_sec=90.0, interval_sec=0.5)

    def create_topic(self) -> None:
        cfg = self.settings.profile
        status, body = self.clients[1].request(
            "POST",
            "/api/topics",
            json_body={
                "name": self.topic,
                "partitions": cfg.partitions,
                "replicationFactor": 2,
            },
            expected_status=None,
        )
        if status not in (201, 409):
            raise BlackboxError(f"topic create failed with {status}: {body!r}")

    def wait_topic_assignments(self) -> list[dict[str, Any]]:
        cfg = self.settings.profile

        def snapshot() -> list[dict[str, Any]] | None:
            cluster = self.get_cluster()
            partitions = cluster.get("partitions", [])
            if not isinstance(partitions, list):
                return None
            ours = [p for p in partitions if isinstance(p, dict) and p.get("topic") == self.topic]
            if len(ours) != cfg.partitions:
                return None
            return ours

        return wait_for(
            f"topic assignments for {self.topic}",
            snapshot,
            timeout_sec=90.0,
            interval_sec=0.5,
        )

    def wait_full_isr(self) -> None:
        cfg = self.settings.profile
        deadline = time.monotonic() + 120.0
        interval_sec = 0.8
        last_assignments: list[dict[str, Any]] = []
        last_version: int | None = None
        last_error: Exception | None = None

        while time.monotonic() < deadline:
            try:
                cluster = self.get_cluster()
                last_version = int(cluster.get("version", 0))
                partitions = cluster.get("partitions", [])
                if isinstance(partitions, list):
                    assignments = [p for p in partitions if isinstance(p, dict) and p.get("topic") == self.topic]
                    if len(assignments) == cfg.partitions:
                        last_assignments = assignments
                        all_full = True
                        for p in assignments:
                            isr = p.get("isr", [])
                            if not isinstance(isr, list) or {int(x) for x in isr} != {1, 2}:
                                all_full = False
                                break
                        if all_full:
                            return
            except Exception as exc:  # noqa: BLE001
                last_error = exc

            time.sleep(interval_sec)

        summary = [
            {
                "partition": int(p.get("partition", -1)),
                "leader": int(p.get("leader", -1)),
                "isr": p.get("isr", []),
                "replicas": p.get("replicas", []),
            }
            for p in last_assignments
        ]
        suffix = ""
        if last_error is not None:
            suffix = f"; last_error={last_error}"
        raise BlackboxError(
            f"timeout while waiting for full ISR (1,2) for all partitions; "
            f"cluster_version={last_version}; assignments={summary}{suffix}"
        )

    def partition_leaders(self) -> dict[int, int]:
        leaders: dict[int, int] = {}
        for p in self.wait_topic_assignments():
            pid = int(p.get("partition", -1))
            leader = int(p.get("leader", -1))
            if pid >= 0 and leader in (1, 2):
                leaders[pid] = leader
        if len(leaders) != self.settings.profile.partitions:
            raise BlackboxError(f"incomplete leaders map: {leaders}")
        return leaders

    def produce(self, broker_id: int, partition: int, value: str) -> dict[str, Any]:
        path = f"/api/topics/{self.topic}/partitions/{partition}/messages"
        last_err: Exception | None = None
        for attempt in range(4):
            try:
                _, data = self.clients[broker_id].post_json(path, {"value": value}, expected_status=200)
                if not isinstance(data, dict):
                    raise BlackboxError(f"produce response is invalid: {data!r}")
                return data
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                if attempt == 3:
                    break
                time.sleep(0.05 * (attempt + 1))

        assert last_err is not None
        raise BlackboxError(f"produce failed broker={broker_id} partition={partition}: {last_err}") from last_err

    def fetch(self, broker_id: int, partition: int, limit: int) -> list[dict[str, Any]]:
        data = self.clients[broker_id].get_json(
            f"/api/topics/{self.topic}/partitions/{partition}/messages",
            params={"limit": limit},
        )
        if not isinstance(data, list):
            raise BlackboxError(f"fetch response must be list, got {type(data)}")
        return [x for x in data if isinstance(x, dict)]

    def scenario_control_plane(self) -> None:
        self.wait_broker_health(1)
        self.wait_broker_health(2)

        for broker_id in (1, 2):
            status = self.clients[broker_id].get_json("/api/controller")
            if not isinstance(status, dict):
                raise BlackboxError(f"broker{broker_id} /api/controller invalid payload")
            if status.get("mode") != "raft":
                raise BlackboxError(f"broker{broker_id} expected mode=raft, got {status.get('mode')!r}")

            info = self.clients[broker_id].get_json("/api/broker")
            if not isinstance(info, dict):
                raise BlackboxError(f"broker{broker_id} /api/broker invalid payload")
            if int(info.get("id", -1)) != broker_id:
                raise BlackboxError(f"broker{broker_id} reported wrong id: {info.get('id')!r}")

        cluster = self.wait_cluster_brokers()
        if int(cluster.get("version", 0)) < 1:
            raise BlackboxError(f"invalid cluster version: {cluster.get('version')}")

    def scenario_distributed_flow(self) -> None:
        cfg = self.settings.profile
        self.create_topic()

        leaders = self.partition_leaders()
        unique_leaders = set(leaders.values())
        if len(unique_leaders) < 2 and cfg.partitions > 1:
            raise BlackboxError(f"expected leadership split, got {leaders}")

        def worker(worker_id: int) -> dict[int, int]:
            local = {pid: 0 for pid in range(cfg.partitions)}
            for i in range(cfg.messages_per_worker):
                pid = (worker_id + i) % cfg.partitions
                leader = leaders[pid]
                payload = f"multi|run={self.run_id}|w={worker_id}|i={i}|p={pid}"
                self.produce(leader, pid, payload)
                local[pid] += 1
            return local

        expected = {pid: 0 for pid in range(cfg.partitions)}
        with ThreadPoolExecutor(max_workers=cfg.workers) as pool:
            futures = [pool.submit(worker, wid) for wid in range(cfg.workers)]
            for fut in as_completed(futures):
                got = fut.result()
                for pid, count in got.items():
                    expected[pid] += count

        self.expected_per_partition = expected

        for pid, cnt in expected.items():
            leader = leaders[pid]
            messages = self.fetch(leader, pid, cnt + 10)
            if len(messages) != cnt:
                raise BlackboxError(f"partition {pid}: expected {cnt}, got {len(messages)}")
            offsets = [int(m.get("offset", -1)) for m in messages]
            want = list(range(cnt - 1, -1, -1))
            if offsets != want:
                raise BlackboxError(f"partition {pid}: offsets mismatch")

        wrong_pid = next(iter(leaders.keys()))
        wrong_leader = leaders[wrong_pid]
        wrong_broker = 2 if wrong_leader == 1 else 1

        status, body = self.clients[wrong_broker].post_json(
            f"/api/topics/{self.topic}/partitions/{wrong_pid}/messages",
            {"value": f"wrong-broker|run={self.run_id}"},
            expected_status=None,
        )
        if status != 409:
            raise BlackboxError(f"wrong-broker produce expected 409, got {status}")
        if not isinstance(body, dict) or body.get("error") != "not_leader":
            raise BlackboxError(f"wrong-broker produce expected not_leader payload, got {body!r}")
        if int(body.get("leaderBrokerID", -1)) != wrong_leader:
            raise BlackboxError(f"wrong-broker produce leader mismatch: {body!r}")

        status, body = self.clients[wrong_broker].request_json(
            "GET",
            f"/api/topics/{self.topic}/partitions/{wrong_pid}/messages",
            expected_status=None,
        )
        if status != 409:
            raise BlackboxError(f"wrong-broker fetch expected 409, got {status}")
        if not isinstance(body, dict) or body.get("error") != "not_leader":
            raise BlackboxError(f"wrong-broker fetch expected not_leader payload, got {body!r}")

        self.wait_full_isr()

    def scenario_restart(self) -> None:
        cfg = self.settings.profile
        if len(self.expected_per_partition) != cfg.partitions:
            raise BlackboxError(
                f"restart requires distributed flow baseline: expected {cfg.partitions} partitions, "
                f"got {len(self.expected_per_partition)}"
            )

        expected_before_restart = dict(self.expected_per_partition)
        self.wait_broker_health(2)

        self.stack_restart("broker2")
        self.wait_broker_health(2, timeout_sec=120.0)
        self.wait_cluster_brokers()
        self.wait_full_isr()

        leaders = self.partition_leaders()
        expected_total = dict(expected_before_restart)
        produced_after_restart: dict[int, list[str]] = {pid: [] for pid in range(cfg.partitions)}
        for pid in range(cfg.partitions):
            leader = leaders[pid]
            for i in range(cfg.restart_messages_per_partition):
                payload = f"after-restart|run={self.run_id}|p={pid}|i={i}"
                self.produce(leader, pid, payload)
                expected_total[pid] += 1
                produced_after_restart[pid].append(payload)

        for pid in range(cfg.partitions):
            leader = leaders[pid]
            expected_count = expected_total[pid]
            messages = self.fetch(leader, pid, expected_count + 20)
            if len(messages) != expected_count:
                raise BlackboxError(
                    f"after restart partition {pid}: expected {expected_count} messages, got {len(messages)}"
                )

            offsets = [int(m.get("offset", -1)) for m in messages]
            want_offsets = list(range(expected_count - 1, -1, -1))
            if offsets != want_offsets:
                raise BlackboxError(
                    f"after restart partition {pid}: offsets mismatch, "
                    f"want tail {want_offsets[:10]} got tail {offsets[:10]}"
                )

            values = {str(m.get("value")) for m in messages if isinstance(m.get("value"), str)}
            has_pre_restart = any(v.startswith(f"multi|run={self.run_id}|") for v in values)
            if not has_pre_restart:
                raise BlackboxError(
                    f"after restart partition {pid}: pre-restart values are missing from partition snapshot"
                )

            for payload in produced_after_restart[pid]:
                if payload not in values:
                    raise BlackboxError(
                        f"after restart partition {pid}: marker {payload!r} not found in fetch window"
                    )

        self.expected_per_partition = expected_total
        self.wait_full_isr()

    def scenario_metrics(self) -> None:
        summaries: dict[int, dict[str, Any]] = {}
        produced_metrics: dict[int, float] = {}
        consumed_metrics: dict[int, float] = {}

        for broker_id in (1, 2):
            summary = self.clients[broker_id].get_json("/api/summary")
            if not isinstance(summary, dict):
                raise BlackboxError(f"broker{broker_id} summary payload invalid")
            for key in ("topics", "partitions", "produced", "consumed", "errors"):
                if key not in summary:
                    raise BlackboxError(f"broker{broker_id} summary missing key {key!r}")
            summaries[broker_id] = summary

            _, metrics = self.clients[broker_id].request("GET", "/metrics", expected_status=200)
            produced = sum_metric(metrics, "wavemq_messages_produced_total")
            consumed = sum_metric(metrics, "wavemq_messages_consumed_total")
            produced_metrics[broker_id] = produced
            consumed_metrics[broker_id] = consumed
            if produced <= 0:
                raise BlackboxError(f"broker{broker_id} produced metric must be > 0")
            if consumed < 0:
                raise BlackboxError(f"broker{broker_id} consumed metric must be >= 0")

        expected_total = sum(self.expected_per_partition.values())
        produced_summary_total = sum(float(s.get("produced", -1)) for s in summaries.values())
        consumed_summary_total = sum(float(s.get("consumed", -1)) for s in summaries.values())
        produced_metric_total = sum(produced_metrics.values())
        consumed_metric_total = sum(consumed_metrics.values())

        if expected_total > 0:
            if produced_summary_total < expected_total:
                raise BlackboxError(
                    f"summary produced total {produced_summary_total} < expected at least {expected_total}"
                )
            if produced_metric_total < expected_total:
                raise BlackboxError(
                    f"metrics produced total {produced_metric_total} < expected at least {expected_total}"
                )
            if consumed_summary_total <= 0:
                raise BlackboxError("summary consumed total must be > 0 after test flow")
            if consumed_metric_total <= 0:
                raise BlackboxError("metrics consumed total must be > 0 after test flow")

    restart_callback: Callable[[str], None] | None = None

    def stack_restart(self, service: str) -> None:
        if self.restart_callback is None:
            raise BlackboxError("restart callback is not configured")
        self.restart_callback(service)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="wave-mq multi-broker blackbox suite")
    parser.add_argument("--profile", choices=sorted(PROFILES.keys()), default="full", help="test load profile")
    parser.add_argument("--http-host", default="127.0.0.1", help="HTTP host")
    parser.add_argument("--http-port-1", type=int, default=28091, help="broker1 HTTP port")
    parser.add_argument("--http-port-2", type=int, default=28092, help="broker2 HTTP port")
    parser.add_argument("--binary-port-1", type=int, default=27912, help="broker1 binary port for docker mapping")
    parser.add_argument("--binary-port-2", type=int, default=28912, help="broker2 binary port for docker mapping")
    parser.add_argument("--mqtt-port-1", type=int, default=21883, help="broker1 MQTT port for docker mapping")
    parser.add_argument("--mqtt-port-2", type=int, default=22883, help="broker2 MQTT port for docker mapping")
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout (sec)")
    parser.add_argument("--no-docker", action="store_true", help="do not manage Docker stack")
    parser.add_argument("--keep-stack", action="store_true", help="keep docker stack running after suite")
    parser.add_argument("--skip-restart", action="store_true", help="skip restart scenario")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    script_dir = Path(__file__).resolve().parent
    compose_file = script_dir / "docker-compose.blackbox.multi.yml"
    profile = PROFILES[args.profile]

    settings = SuiteSettings(
        http_host=args.http_host,
        http_port_1=args.http_port_1,
        http_port_2=args.http_port_2,
        profile=profile,
        restart_enabled=(not args.no_docker and not args.skip_restart),
        timeout_sec=args.timeout,
    )
    suite = MultiBlackboxSuite(settings)

    stack: DockerStack | None = None
    if not args.no_docker:
        env = {
            "WAVEMQ_MBB_HTTP_PORT_1": str(args.http_port_1),
            "WAVEMQ_MBB_HTTP_PORT_2": str(args.http_port_2),
            "WAVEMQ_MBB_BINARY_PORT_1": str(args.binary_port_1),
            "WAVEMQ_MBB_BINARY_PORT_2": str(args.binary_port_2),
            "WAVEMQ_MBB_MQTT_PORT_1": str(args.mqtt_port_1),
            "WAVEMQ_MBB_MQTT_PORT_2": str(args.mqtt_port_2),
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

        suite.restart_callback = stack.restart

    try:
        suite.run()
        log("all scenarios passed")
        return 0
    except Exception as exc:  # noqa: BLE001
        log(f"blackbox failed: {exc}")
        if stack is not None:
            rollback_marker = "rollback failed: tx closed"
            for service in ("broker1", "broker2"):
                try:
                    logs = stack.logs(service, tail=200)
                except Exception as logs_exc:  # noqa: BLE001
                    log(f"cannot fetch {service} logs: {logs_exc}")
                else:
                    print(f"\n===== {service} logs (tail) =====")
                    print(logs)
                    print("===== end logs =====\n")
                    marker_lines = find_log_marker_lines(logs, rollback_marker)
                    if marker_lines:
                        print(f"===== {service} diagnostics: {rollback_marker!r} =====")
                        for line in marker_lines:
                            print(line)
                        print("===== end diagnostics =====\n")
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
