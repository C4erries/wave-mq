#!/usr/bin/env bash

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
MBIN="$ROOT/mbd"
DATA1="$ROOT/data-cluster-1"
DATA2="$ROOT/data-cluster-2"
RAFT_DIR1="$DATA1/raft"
RAFT_DIR2="$DATA2/raft"
LOG1="$ROOT/mbd-cluster-1.log"
LOG2="$ROOT/mbd-cluster-2.log"
HTTP1="127.0.0.1:8091"
HTTP2="127.0.0.1:8092"
BINARY1=":7912"
BINARY2=":8912"
RAFT_PEERS="127.0.0.1:9001,127.0.0.1:9002"
PYTHON_CMD=""

if command -v python3 >/dev/null 2>&1; then
  PYTHON_CMD=python3
elif command -v python >/dev/null 2>&1; then
  PYTHON_CMD=python
else
  echo "python3/python is required" >&2
  exit 1
fi

cleanup() {
  set +e
  [[ -n "${PID1-}" ]] && kill "$PID1"
  [[ -n "${PID2-}" ]] && kill "$PID2"
  rm -rf "$TMP_DIR"
  set -e
}
trap cleanup EXIT

rm -rf "$DATA1" "$DATA2"
mkdir -p "$DATA1" "$DATA2"
TMP_DIR="$(mktemp -d)"

echo "building mbd binary..."
go build -o "$MBIN" ./cmd/mbd

start_broker() {
  local id=$1
  local data_dir=$2
  local raft_dir=$3
  local bind=$4
  local http=$5
  local log=$6
  local pid_var=$7
  local extra_peers=$RAFT_PEERS

  mkdir -p "$raft_dir"
  "$MBIN" \
    -broker-id="$id" \
    -controller=raft \
    -raft-bind="127.0.0.1:90${id}" \
    -raft-peer="$extra_peers" \
    -raft-dir="$raft_dir" \
    -data-dir="$data_dir" \
    -bind="$bind" \
    -http="$http" \
    -replication=true \
    > "$log" 2>&1 &
  eval "$pid_var=$!"
  echo "broker $id starting (pid ${!pid_var}, http $http)"
}

start_broker 1 "$DATA1" "$RAFT_DIR1" "$BINARY1" "$HTTP1" "$LOG1" PID1
start_broker 2 "$DATA2" "$RAFT_DIR2" "$BINARY2" "$HTTP2" "$LOG2" PID2

controller_state() {
  local addr=$1
  local resp
  resp=$(curl -s --max-time 1 "http://$addr/api/controller" || true)
  if [[ -z "$resp" ]]; then
    echo ""
    return
  fi
  printf '%s\n' "$resp" | "$PYTHON_CMD" - <<'PY'
import json, sys
data=json.load(sys.stdin)
state=data.get("raftState","none")
cluster=data.get("clusterID","")
version=data.get("version",0)
print(state, cluster, version)
PY
}

discover_leader() {
  for i in {1..30}; do
    for endpoint in "$HTTP1" "$HTTP2"; do
      read -r state cluster version <<< "$(controller_state "$endpoint")"
      if [[ "$state" == "leader" ]]; then
        echo "$endpoint"
        return 0
      fi
    done
    sleep 1
  done
  return 1
}

leader_http=$(discover_leader)
if [[ -z "$leader_http" ]]; then
  echo "failed to detect Raft leader" >&2
  exit 1
fi
follower_http="$([[ "$leader_http" == "$HTTP1" ]] && echo "$HTTP2" || echo "$HTTP1")"

topic_url="http://$leader_http/api/topics"
partition_url="http://$leader_http/api/topics/cluster-demo/partitions/0/messages"

create_payload='{"name":"cluster-demo","partitions":1,"replicationFactor":2}'
topic_status=$(curl -s -o "$TMP_DIR/topic.json" -w "%{http_code}" -X POST -H "Content-Type: application/json" -d "$create_payload" "$topic_url")
if [[ "$topic_status" != "201" ]]; then
  echo "create topic failed (status $topic_status)"
  cat "$TMP_DIR/topic.json"
  exit 1
fi

produce_status=$(curl -s -o "$TMP_DIR/produce.json" -w "%{http_code}" -X POST -H "Content-Type: application/json" -d '{"value":"hello"}' "$partition_url")
if [[ "$produce_status" != "200" ]]; then
  echo "produce failed (status $produce_status)"
  cat "$TMP_DIR/produce.json"
  exit 1
fi

leader_fetch_status=$(curl -s -o "$TMP_DIR/leader-fetch.json" -w "%{http_code}" "http://$leader_http/api/topics/cluster-demo/partitions/0/messages")
if [[ "$leader_fetch_status" != "200" ]]; then
  echo "leader fetch failed (status $leader_fetch_status)"
  cat "$TMP_DIR/leader-fetch.json"
  exit 1
fi

follower_fetch_status=$(curl -s -o "$TMP_DIR/follower-fetch.json" -w "%{http_code}" "http://$follower_http/api/topics/cluster-demo/partitions/0/messages")
if [[ "$follower_fetch_status" != "409" ]]; then
  echo "expected follower fetch to be rejected, got $follower_fetch_status"
  cat "$TMP_DIR/follower-fetch.json"
  exit 1
fi
leader_id=$("$PYTHON_CMD" - <<'PY'
import json, sys
data=json.load(open("$TMP_DIR/follower-fetch.json"))
print(data.get("leaderBrokerID",""))
PY
)
echo "follower correctly rejected fetch in favor of leader $leader_id"

echo "restarting follower broker (id=2)..."
kill "$PID2"
wait "$PID2"

start_broker 2 "$DATA2" "$RAFT_DIR2" "$BINARY2" "$HTTP2" "$LOG2" PID2
leader_http=$(discover_leader)
if [[ -z "$leader_http" ]]; then
  echo "failed to detect Raft leader after restart" >&2
  exit 1
fi
follower_http="$([[ "$leader_http" == "$HTTP1" ]] && echo "$HTTP2" || echo "$HTTP1")"
topic_url="http://$leader_http/api/topics"
partition_url="http://$leader_http/api/topics/cluster-demo/partitions/0/messages"

produce_status=$(curl -s -o "$TMP_DIR/produce2.json" -w "%{http_code}" -X POST -H "Content-Type: application/json" -d '{"value":"after-restart"}' "$partition_url")
if [[ "$produce_status" != "200" ]]; then
  echo "produce after restart failed (status $produce_status)"
  cat "$TMP_DIR/produce2.json"
  exit 1
fi

echo "waiting for follower to catch up..."
for i in {1..30}; do
  status=$(curl -s -o "$TMP_DIR/follower-fetch.json" -w "%{http_code}" "http://$follower_http/api/topics/cluster-demo/partitions/0/messages")
  if [[ "$status" == "200" ]]; then
    break
  fi
  sleep 1
done
if [[ "$status" != "200" ]]; then
  echo "follower did not catch up; last status $status"
  exit 1
fi

value=$( "$PYTHON_CMD" - <<'PY'
import json
data=json.load(open("$TMP_DIR/follower-fetch.json"))
print(data[0].get("value",""))
PY
)
if [[ "$value" != "after-restart" ]]; then
  echo "expected most recent value after restart to be 'after-restart', got '$value'"
  exit 1
fi

echo "cluster demo succeeded: follower caught up and returns new records"
