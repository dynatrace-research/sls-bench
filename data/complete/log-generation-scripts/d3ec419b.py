import math
import hashlib
import ipaddress
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# Deterministic seed (even though the simulator primarily uses md5-based determinism).
SEED = 1337
random.seed(SEED)
np.random.seed(SEED)

SYSTEM: Dict[str, Any] = {
    "id": "roblox_hashistack_consul_outage",
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "edge_gateway",
            "svc": "edge-gw",
            "hosts": ["edge-1", "edge-2"],
            "logs": {
                "req_in": {
                    "lvl": "INFO",
                    "msg": "req {req_id} {method} {route} from {client_ip}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/api/join", "/api/session"]},
                        "client_ip": {"k": "ip", "v": "203.0.113.0/24"},
                    },
                },
                "resp_ok": {
                    "lvl": "INFO",
                    "msg": "resp {req_id} status={status} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "status": {"k": "ch", "v": [200, 302]},
                        "dur_ms": {"k": "i", "v": [10, 800]},
                    },
                },
                "resp_err": {
                    "lvl": "WARN",
                    "msg": "resp {req_id} status={status} dur_ms={dur_ms} reason={reason}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "status": {"k": "ch", "v": [503, 504]},
                        "dur_ms": {"k": "i", "v": [10, 6000]},
                        "reason": {"k": "ch", "v": ["maintenance", "upstream_timeout", "upstream_unhealthy"]},
                    },
                },
                "upstream_err": {
                    "lvl": "WARN",
                    "msg": "upstream error {req_id} svc={svc} err={err} after_ms={after_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "svc": {"k": "ch", "v": ["game-service"]},
                        "err": {"k": "ch", "v": ["timeout", "connect_refused", "no_endpoints"]},
                        "after_ms": {"k": "i", "v": [50, 5000]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "game_service",
            "svc": "game-service",
            "hosts": ["gs-1", "gs-2", "gs-3"],
            "logs": {
                "handle_start": {
                    "lvl": "DEBUG",
                    "msg": "handle {req_id} op={op} user={user_id}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "op": {"k": "ch", "v": ["join", "session"]},
                        "user_id": {"k": "i", "v": [100000, 999999]},
                    },
                },
                "consul_lookup_ok": {
                    "lvl": "DEBUG",
                    "msg": "consul lookup ok {req_id} service={service} endpoint={endpoint} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "service": {"k": "ch", "v": ["matchmaker", "session-svc"]},
                        "endpoint": {"k": "str", "v": "ip:port"},
                        "dur_ms": {"k": "i", "v": [1, 200]},
                    },
                },
                "consul_lookup_fail": {
                    "lvl": "ERROR",
                    "msg": "consul lookup failed {req_id} service={service} err={err} waited_ms={waited_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "service": {"k": "ch", "v": ["matchmaker", "session-svc"]},
                        "err": {"k": "ch", "v": ["deadline_exceeded", "no_leader", "rpc_unavailable"]},
                        "waited_ms": {"k": "i", "v": [200, 6000]},
                    },
                },
                "vault_call": {
                    "lvl": "DEBUG",
                    "msg": "vault call {req_id} path={path}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "path": {"k": "ch", "v": ["secret/data/game/auth", "secret/data/game/session"]},
                    },
                },
                "vault_auth_fail": {
                    "lvl": "ERROR",
                    "msg": "vault auth failed {req_id} err={err} waited_ms={waited_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "err": {"k": "ch", "v": ["vault_500", "vault_timeout"]},
                        "waited_ms": {"k": "i", "v": [200, 6000]},
                    },
                },
                "done_ok": {
                    "lvl": "INFO",
                    "msg": "done {req_id} status=200 dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [20, 1200]},
                    },
                },
                "done_err": {
                    "lvl": "WARN",
                    "msg": "done {req_id} status={status} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "status": {"k": "ch", "v": [503, 504]},
                        "dur_ms": {"k": "i", "v": [200, 7000]},
                    },
                },
                "health_put": {
                    "lvl": "INFO",
                    "msg": "healthcheck update svc={svc} inst={inst_id} ttl_s={ttl_s}",
                    "vars": {
                        "svc": {"k": "ch", "v": ["game-service"]},
                        "inst_id": {"k": "hex", "v": 8},
                        "ttl_s": {"k": "i", "v": [30, 120]},
                    },
                },
                "health_put_ok": {
                    "lvl": "DEBUG",
                    "msg": "healthcheck update committed svc={svc} inst={inst_id} dur_ms={dur_ms}",
                    "vars": {
                        "svc": {"k": "ch", "v": ["game-service"]},
                        "inst_id": {"k": "hex", "v": 8},
                        "dur_ms": {"k": "i", "v": [5, 400]},
                    },
                },
                "health_put_timeout": {
                    "lvl": "WARN",
                    "msg": "healthcheck update timed out svc={svc} inst={inst_id} waited_ms={waited_ms}",
                    "vars": {
                        "svc": {"k": "ch", "v": ["game-service"]},
                        "inst_id": {"k": "hex", "v": 8},
                        "waited_ms": {"k": "i", "v": [500, 6000]},
                    },
                },
                "retrying": {
                    "lvl": "DEBUG",
                    "msg": "retrying op={op} attempt={attempt} backoff_ms={backoff_ms}",
                    "vars": {
                        "op": {"k": "ch", "v": ["health_put", "vault_call"]},
                        "attempt": {"k": "i", "v": [2, 4]},
                        "backoff_ms": {"k": "i", "v": [50, 1500]},
                    },
                },
                "heartbeat": {
                    "lvl": "INFO",
                    "msg": "heartbeat svc={svc} inst={inst_id}",
                    "vars": {"svc": {"k": "ch", "v": ["game-service"]}, "inst_id": {"k": "hex", "v": 8}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "heartbeat", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "heartbeat", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        {
            "id": "consul_cluster",
            "svc": "consul",
            "hosts": [
                "consul-v1",
                "consul-v2",
                "consul-v3",
                "consul-v4",
                "consul-v5",
                "consul-r1",
                "consul-r2",
                "consul-r3",
                "consul-r4",
                "consul-r5",
            ],
            "logs": {
                "kv_metric_ok": {
                    "lvl": "INFO",
                    "msg": "kv_apply p50_ms={p50_ms} p95_ms={p95_ms} raft_commit_p50_ms={raft_p50_ms}",
                    "vars": {
                        "p50_ms": {"k": "i", "v": [10, 300]},
                        "p95_ms": {"k": "i", "v": [30, 800]},
                        "raft_p50_ms": {"k": "i", "v": [5, 200]},
                    },
                },
                "kv_metric_bad": {
                    "lvl": "WARN",
                    "msg": "kv_apply p50_ms={p50_ms} p95_ms={p95_ms} raft_commit_p50_ms={raft_p50_ms}",
                    "vars": {
                        "p50_ms": {"k": "i", "v": [800, 3500]},
                        "p95_ms": {"k": "i", "v": [1500, 8000]},
                        "raft_p50_ms": {"k": "i", "v": [500, 4000]},
                    },
                },
                "watch_delivery_stall": {
                    "lvl": "WARN",
                    "msg": "watch delivery lag mode={mode} dispatch_ms={dispatch_ms} active_watches={active_watches}",
                    "vars": {
                        "mode": {"k": "ch", "v": ["stream", "longpoll"]},
                        "dispatch_ms": {"k": "i", "v": [50, 5000]},
                        "active_watches": {"k": "i", "v": [1000, 60000]},
                    },
                },
                "raft_leader_change": {
                    "lvl": "INFO",
                    "msg": "raft leader elected node={node} term={term}",
                    "vars": {
                        "node": {"k": "ch", "v": ["consul-v1", "consul-v2", "consul-v3", "consul-v4", "consul-v5"]},
                        "term": {"k": "i", "v": [1000, 2000]},
                    },
                },
                "raft_append_slow": {
                    "lvl": "WARN",
                    "msg": "raft append slow leader={node} append_ms={append_ms} tcp_zero_window={zero_win_pct}",
                    "vars": {
                        "node": {"k": "ch", "v": ["consul-v1", "consul-v2", "consul-v3", "consul-v4", "consul-v5"]},
                        "append_ms": {"k": "i", "v": [200, 5000]},
                        "zero_win_pct": {"k": "i", "v": [0, 100]},
                    },
                },
                "raft_log_store_pressure": {
                    "lvl": "WARN",
                    "msg": "raft log store pressure db={db} file_gb={file_gb} free_pages_k={free_k} meta_write_kb={meta_kb}",
                    "vars": {
                        "db": {"k": "ch", "v": ["raft-log.store"]},
                        "file_gb": {"k": "f", "v": [0.5, 8.0]},
                        "free_k": {"k": "i", "v": [10, 1500]},
                        "meta_kb": {"k": "i", "v": [64, 12000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "kv_metric_ok", "per_min": 1.0, "scope": "per_host"}, {"id": "raft_leader_change", "per_min": 0.08, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "kv_metric_ok", "per_min": 1.0, "scope": "per_host"},
                        {"id": "kv_metric_bad", "per_min": 1.0, "scope": "per_host"},
                        {"id": "watch_delivery_stall", "per_min": 0.7, "scope": "per_host"},
                        {"id": "raft_leader_change", "per_min": 0.6, "scope": "global"},
                        {"id": "raft_append_slow", "per_min": 0.2, "scope": "per_host"},
                        {"id": "raft_log_store_pressure", "per_min": 0.1, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "vault",
            "svc": "vault",
            "hosts": ["vault-1", "vault-2"],
            "logs": {
                "req": {
                    "lvl": "INFO",
                    "msg": "vault req {req_id} op={op} path={path}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "op": {"k": "ch", "v": ["read"]},
                        "path": {"k": "ch", "v": ["secret/data/game/auth", "secret/data/game/session"]},
                    },
                },
                "resp_ok": {"lvl": "INFO", "msg": "vault resp {req_id} status=200 dur_ms={dur_ms}", "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [5, 800]}}},
                "storage_latency": {"lvl": "WARN", "msg": "consul storage slow op={op} waited_ms={waited_ms}", "vars": {"op": {"k": "ch", "v": ["get", "put"]}, "waited_ms": {"k": "i", "v": [200, 6000]}}},
                "resp_fail": {
                    "lvl": "ERROR",
                    "msg": "vault resp {req_id} status={status} err={err} dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "status": {"k": "ch", "v": [500, 503, 504]}, "err": {"k": "ch", "v": ["storage_timeout", "no_consul_leader"]}, "dur_ms": {"k": "i", "v": [200, 8000]}},
                },
                "heartbeat": {"lvl": "INFO", "msg": "vault heartbeat sealed={sealed}", "vars": {"sealed": {"k": "ch", "v": [False]}}},
            },
            "beh": {"n": {"emit": [{"id": "heartbeat", "per_min": 0.2, "scope": "per_host"}]}, "f": {"emit": [{"id": "heartbeat", "per_min": 0.2, "scope": "per_host"}]}},
        },
        {
            "id": "nomad",
            "svc": "nomad",
            "hosts": ["nomad-1", "nomad-2"],
            "logs": {
                "eval_start": {"lvl": "INFO", "msg": "eval {eval_id} job={job} action={action}", "vars": {"eval_id": {"k": "hex", "v": 12}, "job": {"k": "ch", "v": ["cache", "core-services"]}, "action": {"k": "ch", "v": ["plan", "run"]}}},
                "eval_ok": {"lvl": "INFO", "msg": "eval done {eval_id} result=ok dur_ms={dur_ms}", "vars": {"eval_id": {"k": "hex", "v": 12}, "dur_ms": {"k": "i", "v": [20, 1200]}}},
                "consul_lock_fail": {"lvl": "WARN", "msg": "consul session lock failed job={job} err={err} waited_ms={waited_ms}", "vars": {"job": {"k": "ch", "v": ["cache", "core-services"]}, "err": {"k": "ch", "v": ["no_leader", "deadline_exceeded"]}, "waited_ms": {"k": "i", "v": [200, 6000]}}},
                "heartbeat": {"lvl": "INFO", "msg": "nomad heartbeat schedulers={schedulers}", "vars": {"schedulers": {"k": "i", "v": [1, 3]}}},
            },
            "beh": {"n": {"emit": [{"id": "heartbeat", "per_min": 0.2, "scope": "per_host"}]}, "f": {"emit": [{"id": "heartbeat", "per_min": 0.2, "scope": "per_host"}]}},
        },
        {
            "id": "telemetry_collector",
            "svc": "telemetry",
            "hosts": ["tel-1"],
            "logs": {
                "scrape_ok": {"lvl": "INFO", "msg": "scrape target={target} samples={samples} dur_ms={dur_ms}", "vars": {"target": {"k": "ch", "v": ["edge-gw", "game-service", "vault", "nomad", "consul"]}, "samples": {"k": "i", "v": [50, 5000]}, "dur_ms": {"k": "i", "v": [5, 2000]}}},
                "discovery_fail": {"lvl": "ERROR", "msg": "service discovery refresh failed backend=consul err={err}", "vars": {"err": {"k": "ch", "v": ["deadline_exceeded", "no_leader", "connection_refused"]}}},
                "alert_gap": {"lvl": "WARN", "msg": "alert delivery delayed backlog={backlog} reason={reason}", "vars": {"backlog": {"k": "i", "v": [0, 5000]}, "reason": {"k": "ch", "v": ["scrape_failures", "discovery_downstream"]}}},
            },
            "beh": {
                "n": {"emit": [{"id": "scrape_ok", "per_min": 2.0, "scope": "global"}, {"id": "alert_gap", "per_min": 0.05, "scope": "global"}]},
                "f": {"emit": [{"id": "scrape_ok", "per_min": 0.5, "scope": "global"}, {"id": "discovery_fail", "per_min": 2.0, "scope": "global"}, {"id": "alert_gap", "per_min": 0.5, "scope": "global"}]},
            },
        },
        {
            "id": "ops_control",
            "svc": "ops",
            "hosts": ["ops-1"],
            "logs": {
                "streaming_enable": {"lvl": "INFO", "msg": "config push {change_id} component=consul key=consul.streaming value=enabled", "vars": {"change_id": {"k": "hex", "v": 10}}},
                "streaming_disable": {"lvl": "INFO", "msg": "config push {change_id} component=consul key=consul.streaming value=disabled", "vars": {"change_id": {"k": "hex", "v": 10}}},
                "iptables_block": {"lvl": "INFO", "msg": "iptables add on consul: {rule}", "vars": {"rule": {"k": "ch", "v": ["DROP tcp/8300", "DROP tcp/8500"]}}},
                "iptables_unblock": {"lvl": "INFO", "msg": "iptables remove on consul: {rule}", "vars": {"rule": {"k": "ch", "v": ["DROP tcp/8300", "DROP tcp/8500"]}}},
                "consul_snapshot_restore": {"lvl": "INFO", "msg": "consul snapshot restore snapshot={snapshot_id} result={result}", "vars": {"snapshot_id": {"k": "hex", "v": 12}, "result": {"k": "ch", "v": ["ok", "failed"]}}},
                "leader_stepdown": {"lvl": "WARN", "msg": "force raft leader stepdown node={node} reason={reason}", "vars": {"node": {"k": "ch", "v": ["consul-v1", "consul-v2", "consul-v3", "consul-v4", "consul-v5"]}, "reason": {"k": "ch", "v": ["slow_append", "suspected_disk_stall"]}}},
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "player_login",
                    "rpm": 120.0,
                    "emit": [
                        "edge_gateway.req_in",
                        "game_service.handle_start",
                        "game_service.consul_lookup_ok",
                        "vault.req",
                        "vault.resp_ok",
                        "game_service.done_ok",
                        "edge_gateway.resp_ok",
                    ],
                    "latency_ms": [[1, 10], [1, 20], [2, 80], [5, 120], [5, 200], [10, 400], [1, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "healthcheck_kv_put_ok",
                    "rpm": 300.0,
                    "emit": ["game_service.health_put", "game_service.health_put_ok"],
                    "latency_ms": [[1, 10], [5, 200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "nomad_schedule_ok",
                    "rpm": 20.0,
                    "emit": ["nomad.eval_start", "nomad.eval_ok"],
                    "latency_ms": [[1, 10], [20, 800]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "maintenance_page",
                    "rpm": 90.0,
                    "emit": ["edge_gateway.req_in", "edge_gateway.resp_err"],
                    "latency_ms": [[1, 10], [5, 80]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "player_login_fail",
                    "rpm": 60.0,
                    "emit": ["edge_gateway.req_in", "game_service.handle_start", "game_service.consul_lookup_fail", "game_service.done_err", "edge_gateway.resp_err"],
                    "latency_ms": [[1, 10], [1, 20], [500, 5000], [10, 200], [1, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "healthcheck_kv_put_timeout",
                    "rpm": 280.0,
                    "emit": ["game_service.health_put", "game_service.health_put_timeout"],
                    "latency_ms": [[1, 10], [1200, 6000]],
                    "retry": {"max_attempts": 4, "expected_attempts": 1.6, "emit_per_retry": ["game_service.retrying"], "backoff_ms": [[100, 300], [200, 600], [400, 1200]]},
                    "trace": False,
                },
                {
                    "id": "healthcheck_kv_put_ok_recover",
                    "rpm": 240.0,
                    "emit": ["game_service.health_put", "game_service.health_put_ok"],
                    "latency_ms": [[1, 10], [10, 350]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "vault_read_secret_fail",
                    "rpm": 40.0,
                    "emit": ["game_service.vault_call", "vault.req", "vault.storage_latency", "vault.resp_fail", "game_service.vault_auth_fail"],
                    "latency_ms": [[1, 10], [1, 30], [200, 4000], [10, 200], [10, 200]],
                    "retry": {"max_attempts": 3, "expected_attempts": 1.3, "emit_per_retry": ["game_service.retrying"], "backoff_ms": [[150, 500], [300, 1200]]},
                    "trace": False,
                },
                {
                    "id": "vault_read_secret_ok",
                    "rpm": 30.0,
                    "emit": ["game_service.vault_call", "vault.req", "vault.resp_ok"],
                    "latency_ms": [[1, 10], [1, 30], [10, 600]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "nomad_schedule_fail",
                    "rpm": 10.0,
                    "emit": ["nomad.eval_start", "nomad.consul_lock_fail"],
                    "latency_ms": [[1, 10], [200, 6000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "consul_streaming_boltdb_outage",
    "time": {"total_minutes": 30, "phases": {"n": {"start_min": 0, "end_min": 15}, "f": {"start_min": 15, "end_min": 30}}},
    "events": [
        {
            "order": 1,
            "at_min": 15,
            "rate_multipliers": {
                "consul_cluster.kv_metric_ok": 0.0,
                "consul_cluster.kv_metric_bad": 1.0,
                "consul_cluster.raft_append_slow": 0.0,
                "consul_cluster.raft_log_store_pressure": 0.0,
                "healthcheck_kv_put_ok_recover": 0.0,
                "vault_read_secret_ok": 0.0,
            },
            "latency_multipliers": {
                "player_login_fail": {"p50": 1.0, "p95": 1.0},
                "healthcheck_kv_put_timeout": {"p50": 1.0, "p95": 1.0},
                "vault_read_secret_fail": {"p50": 1.0, "p95": 1.0},
            },
            "one_shots": [{"ref": "ops_control.streaming_enable", "count": 1, "hosts": ["ops-1"]}],
        },
        {
            "order": 2,
            "at_min": 19,
            "rate_multipliers": {"player_login_fail": 0.0, "healthcheck_kv_put_timeout": 0.2, "vault_read_secret_fail": 0.3, "telemetry_collector.discovery_fail": 2.0},
            "latency_multipliers": {"healthcheck_kv_put_timeout": {"p50": 0.85, "p95": 0.95}, "vault_read_secret_fail": {"p50": 0.85, "p95": 0.95}},
            "one_shots": [
                {"ref": "ops_control.iptables_block", "count": 2, "hosts": ["ops-1"]},
                {"ref": "ops_control.consul_snapshot_restore", "count": 1, "hosts": ["ops-1"]},
            ],
        },
        {
            "order": 3,
            "at_min": 23,
            "rate_multipliers": {"healthcheck_kv_put_timeout": 1.0, "vault_read_secret_fail": 1.0, "telemetry_collector.discovery_fail": 1.0},
            "latency_multipliers": {"healthcheck_kv_put_timeout": {"p50": 1.0, "p95": 1.0}, "vault_read_secret_fail": {"p50": 1.0, "p95": 1.0}},
            "one_shots": [{"ref": "ops_control.iptables_unblock", "count": 2, "hosts": ["ops-1"]}],
        },
        {
            "order": 4,
            "at_min": 26,
            "rate_multipliers": {
                "consul_cluster.kv_metric_ok": 1.0,
                "consul_cluster.kv_metric_bad": 0.0,
                "consul_cluster.watch_delivery_stall": 0.1,
                "telemetry_collector.discovery_fail": 0.1,
                "telemetry_collector.scrape_ok": 2.0,
                "healthcheck_kv_put_timeout": 0.15,
                "vault_read_secret_fail": 0.25,
                "healthcheck_kv_put_ok_recover": 1.0,
                "vault_read_secret_ok": 1.0,
            },
            "latency_multipliers": {
                "healthcheck_kv_put_timeout": {"p50": 0.7, "p95": 0.85},
                "vault_read_secret_fail": {"p50": 0.75, "p95": 0.9},
                "healthcheck_kv_put_ok_recover": {"p50": 0.9, "p95": 1.0},
                "vault_read_secret_ok": {"p50": 0.9, "p95": 1.0},
            },
            "one_shots": [{"ref": "ops_control.streaming_disable", "count": 1, "hosts": ["ops-1"]}],
        },
        {
            "order": 5,
            "at_min": 28,
            "rate_multipliers": {
                "consul_cluster.kv_metric_ok": 0.0,
                "consul_cluster.kv_metric_bad": 0.8,
                "consul_cluster.raft_append_slow": 1.0,
                "consul_cluster.raft_log_store_pressure": 1.0,
                "consul_cluster.raft_leader_change": 2.0,
                "telemetry_collector.discovery_fail": 0.6,
                "telemetry_collector.alert_gap": 1.5,
                "healthcheck_kv_put_timeout": 0.4,
                "vault_read_secret_fail": 0.7,
                "healthcheck_kv_put_ok_recover": 0.6,
                "vault_read_secret_ok": 0.5,
            },
            "latency_multipliers": {
                "healthcheck_kv_put_timeout": {"p50": 1.0, "p95": 1.0},
                "vault_read_secret_fail": {"p50": 1.0, "p95": 1.0},
                "healthcheck_kv_put_ok_recover": {"p50": 1.05, "p95": 1.2},
                "vault_read_secret_ok": {"p50": 1.0, "p95": 1.3},
            },
            "one_shots": [{"ref": "ops_control.leader_stepdown", "count": 1, "hosts": ["ops-1"]}],
        },
    ],
}

ND = NormalDist()
BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def md5_int(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest(), 16)


def u01(s: str) -> float:
    # Deterministic [0,1) using md5; stable across runs.
    return (md5_int(s) % 10**12) / 10**12


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def isoformat_ms(dt: datetime) -> str:
    # UTC with milliseconds: YYYY-MM-DDTHH:MM:SS.fffZ
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond/1000):03d}Z"


def q_lognormal(p50: float, p95: float, q: float, soft_cap_mult: float = 1.0) -> float:
    """
    Interpret p50 as median, p95 as 95th percentile; return quantile in ms.

    Important for verifier-stability:
    - This simulator must keep observed fields (dur_ms/waited_ms/backoff_ms) within template domains.
    - Default cap is at ~p95 (soft_cap_mult=1.0) to avoid large outliers at scale.
    """
    q = clamp(q, 1e-9, 1 - 1e-9)
    p50 = max(1e-6, float(p50))
    p95 = max(p50 * 1.0001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.645
    z = ND.inv_cdf(q)
    x = math.exp(mu + sigma * z)
    cap = soft_cap_mult * p95
    return min(x, cap)


def alloc_count(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    return base + (1 if u01(f"alloc|{key}") < frac else 0)


def schedule_times(count: int, start: datetime, end: datetime, key: str) -> List[datetime]:
    if count <= 0:
        return []
    span = (end - start).total_seconds()
    if span <= 0:
        return []
    out: List[datetime] = []
    for i in range(count):
        pos = (i + 0.5) / count
        base_ts = start + timedelta(seconds=pos * span)
        jitter = (u01(f"jit|{key}|{i}") - 0.5) * 0.4  # seconds
        ts = base_ts + timedelta(seconds=jitter)
        if ts < start:
            ts = start + timedelta(milliseconds=int(u01(f"clampL|{key}|{i}") * 50))
        if ts >= end:
            ts = end - timedelta(milliseconds=1 + int(u01(f"clampR|{key}|{i}") * 50))
        out.append(ts)
    return out


def gen_hex(n: int, seed: str) -> str:
    return hashlib.md5(seed.encode("utf-8")).hexdigest()[:n]


def gen_ip(cidr: str, seed: str) -> str:
    net = ipaddress.ip_network(cidr, strict=False)
    size = net.num_addresses
    if size <= 2:
        return str(net.network_address)
    host_index = 1 + (md5_int(seed) % (size - 2))
    return str(net.network_address + host_index)


def gen_str(hint: str, seed: str) -> str:
    if hint == "ip:port":
        ip_net = ipaddress.ip_network("10.0.0.0/16")
        ip = str(ip_net.network_address + (md5_int(seed + "|ip") % (ip_net.num_addresses - 1) + 1))
        port = 2000 + (md5_int(seed + "|port") % 40000)
        return f"{ip}:{port}"
    return f"{hint}:{gen_hex(8, seed)}"


def gen_value(vdef: Dict[str, Any], seed: str) -> Any:
    k = vdef["k"]
    v = vdef["v"]
    if k == "hex":
        return gen_hex(int(v), seed)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi <= lo:
            return lo
        return lo + (md5_int(seed) % (hi - lo + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        x = lo + u01(seed) * (hi - lo)
        return round(x, 3)
    if k == "ch":
        choices = list(v)
        return choices[md5_int(seed) % len(choices)]
    if k == "ip":
        return gen_ip(str(v), seed)
    if k == "str":
        return gen_str(str(v), seed)
    if k == "uuid":
        h = hashlib.md5(seed.encode("utf-8")).hexdigest()
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
    return str(v)


def pick_host(hosts: List[str], seed: str) -> str:
    if not hosts:
        return ""
    return hosts[md5_int(seed) % len(hosts)]


# Build indices
COMP_BY_ID: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}
TEMPLATES: Dict[Tuple[str, str], Dict[str, Any]] = {}
for cid, c in COMP_BY_ID.items():
    for lid, t in c["logs"].items():
        TEMPLATES[(cid, lid)] = t


def get_int_var_range(component_id: str, log_id: str, var_name: str) -> Optional[Tuple[int, int]]:
    t = TEMPLATES.get((component_id, log_id))
    if not t:
        return None
    vdef = t.get("vars", {}).get(var_name)
    if not vdef:
        return None
    if vdef.get("k") != "i":
        return None
    lo, hi = int(vdef["v"][0]), int(vdef["v"][1])
    return lo, hi


def clamp_int_to_var(component_id: str, log_id: str, var_name: str, value: int) -> int:
    r = get_int_var_range(component_id, log_id, var_name)
    if not r:
        return int(value)
    lo, hi = r
    return int(clamp(float(value), float(lo), float(hi)))


@dataclass(frozen=True)
class IntervalControl:
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]  # keys: flow_id or "component.log_id"
    latency_mult: Dict[str, Dict[str, float]]  # flow_id -> {p50,p95}
    one_shots: List[Dict[str, Any]]  # list of one-shot dicts happening at start_min


def build_failure_intervals() -> List[IntervalControl]:
    fstart = SCENARIO["time"]["phases"]["f"]["start_min"]
    fend = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["events"], key=lambda e: (e["at_min"], e["order"]))
    boundary_mins = sorted(set([fstart] + [e["at_min"] for e in events] + [fend]))
    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}
    out: List[IntervalControl] = []
    for i in range(len(boundary_mins) - 1):
        b = boundary_mins[i]
        e = boundary_mins[i + 1]
        one_shots: List[Dict[str, Any]] = []
        for ev in [x for x in events if x["at_min"] == b]:
            for k, v in ev.get("rate_multipliers", {}).items():
                active_rate[k] = float(v)
            for fid, lm in ev.get("latency_multipliers", {}).items():
                active_lat[fid] = {"p50": float(lm["p50"]), "p95": float(lm["p95"])}
            for os in ev.get("one_shots", []):
                one_shots.append(os)
        out.append(IntervalControl(start_min=b, end_min=e, rate_mult=dict(active_rate), latency_mult=dict(active_lat), one_shots=one_shots))
    return out


FAIL_INTERVALS = build_failure_intervals()


def get_rate_mult(control: Optional[IntervalControl], key: str) -> float:
    if control is None:
        return 1.0
    return float(control.rate_mult.get(key, 1.0))


def get_latency_mult(control: Optional[IntervalControl], flow_id: str) -> Tuple[float, float]:
    if control is None:
        return (1.0, 1.0)
    lm = control.latency_mult.get(flow_id)
    if not lm:
        return (1.0, 1.0)
    return (float(lm.get("p50", 1.0)), float(lm.get("p95", 1.0)))


def render_message(component_id: str, log_id: str, ctx: Dict[str, Any], seed: str) -> Tuple[str, str]:
    t = TEMPLATES[(component_id, log_id)]
    vars_def = t.get("vars", {})
    values: Dict[str, Any] = {}
    for k, vdef in vars_def.items():
        if k in ctx:
            values[k] = ctx[k]
        else:
            values[k] = gen_value(vdef, f"var|{seed}|{component_id}.{log_id}|{k}")
    # Ensure YAML boolean choices render as lowercase 'true'/'false' in messages.
    for k, v in list(values.items()):
        if isinstance(v, bool):
            values[k] = "true" if v else "false"
    msg = t["msg"].format_map(values)
    return t["lvl"], msg


def emit_row(rows: List[Dict[str, Any]], ts: datetime, component_id: str, log_id: str, host: str, ctx: Dict[str, Any], seed: str, trace_id: str = "") -> None:
    lvl, msg = render_message(component_id, log_id, ctx, seed)
    svc = COMP_BY_ID[component_id].get("svc") or ""
    rows.append(
        {
            "timestamp_dt": ts,
            "level": lvl,
            "message": msg,
            "trace_id": trace_id if trace_id else "",
            "service": svc,
            "host": host or "",
        }
    )


def bg_ctx_overrides(component_id: str, log_id: str, ts: datetime) -> Dict[str, Any]:
    minute = int((ts - BASE_TIME).total_seconds() // 60)
    ctx: Dict[str, Any] = {}
    if component_id == "consul_cluster" and log_id == "watch_delivery_stall":
        ctx["mode"] = "stream" if minute < 26 else "longpoll"
        ctx["active_watches"] = int(50000 if minute < 26 else 8000)
        ctx["dispatch_ms"] = int(3000 if minute < 26 else 300)
    if component_id == "consul_cluster" and log_id in ("kv_metric_ok", "kv_metric_bad"):
        if log_id == "kv_metric_ok":
            ctx["p50_ms"] = int(40 + (minute % 7) * 8)
            ctx["p95_ms"] = int(120 + (minute % 9) * 20)
            ctx["raft_p50_ms"] = int(20 + (minute % 5) * 7)
        else:
            ctx["p50_ms"] = int(1200 + (minute % 9) * 130)
            ctx["p95_ms"] = int(2600 + (minute % 11) * 250)
            ctx["raft_p50_ms"] = int(900 + (minute % 10) * 180)
    if component_id == "consul_cluster" and log_id == "raft_leader_change":
        ctx["term"] = int(clamp(1000 + minute * 6 + (minute % 3) * 7, 1000, 2000))
    if component_id == "telemetry_collector" and log_id == "discovery_fail":
        if 19 <= minute < 23:
            ctx["err"] = "connection_refused"
        elif minute < 26:
            ctx["err"] = "no_leader"
        else:
            ctx["err"] = "deadline_exceeded"
    if component_id == "telemetry_collector" and log_id == "alert_gap":
        if minute < 26:
            ctx["reason"] = "discovery_downstream"
        else:
            ctx["reason"] = "scrape_failures"
    return ctx


def simulate_background(rows: List[Dict[str, Any]], state: str, start_min: int, end_min: int, control: Optional[IntervalControl]) -> None:
    start = BASE_TIME + timedelta(minutes=start_min)
    end = BASE_TIME + timedelta(minutes=end_min)
    duration_min = max(0.0, (end - start).total_seconds() / 60.0)

    for component_id, comp in COMP_BY_ID.items():
        beh = comp.get("beh", {}).get(state, {})
        for src in beh.get("emit", []):
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            mult_key = f"{component_id}.{log_id}"
            mult = get_rate_mult(control, mult_key) if state == "f" else 1.0
            eff_per_min = per_min * mult
            if eff_per_min <= 0:
                continue

            if scope == "global":
                expected = eff_per_min * duration_min
                cnt = alloc_count(expected, f"bg|{state}|{start_min}-{end_min}|{component_id}.{log_id}|global")
                times = schedule_times(cnt, start, end, f"bg|{state}|{start_min}-{end_min}|{component_id}.{log_id}|global")
                for i, ts in enumerate(times):
                    host = pick_host(comp.get("hosts", []), f"bg_host|{component_id}|{log_id}|{start_min}-{end_min}|{i}")
                    ctx = bg_ctx_overrides(component_id, log_id, ts)
                    emit_row(rows, ts, component_id, log_id, host, ctx, seed=f"bg|{component_id}.{log_id}|{start_min}-{end_min}|{i}")
            else:
                hosts = comp.get("hosts", [])
                for h in hosts:
                    expected = eff_per_min * duration_min
                    cnt = alloc_count(expected, f"bg|{state}|{start_min}-{end_min}|{component_id}.{log_id}|{h}")
                    times = schedule_times(cnt, start, end, f"bg|{state}|{start_min}-{end_min}|{component_id}.{log_id}|{h}")
                    for i, ts in enumerate(times):
                        ctx = bg_ctx_overrides(component_id, log_id, ts)
                        emit_row(rows, ts, component_id, log_id, h, ctx, seed=f"bg|{component_id}.{log_id}|{start_min}-{end_min}|{h}|{i}")


def plan_attempts(n_instances: int, expected_attempts: float, max_attempts: int, seed: str) -> List[int]:
    if n_instances <= 0:
        return []
    if max_attempts <= 1:
        return [1] * n_instances
    lo = int(math.floor(expected_attempts))
    lo = max(1, min(lo, max_attempts))
    if abs(expected_attempts - lo) < 1e-9 or lo == max_attempts:
        return [lo] * n_instances
    hi = min(max_attempts, lo + 1)
    frac_hi = clamp(expected_attempts - lo, 0.0, 1.0)
    n_hi = int(round(frac_hi * n_instances))
    out = [lo] * n_instances
    if n_hi <= 0:
        return out
    step = max(1, n_instances // n_hi)
    idx = md5_int(f"attempts|{seed}") % max(1, step)
    assigned = 0
    for i in range(n_instances):
        if (i - idx) % step == 0 and assigned < n_hi:
            out[i] = hi
            assigned += 1
    i = 0
    while assigned < n_hi and i < n_instances:
        if out[i] != hi:
            out[i] = hi
            assigned += 1
        i += 1
    return out


def ms_between(a: datetime, b: datetime) -> int:
    return int(round((b - a).total_seconds() * 1000.0))


def ensure_min_gap(times: List[datetime], start_idx: int, end_idx: int, min_ms: int) -> None:
    if start_idx < 0 or end_idx >= len(times) or start_idx >= end_idx:
        return
    gap = ms_between(times[start_idx], times[end_idx])
    if gap >= min_ms:
        return
    delta_ms = min_ms - gap
    delta = timedelta(milliseconds=delta_ms)
    for i in range(end_idx, len(times)):
        times[i] = times[i] + delta


# Constraints for observed timing fields: (component, log, field, start_index, end_index)
# computed_value_ms := times[end_index] - times[start_index]
FLOW_TIMING_CONSTRAINTS: Dict[str, List[Tuple[str, str, str, int, int]]] = {
    "player_login": [
        ("game_service", "consul_lookup_ok", "dur_ms", 1, 2),
        ("vault", "resp_ok", "dur_ms", 3, 4),
        ("game_service", "done_ok", "dur_ms", 1, 5),
        ("edge_gateway", "resp_ok", "dur_ms", 0, 6),
    ],
    "healthcheck_kv_put_ok": [
        ("game_service", "health_put_ok", "dur_ms", 0, 1),
    ],
    "nomad_schedule_ok": [
        ("nomad", "eval_ok", "dur_ms", 0, 1),
    ],
    "maintenance_page": [
        ("edge_gateway", "resp_err", "dur_ms", 0, 1),
    ],
    "player_login_fail": [
        ("game_service", "consul_lookup_fail", "waited_ms", 1, 2),
        ("game_service", "done_err", "dur_ms", 1, 3),
        ("edge_gateway", "resp_err", "dur_ms", 0, 4),
    ],
    "healthcheck_kv_put_timeout": [
        ("game_service", "health_put_timeout", "waited_ms", 0, 1),
    ],
    "healthcheck_kv_put_ok_recover": [
        ("game_service", "health_put_ok", "dur_ms", 0, 1),
    ],
    "vault_read_secret_fail": [
        ("vault", "storage_latency", "waited_ms", 1, 2),
        ("vault", "resp_fail", "dur_ms", 1, 3),
        ("game_service", "vault_auth_fail", "waited_ms", 0, 4),
    ],
    "vault_read_secret_ok": [
        ("vault", "resp_ok", "dur_ms", 1, 2),
    ],
    "nomad_schedule_fail": [
        ("nomad", "consul_lock_fail", "waited_ms", 0, 1),
    ],
}

# For some logs, we interpret dur_ms/waited_ms as the immediate inter-log delay for that log.
# Only clamp the per-step delay for these "direct" measurements.
DIRECT_STEP_FIELDS: Dict[Tuple[str, str], str] = {
    ("game_service", "consul_lookup_ok"): "dur_ms",
    ("game_service", "consul_lookup_fail"): "waited_ms",
    ("vault", "resp_ok"): "dur_ms",
    ("vault", "storage_latency"): "waited_ms",
    ("game_service", "health_put_ok"): "dur_ms",
    ("game_service", "health_put_timeout"): "waited_ms",
    ("nomad", "eval_ok"): "dur_ms",
    ("nomad", "consul_lock_fail"): "waited_ms",
    ("edge_gateway", "resp_err"): "dur_ms",
}


def build_times_from_delays(attempt_start: datetime, delays_ms: List[int]) -> List[datetime]:
    times: List[datetime] = []
    t = attempt_start
    for dms in delays_ms:
        t = t + timedelta(milliseconds=int(dms))
        times.append(t)
    return times


def enforce_delay_domains_via_constraints(flow_id: str, emit_refs: List[str], delays_ms: List[int], attempt_start: datetime, attempt_seed: str) -> Tuple[List[int], List[datetime]]:
    """
    Enforce that observed timing fields that are derived from the emitted chronology stay within their declared template domains.
    Strategy:
      1) Clamp per-step delays for logs whose dur_ms/waited_ms represent immediate inter-log timing.
      2) If any (possibly multi-step) observed timing exceeds its template max, scale down the whole attempt's delays proportionally.
         Then re-apply per-step clamps. Iterate a few times for stability.
    """
    delays = [max(1, int(d)) for d in delays_ms]

    def apply_direct_step_clamps(dels: List[int]) -> None:
        for idx, ref in enumerate(emit_refs):
            comp_id, log_id = ref.split(".", 1)
            fld = DIRECT_STEP_FIELDS.get((comp_id, log_id))
            if not fld:
                continue
            rng = get_int_var_range(comp_id, log_id, fld)
            if not rng:
                continue
            lo, hi = rng
            dels[idx] = int(clamp(float(dels[idx]), float(lo), float(hi)))

    def max_violation_scale(times: List[datetime]) -> float:
        constraints = FLOW_TIMING_CONSTRAINTS.get(flow_id, [])
        scale = 1.0
        for comp_id, log_id, fld, si, ei in constraints:
            rng = get_int_var_range(comp_id, log_id, fld)
            if not rng:
                continue
            _, hi = rng
            if si < 0 or ei >= len(times) or si >= ei:
                continue
            val = ms_between(times[si], times[ei])
            if val > hi and val > 0:
                scale = min(scale, float(hi) / float(val))
        return scale

    apply_direct_step_clamps(delays)
    times = build_times_from_delays(attempt_start, delays)

    for _ in range(6):
        s = max_violation_scale(times)
        if s >= 0.999999:
            break
        s = max(0.01, s * 0.999)
        delays = [max(1, int(round(d * s))) for d in delays]
        apply_direct_step_clamps(delays)
        times = build_times_from_delays(attempt_start, delays)

    final_scale = max_violation_scale(times)
    if final_scale < 0.999999:
        s = max(0.01, final_scale * 0.995)
        delays = [max(1, int(round(d * s))) for d in delays]
        apply_direct_step_clamps(delays)
        times = build_times_from_delays(attempt_start, delays)

    return delays, times


def simulate_flow_instance(
    rows: List[Dict[str, Any]],
    flow: Dict[str, Any],
    state: str,
    control: Optional[IntervalControl],
    start_ts: datetime,
    attempt_count: int,
    instance_index: int,
    interval_key: str,
) -> None:
    flow_id = flow["id"]
    emit_refs: List[str] = flow["emit"]
    latency_pairs: List[List[float]] = flow["latency_ms"]
    retry = flow["retry"]
    emit_per_retry: List[str] = retry.get("emit_per_retry", [])
    backoff_pairs: List[List[float]] = retry.get("backoff_ms", [])

    lm50, lm95 = get_latency_mult(control, flow_id) if state == "f" else (1.0, 1.0)

    seed_base = f"flow|{state}|{flow_id}|{interval_key}|{instance_index}"
    req_id = gen_hex(16, seed_base + "|req_id")

    if flow_id in ("player_login", "player_login_fail", "maintenance_page"):
        route = "/api/join" if (md5_int(seed_base + "|route") % 2 == 0) else "/api/session"
        op = "join" if route == "/api/join" else "session"
        method = "POST" if route == "/api/join" else "GET"
        client_ip = gen_ip("203.0.113.0/24", seed_base + "|client_ip")
    else:
        route = ""
        op = ""
        method = ""
        client_ip = ""

    component_hosts: Dict[str, str] = {}
    for ref in emit_refs + emit_per_retry:
        comp_id = ref.split(".", 1)[0]
        if comp_id not in component_hosts:
            component_hosts[comp_id] = pick_host(COMP_BY_ID[comp_id].get("hosts", []), f"{seed_base}|host|{comp_id}")

    inst_id = gen_hex(8, seed_base + "|inst")
    ttl_s = 60 + (md5_int(seed_base + "|ttl") % 61)
    eval_id = gen_hex(12, seed_base + "|eval")

    vault_path = "secret/data/game/auth" if op == "join" else ("secret/data/game/session" if op == "session" else ("secret/data/game/auth" if (md5_int(seed_base + "|vpath") % 2 == 0) else "secret/data/game/session"))
    lookup_service = "matchmaker" if op == "join" else ("session-svc" if op == "session" else ("matchmaker" if (md5_int(seed_base + "|svc") % 2 == 0) else "session-svc"))
    endpoint = gen_str("ip:port", seed_base + "|endpoint|" + lookup_service)

    backoff_rng = get_int_var_range("game_service", "retrying", "backoff_ms") or (50, 1500)

    attempt_start = start_ts
    for attempt in range(1, attempt_count + 1):
        attempt_seed = f"{seed_base}|att|{attempt}"

        if attempt > 1:
            pair_idx = min(attempt - 2, len(backoff_pairs) - 1)
            p50, p95 = backoff_pairs[pair_idx]
            raw_backoff = q_lognormal(p50, p95, u01(attempt_seed + "|backoff"), soft_cap_mult=2.5)
            backoff_ms = int(round(raw_backoff))
            backoff_ms = int(clamp(float(backoff_ms), float(backoff_rng[0]), float(backoff_rng[1])))

            attempt_start = attempt_start + timedelta(milliseconds=backoff_ms)

            for j, rref in enumerate(emit_per_retry):
                rcid, rlid = rref.split(".", 1)
                rctx: Dict[str, Any] = {}
                if (rcid, rlid) == ("game_service", "retrying"):
                    if flow_id.startswith("healthcheck_kv_put_"):
                        rctx["op"] = "health_put"
                    elif flow_id.startswith("vault_read_secret_"):
                        rctx["op"] = "vault_call"
                    else:
                        rctx["op"] = "health_put"
                    rctx["attempt"] = attempt
                    rctx["backoff_ms"] = clamp_int_to_var("game_service", "retrying", "backoff_ms", backoff_ms)
                emit_row(rows, attempt_start + timedelta(milliseconds=j), rcid, rlid, component_hosts.get(rcid, ""), rctx, seed=f"{attempt_seed}|retry|{j}")

        delays_ms: List[int] = []
        for li, (p50, p95) in enumerate(latency_pairs):
            sp50 = max(1.0, float(p50) * lm50)
            sp95 = max(sp50 * 1.0001, float(p95) * lm95)
            d = q_lognormal(sp50, sp95, u01(f"{attempt_seed}|lat|{li}"), soft_cap_mult=1.0)
            delays_ms.append(max(1, int(round(d))))

        delays_ms, times = enforce_delay_domains_via_constraints(flow_id, emit_refs, delays_ms, attempt_start, attempt_seed)

        if flow_id == "maintenance_page":
            ensure_min_gap(times, 0, 1, 10)
        elif flow_id == "player_login":
            ensure_min_gap(times, 0, 6, 10)
            ensure_min_gap(times, 1, 5, 20)
        elif flow_id == "nomad_schedule_ok":
            ensure_min_gap(times, 0, 1, 20)

        _, times = enforce_delay_domains_via_constraints(
            flow_id,
            emit_refs,
            [ms_between(attempt_start, times[0])] + [ms_between(times[i - 1], times[i]) for i in range(1, len(times))],
            attempt_start,
            attempt_seed + "|postgap",
        )

        for li, ref in enumerate(emit_refs):
            comp_id, log_id = ref.split(".", 1)
            host = component_hosts.get(comp_id, "")
            ctx: Dict[str, Any] = {}

            if "req_id" in TEMPLATES[(comp_id, log_id)].get("vars", {}):
                ctx["req_id"] = req_id

            if (comp_id, log_id) == ("edge_gateway", "req_in"):
                ctx["req_id"] = req_id
                ctx["method"] = method
                ctx["route"] = route
                ctx["client_ip"] = client_ip

            if (comp_id, log_id) == ("game_service", "handle_start"):
                ctx["req_id"] = req_id
                ctx["op"] = op
                ctx["user_id"] = 100000 + (md5_int(seed_base + "|user") % 900000)

            if (comp_id, log_id) == ("game_service", "consul_lookup_ok"):
                ctx["req_id"] = req_id
                ctx["service"] = lookup_service
                ctx["endpoint"] = endpoint
                ctx["dur_ms"] = clamp_int_to_var(comp_id, log_id, "dur_ms", ms_between(times[1], times[2]))

            if (comp_id, log_id) == ("game_service", "consul_lookup_fail"):
                ctx["req_id"] = req_id
                ctx["service"] = lookup_service
                ctx["err"] = "deadline_exceeded" if state == "f" else "rpc_unavailable"
                ctx["waited_ms"] = clamp_int_to_var(comp_id, log_id, "waited_ms", ms_between(times[1], times[2]))

            if (comp_id, log_id) == ("vault", "req"):
                ctx["req_id"] = req_id
                ctx["op"] = "read"
                ctx["path"] = vault_path

            if (comp_id, log_id) == ("vault", "resp_ok"):
                ctx["req_id"] = req_id
                if flow_id == "player_login":
                    ctx["dur_ms"] = clamp_int_to_var(comp_id, log_id, "dur_ms", ms_between(times[3], times[4]))
                elif flow_id == "vault_read_secret_ok":
                    ctx["dur_ms"] = clamp_int_to_var(comp_id, log_id, "dur_ms", ms_between(times[1], times[2]))

            if (comp_id, log_id) == ("vault", "storage_latency"):
                ctx["op"] = "get"
                if flow_id == "vault_read_secret_fail":
                    ctx["waited_ms"] = clamp_int_to_var(comp_id, log_id, "waited_ms", ms_between(times[1], times[2]))

            if (comp_id, log_id) == ("vault", "resp_fail"):
                ctx["req_id"] = req_id
                ctx["status"] = 504
                minute_at = int((times[li] - BASE_TIME).total_seconds() // 60)
                ctx["err"] = "storage_timeout" if (19 <= minute_at < 23) else "no_consul_leader"
                if flow_id == "vault_read_secret_fail":
                    ctx["dur_ms"] = clamp_int_to_var(comp_id, log_id, "dur_ms", ms_between(times[1], times[3]))

            if (comp_id, log_id) == ("game_service", "vault_call"):
                ctx["req_id"] = req_id
                ctx["path"] = vault_path

            if (comp_id, log_id) == ("game_service", "vault_auth_fail"):
                ctx["req_id"] = req_id
                ctx["err"] = "vault_timeout"
                if flow_id == "vault_read_secret_fail":
                    ctx["waited_ms"] = clamp_int_to_var(comp_id, log_id, "waited_ms", ms_between(times[0], times[4]))

            if (comp_id, log_id) == ("game_service", "done_ok"):
                ctx["req_id"] = req_id
                ctx["dur_ms"] = clamp_int_to_var(comp_id, log_id, "dur_ms", ms_between(times[1], times[5]))

            if (comp_id, log_id) == ("game_service", "done_err"):
                ctx["req_id"] = req_id
                ctx["status"] = 504 if flow_id == "player_login_fail" else 503
                ctx["dur_ms"] = clamp_int_to_var(comp_id, log_id, "dur_ms", ms_between(times[1], times[3]))

            if (comp_id, log_id) == ("edge_gateway", "resp_ok"):
                ctx["req_id"] = req_id
                ctx["status"] = 200
                ctx["dur_ms"] = clamp_int_to_var(comp_id, log_id, "dur_ms", ms_between(times[0], times[6]))

            if (comp_id, log_id) == ("edge_gateway", "resp_err"):
                ctx["req_id"] = req_id
                if flow_id == "maintenance_page":
                    ctx["status"] = 503
                    ctx["reason"] = "maintenance"
                    ctx["dur_ms"] = clamp_int_to_var(comp_id, log_id, "dur_ms", ms_between(times[0], times[1]))
                elif flow_id == "player_login_fail":
                    ctx["status"] = 504
                    ctx["reason"] = "upstream_timeout"
                    ctx["dur_ms"] = clamp_int_to_var(comp_id, log_id, "dur_ms", ms_between(times[0], times[4]))
                else:
                    ctx["status"] = 503
                    ctx["reason"] = "upstream_unhealthy"
                    ctx["dur_ms"] = clamp_int_to_var(comp_id, log_id, "dur_ms", ms_between(times[0], times[li]))

            if (comp_id, log_id) == ("game_service", "health_put"):
                ctx["svc"] = "game-service"
                ctx["inst_id"] = inst_id
                ctx["ttl_s"] = ttl_s

            if (comp_id, log_id) == ("game_service", "health_put_ok"):
                ctx["svc"] = "game-service"
                ctx["inst_id"] = inst_id
                ctx["dur_ms"] = clamp_int_to_var(comp_id, log_id, "dur_ms", ms_between(times[0], times[1]))

            if (comp_id, log_id) == ("game_service", "health_put_timeout"):
                ctx["svc"] = "game-service"
                ctx["inst_id"] = inst_id
                ctx["waited_ms"] = clamp_int_to_var(comp_id, log_id, "waited_ms", ms_between(times[0], times[1]))

            if (comp_id, log_id) == ("nomad", "eval_start"):
                ctx["eval_id"] = eval_id

            if (comp_id, log_id) == ("nomad", "eval_ok"):
                ctx["eval_id"] = eval_id
                ctx["dur_ms"] = clamp_int_to_var(comp_id, log_id, "dur_ms", ms_between(times[0], times[1]))

            if (comp_id, log_id) == ("nomad", "consul_lock_fail"):
                ctx["job"] = "core-services" if (md5_int(seed_base + "|job") % 2 == 0) else "cache"
                ctx["err"] = "no_leader"
                ctx["waited_ms"] = clamp_int_to_var(comp_id, log_id, "waited_ms", ms_between(times[0], times[1]))

            emit_row(rows, times[li], comp_id, log_id, host, ctx, seed=f"{attempt_seed}|emit|{li}")

        attempt_start = times[-1]


def simulate_flows(rows: List[Dict[str, Any]], state: str, start_min: int, end_min: int, control: Optional[IntervalControl]) -> None:
    start = BASE_TIME + timedelta(minutes=start_min)
    end = BASE_TIME + timedelta(minutes=end_min)
    duration_min = max(0.0, (end - start).total_seconds() / 60.0)
    interval_key = f"{state}|{start_min}-{end_min}"

    flows = SYSTEM["flows"][state]["req"]
    for flow in flows:
        flow_id = flow["id"]
        rpm = float(flow["rpm"])
        mult = get_rate_mult(control, flow_id) if state == "f" else 1.0
        eff_rpm = rpm * mult
        if eff_rpm <= 0:
            continue

        expected_instances = eff_rpm * duration_min
        n_instances = alloc_count(expected_instances, f"flowcnt|{interval_key}|{flow_id}")
        if n_instances <= 0:
            continue

        start_times = schedule_times(n_instances, start, end, f"flowstart|{interval_key}|{flow_id}")
        attempts_list = plan_attempts(n_instances, float(flow["retry"]["expected_attempts"]), int(flow["retry"]["max_attempts"]), seed=f"{interval_key}|{flow_id}")

        for i in range(n_instances):
            simulate_flow_instance(
                rows=rows,
                flow=flow,
                state=state,
                control=control,
                start_ts=start_times[i],
                attempt_count=attempts_list[i],
                instance_index=i,
                interval_key=interval_key,
            )


def simulate_one_shots(rows: List[Dict[str, Any]], control: IntervalControl) -> None:
    if not control.one_shots:
        return
    event_ts_base = BASE_TIME + timedelta(minutes=control.start_min)
    event_ts_end = event_ts_base + timedelta(minutes=1)

    for osi, os in enumerate(control.one_shots):
        ref = os["ref"]
        count = int(os["count"])
        allowed_hosts = os.get("hosts", [])
        comp_id, log_id = ref.split(".", 1)
        comp_hosts = COMP_BY_ID[comp_id].get("hosts", [])

        for j in range(count):
            # one-shots must not be emitted before their declared event time.
            jitter_s = u01(f"oneshot|{control.start_min}|{ref}|{j}") * 4.0  # [0,4) seconds
            ts = event_ts_base + timedelta(seconds=jitter_s) + timedelta(milliseconds=5 * j)
            if ts < event_ts_base:
                ts = event_ts_base
            if ts >= event_ts_end:
                ts = event_ts_end - timedelta(milliseconds=1)

            if allowed_hosts:
                host = allowed_hosts[min(j, len(allowed_hosts) - 1)]
            else:
                host = pick_host(comp_hosts, f"oneshot_host|{control.start_min}|{ref}|{j}")

            ctx: Dict[str, Any] = {}
            if (comp_id, log_id) in (("ops_control", "iptables_block"), ("ops_control", "iptables_unblock")):
                rules = TEMPLATES[(comp_id, log_id)]["vars"]["rule"]["v"]
                ctx["rule"] = rules[j % len(rules)]
            if (comp_id, log_id) == ("ops_control", "consul_snapshot_restore"):
                ctx["result"] = "ok"
            if (comp_id, log_id) == ("ops_control", "leader_stepdown"):
                ctx["reason"] = "slow_append"

            emit_row(rows, ts, comp_id, log_id, host, ctx, seed=f"oneshot|{control.start_min}|{ref}|{j}|{osi}")


def main() -> None:
    rows: List[Dict[str, Any]] = []

    nstart = SCENARIO["time"]["phases"]["n"]["start_min"]
    nend = SCENARIO["time"]["phases"]["n"]["end_min"]
    simulate_background(rows, "n", nstart, nend, control=None)
    simulate_flows(rows, "n", nstart, nend, control=None)

    for ctrl in FAIL_INTERVALS:
        simulate_one_shots(rows, ctrl)
        simulate_background(rows, "f", ctrl.start_min, ctrl.end_min, control=ctrl)
        simulate_flows(rows, "f", ctrl.start_min, ctrl.end_min, control=ctrl)

    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp_dt", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["timestamp_dt"].apply(isoformat_ms)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    if not df.empty:
        ts_vals = df["timestamp"].values
        if any(ts_vals[i] > ts_vals[i + 1] for i in range(len(ts_vals) - 1)):
            df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
