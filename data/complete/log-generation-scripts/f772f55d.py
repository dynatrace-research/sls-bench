import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


SYSTEM: Dict[str, Any] = {
    "sys": {"id": "payments_api_db_failover_incident"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "api_service",
            "svc": "payments-api",
            "hosts": ["api-1", "api-2", "api-3", "api-4"],
            "logs": {
                "req_start": {
                    "lvl": "INFO",
                    "msg": "req_start request_id={request_id} method={method} endpoint={endpoint} acct={acct} trace_id={trace_id}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "method": {"k": "ch", "v": ["POST"]},
                        "endpoint": {"k": "ch", "v": ["/v1/charges", "/v1/payment_intents"]},
                        "acct": {"k": "str", "v": "acct_[A-Za-z0-9]{14}"},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "db_retry": {
                    "lvl": "WARN",
                    "msg": "db_retry request_id={request_id} attempt={attempt} reason={reason} backoff_ms={backoff_ms}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "reason": {"k": "ch", "v": ["upstream_timeout", "upstream_overloaded"]},
                        "backoff_ms": {"k": "i", "v": [50, 900]},
                    },
                },
                "req_ok": {
                    "lvl": "INFO",
                    "msg": "req_ok request_id={request_id} status={status} dur_ms={dur_ms} writes={writes}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "status": {"k": "i", "v": [200, 201]},
                        "dur_ms": {"k": "i", "v": [15, 1400]},
                        "writes": {"k": "i", "v": [1, 3]},
                    },
                },
                "req_err": {
                    "lvl": "ERROR",
                    "msg": "req_err request_id={request_id} status={status} err={err} dur_ms={dur_ms} attempts={attempts}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "status": {"k": "i", "v": [503, 504]},
                        "err": {"k": "ch", "v": ["db_timeout", "db_unavailable", "worker_saturation"]},
                        "dur_ms": {"k": "i", "v": [400, 8000]},
                        "attempts": {"k": "i", "v": [1, 3]},
                    },
                },
                "worker_pool_stats": {
                    "lvl": "INFO",
                    "msg": "worker_pool busy_pct={busy_pct} queue_depth={queue_depth} inflight={inflight} p95_handler_ms={p95_handler_ms}",
                    "vars": {
                        "busy_pct": {"k": "f", "v": [10.0, 65.0]},
                        "queue_depth": {"k": "i", "v": [0, 80]},
                        "inflight": {"k": "i", "v": [20, 280]},
                        "p95_handler_ms": {"k": "i", "v": [40, 320]},
                    },
                },
                "worker_pool_stats_degraded": {
                    "lvl": "WARN",
                    "msg": "worker_pool_degraded busy_pct={busy_pct} queue_depth={queue_depth} inflight={inflight} p95_handler_ms={p95_handler_ms}",
                    "vars": {
                        "busy_pct": {"k": "f", "v": [70.0, 100.0]},
                        "queue_depth": {"k": "i", "v": [120, 2000]},
                        "inflight": {"k": "i", "v": [300, 3200]},
                        "p95_handler_ms": {"k": "i", "v": [400, 7000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "worker_pool_stats", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "worker_pool_stats", "per_min": 1.0, "scope": "per_host"},
                        {"id": "worker_pool_stats_degraded", "per_min": 1.0, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "db_router",
            "svc": "shard-router",
            "hosts": ["router-1", "router-2"],
            "logs": {
                "db_write_ok": {
                    "lvl": "INFO",
                    "msg": "db_write_ok request_id={request_id} shard={shard} node={node} dur_ms={dur_ms}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "node": {"k": "ch", "v": ["db-1", "db-2", "db-3"]},
                        "dur_ms": {"k": "i", "v": [5, 1200]},
                    },
                },
                "db_write_timeout": {
                    "lvl": "ERROR",
                    "msg": "db_write_timeout request_id={request_id} shard={shard} timeout_ms={timeout_ms} last_node={node}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "timeout_ms": {"k": "i", "v": [200, 2500]},
                        "node": {"k": "ch", "v": ["db-1", "db-2", "db-3"]},
                    },
                },
                "conn_pool_stats": {
                    "lvl": "INFO",
                    "msg": "conn_pool shard={shard} in_use={in_use} pending={pending} timeouts_1m={timeouts_1m}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "in_use": {"k": "i", "v": [10, 140]},
                        "pending": {"k": "i", "v": [0, 25]},
                        "timeouts_1m": {"k": "i", "v": [0, 5]},
                    },
                },
                "conn_pool_stats_degraded": {
                    "lvl": "WARN",
                    "msg": "conn_pool_degraded shard={shard} in_use={in_use} pending={pending} timeouts_1m={timeouts_1m}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "in_use": {"k": "i", "v": [80, 500]},
                        "pending": {"k": "i", "v": [30, 900]},
                        "timeouts_1m": {"k": "i", "v": [10, 400]},
                    },
                },
                "conn_reset_warn": {
                    "lvl": "WARN",
                    "msg": "conn_reset shard={shard} node={node} err={err}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "node": {"k": "ch", "v": ["db-1", "db-2", "db-3"]},
                        "err": {"k": "ch", "v": ["connection_reset", "connection_refused"]},
                    },
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "conn_pool_stats", "per_min": 0.5, "scope": "per_host"},
                        {"id": "conn_reset_warn", "per_min": 0.05, "scope": "per_host"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "conn_pool_stats", "per_min": 0.5, "scope": "per_host"},
                        {"id": "conn_pool_stats_degraded", "per_min": 0.8, "scope": "per_host"},
                        {"id": "conn_reset_warn", "per_min": 0.08, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "db_cluster",
            "svc": "shard-db",
            "hosts": ["db-1", "db-2", "db-3"],
            "logs": {
                "node_metrics": {
                    "lvl": "INFO",
                    "msg": "node_metrics shard={shard} role={role} cpu_pct={cpu_pct} load1={load1} repl_lag_s={repl_lag_s}",
                    "vars": {"shard": {"k": "ch", "v": ["shard-07"]}},
                    "state_vars": {
                        "n": {
                            "role": {"k": "ch", "v": ["primary", "replica"]},
                            "cpu_pct": {"k": "f", "v": [5.0, 55.0]},
                            "load1": {"k": "f", "v": [0.2, 8.0]},
                            "repl_lag_s": {"k": "i", "v": [0, 5]},
                        },
                        "f": {
                            "role": {"k": "ch", "v": ["primary", "replica", "unknown"]},
                            "cpu_pct": {"k": "f", "v": [5.0, 75.0]},
                            "load1": {"k": "f", "v": [0.2, 16.0]},
                            "repl_lag_s": {"k": "i", "v": [0, 120]},
                        },
                    },
                },
                "node_metrics_hot": {
                    "lvl": "WARN",
                    "msg": "node_metrics_hot shard={shard} cpu_pct={cpu_pct} load1={load1} repl_lag_s={repl_lag_s}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "cpu_pct": {"k": "f", "v": [80.0, 100.0]},
                        "load1": {"k": "f", "v": [16.0, 64.0]},
                        "repl_lag_s": {"k": "i", "v": [50, 900]},
                    },
                },
                "repl_lag_metric_stale": {
                    "lvl": "WARN",
                    "msg": "repl_lag_metric_stale shard={shard} node={node} last_report_s={last_report_s}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "node": {"k": "ch", "v": ["db-2", "db-3"]},
                        "last_report_s": {"k": "i", "v": [600, 6000]},
                    },
                },
                "primary_failed": {
                    "lvl": "ERROR",
                    "msg": "primary_failed shard={shard} host={host} cause={cause}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "host": {"k": "ch", "v": ["db-1", "db-2", "db-3"]},
                        "cause": {"k": "ch", "v": ["process_crash", "host_reboot", "kernel_panic"]},
                    },
                },
                "election_failed_stalled": {
                    "lvl": "ERROR",
                    "msg": "election_failed shard={shard} proto=v2 reason={reason} term={term}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "reason": {"k": "ch", "v": ["stalled_peers", "no_primary"]},
                        "term": {"k": "i", "v": [1000, 1400]},
                    },
                },
                "election_failed_cpu": {
                    "lvl": "ERROR",
                    "msg": "election_failed shard={shard} proto=v1 reason={reason} term={term}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "reason": {"k": "ch", "v": ["cpu_starvation", "lease_expired"]},
                        "term": {"k": "i", "v": [1400, 1800]},
                    },
                },
                "election_succeeded": {
                    "lvl": "INFO",
                    "msg": "election_succeeded shard={shard} new_primary={new_primary} term={term}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "new_primary": {"k": "ch", "v": ["db-1", "db-2", "db-3"]},
                        "term": {"k": "i", "v": [1000, 1800]},
                    },
                },
                "proto_active": {
                    "lvl": "INFO",
                    "msg": "election_proto_active shard={shard} proto={proto}",
                    "vars": {"shard": {"k": "ch", "v": ["shard-07"]}, "proto": {"k": "ch", "v": ["v1", "v2"]}},
                },
                "config_applied": {
                    "lvl": "INFO",
                    "msg": "config_applied shard={shard} key={key} from={from} to={to}",
                    "vars": {
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "key": {"k": "ch", "v": ["peer_probe_threads"]},
                        "from": {"k": "i", "v": [8, 8]},
                        "to": {"k": "i", "v": [2, 2]},
                    },
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "node_metrics", "per_min": 1.0, "scope": "per_host"},
                        {"id": "repl_lag_metric_stale", "per_min": 0.15, "scope": "global"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "node_metrics", "per_min": 1.5, "scope": "per_host"},
                        {"id": "node_metrics_hot", "per_min": 1.0, "scope": "per_host"},
                        {"id": "repl_lag_metric_stale", "per_min": 0.3, "scope": "global"},
                        {"id": "election_failed_stalled", "per_min": 1.2, "scope": "per_host"},
                        {"id": "election_failed_cpu", "per_min": 1.2, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "monitoring",
            "svc": "alerting",
            "hosts": ["mon-1"],
            "logs": {
                "alert_fired": {
                    "lvl": "CRITICAL",
                    "msg": "alert_fired alert={alert} shard={shard} severity={severity}",
                    "vars": {
                        "alert": {"k": "ch", "v": ["ShardNoPrimary", "ApiWriteErrorsHigh"]},
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "severity": {"k": "ch", "v": ["page"]},
                    },
                },
                "pager_ack": {
                    "lvl": "INFO",
                    "msg": "pager_ack incident={incident} by={by}",
                    "vars": {"incident": {"k": "str", "v": "inc_[0-9]{6}"}, "by": {"k": "ch", "v": ["oncall-db", "oncall-api"]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "operator_tool",
            "svc": "ops-tool",
            "hosts": ["ops-1"],
            "logs": {
                "proto_rollback": {
                    "lvl": "WARN",
                    "msg": "proto_rollback cluster={cluster} shard={shard} from={from} to={to}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["payments-writes"]},
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "from": {"k": "ch", "v": ["v2"]},
                        "to": {"k": "ch", "v": ["v1"]},
                    },
                },
                "config_push": {
                    "lvl": "INFO",
                    "msg": "config_push cluster={cluster} shard={shard} key={key} from={from} to={to}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["payments-writes"]},
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "key": {"k": "ch", "v": ["peer_probe_threads"]},
                        "from": {"k": "i", "v": [8, 8]},
                        "to": {"k": "i", "v": [2, 2]},
                    },
                },
                "restart_node": {
                    "lvl": "WARN",
                    "msg": "restart_node host={host} shard={shard} reason={reason}",
                    "vars": {
                        "host": {"k": "ch", "v": ["db-1", "db-2", "db-3"]},
                        "shard": {"k": "ch", "v": ["shard-07"]},
                        "reason": {"k": "ch", "v": ["force_election", "clear_cpu_starvation"]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "api_post_write_ok",
                    "rpm": 220.0,
                    "emit": ["api_service.req_start", "db_router.db_write_ok", "api_service.req_ok"],
                    "latency_ms": [[2, 6], [15, 90], [25, 220]],
                    "retry": {
                        "max_attempts": 3,
                        "expected_attempts": 1.05,
                        "emit_per_retry": ["api_service.db_retry"],
                        "backoff_ms": [[60, 180], [120, 350]],
                    },
                    "trace": True,
                }
            ]
        },
        "f": {
            "req": [
                {
                    "id": "api_post_write_timeout",
                    "rpm": 190.0,
                    "emit": ["api_service.req_start", "db_router.db_write_timeout", "api_service.req_err"],
                    "latency_ms": [[3, 10], [650, 2200], [1200, 7500]],
                    "retry": {
                        "max_attempts": 3,
                        "expected_attempts": 2.4,
                        "emit_per_retry": ["api_service.db_retry"],
                        "backoff_ms": [[150, 700], [250, 950]],
                    },
                    "trace": True,
                },
                {
                    "id": "api_post_write_slow_ok",
                    "rpm": 35.0,
                    "emit": ["api_service.req_start", "db_router.db_write_ok", "api_service.req_ok"],
                    "latency_ms": [[3, 10], [150, 1100], [300, 1800]],
                    "retry": {
                        "max_attempts": 3,
                        "expected_attempts": 1.3,
                        "emit_per_retry": ["api_service.db_retry"],
                        "backoff_ms": [[120, 500], [220, 850]],
                    },
                    "trace": True,
                },
                {
                    "id": "api_post_write_recovered_ok",
                    "rpm": 220.0,
                    "emit": ["api_service.req_start", "db_router.db_write_ok", "api_service.req_ok"],
                    "latency_ms": [[2, 6], [20, 120], [30, 260]],
                    "retry": {
                        "max_attempts": 3,
                        "expected_attempts": 1.05,
                        "emit_per_retry": ["api_service.db_retry"],
                        "backoff_ms": [[60, 200], [120, 400]],
                    },
                    "trace": True,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "api_degradation_two_db_bugs_plus_config_interaction"},
    "time": {
        "total_minutes": 60,
        "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 60}},
    },
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {
                        "api_post_write_recovered_ok": 0.0,
                        "api_service.worker_pool_stats": 0.0,
                        "api_service.worker_pool_stats_degraded": 1.0,
                        "db_router.conn_pool_stats": 0.0,
                        "db_router.conn_pool_stats_degraded": 1.0,
                        "db_cluster.node_metrics_hot": 0.0,
                        "db_cluster.election_failed_cpu": 0.0,
                        "db_cluster.election_failed_stalled": 2.0,
                    },
                    "latency_multipliers": {
                        "api_post_write_timeout": {"p50": 1.2, "p95": 1.2},
                        "api_post_write_slow_ok": {"p50": 1.3, "p95": 1.3},
                    },
                    "one_shots": [
                        {"ref": "db_cluster.primary_failed", "count": 1, "hosts": ["db-1"]},
                        {"ref": "monitoring.alert_fired", "count": 2, "hosts": ["mon-1"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 22,
                    "rate_multipliers": {},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "monitoring.pager_ack", "count": 1, "hosts": ["mon-1"]}],
                },
                {
                    "order": 3,
                    "at_min": 32,
                    "rate_multipliers": {
                        "api_post_write_timeout": 0.0,
                        "api_post_write_slow_ok": 0.05,
                        "api_post_write_recovered_ok": 1.0,
                        "api_service.worker_pool_stats": 1.0,
                        "api_service.worker_pool_stats_degraded": 0.0,
                        "db_router.conn_pool_stats": 1.0,
                        "db_router.conn_pool_stats_degraded": 0.0,
                        "db_cluster.election_failed_stalled": 0.0,
                        "db_cluster.repl_lag_metric_stale": 0.0,
                    },
                    "latency_multipliers": {"api_post_write_recovered_ok": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [
                        {"ref": "operator_tool.restart_node", "count": 3, "hosts": ["ops-1"]},
                        {"ref": "db_cluster.election_succeeded", "count": 1, "hosts": ["db-1"]},
                    ],
                },
                {
                    "order": 4,
                    "at_min": 36,
                    "rate_multipliers": {},
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "operator_tool.proto_rollback", "count": 1, "hosts": ["ops-1"]},
                        {"ref": "db_cluster.proto_active", "count": 3, "hosts": ["db-1", "db-2", "db-3"]},
                    ],
                },
                {
                    "order": 5,
                    "at_min": 42,
                    "rate_multipliers": {
                        "api_post_write_timeout": 1.0,
                        "api_post_write_slow_ok": 1.0,
                        "api_post_write_recovered_ok": 0.0,
                        "api_service.worker_pool_stats": 0.0,
                        "api_service.worker_pool_stats_degraded": 1.0,
                        "db_router.conn_pool_stats": 0.0,
                        "db_router.conn_pool_stats_degraded": 1.0,
                        "db_cluster.node_metrics_hot": 1.0,
                        "db_cluster.election_failed_cpu": 2.0,
                        "db_cluster.election_failed_stalled": 0.0,
                    },
                    "latency_multipliers": {
                        "api_post_write_timeout": {"p50": 1.3, "p95": 1.4},
                        "api_post_write_slow_ok": {"p50": 1.4, "p95": 1.5},
                    },
                    "one_shots": [{"ref": "monitoring.alert_fired", "count": 2, "hosts": ["mon-1"]}],
                },
                {
                    "order": 6,
                    "at_min": 55,
                    "rate_multipliers": {
                        "api_post_write_timeout": 0.35,
                        "api_post_write_slow_ok": 1.6,
                        "api_post_write_recovered_ok": 0.6,
                        "db_cluster.node_metrics_hot": 0.4,
                        "db_cluster.election_failed_cpu": 0.4,
                    },
                    "latency_multipliers": {
                        "api_post_write_timeout": {"p50": 0.9, "p95": 0.9},
                        "api_post_write_slow_ok": {"p50": 0.9, "p95": 0.9},
                        "api_post_write_recovered_ok": {"p50": 1.0, "p95": 1.0},
                    },
                    "one_shots": [
                        {"ref": "operator_tool.config_push", "count": 1, "hosts": ["ops-1"]},
                        {"ref": "operator_tool.restart_node", "count": 3, "hosts": ["ops-1"]},
                        {"ref": "db_cluster.config_applied", "count": 3, "hosts": ["db-1", "db-2", "db-3"]},
                        {"ref": "db_cluster.election_succeeded", "count": 1, "hosts": ["db-2"]},
                    ],
                },
                {
                    "order": 7,
                    "at_min": 58,
                    "rate_multipliers": {
                        "api_post_write_timeout": 0.0,
                        "api_post_write_slow_ok": 0.05,
                        "api_post_write_recovered_ok": 1.0,
                        "api_service.worker_pool_stats": 1.0,
                        "api_service.worker_pool_stats_degraded": 0.0,
                        "db_router.conn_pool_stats": 1.0,
                        "db_router.conn_pool_stats_degraded": 0.0,
                        "db_cluster.node_metrics_hot": 0.0,
                        "db_cluster.election_failed_cpu": 0.0,
                    },
                    "latency_multipliers": {"api_post_write_recovered_ok": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [],
                },
            ]
        }
    },
}


SEED = "payments_api_db_failover_incident::v3::deterministic"
random.seed(SEED)
BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def _sha1_int(s: str) -> int:
    return int(hashlib.sha1(s.encode("utf-8")).hexdigest(), 16)


def det_u(s: str) -> float:
    return (_sha1_int(f"{SEED}|{s}") % (10**12)) / float(10**12)


def stable_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    n = int(math.floor(expected))
    frac = expected - n
    if frac <= 1e-12:
        return n
    return n + (1 if det_u(f"round|{key}") < frac else 0)


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def choose_from(values: List[Any], key: str) -> Any:
    if not values:
        return None
    u = det_u(f"ch|{key}")
    idx = int(u * len(values))
    if idx >= len(values):
        idx = len(values) - 1
    return values[idx]


def det_hex(n: int, key: str) -> str:
    h = hashlib.sha1(f"{SEED}|hex|{key}".encode("utf-8")).hexdigest()
    while len(h) < n:
        h += hashlib.sha1(h.encode("utf-8")).hexdigest()
    return h[:n].lower()


def det_uuid(key: str) -> str:
    h = hashlib.md5(f"{SEED}|uuid|{key}".encode("utf-8")).hexdigest()
    return f"{h[:8]}-{h[8:12]}-4{h[13:16]}-a{h[17:20]}-{h[20:32]}"


def gen_str(pattern: str, key: str) -> str:
    if pattern.startswith("acct_") and "{14}" in pattern:
        alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"
        out = []
        for i in range(14):
            u = det_u(f"acct|{key}|{i}")
            out.append(alphabet[int(u * len(alphabet)) % len(alphabet)])
        return "acct_" + "".join(out)
    if pattern.startswith("inc_") and "{6}" in pattern:
        digits = []
        for i in range(6):
            u = det_u(f"inc|{key}|{i}")
            digits.append(str(int(u * 10) % 10))
        return "inc_" + "".join(digits)
    return pattern


def sample_range_i(lo: int, hi: int, key: str) -> int:
    if lo > hi:
        lo, hi = hi, lo
    u = det_u(f"i|{key}")
    return int(lo + math.floor(u * (hi - lo + 1)))


def sample_range_f(lo: float, hi: float, key: str) -> float:
    if lo > hi:
        lo, hi = hi, lo
    u = det_u(f"f|{key}")
    return float(lo + u * (hi - lo))


def sample_between_p50_p95(p50: float, p95: float, key: str, cap_mult: float = 3.0) -> float:
    p50 = max(1e-6, float(p50))
    p95 = max(p50, float(p95))
    u = det_u(f"lu|{key}")
    val = p50 * ((p95 / p50) ** u)
    cap = cap_mult * p95
    if val > cap:
        u2 = det_u(f"cap|{key}")
        val = cap * (0.85 + 0.15 * u2)
    return float(val)


def sample_delay_ms(p50: float, p95: float, key: str, min_ms: int = 1, max_ms: Optional[int] = None) -> int:
    v = int(round(sample_between_p50_p95(p50, p95, key, cap_mult=3.0)))
    if v < min_ms:
        v = min_ms
    if max_ms is not None and v > max_ms:
        v = max_ms
    return int(v)


def schedule_even_times(start: datetime, end: datetime, count: int, key_prefix: str, jitter_ms: int) -> List[datetime]:
    if count <= 0:
        return []
    dur_ms = max(1, int((end - start).total_seconds() * 1000))
    out = []
    for i in range(count):
        base = int(((i + 0.5) / count) * dur_ms)
        u = det_u(f"jit|{key_prefix}|{i}")
        jit = int((u - 0.5) * 2.0 * jitter_ms)
        off = base + jit
        if off < 0:
            off = 0
        if off >= dur_ms:
            off = dur_ms - 1
        out.append(start + timedelta(milliseconds=off))
    return out


@dataclass(frozen=True)
class Interval:
    state: str
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    latency_mult: Dict[str, Dict[str, float]]


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]]]:
    comps = {c["id"]: c for c in system["components"]}
    logdefs: Dict[str, Dict[str, Any]] = {}
    for cid, c in comps.items():
        for lid, ld in c.get("logs", {}).items():
            logdefs[f"{cid}.{lid}"] = ld
    return comps, logdefs


COMP_BY_ID, LOGDEF_BY_REF = build_indices(SYSTEM)


def build_intervals(scenario: Dict[str, Any]) -> List[Interval]:
    n0 = scenario["time"]["phases"]["n"]["start_min"]
    n1 = scenario["time"]["phases"]["n"]["end_min"]
    f0 = scenario["time"]["phases"]["f"]["start_min"]
    f1 = scenario["time"]["phases"]["f"]["end_min"]

    intervals: List[Interval] = [Interval(state="n", start_min=n0, end_min=n1, rate_mult={}, latency_mult={})]

    events = sorted(scenario["phases"]["f"]["events"], key=lambda e: e["at_min"])
    boundaries = [f0] + [e["at_min"] for e in events if f0 <= e["at_min"] < f1] + [f1]
    boundaries = sorted(set(boundaries))

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}

    events_by_time: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        events_by_time.setdefault(int(e["at_min"]), []).append(e)

    for i in range(len(boundaries) - 1):
        t_start = int(boundaries[i])
        t_end = int(boundaries[i + 1])
        if t_start in events_by_time:
            for e in sorted(events_by_time[t_start], key=lambda x: x.get("order", 0)):
                for k, v in e.get("rate_multipliers", {}).items():
                    active_rate[k] = float(v)
                for k, v in e.get("latency_multipliers", {}).items():
                    active_lat[k] = {"p50": float(v.get("p50", 1.0)), "p95": float(v.get("p95", 1.0))}
        intervals.append(
            Interval(
                state="f",
                start_min=t_start,
                end_min=t_end,
                rate_mult=dict(active_rate),
                latency_mult={k: dict(v) for k, v in active_lat.items()},
            )
        )

    return intervals


INTERVALS = build_intervals(SCENARIO)


def log_identity(component_id: str, host: str) -> Tuple[str, str]:
    c = COMP_BY_ID[component_id]
    return c.get("svc", "") or "", host or ""


def render_log(ref: str, values: Dict[str, Any], state: str, key: str) -> Tuple[str, str]:
    ld = LOGDEF_BY_REF[ref]
    lvl = ld["lvl"]

    doms: Dict[str, Dict[str, Any]] = {}
    doms.update(ld.get("vars", {}))
    if "state_vars" in ld and state in ld["state_vars"]:
        doms.update(ld["state_vars"][state])

    filled: Dict[str, Any] = dict(values)

    for var, spec in doms.items():
        if var in filled:
            continue
        k = spec["k"]
        v = spec["v"]
        vkey = f"{ref}|{key}|{var}"
        if k == "uuid":
            filled[var] = det_uuid(vkey)
        elif k == "hex":
            filled[var] = det_hex(int(v), vkey)
        elif k == "ch":
            filled[var] = choose_from(list(v), vkey)
        elif k == "i":
            lo, hi = int(v[0]), int(v[1])
            filled[var] = sample_range_i(lo, hi, vkey)
        elif k == "f":
            lo, hi = float(v[0]), float(v[1])
            filled[var] = round(sample_range_f(lo, hi, vkey), 1)
        elif k == "str":
            filled[var] = gen_str(str(v), vkey)
        else:
            filled[var] = str(v)

    msg = ld["msg"].format(**filled)
    return lvl, msg


def pick_component_host(component_id: str, key: str) -> str:
    hosts = COMP_BY_ID[component_id].get("hosts", [])
    if not hosts:
        return ""
    idx = int(det_u(f"host|{component_id}|{key}") * len(hosts)) % len(hosts)
    return hosts[idx]


def plan_attempt_counts(flow_def: Dict[str, Any], n_instances: int, key: str) -> List[int]:
    retry = flow_def.get("retry", {})
    max_a = int(retry.get("max_attempts", 1))
    exp_a = float(retry.get("expected_attempts", 1.0))
    a0 = int(math.floor(exp_a))
    a0 = max(1, min(max_a, a0))
    a1 = min(max_a, a0 + 1)
    if a0 == a1 or n_instances <= 0:
        return [a0] * n_instances
    frac = exp_a - a0
    if frac <= 0:
        return [a0] * n_instances
    if frac >= 1:
        return [a1] * n_instances

    n1 = int(math.floor(frac * n_instances))
    rem = frac * n_instances - n1
    if det_u(f"attempts_rem|{key}") < rem:
        n1 += 1

    perm = list(range(n_instances))
    perm.sort(key=lambda i: det_u(f"attempts_perm|{key}|{i}"))

    out = [a0] * n_instances
    for i in range(min(n1, n_instances)):
        out[perm[i]] = a1
    return out


def simulate_background(interval: Interval, rows: List[Tuple[datetime, str, str, str, str, str]]) -> None:
    state = interval.state
    start = BASE_TIME + timedelta(minutes=interval.start_min)
    end = BASE_TIME + timedelta(minutes=interval.end_min)
    dur_min = interval.end_min - interval.start_min

    for cid, comp in COMP_BY_ID.items():
        beh = comp.get("beh", {}).get(state, {}).get("emit", [])
        for emit_def in beh:
            log_id = emit_def["id"]
            per_min = float(emit_def["per_min"])
            scope = emit_def.get("scope", "per_host")
            mult_key = f"{cid}.{log_id}"
            mult = float(interval.rate_mult.get(mult_key, 1.0)) if state == "f" else 1.0
            eff_rate = per_min * mult
            if eff_rate <= 0:
                continue

            ref = f"{cid}.{log_id}"

            if scope == "global":
                expected = eff_rate * dur_min
                count = stable_round(expected, f"bg|{state}|{interval.start_min}-{interval.end_min}|{ref}|global")
                times = schedule_even_times(start, end, count, f"bg|{ref}|global|{interval.start_min}", jitter_ms=700)
                hosts = comp.get("hosts", [])
                for i, t in enumerate(times):
                    host = hosts[i % len(hosts)] if hosts else ""
                    lvl, msg = render_log(ref, {}, state, f"bg|{interval.start_min}|{i}")
                    svc, host_out = log_identity(cid, host)
                    rows.append((t, lvl, msg, "", svc, host_out))
            else:
                hosts = comp.get("hosts", [])
                for host in hosts:
                    expected = eff_rate * dur_min
                    count = stable_round(expected, f"bg|{state}|{interval.start_min}-{interval.end_min}|{ref}|{host}")
                    times = schedule_even_times(start, end, count, f"bg|{ref}|{host}|{interval.start_min}", jitter_ms=700)
                    for i, t in enumerate(times):
                        lvl, msg = render_log(ref, {}, state, f"bg|{host}|{interval.start_min}|{i}")
                        svc, host_out = log_identity(cid, host)
                        rows.append((t, lvl, msg, "", svc, host_out))


def simulate_flow_instances(
    interval: Interval, flow_def: Dict[str, Any], n_instances: int, rows: List[Tuple[datetime, str, str, str, str, str]]
) -> None:
    """
    Chain model used by this simulator:
      - req_start emitted once for the client request.
      - router log emitted for upstream attempt(s); for timeout flows we emit each attempt's timeout;
        for ok-flows we emit only the final successful router log (earlier failed/late attempts are not visible via the ok-only router template).
      - db_retry emitted on retry attempts 2..A at the start of the retry attempt; backoff_ms matches the retry spacing.
      - terminal req_ok/req_err emitted once at the end of the final attempt with dur_ms covering the whole chain.
    """
    state = interval.state
    start = BASE_TIME + timedelta(minutes=interval.start_min)
    end = BASE_TIME + timedelta(minutes=interval.end_min)
    start_times = schedule_even_times(start, end, n_instances, f"flow|{state}|{flow_def['id']}|{interval.start_min}", jitter_ms=250)

    attempt_counts = plan_attempt_counts(flow_def, n_instances, f"{state}|{flow_def['id']}|{interval.start_min}-{interval.end_min}")

    lat_mult = interval.latency_mult.get(flow_def["id"], {"p50": 1.0, "p95": 1.0})
    p50m = float(lat_mult.get("p50", 1.0))
    p95m = float(lat_mult.get("p95", 1.0))

    emit_refs = list(flow_def["emit"])
    latency_pairs = list(flow_def["latency_ms"])
    if len(emit_refs) != 3 or len(latency_pairs) != 3:
        raise ValueError(f"Unsupported flow shape for {flow_def['id']}: emit={len(emit_refs)} latency_ms={len(latency_pairs)}")

    start_ref, mid_ref, end_ref = emit_refs
    (lat0_p50, lat0_p95), (lat1_p50, lat1_p95), (lat2_p50, lat2_p95) = latency_pairs

    retry = flow_def.get("retry", {})
    backoff_pairs = list(retry.get("backoff_ms", []))
    backoff_min, backoff_max = 50, 900  # from api_service.db_retry vars

    is_timeout_flow = flow_def["id"] == "api_post_write_timeout"
    # For ok-flows, to preserve attempt/outcome coherence with db_retry, only the final successful router log is emitted.
    emit_router_each_attempt = is_timeout_flow

    for idx, t0 in enumerate(start_times):
        inst_key = f"{flow_def['id']}|{interval.start_min}|{idx}"
        trace_id = det_hex(32, f"trace|{inst_key}") if (SYSTEM["tracing"]["on"] and flow_def.get("trace", False)) else ""
        request_id = det_uuid(f"req|{inst_key}")

        api_host = pick_component_host("api_service", f"api|{inst_key}")
        router_host = pick_component_host("db_router", f"router|{inst_key}")

        endpoint = choose_from(["/v1/charges", "/v1/payment_intents"], f"endpoint|{inst_key}")
        acct = gen_str("acct_[A-Za-z0-9]{14}", f"acct|{inst_key}")
        writes = 1 + (int(det_u(f"writes|{inst_key}") * 3) % 3)
        node = choose_from(["db-1", "db-2", "db-3"], f"node|{inst_key}")

        attempts_total = int(attempt_counts[idx])

        # Outcome binding (per request)
        if is_timeout_flow:
            terminal_ref = end_ref  # api_service.req_err
            terminal_status = 504
            terminal_err = "db_timeout"
            retry_reason = "upstream_timeout"
            dur_min_bound, dur_max_bound = 400, 8000
        else:
            terminal_ref = end_ref  # api_service.req_ok
            terminal_status = 200 if det_u(f"status|{inst_key}") < (0.8 if "recovered" in flow_def["id"] else 0.7) else 201
            terminal_err = ""
            # Keep within modeled domain; this "reason" is only emitted when retries happen.
            retry_reason = "upstream_overloaded" if det_u(f"retry_reason|{inst_key}") < 0.55 else "upstream_timeout"
            dur_min_bound, dur_max_bound = 15, 1400

        # Router mid log bounds based on template domains.
        if mid_ref == "db_router.db_write_timeout":
            router_min, router_max = 200, 2500
        else:
            router_min, router_max = 5, 1200

        # ---- Plan one coherent chain of timings (ms) relative to req_start ----
        req_start_delay_ms = sample_delay_ms(lat0_p50 * p50m, lat0_p95 * p95m, f"lat0|{inst_key}", min_ms=1, max_ms=None)
        req_start_time = t0 + timedelta(milliseconds=req_start_delay_ms)

        router_delay_ms: List[int] = []
        for a in range(1, attempts_total + 1):
            d = sample_delay_ms(lat1_p50 * p50m, lat1_p95 * p95m, f"lat1|{inst_key}|a{a}", min_ms=router_min, max_ms=router_max)
            router_delay_ms.append(d)

        backoff_ms_for_attempt: Dict[int, int] = {}
        for a in range(2, attempts_total + 1):
            if backoff_pairs:
                bp50, bp95 = backoff_pairs[min(a - 2, len(backoff_pairs) - 1)]
            else:
                bp50, bp95 = (100, 300)
            bo = sample_delay_ms(float(bp50), float(bp95), f"backoff|{inst_key}|to{a}", min_ms=backoff_min, max_ms=backoff_max)
            backoff_ms_for_attempt[a] = bo

        terminal_delay_ms = sample_delay_ms(lat2_p50 * p50m, lat2_p95 * p95m, f"lat2|{inst_key}", min_ms=1, max_ms=None)

        start_marker_offset: Dict[int, int] = {1: 0}
        router_offset: Dict[int, int] = {}
        for a in range(1, attempts_total + 1):
            if a == 1:
                start_marker_offset[a] = 0
            else:
                start_marker_offset[a] = router_offset[a - 1] + backoff_ms_for_attempt[a]
            router_offset[a] = start_marker_offset[a] + router_delay_ms[a - 1]

        total_dur_ms = router_offset[attempts_total] + terminal_delay_ms

        # Enforce terminal duration bounds (adjust in place deterministically while preserving coherence).
        if total_dur_ms > dur_max_bound:
            excess = total_dur_ms - dur_max_bound
            reduc = min(excess, max(0, terminal_delay_ms - 1))
            terminal_delay_ms -= reduc
            total_dur_ms = router_offset[attempts_total] + terminal_delay_ms

            if total_dur_ms > dur_max_bound:
                excess = total_dur_ms - dur_max_bound
                for a in range(attempts_total, 1, -1):
                    if excess <= 0:
                        break
                    cur = backoff_ms_for_attempt.get(a, 0)
                    reduc = min(excess, max(0, cur - backoff_min))
                    if reduc <= 0:
                        continue
                    backoff_ms_for_attempt[a] = cur - reduc
                    excess -= reduc

                for a in range(2, attempts_total + 1):
                    start_marker_offset[a] = router_offset[a - 1] + backoff_ms_for_attempt[a]
                    router_offset[a] = start_marker_offset[a] + router_delay_ms[a - 1]
                total_dur_ms = router_offset[attempts_total] + terminal_delay_ms

            if total_dur_ms > dur_max_bound:
                excess = total_dur_ms - dur_max_bound
                for a in range(attempts_total, 0, -1):
                    if excess <= 0:
                        break
                    cur = router_delay_ms[a - 1]
                    reduc = min(excess, max(0, cur - router_min))
                    if reduc <= 0:
                        continue
                    router_delay_ms[a - 1] = cur - reduc
                    excess -= reduc

                router_offset[1] = start_marker_offset[1] + router_delay_ms[0]
                for a in range(2, attempts_total + 1):
                    start_marker_offset[a] = router_offset[a - 1] + backoff_ms_for_attempt[a]
                    router_offset[a] = start_marker_offset[a] + router_delay_ms[a - 1]
                total_dur_ms = router_offset[attempts_total] + terminal_delay_ms

        if total_dur_ms < dur_min_bound:
            shortage = dur_min_bound - total_dur_ms
            terminal_delay_ms += shortage
            total_dur_ms += shortage

        # ---- Emit logs using the planned schedule ----
        start_vals = {"request_id": request_id, "method": "POST", "endpoint": endpoint, "acct": acct, "trace_id": trace_id}
        lvl, msg = render_log(start_ref, start_vals, state, f"flow|{inst_key}|req_start")
        svc, host_out = log_identity("api_service", api_host)
        rows.append((req_start_time, lvl, msg, trace_id, svc, host_out))

        for a in range(1, attempts_total + 1):
            marker_time = req_start_time + timedelta(milliseconds=start_marker_offset[a])
            if a >= 2 and retry.get("emit_per_retry"):
                ref = retry["emit_per_retry"][0]
                vals = {"request_id": request_id, "attempt": a, "reason": retry_reason, "backoff_ms": int(backoff_ms_for_attempt[a])}
                lvl, msg = render_log(ref, vals, state, f"flow|{inst_key}|retry|a{a}")
                svc, host_out = log_identity("api_service", api_host)
                rows.append((marker_time, lvl, msg, trace_id, svc, host_out))

            # Router log emission policy:
            # - timeout flow: emit timeout log per attempt
            # - ok flows: emit only final attempt's successful router log to avoid "retry-after-ok" contradictions
            if (not emit_router_each_attempt) and (a < attempts_total):
                continue

            router_time = req_start_time + timedelta(milliseconds=router_offset[a])
            if mid_ref.endswith(".db_write_ok"):
                vals = {"request_id": request_id, "shard": "shard-07", "node": node, "dur_ms": int(router_delay_ms[a - 1])}
            else:
                vals = {"request_id": request_id, "shard": "shard-07", "timeout_ms": int(router_delay_ms[a - 1]), "node": node}
            lvl, msg = render_log(mid_ref, vals, state, f"flow|{inst_key}|router|a{a}")
            svc, host_out = log_identity("db_router", router_host)
            rows.append((router_time, lvl, msg, trace_id, svc, host_out))

        terminal_time = req_start_time + timedelta(milliseconds=(router_offset[attempts_total] + terminal_delay_ms))
        if terminal_ref.endswith(".req_ok"):
            vals = {"request_id": request_id, "status": terminal_status, "dur_ms": int(total_dur_ms), "writes": writes}
        else:
            vals = {"request_id": request_id, "status": terminal_status, "err": terminal_err, "dur_ms": int(total_dur_ms), "attempts": attempts_total}
        lvl, msg = render_log(terminal_ref, vals, state, f"flow|{inst_key}|terminal")
        svc, host_out = log_identity("api_service", api_host)
        rows.append((terminal_time, lvl, msg, trace_id, svc, host_out))


def simulate_flows(interval: Interval, rows: List[Tuple[datetime, str, str, str, str, str]]) -> None:
    state = interval.state
    dur_min = interval.end_min - interval.start_min
    flow_defs = SYSTEM["flows"][state]["req"]

    for fdef in flow_defs:
        flow_id = fdef["id"]
        rpm = float(fdef["rpm"])
        if state == "f":
            rpm *= float(interval.rate_mult.get(flow_id, 1.0))
        if rpm <= 0:
            continue
        expected = rpm * dur_min
        n_instances = stable_round(expected, f"flow|{state}|{flow_id}|{interval.start_min}-{interval.end_min}")
        simulate_flow_instances(interval, fdef, n_instances, rows)


def emit_one_shots(rows: List[Tuple[datetime, str, str, str, str, str]]) -> None:
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: int(e["at_min"]))
    for e in events:
        at_min = int(e["at_min"])
        for os in e.get("one_shots", []):
            ref = os["ref"]
            count = int(os["count"])
            allowed_hosts = list(os.get("hosts") or [])
            comp_id, _ = ref.split(".", 1)
            comp_hosts = COMP_BY_ID[comp_id].get("hosts", [])
            if not allowed_hosts:
                allowed_hosts = comp_hosts[:] if comp_hosts else [""]

            center = BASE_TIME + timedelta(minutes=at_min)
            times = schedule_even_times(center, center + timedelta(seconds=2), count, f"oneshot|{at_min}|{ref}", jitter_ms=350)

            for i, t in enumerate(times):
                host_out = allowed_hosts[i % len(allowed_hosts)] if allowed_hosts else ""
                vals: Dict[str, Any] = {}

                if ref == "monitoring.alert_fired":
                    alerts = ["ShardNoPrimary", "ApiWriteErrorsHigh"]
                    vals["alert"] = alerts[i % len(alerts)]
                    vals["shard"] = "shard-07"
                    vals["severity"] = "page"
                elif ref == "monitoring.pager_ack":
                    vals["incident"] = gen_str("inc_[0-9]{6}", f"oneshot|{at_min}|{ref}|{i}")
                    vals["by"] = choose_from(["oncall-db", "oncall-api"], f"oneshot|{at_min}|by|{i}")
                elif ref == "db_cluster.primary_failed":
                    vals["shard"] = "shard-07"
                    vals["host"] = host_out
                    vals["cause"] = choose_from(["process_crash", "host_reboot", "kernel_panic"], f"oneshot|{at_min}|cause|{i}")
                elif ref == "db_cluster.election_succeeded":
                    vals["shard"] = "shard-07"
                    vals["new_primary"] = host_out if host_out in ["db-1", "db-2", "db-3"] else choose_from(["db-1", "db-2", "db-3"], f"oneshot|{at_min}|np|{i}")
                    vals["term"] = sample_range_i(1000, 1800, f"oneshot|{at_min}|term|{i}")
                elif ref == "operator_tool.restart_node":
                    vals["host"] = ["db-1", "db-2", "db-3"][i % 3]
                    vals["shard"] = "shard-07"
                    if at_min == 32:
                        vals["reason"] = "force_election"
                    elif at_min == 55:
                        vals["reason"] = "clear_cpu_starvation"
                    else:
                        vals["reason"] = choose_from(["force_election", "clear_cpu_starvation"], f"oneshot|{at_min}|reason|{i}")
                elif ref == "operator_tool.proto_rollback":
                    vals["cluster"] = "payments-writes"
                    vals["shard"] = "shard-07"
                    vals["from"] = "v2"
                    vals["to"] = "v1"
                elif ref == "db_cluster.proto_active":
                    vals["shard"] = "shard-07"
                    vals["proto"] = "v1"
                elif ref == "operator_tool.config_push":
                    vals["cluster"] = "payments-writes"
                    vals["shard"] = "shard-07"
                    vals["key"] = "peer_probe_threads"
                    vals["from"] = 8
                    vals["to"] = 2
                elif ref == "db_cluster.config_applied":
                    vals["shard"] = "shard-07"
                    vals["key"] = "peer_probe_threads"
                    vals["from"] = 8
                    vals["to"] = 2

                lvl, msg = render_log(ref, vals, "f", f"oneshot|{at_min}|{ref}|{i}")
                svc, host_final = log_identity(comp_id, host_out)
                rows.append((t, lvl, msg, "", svc, host_final))


def main() -> None:
    rows: List[Tuple[datetime, str, str, str, str, str]] = []

    for interval in INTERVALS:
        simulate_background(interval, rows)
        simulate_flows(interval, rows)

    emit_one_shots(rows)

    rows.sort(key=lambda r: r[0])

    df = pd.DataFrame(
        {
            "timestamp": [fmt_ts(r[0]) for r in rows],
            "level": [r[1] for r in rows],
            "message": [r[2] for r in rows],
            "trace_id": [r[3] for r in rows],
            "service": [r[4] for r in rows],
            "host": [r[5] for r in rows],
        }
    )

    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
