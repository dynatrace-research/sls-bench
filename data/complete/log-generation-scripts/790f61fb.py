import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# -----------------------------
# Embedded executable spec (normalized)
# -----------------------------

SYSTEM: Dict[str, Any] = {
    "id": "circleci_build_queue_platform",
    "tracing": {"on": True, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "edge_lb": {
            "svc": "edge-lb",
            "hosts": ["lb1", "lb2"],
            "logs": {
                "access": {
                    "lvl": "INFO",
                    "msg": "{method} {route} -> {status} upstream={upstream} req_id={req_id} dur_ms={dur_ms}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/hooks/github", "/api/v2/me", "/api/v2/project"]},
                        "status": {"k": "ch", "v": [200, 500, 503, 504]},
                        "upstream": {"k": "ch", "v": ["api_service", "none"]},
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [1, 15000]},
                    },
                },
                "capacity_throttle": {
                    "lvl": "WARN",
                    "msg": "Adjusted LB capacity mode=throttled max_inflight={max_inflight} note=protect_db",
                    "vars": {"max_inflight": {"k": "i", "v": [50, 300]}},
                },
                "capacity_rollback": {
                    "lvl": "WARN",
                    "msg": "Adjusted LB capacity mode=normal max_inflight={max_inflight} note=rollback",
                    "vars": {"max_inflight": {"k": "i", "v": [500, 2000]}},
                },
            },
            "beh": {"n": [], "f": []},
        },
        "api_service": {
            "svc": "circle-api",
            "hosts": ["api1", "api2", "api3"],
            "logs": {
                "webhook_received": {
                    "lvl": "INFO",
                    "msg": "Received GitHub hook delivery={delivery_id} event={gh_event} repo={repo}",
                    "vars": {
                        "delivery_id": {"k": "uuid", "v": None},
                        "gh_event": {"k": "ch", "v": ["push", "pull_request"]},
                        "repo": {"k": "ch", "v": ["acme/api", "acme/web", "beta/mobile", "gamma/data"]},
                    },
                },
                "enqueue_ok": {
                    "lvl": "INFO",
                    "msg": "Enqueued build build_id={build_id} repo={repo} queue={queue} db_ms={db_ms}",
                    "vars": {
                        "build_id": {"k": "uuid", "v": None},
                        "repo": {"k": "ch", "v": ["acme/api", "acme/web", "beta/mobile", "gamma/data"]},
                        "queue": {"k": "ch", "v": ["run"]},
                        "db_ms": {"k": "i", "v": [5, 3000]},
                    },
                },
                "enqueue_timeout": {
                    "lvl": "ERROR",
                    "msg": "Failed to enqueue build repo={repo} error=db_timeout db_ms={db_ms}",
                    "vars": {
                        "repo": {"k": "ch", "v": ["acme/api", "acme/web", "beta/mobile", "gamma/data"]},
                        "db_ms": {"k": "i", "v": [500, 8000]},
                    },
                },
                "api_req_ok": {
                    "lvl": "INFO",
                    "msg": "HTTP {method} {endpoint} -> 200 user={user_id} db_ms={db_ms} dur_ms={dur_ms}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET"]},
                        "endpoint": {"k": "ch", "v": ["/api/v2/me", "/api/v2/project"]},
                        "user_id": {"k": "i", "v": [1000, 9000]},
                        "db_ms": {"k": "i", "v": [1, 4000]},
                        "dur_ms": {"k": "i", "v": [5, 12000]},
                    },
                },
                "api_req_error": {
                    "lvl": "ERROR",
                    "msg": "HTTP {method} {endpoint} -> {status} user={user_id} err={err} db_ms={db_ms} dur_ms={dur_ms}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET"]},
                        "endpoint": {"k": "ch", "v": ["/api/v2/me", "/api/v2/project"]},
                        "status": {"k": "i", "v": [500, 503]},
                        "user_id": {"k": "i", "v": [1000, 9000]},
                        "err": {"k": "ch", "v": ["db_timeout", "db_overloaded"]},
                        "db_ms": {"k": "i", "v": [100, 8000]},
                        "dur_ms": {"k": "i", "v": [100, 12000]},
                    },
                },
            },
            "beh": {"n": [], "f": []},
        },
        "scheduler_service": {
            "svc": "build-scheduler",
            "hosts": ["sched1", "sched2"],
            "logs": {
                "dequeue_ok": {
                    "lvl": "INFO",
                    "msg": "Dequeued build build_id={build_id} db_ms={db_ms} run_q={run_q} usage_q={usage_q}",
                    "vars": {
                        "build_id": {"k": "uuid", "v": None},
                        "db_ms": {"k": "i", "v": [2, 8000]},
                        "run_q": {"k": "i", "v": [0, 20000]},
                        "usage_q": {"k": "i", "v": [0, 30000]},
                    },
                },
                "dequeue_timeout": {
                    "lvl": "WARN",
                    "msg": "Dequeue poll timed out tx={tx_id} timeout_ms={timeout_ms} run_q={run_q} usage_q={usage_q}",
                    "vars": {
                        "tx_id": {"k": "hex", "v": 12},
                        "timeout_ms": {"k": "i", "v": [500, 8000]},
                        "run_q": {"k": "i", "v": [0, 20000]},
                        "usage_q": {"k": "i", "v": [0, 30000]},
                    },
                },
                "db_retry": {
                    "lvl": "WARN",
                    "msg": "Retrying dequeue tx={tx_id} attempt={attempt} backoff_ms={backoff_ms}",
                    "vars": {
                        "tx_id": {"k": "hex", "v": 12},
                        "attempt": {"k": "i", "v": [2, 2]},
                        "backoff_ms": {"k": "i", "v": [100, 1500]},
                    },
                },
                "queue_stats": {
                    "lvl": "INFO",
                    "msg": "Queue stats run_q={run_q} usage_q={usage_q} dequeue_rps={dequeue_rps} reenqueue_rps={reenqueue_rps}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "run_q": {"k": "i", "v": [0, 2000]},
                            "usage_q": {"k": "i", "v": [0, 4000]},
                            "dequeue_rps": {"k": "f", "v": [1.0, 3.5]},
                            "reenqueue_rps": {"k": "f", "v": [0.0, 0.05]},
                        },
                        "f": {
                            "run_q": {"k": "i", "v": [0, 20000]},
                            "usage_q": {"k": "i", "v": [0, 30000]},
                            "dequeue_rps": {"k": "f", "v": [0.0, 3.5]},
                            "reenqueue_rps": {"k": "f", "v": [0.0, 3.0]},
                        },
                    },
                },
                "queue_pressure": {
                    "lvl": "WARN",
                    "msg": "Queue pressure run_q={run_q} usage_q={usage_q} dequeue_rps={dequeue_rps} note={note}",
                    "vars": {
                        "run_q": {"k": "i", "v": [5000, 20000]},
                        "usage_q": {"k": "i", "v": [8000, 30000]},
                        "dequeue_rps": {"k": "f", "v": [0.0, 0.5]},
                        "note": {"k": "ch", "v": ["dequeue_slow", "backlog_growing"]},
                    },
                },
                "reenqueue_build": {
                    "lvl": "WARN",
                    "msg": "Re-enqueued build build_id={build_id} reason={reason}",
                    "vars": {
                        "build_id": {"k": "uuid", "v": None},
                        "reason": {"k": "ch", "v": ["infra_failure", "db_failover", "runner_lost"]},
                    },
                },
            },
            "beh": {
                "n": [
                    {"id": "queue_stats", "per_min": 1.0, "scope": "per_host"},
                    {"id": "reenqueue_build", "per_min": 0.1, "scope": "per_host"},
                ],
                "f": [
                    {"id": "queue_stats", "per_min": 1.0, "scope": "per_host"},
                    {"id": "queue_pressure", "per_min": 1.0, "scope": "per_host"},
                    {"id": "reenqueue_build", "per_min": 30.0, "scope": "per_host"},
                ],
            },
        },
        "postgres_db": {
            "svc": "postgres",
            "hosts": ["db1", "db2"],
            "logs": {
                "pool_stats": {
                    "lvl": "INFO",
                    "msg": "DB pool active={active} waiting={waiting} locks_waiting={locks_waiting} cpu_pct={cpu_pct}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "active": {"k": "i", "v": [10, 80]},
                            "waiting": {"k": "i", "v": [0, 5]},
                            "locks_waiting": {"k": "i", "v": [0, 2]},
                            "cpu_pct": {"k": "i", "v": [5, 60]},
                        },
                        "f": {
                            "active": {"k": "i", "v": [10, 150]},
                            "waiting": {"k": "i", "v": [0, 250]},
                            "locks_waiting": {"k": "i", "v": [0, 120]},
                            "cpu_pct": {"k": "i", "v": [5, 100]},
                        },
                    },
                },
                "lock_alert": {
                    "lvl": "WARN",
                    "msg": "High lock waits waiting={waiting} locks_waiting={locks_waiting} top_client={top_client} window_s={window_s}",
                    "vars": {
                        "waiting": {"k": "i", "v": [50, 250]},
                        "locks_waiting": {"k": "i", "v": [20, 120]},
                        "top_client": {"k": "ch", "v": ["api_service", "scheduler_service"]},
                        "window_s": {"k": "i", "v": [30, 60]},
                    },
                },
                "query_timeout": {
                    "lvl": "WARN",
                    "msg": "Statement timeout stmt={stmt} client={client} timeout_ms={timeout_ms}",
                    "vars": {
                        "stmt": {"k": "ch", "v": ["insert_build", "select_next_build", "usage_check"]},
                        "client": {"k": "ch", "v": ["api_service", "scheduler_service"]},
                        "timeout_ms": {"k": "i", "v": [500, 8000]},
                    },
                },
                "slow_query": {
                    "lvl": "WARN",
                    "msg": "Slow query stmt={stmt} client={client} dur_ms={dur_ms} lock_wait_ms={lock_wait_ms} rows={rows}",
                    "vars": {
                        "stmt": {"k": "ch", "v": ["insert_build", "select_next_build", "usage_check"]},
                        "client": {"k": "ch", "v": ["api_service", "scheduler_service"]},
                        "dur_ms": {"k": "i", "v": [500, 30000]},
                        "lock_wait_ms": {"k": "i", "v": [0, 30000]},
                        "rows": {"k": "i", "v": [0, 20000]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "pool_stats", "per_min": 2.0, "scope": "per_host"}],
                "f": [
                    {"id": "pool_stats", "per_min": 3.0, "scope": "per_host"},
                    {"id": "lock_alert", "per_min": 1.0, "scope": "per_host"},
                    {"id": "query_timeout", "per_min": 20.0, "scope": "per_host"},
                    {"id": "slow_query", "per_min": 10.0, "scope": "per_host"},
                ],
            },
        },
        "builder_fleet": {
            "svc": "builder",
            "hosts": ["b01", "b02", "b03", "b04", "b05", "b06", "b07", "b08", "b09", "b10", "b11", "b12"],
            "logs": {
                "build_started": {
                    "lvl": "INFO",
                    "msg": "Builder {builder_host} started build build_id={build_id}",
                    "vars": {
                        "builder_host": {
                            "k": "ch",
                            "v": ["b01", "b02", "b03", "b04", "b05", "b06", "b07", "b08", "b09", "b10", "b11", "b12"],
                        },
                        "build_id": {"k": "uuid", "v": None},
                    },
                },
                "heartbeat": {
                    "lvl": "INFO",
                    "msg": "Heartbeat slots_free={slots_free} running={running}",
                    "vars": {"slots_free": {"k": "i", "v": [0, 8]}, "running": {"k": "i", "v": [0, 8]}},
                },
            },
            "beh": {
                "n": [{"id": "heartbeat", "per_min": 1.0, "scope": "per_host"}],
                "f": [{"id": "heartbeat", "per_min": 1.0, "scope": "per_host"}],
            },
        },
        "ops_repl": {
            "svc": "ops-repl",
            "hosts": ["ops1"],
            "logs": {
                "db_stepdown": {
                    "lvl": "WARN",
                    "msg": "Requested DB stepdown from {from_host} to {to_host}",
                    "vars": {"from_host": {"k": "ch", "v": ["db1", "db2"]}, "to_host": {"k": "ch", "v": ["db1", "db2"]}},
                },
                "repl_patch": {
                    "lvl": "WARN",
                    "msg": "Applied live patch patch={patch} actor={actor}",
                    "vars": {
                        "patch": {"k": "ch", "v": ["disable_auto_reenqueue", "disable_hot_usage_query"]},
                        "actor": {"k": "ch", "v": ["oncall1", "oncall2"]},
                    },
                },
                "auto_reenqueue_disabled": {"lvl": "WARN", "msg": "Set auto_reenqueue=false", "vars": {}},
                "terminate_builders": {
                    "lvl": "WARN",
                    "msg": "Terminated builders count={count} reason={reason}",
                    "vars": {"count": {"k": "i", "v": [5, 20]}, "reason": {"k": "ch", "v": ["reduce_db_load"]}},
                },
                "collect_slow_queries": {
                    "lvl": "INFO",
                    "msg": "Collected slow query logs window_s={window_s}",
                    "vars": {"window_s": {"k": "i", "v": [300, 1800]}},
                },
                "drain_script_start": {
                    "lvl": "INFO",
                    "msg": "Started drain script queue={queue} batch_size={batch_size}",
                    "vars": {"queue": {"k": "ch", "v": ["usage", "run"]}, "batch_size": {"k": "i", "v": [1000, 5000]}},
                },
                "drain_progress": {
                    "lvl": "INFO",
                    "msg": "Drain progress queue={queue} remaining={remaining} deleted={deleted} rate_per_min={rate_per_min}",
                    "vars": {
                        "queue": {"k": "ch", "v": ["usage", "run"]},
                        "remaining": {"k": "i", "v": [0, 500000]},
                        "deleted": {"k": "i", "v": [0, 500000]},
                        "rate_per_min": {"k": "i", "v": [5000, 60000]},
                    },
                },
            },
            "beh": {"n": [], "f": [{"id": "drain_progress", "per_min": 3.0, "scope": "global"}]},
        },
    },
    "flows": {
        "n": {
            "github_webhook_enqueue_ok": {
                "rpm": 0.0,
                "emit": ["api_service.webhook_received", "api_service.enqueue_ok", "edge_lb.access"],
                "latency_ms": [[2, 12], [10, 80], [1, 15]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "customer_api_ok": {
                "rpm": 160.0,
                "emit": ["api_service.api_req_ok", "edge_lb.access"],
                "latency_ms": [[10, 120], [1, 20]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "scheduler_dequeue_ok": {
                "rpm": 20.0,
                "emit": ["scheduler_service.dequeue_ok", "builder_fleet.build_started"],
                "latency_ms": [[10, 80], [30, 150]],
                "retry": {
                    "max_attempts": 2,
                    "expected_attempts": 1.1,
                    "emit_per_retry": ["scheduler_service.db_retry"],
                    "backoff_ms": [[50, 250]],
                },
                "trace": False,
            },
        },
        "f": {
            "github_webhook_enqueue_ok": {
                "rpm": 180.0,
                "emit": ["api_service.webhook_received", "api_service.enqueue_ok", "edge_lb.access"],
                "latency_ms": [[3, 30], [50, 2500], [2, 60]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "github_webhook_enqueue_timeout": {
                "rpm": 120.0,
                "emit": ["api_service.webhook_received", "api_service.enqueue_timeout", "edge_lb.access"],
                "latency_ms": [[3, 50], [800, 8000], [2, 80]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "github_webhook_lb_503": {
                "rpm": 150.0,
                "emit": ["edge_lb.access"],
                "latency_ms": [[1, 30]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            "customer_api_ok": {
                "rpm": 160.0,
                "emit": ["api_service.api_req_ok", "edge_lb.access"],
                "latency_ms": [[30, 3000], [2, 80]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "customer_api_backend_500": {
                "rpm": 40.0,
                "emit": ["api_service.api_req_error", "edge_lb.access"],
                "latency_ms": [[200, 12000], [2, 120]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "customer_api_lb_503": {
                "rpm": 120.0,
                "emit": ["edge_lb.access"],
                "latency_ms": [[1, 40]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            "scheduler_dequeue_ok": {
                "rpm": 120.0,
                "emit": ["scheduler_service.dequeue_ok", "builder_fleet.build_started"],
                "latency_ms": [[20, 5000], [40, 300]],
                "retry": {
                    "max_attempts": 2,
                    "expected_attempts": 1.4,
                    "emit_per_retry": ["scheduler_service.db_retry"],
                    "backoff_ms": [[100, 1200]],
                },
                "trace": False,
            },
            "scheduler_dequeue_timeout": {
                "rpm": 120.0,
                "emit": ["scheduler_service.dequeue_timeout"],
                "latency_ms": [[800, 8000]],
                "retry": {
                    "max_attempts": 2,
                    "expected_attempts": 1.8,
                    "emit_per_retry": ["scheduler_service.db_retry"],
                    "backoff_ms": [[200, 1500]],
                },
                "trace": False,
            },
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "circleci_queue_db_lock_webhook_surge",
    "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}}},
    "events": [
        {
            "order": 1,
            "at_min": 25,
            "rate_multipliers": {
                "github_webhook_enqueue_timeout": 0.0,
                "github_webhook_lb_503": 0.0,
                "scheduler_dequeue_timeout": 0.0,
                "customer_api_backend_500": 0.0,
                "customer_api_lb_503": 0.0,
                "postgres_db.pool_stats": 0.0,
                "postgres_db.lock_alert": 0.0,
                "postgres_db.query_timeout": 0.0,
                "postgres_db.slow_query": 0.0,
                "scheduler_service.queue_stats": 0.0,
                "scheduler_service.queue_pressure": 0.0,
                "scheduler_service.reenqueue_build": 0.0,
                "ops_repl.drain_progress": 0.0,
            },
            "latency_multipliers": {},
            "one_shots": [],
        },
        {
            "order": 2,
            "at_min": 27,
            "rate_multipliers": {
                "postgres_db.pool_stats": 1.0,
                "postgres_db.lock_alert": 1.0,
                "scheduler_service.queue_stats": 1.0,
                "scheduler_service.queue_pressure": 1.0,
                "github_webhook_enqueue_ok": 0.6,
                "github_webhook_enqueue_timeout": 0.8,
                "scheduler_dequeue_ok": 0.01,
                "scheduler_dequeue_timeout": 1.0,
                "customer_api_ok": 0.9,
                "customer_api_backend_500": 0.6,
                "postgres_db.query_timeout": 1.0,
                "postgres_db.slow_query": 1.0,
            },
            "latency_multipliers": {
                "github_webhook_enqueue_ok": {"p50": 2.5, "p95": 4.0},
                "github_webhook_enqueue_timeout": {"p50": 2.0, "p95": 3.0},
                "customer_api_ok": {"p50": 2.0, "p95": 4.0},
                "customer_api_backend_500": {"p50": 1.5, "p95": 2.5},
                "scheduler_dequeue_timeout": {"p50": 1.2, "p95": 1.5},
            },
            "one_shots": [],
        },
        {
            "order": 3,
            "at_min": 31,
            "rate_multipliers": {
                "github_webhook_lb_503": 1.0,
                "github_webhook_enqueue_ok": 0.2,
                "github_webhook_enqueue_timeout": 0.3,
                "customer_api_lb_503": 1.0,
                "customer_api_ok": 0.2,
                "customer_api_backend_500": 0.2,
            },
            "latency_multipliers": {},
            "one_shots": [{"ref": "edge_lb.capacity_throttle", "count": 1, "hosts": ["lb1"]}],
        },
        {
            "order": 4,
            "at_min": 33,
            "rate_multipliers": {
                "github_webhook_lb_503": 0.0,
                "customer_api_lb_503": 0.0,
                "customer_api_ok": 0.9,
                "customer_api_backend_500": 0.7,
                "github_webhook_enqueue_ok": 0.5,
                "github_webhook_enqueue_timeout": 0.9,
                "postgres_db.query_timeout": 1.3,
                "postgres_db.slow_query": 1.2,
                "scheduler_service.reenqueue_build": 1.5,
            },
            "latency_multipliers": {},
            "one_shots": [
                {"ref": "edge_lb.capacity_rollback", "count": 1, "hosts": ["lb1"]},
                {"ref": "ops_repl.db_stepdown", "count": 1, "hosts": ["ops1"]},
            ],
        },
        {
            "order": 5,
            "at_min": 40,
            "rate_multipliers": {
                "scheduler_service.reenqueue_build": 0.0,
                "builder_fleet.heartbeat": 0.3,
                "postgres_db.query_timeout": 0.7,
                "postgres_db.slow_query": 2.0,
                "github_webhook_enqueue_ok": 0.5,
                "github_webhook_enqueue_timeout": 0.7,
                "customer_api_ok": 0.95,
                "customer_api_backend_500": 0.4,
                "scheduler_dequeue_ok": 0.02,
                "scheduler_dequeue_timeout": 0.8,
            },
            "latency_multipliers": {
                "github_webhook_enqueue_timeout": {"p50": 0.9, "p95": 0.9},
                "customer_api_backend_500": {"p50": 0.9, "p95": 0.9},
            },
            "one_shots": [
                {"ref": "ops_repl.auto_reenqueue_disabled", "count": 1, "hosts": ["ops1"]},
                {"ref": "ops_repl.terminate_builders", "count": 1, "hosts": ["ops1"]},
                {"ref": "ops_repl.repl_patch", "count": 2, "hosts": ["ops1"]},
                {"ref": "ops_repl.collect_slow_queries", "count": 1, "hosts": ["ops1"]},
            ],
        },
        {
            "order": 6,
            "at_min": 46,
            "rate_multipliers": {
                "ops_repl.drain_progress": 1.0,
                "scheduler_dequeue_ok": 0.05,
                "scheduler_dequeue_timeout": 0.7,
                "customer_api_backend_500": 0.3,
                "github_webhook_enqueue_timeout": 0.6,
            },
            "latency_multipliers": {"scheduler_dequeue_timeout": {"p50": 0.9, "p95": 0.9}},
            "one_shots": [{"ref": "ops_repl.drain_script_start", "count": 2, "hosts": ["ops1"]}],
        },
    ],
}

# -----------------------------
# Deterministic helpers
# -----------------------------

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
SEED = 1337

random.seed(SEED)
np.random.seed(SEED)


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def md5_int(s: str) -> int:
    return int(md5_hex(s), 16)


def stable_unit(s: str) -> float:
    return (md5_int(s) % 10_000_000) / 10_000_000.0


def stable_choice(values: List[Any], key: str) -> Any:
    if not values:
        return ""
    return values[md5_int(key) % len(values)]


def stable_hex_n(n: int, key: str) -> str:
    h = md5_hex(key)
    while len(h) < n:
        h += md5_hex(h)
    return h[:n]


def stable_uuid(key: str) -> str:
    h = stable_hex_n(32, "uuid|" + key)
    h = list(h)
    h[12] = "4"
    h[16] = stable_choice(list("89ab"), "uuidvar|" + key)
    h = "".join(h)
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def choose_int_range(a: int, b: int, key: str) -> int:
    if b < a:
        a, b = b, a
    span = b - a + 1
    return a + (md5_int(key) % span)


def choose_float_range(a: float, b: float, key: str, ndp: int = 2) -> float:
    u = stable_unit("f|" + key)
    x = a + (b - a) * u
    return float(round(x, ndp))


def iso_ms(dt: datetime) -> str:
    s = dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")
    return s[:-3] + "Z"


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def inv_norm_cdf(p: float) -> float:
    p = clamp(float(p), 1e-12, 1.0 - 1e-12)
    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02, 1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]
    plow = 0.02425
    phigh = 1.0 - plow

    if p < plow:
        q = math.sqrt(-2.0 * math.log(p))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return num / den

    if p > phigh:
        q = math.sqrt(-2.0 * math.log(1.0 - p))
        num = -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return num / den

    q = p - 0.5
    r = q * q
    num = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    den = (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    return num / den


def lognormal_quantile_from_p50_p95(p50: float, p95: float, u: float) -> float:
    p50 = max(0.001, float(p50))
    p95 = max(p50 * 1.001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - mu) / 1.645
    z = inv_norm_cdf(clamp(u, 1e-12, 1.0 - 1e-12))
    return float(math.exp(mu + sigma * z))


def sample_ms(p50: float, p95: float, key: str, cap_min: Optional[int] = None, cap_max: Optional[int] = None) -> int:
    u = 0.55 + 0.40 * stable_unit("u|" + key)  # [0.55,0.95)
    x = lognormal_quantile_from_p50_p95(p50, p95, u)
    soft_cap = 3.0 * float(p95)
    x = min(x, soft_cap)
    if cap_min is not None:
        x = max(x, float(cap_min))
    if cap_max is not None:
        x = min(x, float(cap_max))
    return int(round(x))


def schedule_times(start_dt: datetime, end_dt: datetime, n: int, key: str) -> List[datetime]:
    if n <= 0:
        return []
    dur_s = (end_dt - start_dt).total_seconds()
    if dur_s <= 0:
        return []
    spacing = dur_s / n
    jitter_max = min(0.2 * spacing, 0.2)
    out: List[datetime] = []
    for i in range(n):
        frac = (i + 0.5) / n
        t = start_dt + timedelta(seconds=dur_s * frac)
        j = (stable_unit(f"jit|{key}|{i}") - 0.5) * 2.0 * jitter_max
        t = t + timedelta(seconds=j)
        if t < start_dt:
            t = start_dt
        latest = end_dt - timedelta(milliseconds=1)
        if t > latest:
            t = latest
        out.append(t)
    return out


_RESIDUALS: Dict[str, float] = {}


def alloc_count(expected: float, key: str) -> int:
    r = _RESIDUALS.get(key, 0.0)
    x = expected + r
    n = int(math.floor(x + 1e-12))
    _RESIDUALS[key] = x - n
    return max(0, n)


def parse_ref(ref: str) -> Tuple[str, str]:
    comp_id, log_id = ref.split(".", 1)
    return comp_id, log_id


def get_template(comp_id: str, log_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][comp_id]["logs"][log_id]


def domain_for_var(comp_id: str, log_id: str, var: str, state: str) -> Optional[Dict[str, Any]]:
    tmpl = get_template(comp_id, log_id)
    if "state_vars" in tmpl and tmpl["state_vars"]:
        sv = tmpl["state_vars"].get(state, {})
        if var in sv:
            return sv[var]
    return tmpl.get("vars", {}).get(var)


def render_message(comp_id: str, log_id: str, state: str, bound: Dict[str, Any], key: str) -> str:
    tmpl = get_template(comp_id, log_id)
    msg = tmpl["msg"]

    names: List[str] = []
    i = 0
    while i < len(msg):
        if msg[i] == "{":
            j = msg.find("}", i + 1)
            if j != -1:
                names.append(msg[i + 1 : j])
                i = j + 1
            else:
                i += 1
        else:
            i += 1

    vals: Dict[str, Any] = {}
    for name in names:
        if name in bound:
            vals[name] = bound[name]
            continue
        dom = domain_for_var(comp_id, log_id, name, state)
        if not dom:
            vals[name] = ""
            continue
        k = dom["k"]
        v = dom.get("v")
        dkey = f"{key}|{comp_id}.{log_id}|{name}"
        if k == "ch":
            vals[name] = stable_choice(list(v), dkey)
        elif k == "i":
            a, b = int(v[0]), int(v[1])
            vals[name] = choose_int_range(a, b, dkey)
        elif k == "f":
            a, b = float(v[0]), float(v[1])
            vals[name] = choose_float_range(a, b, dkey, ndp=2)
        elif k == "hex":
            vals[name] = stable_hex_n(int(v), dkey)
        elif k == "uuid":
            vals[name] = stable_uuid(dkey)
        else:
            vals[name] = ""

    for kk, vv in list(vals.items()):
        if isinstance(vv, float):
            vals[kk] = f"{vv:.2f}"

    try:
        return msg.format_map(vals)
    except Exception:
        return msg


# -----------------------------
# Scenario control derivation
# -----------------------------

@dataclass(frozen=True)
class Interval:
    start_min: int
    end_min: int
    state: str
    rate_mult: Dict[str, float]
    lat_mult: Dict[str, Dict[str, float]]  # flow_id -> {p50,p95}


def build_failure_intervals() -> List[Interval]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["events"], key=lambda e: (e["at_min"], e["order"]))

    boundaries = [f_start] + sorted({e["at_min"] for e in events if f_start <= e["at_min"] < f_end}) + [f_end]
    boundaries = sorted(boundaries)

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}

    intervals: List[Interval] = []
    for i in range(len(boundaries) - 1):
        s = boundaries[i]
        e = boundaries[i + 1]

        for ev in events:
            if ev["at_min"] == s:
                for k, v in ev.get("rate_multipliers", {}).items():
                    active_rate[k] = float(v)
                for fk, mv in ev.get("latency_multipliers", {}).items():
                    active_lat[fk] = {"p50": float(mv.get("p50", 1.0)), "p95": float(mv.get("p95", 1.0))}
        intervals.append(Interval(start_min=s, end_min=e, state="f", rate_mult=dict(active_rate), lat_mult=dict(active_lat)))
    return intervals


FAILURE_INTERVALS = build_failure_intervals()


def failure_controls_at(minute: int) -> Interval:
    for iv in FAILURE_INTERVALS:
        if iv.start_min <= minute < iv.end_min:
            return iv
    return FAILURE_INTERVALS[-1]


# -----------------------------
# Simple state-dependent models for coherent values
# -----------------------------

def queue_sizes_at(minute_f: float) -> Tuple[int, int]:
    m = float(minute_f)
    if m < 25:
        run_q = int(80 + 18 * m)
        usage_q = int(120 + 28 * m)
        return max(0, min(2000, run_q)), max(0, min(4000, usage_q))

    def lerp(a: float, b: float, t: float) -> float:
        return a + (b - a) * t

    if 25 <= m < 27:
        t = (m - 25) / 2.0
        run_q = lerp(600, 2000, t)
        usage_q = lerp(1200, 4000, t)
    elif 27 <= m < 31:
        t = (m - 27) / 4.0
        run_q = lerp(2000, 9000, t)
        usage_q = lerp(4000, 12000, t)
    elif 31 <= m < 33:
        t = (m - 31) / 2.0
        run_q = lerp(9000, 11000, t)
        usage_q = lerp(12000, 15000, t)
    elif 33 <= m < 40:
        t = (m - 33) / 7.0
        run_q = lerp(11000, 17000, t)
        usage_q = lerp(15000, 24000, t)
    elif 40 <= m < 46:
        t = (m - 40) / 6.0
        run_q = lerp(17000, 15000, t)
        usage_q = lerp(24000, 20000, t)
    else:
        t = (m - 46) / 4.0
        run_q = lerp(15000, 9000, t)
        usage_q = lerp(20000, 12000, t)

    return int(clamp(run_q, 0, 20000)), int(clamp(usage_q, 0, 30000))


def db_pressure_at(minute_f: float) -> float:
    m = float(minute_f)
    if m < 25:
        return 0.15
    if 25 <= m < 27:
        return 0.35
    if 27 <= m < 33:
        return 0.95
    if 33 <= m < 40:
        return 0.90
    if 40 <= m < 46:
        return 0.75
    return 0.65


def effective_rate_multiplier(state: str, key: str, iv: Optional[Interval]) -> float:
    if state != "f" or iv is None:
        return 1.0
    return float(iv.rate_mult.get(key, 1.0))


def effective_latency_multiplier(flow_id: str, iv: Optional[Interval]) -> Tuple[float, float]:
    if iv is None:
        return (1.0, 1.0)
    mv = iv.lat_mult.get(flow_id)
    if not mv:
        return (1.0, 1.0)
    return (float(mv.get("p50", 1.0)), float(mv.get("p95", 1.0)))


# -----------------------------
# Flow simulation
# -----------------------------

def choose_component_host(comp_id: str, key: str) -> str:
    hosts = SYSTEM["components"][comp_id]["hosts"]
    if not hosts:
        return ""
    return hosts[md5_int("host|" + key) % len(hosts)]


def template_caps(comp_id: str, log_id: str) -> Dict[str, Tuple[Optional[int], Optional[int]]]:
    tmpl = get_template(comp_id, log_id)
    caps: Dict[str, Tuple[Optional[int], Optional[int]]] = {}
    for var, dom in tmpl.get("vars", {}).items():
        if dom.get("k") == "i" and isinstance(dom.get("v"), list) and len(dom["v"]) == 2:
            caps[var] = (int(dom["v"][0]), int(dom["v"][1]))
    if tmpl.get("state_vars"):
        for _, sv in tmpl["state_vars"].items():
            for var, dom in sv.items():
                if dom.get("k") == "i" and isinstance(dom.get("v"), list) and len(dom["v"]) == 2:
                    caps[var] = (int(dom["v"][0]), int(dom["v"][1]))
    return caps


CAPS_CACHE: Dict[str, Dict[str, Tuple[Optional[int], Optional[int]]]] = {}


def caps_for(comp_id: str, log_id: str) -> Dict[str, Tuple[Optional[int], Optional[int]]]:
    k = f"{comp_id}.{log_id}"
    if k not in CAPS_CACHE:
        CAPS_CACHE[k] = template_caps(comp_id, log_id)
    return CAPS_CACHE[k]


def simulate_flow_instance(
    flow_id: str,
    flow_def: Dict[str, Any],
    state: str,
    start_dt: datetime,
    instance_key: str,
    iv: Optional[Interval],
    attempt_count: int,
    rows: List[Dict[str, str]],
) -> None:
    emit_refs = flow_def["emit"]
    lat_hints = flow_def["latency_ms"]
    retry_def = flow_def["retry"]
    backoff_hints = retry_def.get("backoff_ms", [])
    emit_per_retry = retry_def.get("emit_per_retry", [])
    trace_on = bool(flow_def.get("trace", False)) and bool(SYSTEM["tracing"]["on"])
    trace_id = stable_hex_n(32, "trace|" + instance_key) if trace_on else ""

    comp_hosts: Dict[str, str] = {}
    for ref in set(emit_refs + emit_per_retry):
        cid, _ = parse_ref(ref)
        comp_hosts[cid] = choose_component_host(cid, instance_key + "|" + cid)

    req_id = stable_hex_n(16, "req|" + instance_key)
    delivery_id = stable_uuid("delivery|" + instance_key)
    build_id = stable_uuid("build|" + instance_key)
    user_id = choose_int_range(1000, 9000, "user|" + instance_key)
    tx_id = stable_hex_n(12, "tx|" + instance_key)

    if flow_id.startswith("github_webhook"):
        method = "POST"
        route = "/hooks/github"
        repo = stable_choice(["acme/api", "acme/web", "beta/mobile", "gamma/data"], "repo|" + instance_key)
        gh_event = stable_choice(["push", "pull_request"], "ghevent|" + instance_key)
        upstream = "api_service"
    elif flow_id.startswith("customer_api"):
        method = "GET"
        route = stable_choice(["/api/v2/me", "/api/v2/project"], "route|" + instance_key)
        repo = ""
        gh_event = ""
        upstream = "api_service"
    else:
        method = ""
        route = ""
        repo = ""
        gh_event = ""
        upstream = ""

    if flow_id == "github_webhook_enqueue_ok":
        lb_status = 200
    elif flow_id == "github_webhook_enqueue_timeout":
        lb_status = 504
    elif flow_id == "github_webhook_lb_503":
        lb_status = 503
        upstream = "none"
    elif flow_id == "customer_api_ok":
        lb_status = 200
    elif flow_id == "customer_api_backend_500":
        api_status = stable_choice([500, 503], "apistatus|" + instance_key)
        lb_status = api_status
    elif flow_id == "customer_api_lb_503":
        lb_status = 503
        upstream = "none"
    else:
        lb_status = 200

    lat_p50_mult, lat_p95_mult = effective_latency_multiplier(flow_id, iv) if state == "f" else (1.0, 1.0)

    current = start_dt
    start_ms_anchor = start_dt

    def minute_of(dt: datetime) -> float:
        return (dt - BASE_TIME).total_seconds() / 60.0

    # Retry modeling adjustment:
    # Some flows include terminal side effects (e.g., builder start) in emit[] but also specify retries.
    # To avoid duplicating terminal side effects on retries, treat such flows as "final-only emit":
    # - attempts < final: advance time for the retryable step only (first latency hint), emit nothing
    # - final attempt: emit full emit[] chain
    emit_mode = "per_attempt"
    if int(retry_def.get("max_attempts", 1)) > 1:
        if any(ref.startswith("builder_fleet.") for ref in emit_refs):
            emit_mode = "final_only"

    backoff_ms = 0
    if attempt_count > 1 and backoff_hints:
        p50, p95 = backoff_hints[0]
        cap = caps_for("scheduler_service", "db_retry").get("backoff_ms", (None, None))[1]
        backoff_ms = sample_ms(p50, p95, "backoff|" + instance_key, cap_min=100, cap_max=cap)

    for attempt in range(1, attempt_count + 1):
        # If this is a final-only flow, earlier attempts do not emit terminal actions/logs.
        # They still consume time for the retryable operation (modeled as the first latency hint).
        if emit_mode == "final_only" and attempt < attempt_count:
            hint = lat_hints[0] if lat_hints else [1, 10]
            p50 = float(hint[0]) * lat_p50_mult
            p95 = float(hint[1]) * lat_p95_mult
            cid0, lid0 = parse_ref(emit_refs[0])
            caps0 = caps_for(cid0, lid0)
            cap_min, cap_max = None, None
            if cid0 == "scheduler_service" and lid0 == "dequeue_ok":
                cap_min, cap_max = caps0.get("db_ms", (None, None))
            delta_ms = sample_ms(p50, p95, f"lat|{instance_key}|a{attempt}|hidden|{cid0}.{lid0}|0", cap_min=cap_min, cap_max=cap_max)
            current = current + timedelta(milliseconds=delta_ms)
            # proceed to next attempt (which will apply backoff and emit retry marker)
            continue

        if attempt > 1:
            current = current + timedelta(milliseconds=backoff_ms)
            for ref in emit_per_retry:
                cid, lid = parse_ref(ref)
                host = comp_hosts.get(cid, choose_component_host(cid, instance_key + "|" + cid))
                bound = {"tx_id": tx_id, "attempt": 2, "backoff_ms": int(backoff_ms)}
                msg = render_message(cid, lid, state, bound, f"{instance_key}|a{attempt}|retry")
                rows.append(
                    {
                        "timestamp": iso_ms(current + timedelta(milliseconds=1)),
                        "level": get_template(cid, lid)["lvl"],
                        "message": msg,
                        "trace_id": trace_id if flow_def.get("trace", False) else "",
                        "service": SYSTEM["components"][cid]["svc"] or "",
                        "host": host or "",
                    }
                )

        total_elapsed_ms = int(round((current - start_ms_anchor).total_seconds() * 1000.0))
        for j, ref in enumerate(emit_refs):
            cid, lid = parse_ref(ref)
            hint = lat_hints[j] if j < len(lat_hints) else [1, 10]
            p50 = float(hint[0]) * lat_p50_mult
            p95 = float(hint[1]) * lat_p95_mult

            caps = caps_for(cid, lid)
            cap_max = None
            cap_min = None
            if cid == "api_service" and lid in ("api_req_ok", "api_req_error"):
                cap_min, cap_max = caps.get("dur_ms", (None, None))
            elif cid == "api_service" and lid in ("enqueue_ok", "enqueue_timeout"):
                cap_min, cap_max = caps.get("db_ms", (None, None))
            elif cid == "scheduler_service" and lid == "dequeue_ok":
                cap_min, cap_max = caps.get("db_ms", (None, None))
            elif cid == "scheduler_service" and lid == "dequeue_timeout":
                cap_min, cap_max = caps.get("timeout_ms", (None, None))
            elif cid == "edge_lb" and lid == "access" and len(emit_refs) == 1:
                cap_min, cap_max = caps.get("dur_ms", (None, None))

            prev_current = current
            delta_ms = sample_ms(p50, p95, f"lat|{instance_key}|a{attempt}|{cid}.{lid}|{j}", cap_min=cap_min, cap_max=cap_max)
            current = current + timedelta(milliseconds=delta_ms)
            total_elapsed_ms = int(round((current - start_ms_anchor).total_seconds() * 1000.0))

            host = comp_hosts.get(cid, choose_component_host(cid, instance_key + "|" + cid))

            bound: Dict[str, Any] = {}
            if cid == "api_service" and lid == "webhook_received":
                bound = {"delivery_id": delivery_id, "gh_event": gh_event, "repo": repo}
            elif cid == "api_service" and lid == "enqueue_ok":
                bound = {"build_id": build_id, "repo": repo, "queue": "run", "db_ms": int(clamp(delta_ms, 5, 3000))}
            elif cid == "api_service" and lid == "enqueue_timeout":
                bound = {"repo": repo, "db_ms": int(clamp(delta_ms, 500, 8000))}
            elif cid == "api_service" and lid == "api_req_ok":
                dur_ms = int(clamp(delta_ms, 5, 12000))
                db_ms = int(clamp(int(round(dur_ms * 0.6)), 1, 4000))
                bound = {"method": method, "endpoint": route, "user_id": user_id, "db_ms": db_ms, "dur_ms": dur_ms}
            elif cid == "api_service" and lid == "api_req_error":
                dur_ms = int(clamp(delta_ms, 100, 12000))
                db_ms = int(clamp(int(round(dur_ms * 0.7)), 100, 8000))
                err = stable_choice(["db_timeout", "db_overloaded"], "apierr|" + instance_key)
                status = lb_status if flow_id == "customer_api_backend_500" else stable_choice([500, 503], "apistatus2|" + instance_key)
                bound = {"method": method, "endpoint": route, "status": int(status), "user_id": user_id, "err": err, "db_ms": db_ms, "dur_ms": dur_ms}
            elif cid == "edge_lb" and lid == "access":
                max_dur = caps_for("edge_lb", "access").get("dur_ms", (1, 15000))[1] or 15000
                if total_elapsed_ms > max_dur:
                    allowed_total = max_dur
                    target = start_ms_anchor + timedelta(milliseconds=allowed_total)
                    if target <= prev_current:
                        target = prev_current + timedelta(milliseconds=1)
                        allowed_total = int(round((target - start_ms_anchor).total_seconds() * 1000.0))
                    current = target
                    total_elapsed_ms = allowed_total

                if flow_id.startswith("github_webhook"):
                    bound_method, bound_route = "POST", "/hooks/github"
                elif flow_id.startswith("customer_api"):
                    bound_method, bound_route = "GET", route
                else:
                    bound_method = stable_choice(["GET", "POST"], "lbmeth|" + instance_key)
                    bound_route = stable_choice(["/hooks/github", "/api/v2/me", "/api/v2/project"], "lbroute|" + instance_key)

                bound = {
                    "method": bound_method,
                    "route": bound_route,
                    "status": int(lb_status),
                    "upstream": upstream,
                    "req_id": req_id,
                    "dur_ms": int(clamp(total_elapsed_ms, 1, 15000)),
                }
            elif cid == "scheduler_service" and lid == "dequeue_ok":
                run_q, usage_q = queue_sizes_at(minute_of(current))
                db_ms = int(clamp(delta_ms, 2, 8000))
                bound = {"build_id": build_id, "db_ms": db_ms, "run_q": run_q, "usage_q": usage_q}
            elif cid == "scheduler_service" and lid == "dequeue_timeout":
                run_q, usage_q = queue_sizes_at(minute_of(current))
                timeout_ms = int(clamp(delta_ms, 500, 8000))
                bound = {"tx_id": tx_id, "timeout_ms": timeout_ms, "run_q": run_q, "usage_q": usage_q}
            elif cid == "builder_fleet" and lid == "build_started":
                bound = {"builder_host": host, "build_id": build_id}

            msg = render_message(cid, lid, state, bound, f"{instance_key}|a{attempt}|{cid}.{lid}|{j}")
            rows.append(
                {
                    "timestamp": iso_ms(current),
                    "level": get_template(cid, lid)["lvl"],
                    "message": msg,
                    "trace_id": trace_id,
                    "service": SYSTEM["components"][cid]["svc"] or "",
                    "host": host or "",
                }
            )


# -----------------------------
# Background and one-shots
# -----------------------------

def bind_background_vars(comp_id: str, log_id: str, state: str, t: datetime, iv: Optional[Interval], key: str) -> Dict[str, Any]:
    minute_f = (t - BASE_TIME).total_seconds() / 60.0
    run_q, usage_q = queue_sizes_at(minute_f)
    pressure = db_pressure_at(minute_f)

    if comp_id == "scheduler_service" and log_id == "queue_stats":
        if state == "f" and iv is not None:
            eff_deq_rpm = SYSTEM["flows"]["f"]["scheduler_dequeue_ok"]["rpm"] * iv.rate_mult.get("scheduler_dequeue_ok", 1.0)
            eff_reenq_per_host = SYSTEM["components"]["scheduler_service"]["beh"]["f"][2]["per_min"] * iv.rate_mult.get("scheduler_service.reenqueue_build", 1.0)
        else:
            eff_deq_rpm = SYSTEM["flows"]["n"]["scheduler_dequeue_ok"]["rpm"]
            eff_reenq_per_host = SYSTEM["components"]["scheduler_service"]["beh"]["n"][1]["per_min"]

        dequeue_rps = clamp(eff_deq_rpm / 60.0, 0.0 if state == "f" else 1.0, 3.5)
        reenqueue_rps = clamp(eff_reenq_per_host / 60.0, 0.0, 3.0 if state == "f" else 0.05)
        return {
            "run_q": int(run_q),
            "usage_q": int(usage_q),
            "dequeue_rps": float(round(dequeue_rps, 2)),
            "reenqueue_rps": float(round(reenqueue_rps, 2)),
        }

    if comp_id == "scheduler_service" and log_id == "queue_pressure":
        run_q_p = int(max(run_q, 5000))
        usage_q_p = int(max(usage_q, 8000))
        deq = 0.05 + 0.25 * (1.0 - min(1.0, pressure))
        deq = clamp(deq, 0.0, 0.5)
        note = "backlog_growing" if run_q_p > 12000 else "dequeue_slow"
        return {"run_q": run_q_p, "usage_q": usage_q_p, "dequeue_rps": float(round(deq, 2)), "note": note}

    if comp_id == "postgres_db" and log_id == "pool_stats":
        tmpl = get_template("postgres_db", "pool_stats")
        sv = tmpl.get("state_vars", {}).get(state, {})
        a0, a1 = sv["active"]["v"]
        w0, w1 = sv["waiting"]["v"]
        l0, l1 = sv["locks_waiting"]["v"]
        c0, c1 = sv["cpu_pct"]["v"]

        active = int(clamp(a0 + (a1 - a0) * (0.3 + 0.7 * pressure), a0, a1))
        waiting = int(clamp(w0 + (w1 - w0) * pressure, w0, w1))
        locks = int(clamp(l0 + (l1 - l0) * (pressure**1.2), l0, l1))
        cpu = int(clamp(c0 + (c1 - c0) * (0.2 + 0.8 * pressure), c0, c1))

        active = int(clamp(active + choose_int_range(-3, 3, "dbact|" + key), a0, a1))
        waiting = int(clamp(waiting + choose_int_range(-5, 5, "dbwait|" + key), w0, w1))
        locks = int(clamp(locks + choose_int_range(-4, 4, "dblock|" + key), l0, l1))
        cpu = int(clamp(cpu + choose_int_range(-4, 4, "dbcpu|" + key), c0, c1))

        return {"active": active, "waiting": waiting, "locks_waiting": locks, "cpu_pct": cpu}

    if comp_id == "postgres_db" and log_id == "lock_alert":
        waiting = int(clamp(50 + 200 * pressure, 50, 250))
        locks = int(clamp(20 + 100 * (pressure**1.1), 20, 120))
        top = "api_service" if stable_unit("top|" + key) < 0.6 else "scheduler_service"
        window_s = 60 if pressure > 0.8 else 30
        return {"waiting": waiting, "locks_waiting": locks, "top_client": top, "window_s": window_s}

    if comp_id == "postgres_db" and log_id == "query_timeout":
        client = "scheduler_service" if pressure > 0.8 and stable_unit("qtcli|" + key) < 0.7 else "api_service"
        stmt = stable_choice(["insert_build", "select_next_build", "usage_check"], "qtstmt|" + key)
        timeout_ms = choose_int_range(500, 8000, "qtm|" + key)
        return {"stmt": stmt, "client": client, "timeout_ms": timeout_ms}

    if comp_id == "postgres_db" and log_id == "slow_query":
        client = "scheduler_service" if pressure > 0.8 and stable_unit("sqcli|" + key) < 0.55 else "api_service"
        stmt = stable_choice(["insert_build", "select_next_build", "usage_check"], "sqstmt|" + key)
        dur_ms = int(clamp(600 + 28000 * (0.3 + 0.7 * pressure) * stable_unit("sqdur|" + key), 500, 30000))
        lock_wait_ms = int(clamp(dur_ms * stable_unit("sqlw|" + key), 0, 30000))
        rows = int(clamp(20000 * stable_unit("sqrows|" + key), 0, 20000))
        return {"stmt": stmt, "client": client, "dur_ms": dur_ms, "lock_wait_ms": lock_wait_ms, "rows": rows}

    if comp_id == "builder_fleet" and log_id == "heartbeat":
        terminated_bias = 0.0
        if state == "f" and iv is not None:
            hb_mult = iv.rate_mult.get("builder_fleet.heartbeat", 1.0)
            if hb_mult < 0.6:
                terminated_bias = 0.4
        base_run = int(clamp(6 - 5 * terminated_bias + choose_int_range(-1, 1, "brun|" + key), 0, 8))
        slots_free = int(clamp(8 - base_run + choose_int_range(-1, 1, "bfree|" + key), 0, 8))
        return {"slots_free": slots_free, "running": base_run}

    if comp_id == "ops_repl" and log_id == "drain_progress":
        queue = "usage" if (md5_int("dq|" + key) % 2 == 0) else "run"
        start_m = 46.0
        elapsed = max(0.0, minute_f - start_m)
        initial = 380000 if queue == "usage" else 260000
        rate = int(clamp(25000 + 25000 * stable_unit("drate|" + key), 5000, 60000))
        deleted = int(min(initial, elapsed * rate))
        remaining = int(max(0, initial - deleted))
        return {"queue": queue, "remaining": remaining, "deleted": deleted, "rate_per_min": rate}

    if comp_id == "scheduler_service" and log_id == "reenqueue_build":
        return {
            "build_id": stable_uuid("reenq|" + key),
            "reason": stable_choice(["infra_failure", "db_failover", "runner_lost"], "rreason|" + key),
        }

    return {}


def simulate_background_for_interval(iv_state: str, start_min: int, end_min: int, iv: Optional[Interval], rows: List[Dict[str, str]]) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    duration_min = float(end_min - start_min)

    for comp_id, comp in SYSTEM["components"].items():
        beh = comp.get("beh", {}).get(iv_state, [])
        if not beh:
            continue

        for src in beh:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            mult_key = f"{comp_id}.{log_id}"
            mult = effective_rate_multiplier(iv_state, mult_key, iv)
            eff_per_min = per_min * mult

            if eff_per_min <= 0:
                continue

            if scope == "global":
                expected = eff_per_min * duration_min
                n = alloc_count(expected, f"bg|{iv_state}|{start_min}-{end_min}|{mult_key}|global")
                times = schedule_times(start_dt, end_dt, n, f"bg|{iv_state}|{start_min}-{end_min}|{mult_key}|global")
                host = comp["hosts"][0] if comp["hosts"] else ""
                for i, t in enumerate(times):
                    bound = bind_background_vars(comp_id, log_id, iv_state, t, iv, f"bg|{mult_key}|{start_min}|{i}")
                    msg = render_message(comp_id, log_id, iv_state, bound, f"bg|{mult_key}|{start_min}|{i}")
                    rows.append(
                        {
                            "timestamp": iso_ms(t),
                            "level": get_template(comp_id, log_id)["lvl"],
                            "message": msg,
                            "trace_id": "",
                            "service": comp["svc"] or "",
                            "host": host,
                        }
                    )
            else:
                for host in comp.get("hosts", []) or [""]:
                    expected = eff_per_min * duration_min
                    n = alloc_count(expected, f"bg|{iv_state}|{start_min}-{end_min}|{mult_key}|{host}")
                    times = schedule_times(start_dt, end_dt, n, f"bg|{iv_state}|{start_min}-{end_min}|{mult_key}|{host}")
                    for i, t in enumerate(times):
                        bound = bind_background_vars(comp_id, log_id, iv_state, t, iv, f"bg|{mult_key}|{host}|{start_min}|{i}")
                        msg = render_message(comp_id, log_id, iv_state, bound, f"bg|{mult_key}|{host}|{start_min}|{i}")
                        rows.append(
                            {
                                "timestamp": iso_ms(t),
                                "level": get_template(comp_id, log_id)["lvl"],
                                "message": msg,
                                "trace_id": "",
                                "service": comp["svc"] or "",
                                "host": host,
                            }
                        )


def emit_one_shots(rows: List[Dict[str, str]]) -> None:
    for ev in SCENARIO["events"]:
        at_min = int(ev["at_min"])
        base_dt = BASE_TIME + timedelta(minutes=at_min)
        for os_idx, shot in enumerate(ev.get("one_shots", [])):
            ref = shot["ref"]
            count = int(shot["count"])
            allowed_hosts = shot.get("hosts", [])
            cid, lid = parse_ref(ref)
            comp = SYSTEM["components"][cid]
            host = allowed_hosts[0] if allowed_hosts else (comp["hosts"][0] if comp["hosts"] else "")
            for i in range(count):
                t = base_dt + timedelta(milliseconds=int(100 + 250 * stable_unit(f"oneshot|{ref}|{at_min}|{os_idx}|{i}")))
                bound: Dict[str, Any] = {}
                if cid == "edge_lb" and lid in ("capacity_throttle", "capacity_rollback"):
                    dom = get_template(cid, lid)["vars"]["max_inflight"]["v"]
                    bound["max_inflight"] = choose_int_range(int(dom[0]), int(dom[1]), f"maxinf|{ref}|{at_min}|{i}")
                elif cid == "ops_repl" and lid == "db_stepdown":
                    frm = stable_choice(["db1", "db2"], f"stepfrom|{at_min}")
                    to = "db2" if frm == "db1" else "db1"
                    bound = {"from_host": frm, "to_host": to}
                elif cid == "ops_repl" and lid == "repl_patch":
                    patch = stable_choice(["disable_auto_reenqueue", "disable_hot_usage_query"], f"patch|{at_min}|{i}")
                    actor = stable_choice(["oncall1", "oncall2"], f"actor|{at_min}|{i}")
                    bound = {"patch": patch, "actor": actor}
                elif cid == "ops_repl" and lid == "terminate_builders":
                    bound = {"count": choose_int_range(5, 20, f"tcount|{at_min}"), "reason": "reduce_db_load"}
                elif cid == "ops_repl" and lid == "collect_slow_queries":
                    bound = {"window_s": choose_int_range(300, 1800, f"csq|{at_min}")}
                elif cid == "ops_repl" and lid == "drain_script_start":
                    queue = "usage" if i == 0 else "run"
                    bound = {"queue": queue, "batch_size": choose_int_range(1000, 5000, f"dbatch|{at_min}|{queue}")}

                msg = render_message(cid, lid, "f", bound, f"oneshot|{ref}|{at_min}|{i}")
                rows.append(
                    {
                        "timestamp": iso_ms(t),
                        "level": get_template(cid, lid)["lvl"],
                        "message": msg,
                        "trace_id": "",
                        "service": comp["svc"] or "",
                        "host": host,
                    }
                )


# -----------------------------
# Main simulation
# -----------------------------

def simulate_flows_for_interval(state: str, start_min: int, end_min: int, iv: Optional[Interval], rows: List[Dict[str, str]]) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    duration_min = float(end_min - start_min)

    flows = SYSTEM["flows"][state]
    for flow_id in sorted(flows.keys()):
        flow_def = flows[flow_id]
        base_rpm = float(flow_def["rpm"])
        mult = effective_rate_multiplier(state, flow_id, iv)
        eff_rpm = base_rpm * mult
        if eff_rpm <= 0.0:
            continue

        expected_instances = eff_rpm * duration_min
        n_instances = alloc_count(expected_instances, f"flow|{state}|{start_min}-{end_min}|{flow_id}")
        if n_instances <= 0:
            continue

        starts = schedule_times(start_dt, end_dt, n_instances, f"flow|{state}|{start_min}-{end_min}|{flow_id}")

        max_attempts = int(flow_def["retry"]["max_attempts"])
        exp_attempts = float(flow_def["retry"]["expected_attempts"])
        attempt_counts = [1] * n_instances
        if max_attempts > 1:
            extra = int(round((exp_attempts - 1.0) * n_instances))
            extra = max(0, min(extra, n_instances * (max_attempts - 1)))
            scores = [(md5_int(f"retryselect|{flow_id}|{start_min}|{i}"), i) for i in range(n_instances)]
            scores.sort(key=lambda x: x[0])
            for _, idx in scores[:extra]:
                attempt_counts[idx] = 2

        for i, st in enumerate(starts):
            inst_key = f"{state}|{flow_id}|{start_min}-{end_min}|i{i}"
            simulate_flow_instance(flow_id, flow_def, state, st, inst_key, iv, attempt_counts[i], rows)


def simulate() -> pd.DataFrame:
    rows: List[Dict[str, str]] = []

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    simulate_background_for_interval("n", n_start, n_end, None, rows)
    simulate_flows_for_interval("n", n_start, n_end, None, rows)

    for iv in FAILURE_INTERVALS:
        simulate_background_for_interval("f", iv.start_min, iv.end_min, iv, rows)
        simulate_flows_for_interval("f", iv.start_min, iv.end_min, iv, rows)

    emit_one_shots(rows)

    df = pd.DataFrame(rows, columns=["timestamp", "level", "message", "trace_id", "service", "host"])
    dts = pd.to_datetime(df["timestamp"], utc=True, format="%Y-%m-%dT%H:%M:%S.%fZ", errors="coerce")
    df["_dt"] = dts
    df = df.sort_values(["_dt", "service", "host", "level", "message"], kind="mergesort").reset_index(drop=True)

    fixed: List[datetime] = []
    prev: Optional[pd.Timestamp] = None
    for ts in df["_dt"]:
        cur = BASE_TIME if pd.isna(ts) else ts.to_pydatetime()
        if prev is not None and cur <= prev.to_pydatetime():
            cur = prev.to_pydatetime() + timedelta(milliseconds=1)
        fixed.append(cur)
        prev = pd.Timestamp(cur)

    df["timestamp"] = [iso_ms(dt) for dt in fixed]
    df = df.drop(columns=["_dt"])

    df["trace_id"] = df["trace_id"].fillna("").astype(str)
    df["service"] = df["service"].fillna("").astype(str)
    df["host"] = df["host"].fillna("").astype(str)

    return df


if __name__ == "__main__":
    df = simulate()
    df.to_csv("logs.csv", index=False)
