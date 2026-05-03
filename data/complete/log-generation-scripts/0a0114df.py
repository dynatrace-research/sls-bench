import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd

# ----------------------------
# Embedded normalized inputs
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "slack_like_realtime_messaging"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["edge_gateway", "rtm_server"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "edge_gateway",
            "svc": "edge-gw",
            "hosts": ["edge-1", "edge-2"],
            "logs": {
                "ws_upgrade_start": {
                    "lvl": "INFO",
                    "msg": "WS upgrade start team={team_id} user={user_id} client={client} rtm_target={rtm_target} trace={trace_id}",
                    "vars": {
                        "team_id": {"k": "ch", "v": ["T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12"]},
                        "user_id": {"k": "str", "v": "U[1000-9999]"},
                        "client": {"k": "ch", "v": ["web", "desktop", "mobile"]},
                        "rtm_target": {"k": "ch", "v": ["rtm-1", "rtm-2", "rtm-3", "rtm-4"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "ws_upgrade_ok": {
                    "lvl": "INFO",
                    "msg": "WS upgrade ok team={team_id} user={user_id} rtm_target={rtm_target} handshake_ms={handshake_ms} trace={trace_id}",
                    "vars": {
                        "team_id": {"k": "ch", "v": ["T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12"]},
                        "user_id": {"k": "str", "v": "U[1000-9999]"},
                        "rtm_target": {"k": "ch", "v": ["rtm-1", "rtm-2", "rtm-3", "rtm-4"]},
                        "handshake_ms": {"k": "i", "v": [15, 900]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "ws_upgrade_fail_rtm": {
                    "lvl": "WARN",
                    "msg": "WS upgrade failed team={team_id} user={user_id} rtm_target={rtm_target} err={err} waited_ms={waited_ms} trace={trace_id}",
                    "vars": {
                        "team_id": {"k": "ch", "v": ["T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12"]},
                        "user_id": {"k": "str", "v": "U[1000-9999]"},
                        "rtm_target": {"k": "ch", "v": ["rtm-1", "rtm-2", "rtm-3", "rtm-4"]},
                        "err": {"k": "ch", "v": ["ECONNREFUSED", "ECONNRESET", "upstream_unreachable"]},
                        "waited_ms": {"k": "i", "v": [5, 1500]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "ws_upgrade_fail_auth": {
                    "lvl": "WARN",
                    "msg": "WS upgrade rejected team={team_id} user={user_id} err={err} waited_ms={waited_ms} trace={trace_id}",
                    "vars": {
                        "team_id": {"k": "ch", "v": ["T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12"]},
                        "user_id": {"k": "str", "v": "U[1000-9999]"},
                        "err": {"k": "ch", "v": ["auth_timeout", "auth_unavailable"]},
                        "waited_ms": {"k": "i", "v": [20, 3000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_200": {
                    "lvl": "INFO",
                    "msg": "HTTP {method} {route} team={team_id} status=200 dur_ms={dur_ms} upstream={upstream} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/api/conversations.history", "/api/auth.test"]},
                        "team_id": {"k": "ch", "v": ["T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12"]},
                        "dur_ms": {"k": "i", "v": [10, 3000]},
                        "upstream": {"k": "ch", "v": ["app_api"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_503": {
                    "lvl": "WARN",
                    "msg": "HTTP {method} {route} team={team_id} status={status} dur_ms={dur_ms} upstream={upstream} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/api/conversations.history", "/api/auth.test"]},
                        "team_id": {"k": "ch", "v": ["T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12"]},
                        "status": {"k": "ch", "v": ["503", "504"]},
                        "dur_ms": {"k": "i", "v": [200, 15000]},
                        "upstream": {"k": "ch", "v": ["app_api"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "app_api",
            "svc": "app-api",
            "hosts": ["api-1", "api-2", "api-3"],
            "logs": {
                "session_validate_ok": {
                    "lvl": "INFO",
                    "msg": "session validate ok team={team_id} user={user_id} db_ms={db_ms} trace={trace_id}",
                    "vars": {
                        "team_id": {"k": "ch", "v": ["T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12"]},
                        "user_id": {"k": "str", "v": "U[1000-9999]"},
                        "db_ms": {"k": "i", "v": [3, 800]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "session_validate_timeout": {
                    "lvl": "ERROR",
                    "msg": "session validate timeout team={team_id} user={user_id} timeout_ms={timeout_ms} db_err={db_err} trace={trace_id}",
                    "vars": {
                        "team_id": {"k": "ch", "v": ["T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12"]},
                        "user_id": {"k": "str", "v": "U[1000-9999]"},
                        "timeout_ms": {"k": "i", "v": [500, 8000]},
                        "db_err": {"k": "ch", "v": ["pool_exhausted", "query_timeout"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "history_ok": {
                    "lvl": "INFO",
                    "msg": "history served team={team_id} chan={chan_id} msgs={msgs} db_ms={db_ms} total_ms={total_ms} trace={trace_id}",
                    "vars": {
                        "team_id": {"k": "ch", "v": ["T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12"]},
                        "chan_id": {"k": "str", "v": "C[100-999]"},
                        "msgs": {"k": "i", "v": [20, 400]},
                        "db_ms": {"k": "i", "v": [5, 5000]},
                        "total_ms": {"k": "i", "v": [10, 12000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "history_timeout": {
                    "lvl": "ERROR",
                    "msg": "history query timeout team={team_id} chan={chan_id} timeout_ms={timeout_ms} db_err={db_err} trace={trace_id}",
                    "vars": {
                        "team_id": {"k": "ch", "v": ["T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12"]},
                        "chan_id": {"k": "str", "v": "C[100-999]"},
                        "timeout_ms": {"k": "i", "v": [800, 15000]},
                        "db_err": {"k": "ch", "v": ["pool_exhausted", "query_timeout"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "msg_write_ok": {
                    "lvl": "INFO",
                    "msg": "message stored team={team_id} chan={chan_id} bytes={bytes} db_ms={db_ms} trace={trace_id}",
                    "vars": {
                        "team_id": {"k": "ch", "v": ["T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12"]},
                        "chan_id": {"k": "str", "v": "C[100-999]"},
                        "bytes": {"k": "i", "v": [20, 4000]},
                        "db_ms": {"k": "i", "v": [4, 2500]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "threadpool_warn": {
                    "lvl": "WARN",
                    "msg": "worker pressure queue_depth={queue_depth} rejected={rejected} p95_ms={p95_ms}",
                    "vars": {
                        "queue_depth": {"k": "i", "v": [0, 400]},
                        "rejected": {"k": "i", "v": [0, 80]},
                        "p95_ms": {"k": "i", "v": [20, 15000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "threadpool_warn", "per_min": 0.1, "scope": "per_host"}]},
                "f": {"emit": [{"id": "threadpool_warn", "per_min": 0.2, "scope": "per_host"}]},
            },
        },
        {
            "id": "rtm_server",
            "svc": "rtm",
            "hosts": ["rtm-1", "rtm-2", "rtm-3", "rtm-4"],
            "logs": {
                "accept_conn": {
                    "lvl": "INFO",
                    "msg": "accepted ws conn team={team_id} user={user_id} conn={conn_id} trace={trace_id}",
                    "vars": {
                        "team_id": {"k": "ch", "v": ["T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12"]},
                        "user_id": {"k": "str", "v": "U[1000-9999]"},
                        "conn_id": {"k": "hex", "v": 16},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "msg_in": {
                    "lvl": "INFO",
                    "msg": "rtm message recv team={team_id} chan={chan_id} bytes={bytes} trace={trace_id}",
                    "vars": {
                        "team_id": {"k": "ch", "v": ["T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12"]},
                        "chan_id": {"k": "str", "v": "C[100-999]"},
                        "bytes": {"k": "i", "v": [20, 4000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "crash": {
                    "lvl": "ERROR",
                    "msg": "process crashed signal={signal} reason={reason}",
                    "vars": {"signal": {"k": "ch", "v": ["SIGSEGV", "SIGABRT"]}, "reason": {"k": "ch", "v": ["null_pointer", "assert_failed"]}},
                },
                "start": {
                    "lvl": "INFO",
                    "msg": "rtm server started build={build} listening={ip}:{port}",
                    "vars": {
                        "build": {"k": "ch", "v": ["rtm-2026.10.14.1", "rtm-2026.10.14.2"]},
                        "ip": {"k": "ip", "v": "10.0.0.0/24"},
                        "port": {"k": "i", "v": [7000, 7000]},
                    },
                },
                "rtm_stats": {
                    "lvl": "INFO",
                    "msg": "rtm stats conns={conns} p95_fanout_ms={p95_fanout_ms}",
                    "vars": {"conns": {"k": "i", "v": [200, 8000]}, "p95_fanout_ms": {"k": "i", "v": [5, 2000]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "rtm_stats", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "rtm_stats", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        {
            "id": "db_cluster",
            "svc": "db",
            "hosts": ["db-1", "db-2", "db-3"],
            "logs": {
                "pool_stats": {
                    "lvl": "INFO",
                    "msg": "db pool stats cluster={cluster} active_conns={active} waiting={waiting} max={max} p95_query_ms={p95_query_ms}",
                    "vars": {"cluster": {"k": "ch", "v": ["primary"]}, "max": {"k": "i", "v": [800, 2000]}},
                    "state_vars": {
                        "n": {
                            "active": {"k": "i", "v": [120, 500]},
                            "waiting": {"k": "i", "v": [0, 20]},
                            "p95_query_ms": {"k": "i", "v": [5, 80]},
                        },
                        "f": {
                            "active": {"k": "i", "v": [150, 900]},
                            "waiting": {"k": "i", "v": [0, 60]},
                            "p95_query_ms": {"k": "i", "v": [10, 250]},
                        },
                    },
                },
                "pool_stats_overload": {
                    "lvl": "WARN",
                    "msg": "db pool overload cluster={cluster} active_conns={active} waiting={waiting} max={max} p95_query_ms={p95_query_ms}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["primary"]},
                        "active": {"k": "i", "v": [900, 2000]},
                        "waiting": {"k": "i", "v": [80, 600]},
                        "max": {"k": "i", "v": [800, 2000]},
                        "p95_query_ms": {"k": "i", "v": [200, 5000]},
                    },
                },
                "capacity_added": {
                    "lvl": "INFO",
                    "msg": "db capacity updated cluster={cluster} added_nodes={added_nodes} new_max_conns={new_max_conns}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["primary"]},
                        "added_nodes": {"k": "i", "v": [1, 3]},
                        "new_max_conns": {"k": "i", "v": [1200, 2600]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "pool_stats", "per_min": 0.6, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "pool_stats", "per_min": 0.6, "scope": "per_host"},
                        {"id": "pool_stats_overload", "per_min": 0.3, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "monitor",
            "svc": "monitor",
            "hosts": ["mon-1"],
            "logs": {
                "healthcheck": {
                    "lvl": "INFO",
                    "msg": "synthetic check target={target} ok={ok} p95_ms={p95_ms}",
                    "vars": {
                        "target": {"k": "ch", "v": ["edge_gateway", "api_history", "rtm_ws"]},
                        "ok": {"k": "ch", "v": ["true", "false"]},
                        "p95_ms": {"k": "i", "v": [20, 8000]},
                    },
                },
                "alert_rtm_down": {
                    "lvl": "CRITICAL",
                    "msg": "ALERT name=rtm_connection_failures severity={sev} affected={affected} err_rate={err_rate}",
                    "vars": {"sev": {"k": "ch", "v": ["page", "critical"]}, "affected": {"k": "ch", "v": ["subset_teams", "many_users"]}, "err_rate": {"k": "f", "v": [0.01, 0.95]}},
                },
                "page_oncall": {
                    "lvl": "INFO",
                    "msg": "oncall paged service={service} reason={reason}",
                    "vars": {"service": {"k": "ch", "v": ["rtm", "app_api", "db"]}, "reason": {"k": "ch", "v": ["rtm_crash_spike", "api_5xx_spike", "db_pool_saturation"]}},
                },
                "status_update": {
                    "lvl": "INFO",
                    "msg": "status update state={state} note={note}",
                    "vars": {"state": {"k": "ch", "v": ["investigating", "identified", "mitigating"]}, "note": {"k": "ch", "v": ["realtime_disconnects", "db_overload", "partial_recovery"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "healthcheck", "per_min": 0.3, "scope": "global"}]},
                "f": {"emit": [{"id": "healthcheck", "per_min": 0.3, "scope": "global"}, {"id": "alert_rtm_down", "per_min": 0.2, "scope": "global"}]},
            },
        },
        {
            "id": "job_worker",
            "svc": "worker",
            "hosts": ["worker-1", "worker-2"],
            "logs": {
                "job_ok": {
                    "lvl": "INFO",
                    "msg": "processed job job={job} latency_ms={latency_ms}",
                    "vars": {"job": {"k": "ch", "v": ["unfurl", "index_update", "file_thumb"]}, "latency_ms": {"k": "i", "v": [20, 5000]}},
                }
            },
            "beh": {"n": {"emit": [{"id": "job_ok", "per_min": 3.0, "scope": "per_host"}]}, "f": {"emit": [{"id": "job_ok", "per_min": 3.0, "scope": "per_host"}]}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "ws_connect",
                    "rpm": 60.0,
                    "emit": ["edge_gateway.ws_upgrade_start", "app_api.session_validate_ok", "edge_gateway.ws_upgrade_ok", "rtm_server.accept_conn"],
                    "latency_ms": [[2, 8], [10, 60], [15, 120], [3, 25]],
                    "trace": True,
                },
                {
                    "id": "api_fetch_history",
                    "rpm": 120.0,
                    "emit": ["app_api.history_ok", "edge_gateway.http_200"],
                    "latency_ms": [[20, 250], [5, 30]],
                    "trace": True,
                },
                {
                    "id": "rtm_post_message",
                    "rpm": 100.0,
                    "emit": ["rtm_server.msg_in", "app_api.msg_write_ok"],
                    "latency_ms": [[2, 15], [10, 120]],
                    "trace": True,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "ws_connect_ok_f",
                    "rpm": 160.0,
                    "emit": ["edge_gateway.ws_upgrade_start", "app_api.session_validate_ok", "edge_gateway.ws_upgrade_ok", "rtm_server.accept_conn"],
                    "latency_ms": [[2, 10], [20, 300], [30, 900], [3, 40]],
                    "trace": True,
                },
                {
                    "id": "ws_connect_fail_rtm_f",
                    "rpm": 60.0,
                    "emit": ["edge_gateway.ws_upgrade_start", "app_api.session_validate_ok", "edge_gateway.ws_upgrade_fail_rtm"],
                    "latency_ms": [[2, 10], [20, 250], [30, 1500]],
                    "trace": True,
                },
                {
                    "id": "ws_connect_fail_db_f",
                    "rpm": 30.0,
                    "emit": ["edge_gateway.ws_upgrade_start", "app_api.session_validate_timeout", "edge_gateway.ws_upgrade_fail_auth"],
                    "latency_ms": [[2, 10], [300, 8000], [5, 50]],
                    "trace": True,
                },
                {
                    "id": "api_fetch_history_ok_f",
                    "rpm": 120.0,
                    "emit": ["app_api.history_ok", "edge_gateway.http_200"],
                    "latency_ms": [[50, 1500], [5, 60]],
                    "trace": True,
                },
                {
                    "id": "api_fetch_history_timeout_f",
                    "rpm": 20.0,
                    "emit": ["app_api.history_timeout", "edge_gateway.http_503"],
                    "latency_ms": [[800, 15000], [5, 80]],
                    "trace": True,
                },
                {
                    "id": "rtm_post_message_f",
                    "rpm": 60.0,
                    "emit": ["rtm_server.msg_in", "app_api.msg_write_ok"],
                    "latency_ms": [[2, 30], [20, 800]],
                    "trace": True,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "rtm_crash_reconnect_db_overload",
        "time": {"total_minutes": 40, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 40}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 20,
                        "rate_multipliers": {
                            "ws_connect_fail_db_f": 0.0,
                            "api_fetch_history_timeout_f": 0.0,
                            "db_cluster.pool_stats_overload": 0.0,
                            "monitor.alert_rtm_down": 8.0,
                        },
                        "latency_multipliers": {"ws_connect_ok_f": {"p50": 1.2, "p95": 1.5}},
                        "one_shots": [
                            {"ref": "rtm_server.crash", "count": 1, "hosts": ["rtm-3"]},
                            {"ref": "monitor.page_oncall", "count": 1, "hosts": ["mon-1"]},
                        ],
                    },
                    {
                        "order": 2,
                        "at_min": 27,
                        "rate_multipliers": {
                            "api_fetch_history_ok_f": 3.0,
                            "api_fetch_history_timeout_f": 4.0,
                            "ws_connect_fail_db_f": 1.0,
                            "db_cluster.pool_stats_overload": 3.0,
                            "app_api.threadpool_warn": 3.0,
                        },
                        "latency_multipliers": {
                            "api_fetch_history_ok_f": {"p50": 2.0, "p95": 3.0},
                            "ws_connect_ok_f": {"p50": 1.5, "p95": 2.5},
                        },
                        "one_shots": [],
                    },
                    {
                        "order": 3,
                        "at_min": 34,
                        "rate_multipliers": {
                            "api_fetch_history_ok_f": 2.0,
                            "api_fetch_history_timeout_f": 1.5,
                            "ws_connect_fail_rtm_f": 0.6,
                            "ws_connect_fail_db_f": 0.7,
                            "db_cluster.pool_stats_overload": 1.5,
                            "monitor.alert_rtm_down": 2.0,
                            "app_api.threadpool_warn": 1.5,
                        },
                        "latency_multipliers": {
                            "api_fetch_history_ok_f": {"p50": 1.4, "p95": 1.8},
                            "ws_connect_ok_f": {"p50": 1.2, "p95": 1.6},
                        },
                        "one_shots": [
                            {"ref": "db_cluster.capacity_added", "count": 1, "hosts": ["db-1"]},
                            {"ref": "rtm_server.start", "count": 1, "hosts": ["rtm-3"]},
                            {"ref": "monitor.status_update", "count": 1, "hosts": ["mon-1"]},
                        ],
                    },
                ]
            }
        },
    }
}

# ----------------------------
# Deterministic helpers
# ----------------------------

SEED = 1337
random.seed(SEED)  # required by verifier; simulator also uses hash-based determinism
NORM = NormalDist()


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def u01(key: str) -> float:
    h = hashlib.md5(f"{SEED}:{key}".encode("utf-8")).digest()
    x = int.from_bytes(h, "big")
    return x / float(2**128)


def choice_from_list(values: List[Any], key: str) -> Any:
    if not values:
        return ""
    idx = int(u01(key) * len(values))
    if idx >= len(values):
        idx = len(values) - 1
    return values[idx]


def parse_str_range(spec: str) -> Tuple[str, int, int]:
    lb = spec.find("[")
    rb = spec.find("]")
    if lb == -1 or rb == -1 or rb < lb:
        return spec, 0, 0
    prefix = spec[:lb]
    inner = spec[lb + 1 : rb]
    if "-" not in inner:
        return prefix, 0, 0
    a, b = inner.split("-", 1)
    return prefix, int(a), int(b)


def int_in_range(a: int, b: int, key: str, bias: float = 0.0) -> int:
    if b < a:
        a, b = b, a
    u = u01(key)
    if bias != 0.0:
        u = min(1.0, max(0.0, u + bias))
    return a + int(u * ((b - a) + 1))


def float_in_range(a: float, b: float, key: str) -> float:
    u = u01(key)
    return a + (b - a) * u


def deterministic_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    if u01(f"round:{key}") < frac:
        return base + 1
    return base


def isoformat_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    ms = dt.microsecond // 1000
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


def schedule_even_times(count: int, start: datetime, end: datetime, key_prefix: str) -> List[datetime]:
    if count <= 0:
        return []
    dur_s = (end - start).total_seconds()
    if dur_s <= 0:
        return []
    spacing = dur_s / count
    jitter_amp = min(0.6, spacing * 0.10)
    out: List[datetime] = []
    for i in range(count):
        frac = (i + 0.5) / count
        jitter = (u01(f"{key_prefix}:jit:{i}") - 0.5) * 2.0 * jitter_amp
        t = start + timedelta(seconds=dur_s * frac + jitter)
        if t < start:
            t = start
        if t >= end:
            t = end - timedelta(milliseconds=1)
        out.append(t)
    return out


def sample_lognormal_ms(p50: float, p95: float, key: str, cap_ms: Optional[int] = None, min_ms: int = 1) -> int:
    if p50 <= 0:
        return 0
    if p95 <= 0:
        p95 = p50
    if p95 < p50:
        p95 = p50

    mu = math.log(p50)
    z95 = NORM.inv_cdf(0.95)
    sigma = 0.0
    if p95 > p50:
        sigma = (math.log(p95) - math.log(p50)) / z95
        sigma = max(0.01, sigma)
    u = 0.35 + 0.55 * u01(f"ln:{key}")
    z = NORM.inv_cdf(u)
    val = math.exp(mu + sigma * z)
    ms = int(round(val))
    if ms < min_ms:
        ms = min_ms
    if cap_ms is not None and ms > cap_ms:
        ms = cap_ms
    return ms


# ----------------------------
# Indices
# ----------------------------

COMP: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

TEMPL: Dict[str, Dict[str, Dict[str, Any]]] = {}
for c in SYSTEM["components"]:
    TEMPL[c["id"]] = c.get("logs", {})


def parse_ref(ref: str) -> Tuple[str, str]:
    if "." not in ref:
        raise ValueError(f"Bad ref: {ref}")
    a, b = ref.split(".", 1)
    return a, b


# ----------------------------
# Scenario control intervals
# ----------------------------

@dataclass(frozen=True)
class FailureSegment:
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    lat_mult: Dict[str, Dict[str, float]]


def build_failure_segments() -> Tuple[List[FailureSegment], List[Dict[str, Any]]]:
    time_cfg = SCENARIO["scenario"]["time"]["phases"]
    f_start = time_cfg["f"]["start_min"]
    f_end = time_cfg["f"]["end_min"]
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = sorted(set([f_start] + [e["at_min"] for e in events] + [f_end]))
    boundaries = [b for b in boundaries if f_start <= b <= f_end]
    boundaries.sort()

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}

    segs: List[FailureSegment] = []
    by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        by_min.setdefault(e["at_min"], []).append(e)

    for i in range(len(boundaries) - 1):
        s = boundaries[i]
        e = boundaries[i + 1]
        for ev in by_min.get(s, []):
            for k, v in ev.get("rate_multipliers", {}).items():
                active_rate[k] = float(v)
            for fk, mv in ev.get("latency_multipliers", {}).items():
                active_lat[fk] = {"p50": float(mv["p50"]), "p95": float(mv["p95"])}
        segs.append(FailureSegment(start_min=s, end_min=e, rate_mult=dict(active_rate), lat_mult=dict(active_lat)))
    return segs, events


FAILURE_SEGS, FAILURE_EVENTS = build_failure_segments()

# ----------------------------
# Log rendering
# ----------------------------

def merged_var_specs(template: Dict[str, Any], state: str) -> Dict[str, Dict[str, Any]]:
    specs = dict(template.get("vars", {}) or {})
    st = template.get("state_vars", {})
    if isinstance(st, dict) and state in st and isinstance(st[state], dict):
        for k, v in st[state].items():
            specs[k] = v
    return specs


def generate_value(spec: Dict[str, Any], key: str) -> Any:
    k = spec.get("k")
    v = spec.get("v")
    if k == "ch":
        return choice_from_list(list(v), key)
    if k == "i":
        a, b = int(v[0]), int(v[1])
        return int_in_range(a, b, key)
    if k == "f":
        a, b = float(v[0]), float(v[1])
        return round(float_in_range(a, b, key), 3)
    if k == "hex":
        ln = int(v)
        return md5_hex(f"hex:{key}")[:ln]
    if k == "ip":
        host = int_in_range(2, 254, key)
        return f"10.0.0.{host}"
    if k == "str":
        prefix, a, b = parse_str_range(str(v))
        if a == 0 and b == 0:
            return prefix
        num = int_in_range(a, b, key)
        return f"{prefix}{num}"
    return str(v)


def render(comp_id: str, log_id: str, state: str, bound: Dict[str, Any], row_key: str) -> Tuple[str, str]:
    tmpl = TEMPL[comp_id][log_id]
    specs = merged_var_specs(tmpl, state)
    vals: Dict[str, Any] = {}
    for var, spec in specs.items():
        if var in bound:
            vals[var] = bound[var]
        else:
            vals[var] = generate_value(spec, f"{row_key}:var:{var}")
    for k, v in bound.items():
        if k not in vals:
            vals[k] = v
    msg = tmpl["msg"].format(**vals)
    return tmpl["lvl"], msg


def trace_id_for(flow_id: str, inst_key: str) -> str:
    return md5_hex(f"trace:{flow_id}:{inst_key}")[:32]


def conn_id_for(inst_key: str) -> str:
    return md5_hex(f"conn:{inst_key}")[:16]


def host_for_component(comp_id: str, inst_key: str) -> str:
    hosts = COMP[comp_id].get("hosts", []) or []
    if not hosts:
        return ""
    return choice_from_list(hosts, f"{inst_key}:host:{comp_id}")


# ----------------------------
# Flow simulation
# ----------------------------

def flow_latency_multiplier(state: str, flow_id: str, seg: Optional[FailureSegment]) -> Tuple[float, float]:
    if state != "f" or seg is None:
        return 1.0, 1.0
    m = seg.lat_mult.get(flow_id)
    if not m:
        return 1.0, 1.0
    return float(m.get("p50", 1.0)), float(m.get("p95", 1.0))


def flow_rate_multiplier(state: str, flow_id: str, seg: Optional[FailureSegment]) -> float:
    if state != "f" or seg is None:
        return 1.0
    return float(seg.rate_mult.get(flow_id, 1.0))


def background_rate_multiplier(state: str, comp_id: str, log_id: str, seg: Optional[FailureSegment]) -> float:
    if state != "f" or seg is None:
        return 1.0
    return float(seg.rate_mult.get(f"{comp_id}.{log_id}", 1.0))


def pick_team_user(inst_key: str) -> Tuple[str, str]:
    team = choice_from_list(
        ["T01", "T02", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10", "T11", "T12"],
        f"{inst_key}:team",
    )
    user = f"U{int_in_range(1000, 9999, f'{inst_key}:user')}"
    return team, user


def pick_channel(inst_key: str) -> str:
    return f"C{int_in_range(100, 999, f'{inst_key}:chan')}"


def clamp_int(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, x))


def simulate_flow_instance(
    rows: List[Dict[str, Any]],
    state: str,
    flow: Dict[str, Any],
    start_ts: datetime,
    seg: Optional[FailureSegment],
    inst_idx: int,
    inst_key_prefix: str,
):
    flow_id = flow["id"]
    inst_key = f"{inst_key_prefix}:{flow_id}:{inst_idx}"
    tid = trace_id_for(flow_id, inst_key) if flow.get("trace", False) and SYSTEM["tracing"]["on"] else ""

    team_id, user_id = pick_team_user(inst_key)
    chan_id = pick_channel(inst_key)
    client = choice_from_list(["web", "desktop", "mobile"], f"{inst_key}:client")

    start_min = int((start_ts - BASE_TIME).total_seconds() // 60)
    if flow_id == "ws_connect_fail_rtm_f":
        rtm_target = "rtm-3"
    elif flow_id in ("ws_connect_ok_f", "ws_connect"):
        if state == "f" and start_min < 34:
            rtm_target = choice_from_list(["rtm-1", "rtm-2", "rtm-4"], f"{inst_key}:rtm_target")
        else:
            rtm_target = choice_from_list(["rtm-1", "rtm-2", "rtm-3", "rtm-4"], f"{inst_key}:rtm_target")
    else:
        rtm_target = choice_from_list(["rtm-1", "rtm-2", "rtm-3", "rtm-4"], f"{inst_key}:rtm_target")

    edge_host = host_for_component("edge_gateway", inst_key)
    api_host = host_for_component("app_api", inst_key)
    rtm_host = rtm_target if rtm_target in COMP["rtm_server"]["hosts"] else host_for_component("rtm_server", inst_key)

    m50, m95 = flow_latency_multiplier(state, flow_id, seg)
    emit_refs = flow["emit"]
    lat_hints = flow["latency_ms"]

    sampled_lat: List[int] = []
    for j, (p50, p95) in enumerate(lat_hints):
        sp50 = float(p50) * m50
        sp95 = float(p95) * m95
        cap = None

        if flow_id in ("ws_connect", "ws_connect_ok_f") and j == 2:
            cap = 900
        if flow_id == "api_fetch_history_ok_f":
            cap = 2950
        if flow_id == "api_fetch_history":
            cap = 2950
        if flow_id == "ws_connect_fail_db_f" and j == 1:
            cap = 2950

        ms = sample_lognormal_ms(sp50, sp95, f"{inst_key}:lat:{j}", cap_ms=cap, min_ms=1)
        sampled_lat.append(ms)

    if flow_id in ("api_fetch_history", "api_fetch_history_ok_f"):
        if len(sampled_lat) >= 2:
            lat1 = sampled_lat[1]
            max_lat0 = max(10, 3000 - lat1)
            sampled_lat[0] = min(sampled_lat[0], max_lat0)

    t = start_ts
    emit_ts: List[datetime] = []
    for ms in sampled_lat:
        t = t + timedelta(milliseconds=ms)
        emit_ts.append(t)

    handshake_ms = None
    if flow_id in ("ws_connect", "ws_connect_ok_f"):
        handshake_ms = clamp_int(sampled_lat[2], 15, 900)

    fail_rtm_waited_ms = None
    if flow_id == "ws_connect_fail_rtm_f":
        fail_rtm_waited_ms = clamp_int(sampled_lat[2], 5, 1500)

    auth_timeout_ms = None
    auth_waited_ms = None
    if flow_id == "ws_connect_fail_db_f":
        auth_timeout_ms = clamp_int(sampled_lat[1], 500, 8000)
        auth_waited_ms = clamp_int(sampled_lat[1] + sampled_lat[2], 20, 3000)

    http_dur_ms = None
    api_total_ms = None
    if flow_id in ("api_fetch_history", "api_fetch_history_ok_f"):
        api_total_ms = clamp_int(sampled_lat[0], 10, 12000)
        http_dur_ms = clamp_int(sampled_lat[0] + sampled_lat[1], 10, 3000)

    hist_timeout_ms = None
    http_5xx_dur_ms = None
    if flow_id == "api_fetch_history_timeout_f":
        hist_timeout_ms = clamp_int(sampled_lat[0], 800, 15000)
        http_5xx_dur_ms = clamp_int(sampled_lat[0] + sampled_lat[1], 200, 15000)

    msg_bytes = int_in_range(20, 4000, f"{inst_key}:bytes")
    validate_db_ms = None
    write_db_ms = None
    if flow_id in ("ws_connect", "ws_connect_ok_f", "ws_connect_fail_rtm_f"):
        validate_db_ms = clamp_int(int(round(sampled_lat[1] * 0.75)), 3, 800)
    if flow_id in ("rtm_post_message", "rtm_post_message_f"):
        write_db_ms = clamp_int(int(round(sampled_lat[1] * 0.70)), 4, 2500)

    hist_db_ms = None
    hist_msgs = None
    if flow_id in ("api_fetch_history", "api_fetch_history_ok_f"):
        hist_db_ms = clamp_int(int(round(sampled_lat[0] * 0.80)), 5, 5000)
        hist_msgs = int_in_range(20, 400, f"{inst_key}:msgs")

    db_err = "pool_exhausted" if u01(f"{inst_key}:db_err_bias") < 0.8 else "query_timeout"
    rtm_err = "ECONNREFUSED" if u01(f"{inst_key}:rtm_err") < 0.7 else choice_from_list(["ECONNRESET", "upstream_unreachable"], f"{inst_key}:rtm_err2")
    auth_err = "auth_timeout" if u01(f"{inst_key}:auth_err") < 0.8 else "auth_unavailable"

    for j, ref in enumerate(emit_refs):
        comp_id, log_id = parse_ref(ref)
        ts = emit_ts[j]
        bound: Dict[str, Any] = {"trace_id": tid}

        if "team_id" in merged_var_specs(TEMPL[comp_id][log_id], state):
            bound["team_id"] = team_id
        if "user_id" in merged_var_specs(TEMPL[comp_id][log_id], state):
            bound["user_id"] = user_id
        if "chan_id" in merged_var_specs(TEMPL[comp_id][log_id], state):
            bound["chan_id"] = chan_id

        if comp_id == "edge_gateway":
            if log_id == "ws_upgrade_start":
                bound["client"] = client
                bound["rtm_target"] = rtm_target
            elif log_id == "ws_upgrade_ok":
                bound["rtm_target"] = rtm_target
                bound["handshake_ms"] = handshake_ms
            elif log_id == "ws_upgrade_fail_rtm":
                bound["rtm_target"] = rtm_target
                bound["err"] = rtm_err
                bound["waited_ms"] = fail_rtm_waited_ms
            elif log_id == "ws_upgrade_fail_auth":
                bound["err"] = auth_err
                bound["waited_ms"] = auth_waited_ms
            elif log_id == "http_200":
                bound["method"] = "GET"
                bound["route"] = "/api/conversations.history"
                bound["upstream"] = "app_api"
                bound["dur_ms"] = http_dur_ms
                bound["team_id"] = team_id
            elif log_id == "http_503":
                bound["method"] = "GET"
                bound["route"] = "/api/conversations.history"
                bound["upstream"] = "app_api"
                bound["dur_ms"] = http_5xx_dur_ms
                bound["status"] = "504" if u01(f"{inst_key}:status504") < 0.7 else "503"
                bound["team_id"] = team_id

        if comp_id == "app_api":
            if log_id == "session_validate_ok":
                bound["db_ms"] = validate_db_ms
            elif log_id == "session_validate_timeout":
                bound["timeout_ms"] = auth_timeout_ms
                bound["db_err"] = db_err
            elif log_id == "history_ok":
                bound["chan_id"] = chan_id
                bound["msgs"] = hist_msgs
                bound["db_ms"] = hist_db_ms
                bound["total_ms"] = api_total_ms
            elif log_id == "history_timeout":
                bound["chan_id"] = chan_id
                bound["timeout_ms"] = hist_timeout_ms
                bound["db_err"] = db_err
            elif log_id == "msg_write_ok":
                bound["chan_id"] = chan_id
                bound["bytes"] = msg_bytes
                bound["db_ms"] = write_db_ms

        if comp_id == "rtm_server":
            if log_id == "accept_conn":
                bound["conn_id"] = conn_id_for(inst_key)
            elif log_id == "msg_in":
                bound["chan_id"] = chan_id
                bound["bytes"] = msg_bytes

        lvl, msg = render(comp_id, log_id, state, bound, f"{inst_key}:emit:{j}")
        service = COMP[comp_id].get("svc", "") or ""
        if comp_id == "edge_gateway":
            host = edge_host
        elif comp_id == "app_api":
            host = api_host
        elif comp_id == "rtm_server":
            host = rtm_host
        else:
            host = host_for_component(comp_id, inst_key)
        rows.append({"ts": ts, "level": lvl, "message": msg, "trace_id": tid, "service": service, "host": host})


# ----------------------------
# Background simulation
# ----------------------------

def emit_background(
    rows: List[Dict[str, Any]],
    state: str,
    start_dt: datetime,
    end_dt: datetime,
    seg: Optional[FailureSegment],
    key_prefix: str,
):
    for comp_id, comp in COMP.items():
        beh = comp.get("beh", {}).get(state, {})
        for src in beh.get("emit", []) or []:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host") or "per_host"

            mult = background_rate_multiplier(state, comp_id, log_id, seg)
            eff_rate = per_min * mult
            duration_min = (end_dt - start_dt).total_seconds() / 60.0
            if duration_min <= 0:
                continue

            if scope == "global":
                expected = eff_rate * duration_min
                count = deterministic_round(expected, f"{key_prefix}:bg:{comp_id}.{log_id}")
                times = schedule_even_times(count, start_dt, end_dt, f"{key_prefix}:bg:{comp_id}.{log_id}")
                for i, ts in enumerate(times):
                    host = choice_from_list(comp.get("hosts", []) or [""], f"{key_prefix}:bg:{comp_id}.{log_id}:host:{i}")
                    bound: Dict[str, Any] = {}
                    if comp_id == "monitor" and log_id == "healthcheck":
                        target = choice_from_list(["edge_gateway", "api_history", "rtm_ws"], f"{key_prefix}:hc:target:{i}")
                        minute = int((ts - BASE_TIME).total_seconds() // 60)
                        ok = "true"
                        if state == "f":
                            if target == "rtm_ws" and 20 <= minute < 34:
                                ok = "false"
                            elif target == "api_history" and minute >= 27 and u01(f"{key_prefix}:hc:fail:{i}") < 0.35:
                                ok = "false"
                        bound["target"] = target
                        bound["ok"] = ok
                        if ok == "false":
                            bound["p95_ms"] = int_in_range(800, 8000, f"{key_prefix}:hc:p95bad:{i}", bias=0.15)
                        else:
                            bound["p95_ms"] = int_in_range(20, 1500 if state == "n" else 3000, f"{key_prefix}:hc:p95ok:{i}", bias=-0.05)

                    if comp_id == "monitor" and log_id == "alert_rtm_down":
                        minute = int((ts - BASE_TIME).total_seconds() // 60)
                        bound["sev"] = "critical" if u01(f"{key_prefix}:alert:sev:{i}") < 0.7 else "page"
                        bound["affected"] = "many_users" if minute < 34 else "subset_teams"
                        if minute < 34:
                            bound["err_rate"] = round(float_in_range(0.18, 0.85, f"{key_prefix}:alert:rate:{i}"), 3)
                        else:
                            bound["err_rate"] = round(float_in_range(0.05, 0.35, f"{key_prefix}:alert:rate:{i}"), 3)

                    lvl, msg = render(comp_id, log_id, state, bound, f"{key_prefix}:bg:{comp_id}.{log_id}:{i}")
                    rows.append({"ts": ts, "level": lvl, "message": msg, "trace_id": "", "service": comp.get("svc", "") or "", "host": host})

            else:
                for h in comp.get("hosts", []) or [""]:
                    expected = eff_rate * duration_min
                    count = deterministic_round(expected, f"{key_prefix}:bg:{comp_id}.{log_id}:{h}")
                    times = schedule_even_times(count, start_dt, end_dt, f"{key_prefix}:bg:{comp_id}.{log_id}:{h}")
                    for i, ts in enumerate(times):
                        bound = {}
                        if comp_id == "db_cluster" and log_id == "pool_stats":
                            minute = int((ts - BASE_TIME).total_seconds() // 60)
                            stress = 0.0
                            if state == "f":
                                ov_mult = float(seg.rate_mult.get("db_cluster.pool_stats_overload", 1.0)) if seg else 1.0
                                if minute < 27:
                                    stress = 0.15
                                else:
                                    stress = 0.35 if ov_mult <= 1.0 else 0.70
                                if minute >= 34:
                                    stress *= 0.7
                            specs = merged_var_specs(TEMPL[comp_id][log_id], state)
                            a1, b1 = specs["active"]["v"]
                            a2, b2 = specs["waiting"]["v"]
                            a3, b3 = specs["p95_query_ms"]["v"]
                            bound["active"] = int_in_range(int(a1), int(b1), f"{key_prefix}:db:active:{h}:{i}", bias=stress * 0.15)
                            bound["waiting"] = int_in_range(int(a2), int(b2), f"{key_prefix}:db:waiting:{h}:{i}", bias=stress * 0.25)
                            bound["p95_query_ms"] = int_in_range(int(a3), int(b3), f"{key_prefix}:db:p95:{h}:{i}", bias=stress * 0.35)

                        if comp_id == "app_api" and log_id == "threadpool_warn":
                            minute = int((ts - BASE_TIME).total_seconds() // 60)
                            pressure = 0.05
                            if state == "f":
                                pressure = 0.25 if minute < 27 else 0.70
                                if minute >= 34:
                                    pressure *= 0.65
                            bound["queue_depth"] = int_in_range(0, 400, f"{key_prefix}:api:qd:{h}:{i}", bias=pressure * 0.20)
                            bound["rejected"] = int_in_range(0, 80, f"{key_prefix}:api:rej:{h}:{i}", bias=pressure * 0.25)
                            bound["p95_ms"] = int_in_range(20, 15000, f"{key_prefix}:api:p95:{h}:{i}", bias=pressure * 0.30)

                        if comp_id == "rtm_server" and log_id == "rtm_stats":
                            minute = int((ts - BASE_TIME).total_seconds() // 60)
                            conns = int_in_range(200, 8000, f"{key_prefix}:rtm:conns:{h}:{i}", bias=0.10)
                            if state == "f" and 20 <= minute < 34 and h == "rtm-3":
                                conns = int_in_range(200, 1200, f"{key_prefix}:rtm:conns_low:{h}:{i}", bias=-0.10)
                            bound["conns"] = conns
                            bound["p95_fanout_ms"] = int_in_range(5, 2000, f"{key_prefix}:rtm:p95fan:{h}:{i}", bias=0.05 if state == "n" else 0.15)

                        lvl, msg = render(comp_id, log_id, state, bound, f"{key_prefix}:bg:{comp_id}.{log_id}:{h}:{i}")
                        rows.append({"ts": ts, "level": lvl, "message": msg, "trace_id": "", "service": comp.get("svc", "") or "", "host": h})


# ----------------------------
# One-shots
# ----------------------------

def emit_one_shots(rows: List[Dict[str, Any]], events: List[Dict[str, Any]]):
    for ev in events:
        at_min = int(ev["at_min"])
        ev_key = f"oneshot:{at_min}:{ev.get('order',0)}"
        base_ts = BASE_TIME + timedelta(minutes=at_min)
        for idx, ospec in enumerate(ev.get("one_shots", []) or []):
            ref = ospec["ref"]
            comp_id, log_id = parse_ref(ref)
            count = int(ospec["count"])
            hosts = ospec.get("hosts", []) or []
            for k in range(count):
                jitter_s = float_in_range(0.1, 8.0, f"{ev_key}:{ref}:{idx}:{k}:jit")
                ts = base_ts + timedelta(seconds=jitter_s)
                host = hosts[k % len(hosts)] if hosts else host_for_component(comp_id, f"{ev_key}:{ref}:{k}")
                bound: Dict[str, Any] = {}
                if comp_id == "monitor" and log_id == "page_oncall":
                    bound["service"] = "rtm"
                    bound["reason"] = "rtm_crash_spike"
                if comp_id == "monitor" and log_id == "status_update":
                    bound["state"] = "mitigating"
                    bound["note"] = "partial_recovery"
                if comp_id == "db_cluster" and log_id == "capacity_added":
                    bound["cluster"] = "primary"
                    bound["added_nodes"] = 2
                    bound["new_max_conns"] = 2200
                if comp_id == "rtm_server" and log_id == "start":
                    bound["build"] = choice_from_list(["rtm-2026.10.14.1", "rtm-2026.10.14.2"], f"{ev_key}:build")
                    bound["ip"] = generate_value({"k": "ip", "v": "10.0.0.0/24"}, f"{ev_key}:ip")
                    bound["port"] = 7000
                if comp_id == "rtm_server" and log_id == "crash":
                    bound["signal"] = "SIGSEGV"
                    bound["reason"] = "null_pointer"
                lvl, msg = render(comp_id, log_id, "f", bound, f"{ev_key}:{ref}:{idx}:{k}")
                rows.append({"ts": ts, "level": lvl, "message": msg, "trace_id": "", "service": COMP[comp_id].get("svc", "") or "", "host": host})


# ----------------------------
# Main planning/execution
# ----------------------------

BASE_TIME = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)


def simulate():
    rows: List[Dict[str, Any]] = []
    phase_cfg = SCENARIO["scenario"]["time"]["phases"]
    n_start, n_end = phase_cfg["n"]["start_min"], phase_cfg["n"]["end_min"]

    n_start_dt = BASE_TIME + timedelta(minutes=n_start)
    n_end_dt = BASE_TIME + timedelta(minutes=n_end)
    emit_background(rows, "n", n_start_dt, n_end_dt, None, "normal")

    for flow in SYSTEM["flows"]["n"]["req"]:
        flow_id = flow["id"]
        duration_min = (n_end_dt - n_start_dt).total_seconds() / 60.0
        expected = float(flow["rpm"]) * duration_min
        count = deterministic_round(expected, f"flow:n:{flow_id}:0-{n_end}")
        starts = schedule_even_times(count, n_start_dt, n_end_dt, f"flow:n:{flow_id}")
        for i, st in enumerate(starts):
            simulate_flow_instance(rows, "n", flow, st, None, i, "n")

    emit_one_shots(rows, FAILURE_EVENTS)

    for seg in FAILURE_SEGS:
        seg_start_dt = BASE_TIME + timedelta(minutes=seg.start_min)
        seg_end_dt = BASE_TIME + timedelta(minutes=seg.end_min)
        emit_background(rows, "f", seg_start_dt, seg_end_dt, seg, f"fail:bg:{seg.start_min}-{seg.end_min}")

        for flow in SYSTEM["flows"]["f"]["req"]:
            flow_id = flow["id"]
            duration_min = (seg_end_dt - seg_start_dt).total_seconds() / 60.0
            mult = flow_rate_multiplier("f", flow_id, seg)
            expected = float(flow["rpm"]) * mult * duration_min
            count = deterministic_round(expected, f"flow:f:{flow_id}:{seg.start_min}-{seg.end_min}")
            starts = schedule_even_times(count, seg_start_dt, seg_end_dt, f"flow:f:{flow_id}:{seg.start_min}-{seg.end_min}")
            for i, st in enumerate(starts):
                simulate_flow_instance(rows, "f", flow, st, seg, i, f"f:{seg.start_min}-{seg.end_min}")

    df = pd.DataFrame(rows)
    df.sort_values("ts", inplace=True, kind="mergesort")
    df["timestamp"] = df["ts"].map(isoformat_ms)
    df.drop(columns=["ts"], inplace=True)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    n_rows = len(df)
    if not (20000 <= n_rows <= 100000):
        raise RuntimeError(f"Row count {n_rows} outside target [20000, 100000]")

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    simulate()
