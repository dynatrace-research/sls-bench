import hashlib
import ipaddress
import math
import random
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Deterministic seeding (required by verifier, even though this simulator uses stable hashing)
SEED = 1337
random.seed(SEED)
np.random.seed(SEED)

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "control_plane_api_dashboard"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["api_gateway", "dashboard_web"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "rack_switch",
            "svc": None,
            "hosts": ["sw-r12a"],
            "logs": {
                "switch_ping_loss": {
                    "lvl": "WARN",
                    "msg": "switch {switch} ping loss {loss_pct}% over 60s",
                    "vars": {"switch": {"k": "ch", "v": ["sw-r12a"]}, "loss_pct": {"k": "i", "v": [5, 60]}},
                },
                "switch_recovered": {
                    "lvl": "INFO",
                    "msg": "switch {switch} forwarding stable (loss back to {loss_pct}%)",
                    "vars": {"switch": {"k": "ch", "v": ["sw-r12a"]}, "loss_pct": {"k": "i", "v": [0, 1]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "switch_ping_loss", "per_min": 0.02, "scope": "global"}]},
                "f": {"emit": [{"id": "switch_ping_loss", "per_min": 4.0, "scope": "global"}]},
            },
        },
        {
            "id": "etcd_cluster",
            "svc": "etcd",
            "hosts": ["etcd-1", "etcd-2", "etcd-3"],
            "logs": {
                "raft_election_started": {
                    "lvl": "WARN",
                    "msg": "raft election started member={member} term={term} reason={reason}",
                    "vars": {
                        "member": {"k": "ch", "v": ["etcd-1", "etcd-2", "etcd-3"]},
                        "term": {"k": "i", "v": [1200, 1400]},
                        "reason": {"k": "ch", "v": ["election_timeout", "heartbeat_missed"]},
                    },
                },
                "raft_write_blocked": {
                    "lvl": "ERROR",
                    "msg": "write blocked (no leader) member={member} wait_ms={wait_ms}",
                    "vars": {"member": {"k": "ch", "v": ["etcd-1", "etcd-2", "etcd-3"]}, "wait_ms": {"k": "i", "v": [200, 5000]}},
                },
                "raft_leader_heartbeat": {
                    "lvl": "INFO",
                    "msg": "raft leader heartbeat leader={leader} term={term}",
                    "vars": {"leader": {"k": "ch", "v": ["etcd-1", "etcd-2", "etcd-3"]}, "term": {"k": "i", "v": [1200, 1400]}},
                },
                "raft_leader_stable": {
                    "lvl": "INFO",
                    "msg": "raft leader stabilized leader={leader} term={term}",
                    "vars": {"leader": {"k": "ch", "v": ["etcd-1", "etcd-2", "etcd-3"]}, "term": {"k": "i", "v": [1200, 1400]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "raft_leader_heartbeat", "per_min": 1.0, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "raft_leader_heartbeat", "per_min": 1.0, "scope": "global"},
                        {"id": "raft_election_started", "per_min": 5.0, "scope": "global"},
                        {"id": "raft_write_blocked", "per_min": 4.0, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "db_cluster_mgr",
            "svc": "db-cluster-mgr",
            "hosts": ["dbmgr-a1", "dbmgr-a2"],
            "logs": {
                "promote_primary": {
                    "lvl": "INFO",
                    "msg": "promoted primary cluster={cluster} new_primary={new_primary} reason={reason}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["auth-db"]},
                        "new_primary": {"k": "ch", "v": ["authdb-a1", "authdb-a2"]},
                        "reason": {"k": "ch", "v": ["coordination_unavailable", "primary_not_confirmed"]},
                    },
                },
                "replica_rebuild_started": {
                    "lvl": "WARN",
                    "msg": "replica rebuild started cluster={cluster} primary={primary} estimated_minutes={eta_min}",
                    "vars": {"cluster": {"k": "ch", "v": ["auth-db"]}, "primary": {"k": "ch", "v": ["authdb-a1", "authdb-a2"]}, "eta_min": {"k": "i", "v": [120, 420]}},
                },
                "replica_rebuild_progress": {
                    "lvl": "INFO",
                    "msg": "replica rebuild progress cluster={cluster} pct={pct} bytes_copied_gb={gb}",
                    "vars": {"cluster": {"k": "ch", "v": ["auth-db"]}, "pct": {"k": "i", "v": [0, 99]}, "gb": {"k": "i", "v": [0, 600]}},
                },
                "read_routing_enabled_all": {"lvl": "INFO", "msg": "read routing enabled scope=all_traffic target=dr_replica", "vars": {}},
                "read_routing_enabled_api_only": {"lvl": "INFO", "msg": "read routing enabled scope=api_only target=dr_replica", "vars": {}},
                "noncritical_work_throttled": {
                    "lvl": "WARN",
                    "msg": "noncritical work throttled features={features} new_limit_rps={limit_rps}",
                    "vars": {"features": {"k": "ch", "v": ["cert_push", "email", "analytics"]}, "limit_rps": {"k": "i", "v": [5, 50]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": [{"id": "replica_rebuild_progress", "per_min": 1.0, "scope": "global"}]}},
        },
        {
            "id": "auth_db_primary",
            "svc": "auth-db",
            "hosts": ["authdb-a1", "authdb-a2"],
            "logs": {
                "db_health": {
                    "lvl": "INFO",
                    "msg": "db health node={node} role=primary cpu_pct={cpu_pct} connections={connections} qps={qps}",
                    "vars": {"cpu_pct": {"k": "i", "v": [10, 95]}, "connections": {"k": "i", "v": [50, 800]}, "qps": {"k": "i", "v": [200, 6000]}},
                    "state_vars": {"n": {"node": {"k": "ch", "v": ["authdb-a1"]}}, "f": {"node": {"k": "ch", "v": ["authdb-a1", "authdb-a2"]}}},
                },
                "db_overload": {
                    "lvl": "ERROR",
                    "msg": "db overload node={node} role=primary cpu_pct={cpu_pct} connections={connections} queue_ms={queue_ms}",
                    "vars": {"node": {"k": "ch", "v": ["authdb-a1", "authdb-a2"]}, "cpu_pct": {"k": "i", "v": [85, 99]}, "connections": {"k": "i", "v": [600, 1500]}, "queue_ms": {"k": "i", "v": [50, 4000]}},
                },
            },
            "beh": {"n": {"emit": [{"id": "db_health", "per_min": 1.0, "scope": "global"}]}, "f": {"emit": [{"id": "db_health", "per_min": 1.0, "scope": "global"}, {"id": "db_overload", "per_min": 2.0, "scope": "global"}]}},
        },
        {
            "id": "auth_db_dr_replica",
            "svc": "auth-db",
            "hosts": ["authdb-b1"],
            "logs": {
                "replica_health": {
                    "lvl": "INFO",
                    "msg": "db health role=dr_replica cpu_pct={cpu_pct} qps={qps} repl_lag_s={lag_s}",
                    "vars": {"cpu_pct": {"k": "i", "v": [5, 70]}, "qps": {"k": "i", "v": [100, 5000]}, "lag_s": {"k": "i", "v": [0, 60]}},
                }
            },
            "beh": {"n": {"emit": [{"id": "replica_health", "per_min": 0.5}]}, "f": {"emit": [{"id": "replica_health", "per_min": 0.5}]}},
        },
        {
            "id": "redis_cache",
            "svc": "redis",
            "hosts": ["redis-a1", "redis-a2", "redis-a3"],
            "logs": {"redis_latency": {"lvl": "INFO", "msg": "redis p95_ms={p95_ms} ops_s={ops_s} node={node}", "vars": {"p95_ms": {"k": "i", "v": [1, 40]}, "ops_s": {"k": "i", "v": [500, 6000]}, "node": {"k": "ch", "v": ["redis-a1", "redis-a2", "redis-a3"]}}}},
            "beh": {"n": {"emit": [{"id": "redis_latency", "per_min": 1.0, "scope": "global"}]}, "f": {"emit": [{"id": "redis_latency", "per_min": 1.0, "scope": "global"}]}},
        },
        {
            "id": "auth_service",
            "svc": "auth-svc",
            "hosts": ["auth-a1", "auth-a2", "auth-a3"],
            "logs": {
                "auth_ok_primary": {"lvl": "INFO", "msg": "auth ok user={user_id} db=primary req_id={req_id}", "vars": {"user_id": {"k": "i", "v": [1000, 9999]}, "req_id": {"k": "uuid", "v": None}}},
                "auth_ok_dr": {"lvl": "INFO", "msg": "auth ok user={user_id} db=dr_replica req_id={req_id}", "vars": {"user_id": {"k": "i", "v": [1000, 9999]}, "req_id": {"k": "uuid", "v": None}}},
                "auth_fail_primary": {"lvl": "ERROR", "msg": "auth failed reason={reason} db=primary req_id={req_id}", "vars": {"reason": {"k": "ch", "v": ["db_timeout", "db_overloaded"]}, "req_id": {"k": "uuid", "v": None}}},
                "auth_fail_dr": {"lvl": "ERROR", "msg": "auth failed reason={reason} db=dr_replica req_id={req_id}", "vars": {"reason": {"k": "ch", "v": ["db_timeout"]}, "req_id": {"k": "uuid", "v": None}}},
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "api_gateway",
            "svc": "api-gw",
            "hosts": ["api-a1", "api-a2", "api-a3", "api-a4"],
            "logs": {
                "api_req": {"lvl": "INFO", "msg": "api req {method} {endpoint} client={client_ip} req_id={req_id}", "vars": {"method": {"k": "ch", "v": ["GET", "POST"]}, "endpoint": {"k": "ch", "v": ["/client/v4/zones", "/client/v4/user", "/client/v4/accounts"]}, "client_ip": {"k": "ip", "v": "203.0.113.0/24"}, "req_id": {"k": "uuid", "v": None}}},
                "auth_call": {"lvl": "DEBUG", "msg": "auth call attempt={attempt} req_id={req_id} target=auth-svc", "vars": {"attempt": {"k": "i", "v": [1, 3]}, "req_id": {"k": "uuid", "v": None}}},
                "api_resp_200": {"lvl": "INFO", "msg": "api resp 200 req_id={req_id} dur_ms={dur_ms} bytes={bytes}", "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [5, 3000]}, "bytes": {"k": "i", "v": [200, 60000]}}},
                "api_resp_503": {"lvl": "WARN", "msg": "api resp 503 upstream=auth err={err} req_id={req_id} dur_ms={dur_ms}", "vars": {"err": {"k": "ch", "v": ["timeout", "upstream_unavailable", "db_busy"]}, "req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [50, 30000]}}},
                "api_retry": {"lvl": "WARN", "msg": "retry auth req_id={req_id} attempt={attempt} backoff_ms={backoff_ms}", "vars": {"req_id": {"k": "uuid", "v": None}, "attempt": {"k": "i", "v": [2, 3]}, "backoff_ms": {"k": "i", "v": [50, 3000]}}},
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "dashboard_web",
            "svc": "dash-web",
            "hosts": ["dash-a1", "dash-a2", "dash-a3"],
            "logs": {
                "page_req": {"lvl": "INFO", "msg": "dash req {method} {route} session={session_id} req_id={req_id}", "vars": {"method": {"k": "ch", "v": ["GET", "POST"]}, "route": {"k": "ch", "v": ["/login", "/dash", "/dash/zones"]}, "session_id": {"k": "uuid", "v": None}, "req_id": {"k": "uuid", "v": None}}},
                "page_resp_200": {"lvl": "INFO", "msg": "dash resp 200 req_id={req_id} dur_ms={dur_ms} bytes={bytes}", "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [20, 40000]}, "bytes": {"k": "i", "v": [500, 120000]}}},
                "page_resp_504": {"lvl": "WARN", "msg": "dash resp 504 upstream=auth req_id={req_id} dur_ms={dur_ms}", "vars": {"req_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [500, 40000]}}},
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {"id": "api_primary_ok_n", "rpm": 315.0, "emit": ["api_gateway.api_req", "auth_service.auth_ok_primary", "api_gateway.api_resp_200"], "latency_ms": [[2, 5], [6, 20], [8, 35]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "api_ingress_503_n", "rpm": 5.0, "emit": ["api_gateway.api_req", "api_gateway.api_resp_503"], "latency_ms": [[2, 6], [80, 1200]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "api_auth_primary_fail_n", "rpm": 5.0, "emit": ["api_gateway.auth_call", "auth_service.auth_fail_primary"], "latency_ms": [[3, 10], [40, 300]], "retry": {"max_attempts": 3, "expected_attempts": 1.3, "emit_per_retry": ["api_gateway.api_retry"], "backoff_ms": [[80, 250], [150, 600]]}, "trace": True},
                {"id": "dash_primary_ok_n", "rpm": 118.0, "emit": ["dashboard_web.page_req", "auth_service.auth_ok_primary", "dashboard_web.page_resp_200"], "latency_ms": [[3, 8], [15, 50], [60, 180]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "dash_primary_504_n", "rpm": 2.0, "emit": ["dashboard_web.page_req", "auth_service.auth_fail_primary", "dashboard_web.page_resp_504"], "latency_ms": [[3, 10], [200, 1200], [800, 5000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            ]
        },
        "f": {
            "req": [
                {"id": "api_primary_ok_f", "rpm": 315.0, "emit": ["api_gateway.api_req", "auth_service.auth_ok_primary", "api_gateway.api_resp_200"], "latency_ms": [[2, 6], [10, 40], [10, 60]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "api_ingress_503_primary_f", "rpm": 5.0, "emit": ["api_gateway.api_req", "api_gateway.api_resp_503"], "latency_ms": [[2, 6], [400, 12000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "api_auth_primary_fail_f", "rpm": 5.0, "emit": ["api_gateway.auth_call", "auth_service.auth_fail_primary"], "latency_ms": [[3, 12], [120, 1200]], "retry": {"max_attempts": 3, "expected_attempts": 2.2, "emit_per_retry": ["api_gateway.api_retry"], "backoff_ms": [[100, 400], [200, 1200]]}, "trace": True},
                {"id": "api_dr_ok_f", "rpm": 315.0, "emit": ["api_gateway.api_req", "auth_service.auth_ok_dr", "api_gateway.api_resp_200"], "latency_ms": [[2, 6], [60, 250], [20, 90]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "api_ingress_503_dr_f", "rpm": 5.0, "emit": ["api_gateway.api_req", "api_gateway.api_resp_503"], "latency_ms": [[2, 6], [600, 15000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "api_auth_dr_fail_f", "rpm": 5.0, "emit": ["api_gateway.auth_call", "auth_service.auth_fail_dr"], "latency_ms": [[3, 12], [200, 2000]], "retry": {"max_attempts": 3, "expected_attempts": 1.6, "emit_per_retry": ["api_gateway.api_retry"], "backoff_ms": [[120, 500], [250, 1400]]}, "trace": True},
                {"id": "dash_primary_ok_f", "rpm": 118.0, "emit": ["dashboard_web.page_req", "auth_service.auth_ok_primary", "dashboard_web.page_resp_200"], "latency_ms": [[3, 10], [50, 600], [150, 5000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "dash_primary_504_f", "rpm": 2.0, "emit": ["dashboard_web.page_req", "auth_service.auth_fail_primary", "dashboard_web.page_resp_504"], "latency_ms": [[3, 10], [500, 6000], [2000, 30000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "dash_dr_ok_f", "rpm": 118.0, "emit": ["dashboard_web.page_req", "auth_service.auth_ok_dr", "dashboard_web.page_resp_200"], "latency_ms": [[3, 10], [600, 12000], [1200, 35000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "dash_dr_504_f", "rpm": 2.0, "emit": ["dashboard_web.page_req", "auth_service.auth_fail_dr", "dashboard_web.page_resp_504"], "latency_ms": [[3, 10], [1500, 20000], [5000, 40000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "api_dashboard_availability_2020_11_02_compact",
        "time": {"total_minutes": 48, "phases": {"n": {"start_min": 0, "end_min": 24}, "f": {"start_min": 24, "end_min": 48}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 24,
                        "rate_multipliers": {
                            "rack_switch.switch_ping_loss": 1.0,
                            "etcd_cluster.raft_election_started": 0.0,
                            "etcd_cluster.raft_write_blocked": 0.0,
                            "db_cluster_mgr.replica_rebuild_progress": 0.0,
                            "auth_db_primary.db_overload": 0.0,
                            "api_ingress_503_primary_f": 0.0,
                            "api_auth_primary_fail_f": 0.0,
                            "dash_primary_504_f": 0.0,
                            "api_dr_ok_f": 0.0,
                            "api_ingress_503_dr_f": 0.0,
                            "api_auth_dr_fail_f": 0.0,
                            "dash_dr_ok_f": 0.0,
                            "dash_dr_504_f": 0.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [],
                    },
                    {
                        "order": 2,
                        "at_min": 26,
                        "rate_multipliers": {
                            "etcd_cluster.raft_leader_heartbeat": 0.0,
                            "etcd_cluster.raft_election_started": 1.0,
                            "etcd_cluster.raft_write_blocked": 1.0,
                            "db_cluster_mgr.replica_rebuild_progress": 1.0,
                            "auth_db_primary.db_overload": 1.0,
                            "api_primary_ok_f": 0.67,
                            "api_ingress_503_primary_f": 14.0,
                            "api_auth_primary_fail_f": 14.0,
                            "dash_primary_ok_f": 0.76,
                            "dash_primary_504_f": 15.0,
                        },
                        "latency_multipliers": {"api_primary_ok_f": {"p50": 1.5, "p95": 3.0}, "api_ingress_503_primary_f": {"p50": 1.8, "p95": 2.5}, "dash_primary_ok_f": {"p50": 4.0, "p95": 8.0}, "dash_primary_504_f": {"p50": 1.3, "p95": 1.3}},
                        "one_shots": [{"ref": "db_cluster_mgr.promote_primary", "count": 1, "hosts": ["dbmgr-a1"]}, {"ref": "db_cluster_mgr.replica_rebuild_started", "count": 1, "hosts": ["dbmgr-a1"]}],
                    },
                    {
                        "order": 3,
                        "at_min": 30,
                        "rate_multipliers": {"rack_switch.switch_ping_loss": 0.0, "etcd_cluster.raft_election_started": 0.0, "etcd_cluster.raft_write_blocked": 0.0, "etcd_cluster.raft_leader_heartbeat": 1.0},
                        "latency_multipliers": {},
                        "one_shots": [{"ref": "rack_switch.switch_recovered", "count": 1, "hosts": ["sw-r12a"]}, {"ref": "etcd_cluster.raft_leader_stable", "count": 1, "hosts": ["etcd-3"]}],
                    },
                    {
                        "order": 4,
                        "at_min": 34,
                        "rate_multipliers": {"auth_db_primary.db_overload": 0.4, "api_primary_ok_f": 0.0, "api_ingress_503_primary_f": 0.0, "api_auth_primary_fail_f": 0.0, "api_dr_ok_f": 0.95, "api_ingress_503_dr_f": 1.2, "api_auth_dr_fail_f": 1.2, "dash_primary_ok_f": 0.0, "dash_primary_504_f": 0.0, "dash_dr_ok_f": 0.95, "dash_dr_504_f": 3.0},
                        "latency_multipliers": {},
                        "one_shots": [{"ref": "db_cluster_mgr.noncritical_work_throttled", "count": 1, "hosts": ["dbmgr-a2"]}, {"ref": "db_cluster_mgr.read_routing_enabled_all", "count": 1, "hosts": ["dbmgr-a2"]}],
                    },
                    {
                        "order": 5,
                        "at_min": 40,
                        "rate_multipliers": {"api_dr_ok_f": 0.95, "api_ingress_503_dr_f": 1.2, "api_auth_dr_fail_f": 1.2, "dash_dr_ok_f": 0.0, "dash_dr_504_f": 0.0, "dash_primary_ok_f": 0.70, "dash_primary_504_f": 2.5, "auth_db_primary.db_overload": 0.7},
                        "latency_multipliers": {"dash_primary_ok_f": {"p50": 1.3, "p95": 1.6}},
                        "one_shots": [{"ref": "db_cluster_mgr.read_routing_enabled_api_only", "count": 1, "hosts": ["dbmgr-a2"]}],
                    },
                ]
            }
        },
    }
}

# ---------------- Deterministic helpers ----------------


def stable_int(key: str, mod: Optional[int] = None) -> int:
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    val = int(h[:16], 16)
    return val if mod is None else val % mod


def stable_u01(key: str) -> float:
    return stable_int(key, 2**53) / float(2**53)


def deterministic_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    return base + (1 if stable_u01(f"round:{key}") < frac else 0)


def clamp_dt(t: datetime, start: datetime, end: datetime) -> datetime:
    if t < start:
        return start + timedelta(milliseconds=1)
    if t >= end:
        return end - timedelta(milliseconds=1)
    return t


def schedule_times(start: datetime, end: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    dur_s = (end - start).total_seconds()
    if dur_s <= 0:
        return []
    spacing = dur_s / count
    max_jitter = min(spacing * 0.30, 0.80)
    out: List[datetime] = []
    for i in range(count):
        center = (i + 0.5) * spacing
        jitter = (stable_u01(f"jit:{key}:{i}") - 0.5) * max_jitter
        t = start + timedelta(seconds=center + jitter)
        out.append(clamp_dt(t, start, end))
    return out


# Acklam's inverse normal CDF approximation
def inv_norm_cdf(p: float) -> float:
    p = min(max(p, 1e-12), 1 - 1e-12)
    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02, 1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]
    plow = 0.02425
    phigh = 1 - plow
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)


def lognormal_quantile_from_p50_p95(p50: float, p95: float, q: float, soft_cap: Optional[float] = None) -> float:
    p50 = max(p50, 0.001)
    p95 = max(p95, p50 * 1.001)
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.6448536269514722
    z = inv_norm_cdf(q)
    x = math.exp(mu + sigma * z)
    if soft_cap is not None:
        x = min(x, soft_cap)
    return max(x, 0.001)


def deterministic_uuid_str(key: str) -> str:
    b = bytearray(hashlib.md5(key.encode("utf-8")).digest())
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(b)))


def deterministic_hex(key: str, n: int) -> str:
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    if n <= 32:
        return h[:n]
    out = h
    k = 1
    while len(out) < n:
        out += hashlib.md5((key + f":{k}").encode("utf-8")).hexdigest()
        k += 1
    return out[:n]


def deterministic_ip_from_cidr(cidr: str, key: str) -> str:
    net = ipaddress.ip_network(cidr, strict=False)
    hosts = list(net.hosts())
    if not hosts:
        return str(net.network_address)
    idx = stable_int(f"ip:{key}", len(hosts))
    return str(hosts[idx])


def isoformat_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond / 1000):03d}Z"


# ---------------- Model indices ----------------


@dataclass(frozen=True)
class LogTemplate:
    component_id: str
    log_id: str
    lvl: str
    msg: str
    vars: Dict[str, Any]
    state_vars: Dict[str, Dict[str, Any]]
    fields: Tuple[str, ...]


COMPONENTS: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

LOGS: Dict[str, LogTemplate] = {}
for cid, comp in COMPONENTS.items():
    for lid, t in comp.get("logs", {}).items():
        msg = t["msg"]
        fields = tuple(sorted({m.group(1) for m in re.finditer(r"\{(\w+)\}", msg)}))
        LOGS[f"{cid}.{lid}"] = LogTemplate(component_id=cid, log_id=lid, lvl=t["lvl"], msg=msg, vars=t.get("vars", {}) or {}, state_vars=t.get("state_vars", {}) or {}, fields=fields)

FLOWS: Dict[str, Dict[str, Any]] = {"n": {f["id"]: f for f in SYSTEM["flows"]["n"]["req"]}, "f": {f["id"]: f for f in SYSTEM["flows"]["f"]["req"]}}

# ---------------- Scenario controls ----------------


def build_failure_intervals() -> List[Dict[str, Any]]:
    f_phase = SCENARIO["scenario"]["time"]["phases"]["f"]
    f_start = f_phase["start_min"]
    f_end = f_phase["end_min"]
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = [f_start] + [e["at_min"] for e in events] + [f_end]
    boundaries = sorted(set(boundaries))

    active_rate: Dict[str, float] = {}
    active_latency: Dict[str, Dict[str, float]] = {}
    events_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        events_by_min.setdefault(e["at_min"], []).append(e)

    intervals: List[Dict[str, Any]] = []
    for i in range(len(boundaries) - 1):
        s = boundaries[i]
        for e in events_by_min.get(s, []):
            for k, v in (e.get("rate_multipliers") or {}).items():
                active_rate[k] = float(v)
            for k, v in (e.get("latency_multipliers") or {}).items():
                active_latency[k] = {"p50": float(v["p50"]), "p95": float(v["p95"])}
        e = boundaries[i + 1]
        if s < e:
            intervals.append({"start_min": s, "end_min": e, "rate": dict(active_rate), "latency": dict(active_latency)})
    return intervals


FAIL_INTERVALS = build_failure_intervals()

# ---------------- Emission engine ----------------


def component_identity(component_id: str) -> Tuple[str, List[str]]:
    comp = COMPONENTS[component_id]
    svc = comp.get("svc") or ""
    hosts = comp.get("hosts") or []
    return svc, hosts


def choose_from_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    if k == "ch":
        choices = list(v)
        return choices[stable_int(f"ch:{key}", len(choices))]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi <= lo:
            return lo
        return lo + stable_int(f"i:{key}", hi - lo + 1)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        u = stable_u01(f"f:{key}")
        return lo + (hi - lo) * u
    if k == "uuid":
        return deterministic_uuid_str(f"uuid:{key}")
    if k == "hex":
        n = int(v)
        return deterministic_hex(f"hex:{key}", n)
    if k == "ip":
        return deterministic_ip_from_cidr(str(v), key)
    if k == "str":
        return f"{dom.get('v', '')}-{stable_int(f's:{key}', 1000000)}"
    return ""


def get_int_domain(template_ref: str, field: str) -> Optional[Tuple[int, int]]:
    t = LOGS[template_ref]
    dom = t.vars.get(field)
    if not dom or dom.get("k") != "i":
        return None
    lo, hi = int(dom["v"][0]), int(dom["v"][1])
    return lo, hi


def render_message(template: LogTemplate, state: str, ctx: Dict[str, Any], overrides: Dict[str, Any], key: str) -> str:
    vals: Dict[str, Any] = {}
    for field in template.fields:
        if field in overrides:
            vals[field] = overrides[field]
            ctx[field] = vals[field]
            continue
        if field in ctx:
            vals[field] = ctx[field]
            continue
        dom = None
        if state in template.state_vars and field in template.state_vars[state]:
            dom = template.state_vars[state][field]
        elif field in template.vars:
            dom = template.vars[field]
        if dom is None:
            vals[field] = ""
        else:
            vals[field] = choose_from_domain(dom, f"{key}:{field}")
        ctx[field] = vals[field]
    return template.msg.format(**vals)


def emit_row(
    rows: List[Dict[str, Any]],
    ts: datetime,
    template_ref: str,
    state: str,
    trace_id: str,
    host: str,
    ctx: Optional[Dict[str, Any]] = None,
    overrides: Optional[Dict[str, Any]] = None,
    key: str = "",
) -> None:
    template = LOGS[template_ref]
    comp = COMPONENTS[template.component_id]
    svc = comp.get("svc") or ""
    h = host or ""
    local_ctx = ctx if ctx is not None else {}
    ov = overrides if overrides is not None else {}
    msg = render_message(template, state, local_ctx, ov, key)
    rows.append({"timestamp_dt": ts, "level": template.lvl, "message": msg, "trace_id": trace_id, "service": svc, "host": h})


def attempt_count_for_flow(flow: Dict[str, Any], key: str) -> int:
    retry = flow["retry"]
    max_a = int(retry["max_attempts"])
    e = float(retry["expected_attempts"])
    e = min(max(e, 1.0), float(max_a))
    lo = int(math.floor(e))
    hi = int(min(max_a, lo + 1))
    if hi == lo:
        return lo
    p = e - lo
    return hi if stable_u01(f"att:{key}") < p else lo


def scale_latency_pair(pair: List[float], mult: Optional[Dict[str, float]]) -> Tuple[float, float]:
    p50, p95 = float(pair[0]), float(pair[1])
    if mult is None:
        return p50, p95
    return p50 * float(mult.get("p50", 1.0)), p95 * float(mult.get("p95", 1.0))


def bounded_latency_ms(p50: float, p95: float, key: str) -> int:
    u = stable_u01(f"lat_u:{key}")
    q = 0.15 + 0.70 * u
    soft_cap = 2.8 * p95
    x = lognormal_quantile_from_p50_p95(p50, p95, q, soft_cap=soft_cap)
    return int(max(1, round(x)))


def bounded_backoff_ms(p50: float, p95: float, key: str, hard_min: int = 1, hard_max: int = 60000) -> int:
    u = stable_u01(f"bo_u:{key}")
    q = 0.20 + 0.75 * u
    soft_cap = min(3.0 * p95, float(hard_max))
    x = lognormal_quantile_from_p50_p95(p50, p95, q, soft_cap=soft_cap)
    x = max(float(hard_min), min(float(hard_max), x))
    return int(max(1, round(x)))


def maybe_adjust_delays_for_duration(delays_ms: List[int], dur_range: Tuple[int, int]) -> List[int]:
    if len(delays_ms) < 2:
        return delays_ms
    min_d, max_d = dur_range
    cur = sum(delays_ms[1:])
    if cur <= 0:
        return delays_ms
    target = cur
    if cur < min_d:
        target = min_d
    elif cur > max_d:
        target = max_d
    if target == cur:
        return delays_ms
    scale = target / cur
    out = [delays_ms[0]]
    for d in delays_ms[1:]:
        out.append(int(max(1, round(d * scale))))
    drift = target - sum(out[1:])
    if drift != 0:
        out[-1] = max(1, out[-1] + drift)
    return out


def get_dur_domain_ms(template_ref: str) -> Optional[Tuple[int, int]]:
    return get_int_domain(template_ref, "dur_ms")


def choose_host_for_component(component_id: str, chain_key: str) -> str:
    _, hosts = component_identity(component_id)
    if not hosts:
        return ""
    return hosts[stable_int(f"host:{component_id}:{chain_key}", len(hosts))]


def simulate_flow_instance(rows: List[Dict[str, Any]], flow: Dict[str, Any], state: str, start_ts: datetime, latency_mult: Optional[Dict[str, float]], instance_key: str) -> None:
    trace_id = deterministic_hex(f"trace:{flow['id']}:{instance_key}", 32) if (SYSTEM["tracing"]["on"] and flow.get("trace")) else ""

    comp_hosts: Dict[str, str] = {}

    def host_for_ref(ref: str) -> str:
        cid, _ = ref.split(".", 1)
        if cid not in comp_hosts:
            comp_hosts[cid] = choose_host_for_component(cid, trace_id or instance_key)
        return comp_hosts[cid]

    flow_ctx: Dict[str, Any] = {}
    flow_ctx["req_id"] = deterministic_uuid_str(f"reqid:{flow['id']}:{trace_id or instance_key}")
    flow_ctx["session_id"] = deterministic_uuid_str(f"sess:{flow['id']}:{trace_id or instance_key}")

    # Coarse shared attributes (not all are used by all flows)
    flow_ctx["method"] = choose_from_domain(LOGS["api_gateway.api_req"].vars["method"], f"m:{trace_id}:{flow['id']}")
    flow_ctx["endpoint"] = choose_from_domain(LOGS["api_gateway.api_req"].vars["endpoint"], f"e:{trace_id}:{flow['id']}")
    flow_ctx["client_ip"] = choose_from_domain(LOGS["api_gateway.api_req"].vars["client_ip"], f"ip:{trace_id}:{flow['id']}")
    flow_ctx["route"] = choose_from_domain(LOGS["dashboard_web.page_req"].vars["route"], f"r:{trace_id}:{flow['id']}")
    flow_ctx["user_id"] = choose_from_domain(LOGS["auth_service.auth_ok_primary"].vars["user_id"], f"u:{trace_id}:{flow['id']}")

    attempts = attempt_count_for_flow(flow, f"{flow['id']}:{instance_key}")
    emit_refs = list(flow["emit"])
    latency_pairs = list(flow["latency_ms"])
    retry = flow["retry"]
    emit_per_retry = list(retry.get("emit_per_retry") or [])
    backoff_pairs = list(retry.get("backoff_ms") or [])

    # Derive backoff clamp bounds from retry-log template domains (so message and spacing stay consistent).
    backoff_min = 1
    backoff_max = 60000
    for rr_ref in emit_per_retry:
        dom = get_int_domain(rr_ref, "backoff_ms")
        if dom is not None:
            backoff_min = max(backoff_min, dom[0])
            backoff_max = min(backoff_max, dom[1])

    prev_attempt_end = start_ts
    attempt_start = start_ts

    for attempt in range(1, attempts + 1):
        attempt_key = f"{instance_key}:a{attempt}"

        backoff_ms = 0
        if attempt > 1:
            idx = attempt - 2
            if idx < len(backoff_pairs):
                p50, p95 = float(backoff_pairs[idx][0]), float(backoff_pairs[idx][1])
            else:
                p50, p95 = 100.0, 400.0

            backoff_ms = bounded_backoff_ms(p50, p95, f"bo:{flow['id']}:{attempt_key}", hard_min=backoff_min, hard_max=backoff_max)
            attempt_start = prev_attempt_end + timedelta(milliseconds=backoff_ms)

            for rr_i, rr_ref in enumerate(emit_per_retry):
                overrides = {"attempt": attempt, "backoff_ms": backoff_ms, "req_id": flow_ctx["req_id"]}
                emit_row(
                    rows,
                    attempt_start + timedelta(milliseconds=rr_i),
                    rr_ref,
                    state,
                    trace_id,
                    host_for_ref(rr_ref),
                    ctx=flow_ctx,
                    overrides=overrides,
                    key=f"retry:{flow['id']}:{attempt_key}:{rr_ref}",
                )
        else:
            attempt_start = start_ts

        delays: List[int] = []
        for i, pair in enumerate(latency_pairs):
            p50, p95 = scale_latency_pair(pair, latency_mult if state == "f" else None)
            delays.append(bounded_latency_ms(p50, p95, f"lat:{flow['id']}:{attempt_key}:i{i}"))

        dur_dom = get_dur_domain_ms(emit_refs[-1])
        if dur_dom is not None:
            delays = maybe_adjust_delays_for_duration(delays, dur_dom)

        attempt_ctx = dict(flow_ctx)
        attempt_ctx["attempt"] = attempt

        # Bind coherent outcome-ish fields per attempt/flow type.
        if "auth_fail_primary" in emit_refs[-1]:
            reason_choices = LOGS["auth_service.auth_fail_primary"].vars["reason"]["v"]
            attempt_ctx["reason"] = reason_choices[0] if stable_u01(f"rsn:{trace_id}:{flow['id']}:{attempt}") < 0.7 else reason_choices[1]
        if "auth_fail_dr" in emit_refs[-1]:
            attempt_ctx["reason"] = "db_timeout"

        if emit_refs[-1] == "api_gateway.api_resp_503":
            err_choices = LOGS["api_gateway.api_resp_503"].vars["err"]["v"]
            if "dr" in flow["id"]:
                attempt_ctx["err"] = "timeout"
            else:
                attempt_ctx["err"] = err_choices[stable_int(f"err:{trace_id}:{flow['id']}", len(err_choices))]

        t = attempt_start
        times: List[datetime] = []
        for i, _ref in enumerate(emit_refs):
            t = t + timedelta(milliseconds=delays[i])
            times.append(t)

        overrides_by_ref: Dict[str, Dict[str, Any]] = {}
        if emit_refs[-1] in ("api_gateway.api_resp_200", "api_gateway.api_resp_503", "dashboard_web.page_resp_200", "dashboard_web.page_resp_504"):
            dur_ms = int(round((times[-1] - times[0]).total_seconds() * 1000))
            overrides_by_ref.setdefault(emit_refs[-1], {})["dur_ms"] = dur_ms

        if emit_refs[-1] in ("api_gateway.api_resp_200", "dashboard_web.page_resp_200"):
            if emit_refs[-1] == "api_gateway.api_resp_200":
                lo, hi = LOGS["api_gateway.api_resp_200"].vars["bytes"]["v"]
                base = 1200 if attempt_ctx.get("method") == "GET" else 8000
                span = int(hi) - int(lo)
                val = int(lo) + (base + stable_int(f"bytes:{trace_id}:{flow['id']}", span)) % (span + 1)
                overrides_by_ref.setdefault(emit_refs[-1], {})["bytes"] = int(val)
            else:
                lo, hi = LOGS["dashboard_web.page_resp_200"].vars["bytes"]["v"]
                base = 15000 if attempt_ctx.get("route") != "/login" else 3000
                span = int(hi) - int(lo)
                val = int(lo) + (base + stable_int(f"bytes:{trace_id}:{flow['id']}", span)) % (span + 1)
                overrides_by_ref.setdefault(emit_refs[-1], {})["bytes"] = int(val)

        if "api_gateway.auth_call" in emit_refs:
            overrides_by_ref.setdefault("api_gateway.auth_call", {})["attempt"] = attempt

        for ref, ts in zip(emit_refs, times):
            emit_row(rows, ts, ref, state, trace_id, host_for_ref(ref), ctx=attempt_ctx, overrides=overrides_by_ref.get(ref, {}), key=f"flow:{flow['id']}:{attempt_key}:{ref}")

        prev_attempt_end = times[-1]


# ---------------- Simulation ----------------

BASE_TIME = datetime(2020, 11, 2, 0, 0, 0, tzinfo=timezone.utc)


def minute_to_dt(minute: int) -> datetime:
    return BASE_TIME + timedelta(minutes=int(minute))


def effective_mult(rate_controls: Dict[str, float], key: str) -> float:
    return float(rate_controls.get(key, 1.0))


def effective_latency_mult(lat_controls: Dict[str, Dict[str, float]], flow_id: str) -> Optional[Dict[str, float]]:
    return lat_controls.get(flow_id)


def _pick_host_from_list(hosts: List[str], key: str) -> str:
    if not hosts:
        return ""
    return hosts[stable_int(f"pick:{key}", len(hosts))]


def plan_background(rows: List[Dict[str, Any]], state: str, start_min: int, end_min: int, rate_controls: Optional[Dict[str, float]] = None, primary_node: str = "authdb-a1") -> None:
    rate_controls = rate_controls or {}
    start_dt = minute_to_dt(start_min)
    end_dt = minute_to_dt(end_min)
    duration_min = (end_dt - start_dt).total_seconds() / 60.0

    for cid, comp in COMPONENTS.items():
        beh = comp.get("beh", {}).get(state, {})
        for emission in beh.get("emit", []) or []:
            log_id = emission["id"]
            per_min = float(emission["per_min"])
            scope = emission.get("scope", "per_host")
            ref = f"{cid}.{log_id}"
            mult = 1.0
            if state == "f":
                mult = effective_mult(rate_controls, ref)
            eff_rate = per_min * mult
            if eff_rate <= 0:
                continue

            _svc, hosts = component_identity(cid)
            if scope == "global":
                expected = eff_rate * duration_min
                n = deterministic_round(expected, f"bg:{state}:{start_min}-{end_min}:{ref}:global")
                times = schedule_times(start_dt, end_dt, n, f"bg:{state}:{start_min}-{end_min}:{ref}:global")
                for i, ts in enumerate(times):
                    overrides: Dict[str, Any] = {}
                    host = ""

                    # Keep host consistent with node-identifying fields in the message for multi-host components.
                    if ref == "etcd_cluster.raft_leader_heartbeat":
                        leader = "etcd-3"
                        host = leader
                        overrides["leader"] = leader
                        overrides["term"] = 1320 + stable_int(f"term:{start_min}:{i}", 12)
                    elif ref == "etcd_cluster.raft_election_started":
                        member = _pick_host_from_list(hosts, f"etcd_member:{start_min}:{end_min}:{i}")
                        host = member
                        overrides["member"] = member
                        overrides["term"] = 1320 + stable_int(f"eterm:{start_min}:{i}", 25)
                        # deterministic choice of reason
                        reasons = LOGS[ref].vars["reason"]["v"]
                        overrides["reason"] = reasons[stable_int(f"ereason:{start_min}:{i}", len(reasons))]
                    elif ref == "etcd_cluster.raft_write_blocked":
                        member = _pick_host_from_list(hosts, f"etcd_member_blk:{start_min}:{end_min}:{i}")
                        host = member
                        overrides["member"] = member
                        dom = LOGS[ref].vars["wait_ms"]["v"]
                        overrides["wait_ms"] = int(dom[0]) + stable_int(f"w:{start_min}:{i}", int(dom[1]) - int(dom[0]) + 1)
                    elif ref == "auth_db_primary.db_health":
                        host = primary_node
                        overrides["node"] = primary_node
                        if state == "n":
                            overrides["cpu_pct"] = 18 + stable_int(f"cpu:n:{start_min}:{i}", 25)
                            overrides["connections"] = 120 + stable_int(f"con:n:{start_min}:{i}", 220)
                            overrides["qps"] = 900 + stable_int(f"qps:n:{start_min}:{i}", 1200)
                        else:
                            overrides["cpu_pct"] = 55 + stable_int(f"cpu:f:{start_min}:{i}", 40)
                            overrides["connections"] = 420 + stable_int(f"con:f:{start_min}:{i}", 360)
                            overrides["qps"] = 2200 + stable_int(f"qps:f:{start_min}:{i}", 2500)
                    elif ref == "auth_db_primary.db_overload":
                        host = primary_node
                        overrides["node"] = primary_node
                        overrides["cpu_pct"] = 88 + stable_int(f"ocpu:{start_min}:{i}", 12)
                    elif ref == "redis_cache.redis_latency":
                        node = _pick_host_from_list(hosts, f"redis_node:{start_min}:{end_min}:{i}")
                        host = node
                        overrides["node"] = node
                        overrides["p95_ms"] = 2 + stable_int(f"rms:{start_min}:{i}", 9)
                    elif ref == "rack_switch.switch_ping_loss":
                        host = hosts[0] if hosts else ""
                        overrides["switch"] = host or "sw-r12a"
                        if state == "f":
                            overrides["loss_pct"] = 8 + stable_int(f"loss:{start_min}:{i}", 53)
                    elif ref == "db_cluster_mgr.replica_rebuild_progress":
                        # no node-identifying field in msg; still emit with a deterministic mgr host
                        host = _pick_host_from_list(hosts, f"dbmgr_host:{start_min}:{end_min}:{i}")
                        elapsed = max(0.0, (ts - minute_to_dt(26)).total_seconds() / 60.0)
                        pct = int(min(99, round((elapsed / 22.0) * 90.0 + (stable_u01(f"pct:{ts.isoformat()}") - 0.5) * 6)))
                        gb = int(min(600, max(0, round((elapsed / 22.0) * 520.0 + (stable_u01(f"gb:{ts.isoformat()}") - 0.5) * 40))))
                        overrides["pct"] = max(0, min(99, pct))
                        overrides["gb"] = max(0, min(600, gb))
                    else:
                        host = _pick_host_from_list(hosts, f"default_global_host:{ref}:{start_min}:{end_min}:{i}")

                    emit_row(rows, ts, ref, state, "", host, ctx={}, overrides=overrides, key=f"bg:{ref}:{start_min}-{end_min}:{i}")
            else:
                for h in (hosts or [""]):
                    expected = eff_rate * duration_min
                    n = deterministic_round(expected, f"bg:{state}:{start_min}-{end_min}:{ref}:{h}")
                    times = schedule_times(start_dt, end_dt, n, f"bg:{state}:{start_min}-{end_min}:{ref}:{h}")
                    for i, ts in enumerate(times):
                        overrides = {}
                        if ref == "auth_db_dr_replica.replica_health":
                            overrides["lag_s"] = 2 + stable_int(f"lag:{start_min}:{i}", 12)
                            overrides["cpu_pct"] = 8 + stable_int(f"dcpu:{start_min}:{i}", 25)
                        emit_row(rows, ts, ref, state, "", h, ctx={}, overrides=overrides, key=f"bg:{ref}:{start_min}-{end_min}:{h}:{i}")


def plan_flows(rows: List[Dict[str, Any]], state: str, start_min: int, end_min: int, rate_controls: Optional[Dict[str, float]] = None, lat_controls: Optional[Dict[str, Dict[str, float]]] = None) -> None:
    rate_controls = rate_controls or {}
    lat_controls = lat_controls or {}

    start_dt = minute_to_dt(start_min)
    end_dt = minute_to_dt(end_min)
    duration_min = (end_dt - start_dt).total_seconds() / 60.0
    flows = FLOWS[state]

    for flow_id, flow in flows.items():
        rpm = float(flow["rpm"])
        eff_rpm = rpm
        if state == "f":
            eff_rpm = rpm * effective_mult(rate_controls, flow_id)
        if eff_rpm <= 0:
            continue
        expected_instances = eff_rpm * duration_min
        n_instances = deterministic_round(expected_instances, f"flow:{state}:{start_min}-{end_min}:{flow_id}")
        if n_instances <= 0:
            continue
        starts = schedule_times(start_dt, end_dt, n_instances, f"flowstart:{state}:{start_min}-{end_min}:{flow_id}")
        for idx, st in enumerate(starts):
            lat_mult = effective_latency_mult(lat_controls, flow_id) if state == "f" else None
            simulate_flow_instance(rows, flow, state, st, lat_mult, instance_key=f"{state}:{flow_id}:{start_min}-{end_min}:{idx}")


def emit_one_shots(rows: List[Dict[str, Any]], primary_state: Dict[str, Any]) -> None:
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for e in events:
        at_dt = minute_to_dt(e["at_min"])
        for j, os in enumerate(e.get("one_shots") or []):
            ref = os["ref"]
            count = int(os["count"])
            allowed_hosts = os.get("hosts") or []
            for k in range(count):
                jitter_ms = int(round((stable_u01(f"osjit:{ref}:{e['at_min']}:{k}") * 500.0))) + (j * 20)
                ts = at_dt + timedelta(milliseconds=jitter_ms)
                template = LOGS[ref]
                cid = template.component_id
                comp_hosts = COMPONENTS[cid].get("hosts") or []
                if allowed_hosts:
                    host = allowed_hosts[k % len(allowed_hosts)]
                else:
                    host = comp_hosts[k % len(comp_hosts)] if comp_hosts else ""

                overrides: Dict[str, Any] = {}
                if ref == "db_cluster_mgr.promote_primary":
                    overrides["cluster"] = "auth-db"
                    overrides["new_primary"] = "authdb-a2"
                    overrides["reason"] = "coordination_unavailable"
                    primary_state["current_primary"] = overrides["new_primary"]
                elif ref == "db_cluster_mgr.replica_rebuild_started":
                    overrides["cluster"] = "auth-db"
                    overrides["primary"] = primary_state.get("current_primary", "authdb-a2")
                    overrides["eta_min"] = 240 + stable_int(f"eta:{e['at_min']}", 120)
                elif ref == "rack_switch.switch_recovered":
                    overrides["switch"] = "sw-r12a"
                    overrides["loss_pct"] = 0
                elif ref == "etcd_cluster.raft_leader_stable":
                    overrides["leader"] = "etcd-3"
                    overrides["term"] = 1333
                elif ref == "db_cluster_mgr.noncritical_work_throttled":
                    overrides["features"] = "analytics"
                    overrides["limit_rps"] = 12

                emit_row(rows, ts, ref, "f", "", host, ctx={}, overrides=overrides, key=f"oneshot:{ref}:{e['at_min']}:{k}")


def run() -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    n_phase = SCENARIO["scenario"]["time"]["phases"]["n"]
    primary_state = {"current_primary": "authdb-a1"}

    plan_background(rows, "n", n_phase["start_min"], n_phase["end_min"], rate_controls=None, primary_node=primary_state["current_primary"])
    plan_flows(rows, "n", n_phase["start_min"], n_phase["end_min"], rate_controls=None, lat_controls=None)

    for it in FAIL_INTERVALS:
        s = int(it["start_min"])
        e = int(it["end_min"])
        rate_controls = it["rate"]
        lat_controls = it["latency"]

        if s <= 26 < e:
            primary_state["current_primary"] = "authdb-a2"

        plan_background(rows, "f", s, e, rate_controls=rate_controls, primary_node=primary_state["current_primary"])
        plan_flows(rows, "f", s, e, rate_controls=rate_controls, lat_controls=lat_controls)

    emit_one_shots(rows, primary_state)

    df = pd.DataFrame(rows)
    df.sort_values(by=["timestamp_dt", "service", "host", "level", "trace_id", "message"], inplace=True, kind="mergesort")
    df["timestamp"] = df["timestamp_dt"].apply(isoformat_ms)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)
    return df


if __name__ == "__main__":
    run()
