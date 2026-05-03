import math
import hashlib
import uuid
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# -----------------------------
# Embedded normalized model data
# -----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "simpledb_use1_cluster"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "api_frontend",
            "svc": "simpledb-api",
            "hosts": ["api-1", "api-2"],
            "logs": {
                "read_in": {
                    "lvl": "INFO",
                    "msg": "recv GetAttributes domain={domain} request_id={request_id}",
                    "vars": {
                        "domain": {"k": "ch", "v": ["orders", "users", "catalog", "sessions", "metrics"]},
                        "request_id": {"k": "uuid", "v": None},
                    },
                },
                "write_in": {
                    "lvl": "INFO",
                    "msg": "recv PutAttributes domain={domain} request_id={request_id}",
                    "vars": {
                        "domain": {"k": "ch", "v": ["orders", "users", "catalog", "sessions", "metrics"]},
                        "request_id": {"k": "uuid", "v": None},
                    },
                },
                "domain_in": {
                    "lvl": "INFO",
                    "msg": "recv {op} domain={domain} request_id={request_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["CreateDomain", "DeleteDomain"]},
                        "domain": {"k": "ch", "v": ["orders", "users", "catalog", "sessions", "metrics"]},
                        "request_id": {"k": "uuid", "v": None},
                    },
                },
                "read_200": {
                    "lvl": "INFO",
                    "msg": "respond 200 op=GetAttributes domain={domain} latency_ms={latency_ms} request_id={request_id}",
                    "vars": {
                        "domain": {"k": "ch", "v": ["orders", "users", "catalog", "sessions", "metrics"]},
                        "latency_ms": {"k": "i", "v": [2, 800]},
                        "request_id": {"k": "uuid", "v": None},
                    },
                },
                "read_500": {
                    "lvl": "ERROR",
                    "msg": "respond 500 op=GetAttributes error=InternalError latency_ms={latency_ms} request_id={request_id}",
                    "vars": {"latency_ms": {"k": "i", "v": [50, 2500]}, "request_id": {"k": "uuid", "v": None}},
                },
                "write_200": {
                    "lvl": "INFO",
                    "msg": "respond 200 op=PutAttributes domain={domain} latency_ms={latency_ms} request_id={request_id}",
                    "vars": {
                        "domain": {"k": "ch", "v": ["orders", "users", "catalog", "sessions", "metrics"]},
                        "latency_ms": {"k": "i", "v": [3, 1200]},
                        "request_id": {"k": "uuid", "v": None},
                    },
                },
                "write_500": {
                    "lvl": "ERROR",
                    "msg": "respond 500 op=PutAttributes error=InternalError latency_ms={latency_ms} request_id={request_id}",
                    "vars": {"latency_ms": {"k": "i", "v": [50, 3000]}, "request_id": {"k": "uuid", "v": None}},
                },
                "domain_200": {
                    "lvl": "INFO",
                    "msg": "respond 200 op={op} domain={domain} latency_ms={latency_ms} request_id={request_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["CreateDomain", "DeleteDomain"]},
                        "domain": {"k": "ch", "v": ["orders", "users", "catalog", "sessions", "metrics"]},
                        "latency_ms": {"k": "i", "v": [5, 5000]},
                        "request_id": {"k": "uuid", "v": None},
                    },
                },
                "domain_500": {
                    "lvl": "ERROR",
                    "msg": "respond 500 op={op} error=InternalError latency_ms={latency_ms} request_id={request_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["CreateDomain", "DeleteDomain"]},
                        "latency_ms": {"k": "i", "v": [50, 4000]},
                        "request_id": {"k": "uuid", "v": None},
                    },
                },
                "domain_503_throttled": {
                    "lvl": "WARN",
                    "msg": "respond 503 op={op} error=Throttled limit_rps={limit_rps} request_id={request_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["CreateDomain", "DeleteDomain"]},
                        "limit_rps": {"k": "f", "v": [0.01, 0.05]},
                        "request_id": {"k": "uuid", "v": None},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "metadata_node",
            "svc": "simpledb-metadata",
            "hosts": ["meta-1", "meta-2", "meta-3"],
            "logs": {
                "lookup_ok": {
                    "lvl": "INFO",
                    "msg": "resolved domain={domain} replicas={replicas} request_id={request_id}",
                    "vars": {
                        "domain": {"k": "ch", "v": ["orders", "users", "catalog", "sessions", "metrics"]},
                        "replicas": {"k": "str", "v": "comma-separated host ids (e.g., stor-1,stor-2,stor-3)"},
                        "request_id": {"k": "uuid", "v": None},
                    },
                },
                "domain_mutation_ok": {
                    "lvl": "INFO",
                    "msg": "domain {op} applied domain={domain} latency_ms={latency_ms} request_id={request_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["CreateDomain", "DeleteDomain"]},
                        "domain": {"k": "ch", "v": ["orders", "users", "catalog", "sessions", "metrics"]},
                        "latency_ms": {"k": "i", "v": [10, 6000]},
                        "request_id": {"k": "uuid", "v": None},
                    },
                },
                "handshake_ok": {
                    "lvl": "DEBUG",
                    "msg": "lock handshake ok epoch={epoch} latency_ms={latency_ms}",
                    "vars": {"epoch": {"k": "i", "v": [1000, 5000]}, "latency_ms": {"k": "i", "v": [5, 200]}},
                },
                "handshake_timeout_pre": {
                    "lvl": "WARN",
                    "msg": "lock handshake timed out elapsed_ms={elapsed_ms} deadline_ms={deadline_ms} attempt={attempt}",
                    "vars": {
                        "elapsed_ms": {"k": "i", "v": [420, 2000]},
                        "deadline_ms": {"k": "i", "v": [150, 400]},
                        "attempt": {"k": "i", "v": [1, 6]},
                    },
                },
                "handshake_timeout_post": {
                    "lvl": "WARN",
                    "msg": "lock handshake timed out elapsed_ms={elapsed_ms} deadline_ms={deadline_ms} attempt={attempt}",
                    "vars": {
                        "elapsed_ms": {"k": "i", "v": [1250, 3500]},
                        "deadline_ms": {"k": "i", "v": [800, 1200]},
                        "attempt": {"k": "i", "v": [1, 6]},
                    },
                },
                "leave_cluster": {
                    "lvl": "ERROR",
                    "msg": "removing self from cluster after {timeouts} handshake timeouts",
                    "vars": {"timeouts": {"k": "i", "v": [3, 10]}},
                },
                "process_start": {
                    "lvl": "INFO",
                    "msg": "metadata node started version={version}",
                    "vars": {"version": {"k": "ch", "v": ["1.42.0"]}},
                },
                "authorize_storage": {
                    "lvl": "INFO",
                    "msg": "authorized storage node={storage_id} for domain={domain} epoch={epoch}",
                    "vars": {
                        "storage_id": {"k": "ch", "v": ["stor-1", "stor-2", "stor-3", "stor-4", "stor-5", "stor-6"]},
                        "domain": {"k": "ch", "v": ["orders", "users", "catalog", "sessions", "metrics"]},
                        "epoch": {"k": "i", "v": [1000, 6000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "handshake_ok", "per_min": 2.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "handshake_timeout_pre", "per_min": 4.0, "scope": "per_host"},
                        {"id": "handshake_timeout_post", "per_min": 2.0, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "storage_node",
            "svc": "simpledb-storage",
            "hosts": ["stor-1", "stor-2", "stor-3", "stor-4", "stor-5", "stor-6"],
            "logs": {
                "read_ok": {
                    "lvl": "INFO",
                    "msg": "read ok domain={domain} bytes={bytes} request_id={request_id}",
                    "vars": {
                        "domain": {"k": "ch", "v": ["orders", "users", "catalog", "sessions", "metrics"]},
                        "bytes": {"k": "i", "v": [200, 20000]},
                        "request_id": {"k": "uuid", "v": None},
                    },
                },
                "write_ok": {
                    "lvl": "INFO",
                    "msg": "write ok domain={domain} items={items} request_id={request_id}",
                    "vars": {
                        "domain": {"k": "ch", "v": ["orders", "users", "catalog", "sessions", "metrics"]},
                        "items": {"k": "i", "v": [1, 25]},
                        "request_id": {"k": "uuid", "v": None},
                    },
                },
                "handshake_ok": {
                    "lvl": "DEBUG",
                    "msg": "lock handshake ok epoch={epoch} latency_ms={latency_ms}",
                    "vars": {"epoch": {"k": "i", "v": [1000, 5000]}, "latency_ms": {"k": "i", "v": [5, 200]}},
                },
                "handshake_timeout_pre": {
                    "lvl": "WARN",
                    "msg": "lock handshake timed out elapsed_ms={elapsed_ms} deadline_ms={deadline_ms} attempt={attempt}",
                    "vars": {
                        "elapsed_ms": {"k": "i", "v": [420, 2200]},
                        "deadline_ms": {"k": "i", "v": [150, 400]},
                        "attempt": {"k": "i", "v": [1, 6]},
                    },
                },
                "handshake_timeout_post": {
                    "lvl": "WARN",
                    "msg": "lock handshake timed out elapsed_ms={elapsed_ms} deadline_ms={deadline_ms} attempt={attempt}",
                    "vars": {
                        "elapsed_ms": {"k": "i", "v": [1250, 3800]},
                        "deadline_ms": {"k": "i", "v": [800, 1200]},
                        "attempt": {"k": "i", "v": [1, 6]},
                    },
                },
                "leave_cluster": {
                    "lvl": "ERROR",
                    "msg": "storage node leaving cluster after {timeouts} handshake timeouts",
                    "vars": {"timeouts": {"k": "i", "v": [3, 10]}},
                },
                "rejoin_success": {
                    "lvl": "INFO",
                    "msg": "storage node rejoined after authorization epoch={epoch}",
                    "vars": {"epoch": {"k": "i", "v": [1000, 6000]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "handshake_ok", "per_min": 2.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "handshake_timeout_pre", "per_min": 3.0, "scope": "per_host"},
                        {"id": "handshake_timeout_post", "per_min": 1.5, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "lock_service",
            "svc": "internal-locksvc",
            "hosts": ["lock-1", "lock-2", "lock-3"],
            "logs": {
                "stats": {
                    "lvl": "INFO",
                    "msg": "locksvc stats inflight={inflight} p95_ms={p95_ms} dc={dc}",
                    "vars": {
                        "inflight": {"k": "i", "v": [0, 50000]},
                        "p95_ms": {"k": "i", "v": [20, 5000]},
                        "dc": {"k": "ch", "v": ["use1a", "use1b", "use1c"]},
                    },
                },
                "queue_warn": {
                    "lvl": "WARN",
                    "msg": "locksvc handshake queueing inflight={inflight} p95_ms={p95_ms} dc={dc}",
                    "vars": {
                        "inflight": {"k": "i", "v": [500, 80000]},
                        "p95_ms": {"k": "i", "v": [200, 5000]},
                        "dc": {"k": "ch", "v": ["use1a", "use1b", "use1c"]},
                    },
                },
                "deregister_batch": {
                    "lvl": "INFO",
                    "msg": "deregistered failed nodes dc={dc} count={count}",
                    "vars": {"dc": {"k": "ch", "v": ["use1a", "use1b", "use1c"]}, "count": {"k": "i", "v": [10, 500]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "stats", "per_min": 2.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "queue_warn", "per_min": 4.0, "scope": "per_host"},
                        {"id": "deregister_batch", "per_min": 2.0, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "control_plane_ops",
            "svc": "ops-control",
            "hosts": ["ops-1"],
            "logs": {
                "config_change": {
                    "lvl": "INFO",
                    "msg": "updated handshake_timeout_ms from {old_ms} to {new_ms}",
                    "vars": {"old_ms": {"k": "i", "v": [150, 400]}, "new_ms": {"k": "i", "v": [800, 1200]}},
                },
                "throttle_enabled": {
                    "lvl": "WARN",
                    "msg": "enabled throttling for domain ops limit_rps={limit_rps}",
                    "vars": {"limit_rps": {"k": "f", "v": [0.01, 0.05]}},
                },
                "throttle_status": {
                    "lvl": "INFO",
                    "msg": "throttle status domain_ops in_flight={inflight} rejected_last_min={rejected}",
                    "vars": {"inflight": {"k": "i", "v": [0, 30]}, "rejected": {"k": "i", "v": [0, 20]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": [{"id": "throttle_status", "per_min": 1.0, "scope": "global"}]}},
        },
        {
            "id": "facility_monitor",
            "svc": "facility-monitor",
            "hosts": ["fac-1"],
            "logs": {
                "power_loss": {
                    "lvl": "CRITICAL",
                    "msg": "facility power event dc={dc} affected_hosts={affected_hosts}",
                    "vars": {"dc": {"k": "ch", "v": ["use1a"]}, "affected_hosts": {"k": "str", "v": "host-id list summary"}},
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "api_read_ok",
                    "rpm": 240.0,
                    "emit": ["api_frontend.read_in", "metadata_node.lookup_ok", "storage_node.read_ok", "api_frontend.read_200"],
                    "latency_ms": [[1, 5], [2, 15], [2, 30], [1, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "api_write_ok",
                    "rpm": 90.0,
                    "emit": ["api_frontend.write_in", "metadata_node.lookup_ok", "storage_node.write_ok", "api_frontend.write_200"],
                    "latency_ms": [[1, 5], [2, 15], [3, 40], [1, 15]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "api_domain_ok",
                    "rpm": 6.0,
                    "emit": ["api_frontend.domain_in", "metadata_node.domain_mutation_ok", "api_frontend.domain_200"],
                    "latency_ms": [[1, 5], [20, 200], [1, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "api_read_ok",
                    "rpm": 240.0,
                    "emit": ["api_frontend.read_in", "metadata_node.lookup_ok", "storage_node.read_ok", "api_frontend.read_200"],
                    "latency_ms": [[1, 10], [5, 80], [5, 120], [1, 20]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "api_read_500",
                    "rpm": 240.0,
                    "emit": ["api_frontend.read_in", "api_frontend.read_500"],
                    "latency_ms": [[1, 10], [80, 900]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "api_write_ok",
                    "rpm": 90.0,
                    "emit": ["api_frontend.write_in", "metadata_node.lookup_ok", "storage_node.write_ok", "api_frontend.write_200"],
                    "latency_ms": [[1, 10], [5, 100], [10, 200], [1, 25]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "api_write_500",
                    "rpm": 90.0,
                    "emit": ["api_frontend.write_in", "api_frontend.write_500"],
                    "latency_ms": [[1, 10], [80, 1200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "api_domain_ok_slow",
                    "rpm": 6.0,
                    "emit": ["api_frontend.domain_in", "metadata_node.domain_mutation_ok", "api_frontend.domain_200"],
                    "latency_ms": [[1, 10], [200, 2500], [1, 30]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "api_domain_500",
                    "rpm": 6.0,
                    "emit": ["api_frontend.domain_in", "api_frontend.domain_500"],
                    "latency_ms": [[1, 10], [100, 1800]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "api_domain_throttled_503",
                    "rpm": 6.0,
                    "emit": ["api_frontend.domain_in", "api_frontend.domain_503_throttled"],
                    "latency_ms": [[1, 10], [1, 20]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "simpledb_lock_handshake_cascade_use1a",
        "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 50}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 20,
                        "rate_multipliers": {
                            "api_read_ok": 0.2,
                            "api_read_500": 0.8,
                            "api_write_ok": 0.0,
                            "api_write_500": 1.0,
                            "api_domain_ok_slow": 0.0,
                            "api_domain_500": 1.0,
                            "api_domain_throttled_503": 0.0,
                            "lock_service.queue_warn": 2.0,
                            "lock_service.deregister_batch": 3.0,
                            "metadata_node.handshake_timeout_pre": 1.5,
                            "metadata_node.handshake_timeout_post": 0.0,
                            "storage_node.handshake_timeout_pre": 1.5,
                            "storage_node.handshake_timeout_post": 0.0,
                            "control_plane_ops.throttle_status": 0.0,
                        },
                        "latency_multipliers": {
                            "api_read_500": {"p50": 1.2, "p95": 1.5},
                            "api_write_500": {"p50": 1.2, "p95": 1.6},
                            "api_domain_500": {"p50": 1.2, "p95": 1.6},
                        },
                        "one_shots": [{"ref": "facility_monitor.power_loss", "count": 1, "hosts": ["fac-1"]}],
                    },
                    {
                        "order": 2,
                        "at_min": 25,
                        "rate_multipliers": {
                            "api_read_ok": 0.0,
                            "api_read_500": 1.0,
                            "lock_service.queue_warn": 3.0,
                            "metadata_node.handshake_timeout_pre": 2.5,
                            "metadata_node.handshake_timeout_post": 0.0,
                            "storage_node.handshake_timeout_pre": 2.5,
                            "storage_node.handshake_timeout_post": 0.0,
                        },
                        "latency_multipliers": {
                            "api_read_500": {"p50": 1.4, "p95": 2.0},
                            "api_write_500": {"p50": 1.4, "p95": 2.0},
                            "api_domain_500": {"p50": 1.3, "p95": 1.9},
                        },
                        "one_shots": [
                            {"ref": "metadata_node.leave_cluster", "count": 2, "hosts": ["meta-2", "meta-3"]},
                            {"ref": "storage_node.leave_cluster", "count": 4, "hosts": ["stor-2", "stor-3", "stor-4", "stor-5"]},
                        ],
                    },
                    {
                        "order": 3,
                        "at_min": 35,
                        "rate_multipliers": {
                            "api_read_ok": 1.0,
                            "api_read_500": 0.0,
                            "api_write_ok": 1.0,
                            "api_write_500": 0.0,
                            "api_domain_500": 0.0,
                            "api_domain_throttled_503": 0.8,
                            "api_domain_ok_slow": 0.2,
                            "lock_service.queue_warn": 0.2,
                            "lock_service.deregister_batch": 0.2,
                            "metadata_node.handshake_timeout_pre": 0.0,
                            "metadata_node.handshake_timeout_post": 0.1,
                            "storage_node.handshake_timeout_pre": 0.0,
                            "storage_node.handshake_timeout_post": 0.1,
                            "control_plane_ops.throttle_status": 1.0,
                        },
                        "latency_multipliers": {"api_domain_ok_slow": {"p50": 4.0, "p95": 6.0}},
                        "one_shots": [
                            {"ref": "control_plane_ops.config_change", "count": 1, "hosts": ["ops-1"]},
                            {"ref": "metadata_node.process_start", "count": 2, "hosts": ["meta-2", "meta-3"]},
                            {"ref": "metadata_node.authorize_storage", "count": 6, "hosts": ["meta-1", "meta-2"]},
                            {"ref": "storage_node.rejoin_success", "count": 4, "hosts": ["stor-2", "stor-3", "stor-4", "stor-5"]},
                            {"ref": "control_plane_ops.throttle_enabled", "count": 1, "hosts": ["ops-1"]},
                        ],
                    },
                ]
            }
        },
    }
}

# -----------------------------
# Deterministic helpers
# -----------------------------

SEED = 1337


def stable_hash_int(*parts: Any, bits: int = 64) -> int:
    h = hashlib.sha256()
    h.update(str(SEED).encode("utf-8"))
    for p in parts:
        h.update(b"|")
        h.update(str(p).encode("utf-8"))
    digest = h.digest()
    nbytes = bits // 8
    return int.from_bytes(digest[:nbytes], "big", signed=False)


def stable_u01(*parts: Any) -> float:
    return (stable_hash_int(*parts, bits=32) + 0.5) / 2**32


def alloc_count(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    u = stable_u01("alloc", key)
    return base + (1 if u < frac else 0)


def iso8601_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    ms = dt.microsecond // 1000
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


# Acklam inverse normal CDF approximation (deterministic, no scipy)
def inv_norm_cdf(p: float) -> float:
    if p <= 0.0:
        return -float("inf")
    if p >= 1.0:
        return float("inf")

    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02, 1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]

    plow = 0.02425
    phigh = 1 - plow

    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        return num / den
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        num = -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        return num / den

    q = p - 0.5
    r = q * q
    num = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    den = (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    return num / den


def lognormal_sample_ms(p50: float, p95: float, key: str, u_low: float = 0.5, u_high: float = 0.92, soft_cap_mult: float = 3.0) -> int:
    p50 = max(0.1, float(p50))
    p95 = max(p50 * 1.001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.645
    u = u_low + (u_high - u_low) * stable_u01("lnu", key)
    z = inv_norm_cdf(u)
    x = math.exp(mu + sigma * z)
    cap = soft_cap_mult * p95
    if x > cap:
        x = cap + (x - cap) * 0.05
    return int(max(1, round(x)))


def schedule_times(start: datetime, end: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    duration_ms = max(1.0, (end - start).total_seconds() * 1000.0)
    step_ms = duration_ms / count
    max_jitter = min(200.0, 0.2 * step_ms)
    out = []
    for i in range(count):
        base_offset = (i + 0.5) * step_ms
        jitter = (stable_u01("jit", key, i) - 0.5) * 2.0 * max_jitter
        t = start + timedelta(milliseconds=base_offset + jitter)
        if t < start:
            t = start
        if t >= end:
            t = end - timedelta(milliseconds=1)
        out.append(t)
    return out


def parse_ref(ref: str) -> Tuple[str, str]:
    comp, logid = ref.split(".", 1)
    return comp, logid


def format_value(v: Any) -> str:
    if isinstance(v, float):
        return f"{v:.3f}"
    return str(v)


def render_message(template: str, values: Dict[str, Any]) -> str:
    vals = {k: format_value(v) for k, v in values.items()}
    return template.format(**vals)


def make_uuid_deterministic(key: str) -> str:
    n = stable_hash_int("uuid", key, bits=128)
    u = uuid.UUID(int=n)
    return str(u)


def make_trace_id(key: str) -> str:
    return ""


# -----------------------------
# Build indices
# -----------------------------

COMP: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

LOG_TPL: Dict[str, Dict[str, Any]] = {}
for c in SYSTEM["components"]:
    for lid, tpl in c["logs"].items():
        LOG_TPL[f"{c['id']}.{lid}"] = {"component": c["id"], "log_id": lid, **tpl}

FLOWS_BY_STATE: Dict[str, Dict[str, Dict[str, Any]]] = {"n": {}, "f": {}}
for st in ["n", "f"]:
    for f in SYSTEM["flows"][st]["req"]:
        FLOWS_BY_STATE[st][f["id"]] = f


# -----------------------------
# Scenario controls -> failure intervals
# -----------------------------

@dataclass(frozen=True)
class Interval:
    state: str
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    lat_mult: Dict[str, Dict[str, float]]


def build_failure_intervals() -> Tuple[List[Interval], List[Dict[str, Any]]]:
    sc = SCENARIO["scenario"]
    f_start = sc["time"]["phases"]["f"]["start_min"]
    f_end = sc["time"]["phases"]["f"]["end_min"]
    events = sorted(sc["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = sorted({f_start, f_end, *[e["at_min"] for e in events]})
    if boundaries[0] != f_start:
        boundaries = [f_start] + boundaries
    if boundaries[-1] != f_end:
        boundaries = boundaries + [f_end]

    by_time: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        by_time.setdefault(e["at_min"], []).append(e)

    intervals: List[Interval] = []
    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}

    for i in range(len(boundaries) - 1):
        t0 = boundaries[i]
        t1 = boundaries[i + 1]
        for e in by_time.get(t0, []):
            for k, v in e.get("rate_multipliers", {}).items():
                active_rate[k] = float(v)
            for fid, mm in e.get("latency_multipliers", {}).items():
                active_lat[fid] = {"p50": float(mm["p50"]), "p95": float(mm["p95"])}
        intervals.append(Interval(state="f", start_min=t0, end_min=t1, rate_mult=dict(active_rate), lat_mult=dict(active_lat)))

    return intervals, events


failure_intervals, failure_events = build_failure_intervals()

normal_interval = Interval(state="n", start_min=0, end_min=SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"], rate_mult={}, lat_mult={})

THROTTLE_LIMIT_RPS = 0.030  # within [0.01, 0.05]

# -----------------------------
# Emission
# -----------------------------

def choose_host(component_id: str, key: str) -> str:
    hosts = COMP[component_id].get("hosts", [])
    if not hosts:
        return ""
    idx = stable_hash_int("host", component_id, key, bits=32) % len(hosts)
    return hosts[idx]


def choose_ch(choices: List[Any], key: str) -> Any:
    idx = stable_hash_int("ch", key, bits=32) % len(choices)
    return choices[idx]


def choose_int(lo: int, hi: int, key: str) -> int:
    if lo == hi:
        return int(lo)
    u = stable_u01("i", key)
    return int(lo + math.floor(u * (hi - lo + 1)))


def choose_float(lo: float, hi: float, key: str) -> float:
    if lo == hi:
        return float(lo)
    u = stable_u01("f", key)
    return float(lo + u * (hi - lo))


def emit_row(rows: List[Dict[str, Any]], ts: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append(
        {"ts": ts, "timestamp": iso8601_ms(ts), "level": level, "message": message, "trace_id": trace_id, "service": service, "host": host}
    )


def build_background_vars(component_id: str, log_id: str, state: str, host: str, idx: int, interval_key: str) -> Dict[str, Any]:
    ref = f"{component_id}.{log_id}"
    tpl = LOG_TPL[ref]
    vars_def = tpl.get("vars", {})
    out: Dict[str, Any] = {}

    if component_id == "metadata_node" and log_id == "handshake_ok":
        out["epoch"] = choose_int(1000, 5000, f"{interval_key}|{host}|{idx}|epoch")
        out["latency_ms"] = choose_int(5, 80, f"{interval_key}|{host}|{idx}|lat")
    elif component_id == "storage_node" and log_id == "handshake_ok":
        out["epoch"] = choose_int(1000, 5000, f"{interval_key}|{host}|{idx}|epoch")
        out["latency_ms"] = choose_int(5, 90, f"{interval_key}|{host}|{idx}|lat")
    elif component_id in ("metadata_node", "storage_node") and log_id.startswith("handshake_timeout_"):
        attempt = 1 + (idx % 6)
        out["attempt"] = attempt
        d_lo, d_hi = vars_def["deadline_ms"]["v"]
        deadline = choose_int(int(d_lo), int(d_hi), f"{interval_key}|{host}|{idx}|deadline")
        out["deadline_ms"] = deadline
        e_lo, e_hi = vars_def["elapsed_ms"]["v"]
        elapsed_min = max(int(e_lo), int(deadline))
        elapsed = choose_int(elapsed_min, int(e_hi), f"{interval_key}|{host}|{idx}|elapsed")
        out["elapsed_ms"] = elapsed
    elif component_id == "lock_service" and log_id == "stats":
        out["dc"] = choose_ch(vars_def["dc"]["v"], f"{interval_key}|{host}|{idx}|dc")
        out["inflight"] = choose_int(100, 5000, f"{interval_key}|{host}|{idx}|inflight")
        out["p95_ms"] = choose_int(20, 120, f"{interval_key}|{host}|{idx}|p95")
    elif component_id == "lock_service" and log_id == "queue_warn":
        out["dc"] = choose_ch(vars_def["dc"]["v"], f"{interval_key}|{host}|{idx}|dc")
        out["inflight"] = choose_int(5000, 70000, f"{interval_key}|{host}|{idx}|inflight")
        out["p95_ms"] = choose_int(800, 4500, f"{interval_key}|{host}|{idx}|p95")
    elif component_id == "lock_service" and log_id == "deregister_batch":
        out["dc"] = choose_ch(vars_def["dc"]["v"], f"{interval_key}|{idx}|dc")
        out["count"] = choose_int(50, 450, f"{interval_key}|{idx}|count")
    elif component_id == "control_plane_ops" and log_id == "throttle_status":
        out["inflight"] = choose_int(0, 12, f"{interval_key}|{idx}|inflight")
        out["rejected"] = choose_int(3, 9, f"{interval_key}|{idx}|rejected")
    else:
        for k, d in vars_def.items():
            kk = d["k"]
            vv = d["v"]
            if kk == "ch":
                out[k] = choose_ch(vv, f"{interval_key}|{host}|{idx}|{k}")
            elif kk == "i":
                out[k] = choose_int(int(vv[0]), int(vv[1]), f"{interval_key}|{host}|{idx}|{k}")
            elif kk == "f":
                out[k] = choose_float(float(vv[0]), float(vv[1]), f"{interval_key}|{host}|{idx}|{k}")
            elif kk == "uuid":
                out[k] = make_uuid_deterministic(f"{interval_key}|{host}|{idx}|{k}")
            elif kk == "str":
                out[k] = str(vv)
            else:
                out[k] = str(vv)

    for k, d in vars_def.items():
        if k in out:
            continue
        kk = d["k"]
        vv = d["v"]
        if kk == "ch":
            out[k] = choose_ch(vv, f"{interval_key}|{host}|{idx}|{k}")
        elif kk == "i":
            out[k] = choose_int(int(vv[0]), int(vv[1]), f"{interval_key}|{host}|{idx}|{k}")
        elif kk == "f":
            out[k] = choose_float(float(vv[0]), float(vv[1]), f"{interval_key}|{host}|{idx}|{k}")
        elif kk == "uuid":
            out[k] = make_uuid_deterministic(f"{interval_key}|{host}|{idx}|{k}")
        elif kk == "str":
            out[k] = str(vv)
        else:
            out[k] = str(vv)

    return out


def apply_latency_multiplier(pair: List[float], mult: Optional[Dict[str, float]]) -> Tuple[float, float]:
    p50, p95 = float(pair[0]), float(pair[1])
    if mult is None:
        return p50, p95
    return p50 * float(mult.get("p50", 1.0)), p95 * float(mult.get("p95", 1.0))


def cap_to_domain_int(value: int, domain: List[int]) -> int:
    lo, hi = int(domain[0]), int(domain[1])
    return max(lo, min(hi, int(value)))


def simulate_flow_instance(
    rows: List[Dict[str, Any]],
    flow: Dict[str, Any],
    state: str,
    start_ts: datetime,
    interval_key: str,
    latency_mult: Optional[Dict[str, float]],
) -> None:
    emit_refs = flow["emit"]
    latency_pairs = flow["latency_ms"]
    assert len(emit_refs) == len(latency_pairs)

    flow_id = flow["id"]
    request_id = make_uuid_deterministic(f"{interval_key}|{flow_id}|{iso8601_ms(start_ts)}|request_id")

    api_host = choose_host("api_frontend", request_id)
    meta_host = choose_host("metadata_node", request_id)
    stor_host = choose_host("storage_node", request_id)

    domain = choose_ch(COMP["api_frontend"]["logs"]["read_in"]["vars"]["domain"]["v"], f"{request_id}|domain")
    op = choose_ch(COMP["api_frontend"]["logs"]["domain_in"]["vars"]["op"]["v"], f"{request_id}|op")

    all_stor = COMP["storage_node"]["hosts"]
    others = [h for h in all_stor if h != stor_host]
    r1 = others[stable_hash_int("rep", request_id, 1, bits=32) % len(others)]
    others2 = [h for h in others if h != r1]
    r2 = others2[stable_hash_int("rep", request_id, 2, bits=32) % len(others2)]
    replicas = ",".join([stor_host, r1, r2])

    deltas: List[int] = []
    for j, pair in enumerate(latency_pairs):
        p50, p95 = apply_latency_multiplier(pair, latency_mult)
        if flow_id == "api_domain_ok_slow":
            d = lognormal_sample_ms(p50, p95, f"{interval_key}|{flow_id}|{request_id}|d{j}", u_low=0.50, u_high=0.85, soft_cap_mult=2.5)
        else:
            d = lognormal_sample_ms(p50, p95, f"{interval_key}|{flow_id}|{request_id}|d{j}", u_low=0.50, u_high=0.92, soft_cap_mult=3.0)
        deltas.append(d)

    if "metadata_node.domain_mutation_ok" in emit_refs:
        idx_mut = emit_refs.index("metadata_node.domain_mutation_ok")
        tpl_mut = LOG_TPL["metadata_node.domain_mutation_ok"]
        mut_dom = tpl_mut["vars"]["latency_ms"]["v"]
        deltas[idx_mut] = cap_to_domain_int(deltas[idx_mut], mut_dom)

    resp_ref = emit_refs[-1]
    if resp_ref in ("api_frontend.read_200", "api_frontend.read_500", "api_frontend.write_200", "api_frontend.write_500", "api_frontend.domain_200", "api_frontend.domain_500", "api_frontend.domain_503_throttled"):
        resp_tpl = LOG_TPL[resp_ref]
        lat_dom = resp_tpl.get("vars", {}).get("latency_ms", {}).get("v")
        if lat_dom is not None and len(deltas) >= 2:
            observed = sum(deltas[1:])
            max_allowed = int(lat_dom[1])
            min_allowed = int(lat_dom[0])
            if observed > max_allowed:
                scale = max_allowed / float(observed)
                new_post = [max(1, int(round(d * scale))) for d in deltas[1:]]
                new_obs = sum(new_post)
                if new_obs > max_allowed:
                    new_post[-1] = max(1, new_post[-1] - (new_obs - max_allowed))
                elif new_obs < max_allowed:
                    new_post[-1] = max(1, new_post[-1] + (max_allowed - new_obs))
                deltas = [deltas[0]] + new_post
            elif observed < min_allowed:
                scale = min(1.5, min_allowed / float(max(1, observed)))
                new_post = [max(1, int(round(d * scale))) for d in deltas[1:]]
                deltas = [deltas[0]] + new_post

    trace_id = make_trace_id(request_id)
    t = start_ts
    t_recv: Optional[datetime] = None

    for j, ref in enumerate(emit_refs):
        t = t + timedelta(milliseconds=int(deltas[j]))
        comp_id, _log_id = parse_ref(ref)
        tpl = LOG_TPL[ref]
        svc = COMP[comp_id]["svc"]
        if comp_id == "api_frontend":
            host = api_host
        elif comp_id == "metadata_node":
            host = meta_host
        elif comp_id == "storage_node":
            host = stor_host
        else:
            host = choose_host(comp_id, request_id)

        vals: Dict[str, Any] = {}
        vars_def = tpl.get("vars", {})

        for k, d in vars_def.items():
            if k == "request_id":
                vals[k] = request_id
            elif k == "domain":
                vals[k] = domain
            elif k == "op":
                vals[k] = op
            elif k == "replicas":
                vals[k] = replicas
            elif k == "limit_rps":
                vals[k] = THROTTLE_LIMIT_RPS
            elif k in ("bytes", "items"):
                lo, hi = d["v"]
                vals[k] = choose_int(int(lo), int(hi), f"{request_id}|{ref}|{k}")
            elif k == "latency_ms":
                pass
            else:
                kk = d["k"]
                vv = d["v"]
                if kk == "ch":
                    vals[k] = choose_ch(vv, f"{request_id}|{ref}|{k}")
                elif kk == "i":
                    vals[k] = choose_int(int(vv[0]), int(vv[1]), f"{request_id}|{ref}|{k}")
                elif kk == "f":
                    vals[k] = choose_float(float(vv[0]), float(vv[1]), f"{request_id}|{ref}|{k}")
                elif kk == "uuid":
                    vals[k] = make_uuid_deterministic(f"{request_id}|{ref}|{k}")
                elif kk == "str":
                    vals[k] = str(vv)
                else:
                    vals[k] = str(vv)

        if ref in ("api_frontend.read_in", "api_frontend.write_in", "api_frontend.domain_in"):
            t_recv = t

        if "latency_ms" in vars_def:
            dom = vars_def["latency_ms"]["v"]
            if ref == "metadata_node.domain_mutation_ok":
                vals["latency_ms"] = cap_to_domain_int(int(deltas[j]), dom)
            else:
                if t_recv is None:
                    obs = int(sum(deltas))
                else:
                    obs = int(round((t - t_recv).total_seconds() * 1000.0))
                vals["latency_ms"] = cap_to_domain_int(obs, dom)

        msg = render_message(tpl["msg"], vals)
        emit_row(rows, t, tpl["lvl"], msg, trace_id, svc, host)


def simulate_background(rows: List[Dict[str, Any]], interval: Interval, start_dt: datetime, end_dt: datetime) -> None:
    duration_min = (end_dt - start_dt).total_seconds() / 60.0
    state = interval.state
    for comp_id, comp in COMP.items():
        beh = comp.get("beh", {}).get(state, {}).get("emit", [])
        if not beh:
            continue
        for src in beh:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            mult_key = f"{comp_id}.{log_id}"
            rate_mult = 1.0
            if state == "f":
                rate_mult = float(interval.rate_mult.get(mult_key, 1.0))
            eff_per_min = per_min * rate_mult
            ref = f"{comp_id}.{log_id}"
            tpl = LOG_TPL[ref]
            svc = comp["svc"]

            if scope == "per_host":
                for h in comp.get("hosts", []):
                    expected = eff_per_min * duration_min
                    cnt = alloc_count(expected, f"bg|{interval.start_min}-{interval.end_min}|{mult_key}|{h}")
                    times = schedule_times(start_dt, end_dt, cnt, f"bg|{interval.start_min}-{interval.end_min}|{mult_key}|{h}")
                    for i, ts in enumerate(times):
                        vals = build_background_vars(comp_id, log_id, state, h, i, f"{interval.start_min}-{interval.end_min}")
                        msg = render_message(tpl["msg"], vals)
                        emit_row(rows, ts, tpl["lvl"], msg, "", svc, h)
            elif scope == "global":
                expected = eff_per_min * duration_min
                cnt = alloc_count(expected, f"bg|{interval.start_min}-{interval.end_min}|{mult_key}|global")
                times = schedule_times(start_dt, end_dt, cnt, f"bg|{interval.start_min}-{interval.end_min}|{mult_key}|global")
                hosts = comp.get("hosts", [])
                for i, ts in enumerate(times):
                    h = hosts[i % len(hosts)] if hosts else ""
                    vals = build_background_vars(comp_id, log_id, state, h, i, f"{interval.start_min}-{interval.end_min}")
                    msg = render_message(tpl["msg"], vals)
                    emit_row(rows, ts, tpl["lvl"], msg, "", svc, h)
            else:
                expected = eff_per_min * duration_min
                cnt = alloc_count(expected, f"bg|{interval.start_min}-{interval.end_min}|{mult_key}|global2")
                times = schedule_times(start_dt, end_dt, cnt, f"bg|{interval.start_min}-{interval.end_min}|{mult_key}|global2")
                hosts = comp.get("hosts", [])
                for i, ts in enumerate(times):
                    h = hosts[i % len(hosts)] if hosts else ""
                    vals = build_background_vars(comp_id, log_id, state, h, i, f"{interval.start_min}-{interval.end_min}")
                    msg = render_message(tpl["msg"], vals)
                    emit_row(rows, ts, tpl["lvl"], msg, "", svc, h)


def simulate_flows(rows: List[Dict[str, Any]], interval: Interval, start_dt: datetime, end_dt: datetime) -> None:
    duration_min = (end_dt - start_dt).total_seconds() / 60.0
    state = interval.state
    for flow_id, flow in FLOWS_BY_STATE[state].items():
        rpm = float(flow["rpm"])
        rate_mult = 1.0
        if state == "f":
            rate_mult = float(interval.rate_mult.get(flow_id, 1.0))
        eff_rpm = rpm * rate_mult
        expected = eff_rpm * duration_min
        cnt = alloc_count(expected, f"flow|{state}|{interval.start_min}-{interval.end_min}|{flow_id}")
        starts = schedule_times(start_dt, end_dt, cnt, f"flow|{state}|{interval.start_min}-{interval.end_min}|{flow_id}")
        lat_mult = interval.lat_mult.get(flow_id) if state == "f" else None
        interval_key = f"{state}|{interval.start_min}-{interval.end_min}"
        for st_ts in starts:
            simulate_flow_instance(rows, flow, state, st_ts, interval_key, lat_mult)


def emit_one_shots(rows: List[Dict[str, Any]], base_dt: datetime) -> None:
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for e in events:
        at_min = int(e["at_min"])
        at_ts = base_dt + timedelta(minutes=at_min)
        for shot_idx, s in enumerate(e.get("one_shots", [])):
            ref = s["ref"]
            count = int(s["count"])
            allowed_hosts = s.get("hosts", [])
            comp_id, _log_id = parse_ref(ref)
            tpl = LOG_TPL[ref]
            svc = COMP[comp_id]["svc"]

            for i in range(count):
                jitter_ms = int(round(900.0 * stable_u01("oneshot", ref, at_min, shot_idx, i)))
                ts = at_ts + timedelta(milliseconds=jitter_ms)
                host = allowed_hosts[i % len(allowed_hosts)] if allowed_hosts else choose_host(comp_id, f"{ref}|{at_min}|{i}")

                vals_def = tpl.get("vars", {})
                vals: Dict[str, Any] = {}

                if ref == "facility_monitor.power_loss":
                    vals["dc"] = "use1a"
                    vals["affected_hosts"] = "stor-2,stor-3,stor-4,stor-5"
                elif ref == "control_plane_ops.config_change":
                    vals["old_ms"] = 300
                    vals["new_ms"] = 1000
                elif ref == "control_plane_ops.throttle_enabled":
                    vals["limit_rps"] = THROTTLE_LIMIT_RPS
                elif ref == "metadata_node.process_start":
                    vals["version"] = "1.42.0"
                elif ref == "metadata_node.authorize_storage":
                    stor_choices = COMP["metadata_node"]["logs"]["authorize_storage"]["vars"]["storage_id"]["v"]
                    dom_choices = COMP["metadata_node"]["logs"]["authorize_storage"]["vars"]["domain"]["v"]
                    vals["storage_id"] = stor_choices[i % len(stor_choices)]
                    vals["domain"] = choose_ch(dom_choices, f"oneshot|{ref}|{at_min}|{i}|domain")
                    vals["epoch"] = choose_int(1000, 6000, f"oneshot|{ref}|{at_min}|{i}|epoch")
                elif ref == "storage_node.rejoin_success":
                    vals["epoch"] = choose_int(1000, 6000, f"oneshot|{ref}|{at_min}|{i}|epoch")
                elif ref in ("metadata_node.leave_cluster", "storage_node.leave_cluster"):
                    vals["timeouts"] = choose_int(3, 10, f"oneshot|{ref}|{at_min}|{i}|timeouts")

                for k, d in vals_def.items():
                    if k in vals:
                        continue
                    kk = d["k"]
                    vv = d["v"]
                    if kk == "ch":
                        vals[k] = choose_ch(vv, f"oneshot|{ref}|{at_min}|{i}|{k}")
                    elif kk == "i":
                        vals[k] = choose_int(int(vv[0]), int(vv[1]), f"oneshot|{ref}|{at_min}|{i}|{k}")
                    elif kk == "f":
                        vals[k] = choose_float(float(vv[0]), float(vv[1]), f"oneshot|{ref}|{at_min}|{i}|{k}")
                    elif kk == "uuid":
                        vals[k] = make_uuid_deterministic(f"oneshot|{ref}|{at_min}|{i}|{k}")
                    elif kk == "str":
                        vals[k] = str(vv)
                    else:
                        vals[k] = str(vv)

                msg = render_message(tpl["msg"], vals)
                emit_row(rows, ts, tpl["lvl"], msg, "", svc, host)


def main() -> None:
    # Explicitly seed RNGs for verifier expectations (even though the simulation uses stable hashes).
    random.seed(SEED)
    np.random.seed(SEED)

    base_dt = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    rows: List[Dict[str, Any]] = []

    n_start = base_dt + timedelta(minutes=normal_interval.start_min)
    n_end = base_dt + timedelta(minutes=normal_interval.end_min)
    simulate_background(rows, normal_interval, n_start, n_end)
    simulate_flows(rows, normal_interval, n_start, n_end)

    for interval in failure_intervals:
        f_start = base_dt + timedelta(minutes=interval.start_min)
        f_end = base_dt + timedelta(minutes=interval.end_min)
        simulate_background(rows, interval, f_start, f_end)
        simulate_flows(rows, interval, f_start, f_end)

    emit_one_shots(rows, base_dt)

    df = pd.DataFrame(rows)
    df = df.sort_values(["ts", "service", "host", "level", "message"], kind="mergesort").reset_index(drop=True)
    df_out = df[["timestamp", "level", "message", "trace_id", "service", "host"]].copy()

    assert list(df_out.columns) == ["timestamp", "level", "message", "trace_id", "service", "host"]
    assert 20000 <= len(df_out) <= 100000, f"row_count={len(df_out)} outside target"
    ts_parsed = pd.to_datetime(df_out["timestamp"], utc=True, format="%Y-%m-%dT%H:%M:%S.%fZ", errors="raise")
    assert ts_parsed.is_monotonic_increasing

    df_out.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
