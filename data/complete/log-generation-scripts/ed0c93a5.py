import re
import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional
from collections import defaultdict

import numpy as np
import pandas as pd

# ----------------------------
# Embedded normalized model data
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "bigquery_writeapi_us_multi"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "edge_lb",
            "svc": "edge-lb",
            "hosts": ["lb-usm-1", "lb-usm-2", "lb-usm-3"],
            "logs": {
                "access_ok": {
                    "lvl": "INFO",
                    "msg": "writeapi request complete status={status} backend={backend} dur_ms={dur_ms} peer={peer_ip}",
                    "vars": {
                        "status": {"k": "ch", "v": ["200"]},
                        "backend": {"k": "str", "v": "writeapi-fe-<id> (id~1..100)"},
                        "dur_ms": {"k": "i", "v": [1, 8000]},
                        "peer_ip": {"k": "ip", "v": None},
                    },
                },
                "access_err": {
                    "lvl": "INFO",
                    "msg": "writeapi request complete status={status} backend={backend} dur_ms={dur_ms} peer={peer_ip}",
                    "vars": {
                        "status": {"k": "ch", "v": ["503", "504"]},
                        "backend": {"k": "str", "v": "writeapi-fe-<id> (id~1..100)"},
                        "dur_ms": {"k": "i", "v": [10, 8000]},
                        "peer_ip": {"k": "ip", "v": None},
                    },
                },
                "conn_failure": {
                    "lvl": "WARN",
                    "msg": "writeapi connection failure backend={backend} reason={reason} waited_ms={waited_ms} peer={peer_ip}",
                    "vars": {
                        "backend": {"k": "str", "v": "writeapi-fe-<id> (id~1..100)"},
                        "reason": {"k": "ch", "v": ["timeout", "reset_by_peer", "handshake_failed"]},
                        "waited_ms": {"k": "i", "v": [50, 8000]},
                        "peer_ip": {"k": "ip", "v": None},
                    },
                },
                "pool_health": {
                    "lvl": "INFO",
                    "msg": "backend_pool healthy={healthy} stuck={stuck} inflight_conns={inflight}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "healthy": {"k": "i", "v": [15, 20]},
                            "stuck": {"k": "i", "v": [0, 0]},
                            "inflight": {"k": "i", "v": [5000, 20000]},
                        },
                        "f": {
                            "healthy": {"k": "i", "v": [5, 15]},
                            "stuck": {"k": "i", "v": [1, 5]},
                            "inflight": {"k": "i", "v": [15000, 90000]},
                        },
                    },
                },
                "pool_health_postcap": {
                    "lvl": "INFO",
                    "msg": "backend_pool healthy={healthy} stuck={stuck} inflight_conns={inflight}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "healthy": {"k": "i", "v": [15, 20]},
                            "stuck": {"k": "i", "v": [0, 0]},
                            "inflight": {"k": "i", "v": [5000, 20000]},
                        },
                        "f": {
                            "healthy": {"k": "i", "v": [20, 80]},
                            "stuck": {"k": "i", "v": [1, 20]},
                            "inflight": {"k": "i", "v": [20000, 120000]},
                        },
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "pool_health", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "pool_health", "per_min": 1.0, "scope": "per_host"},
                        {"id": "pool_health_postcap", "per_min": 1.0, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "writeapi_frontend",
            "svc": "bq-writeapi-frontend",
            "hosts": [f"writeapi-fe-{i}" for i in range(1, 101)],
            "logs": {
                "append_begin": {
                    "lvl": "INFO",
                    "msg": "AppendRows begin stream_id={stream_id} project={project} table={dataset}.{table} inflight={inflight}",
                    "vars": {
                        "stream_id": {"k": "hex", "v": 16},
                        "project": {"k": "ch", "v": ["analytics-prod", "etl-prod", "customer-x"]},
                        "dataset": {"k": "ch", "v": ["events", "logs"]},
                        "table": {"k": "ch", "v": ["clicks", "impressions", "raw"]},
                    },
                    "state_vars": {
                        "n": {"inflight": {"k": "i", "v": [1, 500]}},
                        "f": {"inflight": {"k": "i", "v": [200, 8000]}},
                    },
                },
                "append_ok": {
                    "lvl": "INFO",
                    "msg": "AppendRows ok stream_id={stream_id} rows={rows} dur_ms={dur_ms}",
                    "vars": {
                        "stream_id": {"k": "hex", "v": 16},
                        "rows": {"k": "i", "v": [1, 20000]},
                        "dur_ms": {"k": "i", "v": [2, 4000]},
                    },
                },
                "append_unavail": {
                    "lvl": "ERROR",
                    "msg": "AppendRows error stream_id={stream_id} grpc_status=UNAVAILABLE err={err} dur_ms={dur_ms}",
                    "vars": {
                        "stream_id": {"k": "hex", "v": 16},
                        "err": {"k": "ch", "v": ["frontend_overloaded", "rpc_deadlock", "queue_full"]},
                        "dur_ms": {"k": "i", "v": [50, 8000]},
                    },
                },
                "rpc_deadlock_watchdog": {
                    "lvl": "ERROR",
                    "msg": "rpc watchdog blocked_handlers={blocked} goroutines={goroutines}",
                    "vars": {
                        "blocked": {"k": "i", "v": [1, 400]},
                        "goroutines": {"k": "i", "v": [200, 8000]},
                    },
                },
                "fe_health": {
                    "lvl": "INFO",
                    "msg": "health ok={ok} cpu_pct={cpu_pct} mem_gb={mem_gb} active_streams={active_streams}",
                    "vars": {"ok": {"k": "ch", "v": ["true", "false"]}},
                    "state_vars": {
                        "n": {
                            "cpu_pct": {"k": "i", "v": [5, 60]},
                            "mem_gb": {"k": "i", "v": [8, 16]},
                            "active_streams": {"k": "i", "v": [50, 800]},
                        },
                        "f": {
                            "cpu_pct": {"k": "i", "v": [30, 95]},
                            "mem_gb": {"k": "i", "v": [16, 64]},
                            "active_streams": {"k": "i", "v": [500, 8000]},
                        },
                    },
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "fe_health", "per_min": 0.5, "scope": "per_host"},
                        {"id": "rpc_deadlock_watchdog", "per_min": 0.0, "scope": "per_host"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "fe_health", "per_min": 1.0, "scope": "per_host"},
                        {"id": "rpc_deadlock_watchdog", "per_min": 0.15, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "insertall_api",
            "svc": "bq-insertall",
            "hosts": ["insertall-1", "insertall-2"],
            "logs": {
                "insert_req": {
                    "lvl": "INFO",
                    "msg": "InsertAll begin project={project} table={dataset}.{table} rows={rows}",
                    "vars": {
                        "project": {"k": "ch", "v": ["analytics-prod", "etl-prod", "customer-x"]},
                        "dataset": {"k": "ch", "v": ["events", "logs"]},
                        "table": {"k": "ch", "v": ["clicks", "impressions", "raw"]},
                        "rows": {"k": "i", "v": [1, 500]},
                    },
                },
                "insert_ok": {
                    "lvl": "INFO",
                    "msg": "InsertAll ok rows={rows} dur_ms={dur_ms}",
                    "vars": {"rows": {"k": "i", "v": [1, 500]}, "dur_ms": {"k": "i", "v": [5, 2000]}},
                },
                "insert_fail": {
                    "lvl": "WARN",
                    "msg": "InsertAll failed status={status} reason={reason} dur_ms={dur_ms}",
                    "vars": {
                        "status": {"k": "i", "v": [503, 504]},
                        "reason": {"k": "ch", "v": ["backend_overloaded", "timeout"]},
                        "dur_ms": {"k": "i", "v": [50, 5000]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "storage_backend",
            "svc": "bq-storage-backend",
            "hosts": ["storage-1", "storage-2", "storage-3"],
            "logs": {
                "ingest_metric": {
                    "lvl": "INFO",
                    "msg": "storage ingest qps={qps} err_rate={err_rate}",
                    "vars": {},
                    "state_vars": {
                        "n": {"qps": {"k": "i", "v": [2000, 6000]}, "err_rate": {"k": "f", "v": [0.0, 0.01]}},
                        "f": {"qps": {"k": "i", "v": [2500, 9000]}, "err_rate": {"k": "f", "v": [0.0, 0.02]}},
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "ingest_metric", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "ingest_metric", "per_min": 1.0, "scope": "global"}]},
            },
        },
        {
            "id": "autoscaler",
            "svc": "autoscaler",
            "hosts": ["as-1"],
            "logs": {
                "memory_increase": {
                    "lvl": "INFO",
                    "msg": "autoscaler increased memory target={target} from_gb={from_gb} to_gb={to_gb}",
                    "vars": {
                        "target": {"k": "ch", "v": ["writeapi-frontend"]},
                        "from_gb": {"k": "i", "v": [16, 32]},
                        "to_gb": {"k": "i", "v": [32, 64]},
                    },
                },
                "scale_out": {
                    "lvl": "INFO",
                    "msg": "autoscaler scaled target={target} from={from} to={to} instances",
                    "vars": {
                        "target": {"k": "ch", "v": ["writeapi-frontend"]},
                        "from": {"k": "i", "v": [10, 18]},
                        "to": {"k": "i", "v": [15, 20]},
                    },
                },
                "scale_out_postcap": {
                    "lvl": "INFO",
                    "msg": "autoscaler scaled target={target} from={from} to={to} instances",
                    "vars": {
                        "target": {"k": "ch", "v": ["writeapi-frontend"]},
                        "from": {"k": "i", "v": [20, 70]},
                        "to": {"k": "i", "v": [60, 100]},
                    },
                },
                "tick": {"lvl": "DEBUG", "msg": "autoscaler tick pending_actions={pending}", "vars": {"pending": {"k": "i", "v": [0, 5]}}},
            },
            "beh": {"n": {"emit": [{"id": "tick", "per_min": 0.2, "scope": "global"}]}, "f": {"emit": [{"id": "tick", "per_min": 0.2, "scope": "global"}]}},
        },
        {
            "id": "control_plane",
            "svc": "control-plane",
            "hosts": ["cp-1"],
            "logs": {
                "set_max_instances": {
                    "lvl": "INFO",
                    "msg": "updated target={target} max_instances {from}->{to}",
                    "vars": {"target": {"k": "ch", "v": ["writeapi-frontend"]}, "from": {"k": "i", "v": [20, 20]}, "to": {"k": "i", "v": [100, 100]}},
                },
                "restart_instance": {
                    "lvl": "WARN",
                    "msg": "restarting target={target} instance={instance} reason={reason}",
                    "vars": {"target": {"k": "ch", "v": ["writeapi-frontend"]}, "instance": {"k": "str", "v": "writeapi-fe-<id> (id~1..100)"}, "reason": {"k": "ch", "v": ["deadlock_suspected", "stuck_process"]}},
                },
                "audit_tick": {"lvl": "DEBUG", "msg": "control-plane audit tick ops_in_flight={ops}", "vars": {"ops": {"k": "i", "v": [0, 20]}}},
            },
            "beh": {"n": {"emit": [{"id": "audit_tick", "per_min": 0.2, "scope": "global"}]}, "f": {"emit": [{"id": "audit_tick", "per_min": 0.2, "scope": "global"}]}},
        },
        {
            "id": "monitoring",
            "svc": "monitoring",
            "hosts": ["mon-1"],
            "logs": {
                "synthetic_check": {"lvl": "INFO", "msg": "synthetic writeapi check status={status} dur_ms={dur_ms}", "vars": {"status": {"k": "ch", "v": ["OK", "UNAVAILABLE"]}, "dur_ms": {"k": "i", "v": [10, 8000]}}},
                "alert_firing": {
                    "lvl": "CRITICAL",
                    "msg": "alert={name} region={region} value={value} threshold={threshold}",
                    "vars": {"name": {"k": "ch", "v": ["writeapi_streaming_error_rate"]}, "region": {"k": "ch", "v": ["us-multi"]}, "value": {"k": "f", "v": [0.03, 0.6]}, "threshold": {"k": "f", "v": [0.02, 0.02]}},
                },
            },
            "beh": {"n": {"emit": [{"id": "synthetic_check", "per_min": 0.2, "scope": "global"}]}, "f": {"emit": [{"id": "synthetic_check", "per_min": 0.5, "scope": "global"}]}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {"id": "write_append_success_n", "rpm": 350.0, "emit": ["writeapi_frontend.append_begin", "writeapi_frontend.append_ok", "edge_lb.access_ok"], "latency_ms": [[1, 6], [15, 120], [20, 180]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "insertall_success_n", "rpm": 70.0, "emit": ["insertall_api.insert_req", "insertall_api.insert_ok"], "latency_ms": [[1, 4], [20, 250]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
            ]
        },
        "f": {
            "req": [
                {"id": "write_append_success_f", "rpm": 220.0, "emit": ["writeapi_frontend.append_begin", "writeapi_frontend.append_ok", "edge_lb.access_ok"], "latency_ms": [[2, 10], [120, 2000], [150, 2500]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "write_append_fail_unavailable_f", "rpm": 220.0, "emit": ["writeapi_frontend.append_begin", "writeapi_frontend.append_unavail", "edge_lb.access_err"], "latency_ms": [[2, 10], [150, 3500], [200, 5000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "write_append_fail_conn_f", "rpm": 300.0, "emit": ["edge_lb.conn_failure"], "latency_ms": [[300, 7000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "insertall_success_f", "rpm": 80.0, "emit": ["insertall_api.insert_req", "insertall_api.insert_ok"], "latency_ms": [[1, 5], [40, 800]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "insertall_fail_f", "rpm": 20.0, "emit": ["insertall_api.insert_req", "insertall_api.insert_fail"], "latency_ms": [[1, 5], [80, 2500]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "writeapi_streaming_deadlock_usm_20221013"},
    "time": {"total_minutes": 55, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 55}}},
    "phases": {
        "f": {
            "events": [
                {"order": 1, "at_min": 25, "rate_multipliers": {"edge_lb.pool_health_postcap": 0.0}, "latency_multipliers": {"write_append_success_f": {"p50": 1.8, "p95": 2.5}, "write_append_fail_unavailable_f": {"p50": 1.8, "p95": 2.5}, "write_append_fail_conn_f": {"p50": 1.4, "p95": 1.6}}, "one_shots": []},
                {"order": 2, "at_min": 28, "rate_multipliers": {"monitoring.synthetic_check": 3.0}, "latency_multipliers": {}, "one_shots": [{"ref": "monitoring.alert_firing", "count": 1, "hosts": ["mon-1"]}, {"ref": "autoscaler.memory_increase", "count": 6, "hosts": ["as-1"]}, {"ref": "autoscaler.scale_out", "count": 1, "hosts": ["as-1"]}]},
                {"order": 3, "at_min": 34, "rate_multipliers": {"edge_lb.pool_health": 0.0, "edge_lb.pool_health_postcap": 1.0, "write_append_fail_conn_f": 1.2, "write_append_fail_unavailable_f": 1.1, "insertall_fail_f": 1.1}, "latency_multipliers": {"write_append_fail_unavailable_f": {"p50": 1.1, "p95": 1.2}}, "one_shots": [{"ref": "control_plane.set_max_instances", "count": 1, "hosts": ["cp-1"]}]},
                {"order": 4, "at_min": 48, "rate_multipliers": {"write_append_success_f": 0.9, "write_append_fail_conn_f": 1.4, "write_append_fail_unavailable_f": 1.2, "writeapi_frontend.rpc_deadlock_watchdog": 4.0}, "latency_multipliers": {"write_append_fail_conn_f": {"p50": 1.2, "p95": 1.3}, "write_append_fail_unavailable_f": {"p50": 1.3, "p95": 1.5}}, "one_shots": [{"ref": "autoscaler.scale_out_postcap", "count": 1, "hosts": ["as-1"]}]},
                {"order": 5, "at_min": 52, "rate_multipliers": {"write_append_success_f": 1.4, "write_append_fail_conn_f": 0.6, "write_append_fail_unavailable_f": 0.7, "insertall_fail_f": 0.7, "writeapi_frontend.rpc_deadlock_watchdog": 0.4}, "latency_multipliers": {"write_append_success_f": {"p50": 0.7, "p95": 0.8}, "write_append_fail_unavailable_f": {"p50": 0.8, "p95": 0.9}, "write_append_fail_conn_f": {"p50": 0.8, "p95": 0.9}}, "one_shots": [{"ref": "control_plane.restart_instance", "count": 25, "hosts": ["cp-1"]}]},
            ]
        }
    },
}

# ----------------------------
# Helpers
# ----------------------------

PLACEHOLDER_RE = re.compile(r"{([a-zA-Z_][a-zA-Z0-9_]*)}")


def md5_u64(s: str) -> int:
    return int.from_bytes(hashlib.md5(s.encode("utf-8")).digest()[:8], "big", signed=False)


def u01_from_str(s: str) -> float:
    return (md5_u64(s) & ((1 << 53) - 1)) / float(1 << 53)


def isoformat_ms_utc(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    s = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return s[:-3] + "Z"


class Accumulator:
    def __init__(self):
        self.rem = defaultdict(float)

    def alloc(self, key: Tuple[Any, ...], expected: float) -> int:
        if expected <= 0:
            return 0
        total = self.rem[key] + float(expected)
        n = int(total + 1e-12)
        self.rem[key] = total - n
        return n


def build_ip_pool() -> List[str]:
    pool = []
    for a, b, c in [(203, 0, 113), (198, 51, 100), (192, 0, 2)]:
        for i in range(1, 255):
            pool.append(f"{a}.{b}.{c}.{i}")
    return pool


IP_POOL = build_ip_pool()


def ip_for_index(i: int) -> str:
    return IP_POOL[i % len(IP_POOL)]


def sample_hex(rng: np.random.RandomState, length: int) -> str:
    hexd = "0123456789abcdef"
    return "".join(hexd[int(rng.randint(0, 16))] for _ in range(length))


def sample_choice(rng: np.random.RandomState, vals: List[Any]) -> Any:
    return vals[int(rng.randint(0, len(vals)))]


def sample_int(rng: np.random.RandomState, lo: int, hi: int) -> int:
    if lo >= hi:
        return int(lo)
    return int(rng.randint(lo, hi + 1))


def sample_float(rng: np.random.RandomState, lo: float, hi: float) -> float:
    if lo >= hi:
        return float(lo)
    return float(lo + (hi - lo) * rng.rand())


def lognormal_ms(rng: np.random.RandomState, p50: float, p95: float, min_v: Optional[float], max_v: Optional[float]) -> int:
    p50 = max(0.1, float(p50))
    p95 = max(p50 * 1.001, float(p95))
    z95 = 1.6448536269514722
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / z95
    sigma = max(0.05, sigma)

    x = float(rng.lognormal(mean=mu, sigma=sigma))
    soft_cap = 3.0 * p95
    if max_v is not None:
        soft_cap = min(soft_cap, float(max_v))
    x = min(x, soft_cap)

    if min_v is not None:
        x = max(x, float(min_v))
    if max_v is not None:
        x = min(x, float(max_v))

    if x < 0:
        x = 0.0
    return int(round(x))


def sample_domain_value(
    rng: np.random.RandomState,
    domain: Dict[str, Any],
    *,
    str_hint_hosts: Optional[List[str]] = None,
) -> Any:
    k = domain.get("k")
    v = domain.get("v")
    if k == "ch":
        return str(sample_choice(rng, list(v)))
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return sample_int(rng, lo, hi)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return sample_float(rng, lo, hi)
    if k == "hex":
        return sample_hex(rng, int(v))
    if k == "ip":
        return ip_for_index(int(rng.randint(0, len(IP_POOL))))
    if k == "str":
        hint = str(v)
        if "writeapi-fe-<id>" in hint:
            if str_hint_hosts:
                return str_hint_hosts[int(rng.randint(0, len(str_hint_hosts)))]
            return f"writeapi-fe-{sample_int(rng, 1, 100)}"
        return hint
    return str(v)


def placeholders_in_msg(msg: str) -> List[str]:
    return PLACEHOLDER_RE.findall(msg)


@dataclass
class Template:
    component_id: str
    log_id: str
    lvl: str
    msg: str
    vars: Dict[str, Any]
    state_vars: Dict[str, Dict[str, Any]]


# ----------------------------
# Build indices
# ----------------------------

COMP_BY_ID: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}
HOSTS_BY_COMP: Dict[str, List[str]] = {c["id"]: list(c.get("hosts", [])) for c in SYSTEM["components"]}
SVC_BY_COMP: Dict[str, str] = {c["id"]: str(c.get("svc") or "") for c in SYSTEM["components"]}

TEMPLATES: Dict[str, Template] = {}
for comp in SYSTEM["components"]:
    cid = comp["id"]
    for log_id, t in comp.get("logs", {}).items():
        ref = f"{cid}.{log_id}"
        TEMPLATES[ref] = Template(
            component_id=cid,
            log_id=log_id,
            lvl=t["lvl"],
            msg=t["msg"],
            vars=dict(t.get("vars", {})),
            state_vars=dict(t.get("state_vars", {})),
        )

FLOW_BY_STATE_ID: Dict[Tuple[str, str], Dict[str, Any]] = {}
for st in ("n", "f"):
    for flow in SYSTEM["flows"][st]["req"]:
        FLOW_BY_STATE_ID[(st, flow["id"])] = flow

# ----------------------------
# Controls (failure phase events)
# ----------------------------

EVENTS = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e.get("order", 0)))

active_rate_mult: Dict[str, float] = defaultdict(lambda: 1.0)
active_latency_mult: Dict[str, Tuple[float, float]] = defaultdict(lambda: (1.0, 1.0))

ONE_SHOTS: List[Dict[str, Any]] = []
for ev in EVENTS:
    for os_ in ev.get("one_shots", []):
        ONE_SHOTS.append({"at_min": ev["at_min"], **os_})


def apply_event_controls(ev: Dict[str, Any]) -> None:
    for k, v in (ev.get("rate_multipliers") or {}).items():
        active_rate_mult[str(k)] = float(v)
    for fid, lm in (ev.get("latency_multipliers") or {}).items():
        active_latency_mult[str(fid)] = (float(lm["p50"]), float(lm["p95"]))


# ----------------------------
# Emission
# ----------------------------

def render_message(template: Template, state: str, rng: np.random.RandomState, bound: Dict[str, Any]) -> str:
    needed = placeholders_in_msg(template.msg)
    ctx: Dict[str, Any] = {}
    for k, v in bound.items():
        ctx[k] = v

    for name in needed:
        if name in ctx:
            continue
        if state in template.state_vars and name in template.state_vars[state]:
            domain = template.state_vars[state][name]
            ctx[name] = sample_domain_value(rng, domain, str_hint_hosts=HOSTS_BY_COMP.get("writeapi_frontend"))
            continue
        if name in template.vars:
            domain = template.vars[name]
            ctx[name] = sample_domain_value(rng, domain, str_hint_hosts=HOSTS_BY_COMP.get("writeapi_frontend"))
            continue
        ctx[name] = ""

    for k, v in list(ctx.items()):
        if isinstance(v, float):
            ctx[k] = f"{v:.3f}"
        else:
            ctx[k] = str(v)

    return template.msg.format(**ctx)


def emit_row(rows: List[Dict[str, Any]], ts: datetime, template_ref: str, state: str, host: str, bound: Dict[str, Any]) -> None:
    t = TEMPLATES[template_ref]
    svc = SVC_BY_COMP.get(t.component_id, "") or ""
    ms = int(ts.timestamp() * 1000)
    seed = md5_u64(f"{template_ref}|{ms}|{host}") & 0xFFFFFFFF
    rng = np.random.RandomState(seed)
    msg = render_message(t, state, rng, bound)
    rows.append(
        {
            "timestamp_dt": ts,
            "level": t.lvl,
            "message": msg,
            "trace_id": "",
            "service": svc,
            "host": host or "",
        }
    )


def schedule_offsets_within_minute(minute: int, count: int, key: str) -> List[float]:
    if count <= 0:
        return []
    offsets = []
    for i in range(count):
        base = (i + 0.5) * 60.0 / count
        jitter = (u01_from_str(f"{key}|{minute}|{i}") - 0.5) * 0.10  # +/- 0.05s
        off = base + jitter
        if off < 0.0:
            off = 0.0
        if off >= 60.0:
            off = 59.999
        offsets.append(off)
    return offsets


def latency_pair_scaled(pair: List[float], mult: Tuple[float, float]) -> Tuple[float, float]:
    return float(pair[0]) * float(mult[0]), float(pair[1]) * float(mult[1])


def get_template_int_range(template_ref: str, var_name: str) -> Tuple[Optional[int], Optional[int]]:
    t = TEMPLATES[template_ref]
    if var_name in t.vars and t.vars[var_name].get("k") == "i":
        lo, hi = t.vars[var_name]["v"]
        return int(lo), int(hi)
    return None, None


def simulate_flow_instance(
    rows: List[Dict[str, Any]],
    flow: Dict[str, Any],
    start_dt: datetime,
    state: str,
    latency_mult: Tuple[float, float],
    inst_idx: int,
) -> None:
    fid = flow["id"]
    emit_refs = flow["emit"]
    lat_pairs = flow["latency_ms"]

    seed = md5_u64(f"flow|{fid}|{inst_idx}") & 0xFFFFFFFF
    rng = np.random.RandomState(seed)

    def pick_host(comp_id: str) -> str:
        hosts = HOSTS_BY_COMP.get(comp_id, [])
        if not hosts:
            return ""
        return hosts[inst_idx % len(hosts)]

    peer_ip = ip_for_index(md5_u64(f"peer|{fid}|{inst_idx}") % len(IP_POOL))
    project = ["analytics-prod", "etl-prod", "customer-x"][inst_idx % 3]
    dataset = ["events", "logs"][inst_idx % 2]
    table = ["clicks", "impressions", "raw"][(inst_idx // 2) % 3]

    writeapi_hosts = HOSTS_BY_COMP["writeapi_frontend"]
    backend_host = writeapi_hosts[inst_idx % len(writeapi_hosts)]
    lb_host = HOSTS_BY_COMP["edge_lb"][inst_idx % len(HOSTS_BY_COMP["edge_lb"])]

    def sample_delay_ms(p50: float, p95: float, min_v: Optional[int] = None, max_v: Optional[int] = None) -> int:
        return lognormal_ms(rng, p50, p95, min_v, max_v)

    # Single-log conn failure flow: latency_ms is "waited_ms" and also determines timestamp.
    if len(emit_refs) == 1 and emit_refs[0] == "edge_lb.conn_failure":
        p50, p95 = latency_pair_scaled(lat_pairs[0], latency_mult)
        w_lo, w_hi = get_template_int_range("edge_lb.conn_failure", "waited_ms")
        waited_ms = sample_delay_ms(p50, p95, w_lo, w_hi)

        ts0 = start_dt + timedelta(milliseconds=waited_ms)
        bound = {
            "backend": backend_host,
            "reason": sample_choice(rng, ["timeout", "reset_by_peer", "handshake_failed"]),
            "waited_ms": waited_ms,
            "peer_ip": peer_ip,
        }
        emit_row(rows, ts0, "edge_lb.conn_failure", state, lb_host, bound)
        return

    # Multi-log attempt flow: build coherent per-segment delays.
    delays_ms: List[int] = []

    # Segment 0: start -> first emitted log
    p50_0, p95_0 = latency_pair_scaled(lat_pairs[0], latency_mult)

    # Segment 1: first -> second log (often carries dur_ms)
    second_ref = emit_refs[1]
    p50_1, p95_1 = latency_pair_scaled(lat_pairs[1], latency_mult)
    sdur_lo, sdur_hi = get_template_int_range(second_ref, "dur_ms")

    # Segment 2: second -> third log (LB access) where third log carries total dur_ms from start.
    access_lo: Optional[int] = None
    access_hi: Optional[int] = None
    p50_2: float = 0.0
    p95_2: float = 0.0
    if len(emit_refs) == 3:
        third_ref = emit_refs[2]
        p50_2, p95_2 = latency_pair_scaled(lat_pairs[2], latency_mult)
        access_lo, access_hi = get_template_int_range(third_ref, "dur_ms")

    # Sample with coordination so that access_* dur_ms stays within its template domain.
    d0 = sample_delay_ms(p50_0, p95_0, 0, None)

    if access_hi is not None:
        # Leave at least 1ms for the remaining segments so total <= access_hi is always feasible.
        d0 = min(d0, max(0, int(access_hi) - 2))

    # Ensure d0 doesn't make it impossible to fit the second segment minimum within access_hi.
    if access_hi is not None:
        min_d1 = int(sdur_lo) if sdur_lo is not None else 0
        max_allowed_d0 = max(0, int(access_hi) - min_d1 - 1)
        if d0 > max_allowed_d0:
            d0 = max_allowed_d0

    # Determine bounds for d1. If we have an access_hi, enforce total headroom.
    d1_min = int(sdur_lo) if sdur_lo is not None else 0
    d1_max = int(sdur_hi) if sdur_hi is not None else None
    if access_hi is not None:
        headroom_for_d1 = max(d1_min, int(access_hi) - d0 - 1)
        if d1_max is None:
            d1_max = headroom_for_d1
        else:
            d1_max = min(d1_max, headroom_for_d1)
        if d1_max < d1_min:
            d1_max = d1_min

    d1 = sample_delay_ms(p50_1, p95_1, d1_min if (sdur_lo is not None) else 0, d1_max)
    delays_ms.append(d0)
    delays_ms.append(d1)

    if len(emit_refs) == 3:
        # Enforce both min and max for the access log total dur_ms by bounding d2.
        assert access_lo is not None and access_hi is not None
        total_so_far = d0 + d1
        d2_min = max(0, int(access_lo) - total_so_far)
        d2_max = max(d2_min, int(access_hi) - total_so_far)
        d2 = sample_delay_ms(p50_2, p95_2, d2_min, d2_max)
        delays_ms.append(d2)

    # Build timestamps from delays.
    ts = start_dt
    timestamps: List[datetime] = []
    for dms in delays_ms:
        ts = ts + timedelta(milliseconds=int(dms))
        timestamps.append(ts)

    stream_id = sample_hex(rng, 16)
    rows_count = sample_int(rng, 1, 20000)

    ref0 = emit_refs[0]
    cid0, _ = ref0.split(".", 1)
    host0 = pick_host(cid0)
    if ref0 == "writeapi_frontend.append_begin":
        host0 = backend_host
        inflight_domain = TEMPLATES[ref0].state_vars.get(state, {}).get("inflight")
        inflight = sample_domain_value(rng, inflight_domain) if inflight_domain else (100 if state == "n" else 1000)
        bound0 = {"stream_id": stream_id, "project": project, "dataset": dataset, "table": table, "inflight": inflight}
        emit_row(rows, timestamps[0], ref0, state, host0, bound0)
    elif ref0 == "insertall_api.insert_req":
        host0 = pick_host("insertall_api")
        ia_rows = sample_int(rng, 1, 500)
        bound0 = {"project": project, "dataset": dataset, "table": table, "rows": ia_rows}
        simulate_flow_instance._last_insertall_rows = ia_rows  # type: ignore[attr-defined]
        emit_row(rows, timestamps[0], ref0, state, host0, bound0)
    else:
        emit_row(rows, timestamps[0], ref0, state, host0, {})

    ref1 = emit_refs[1]
    cid1, _ = ref1.split(".", 1)
    host1 = pick_host(cid1)
    if ref1 == "writeapi_frontend.append_ok":
        host1 = backend_host
        dur_ms = int((timestamps[1] - timestamps[0]).total_seconds() * 1000)
        bound1 = {"stream_id": stream_id, "rows": rows_count, "dur_ms": dur_ms}
        emit_row(rows, timestamps[1], ref1, state, host1, bound1)
    elif ref1 == "writeapi_frontend.append_unavail":
        host1 = backend_host
        dur_ms = int((timestamps[1] - timestamps[0]).total_seconds() * 1000)
        bound1 = {"stream_id": stream_id, "err": sample_choice(rng, ["frontend_overloaded", "rpc_deadlock", "queue_full"]), "dur_ms": dur_ms}
        emit_row(rows, timestamps[1], ref1, state, host1, bound1)
    elif ref1 == "insertall_api.insert_ok":
        host1 = pick_host("insertall_api")
        dur_ms = int((timestamps[1] - timestamps[0]).total_seconds() * 1000)
        ia_rows = getattr(simulate_flow_instance, "_last_insertall_rows", None)  # type: ignore[attr-defined]
        if ia_rows is None:
            ia_rows = sample_int(rng, 1, 500)
        bound1 = {"rows": int(ia_rows), "dur_ms": dur_ms}
        emit_row(rows, timestamps[1], ref1, state, host1, bound1)
    elif ref1 == "insertall_api.insert_fail":
        host1 = pick_host("insertall_api")
        dur_ms = int((timestamps[1] - timestamps[0]).total_seconds() * 1000)
        status = 503 if (inst_idx % 4 != 0) else 504
        reason = "backend_overloaded" if (inst_idx % 3 != 0) else "timeout"
        bound1 = {"status": status, "reason": reason, "dur_ms": dur_ms}
        emit_row(rows, timestamps[1], ref1, state, host1, bound1)
    else:
        emit_row(rows, timestamps[1], ref1, state, host1, {})

    if len(emit_refs) == 3:
        ref2 = emit_refs[2]
        cid2, _ = ref2.split(".", 1)
        host2 = pick_host(cid2)
        if cid2 == "edge_lb":
            host2 = lb_host
        total_ms = int((timestamps[2] - start_dt).total_seconds() * 1000)
        # total_ms is coordinated with delays so it stays within access_* template bounds.
        if ref2 == "edge_lb.access_ok":
            bound2 = {"status": "200", "backend": backend_host, "dur_ms": total_ms, "peer_ip": peer_ip}
            emit_row(rows, timestamps[2], ref2, state, host2, bound2)
        elif ref2 == "edge_lb.access_err":
            status = "503" if (inst_idx % 5 != 0) else "504"
            bound2 = {"status": status, "backend": backend_host, "dur_ms": total_ms, "peer_ip": peer_ip}
            emit_row(rows, timestamps[2], ref2, state, host2, bound2)
        else:
            emit_row(rows, timestamps[2], ref2, state, host2, {})


# ----------------------------
# Main simulation
# ----------------------------

def main() -> None:
    random.seed(0)
    np.random.seed(0)

    base_time = datetime(2022, 10, 13, 16, 0, 0, tzinfo=timezone.utc)

    total_minutes = SCENARIO["time"]["total_minutes"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]

    acc = Accumulator()
    rows: List[Dict[str, Any]] = []

    bg_global_rr = defaultdict(int)
    flow_inst_counter = defaultdict(int)

    one_shots_by_min = defaultdict(list)
    for os_ in ONE_SHOTS:
        one_shots_by_min[int(os_["at_min"])].append(os_)

    ev_idx = 0
    for minute in range(total_minutes):
        state = "n" if minute < n_end else "f"

        if state == "f":
            while ev_idx < len(EVENTS) and int(EVENTS[ev_idx]["at_min"]) == minute:
                apply_event_controls(EVENTS[ev_idx])
                ev_idx += 1

        t0 = base_time + timedelta(minutes=minute)

        # One-shots at the exact event minute (not scaled by multipliers).
        if state == "f" and minute in one_shots_by_min:
            for os_ in one_shots_by_min[minute]:
                ref = os_["ref"]
                count = int(os_["count"])
                allowed_hosts = list(os_.get("hosts") or [])
                if ref not in TEMPLATES:
                    continue
                template = TEMPLATES[ref]
                for i in range(count):
                    off = min(11.5, (i + 0.5) * (12.0 / max(1, count)))
                    jitter = (u01_from_str(f"oneshot|{ref}|{minute}|{i}") - 0.5) * 0.15
                    ts = t0 + timedelta(seconds=max(0.0, off + jitter))

                    host = allowed_hosts[0] if allowed_hosts else (HOSTS_BY_COMP.get(template.component_id, [""])[0] if HOSTS_BY_COMP.get(template.component_id) else "")

                    bound: Dict[str, Any] = {}
                    if ref == "monitoring.alert_firing":
                        threshold = 0.02
                        value = max(0.03, float(0.12 + 0.25 * u01_from_str(f"alertval|{minute}")))
                        value = min(value, 0.6)
                        value = max(value, threshold)
                        bound = {"threshold": threshold, "value": value}
                    elif ref == "autoscaler.memory_increase":
                        from_gb = 16 if (i % 2 == 0) else 32
                        to_gb = 32 if from_gb == 16 else 64
                        bound = {"from_gb": from_gb, "to_gb": to_gb, "target": "writeapi-frontend"}
                    elif ref == "autoscaler.scale_out":
                        from_n = 12 + (i % 4)
                        to_n = min(20, from_n + 5)
                        bound = {"from": from_n, "to": to_n, "target": "writeapi-frontend"}
                    elif ref == "autoscaler.scale_out_postcap":
                        bound = {"from": 30, "to": 80, "target": "writeapi-frontend"}
                    elif ref == "control_plane.set_max_instances":
                        bound = {"target": "writeapi-frontend", "from": 20, "to": 100}
                    elif ref == "control_plane.restart_instance":
                        inst_name = HOSTS_BY_COMP["writeapi_frontend"][i % len(HOSTS_BY_COMP["writeapi_frontend"])]
                        reason = "deadlock_suspected" if (i % 2 == 0) else "stuck_process"
                        bound = {"target": "writeapi-frontend", "instance": inst_name, "reason": reason}

                    emit_row(rows, ts, ref, state, host, bound)

        # Background emissions.
        for comp in SYSTEM["components"]:
            cid = comp["id"]
            beh = comp.get("beh", {}).get(state, {})
            emits = beh.get("emit", []) or []
            for e in emits:
                log_id = e["id"]
                scope = e.get("scope") or "per_host"
                per_min = float(e.get("per_min", 0.0))
                template_ref = f"{cid}.{log_id}"

                mult = 1.0
                if state == "f":
                    mult = float(active_rate_mult.get(template_ref, 1.0))
                eff = per_min * mult
                if eff <= 0.0:
                    continue

                if scope == "global":
                    k = ("bg", state, cid, log_id, "global")
                    cnt = acc.alloc(k, eff)
                    if cnt <= 0:
                        continue
                    offsets = schedule_offsets_within_minute(minute, cnt, f"bg|{template_ref}|global")
                    hosts = HOSTS_BY_COMP.get(cid, [])
                    for j, off in enumerate(offsets):
                        ts = t0 + timedelta(seconds=off)
                        host = ""
                        if hosts:
                            idx = bg_global_rr[(cid, log_id)]
                            host = hosts[idx % len(hosts)]
                            bg_global_rr[(cid, log_id)] += 1

                        bound: Dict[str, Any] = {}
                        if template_ref == "monitoring.synthetic_check":
                            if state == "n":
                                status = "OK"
                            else:
                                p_unavail = 0.70 if minute < 52 else 0.45
                                status = "UNAVAILABLE" if u01_from_str(f"syn|{minute}|{j}") < p_unavail else "OK"
                            if status == "UNAVAILABLE":
                                dur_ms = int(2000 + 6000 * u01_from_str(f"synlat|{minute}|{j}"))
                            else:
                                dur_ms = int(10 + 790 * u01_from_str(f"synlat|{minute}|{j}"))
                            bound = {"status": status, "dur_ms": dur_ms}

                        emit_row(rows, ts, template_ref, state, host, bound)
                else:
                    hosts = HOSTS_BY_COMP.get(cid, [])
                    if not hosts:
                        k = ("bg", state, cid, log_id, "nohost")
                        cnt = acc.alloc(k, eff)
                        offsets = schedule_offsets_within_minute(minute, cnt, f"bg|{template_ref}|nohost")
                        for j, off in enumerate(offsets):
                            ts = t0 + timedelta(seconds=off)
                            emit_row(rows, ts, template_ref, state, "", {})
                        continue

                    for h in hosts:
                        k = ("bg", state, cid, log_id, h)
                        cnt = acc.alloc(k, eff)
                        if cnt <= 0:
                            continue
                        offsets = schedule_offsets_within_minute(minute, cnt, f"bg|{template_ref}|{h}")
                        for j, off in enumerate(offsets):
                            ts = t0 + timedelta(seconds=off)

                            bound: Dict[str, Any] = {}
                            if template_ref == "writeapi_frontend.fe_health":
                                if state == "n":
                                    ok = "true"
                                else:
                                    ok = "false" if u01_from_str(f"feok|{h}|{minute}|{j}") < 0.35 else "true"
                                bound["ok"] = ok

                            emit_row(rows, ts, template_ref, state, h, bound)

        # Flow instances.
        for flow in SYSTEM["flows"][state]["req"]:
            fid = flow["id"]
            rpm = float(flow["rpm"])
            if state == "f":
                rpm *= float(active_rate_mult.get(fid, 1.0))
            if rpm <= 0.0:
                continue

            cnt = acc.alloc(("flow", state, fid), rpm)
            if cnt <= 0:
                continue
            offsets = schedule_offsets_within_minute(minute, cnt, f"flow|{fid}")

            for off in offsets:
                inst_idx = flow_inst_counter[fid]
                flow_inst_counter[fid] += 1

                start_dt = t0 + timedelta(seconds=off)

                lm = (1.0, 1.0)
                if state == "f":
                    lm = active_latency_mult.get(fid, (1.0, 1.0))

                simulate_flow_instance(rows, flow, start_dt, state, lm, inst_idx)

    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp_dt", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["timestamp_dt"].apply(isoformat_ms_utc)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
