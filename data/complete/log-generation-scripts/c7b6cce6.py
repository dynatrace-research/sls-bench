import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# ----------------------------
# Embedded normalized model data
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "reddit_legacy_k8s_cluster"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "edge_gateway",
            "svc": "edge-gw",
            "hosts": ["edge-1", "edge-2"],
            "logs": {
                "http_request_start": {
                    "lvl": "INFO",
                    "msg": "req {req_id} start GET {uri_path} ua={ua}",
                    "vars": {
                        "req_id": {"k": "uuid"},
                        "uri_path": {"k": "ch", "v": ["/", "/r/all", "/hot"]},
                        "ua": {"k": "ch", "v": ["web", "ios", "android"]},
                    },
                },
                "http_access_ok": {
                    "lvl": "INFO",
                    "msg": "req {req_id} GET {uri_path} {status} {bytes}B in {dur_ms}ms upstream={upstream}",
                    "vars": {
                        "req_id": {"k": "uuid"},
                        "uri_path": {"k": "ch", "v": ["/", "/r/all", "/hot"]},
                        "status": {"k": "ch", "v": ["200"]},
                        "bytes": {"k": "i", "v": [2000, 200000]},
                        "dur_ms": {"k": "i", "v": [20, 800]},
                        "upstream": {"k": "ch", "v": ["legacy-app"]},
                    },
                },
                "http_access_5xx": {
                    "lvl": "WARN",
                    "msg": "req {req_id} GET {uri_path} {status} {bytes}B in {dur_ms}ms upstream={upstream} upstream_status={upstream_status} err={err}",
                    "vars": {
                        "req_id": {"k": "uuid"},
                        "uri_path": {"k": "ch", "v": ["/", "/r/all", "/hot"]},
                        "status": {"k": "ch", "v": ["502", "503", "504"]},
                        "bytes": {"k": "i", "v": [0, 2000]},
                        "dur_ms": {"k": "i", "v": [5, 30000]},
                        "upstream": {"k": "ch", "v": ["legacy-app"]},
                        "upstream_status": {"k": "ch", "v": ["-", "502", "504"]},
                        "err": {"k": "ch", "v": ["no_endpoints", "connect_timeout", "upstream_reset"]},
                    },
                },
                "http_access_empty": {
                    "lvl": "INFO",
                    "msg": "req {req_id} GET {uri_path} 200 {bytes}B in {dur_ms}ms upstream={upstream} note=empty_feed",
                    "vars": {
                        "req_id": {"k": "uuid"},
                        "uri_path": {"k": "ch", "v": ["/"]},
                        "bytes": {"k": "i", "v": [0, 900]},
                        "dur_ms": {"k": "i", "v": [10, 500]},
                        "upstream": {"k": "ch", "v": ["edge-fallback"]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "legacy_app",
            "svc": "legacy-app",
            "hosts": ["app-1", "app-2", "app-3"],
            "logs": {
                "req_done": {
                    "lvl": "INFO",
                    "msg": "req {req_id} {endpoint} 200 in {dur_ms}ms user={user_kind}",
                    "vars": {
                        "req_id": {"k": "uuid"},
                        "endpoint": {"k": "ch", "v": ["/", "/r/all", "/hot"]},
                        "dur_ms": {"k": "i", "v": [10, 1200]},
                        "user_kind": {"k": "ch", "v": ["anon", "logged_in"]},
                    },
                },
                "req_timeout": {
                    "lvl": "WARN",
                    "msg": "req {req_id} {endpoint} 504 upstream_timeout after {dur_ms}ms",
                    "vars": {
                        "req_id": {"k": "uuid"},
                        "endpoint": {"k": "ch", "v": ["/", "/r/all", "/hot"]},
                        "dur_ms": {"k": "i", "v": [3000, 30000]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "kube_apiserver",
            "svc": "kube-apiserver",
            "hosts": ["cp-1", "cp-2", "cp-3"],
            "logs": {
                "apiserver_slow_request": {
                    "lvl": "WARN",
                    "msg": "slow request {verb} {resource} took {dur_ms}ms client={client} code={code}",
                    "vars": {
                        "verb": {"k": "ch", "v": ["POST", "PUT", "DELETE", "PATCH"]},
                        "resource": {"k": "ch", "v": ["pods", "endpoints", "nodes", "configmaps"]},
                        "dur_ms": {"k": "i", "v": [50, 60000]},
                        "client": {"k": "ch", "v": ["kube-controller-manager", "kube-scheduler", "kubelet", "calico"]},
                        "code": {"k": "i", "v": [200, 504]},
                    },
                },
                "admission_webhook_timeout": {
                    "lvl": "ERROR",
                    "msg": "admission webhook {webhook} timed out for {verb} {resource} after {dur_ms}ms",
                    "vars": {
                        "webhook": {"k": "ch", "v": ["opa.k8s.policy"]},
                        "verb": {"k": "ch", "v": ["POST", "PUT", "DELETE"]},
                        "resource": {"k": "ch", "v": ["pods", "endpoints", "configmaps"]},
                        "dur_ms": {"k": "i", "v": [1000, 30000]},
                    },
                },
                "etcd_write_timeout": {
                    "lvl": "ERROR",
                    "msg": "etcd request {verb} key={key} failed: context deadline exceeded after {dur_ms}ms",
                    "vars": {
                        "verb": {"k": "ch", "v": ["PUT", "DELETE"]},
                        "key": {"k": "str", "v": "k8s/registry/..."},
                        "dur_ms": {"k": "i", "v": [200, 30000]},
                    },
                },
                "apiserver_node_delete": {
                    "lvl": "INFO",
                    "msg": "request {verb} nodes/w-{node_id} completed code={code} dur={dur_ms}ms",
                    "vars": {
                        "verb": {"k": "ch", "v": ["DELETE"]},
                        "node_id": {"k": "i", "v": [1, 4000]},
                        "code": {"k": "ch", "v": ["200", "504"]},
                        "dur_ms": {"k": "i", "v": [50, 60000]},
                    },
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "apiserver_slow_request", "per_min": 1.0, "scope": "global"},
                        {"id": "admission_webhook_timeout", "per_min": 0.1, "scope": "global"},
                        {"id": "etcd_write_timeout", "per_min": 0.05, "scope": "global"},
                        {"id": "apiserver_node_delete", "per_min": 0.1, "scope": "global"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "apiserver_slow_request", "per_min": 20.0, "scope": "global"},
                        {"id": "admission_webhook_timeout", "per_min": 10.0, "scope": "global"},
                        {"id": "etcd_write_timeout", "per_min": 5.0, "scope": "global"},
                        {"id": "apiserver_node_delete", "per_min": 0.1, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "calico_node",
            "svc": "calico-node",
            "hosts": ["node-cp-1", "node-w-1", "node-w-2", "node-w-3"],
            "logs": {
                "route_withdraw": {
                    "lvl": "WARN",
                    "msg": "BGP route withdrawal: node={node} withdrawn={count} reason={reason}",
                    "vars": {
                        "node": {"k": "ch", "v": ["cp-1", "w-1", "w-2", "w-3"]},
                        "count": {"k": "i", "v": [1, 2000]},
                        "reason": {"k": "ch", "v": ["peer_down", "reflector_unreachable"]},
                    },
                },
                "reflector_selector_zero": {
                    "lvl": "INFO",
                    "msg": "route reflector selector matched 0 nodes selector={selector}",
                    "vars": {"selector": {"k": "ch", "v": ["node-role.kubernetes.io/master"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "route_withdraw", "per_min": 0.2}, {"id": "reflector_selector_zero", "per_min": 0.0}]},
                "f": {"emit": [{"id": "route_withdraw", "per_min": 10.0}, {"id": "reflector_selector_zero", "per_min": 0.7}]},
            },
        },
        {
            "id": "coredns",
            "svc": "coredns",
            "hosts": ["dns-1", "dns-2"],
            "logs": {
                "dns_internal_servfail": {
                    "lvl": "ERROR",
                    "msg": "DNS query {qname} {qtype} -> SERVFAIL upstream={upstream}",
                    "vars": {
                        "qname": {
                            "k": "ch",
                            "v": [
                                "legacy-app.default.svc.cluster.local",
                                "kube-dns.kube-system.svc.cluster.local",
                                "web.service.consul",
                            ],
                        },
                        "qtype": {"k": "ch", "v": ["A", "AAAA"]},
                        "upstream": {"k": "ch", "v": ["10.0.0.2"]},
                    },
                },
                "dns_public_ok": {
                    "lvl": "INFO",
                    "msg": "DNS query {qname} {qtype} -> NOERROR upstream={upstream}",
                    "vars": {
                        "qname": {"k": "ch", "v": ["www.google.com", "aws.amazon.com", "example.com"]},
                        "qtype": {"k": "ch", "v": ["A", "AAAA"]},
                        "upstream": {"k": "ch", "v": ["8.8.8.8"]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "dns_internal_servfail", "per_min": 0.2}, {"id": "dns_public_ok", "per_min": 2.0}]},
                "f": {"emit": [{"id": "dns_internal_servfail", "per_min": 6.0}, {"id": "dns_public_ok", "per_min": 2.0}]},
            },
        },
        {
            "id": "opa_webhook",
            "svc": "opa-admission",
            "hosts": ["opa-1"],
            "logs": {
                "opa_decision": {
                    "lvl": "INFO",
                    "msg": "decision {decision_id} result={result} resource={resource}",
                    "vars": {
                        "decision_id": {"k": "hex", "v": 16},
                        "result": {"k": "ch", "v": ["allow", "deny"]},
                        "resource": {"k": "ch", "v": ["pods", "endpoints", "configmaps"]},
                    },
                }
            },
            "beh": {"n": {"emit": [{"id": "opa_decision", "per_min": 3.0}]}, "f": {"emit": [{"id": "opa_decision", "per_min": 0.0}]}},
        },
        {
            "id": "compute_ops",
            "svc": None,
            "hosts": ["ops-1"],
            "logs": {
                "ops_heartbeat": {"lvl": "DEBUG", "msg": "ops console heartbeat session={session}", "vars": {"session": {"k": "hex", "v": 8}}},
                "incident_opened": {
                    "lvl": "INFO",
                    "msg": "incident opened cluster={cluster} severity={sev}",
                    "vars": {"cluster": {"k": "ch", "v": ["legacy-prod"]}, "sev": {"k": "ch", "v": ["SEV-1"]}},
                },
                "upgrade_started": {
                    "lvl": "INFO",
                    "msg": "started k8s control-plane upgrade from {from_ver} to {to_ver} cluster={cluster}",
                    "vars": {"from_ver": {"k": "ch", "v": ["1.23"]}, "to_ver": {"k": "ch", "v": ["1.24"]}, "cluster": {"k": "ch", "v": ["legacy-prod"]}},
                },
                "deleted_opa_webhook": {
                    "lvl": "WARN",
                    "msg": "deleted validatingwebhookconfiguration name={name} to unblock apiserver",
                    "vars": {"name": {"k": "ch", "v": ["opa-validating-webhook"]}},
                },
                "worker_termination_started": {
                    "lvl": "WARN",
                    "msg": "began draining/terminating worker nodes count={count} cluster={cluster}",
                    "vars": {"count": {"k": "i", "v": [1000, 4000]}, "cluster": {"k": "ch", "v": ["legacy-prod"]}},
                },
            },
            "beh": {"n": {"emit": [{"id": "ops_heartbeat", "per_min": 0.2, "scope": "global"}]}, "f": {"emit": [{"id": "ops_heartbeat", "per_min": 0.2, "scope": "global"}]}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "homepage_view_ok",
                    "rpm": 250.0,
                    "emit": ["edge_gateway.http_request_start", "legacy_app.req_done", "edge_gateway.http_access_ok"],
                    "latency_ms": [[1, 3], [20, 250], [25, 350]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                }
            ]
        },
        "f": {
            "req": [
                {
                    "id": "homepage_view_edge_5xx",
                    "rpm": 220.0,
                    "emit": ["edge_gateway.http_request_start", "edge_gateway.http_access_5xx"],
                    "latency_ms": [[1, 3], [5, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "homepage_view_backend_timeout",
                    "rpm": 30.0,
                    "emit": ["edge_gateway.http_request_start", "legacy_app.req_timeout", "edge_gateway.http_access_5xx"],
                    "latency_ms": [[1, 3], [5000, 20000], [5000, 30000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "homepage_view_empty_200",
                    "rpm": 25.0,
                    "emit": ["edge_gateway.http_request_start", "edge_gateway.http_access_empty"],
                    "latency_ms": [[1, 3], [10, 200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "pi_day_k8s_upgrade_network_outage",
        "time": {"total_minutes": 60, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 60}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 25,
                        "rate_multipliers": {
                            "calico_node.route_withdraw": 2.5,
                            "calico_node.reflector_selector_zero": 2.0,
                            "coredns.dns_internal_servfail": 3.0,
                            "kube_apiserver.apiserver_slow_request": 5.0,
                            "kube_apiserver.admission_webhook_timeout": 5.0,
                            "kube_apiserver.etcd_write_timeout": 3.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "compute_ops.incident_opened", "count": 1, "hosts": ["ops-1"]},
                            {"ref": "compute_ops.upgrade_started", "count": 1, "hosts": ["ops-1"]},
                        ],
                    },
                    {
                        "order": 2,
                        "at_min": 35,
                        "rate_multipliers": {"kube_apiserver.admission_webhook_timeout": 0.0},
                        "latency_multipliers": {},
                        "one_shots": [{"ref": "compute_ops.deleted_opa_webhook", "count": 1, "hosts": ["ops-1"]}],
                    },
                    {
                        "order": 3,
                        "at_min": 48,
                        "rate_multipliers": {
                            "kube_apiserver.apiserver_node_delete": 800.0,
                            "calico_node.route_withdraw": 4.0,
                            "homepage_view_edge_5xx": 1.2,
                            "homepage_view_empty_200": 0.6,
                            "homepage_view_backend_timeout": 1.1,
                        },
                        "latency_multipliers": {"homepage_view_backend_timeout": {"p50": 1.2, "p95": 1.3}},
                        "one_shots": [{"ref": "compute_ops.worker_termination_started", "count": 1, "hosts": ["ops-1"]}],
                    },
                ]
            }
        },
    }
}

# ----------------------------
# Helpers
# ----------------------------

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
SEED = 0


def _hbytes(*parts: Any, digest_size: int = 16) -> bytes:
    s = "|".join("" if p is None else str(p) for p in parts).encode("utf-8")
    return hashlib.blake2b(s, digest_size=digest_size).digest()


def hfloat(*parts: Any) -> float:
    b = _hbytes(*parts, digest_size=8)
    x = int.from_bytes(b, "big", signed=False)
    return (x + 0.5) / (2**64)


def hhex(n: int, *parts: Any) -> str:
    needed = (n + 1) // 2
    b = _hbytes(*parts, digest_size=max(needed, 16))
    return b.hex()[:n]


def huuid(*parts: Any) -> str:
    h = hhex(32, *parts)
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def clamp01(u: float) -> float:
    return min(max(u, 1e-12), 1.0 - 1e-12)


def norm_ppf(p: float) -> float:
    p = clamp01(p)
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


def lognormal_sample_from_p50_p95(p50: float, p95: float, u: float, soft_cap_mult: float = 3.0) -> float:
    p50 = max(p50, 0.001)
    p95 = max(p95, p50)
    if p95 == p50:
        x = p50
    else:
        sigma = math.log(p95 / p50) / 1.6448536269514722
        mu = math.log(p50)
        z = norm_ppf(clamp01(u))
        x = math.exp(mu + sigma * z)
    cap = soft_cap_mult * p95
    return min(max(x, 0.001), cap)


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond / 1000):03d}Z"


@dataclass
class Template:
    component_id: str
    log_id: str
    level: str
    msg: str
    vars: Dict[str, Dict[str, Any]]


class CarryRounding:
    def __init__(self) -> None:
        self.carry: Dict[str, float] = {}

    def alloc(self, key: str, expected: float) -> int:
        c = self.carry.get(key, 0.0)
        x = expected + c
        n = int(math.floor(x + 1e-12))
        self.carry[key] = x - n
        return n


class FlowInstanceSequencer:
    """
    Provides a deterministic globally-unique per-flow-instance integer id.
    This prevents req_id/trace_id reuse across segments where enumerate() restarts.
    """

    def __init__(self) -> None:
        self._global = 0

    def next(self) -> int:
        self._global += 1
        return self._global


def choose_from_domain(domain: Dict[str, Any], *seed_parts: Any) -> Any:
    k = domain.get("k")
    v = domain.get("v")
    u = hfloat(*seed_parts, k, v)
    if k == "ch":
        assert isinstance(v, list) and len(v) > 0
        idx = min(int(u * len(v)), len(v) - 1)
        return v[idx]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi <= lo:
            return lo
        return lo + int(u * (hi - lo + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return lo + u * (hi - lo)
    if k == "hex":
        n = int(v)
        return hhex(n, *seed_parts)
    if k == "uuid":
        return huuid(*seed_parts)
    if k == "ip":
        return "127.0.0.1"
    if k == "str":
        return str(v)
    return ""


def int_domain_bounds(dom: Optional[Dict[str, Any]]) -> Tuple[Optional[int], Optional[int]]:
    if not dom or dom.get("k") != "i":
        return None, None
    v = dom.get("v")
    if not isinstance(v, (list, tuple)) or len(v) != 2:
        return None, None
    try:
        return int(v[0]), int(v[1])
    except Exception:
        return None, None


# ----------------------------
# Build indices
# ----------------------------

component_by_id: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

templates_by_ref: Dict[str, Template] = {}
for c in SYSTEM["components"]:
    cid = c["id"]
    for lid, ld in c.get("logs", {}).items():
        templates_by_ref[f"{cid}.{lid}"] = Template(component_id=cid, log_id=lid, level=ld["lvl"], msg=ld["msg"], vars=ld.get("vars", {}))

flows_by_state: Dict[str, Dict[str, Any]] = {"n": {}, "f": {}}
for state in ["n", "f"]:
    for f in SYSTEM["flows"][state]["req"]:
        flows_by_state[state][f["id"]] = f

# ----------------------------
# Derive failure control segments
# ----------------------------


def derive_failure_segments() -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    phases = SCENARIO["scenario"]["time"]["phases"]
    f_start = int(phases["f"]["start_min"])
    f_end = int(phases["f"]["end_min"])
    events = list(SCENARIO["scenario"]["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e["order"]))

    cur_bg_mult: Dict[str, float] = {}
    cur_flow_mult: Dict[str, float] = {}
    cur_flow_lat: Dict[str, Dict[str, float]] = {}

    segments: List[Dict[str, Any]] = []
    one_shots: List[Dict[str, Any]] = []

    idx = 0
    t = f_start
    while idx < len(events) and int(events[idx]["at_min"]) == t:
        e = events[idx]
        for k, m in e.get("rate_multipliers", {}).items():
            if k in flows_by_state["f"]:
                cur_flow_mult[k] = float(m)
            else:
                cur_bg_mult[k] = float(m)
        for fk, lm in e.get("latency_multipliers", {}).items():
            cur_flow_lat[fk] = {"p50": float(lm.get("p50", 1.0)), "p95": float(lm.get("p95", 1.0))}
        for os in e.get("one_shots", []) or []:
            one_shots.append({"at_min": int(e["at_min"]), **os})
        idx += 1

    boundaries = [f_start]
    for e in events:
        at = int(e["at_min"])
        if f_start <= at <= f_end:
            boundaries.append(at)
    boundaries.append(f_end)
    boundaries = sorted(set(boundaries))

    for si in range(boundaries.index(f_start), len(boundaries) - 1):
        seg_start = boundaries[si]
        seg_end = boundaries[si + 1]

        while idx < len(events) and int(events[idx]["at_min"]) == seg_start:
            e = events[idx]
            for k, m in e.get("rate_multipliers", {}).items():
                if k in flows_by_state["f"]:
                    cur_flow_mult[k] = float(m)
                else:
                    cur_bg_mult[k] = float(m)
            for fk, lm in e.get("latency_multipliers", {}).items():
                cur_flow_lat[fk] = {"p50": float(lm.get("p50", 1.0)), "p95": float(lm.get("p95", 1.0))}
            for os in e.get("one_shots", []) or []:
                one_shots.append({"at_min": int(e["at_min"]), **os})
            idx += 1

        segments.append({"start_min": seg_start, "end_min": seg_end, "bg_mult": dict(cur_bg_mult), "flow_mult": dict(cur_flow_mult), "flow_lat": dict(cur_flow_lat)})

    segments = [s for s in segments if s["start_min"] < s["end_min"]]
    segments.sort(key=lambda s: s["start_min"])
    return segments, one_shots


failure_segments, failure_one_shots = derive_failure_segments()

# ----------------------------
# Emission engine
# ----------------------------


def pick_component_host(component_id: str, *seed_parts: Any, preferred: Optional[str] = None) -> str:
    comp = component_by_id[component_id]
    hosts = comp.get("hosts", []) or []
    if not hosts:
        return ""
    if preferred is not None and preferred in hosts:
        return preferred
    u = hfloat(*seed_parts, component_id, "host")
    idx = min(int(u * len(hosts)), len(hosts) - 1)
    return hosts[idx]


def render_message(tpl: Template, values: Dict[str, Any]) -> str:
    class _DDict(dict):
        def __missing__(self, key):
            return ""

    return tpl.msg.format_map(_DDict(values))


def schedule_even_times(start_dt: datetime, end_dt: datetime, n: int, key: str) -> List[datetime]:
    if n <= 0:
        return []
    dur_s = (end_dt - start_dt).total_seconds()
    if dur_s <= 0:
        return [start_dt] * n
    out: List[datetime] = []
    for i in range(n):
        frac = (i + 0.5) / n
        jitter = (hfloat(key, i, "jitter") - 0.5) * 0.8  # +/- 0.4s
        offset = frac * dur_s + jitter
        if offset < 0:
            offset = 0.0
        if offset >= dur_s:
            offset = max(dur_s - 0.001, 0.0)
        out.append(start_dt + timedelta(seconds=offset))
    return out


def emit_background(rows: List[Dict[str, Any]], state: str, start_min: int, end_min: int, bg_mult: Optional[Dict[str, float]], allocator: CarryRounding) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    duration_min = (end_dt - start_dt).total_seconds() / 60.0
    bg_mult = bg_mult or {}

    for comp in SYSTEM["components"]:
        cid = comp["id"]
        beh = comp.get("beh", {}).get(state, {})
        emits = beh.get("emit", []) or []
        for e in emits:
            log_id = e["id"]
            per_min = float(e["per_min"])
            scope = e.get("scope", "per_host")
            mult_key = f"{cid}.{log_id}"
            mult = float(bg_mult.get(mult_key, 1.0)) if state == "f" else 1.0
            eff_rate = per_min * mult

            tpl = templates_by_ref[f"{cid}.{log_id}"]
            comp_hosts = comp.get("hosts", []) or []

            if scope == "global":
                expected = eff_rate * duration_min
                key = f"bg|{state}|{mult_key}|{start_min}-{end_min}"
                count = allocator.alloc(key, expected)
                times = schedule_even_times(start_dt, end_dt, count, key)
                for i, ts in enumerate(times):
                    host = pick_component_host(cid, "bg", mult_key, start_min, end_min, i)
                    values: Dict[str, Any] = {}
                    for var_name, dom in tpl.vars.items():
                        values[var_name] = choose_from_domain(dom, "bg", mult_key, start_min, end_min, i, var_name)
                    msg = render_message(tpl, values)
                    rows.append({"timestamp": ts, "level": tpl.level, "message": msg, "trace_id": "", "service": comp.get("svc") or "", "host": host})
            else:
                for h in comp_hosts:
                    expected = eff_rate * duration_min
                    key = f"bg|{state}|{mult_key}|{h}|{start_min}-{end_min}"
                    count = allocator.alloc(key, expected)
                    times = schedule_even_times(start_dt, end_dt, count, key)
                    for i, ts in enumerate(times):
                        values = {}
                        for var_name, dom in tpl.vars.items():
                            if cid == "calico_node" and log_id == "route_withdraw" and var_name == "node":
                                mapping = {"node-cp-1": "cp-1", "node-w-1": "w-1", "node-w-2": "w-2", "node-w-3": "w-3"}
                                values[var_name] = mapping.get(h, choose_from_domain(dom, "bg", mult_key, h, i, var_name))
                            else:
                                values[var_name] = choose_from_domain(dom, "bg", mult_key, h, start_min, end_min, i, var_name)
                        msg = render_message(tpl, values)
                        rows.append({"timestamp": ts, "level": tpl.level, "message": msg, "trace_id": "", "service": comp.get("svc") or "", "host": h})


def plan_attempt_count(expected_attempts: float, max_attempts: int, u: float) -> int:
    max_attempts = max(1, int(max_attempts))
    expected_attempts = max(1.0, min(float(expected_attempts), float(max_attempts)))
    lo = int(math.floor(expected_attempts))
    hi = int(math.ceil(expected_attempts))
    if lo == hi:
        return lo
    p_hi = expected_attempts - lo
    return hi if u < p_hi else lo


def simulate_flow_instance(
    rows: List[Dict[str, Any]],
    flow: Dict[str, Any],
    state: str,
    flow_start: datetime,
    instance_id: int,
    flow_latency_mult: Optional[Dict[str, float]] = None,
) -> None:
    flow_id = flow["id"]
    emit_refs: List[str] = flow["emit"]
    latency_pairs: List[List[float]] = flow["latency_ms"]
    retry = flow.get("retry", {}) or {}
    max_attempts = int(retry.get("max_attempts", 1))
    expected_attempts = float(retry.get("expected_attempts", 1.0))
    trace_on = bool(SYSTEM["tracing"]["on"]) and bool(flow.get("trace", False))
    trace_id = hhex(32, "trace", state, flow_id, instance_id) if trace_on else ""

    req_id = huuid("req", state, flow_id, instance_id)

    if flow_id == "homepage_view_empty_200":
        uri_path = "/"
    else:
        uri_choices = templates_by_ref["edge_gateway.http_request_start"].vars["uri_path"]["v"]
        uri_path = uri_choices[min(int(hfloat("uri", state, flow_id, instance_id) * len(uri_choices)), len(uri_choices) - 1)]

    ua = choose_from_domain(templates_by_ref["edge_gateway.http_request_start"].vars["ua"], "ua", state, flow_id, instance_id)

    host_by_component: Dict[str, str] = {}
    for ref in emit_refs:
        cid, _ = ref.split(".", 1)
        if cid not in host_by_component:
            host_by_component[cid] = pick_component_host(cid, "flow", state, flow_id, instance_id, cid)

    attempts = plan_attempt_count(expected_attempts, max_attempts, hfloat("attempts", state, flow_id, instance_id))

    lat_mult = flow_latency_mult or {"p50": 1.0, "p95": 1.0}
    p50m = float(lat_mult.get("p50", 1.0))
    p95m = float(lat_mult.get("p95", 1.0))

    # Bind coherent outcome-bearing fields for this flow instance
    if flow_id == "homepage_view_edge_5xx":
        err_class = ["no_endpoints", "connect_timeout", "upstream_reset"][min(int(hfloat("err", flow_id, instance_id) * 3), 2)]
        if err_class == "no_endpoints":
            status, upstream_status, bytes_minmax = "503", "-", (50, 900)
        elif err_class == "connect_timeout":
            status, upstream_status, bytes_minmax = "504", "504", (0, 50)
        else:
            status, upstream_status, bytes_minmax = "502", "502", (0, 200)
    elif flow_id == "homepage_view_backend_timeout":
        err_class = "connect_timeout"
        status, upstream_status, bytes_minmax = "504", "504", (0, 150)
    else:
        err_class, status, upstream_status, bytes_minmax = "", "", "", (0, 0)

    # NOTE: Verifier expects latency_ms[j] to be interpreted as a *cumulative* offset
    # from attempt start for emitted log j (not a delta since previous log).
    for attempt in range(1, attempts + 1):
        attempt_start_ts = flow_start

        cum_ms: List[float] = []
        prev_cum = 0.0
        for j, ref in enumerate(emit_refs):
            if j < len(latency_pairs):
                p50, p95 = float(latency_pairs[j][0]), float(latency_pairs[j][1])
            else:
                p50, p95 = (1.0, 2.0)

            if state == "f" and flow_latency_mult is not None:
                p50 *= p50m
                p95 *= p95m

            # Trim quantile to reduce outliers past p95 (important when p95 ~= domain max).
            u_raw = hfloat("lat", state, flow_id, instance_id, attempt, j)
            u = 0.05 + 0.9 * u_raw  # [0.05, 0.95]

            sampled_cum = lognormal_sample_from_p50_p95(p50, p95, u)

            # If this log exposes dur_ms in the message, keep cum time within that domain bounds.
            tpl_j = templates_by_ref[ref]
            dmin, dmax = int_domain_bounds(tpl_j.vars.get("dur_ms"))

            target = sampled_cum
            if dmin is not None:
                target = max(target, float(dmin))
            if dmax is not None:
                target = min(target, float(dmax))

            # Enforce non-decreasing chronology across the chain.
            eps = 0.5  # ms
            if target < prev_cum:
                target = prev_cum
            elif target < prev_cum + eps:
                # Try to advance slightly; but if dmax prevents it, keep it equal.
                target = prev_cum + eps
                if dmax is not None and target > float(dmax):
                    target = prev_cum

            cum_ms.append(target)
            prev_cum = target

        for j, ref in enumerate(emit_refs):
            t = attempt_start_ts + timedelta(milliseconds=cum_ms[j])
            tpl = templates_by_ref[ref]
            cid = tpl.component_id
            host = host_by_component.get(cid, pick_component_host(cid, "flow", state, flow_id, instance_id, cid))

            values: Dict[str, Any] = {}
            if "req_id" in tpl.vars:
                values["req_id"] = req_id
            if "uri_path" in tpl.vars:
                values["uri_path"] = uri_path
            if "endpoint" in tpl.vars:
                values["endpoint"] = uri_path
            if "ua" in tpl.vars:
                values["ua"] = ua

            for var_name, dom in tpl.vars.items():
                if var_name in values:
                    continue
                values[var_name] = choose_from_domain(dom, "flow", state, flow_id, instance_id, attempt, j, var_name)

            if "dur_ms" in tpl.vars:
                dmin, dmax = int_domain_bounds(tpl.vars.get("dur_ms"))
                dur_val = int(round(cum_ms[j]))
                if dmin is not None:
                    dur_val = max(dur_val, dmin)
                if dmax is not None:
                    dur_val = min(dur_val, dmax)
                values["dur_ms"] = dur_val

            if ref == "edge_gateway.http_access_ok":
                values["status"] = "200"
                values["upstream"] = "legacy-app"
                dom = tpl.vars["bytes"]
                lo, hi = dom["v"]
                ubytes = hfloat("bytes", "ok", state, flow_id, instance_id)
                values["bytes"] = int(lo + ubytes * (hi - lo + 1))
            elif ref == "edge_gateway.http_access_empty":
                values["upstream"] = "edge-fallback"
                dom = tpl.vars["bytes"]
                lo, hi = dom["v"]
                ubytes = hfloat("bytes", "empty", state, flow_id, instance_id)
                values["bytes"] = int(lo + ubytes * (hi - lo + 1))
            elif ref == "edge_gateway.http_access_5xx":
                values["upstream"] = "legacy-app"
                values["status"] = status
                values["upstream_status"] = upstream_status
                values["err"] = err_class
                lo, hi = bytes_minmax
                ubytes = hfloat("bytes", "5xx", state, flow_id, instance_id, err_class)
                values["bytes"] = int(lo + ubytes * (hi - lo + 1)) if hi >= lo else lo

            if ref == "legacy_app.req_done":
                values["user_kind"] = choose_from_domain(tpl.vars["user_kind"], "user", state, flow_id, instance_id)

            msg = render_message(tpl, values)
            rows.append({"timestamp": t, "level": tpl.level, "message": msg, "trace_id": trace_id, "service": component_by_id[cid].get("svc") or "", "host": host})


def emit_flows(
    rows: List[Dict[str, Any]],
    state: str,
    start_min: int,
    end_min: int,
    flow_mult: Optional[Dict[str, float]],
    flow_lat: Optional[Dict[str, Dict[str, float]]],
    allocator: CarryRounding,
    sequencer: FlowInstanceSequencer,
) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    duration_min = (end_dt - start_dt).total_seconds() / 60.0
    flow_mult = flow_mult or {}
    flow_lat = flow_lat or {}

    for flow in SYSTEM["flows"][state]["req"]:
        flow_id = flow["id"]
        mult = float(flow_mult.get(flow_id, 1.0)) if state == "f" else 1.0
        rpm_eff = float(flow["rpm"]) * mult
        expected = rpm_eff * duration_min
        key = f"flow|{state}|{flow_id}|{start_min}-{end_min}"
        n_instances = allocator.alloc(key, expected)
        start_times = schedule_even_times(start_dt, end_dt, n_instances, key)

        for st in start_times:
            instance_id = sequencer.next()
            lm = flow_lat.get(flow_id)
            simulate_flow_instance(rows, flow, state, st, instance_id, flow_latency_mult=lm)


def emit_one_shots(rows: List[Dict[str, Any]]) -> None:
    for os in failure_one_shots:
        ref = os["ref"]
        count = int(os["count"])
        at_min = int(os["at_min"])
        allowed_hosts = os.get("hosts", None)

        tpl = templates_by_ref[ref]
        cid = tpl.component_id
        comp = component_by_id[cid]
        comp_hosts = comp.get("hosts", []) or []
        if allowed_hosts:
            eligible = [h for h in comp_hosts if h in allowed_hosts]
        else:
            eligible = comp_hosts
        if not eligible:
            eligible = [""]

        base_dt = BASE_TIME + timedelta(minutes=at_min)
        max_dt = base_dt + timedelta(minutes=1) - timedelta(milliseconds=1)

        for i in range(count):
            jitter_s = hfloat("oneshot", ref, at_min, i) * 10.0  # [0, 10) seconds
            ts = base_dt + timedelta(seconds=jitter_s)
            if ts < base_dt:
                ts = base_dt
            if ts > max_dt:
                ts = max_dt

            host = eligible[min(int(hfloat("oneshot_host", ref, at_min, i) * len(eligible)), len(eligible) - 1)]
            values = {}
            for var_name, dom in tpl.vars.items():
                values[var_name] = choose_from_domain(dom, "oneshot", ref, at_min, i, var_name)
            msg = render_message(tpl, values)
            rows.append({"timestamp": ts, "level": tpl.level, "message": msg, "trace_id": "", "service": comp.get("svc") or "", "host": host})


# ----------------------------
# Run simulation
# ----------------------------


def main() -> None:
    random.seed(SEED)
    np.random.seed(SEED)

    rows: List[Dict[str, Any]] = []
    allocator = CarryRounding()
    sequencer = FlowInstanceSequencer()

    phases = SCENARIO["scenario"]["time"]["phases"]
    n_start, n_end = int(phases["n"]["start_min"]), int(phases["n"]["end_min"])
    f_start, f_end = int(phases["f"]["start_min"]), int(phases["f"]["end_min"])

    emit_background(rows, "n", n_start, n_end, bg_mult=None, allocator=allocator)
    emit_flows(rows, "n", n_start, n_end, flow_mult=None, flow_lat=None, allocator=allocator, sequencer=sequencer)

    for seg in failure_segments:
        s, e = int(seg["start_min"]), int(seg["end_min"])
        emit_background(rows, "f", s, e, bg_mult=seg["bg_mult"], allocator=allocator)
        emit_flows(rows, "f", s, e, flow_mult=seg["flow_mult"], flow_lat=seg["flow_lat"], allocator=allocator, sequencer=sequencer)

    emit_one_shots(rows)

    df = pd.DataFrame(rows)
    df.sort_values("timestamp", inplace=True, kind="mergesort")
    df["timestamp"] = df["timestamp"].apply(fmt_ts)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)

    n_rows = len(df)
    if not (20000 <= n_rows <= 100000):
        raise RuntimeError(f"Row count {n_rows} outside target [20000, 100000].")


if __name__ == "__main__":
    main()
