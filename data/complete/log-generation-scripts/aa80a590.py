import math
import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd

# Deterministic seed (kept for any incidental stdlib random usage)
random.seed(0)

# ----------------------------
# Embedded executable spec
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "id": "ravendb_cloud_tls_split_incident",
    "states": {"n": "normal", "f": "failure"},
    "components": [
        {
            "id": "ravendb_node",
            "svc": "ravendb",
            "hosts": ["rdb-a", "rdb-b", "rdb-c", "rdb-d", "rdb-e", "rdb-f"],
            "logs": {
                "http_access": {
                    "lvl": "INFO",
                    "msg": "HTTP {method} /databases/{db}/{endpoint} -> {status} in {dur_ms}ms",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST", "PUT"]},
                        "db": {"k": "ch", "v": ["orders", "users", "inventory"]},
                        "endpoint": {"k": "ch", "v": ["docs", "queries", "attachments"]},
                        "status": {"k": "i", "v": [200, 204]},
                        "dur_ms": {"k": "i", "v": [2, 160]},
                    },
                },
                "cluster_link_state": {
                    "lvl": "INFO",
                    "msg": "Cluster links: connected={connected} disconnected={disconnected} leader={leader}",
                    "vars": {"leader": {"k": "ch", "v": ["rdb-a", "rdb-b", "rdb-c", "rdb-d", "rdb-e", "rdb-f"]}},
                    "state_vars": {
                        "n": {
                            "connected": {"k": "i", "v": [4, 5]},
                            "disconnected": {"k": "i", "v": [0, 1]},
                        },
                        "f": {
                            "connected": {"k": "i", "v": [0, 4]},
                            "disconnected": {"k": "i", "v": [1, 5]},
                        },
                    },
                },
                "cluster_connect_ok": {
                    "lvl": "INFO",
                    "msg": "Cluster TLS connection established to {peer} proto={proto} handshake_ms={handshake_ms}",
                    "vars": {
                        "peer": {"k": "ch", "v": ["rdb-a", "rdb-b", "rdb-c", "rdb-d", "rdb-e", "rdb-f"]},
                        "proto": {"k": "ch", "v": ["tls1.2", "tls1.3"]},
                        "handshake_ms": {"k": "i", "v": [15, 140]},
                    },
                },
                "cluster_connect_attempt": {
                    "lvl": "INFO",
                    "msg": "Attempting cluster TLS connection to {peer} attempt={attempt}",
                    "vars": {
                        "peer": {"k": "ch", "v": ["rdb-a", "rdb-b", "rdb-c", "rdb-d", "rdb-e", "rdb-f"]},
                        "attempt": {"k": "i", "v": [1, 3]},
                    },
                },
                "cluster_connect_retry": {
                    "lvl": "WARN",
                    "msg": "Retrying cluster connection to {peer} in {backoff_ms}ms (attempt {attempt}/{max_attempts})",
                    "vars": {
                        "peer": {"k": "ch", "v": ["rdb-a", "rdb-b", "rdb-c", "rdb-d", "rdb-e", "rdb-f"]},
                        "backoff_ms": {"k": "i", "v": [100, 1200]},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "max_attempts": {"k": "i", "v": [3, 3]},
                    },
                },
                "cluster_peer_untrusted": {
                    "lvl": "WARN",
                    "msg": "Cluster connection rejected: peer_cert_untrusted peer={peer} err={err} chain={chain} elapsed_ms={elapsed_ms}",
                    "vars": {
                        "peer": {"k": "ch", "v": ["rdb-a", "rdb-b", "rdb-c", "rdb-d", "rdb-e", "rdb-f"]},
                        "err": {
                            "k": "ch",
                            "v": [
                                "unable to get local issuer certificate",
                                "remote certificate is invalid because of errors in the certificate chain",
                            ],
                        },
                        "chain": {"k": "ch", "v": ["dst_root_ca_x3"]},
                        "elapsed_ms": {"k": "i", "v": [120, 1600]},
                    },
                },
                "inbound_tls_handshake_failed": {
                    "lvl": "WARN",
                    "msg": "Inbound TLS handshake failed peer={client_ip}:{client_port} sni={sni} alert={alert} reason={reason}",
                    "vars": {
                        "client_ip": {"k": "ip", "v": "10.0.0.0/8"},
                        "client_port": {"k": "i", "v": [1024, 65535]},
                        "sni": {"k": "str", "v": "cluster-or-customer-domain"},
                        "alert": {"k": "ch", "v": ["unknown_ca", "bad_certificate", "handshake_failure", "internal_error"]},
                        "reason": {"k": "ch", "v": ["tls_alert", "eof", "timeout"]},
                    },
                },
                "cluster_command_ok": {
                    "lvl": "INFO",
                    "msg": "Cluster command {cmd} completed in {dur_ms}ms",
                    "vars": {"cmd": {"k": "ch", "v": ["DeployIndex", "UpdateTopology"]}, "dur_ms": {"k": "i", "v": [40, 900]}},
                },
                "raft_quorum_lost": {
                    "lvl": "ERROR",
                    "msg": "Cluster command {cmd} failed: no quorum (required={required} reachable={reachable}) wait_ms={wait_ms}",
                    "vars": {
                        "cmd": {"k": "ch", "v": ["DeployIndex", "UpdateTopology"]},
                        "required": {"k": "i", "v": [3, 4]},
                        "reachable": {"k": "i", "v": [0, 3]},
                        "wait_ms": {"k": "i", "v": [500, 7000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "cluster_link_state", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "cluster_link_state", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "host_os",
            "svc": None,
            "hosts": ["rdb-a", "rdb-b", "rdb-c", "rdb-d", "rdb-e", "rdb-f"],
            "logs": {
                "unattended_upgrades": {
                    "lvl": "INFO",
                    "msg": "unattended-upgrades: packages upgraded={count}",
                    "vars": {"count": {"k": "i", "v": [0, 6]}},
                },
                "ca_certs_update": {
                    "lvl": "INFO",
                    "msg": "apt: installed ca-certificates {ver}; trust anchor removed={anchor}",
                    "vars": {"ver": {"k": "ch", "v": ["2021.09.01", "2021.09.10"]}, "anchor": {"k": "ch", "v": ["DST Root CA X3"]}},
                },
                "trust_store_refresh": {
                    "lvl": "INFO",
                    "msg": "update-ca-certificates: added={added} removed={removed}",
                    "vars": {"added": {"k": "i", "v": [0, 2]}, "removed": {"k": "i", "v": [0, 2]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "unattended_upgrades", "per_min": 0.05, "scope": "per_host"}]},
                "f": {"emit": [{"id": "unattended_upgrades", "per_min": 0.05, "scope": "per_host"}]},
            },
        },
        {
            "id": "monitoring",
            "svc": "monitoring",
            "hosts": ["mon-1"],
            "logs": {
                "probe_ok": {
                    "lvl": "INFO",
                    "msg": "probe target={target} result=ok lat_ms={lat_ms}",
                    "vars": {"target": {"k": "ch", "v": ["rdb-a", "rdb-b", "rdb-c", "rdb-d", "rdb-e", "rdb-f"]}, "lat_ms": {"k": "i", "v": [5, 120]}},
                },
                "probe_tls_fail": {
                    "lvl": "WARN",
                    "msg": "probe target={target} result=tls_error err={err}",
                    "vars": {
                        "target": {"k": "ch", "v": ["rdb-a", "rdb-b", "rdb-c", "rdb-d", "rdb-e", "rdb-f"]},
                        "err": {"k": "ch", "v": ["unable to get local issuer certificate", "certificate chain incomplete"]},
                    },
                },
                "alert_cluster_connectivity": {
                    "lvl": "CRITICAL",
                    "msg": "ALERT cluster-connectivity degraded clusters_affected={clusters} nodes_affected={nodes}",
                    "vars": {"clusters": {"k": "i", "v": [10, 80]}, "nodes": {"k": "i", "v": [20, 240]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "probe_ok", "per_min": 10.0, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "probe_ok", "per_min": 8.0, "scope": "global"},
                        {"id": "probe_tls_fail", "per_min": 6.0, "scope": "global"},
                        {"id": "alert_cluster_connectivity", "per_min": 1.0, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "ops_automation",
            "svc": "ops",
            "hosts": ["ops-1"],
            "logs": {
                "rollout_status": {
                    "lvl": "INFO",
                    "msg": "rollout {plan} progress={done}/{total} phase={phase}",
                    "vars": {
                        "plan": {"k": "ch", "v": ["ca-certificates-hotfix"]},
                        "done": {"k": "i", "v": [0, 200]},
                        "total": {"k": "i", "v": [200, 200]},
                        "phase": {"k": "ch", "v": ["prepare", "apply", "verify"]},
                    },
                },
                "cmd_exec": {
                    "lvl": "INFO",
                    "msg": "ops exec target={target} cmd='{cmd}' result={result}",
                    "vars": {
                        "target": {"k": "ch", "v": ["rdb-a", "rdb-b", "rdb-c", "rdb-d", "rdb-e", "rdb-f"]},
                        "cmd": {
                            "k": "ch",
                            "v": [
                                "sudo update-ca-certificates --fresh",
                                "rm ~/.dotnet/corefx/cryptography/x509stores/ca/DAC9024F54D8F6DF94935FB1732638CA6AD77C13.pfx",
                                "rm ~/.dotnet/corefx/cryptography/x509stores/ca/48504E974C0DAC5B5CD476C8202274B24C8C7172.pfx",
                            ],
                        },
                        "result": {"k": "ch", "v": ["ok", "failed"]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "rollout_status", "per_min": 0.0, "scope": "global"}]},
                "f": {"emit": [{"id": "rollout_status", "per_min": 2.0, "scope": "global"}]},
            },
        },
    ],
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "flows": {
        "n": [
            {
                "id": "client_api_request_n",
                "rpm": 900.0,
                "emit": ["ravendb_node.http_access"],
                "latency_ms": [[8, 45]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "cluster_connect_n",
                "rpm": 18.0,
                "emit": ["ravendb_node.cluster_connect_ok"],
                "latency_ms": [[25, 110]],
                "retry": {
                    "max_attempts": 3,
                    "expected_attempts": 1.07,
                    "emit_per_retry": ["ravendb_node.cluster_connect_retry"],
                    "backoff_ms": [[120, 260], [240, 520]],
                },
                "trace": False,
            },
            {
                "id": "cluster_task_deploy_index_n",
                "rpm": 2.0,
                "emit": ["ravendb_node.cluster_command_ok"],
                "latency_ms": [[120, 900]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
        "f": [
            {
                "id": "client_api_request_f",
                "rpm": 850.0,
                "emit": ["ravendb_node.http_access"],
                "latency_ms": [[10, 60]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "cluster_connect_tls_fail_f",
                "rpm": 15.0,
                "emit": ["ravendb_node.cluster_connect_attempt", "ravendb_node.cluster_peer_untrusted"],
                "latency_ms": [[2, 8], [220, 1050]],
                "retry": {
                    "max_attempts": 3,
                    "expected_attempts": 2.2,
                    "emit_per_retry": ["ravendb_node.cluster_connect_retry"],
                    "backoff_ms": [[220, 900], [450, 1200]],
                },
                "trace": False,
            },
            {
                "id": "inbound_client_handshake_fail_f",
                "rpm": 1.0,
                "emit": ["ravendb_node.inbound_tls_handshake_failed"],
                "latency_ms": [[20, 160]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "cluster_task_deploy_index_fail_f",
                "rpm": 3.0,
                "emit": ["ravendb_node.raft_quorum_lost"],
                "latency_ms": [[900, 6500]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "ravendb_cloud_ca_trust_store_split_2021_09_24",
    "time": {"total_minutes": 32, "phases": {"n": {"start_min": 0, "end_min": 16}, "f": {"start_min": 16, "end_min": 32}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 16,
                    "rate_multipliers": {
                        "cluster_task_deploy_index_fail_f": 0.0,
                        "ops_automation.rollout_status": 0.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "host_os.ca_certs_update", "count": 3, "hosts": ["rdb-b", "rdb-e", "rdb-f"]}],
                },
                {
                    "order": 2,
                    "at_min": 22,
                    "rate_multipliers": {
                        "cluster_connect_tls_fail_f": 1.8,
                        "cluster_task_deploy_index_fail_f": 1.4,
                        "monitoring.probe_tls_fail": 1.7,
                        "monitoring.alert_cluster_connectivity": 1.5,
                    },
                    "latency_multipliers": {"cluster_task_deploy_index_fail_f": {"p50": 1.3, "p95": 1.4}},
                    "one_shots": [],
                },
                {
                    "order": 3,
                    "at_min": 28,
                    "rate_multipliers": {
                        "cluster_connect_tls_fail_f": 0.6,
                        "inbound_client_handshake_fail_f": 0.6,
                        "monitoring.probe_tls_fail": 0.7,
                        "monitoring.alert_cluster_connectivity": 0.8,
                        "ops_automation.rollout_status": 1.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "ops_automation.cmd_exec", "count": 9, "hosts": ["ops-1"]},
                        {"ref": "host_os.trust_store_refresh", "count": 3, "hosts": ["rdb-b", "rdb-e", "rdb-f"]},
                    ],
                },
            ]
        }
    },
}

# ----------------------------
# Deterministic helpers
# ----------------------------

BASE_TIME = datetime(2021, 9, 24, 0, 0, 0, tzinfo=timezone.utc)


def _hash_u64(*parts: Any) -> int:
    s = "|".join(str(p) for p in parts).encode("utf-8")
    d = hashlib.sha256(s).digest()
    return int.from_bytes(d[:8], "big", signed=False)


def u01(*parts: Any) -> float:
    return (_hash_u64(*parts) & ((1 << 53) - 1)) / float(1 << 53)


def choose_from_list(values: List[Any], *parts: Any) -> Any:
    if not values:
        return ""
    idx = int(u01(*parts) * len(values))
    if idx >= len(values):
        idx = len(values) - 1
    return values[idx]


def rand_int_inclusive(a: int, b: int, *parts: Any) -> int:
    if b < a:
        a, b = b, a
    span = b - a + 1
    x = int(u01(*parts) * span)
    if x >= span:
        x = span - 1
    return a + x


def rand_ip_10_8(*parts: Any) -> str:
    x = _hash_u64(*parts) & ((1 << 24) - 1)
    b = (x >> 16) & 0xFF
    c = (x >> 8) & 0xFF
    d = x & 0xFF
    return f"10.{b}.{c}.{d}"


# Acklam inverse normal CDF approximation
def inv_norm_cdf(p: float) -> float:
    if p <= 0.0:
        return float("-inf")
    if p >= 1.0:
        return float("inf")

    a = [
        -3.969683028665376e01,
        2.209460984245205e02,
        -2.759285104469687e02,
        1.383577518672690e02,
        -3.066479806614716e01,
        2.506628277459239e00,
    ]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]

    plow = 0.02425
    phigh = 1 - plow

    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return num / den
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return -(num / den)

    q = p - 0.5
    r = q * q
    num = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    den = (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    return num / den


def sample_lognormal_ms(
    p50: float,
    p95: float,
    u: float,
    soft_cap_mult: float = 2.5,
    hard_min: Optional[float] = None,
    hard_max: Optional[float] = None,
) -> float:
    p50 = max(1e-3, float(p50))
    p95 = max(p50 * 1.0001, float(p95))
    sigma = math.log(p95 / p50) / 1.6448536269514722
    mu = math.log(p50)

    p = min(1.0 - 1e-9, max(1e-9, u))
    z = inv_norm_cdf(p)
    x = math.exp(mu + sigma * z)

    cap = soft_cap_mult * p95
    if hard_max is not None:
        cap = min(cap, float(hard_max))
    if x > cap:
        x = cap * (1.0 + 0.02 * (p - 0.5))

    if hard_min is not None:
        x = max(float(hard_min), x)
    if hard_max is not None:
        x = min(float(hard_max), x)
    return x


def fmt_ts(dt: datetime) -> str:
    ms = int(dt.microsecond / 1000)
    dt2 = dt.replace(microsecond=ms * 1000)
    return dt2.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


class CarryRounding:
    def __init__(self) -> None:
        self.carry: Dict[str, float] = {}

    def alloc(self, key: str, expected: float) -> int:
        c = self.carry.get(key, 0.0)
        x = expected + c
        n = int(math.floor(x + 1e-12))
        self.carry[key] = x - n
        return n


def spread_times(min_start: datetime, count: int, jitter_ms: int, *parts: Any, **kwargs: Any) -> List[datetime]:
    if kwargs:
        kv = []
        for k in sorted(kwargs.keys()):
            kv.append(k)
            kv.append(kwargs[k])
        parts = tuple(list(parts) + kv)

    if count <= 0:
        return []
    end = min_start + timedelta(minutes=1)
    dur = (end - min_start).total_seconds()
    out: List[datetime] = []
    for i in range(count):
        frac = (i + 0.5) / count
        base = min_start + timedelta(seconds=dur * frac)
        j = (u01(*parts, "jitter", i) - 0.5) * 2.0 * (jitter_ms / 1000.0)
        t = base + timedelta(seconds=j)
        if t < min_start:
            t = min_start
        if t >= end:
            t = end - timedelta(milliseconds=1)
        out.append(t)
    return out


def clamp_int(x: int, lo: int, hi: int) -> int:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


# ----------------------------
# Build indices
# ----------------------------

COMP: Dict[str, Any] = {c["id"]: c for c in SYSTEM["components"]}

LOGS: Dict[str, Dict[str, Any]] = {}
for comp_id, comp in COMP.items():
    for log_id, tmpl in comp["logs"].items():
        LOGS[f"{comp_id}.{log_id}"] = {"component_id": comp_id, "log_id": log_id, **tmpl}

FLOWS: Dict[str, Dict[str, Any]] = {"n": {}, "f": {}}
for st in ("n", "f"):
    for flow in SYSTEM["flows"][st]:
        FLOWS[st][flow["id"]] = flow


def comp_identity(component_id: str) -> Tuple[str, List[str]]:
    c = COMP[component_id]
    svc = c["svc"] or ""
    hosts = c.get("hosts", []) or []
    return svc, hosts


def get_int_var_range(template_key: str, field: str) -> Optional[Tuple[int, int]]:
    tmpl = LOGS.get(template_key)
    if not tmpl:
        return None
    dom = (tmpl.get("vars", {}) or {}).get(field)
    if not dom:
        return None
    if dom.get("k") != "i":
        return None
    v = dom.get("v")
    if not isinstance(v, (list, tuple)) or len(v) != 2:
        return None
    return int(v[0]), int(v[1])


# ----------------------------
# Scenario controls (persistent)
# ----------------------------

F_START = SCENARIO["time"]["phases"]["f"]["start_min"]
F_END = SCENARIO["time"]["phases"]["f"]["end_min"]
EVENTS = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e.get("order", 0)))

active_rate_mult: Dict[int, Dict[str, float]] = {}
active_lat_mult: Dict[int, Dict[str, Dict[str, float]]] = {}

cur_rate: Dict[str, float] = {}
cur_lat: Dict[str, Dict[str, float]] = {}
ei = 0
for m in range(F_START, F_END):
    while ei < len(EVENTS) and EVENTS[ei]["at_min"] <= m:
        e = EVENTS[ei]
        for k, v in e.get("rate_multipliers", {}).items():
            cur_rate[k] = float(v)
        for k, v in e.get("latency_multipliers", {}).items():
            cur_lat[k] = {"p50": float(v.get("p50", 1.0)), "p95": float(v.get("p95", 1.0))}
        ei += 1
    active_rate_mult[m] = dict(cur_rate)
    active_lat_mult[m] = dict(cur_lat)


def get_rate_mult(minute: int, key: str) -> float:
    if minute < F_START:
        return 1.0
    return active_rate_mult.get(minute, {}).get(key, 1.0)


def get_latency_mult(minute: int, flow_id: str) -> Tuple[float, float]:
    if minute < F_START:
        return 1.0, 1.0
    lm = active_lat_mult.get(minute, {}).get(flow_id)
    if not lm:
        return 1.0, 1.0
    return float(lm.get("p50", 1.0)), float(lm.get("p95", 1.0))


# ----------------------------
# Rendering
# ----------------------------

def sample_from_domain(domain: Dict[str, Any], state: str, key_parts: Tuple[Any, ...]) -> Any:
    k = domain.get("k")
    v = domain.get("v")
    if k == "ch":
        return choose_from_list(list(v), *key_parts)
    if k == "i":
        a, b = int(v[0]), int(v[1])
        return rand_int_inclusive(a, b, *key_parts)
    if k == "f":
        a, b = float(v[0]), float(v[1])
        return a + (b - a) * u01(*key_parts)
    if k == "hex":
        n = int(v)
        raw = hashlib.sha256(("|".join(str(p) for p in key_parts)).encode("utf-8")).hexdigest()
        return raw[:n].lower()
    if k == "ip":
        return rand_ip_10_8(*key_parts)
    if k == "str":
        return str(v)
    if k == "uuid":
        raw = hashlib.sha256(("|".join(str(p) for p in key_parts)).encode("utf-8")).hexdigest()
        return f"{raw[:8]}-{raw[8:12]}-4{raw[13:16]}-{raw[16:20]}-{raw[20:32]}"
    return ""


def render_log(template_key: str, state: str, bind: Dict[str, Any], key_parts: Tuple[Any, ...]) -> Tuple[str, str]:
    tmpl = LOGS[template_key]
    vars_def = tmpl.get("vars", {}) or {}
    state_vars_def = (tmpl.get("state_vars", {}) or {}).get(state, {}) or {}

    vals: Dict[str, Any] = {}
    for name, dom in state_vars_def.items():
        vals[name] = sample_from_domain(dom, state, key_parts + (template_key, "state_var", name))
    for name, dom in vars_def.items():
        vals[name] = sample_from_domain(dom, state, key_parts + (template_key, "var", name))
    for k, v in bind.items():
        vals[k] = v

    msg = tmpl["msg"].format_map(vals)
    lvl = tmpl["lvl"]
    return lvl, msg


# ----------------------------
# Simulation
# ----------------------------

def attempt_count(flow_id: str, expected: float, max_attempts: int, instance_idx: int) -> int:
    e = max(1.0, min(float(expected), float(max_attempts)))
    lo = int(math.floor(e + 1e-12))
    hi = int(min(max_attempts, lo + 1))
    if abs(e - lo) < 1e-9 or hi == lo:
        return lo
    p_hi = e - lo
    u = u01("attempt_mix", flow_id, instance_idx)
    return hi if u < p_hi else lo


def pick_host(component_id: str, idx: int) -> str:
    _, hosts = comp_identity(component_id)
    if not hosts:
        return ""
    return hosts[idx % len(hosts)]


def pick_peer(src_host: str, idx: int, hosts: List[str]) -> str:
    if not hosts:
        return ""
    if len(hosts) == 1:
        return hosts[0]
    base = idx % len(hosts)
    peer = hosts[base]
    if peer == src_host:
        peer = hosts[(base + 1) % len(hosts)]
    return peer


# For per-log delay caps: cap the delay that will be bound into message timing fields.
DELAY_FIELD_BY_TEMPLATE: Dict[str, str] = {
    "ravendb_node.http_access": "dur_ms",
    "ravendb_node.cluster_connect_ok": "handshake_ms",
    "ravendb_node.cluster_command_ok": "dur_ms",
    "ravendb_node.raft_quorum_lost": "wait_ms",
}


def simulate_flow_instance(
    minute: int,
    flow: Dict[str, Any],
    start_time: datetime,
    instance_idx: int,
    out_rows: List[Dict[str, Any]],
) -> None:
    flow_id = flow["id"]
    state = "n" if minute < F_START else "f"
    trace_id = ""  # tracing off

    emit_keys: List[str] = list(flow["emit"])
    lat_pairs: List[List[float]] = list(flow["latency_ms"])
    retry = flow["retry"]
    max_attempts = int(retry["max_attempts"])
    exp_attempts = float(retry["expected_attempts"])
    retry_emit_keys: List[str] = list(retry.get("emit_per_retry", []) or [])
    backoff_pairs: List[List[float]] = list(retry.get("backoff_ms", []) or [])

    comp_host: Dict[str, str] = {}

    def host_for_component(cid: str) -> str:
        if cid not in comp_host:
            comp_host[cid] = pick_host(cid, instance_idx)
        return comp_host[cid]

    p50_mult, p95_mult = get_latency_mult(minute, flow_id)

    rav_hosts = COMP["ravendb_node"]["hosts"]
    src_node = host_for_component("ravendb_node")
    peer_node = pick_peer(src_node, instance_idx, rav_hosts)
    proto = choose_from_list(["tls1.2", "tls1.3"], "proto", flow_id, instance_idx)
    http_method = choose_from_list(["GET", "POST", "PUT"], "method", flow_id, instance_idx)
    http_db = choose_from_list(["orders", "users", "inventory"], "db", flow_id, instance_idx)
    http_ep = choose_from_list(["docs", "queries", "attachments"], "endpoint", flow_id, instance_idx)
    http_status = 200 if u01("status", flow_id, instance_idx) < 0.88 else 204

    untrusted_err = choose_from_list(
        [
            "unable to get local issuer certificate",
            "remote certificate is invalid because of errors in the certificate chain",
        ],
        "untrusted_err",
        flow_id,
        instance_idx,
    )
    untrusted_chain = "dst_root_ca_x3"

    A = attempt_count(flow_id, exp_attempts, max_attempts, instance_idx)

    backoff_range = get_int_var_range("ravendb_node.cluster_connect_retry", "backoff_ms") or (0, 10**9)
    elapsed_range = get_int_var_range("ravendb_node.cluster_peer_untrusted", "elapsed_ms") or (0, 10**9)

    t_attempt_start = start_time
    for a in range(1, A + 1):
        delays_int: List[int] = []
        for li, pair in enumerate(lat_pairs):
            p50, p95 = float(pair[0]), float(pair[1])
            p50s = p50 * p50_mult
            p95s = p95 * p95_mult
            u = u01("lat", flow_id, instance_idx, a, li)

            hard_min: Optional[int] = None
            hard_max: Optional[int] = None
            if li < len(emit_keys):
                field = DELAY_FIELD_BY_TEMPLATE.get(emit_keys[li])
                if field:
                    r = get_int_var_range(emit_keys[li], field)
                    if r:
                        hard_min, hard_max = r[0], r[1]

            x = sample_lognormal_ms(p50s, p95s, u, soft_cap_mult=2.8, hard_min=hard_min, hard_max=hard_max)
            xi = int(round(x))

            if hard_min is not None and hard_max is not None:
                xi = clamp_int(xi, hard_min, hard_max)
            else:
                if xi <= 0:
                    xi = 1
            delays_int.append(xi)

        # Ensure elapsed_ms coheres with timestamp gaps for untrusted flow.
        if flow_id == "cluster_connect_tls_fail_f" and len(delays_int) >= 2:
            min_elapsed, max_elapsed = elapsed_range
            s = delays_int[0] + delays_int[1]
            if s > max_elapsed:
                delays_int[1] = max(1, max_elapsed - delays_int[0])
                s = delays_int[0] + delays_int[1]
            if s < min_elapsed:
                delays_int[1] += (min_elapsed - s)
                s = delays_int[0] + delays_int[1]
                if s > max_elapsed:
                    delays_int[1] = max(1, max_elapsed - delays_int[0])

        t_prev = t_attempt_start
        for li, tmpl_key in enumerate(emit_keys):
            tmpl = LOGS[tmpl_key]
            cid = tmpl["component_id"]
            svc, _ = comp_identity(cid)
            host = host_for_component(cid)

            t_log = t_prev + timedelta(milliseconds=delays_int[li])

            # Fix for verifier S5:
            # cluster_connect_n only has a terminal "connection established" log; if a retry occurs,
            # emitting it on non-final attempts implies success followed by retry. We therefore emit
            # cluster_connect_ok only on the final attempt, while still advancing time for earlier
            # attempts to preserve backoff chronology.
            if flow_id == "cluster_connect_n" and tmpl_key == "ravendb_node.cluster_connect_ok" and a < A:
                t_prev = t_log
                continue

            bind: Dict[str, Any] = {}
            if tmpl_key == "ravendb_node.http_access":
                r = get_int_var_range(tmpl_key, "dur_ms") or (2, 160)
                dur = clamp_int(delays_int[li], r[0], r[1])
                bind = {
                    "method": http_method,
                    "db": http_db,
                    "endpoint": http_ep,
                    "status": http_status,
                    "dur_ms": dur,
                }
            elif tmpl_key == "ravendb_node.cluster_connect_ok":
                r = get_int_var_range(tmpl_key, "handshake_ms") or (15, 140)
                hms = clamp_int(delays_int[li], r[0], r[1])
                bind = {"peer": peer_node, "proto": proto, "handshake_ms": hms}
            elif tmpl_key == "ravendb_node.cluster_connect_attempt":
                bind = {"peer": peer_node, "attempt": a}
            elif tmpl_key == "ravendb_node.cluster_peer_untrusted":
                elapsed_total = sum(delays_int[: li + 1])
                elapsed_total = clamp_int(elapsed_total, elapsed_range[0], elapsed_range[1])
                bind = {"peer": peer_node, "err": untrusted_err, "chain": untrusted_chain, "elapsed_ms": elapsed_total}
            elif tmpl_key == "ravendb_node.cluster_command_ok":
                r = get_int_var_range(tmpl_key, "dur_ms") or (40, 900)
                dur = clamp_int(delays_int[li], r[0], r[1])
                cmd = choose_from_list(["DeployIndex", "UpdateTopology"], "cmd_ok", flow_id, instance_idx)
                bind = {"cmd": cmd, "dur_ms": dur}
            elif tmpl_key == "ravendb_node.raft_quorum_lost":
                r = get_int_var_range(tmpl_key, "wait_ms") or (500, 7000)
                wait_ms = clamp_int(delays_int[li], r[0], r[1])
                cmd = choose_from_list(["DeployIndex", "UpdateTopology"], "cmd_fail", flow_id, instance_idx)
                required = 4 if u01("required", flow_id, instance_idx) < 0.35 else 3
                reachable = rand_int_inclusive(0, max(0, required - 1), "reachable", flow_id, instance_idx)
                bind = {"cmd": cmd, "required": required, "reachable": reachable, "wait_ms": wait_ms}
            elif tmpl_key == "ravendb_node.inbound_tls_handshake_failed":
                bind = {
                    "client_ip": rand_ip_10_8("client_ip", flow_id, instance_idx),
                    "client_port": rand_int_inclusive(1024, 65535, "client_port", flow_id, instance_idx),
                    "sni": "cluster-or-customer-domain",
                    "alert": choose_from_list(["unknown_ca", "bad_certificate", "handshake_failure", "internal_error"], "alert", flow_id, instance_idx),
                    "reason": choose_from_list(["tls_alert", "eof", "timeout"], "reason", flow_id, instance_idx),
                }

            lvl, msg = render_log(tmpl_key, state, bind, (minute, flow_id, instance_idx, a, li))
            out_rows.append({"ts": t_log, "level": lvl, "message": msg, "trace_id": trace_id, "service": svc, "host": host})
            t_prev = t_log

        if a < A and retry_emit_keys:
            backoff_idx = min(a - 1, len(backoff_pairs) - 1) if backoff_pairs else 0
            if backoff_pairs:
                bp50, bp95 = float(backoff_pairs[backoff_idx][0]), float(backoff_pairs[backoff_idx][1])
            else:
                bp50, bp95 = 200.0, 500.0

            u = u01("backoff", flow_id, instance_idx, a + 1)
            b = sample_lognormal_ms(bp50, bp95, u, soft_cap_mult=2.7, hard_min=backoff_range[0], hard_max=backoff_range[1])
            backoff_ms = clamp_int(int(round(b)), backoff_range[0], backoff_range[1])

            t_retry = t_prev + timedelta(milliseconds=1)
            for ri, retry_tmpl_key in enumerate(retry_emit_keys):
                rt = LOGS[retry_tmpl_key]
                cid = rt["component_id"]
                svc, _ = comp_identity(cid)
                host = host_for_component(cid)
                bind = {"peer": peer_node, "backoff_ms": backoff_ms, "attempt": a + 1, "max_attempts": max_attempts}
                lvl, msg = render_log(retry_tmpl_key, state, bind, (minute, flow_id, instance_idx, a, "retry", ri))
                out_rows.append({"ts": t_retry, "level": lvl, "message": msg, "trace_id": trace_id, "service": svc, "host": host})

            t_attempt_start = t_retry + timedelta(milliseconds=backoff_ms)
        else:
            t_attempt_start = t_prev


def emit_background_for_minute(minute: int, rounding: CarryRounding, out_rows: List[Dict[str, Any]]) -> None:
    state = "n" if minute < F_START else "f"
    min_start = BASE_TIME + timedelta(minutes=minute)

    for comp_id in sorted(COMP.keys()):
        comp = COMP[comp_id]
        beh = comp.get("beh", {}).get(state, {})
        emits = beh.get("emit", []) or []
        for spec in emits:
            log_id = spec["id"]
            tmpl_key = f"{comp_id}.{log_id}"
            per_min = float(spec.get("per_min", 0.0))
            scope = spec.get("scope", "per_host") or "per_host"
            mult_key = f"{comp_id}.{log_id}"
            mult = get_rate_mult(minute, mult_key) if state == "f" else 1.0
            eff = per_min * mult

            svc, hosts = comp_identity(comp_id)
            if scope == "global":
                count = rounding.alloc(f"bg|{state}|{tmpl_key}|global", eff)
                times = spread_times(min_start, count, 250, minute, tmpl_key, "global")
                for j, t in enumerate(times):
                    host = hosts[j % len(hosts)] if hosts else ""
                    bind: Dict[str, Any] = {}

                    if tmpl_key == "monitoring.probe_ok":
                        bind["target"] = choose_from_list(["rdb-a", "rdb-b", "rdb-c", "rdb-d", "rdb-e", "rdb-f"], "probe_target_ok", minute, j)
                        bind["lat_ms"] = rand_int_inclusive(5, 120, "probe_lat", minute, j, tmpl_key)
                    elif tmpl_key == "monitoring.probe_tls_fail":
                        bind["target"] = choose_from_list(["rdb-a", "rdb-b", "rdb-c", "rdb-d", "rdb-e", "rdb-f"], "probe_target_fail", minute, j)
                        bind["err"] = choose_from_list(["unable to get local issuer certificate", "certificate chain incomplete"], "probe_err", minute, j)
                    elif tmpl_key == "monitoring.alert_cluster_connectivity":
                        bind["clusters"] = rand_int_inclusive(10, 80, "clusters", minute, j)
                        bind["nodes"] = rand_int_inclusive(20, 240, "nodes", minute, j)
                    elif tmpl_key == "ops_automation.rollout_status":
                        total = 200
                        done_base = min(total, int(round((minute - 28 + 1) / 4.0 * total)))
                        jitter = rand_int_inclusive(-8, 8, "rollout_done_j", minute, j)
                        done = max(0, min(total, done_base + jitter))
                        bind = {
                            "plan": "ca-certificates-hotfix",
                            "done": done,
                            "total": total,
                            "phase": choose_from_list(["prepare", "apply", "verify"], "rollout_phase", minute, j),
                        }

                    lvl, msg = render_log(tmpl_key, state, bind, (minute, comp_id, log_id, "bg", "global", j))
                    out_rows.append({"ts": t, "level": lvl, "message": msg, "trace_id": "", "service": svc, "host": host})

            else:  # per_host default
                if not hosts:
                    continue
                for h in hosts:
                    count = rounding.alloc(f"bg|{state}|{tmpl_key}|host|{h}", eff)
                    times = spread_times(min_start, count, 250, minute, tmpl_key, h)
                    for j, t in enumerate(times):
                        bind = {}
                        if tmpl_key == "ravendb_node.cluster_link_state":
                            if state == "n":
                                connected = 5 if u01("cls", minute, h, j) < 0.75 else 4
                            else:
                                connected = rand_int_inclusive(0, 4, "cls_f_conn", minute, h, j)
                            disconnected = 5 - connected
                            if state == "n":
                                disconnected = 0 if disconnected <= 0 else 1
                                connected = 5 - disconnected
                            else:
                                disconnected = max(1, min(5, disconnected))
                                connected = max(0, min(4, 5 - disconnected))
                            bind = {
                                "connected": connected,
                                "disconnected": disconnected,
                                "leader": choose_from_list(["rdb-a", "rdb-b", "rdb-c", "rdb-d", "rdb-e", "rdb-f"], "leader", minute, h),
                            }
                        elif tmpl_key == "host_os.unattended_upgrades":
                            bind = {"count": rand_int_inclusive(0, 6, "uu_count", minute, h, j)}

                        lvl, msg = render_log(tmpl_key, state, bind, (minute, comp_id, log_id, "bg", h, j))
                        out_rows.append({"ts": t, "level": lvl, "message": msg, "trace_id": "", "service": svc, "host": h})


def emit_flows_for_minute(minute: int, rounding: CarryRounding, flow_instance_counters: Dict[str, int], out_rows: List[Dict[str, Any]]) -> None:
    state = "n" if minute < F_START else "f"
    min_start = BASE_TIME + timedelta(minutes=minute)

    flows = SYSTEM["flows"][state]
    for flow in flows:
        flow_id = flow["id"]
        rpm = float(flow["rpm"])
        mult = get_rate_mult(minute, flow_id) if state == "f" else 1.0
        eff = rpm * mult

        count = rounding.alloc(f"flow|{state}|{flow_id}", eff)
        starts = spread_times(min_start, count, 180, minute, flow_id)
        for st in starts:
            instance_idx = flow_instance_counters.get(flow_id, 0)
            flow_instance_counters[flow_id] = instance_idx + 1
            simulate_flow_instance(minute, flow, st, instance_idx, out_rows)


def emit_one_shots(out_rows: List[Dict[str, Any]]) -> None:
    for e in EVENTS:
        at_min = int(e["at_min"])
        state = "f"
        base_t = BASE_TIME + timedelta(minutes=at_min)
        for shot in e.get("one_shots", []) or []:
            ref = shot["ref"]
            count = int(shot["count"])
            allowed_hosts = list(shot.get("hosts") or [])

            tmpl = LOGS[ref]
            comp_id = tmpl["component_id"]
            svc, comp_hosts = comp_identity(comp_id)

            for i in range(count):
                off_s = (i * 0.7) + (u01("oneshot_j", ref, at_min, i) * 3.0)
                t = base_t + timedelta(seconds=off_s)

                if allowed_hosts:
                    host = allowed_hosts[i % len(allowed_hosts)]
                else:
                    host = comp_hosts[i % len(comp_hosts)] if comp_hosts else ""

                bind: Dict[str, Any] = {}
                if ref == "ops_automation.cmd_exec":
                    bind = {
                        "target": choose_from_list(["rdb-a", "rdb-b", "rdb-c", "rdb-d", "rdb-e", "rdb-f"], "cmd_target", at_min, i),
                        "cmd": choose_from_list(
                            [
                                "sudo update-ca-certificates --fresh",
                                "rm ~/.dotnet/corefx/cryptography/x509stores/ca/DAC9024F54D8F6DF94935FB1732638CA6AD77C13.pfx",
                                "rm ~/.dotnet/corefx/cryptography/x509stores/ca/48504E974C0DAC5B5CD476C8202274B24C8C7172.pfx",
                            ],
                            "cmd",
                            at_min,
                            i,
                        ),
                        "result": "ok" if u01("cmd_result", at_min, i) < 0.88 else "failed",
                    }
                elif ref == "host_os.ca_certs_update":
                    bind = {"ver": choose_from_list(["2021.09.01", "2021.09.10"], "ver", at_min, i), "anchor": "DST Root CA X3"}
                elif ref == "host_os.trust_store_refresh":
                    added = 1 if u01("added", at_min, i) < 0.55 else 0
                    removed = 0 if added == 1 else (1 if u01("removed", at_min, i) < 0.25 else 0)
                    bind = {"added": added, "removed": removed}

                lvl, msg = render_log(ref, state, bind, ("oneshot", ref, at_min, i))
                out_rows.append({"ts": t, "level": lvl, "message": msg, "trace_id": "", "service": svc, "host": host})


def main() -> None:
    total_minutes = int(SCENARIO["time"]["total_minutes"])
    rounding = CarryRounding()
    out_rows: List[Dict[str, Any]] = []
    flow_instance_counters: Dict[str, int] = {}

    for minute in range(total_minutes):
        emit_background_for_minute(minute, rounding, out_rows)
        emit_flows_for_minute(minute, rounding, flow_instance_counters, out_rows)

    emit_one_shots(out_rows)

    out_rows.sort(key=lambda r: r["ts"])

    df = pd.DataFrame(
        {
            "timestamp": [fmt_ts(r["ts"]) for r in out_rows],
            "level": [r["level"] for r in out_rows],
            "message": [r["message"] for r in out_rows],
            "trace_id": [r["trace_id"] for r in out_rows],
            "service": [r["service"] for r in out_rows],
            "host": [r["host"] for r in out_rows],
        }
    )

    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
