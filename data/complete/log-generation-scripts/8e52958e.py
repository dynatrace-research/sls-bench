import hashlib
import ipaddress
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ----------------------------
# Embedded normalized model data
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "tracing": {"on": True, "origins": ["edge_lb"], "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "edge_lb": {
            "svc": "haproxy",
            "hosts": ["lb-1", "lb-2"],
            "logs": {
                "req_received": {
                    "lvl": "INFO",
                    "msg": "req_in {method} {uri} src={client_ip} trace_id={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "uri": {
                            "k": "ch",
                            "v": [
                                "/",
                                "/login",
                                "/octo/hello",
                                "/octo/hello/pull/1",
                                "/api/v3/repos",
                                "/octo/hello.git/info/refs",
                            ],
                        },
                        "client_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "backend_conn_fail": {
                    "lvl": "WARN",
                    "msg": "backend_connect_fail backend={backend} err={err} trace_id={trace_id}",
                    "vars": {
                        "backend": {"k": "ch", "v": ["app-1", "app-2", "app-3", "app-4", "app-5", "app-6"]},
                        "err": {"k": "ch", "v": ["connection_refused", "timeout", "no_route"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_access_ok": {
                    "lvl": "INFO",
                    "msg": "resp 200 bytes={bytes} dur_ms={dur_ms} backend={backend} trace_id={trace_id}",
                    "vars": {
                        "bytes": {"k": "i", "v": [800, 200000]},
                        "dur_ms": {"k": "i", "v": [20, 600]},
                        "backend": {"k": "ch", "v": ["app-1", "app-2", "app-3", "app-4", "app-5", "app-6"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_access_503": {
                    "lvl": "WARN",
                    "msg": "resp 503 bytes={bytes} dur_ms={dur_ms} reason={reason} trace_id={trace_id}",
                    "vars": {
                        "bytes": {"k": "i", "v": [400, 8000]},
                        "dur_ms": {"k": "i", "v": [10, 4000]},
                        "reason": {"k": "ch", "v": ["backend_connect_fail", "upstream_503"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "pool_state": {
                    "lvl": "INFO",
                    "msg": "backend_pool pool={pool} up={up} down={down}",
                    "vars": {"pool": {"k": "ch", "v": ["web_app_pool"]}},
                    "state_vars": {
                        "n": {"up": {"k": "i", "v": [4, 6]}, "down": {"k": "i", "v": [0, 2]}},
                        "f": {"up": {"k": "i", "v": [0, 2]}, "down": {"k": "i", "v": [4, 6]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "pool_state", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "pool_state", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        "web_app": {
            "svc": "web-app",
            "hosts": ["app-1", "app-2", "app-3", "app-4", "app-5", "app-6"],
            "logs": {
                "req_completed_ok": {
                    "lvl": "INFO",
                    "msg": "app_resp status=200 route={route} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "route": {"k": "ch", "v": ["home", "login", "repo", "pull_request", "api_repos", "git_info_refs"]},
                        "dur_ms": {"k": "i", "v": [10, 500]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "redis_conn_error": {
                    "lvl": "ERROR",
                    "msg": "redis_error cluster={cluster} err={err} timeout_ms={timeout_ms} trace_id={trace_id}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["cache-main"]},
                        "err": {"k": "ch", "v": ["connection_refused", "timeout", "cluster_down"]},
                        "timeout_ms": {"k": "i", "v": [100, 2000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "req_completed_503": {
                    "lvl": "ERROR",
                    "msg": "app_resp status=503 err={err} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "err": {"k": "ch", "v": ["redis_unavailable", "dependency_timeout", "circuit_open"]},
                        "dur_ms": {"k": "i", "v": [50, 3000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "healthcheck": {"lvl": "INFO", "msg": "health ok version={version}", "vars": {"version": {"k": "ch", "v": ["2016.01.28"]}}},
                "boot_start": {"lvl": "INFO", "msg": "boot starting web-app version={version}", "vars": {"version": {"k": "ch", "v": ["2016.01.28"]}}},
                "boot_fail_redis": {
                    "lvl": "ERROR",
                    "msg": "boot failed: redis_required cluster={cluster} err={err}",
                    "vars": {"cluster": {"k": "ch", "v": ["cache-main"]}, "err": {"k": "ch", "v": ["cluster_down", "timeout"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "healthcheck", "per_min": 0.2, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "healthcheck", "per_min": 0.1, "scope": "per_host"},
                        {"id": "boot_start", "per_min": 0.4, "scope": "per_host"},
                        {"id": "boot_fail_redis", "per_min": 0.4, "scope": "per_host"},
                    ]
                },
            },
        },
        "redis_cluster": {
            "svc": "redis",
            "hosts": ["redis-1", "redis-2", "redis-3", "redis-4", "redis-5", "redis-6"],
            "logs": {
                "cluster_state": {
                    "lvl": "INFO",
                    "msg": "cluster {cluster} state={state} masters_up={masters_up}/{masters_total}",
                    "vars": {"cluster": {"k": "ch", "v": ["cache-main"]}, "masters_total": {"k": "i", "v": [3, 3]}},
                    "state_vars": {
                        "n": {"state": {"k": "ch", "v": ["ok"]}, "masters_up": {"k": "i", "v": [3, 3]}},
                        "f": {"state": {"k": "ch", "v": ["down", "degraded"]}, "masters_up": {"k": "i", "v": [0, 1]}},
                    },
                },
                "node_unreachable": {
                    "lvl": "WARN",
                    "msg": "node_unreachable node={node} last_seen_s={last_seen_s}",
                    "vars": {"node": {"k": "ch", "v": ["redis-1", "redis-2", "redis-3", "redis-4", "redis-5", "redis-6"]}, "last_seen_s": {"k": "i", "v": [5, 900]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "cluster_state", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "cluster_state", "per_min": 0.7, "scope": "per_host"}, {"id": "node_unreachable", "per_min": 0.6, "scope": "per_host"}]},
            },
        },
        "chatops": {
            "svc": "chatops",
            "hosts": ["chat-1", "chat-2"],
            "logs": {
                "heartbeat": {"lvl": "INFO", "msg": "chatops heartbeat leader={leader}", "vars": {"leader": {"k": "ch", "v": ["chat-1", "chat-2"]}}},
                "command_fail": {
                    "lvl": "WARN",
                    "msg": "chatops cmd_failed cmd={cmd} err={err}",
                    "vars": {"cmd": {"k": "ch", "v": ["set_status_red", "page_oncall", "restart_service"]}, "err": {"k": "ch", "v": ["host_unreachable", "dependency_down", "timeout"]}},
                },
                "status_post_red": {"lvl": "INFO", "msg": "status_posted status=red summary={summary}", "vars": {"summary": {"k": "ch", "v": ["investigating connectivity", "service outage - recovery in progress"]}}},
            },
            "beh": {"n": {"emit": [{"id": "heartbeat", "per_min": 1.0, "scope": "per_host"}]}, "f": {"emit": [{"id": "heartbeat", "per_min": 0.2, "scope": "per_host"}, {"id": "command_fail", "per_min": 0.6, "scope": "per_host"}]}},
        },
        "infra_monitor": {
            "svc": "monitoring",
            "hosts": ["mon-1"],
            "logs": {
                "power_glitch": {"lvl": "CRITICAL", "msg": "facility power event affected_pct={affected_pct} rebooted_hosts~={rebooted_hosts}", "vars": {"affected_pct": {"k": "i", "v": [20, 35]}, "rebooted_hosts": {"k": "i", "v": [200, 2000]}}},
                "alert_page": {"lvl": "INFO", "msg": "paged oncall team={team} incident_id={incident_id}", "vars": {"team": {"k": "ch", "v": ["ops", "web", "storage"]}, "incident_id": {"k": "uuid", "v": None}}},
                "host_reboot_alert": {
                    "lvl": "WARN",
                    "msg": "host_reboot host={host} hw_class={hw_class} uptime_s={uptime_s}",
                    "vars": {"host": {"k": "ch", "v": ["app-1", "app-2", "app-3", "app-4", "app-5", "app-6", "redis-1", "redis-2", "redis-3", "redis-4", "redis-5", "redis-6", "chat-1", "chat-2"]}, "hw_class": {"k": "ch", "v": ["class_a", "class_b"]}, "uptime_s": {"k": "i", "v": [30, 1800]}},
                },
                "conn_spike": {"lvl": "WARN", "msg": "inbound_conn_spike src={src} cps={cps}", "vars": {"src": {"k": "ch", "v": ["mixed_internet", "single_asn"]}, "cps": {"k": "i", "v": [5000, 80000]}}},
            },
            "beh": {
                "n": {"emit": [{"id": "host_reboot_alert", "per_min": 0.05, "scope": "global"}, {"id": "conn_spike", "per_min": 0.02, "scope": "global"}]},
                "f": {"emit": [{"id": "host_reboot_alert", "per_min": 2.0, "scope": "global"}, {"id": "conn_spike", "per_min": 1.0, "scope": "global"}]},
            },
        },
        "ddos_shield": {
            "svc": "ddos-mitigation",
            "hosts": ["waf-1", "waf-2"],
            "logs": {
                "mitigation_enabled": {"lvl": "INFO", "msg": "mitigation enabled profile={profile}", "vars": {"profile": {"k": "ch", "v": ["standard"]}}},
                "mitigation_stats": {"lvl": "INFO", "msg": "mitigation stats cps_in={cps_in} drop_pct={drop_pct}", "vars": {"cps_in": {"k": "i", "v": [1000, 90000]}, "drop_pct": {"k": "i", "v": [0, 60]}}},
            },
            "beh": {"n": {"emit": [{"id": "mitigation_stats", "per_min": 0.1, "scope": "per_host"}]}, "f": {"emit": [{"id": "mitigation_stats", "per_min": 0.5, "scope": "per_host"}]}},
        },
        "fleet_manager": {
            "svc": "fleet",
            "hosts": ["fleet-1", "fleet-2"],
            "logs": {
                "provision_blocked": {"lvl": "WARN", "msg": "provisioning delayed dependency={dep} err={err}", "vars": {"dep": {"k": "ch", "v": ["pxe", "inventory_db", "config_repo"]}, "err": {"k": "ch", "v": ["unreachable", "timeout"]}}},
                "oob_boot_error": {"lvl": "ERROR", "msg": "oob_boot_error host={host} hw_class={hw_class} boot_error={boot_error}", "vars": {"host": {"k": "ch", "v": ["app-2", "app-3", "redis-2", "redis-4"]}, "hw_class": {"k": "ch", "v": ["class_a"]}, "boot_error": {"k": "ch", "v": ["no_boot_disk", "raid_controller_missing"]}}},
                "cold_power_cycle": {"lvl": "INFO", "msg": "cold_power_cycle requested host={host} action={action}", "vars": {"host": {"k": "ch", "v": ["app-2", "app-3", "redis-2", "redis-4"]}, "action": {"k": "ch", "v": ["drain_flea_power"]}}},
                "redis_rebuild_start": {"lvl": "INFO", "msg": "redis_rebuild start cluster={cluster} target_hw={target_hw}", "vars": {"cluster": {"k": "ch", "v": ["cache-main"]}, "target_hw": {"k": "ch", "v": ["standby_pool"]}}},
            },
            "beh": {"n": {"emit": [{"id": "provision_blocked", "per_min": 0.05, "scope": "per_host"}]}, "f": {"emit": [{"id": "provision_blocked", "per_min": 0.6, "scope": "per_host"}, {"id": "oob_boot_error", "per_min": 0.3, "scope": "per_host"}]}},
        },
    },
    "flows": {
        "n": [
            {
                "id": "web_request_ok",
                "rpm": 320.0,
                "emit": ["edge_lb.req_received", "web_app.req_completed_ok", "edge_lb.http_access_ok"],
                "latency_ms": [[1, 4], [20, 180], [1, 4]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            }
        ],
        "f": [
            {
                "id": "web_request_503_backend_down",
                "rpm": 220.0,
                "emit": ["edge_lb.req_received", "edge_lb.backend_conn_fail", "edge_lb.http_access_503"],
                "latency_ms": [[1, 4], [20, 250], [1, 4]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "web_request_503_redis",
                "rpm": 120.0,
                "emit": ["edge_lb.req_received", "web_app.redis_conn_error", "web_app.req_completed_503", "edge_lb.http_access_503"],
                "latency_ms": [[1, 4], [40, 800], [30, 1200], [1, 4]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "time": {"total_minutes": 44, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 44}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {
                        "web_request_503_backend_down": 1.4,
                        "web_request_503_redis": 0.3,
                        "infra_monitor.conn_spike": 0.0,
                        "ddos_shield.mitigation_stats": 0.0,
                        "fleet_manager.provision_blocked": 0.1,
                        "fleet_manager.oob_boot_error": 0.0,
                        "chatops.command_fail": 1.4,
                    },
                    "latency_multipliers": {"web_request_503_backend_down": {"p50": 1.2, "p95": 1.4}},
                    "one_shots": [{"ref": "infra_monitor.power_glitch", "count": 1, "hosts": ["mon-1"]}, {"ref": "infra_monitor.alert_page", "count": 3, "hosts": ["mon-1"]}],
                },
                {
                    "order": 2,
                    "at_min": 23,
                    "rate_multipliers": {"infra_monitor.conn_spike": 1.0, "ddos_shield.mitigation_stats": 2.0},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "ddos_shield.mitigation_enabled", "count": 1, "hosts": ["waf-1"]}],
                },
                {"order": 3, "at_min": 28, "rate_multipliers": {"chatops.command_fail": 1.0}, "latency_multipliers": {}, "one_shots": [{"ref": "chatops.status_post_red", "count": 1, "hosts": ["chat-2"]}]},
                {
                    "order": 4,
                    "at_min": 32,
                    "rate_multipliers": {"web_request_503_backend_down": 0.6, "web_request_503_redis": 1.8, "fleet_manager.oob_boot_error": 2.0, "fleet_manager.provision_blocked": 1.5},
                    "latency_multipliers": {"web_request_503_redis": {"p50": 1.4, "p95": 1.8}},
                    "one_shots": [{"ref": "fleet_manager.redis_rebuild_start", "count": 1, "hosts": ["fleet-1"]}, {"ref": "fleet_manager.cold_power_cycle", "count": 6, "hosts": ["fleet-1", "fleet-2"]}],
                },
            ]
        }
    },
}

# ----------------------------
# Deterministic helpers
# ----------------------------

SEED = "incident-sim-v3|code_hosting_web_platform|jan_28_power_reboot_redis_dependency"


def _h64(key: str) -> int:
    d = hashlib.md5((SEED + "|" + key).encode("utf-8")).digest()
    return int.from_bytes(d[:8], "big", signed=False)


def u01(key: str) -> float:
    return (_h64(key) % (1 << 53)) / float(1 << 53)


def stable_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    f = math.floor(expected)
    frac = expected - f
    if frac <= 1e-12:
        return int(f)
    return int(f + (1.0 if u01(f"round|{key}") < frac else 0.0))


def choose(lst: List[Any], key: str) -> Any:
    if not lst:
        return None
    idx = int(u01(f"choose|{key}") * len(lst))
    if idx == len(lst):
        idx = len(lst) - 1
    return lst[idx]


def gen_hex(n: int, key: str) -> str:
    out = ""
    counter = 0
    while len(out) < n:
        out += hashlib.md5((SEED + "|" + key + f"|{counter}").encode("utf-8")).hexdigest()
        counter += 1
    return out[:n].lower()


def gen_uuid(key: str) -> str:
    b = hashlib.md5((SEED + "|uuid|" + key).encode("utf-8")).digest()
    bb = bytearray(b[:16])
    bb[6] = (bb[6] & 0x0F) | 0x40
    bb[8] = (bb[8] & 0x3F) | 0x80
    hx = bytes(bb).hex()
    return f"{hx[0:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"


def sample_int(lo: int, hi: int, key: str) -> int:
    if hi < lo:
        lo, hi = hi, lo
    if hi == lo:
        return int(lo)
    v = int(lo + math.floor(u01(f"int|{key}") * (hi - lo + 1)))
    return min(max(v, lo), hi)


def sample_ip(cidr: str, key: str) -> str:
    net = ipaddress.ip_network(cidr, strict=False)
    if net.num_addresses <= 2:
        return str(net.network_address)
    usable = net.num_addresses - 2
    offset = 1 + int(u01(f"ip|{key}") * usable)
    if offset >= net.num_addresses:
        offset = net.num_addresses - 1
    return str(net.network_address + offset)


def sample_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    if k == "ch":
        return choose(list(v), f"{key}|ch")
    if k == "i":
        return sample_int(int(v[0]), int(v[1]), f"{key}|i")
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return lo + (hi - lo) * u01(f"{key}|f")
    if k == "hex":
        return gen_hex(int(v), f"{key}|hex")
    if k == "uuid":
        return gen_uuid(f"{key}|uuid")
    if k == "ip":
        return sample_ip(str(v), f"{key}|ip")
    if k == "str":
        return f"{v}-{gen_hex(8, f'{key}|str')}"
    return str(v)


def norm_ppf(p: float) -> float:
    # Peter J. Acklam's inverse normal CDF approximation.
    p = min(max(float(p), 1e-12), 1.0 - 1e-12)

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
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return -num / den

    q = p - 0.5
    r = q * q
    num = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    den = (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    return num / den


def lognormal_from_p50_p95(p50: float, p95: float, u: float) -> float:
    p50 = max(float(p50), 0.001)
    p95 = max(float(p95), p50 + 0.001)
    sigma = math.log(p95 / p50) / 1.645
    mu = math.log(p50)
    z = norm_ppf(u)
    x = math.exp(mu + sigma * z)
    cap = 3.0 * p95  # soft cap per prompt guidance
    return min(x, cap)


def sample_delay_ms(p50: float, p95: float, key: str) -> int:
    val = lognormal_from_p50_p95(p50, p95, u01(f"delay|{key}"))
    return max(1, int(round(val)))


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    ms = int(dt.microsecond / 1000)
    return dt.strftime(f"%Y-%m-%dT%H:%M:%S.{ms:03d}Z")


def clamp_dt(dt: datetime, start: datetime, end: datetime) -> datetime:
    if dt < start:
        return start
    if dt >= end:
        return end - timedelta(milliseconds=1)
    return dt


def schedule_times(start: datetime, end: datetime, n: int, key: str, jitter_ms: int = 450) -> List[datetime]:
    if n <= 0:
        return []
    span = (end - start).total_seconds()
    if span <= 0:
        return [start] * n
    out: List[datetime] = []
    for i in range(n):
        frac = (i + 0.5) / n
        base = start + timedelta(seconds=span * frac)
        j = int(round((u01(f"sched|{key}|{i}") - 0.5) * 2.0 * jitter_ms))
        t = base + timedelta(milliseconds=j)
        out.append(clamp_dt(t, start, end))
    out.sort()
    return out


# ----------------------------
# Indices
# ----------------------------

COMP = SYSTEM["components"]

LOG_TEMPLATES: Dict[str, Dict[str, Any]] = {}
for cid, c in COMP.items():
    for lid, tmpl in c["logs"].items():
        LOG_TEMPLATES[f"{cid}.{lid}"] = tmpl

FLOWS_BY_STATE: Dict[str, Dict[str, Dict[str, Any]]] = {"n": {}, "f": {}}
for st in ("n", "f"):
    for fl in SYSTEM["flows"][st]:
        FLOWS_BY_STATE[st][fl["id"]] = fl

# ----------------------------
# Failure control intervals
# ----------------------------

@dataclass(frozen=True)
class Interval:
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    latency_mult: Dict[str, Dict[str, float]]  # flow_id -> {p50, p95}


def build_failure_intervals() -> List[Interval]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = sorted([f_start] + sorted({e["at_min"] for e in events}) + [f_end])

    rate_mult: Dict[str, float] = {}
    latency_mult: Dict[str, Dict[str, float]] = {}
    out: List[Interval] = []

    events_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        events_by_min.setdefault(e["at_min"], []).append(e)

    for i in range(len(boundaries) - 1):
        s = boundaries[i]
        e = boundaries[i + 1]
        for ev in events_by_min.get(s, []):
            for k, v in ev.get("rate_multipliers", {}).items():
                rate_mult[k] = float(v)
            for fid, mv in ev.get("latency_multipliers", {}).items():
                latency_mult[fid] = {"p50": float(mv.get("p50", 1.0)), "p95": float(mv.get("p95", 1.0))}
        out.append(Interval(start_min=s, end_min=e, rate_mult=dict(rate_mult), latency_mult=dict(latency_mult)))
    return out


FAILURE_INTERVALS = build_failure_intervals()

# ----------------------------
# Emission
# ----------------------------

def get_int_bounds(template_key: str, state: str, var_name: str) -> Optional[Tuple[int, int]]:
    tmpl = LOG_TEMPLATES[template_key]
    dom = None
    stv = tmpl.get("state_vars", {}).get(state, {})
    if var_name in stv:
        dom = stv[var_name]
    elif var_name in tmpl.get("vars", {}):
        dom = tmpl["vars"][var_name]
    if not dom or dom.get("k") != "i":
        return None
    v = dom.get("v")
    if not isinstance(v, (list, tuple)) or len(v) != 2:
        return None
    return int(v[0]), int(v[1])


def adjust_attempt_timing_delays(delays_ms: List[int], emit_refs: List[str], state: str, flow_key: str) -> List[int]:
    """
    Ensure that observed timing fields derived from timestamps (dur_ms/timeout_ms) remain within the
    template integer domains by adjusting segment delays (between logs) deterministically.

    delays_ms has length == len(emit_refs), where:
      delays_ms[0] is delay from flow start to first emitted log,
      delays_ms[i] for i>=1 is delay between log i-1 and log i.
    """
    if len(emit_refs) <= 1:
        return delays_ms

    d0 = max(1, int(delays_ms[0]))
    seg = [max(1, int(x)) for x in delays_ms[1:]]  # segment delays between logs
    m = len(seg)

    # Prefix bounds for durations since req_received (log0). Prefix i means sum(seg[:i]) -> time from log0 to log i.
    lo_prefix = [i + 1 for i in range(m)]  # baseline from seg>=1
    hi_prefix = [10**12 for _ in range(m)]

    for log_idx in range(1, len(emit_refs)):
        ref = emit_refs[log_idx]
        b_dur = get_int_bounds(ref, state, "dur_ms")
        b_to = get_int_bounds(ref, state, "timeout_ms")
        if b_dur:
            lo_prefix[log_idx - 1] = max(lo_prefix[log_idx - 1], b_dur[0])
            hi_prefix[log_idx - 1] = min(hi_prefix[log_idx - 1], b_dur[1])
        if b_to:
            lo_prefix[log_idx - 1] = max(lo_prefix[log_idx - 1], b_to[0])
            hi_prefix[log_idx - 1] = min(hi_prefix[log_idx - 1], b_to[1])

    # Step 1: enforce prefix minimums by pushing deficit into the latest segment of each prefix.
    prefix_sum = 0
    for i in range(m):
        prefix_sum += seg[i]
        if prefix_sum < lo_prefix[i]:
            bump = lo_prefix[i] - prefix_sum
            seg[i] += bump
            prefix_sum = lo_prefix[i]

    # Helper to compute current prefix sums
    def prefix_sums(upto: int) -> List[int]:
        out = []
        s = 0
        for x in seg[: upto + 1]:
            s += x
            out.append(s)
        return out

    # Step 2: enforce prefix maximums by reducing segment delays in the offending prefix, from the end backward,
    # without violating any prefix minimums in the affected range.
    for i in range(m):
        ps = sum(seg[: i + 1])
        if ps <= hi_prefix[i]:
            continue
        delta = ps - hi_prefix[i]

        # Reduce within seg[0..i] while preserving lo_prefix[0..i] constraints.
        while delta > 0:
            ps_list = prefix_sums(i)
            reduced_any = False

            for j in range(i, -1, -1):
                if seg[j] <= 1:
                    continue

                # Slack for all prefixes t in [j..i] (0-based) impacted by seg[j] reduction
                min_slack = None
                for t in range(j, i + 1):
                    slack = ps_list[t] - lo_prefix[t]
                    if min_slack is None or slack < min_slack:
                        min_slack = slack
                if min_slack is None or min_slack <= 0:
                    continue

                reducible = min(delta, seg[j] - 1, min_slack)
                if reducible <= 0:
                    continue
                seg[j] -= int(reducible)
                delta -= int(reducible)
                reduced_any = True
                break

            if not reduced_any:
                # Infeasible bounds (should not happen with this model); stop to avoid infinite loop.
                break

    return [d0] + seg


def render_log_message(template_key: str, state: str, bind: Dict[str, Any], key: str) -> Tuple[str, str]:
    tmpl = LOG_TEMPLATES[template_key]
    vars_dom = dict(tmpl.get("vars", {}))
    stv = tmpl.get("state_vars", {}).get(state, {})
    for k, dom in stv.items():
        vars_dom[k] = dom

    vals: Dict[str, Any] = {}
    for var_name, dom in vars_dom.items():
        if var_name in bind:
            vals[var_name] = bind[var_name]
        else:
            vals[var_name] = sample_domain(dom, f"{key}|{template_key}|{var_name}")

    msg = tmpl["msg"].format(**vals)
    return tmpl["lvl"], msg


def emit_row(rows: List[Dict[str, Any]], ts: datetime, comp_id: str, host: str, level: str, message: str, trace_id: str) -> None:
    rows.append(
        {
            "timestamp_dt": ts,
            "level": level,
            "message": message,
            "trace_id": trace_id,
            "service": COMP[comp_id].get("svc", "") or "",
            "host": host or "",
        }
    )


URI_TO_ROUTE = {
    "/": "home",
    "/login": "login",
    "/octo/hello": "repo",
    "/octo/hello/pull/1": "pull_request",
    "/api/v3/repos": "api_repos",
    "/octo/hello.git/info/refs": "git_info_refs",
}


def simulate_flow_instance(
    rows: List[Dict[str, Any]],
    flow: Dict[str, Any],
    state: str,
    start_dt: datetime,
    flow_key: str,
    latency_mult: Optional[Dict[str, float]] = None,
) -> None:
    latency_mult = latency_mult or {"p50": 1.0, "p95": 1.0}
    trace_id = gen_hex(32, f"trace|{flow_key}") if SYSTEM["tracing"]["on"] and flow.get("trace", False) else ""

    edge_host = choose(COMP["edge_lb"]["hosts"], f"{flow_key}|edge_host")
    app_host = choose(COMP["web_app"]["hosts"], f"{flow_key}|app_host")

    uri = choose(COMP["edge_lb"]["logs"]["req_received"]["vars"]["uri"]["v"], f"{flow_key}|uri")
    method = "POST" if uri == "/login" and u01(f"{flow_key}|method_post_bias") < 0.7 else "GET"
    client_ip = sample_ip(COMP["edge_lb"]["logs"]["req_received"]["vars"]["client_ip"]["v"], f"{flow_key}|client_ip")
    route = URI_TO_ROUTE.get(uri, choose(COMP["web_app"]["logs"]["req_completed_ok"]["vars"]["route"]["v"], f"{flow_key}|route_fallback"))

    # Sample raw latency segments, then adjust segments so any dur_ms/timeout_ms derived from timestamps stays in-domain.
    delays_ms: List[int] = []
    for j, (p50, p95) in enumerate(flow["latency_ms"]):
        p50s = float(p50) * float(latency_mult.get("p50", 1.0))
        p95s = float(p95) * float(latency_mult.get("p95", 1.0))
        delays_ms.append(sample_delay_ms(p50s, p95s, f"{flow_key}|lat|{j}"))
    emit_refs = flow["emit"]
    delays_ms = adjust_attempt_timing_delays(delays_ms, emit_refs, state, flow_key)

    t = start_dt + timedelta(milliseconds=delays_ms[0])
    req_received_time: Optional[datetime] = None
    redis_error_time: Optional[datetime] = None

    for idx, ref in enumerate(emit_refs):
        comp_id, _log_id = ref.split(".", 1)
        if idx > 0:
            t = t + timedelta(milliseconds=delays_ms[idx])

        def dur_since_req_ms(now: datetime) -> int:
            if req_received_time is None:
                return 0
            return int(round((now - req_received_time).total_seconds() * 1000.0))

        if ref == "edge_lb.req_received":
            req_received_time = t
            bind = {"method": method, "uri": uri, "client_ip": client_ip, "trace_id": trace_id}
            level, msg = render_log_message(ref, state, bind, f"{flow_key}|log{idx}")
            emit_row(rows, t, comp_id, edge_host, level, msg, trace_id)
            continue

        if ref == "web_app.req_completed_ok":
            dur_ms = dur_since_req_ms(t)
            bind = {"route": route, "dur_ms": dur_ms, "trace_id": trace_id}
            level, msg = render_log_message(ref, state, bind, f"{flow_key}|log{idx}")
            emit_row(rows, t, comp_id, app_host, level, msg, trace_id)
            continue

        if ref == "edge_lb.http_access_ok":
            dur_ms = dur_since_req_ms(t)
            bind = {"dur_ms": dur_ms, "backend": app_host, "trace_id": trace_id}
            level, msg = render_log_message(ref, state, bind, f"{flow_key}|log{idx}")
            emit_row(rows, t, comp_id, edge_host, level, msg, trace_id)
            continue

        if ref == "edge_lb.backend_conn_fail":
            backend = choose(COMP["web_app"]["hosts"], f"{flow_key}|backend_fail")
            err = choose(COMP["edge_lb"]["logs"]["backend_conn_fail"]["vars"]["err"]["v"], f"{flow_key}|backend_err")
            bind = {"backend": backend, "err": err, "trace_id": trace_id}
            level, msg = render_log_message(ref, state, bind, f"{flow_key}|log{idx}")
            emit_row(rows, t, comp_id, edge_host, level, msg, trace_id)
            continue

        if ref == "edge_lb.http_access_503":
            dur_ms = dur_since_req_ms(t)
            reason = "backend_connect_fail" if flow["id"] == "web_request_503_backend_down" else "upstream_503"
            bind = {"dur_ms": dur_ms, "reason": reason, "trace_id": trace_id}
            level, msg = render_log_message(ref, state, bind, f"{flow_key}|log{idx}")
            emit_row(rows, t, comp_id, edge_host, level, msg, trace_id)
            continue

        if ref == "web_app.redis_conn_error":
            redis_error_time = t
            timeout_ms = dur_since_req_ms(t)
            err = choose(COMP["web_app"]["logs"]["redis_conn_error"]["vars"]["err"]["v"], f"{flow_key}|redis_err")
            bind = {"cluster": "cache-main", "err": err, "timeout_ms": timeout_ms, "trace_id": trace_id}
            level, msg = render_log_message(ref, state, bind, f"{flow_key}|log{idx}")
            emit_row(rows, t, comp_id, app_host, level, msg, trace_id)
            continue

        if ref == "web_app.req_completed_503":
            dur_ms = dur_since_req_ms(t)
            if redis_error_time is not None and dur_since_req_ms(redis_error_time) >= 600:
                app_err = "dependency_timeout"
            elif redis_error_time is not None:
                app_err = choose(["redis_unavailable", "dependency_timeout"], f"{flow_key}|app_err_mix")
            else:
                app_err = choose(COMP["web_app"]["logs"]["req_completed_503"]["vars"]["err"]["v"], f"{flow_key}|app_err")
            bind = {"err": app_err, "dur_ms": dur_ms, "trace_id": trace_id}
            level, msg = render_log_message(ref, state, bind, f"{flow_key}|log{idx}")
            emit_row(rows, t, comp_id, app_host, level, msg, trace_id)
            continue

        level, msg = render_log_message(ref, state, {"trace_id": trace_id}, f"{flow_key}|log{idx}")
        host = choose(COMP[comp_id].get("hosts", [""]), f"{flow_key}|fallback_host|{comp_id}") or ""
        emit_row(rows, t, comp_id, host, level, msg, trace_id)


def simulate_background(
    rows: List[Dict[str, Any]],
    state: str,
    interval_start: datetime,
    interval_end: datetime,
    duration_min: float,
    rate_mult: Optional[Dict[str, float]] = None,
    key_prefix: str = "",
) -> None:
    rate_mult = rate_mult or {}
    for comp_id, comp in COMP.items():
        beh = comp.get("beh", {}).get(state, {})
        for emit_def in beh.get("emit", []):
            log_id = emit_def["id"]
            per_min = float(emit_def["per_min"])
            scope = emit_def.get("scope", "per_host")

            source_key = f"{comp_id}.{log_id}"
            mult = 1.0
            if state == "f":
                mult = float(rate_mult.get(source_key, 1.0))

            eff = per_min * mult
            if eff <= 0:
                continue

            if scope == "global":
                expected = eff * duration_min
                n = stable_round(expected, f"bg|{key_prefix}|{state}|{source_key}|{interval_start.isoformat()}|{interval_end.isoformat()}")
                times = schedule_times(interval_start, interval_end, n, f"bg|{key_prefix}|{state}|{source_key}")
                for i, ts in enumerate(times):
                    host = choose(comp.get("hosts", [""]), f"bg_host|{source_key}|{key_prefix}|{i}|{fmt_ts(ts)}") or ""
                    lvl, msg = render_log_message(source_key, state, {}, f"bg|{key_prefix}|{source_key}|{i}")
                    emit_row(rows, ts, comp_id, host, lvl, msg, "")
            else:
                for host in comp.get("hosts", []):
                    expected = eff * duration_min
                    n = stable_round(expected, f"bg|{key_prefix}|{state}|{source_key}|{host}|{interval_start.isoformat()}|{interval_end.isoformat()}")
                    times = schedule_times(interval_start, interval_end, n, f"bg|{key_prefix}|{state}|{source_key}|{host}")
                    for i, ts in enumerate(times):
                        lvl, msg = render_log_message(source_key, state, {}, f"bg|{key_prefix}|{source_key}|{host}|{i}")
                        emit_row(rows, ts, comp_id, host, lvl, msg, "")


def emit_one_shots(rows: List[Dict[str, Any]], base: datetime) -> None:
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    power_incident_id = gen_uuid("incident|power_event")
    for ev in events:
        at_dt = base + timedelta(minutes=int(ev["at_min"]))
        for one in ev.get("one_shots", []):
            ref = one["ref"]
            comp_id, _log_id = ref.split(".", 1)
            allowed_hosts = list(one.get("hosts") or COMP[comp_id].get("hosts", []))
            count = int(one["count"])
            for j in range(count):
                jitter = int(round(u01(f"oneshot|{ev['at_min']}|{ref}|{j}") * 9000.0))  # 0..9s
                ts = at_dt + timedelta(milliseconds=jitter)
                host = allowed_hosts[j % len(allowed_hosts)] if allowed_hosts else ""
                bind: Dict[str, Any] = {}
                if ref == "infra_monitor.alert_page" and ev["at_min"] == 20:
                    bind["incident_id"] = power_incident_id
                lvl, msg = render_log_message(ref, "f", bind, f"oneshot|{ev['at_min']}|{ref}|{j}")
                emit_row(rows, ts, comp_id, host, lvl, msg, "")


# ----------------------------
# Main simulation
# ----------------------------

def main() -> None:
    # Verifier-required seeding (even though this simulator uses deterministic hashing for most choices)
    seed_int = _h64("seed") % (2**32)
    random.seed(seed_int)
    np.random.seed(seed_int)

    base = datetime(2016, 1, 28, 0, 0, 0, tzinfo=timezone.utc)
    rows: List[Dict[str, Any]] = []

    # Normal phase
    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    n_start_dt = base + timedelta(minutes=n_start)
    n_end_dt = base + timedelta(minutes=n_end)
    n_dur_min = float(n_end - n_start)

    simulate_background(rows, "n", n_start_dt, n_end_dt, n_dur_min, rate_mult=None, key_prefix="normal")

    for flow in SYSTEM["flows"]["n"]:
        expected = float(flow["rpm"]) * n_dur_min
        n_inst = stable_round(expected, f"flow|n|{flow['id']}|{n_start}|{n_end}")
        starts = schedule_times(n_start_dt, n_end_dt, n_inst, f"flow|n|{flow['id']}", jitter_ms=700)
        for i, st_dt in enumerate(starts):
            simulate_flow_instance(rows, flow, "n", st_dt, f"n|{flow['id']}|{i}")

    # Failure phase piecewise intervals
    for interval in FAILURE_INTERVALS:
        s_dt = base + timedelta(minutes=interval.start_min)
        e_dt = base + timedelta(minutes=interval.end_min)
        dur_min = float(interval.end_min - interval.start_min)

        simulate_background(rows, "f", s_dt, e_dt, dur_min, rate_mult=interval.rate_mult, key_prefix=f"fail|{interval.start_min}-{interval.end_min}")

        for flow in SYSTEM["flows"]["f"]:
            mult = float(interval.rate_mult.get(flow["id"], 1.0))
            eff_rpm = float(flow["rpm"]) * mult
            expected = eff_rpm * dur_min
            n_inst = stable_round(expected, f"flow|f|{flow['id']}|{interval.start_min}|{interval.end_min}")
            starts = schedule_times(s_dt, e_dt, n_inst, f"flow|f|{flow['id']}|{interval.start_min}-{interval.end_min}", jitter_ms=700)
            lm = interval.latency_mult.get(flow["id"], {"p50": 1.0, "p95": 1.0})
            for i, st_dt in enumerate(starts):
                simulate_flow_instance(rows, flow, "f", st_dt, f"f|{interval.start_min}-{interval.end_min}|{flow['id']}|{i}", latency_mult=lm)

    # One-shots
    emit_one_shots(rows, base)

    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp_dt", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["timestamp_dt"].apply(fmt_ts)

    out = df[["timestamp", "level", "message", "trace_id", "service", "host"]].copy()

    assert list(out.columns) == ["timestamp", "level", "message", "trace_id", "service", "host"]
    total_rows = len(out)
    assert 20000 <= total_rows <= 100000, f"row count {total_rows} outside target range"
    assert (df["timestamp_dt"].values[:-1] <= df["timestamp_dt"].values[1:]).all()

    out.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
