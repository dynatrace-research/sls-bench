import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "datadog_us_platform"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["web_frontend", "intake_cluster"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "web_frontend",
            "svc": "web",
            "hosts": ["web-1", "web-2", "web-3"],
            "logs": {
                "req_start": {
                    "lvl": "INFO",
                    "msg": "req start method={method} route={route} req_id={req_id} user_id={user_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/", "/dash", "/api/v1/query"]},
                        "req_id": {"k": "hex", "v": 16},
                        "user_id": {"k": "i", "v": [10000, 99999]},
                    },
                },
                "access_ok": {
                    "lvl": "INFO",
                    "msg": "req end method={method} route={route} status=200 bytes={bytes} dur_ms={dur_ms} req_id={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/", "/dash", "/api/v1/query"]},
                        "bytes": {"k": "i", "v": [2000, 250000]},
                        "dur_ms": {"k": "i", "v": [20, 2500]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "access_err": {
                    "lvl": "WARN",
                    "msg": "req end method={method} route={route} status={status} err={err} dur_ms={dur_ms} req_id={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/", "/dash", "/api/v1/query"]},
                        "status": {"k": "i", "v": [502, 504]},
                        "err": {"k": "ch", "v": ["upstream_503", "lookup_failed", "timeout"]},
                        "dur_ms": {"k": "i", "v": [50, 8000]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "dep_lookup_failed": {
                    "lvl": "WARN",
                    "msg": "dependency lookup failed dep={dep} via=dns err={err} req_id={req_id}",
                    "vars": {
                        "dep": {"k": "ch", "v": ["backend-api"]},
                        "err": {"k": "ch", "v": ["SERVFAIL", "timeout"]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "backend_api",
            "svc": "api",
            "hosts": ["api-1", "api-2", "api-3"],
            "logs": {
                "req_ok": {
                    "lvl": "INFO",
                    "msg": "handled op={op} status=200 dur_ms={dur_ms} req_id={req_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["get_dashboard", "query_timeseries", "list_monitors"]},
                        "dur_ms": {"k": "i", "v": [10, 800]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "req_err": {
                    "lvl": "ERROR",
                    "msg": "failed op={op} status=503 reason={reason} dur_ms={dur_ms} req_id={req_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["get_dashboard", "query_timeseries", "list_monitors"]},
                        "reason": {"k": "ch", "v": ["config_fetch_failed", "dependency_unresolved", "upstream_timeout"]},
                        "dur_ms": {"k": "i", "v": [30, 6000]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "config_refresh_failed": {
                    "lvl": "ERROR",
                    "msg": "config refresh failed src=service-discovery err={err} waited_ms={waited_ms}",
                    "vars": {
                        "err": {"k": "ch", "v": ["timeout", "no_leader", "connection_refused"]},
                        "waited_ms": {"k": "i", "v": [200, 8000]},
                    },
                },
                "fallback_config_enabled": {
                    "lvl": "WARN",
                    "msg": "enabled local config fallback mode={mode} by={actor}",
                    "vars": {"mode": {"k": "ch", "v": ["read_only"]}, "actor": {"k": "ch", "v": ["oncall"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "config_refresh_failed", "per_min": 0.02, "scope": "per_host"}]},
                "f": {"emit": [{"id": "config_refresh_failed", "per_min": 0.8, "scope": "per_host"}]},
            },
        },
        {
            "id": "intake_cluster",
            "svc": "intake",
            "hosts": ["intake-1", "intake-2", "intake-3", "intake-4"],
            "logs": {
                "payload_received": {
                    "lvl": "INFO",
                    "msg": "payload received size_bytes={size_bytes} src_ip={src_ip} payload_id={payload_id}",
                    "vars": {
                        "size_bytes": {"k": "i", "v": [500, 60000]},
                        "src_ip": {"k": "ip", "v": "10.0.0.0/8"},
                        "payload_id": {"k": "hex", "v": 24},
                    },
                },
                "payload_accepted": {
                    "lvl": "INFO",
                    "msg": "payload accepted payload_id={payload_id} ingest_ms={ingest_ms}",
                    "vars": {"payload_id": {"k": "hex", "v": 24}, "ingest_ms": {"k": "i", "v": [5, 300]}},
                },
                "tracking_dns_attempt": {
                    "lvl": "INFO",
                    "msg": "latency-tracker lookup qname={qname} attempt_id={attempt_id}",
                    "vars": {"qname": {"k": "ch", "v": ["latency-tracker.service.local"]}, "attempt_id": {"k": "hex", "v": 12}},
                },
                "tracking_started": {
                    "lvl": "INFO",
                    "msg": "latency tracking started qname={qname} attempt_id={attempt_id}",
                    "vars": {"qname": {"k": "ch", "v": ["latency-tracker.service.local"]}, "attempt_id": {"k": "hex", "v": 12}},
                },
                "tracking_skipped": {
                    "lvl": "WARN",
                    "msg": "latency tracking skipped reason={reason} attempt_id={attempt_id}",
                    "vars": {"reason": {"k": "ch", "v": ["nxdomain", "servfail", "resolver_timeout"]}, "attempt_id": {"k": "hex", "v": 12}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "dns_resolver",
            "svc": "dns-resolver",
            "hosts": ["dns-1", "dns-2", "dns-3", "dns-4", "dns-5"],
            "logs": {
                "cache_hit": {
                    "lvl": "INFO",
                    "msg": "dns cache hit qname={qname} rcode=NOERROR ttl_s={ttl_s}",
                    "vars": {"qname": {"k": "ch", "v": ["latency-tracker.service.local"]}, "ttl_s": {"k": "i", "v": [5, 60]}},
                },
                "cache_miss_nxdomain": {
                    "lvl": "INFO",
                    "msg": "dns cache miss qname={qname} upstream=sd-dns rcode=NXDOMAIN cacheable=false dur_ms={dur_ms}",
                    "vars": {"qname": {"k": "ch", "v": ["latency-tracker.service.local"]}, "dur_ms": {"k": "i", "v": [1, 800]}},
                },
                "cache_miss_servfail": {
                    "lvl": "WARN",
                    "msg": "dns cache miss qname={qname} upstream=sd-dns rcode=SERVFAIL cacheable=true dur_ms={dur_ms}",
                    "vars": {"qname": {"k": "ch", "v": ["latency-tracker.service.local"]}, "dur_ms": {"k": "i", "v": [5, 1200]}},
                },
                "upstream_timeout": {
                    "lvl": "WARN",
                    "msg": "dns upstream timeout qname={qname} upstream=sd-dns timeout_ms={timeout_ms}",
                    "vars": {"qname": {"k": "ch", "v": ["latency-tracker.service.local"]}, "timeout_ms": {"k": "i", "v": [50, 2000]}},
                },
                "upstream_timeout_backend_api": {
                    "lvl": "WARN",
                    "msg": "dns upstream timeout qname={qname} upstream=sd-dns timeout_ms={timeout_ms}",
                    "vars": {"qname": {"k": "ch", "v": ["backend-api.service.local"]}, "timeout_ms": {"k": "i", "v": [50, 2000]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "service_discovery",
            "svc": "service-discovery",
            "hosts": ["sd-1", "sd-2", "sd-3", "sd-4", "sd-5"],
            "logs": {
                "dns_nxdomain": {
                    "lvl": "INFO",
                    "msg": "dns query qname={qname} rcode=NXDOMAIN ans_count=0 dur_ms={dur_ms} client={client_ip}",
                    "vars": {"qname": {"k": "ch", "v": ["latency-tracker.service.local"]}, "dur_ms": {"k": "i", "v": [1, 900]}, "client_ip": {"k": "ip", "v": "10.0.0.0/8"}},
                },
                "dns_servfail": {
                    "lvl": "WARN",
                    "msg": "dns query qname={qname} rcode=SERVFAIL ans_count=0 dur_ms={dur_ms} client={client_ip}",
                    "vars": {"qname": {"k": "ch", "v": ["latency-tracker.service.local"]}, "dur_ms": {"k": "i", "v": [10, 1500]}, "client_ip": {"k": "ip", "v": "10.0.0.0/8"}},
                },
                "raft_state": {
                    "lvl": "INFO",
                    "msg": "raft state role={role} leader={leader} term={term} commit_lag={commit_lag} qps={qps}",
                    "vars": {"role": {"k": "ch", "v": ["leader", "follower", "candidate"]}, "leader": {"k": "ch", "v": ["true", "false"]}, "term": {"k": "i", "v": [50, 140]}},
                    "state_vars": {"n": {"commit_lag": {"k": "i", "v": [0, 80]}, "qps": {"k": "i", "v": [2, 12]}}, "f": {"commit_lag": {"k": "i", "v": [200, 7000]}, "qps": {"k": "i", "v": [8, 60]}}},
                },
                "quorum_lost": {
                    "lvl": "ERROR",
                    "msg": "raft quorum not met nodes_up={nodes_up} needed=3 inflight={inflight}",
                    "vars": {"nodes_up": {"k": "i", "v": [0, 2]}, "inflight": {"k": "i", "v": [500, 80000]}},
                },
                "client_admission": {
                    "lvl": "WARN",
                    "msg": "client admission mode set to {mode} by={actor} note={note}",
                    "vars": {"mode": {"k": "ch", "v": ["rate_limit"]}, "actor": {"k": "ch", "v": ["sd-oncall"]}, "note": {"k": "str", "v": "short_reason"}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "raft_state", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "raft_state", "per_min": 1.0, "scope": "global"}, {"id": "quorum_lost", "per_min": 0.2, "scope": "global"}]},
            },
        },
        {
            "id": "latency_tracker",
            "svc": "latency-tracker",
            "hosts": ["lt-1", "lt-2"],
            "logs": {
                "cluster_recycle": {
                    "lvl": "INFO",
                    "msg": "cluster recycle started action={action} by={actor}",
                    "vars": {"action": {"k": "ch", "v": ["scale_down_up"]}, "actor": {"k": "ch", "v": ["automation", "oncall"]}},
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "ui_widget_ok",
                    "rpm": 176,
                    "emit": ["web_frontend.req_start", "backend_api.req_ok", "web_frontend.access_ok"],
                    "latency_ms": [[1, 3], [30, 200], [10, 80]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "ui_widget_backend_503",
                    "rpm": 3,
                    "emit": ["web_frontend.req_start", "backend_api.req_err", "web_frontend.access_err"],
                    "latency_ms": [[1, 3], [40, 400], [10, 80]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "ui_widget_lookup_failed",
                    "rpm": 1,
                    "emit": ["web_frontend.req_start", "dns_resolver.upstream_timeout_backend_api", "web_frontend.dep_lookup_failed", "web_frontend.access_err"],
                    "latency_ms": [[1, 3], [50, 300], [1, 5], [5, 50]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "payload_ingest",
                    "rpm": 110,
                    "emit": ["intake_cluster.payload_received", "intake_cluster.payload_accepted"],
                    "latency_ms": [[1, 3], [5, 150]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "latency_tracker_dns_ok",
                    "rpm": 80,
                    "emit": ["intake_cluster.tracking_dns_attempt", "dns_resolver.cache_hit", "intake_cluster.tracking_started"],
                    "latency_ms": [[1, 2], [1, 5], [1, 5]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "ui_widget_ok",
                    "rpm": 60,
                    "emit": ["web_frontend.req_start", "backend_api.req_ok", "web_frontend.access_ok"],
                    "latency_ms": [[1, 5], [60, 800], [20, 200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "ui_widget_backend_503",
                    "rpm": 40,
                    "emit": ["web_frontend.req_start", "backend_api.req_err", "web_frontend.access_err"],
                    "latency_ms": [[1, 5], [80, 1500], [20, 200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "ui_widget_lookup_failed",
                    "rpm": 80,
                    "emit": ["web_frontend.req_start", "dns_resolver.upstream_timeout_backend_api", "web_frontend.dep_lookup_failed", "web_frontend.access_err"],
                    "latency_ms": [[1, 5], [80, 1500], [1, 10], [20, 200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "payload_ingest",
                    "rpm": 110,
                    "emit": ["intake_cluster.payload_received", "intake_cluster.payload_accepted"],
                    "latency_ms": [[1, 3], [5, 250]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "latency_tracker_dns_nxdomain",
                    "rpm": 160,
                    "emit": ["intake_cluster.tracking_dns_attempt", "service_discovery.dns_nxdomain", "dns_resolver.cache_miss_nxdomain", "intake_cluster.tracking_skipped"],
                    "latency_ms": [[1, 2], [2, 50], [2, 80], [1, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "latency_tracker_dns_servfail",
                    "rpm": 15,
                    "emit": ["intake_cluster.tracking_dns_attempt", "service_discovery.dns_servfail", "dns_resolver.cache_miss_servfail", "intake_cluster.tracking_skipped"],
                    "latency_ms": [[1, 2], [10, 200], [10, 200], [1, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "latency_tracker_dns_timeout",
                    "rpm": 20,
                    "emit": ["intake_cluster.tracking_dns_attempt", "dns_resolver.upstream_timeout", "intake_cluster.tracking_skipped"],
                    "latency_ms": [[1, 2], [100, 2000], [1, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "us_region_service_discovery_thundering_herd_2020_09_24",
        "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 50}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 20,
                        "rate_multipliers": {
                            "latency_tracker_dns_nxdomain": 1.5,
                            "latency_tracker_dns_servfail": 0.0,
                            "latency_tracker_dns_timeout": 0.0,
                            "service_discovery.quorum_lost": 2.0,
                            "backend_api.config_refresh_failed": 1.0,
                        },
                        "latency_multipliers": {"ui_widget_lookup_failed": {"p50": 1.2, "p95": 1.5}},
                        "one_shots": [{"ref": "latency_tracker.cluster_recycle", "count": 1, "hosts": ["lt-1"]}],
                    },
                    {
                        "order": 2,
                        "at_min": 30,
                        "rate_multipliers": {
                            "latency_tracker_dns_nxdomain": 2.2,
                            "latency_tracker_dns_servfail": 6.0,
                            "latency_tracker_dns_timeout": 5.0,
                            "ui_widget_ok": 0.85,
                            "ui_widget_backend_503": 1.2,
                            "ui_widget_lookup_failed": 1.25,
                            "service_discovery.quorum_lost": 3.0,
                            "backend_api.config_refresh_failed": 1.6,
                        },
                        "latency_multipliers": {
                            "ui_widget_backend_503": {"p50": 1.3, "p95": 1.6},
                            "ui_widget_lookup_failed": {"p50": 1.4, "p95": 1.8},
                        },
                        "one_shots": [],
                    },
                    {
                        "order": 3,
                        "at_min": 42,
                        "rate_multipliers": {
                            "latency_tracker_dns_nxdomain": 0.7,
                            "latency_tracker_dns_servfail": 0.8,
                            "latency_tracker_dns_timeout": 0.6,
                            "ui_widget_ok": 1.35,
                            "ui_widget_backend_503": 0.8,
                            "ui_widget_lookup_failed": 0.8,
                            "service_discovery.quorum_lost": 0.8,
                            "backend_api.config_refresh_failed": 0.7,
                        },
                        "latency_multipliers": {"ui_widget_ok": {"p50": 0.9, "p95": 0.9}},
                        "one_shots": [
                            {"ref": "service_discovery.client_admission", "count": 1, "hosts": ["sd-2"]},
                            {"ref": "backend_api.fallback_config_enabled", "count": 1, "hosts": ["api-1"]},
                        ],
                    },
                ]
            }
        },
    }
}


def _sha256_bytes(s: str) -> bytes:
    return hashlib.sha256(s.encode("utf-8")).digest()


def u01(key: str) -> float:
    b = _sha256_bytes(key)
    x = int.from_bytes(b[:8], "big")
    return x / float(2**64)


def choose_from(key: str, values: List[Any]) -> Any:
    if not values:
        return ""
    idx = int(math.floor(u01(key) * len(values)))
    if idx >= len(values):
        idx = len(values) - 1
    return values[idx]


def rand_int(key: str, lo: int, hi: int) -> int:
    if hi < lo:
        lo, hi = hi, lo
    span = hi - lo + 1
    r = int(math.floor(u01(key) * span))
    if r >= span:
        r = span - 1
    return lo + r


def hex_str(key: str, length: int) -> str:
    out = ""
    ctr = 0
    while len(out) < length:
        h = hashlib.sha256((key + f"|{ctr}").encode("utf-8")).hexdigest()
        out += h
        ctr += 1
    return out[:length]


def ip_from_cidr(key: str, cidr: str) -> str:
    net, pref = cidr.split("/")
    pref = int(pref)
    base_parts = [int(x) for x in net.split(".")]
    base = (base_parts[0] << 24) | (base_parts[1] << 16) | (base_parts[2] << 8) | base_parts[3]
    host_bits = 32 - pref
    if host_bits <= 0:
        val = base
    else:
        mask = (1 << host_bits) - 1
        host = int(math.floor(u01(key) * (mask + 1)))
        if host > mask:
            host = mask
        val = (base & (~mask)) | host
    return f"{(val >> 24) & 255}.{(val >> 16) & 255}.{(val >> 8) & 255}.{val & 255}"


def norm_ppf(p: float) -> float:
    if p <= 0.0:
        return -math.inf
    if p >= 1.0:
        return math.inf

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
        return -(num / den)

    q = p - 0.5
    r = q * q
    num = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    den = (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    return num / den


def lognormal_ms_from_p50_p95(key: str, p50: float, p95: float, cap_mult: float = 3.0) -> int:
    p50 = max(0.1, float(p50))
    p95 = max(p50 * 1.001, float(p95))
    mu = math.log(p50)
    sigma = math.log(p95 / p50) / 1.6448536269514722  # norm.ppf(0.95)
    q = 0.02 + 0.96 * u01(key)  # avoid extreme tails
    z = norm_ppf(q)
    val = math.exp(mu + sigma * z)
    cap = cap_mult * p95
    if val > cap:
        val = cap
    if val < 1.0:
        val = 1.0
    return int(round(val))


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    ms = int(dt.microsecond / 1000)
    return dt.replace(microsecond=ms * 1000).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_ref(ref: str) -> Tuple[str, str]:
    a, b = ref.split(".", 1)
    return a, b


def find_placeholders(msg: str) -> List[str]:
    out = []
    i = 0
    while True:
        i = msg.find("{", i)
        if i < 0:
            break
        j = msg.find("}", i + 1)
        if j < 0:
            break
        name = msg[i + 1 : j].strip()
        if name and name not in out:
            out.append(name)
        i = j + 1
    return out


@dataclass(frozen=True)
class Interval:
    state: str  # 'n' or 'f'
    start_min: int
    end_min: int
    flow_rate_mult: Dict[str, float]
    bg_rate_mult: Dict[str, float]
    flow_lat_mult: Dict[str, Dict[str, float]]  # flow_id -> {p50, p95}


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[Tuple[str, str], Dict[str, Any]], Dict[Tuple[str, str], List[str]]]:
    comps = {c["id"]: c for c in system["components"]}
    templates: Dict[Tuple[str, str], Dict[str, Any]] = {}
    placeholder_index: Dict[Tuple[str, str], List[str]] = {}
    for cid, c in comps.items():
        for lid, t in c.get("logs", {}).items():
            templates[(cid, lid)] = t
            placeholder_index[(cid, lid)] = find_placeholders(t["msg"])
    return comps, templates, placeholder_index


def get_domain(templates: Dict[Tuple[str, str], Dict[str, Any]], comp_id: str, log_id: str, var: str, state: str) -> Optional[Dict[str, Any]]:
    t = templates[(comp_id, log_id)]
    sv = t.get("state_vars", {})
    if state in sv and var in sv[state]:
        return sv[state][var]
    return t.get("vars", {}).get(var)


def sample_from_domain(domain: Dict[str, Any], key: str) -> Any:
    k = domain["k"]
    v = domain["v"]
    if k == "ch":
        return choose_from(key, list(v))
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return rand_int(key, lo, hi)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return lo + (hi - lo) * u01(key)
    if k == "hex":
        return hex_str(key, int(v))
    if k == "ip":
        return ip_from_cidr(key, str(v))
    if k == "uuid":
        h = hex_str(key, 32)
        return f"{h[:8]}-{h[8:12]}-4{h[13:16]}-a{h[17:20]}-{h[20:32]}"
    if k == "str":
        return str(v)
    return str(v)


def det_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    n = int(math.floor(expected))
    r = expected - n
    if r <= 1e-12:
        return n
    if u01(key + "|bernoulli") < r:
        return n + 1
    return n


def schedule_times(base: datetime, start_min: int, end_min: int, count: int, key_prefix: str) -> List[datetime]:
    if count <= 0:
        return []
    t0 = base + timedelta(minutes=start_min)
    t1 = base + timedelta(minutes=end_min)
    dur_s = (t1 - t0).total_seconds()
    base_step = dur_s / count
    jitter_max = min(0.25, base_step / 10.0)  # seconds
    out: List[datetime] = []
    for i in range(count):
        frac = (i + 0.5) / count
        jitter = (u01(f"{key_prefix}|jit|{i}") - 0.5) * 2.0 * jitter_max
        ts = t0 + timedelta(seconds=frac * dur_s + jitter)
        if ts < t0:
            ts = t0
        if ts >= t1:
            ts = t1 - timedelta(milliseconds=1)
        out.append(ts)
    return out


def build_failure_intervals(scenario: Dict[str, Any]) -> List[Interval]:
    phases = scenario["scenario"]["time"]["phases"]
    n0, n1 = phases["n"]["start_min"], phases["n"]["end_min"]
    f0, f1 = phases["f"]["start_min"], phases["f"]["end_min"]
    events = sorted(scenario["scenario"]["phases"]["f"]["events"], key=lambda e: e["at_min"])

    flow_rate_mult: Dict[str, float] = {}
    bg_rate_mult: Dict[str, float] = {}
    flow_lat_mult: Dict[str, Dict[str, float]] = {}

    intervals: List[Interval] = [Interval(state="n", start_min=n0, end_min=n1, flow_rate_mult={}, bg_rate_mult={}, flow_lat_mult={})]

    boundaries = [f0] + [e["at_min"] for e in events if f0 <= e["at_min"] < f1] + [f1]
    boundaries = sorted(list(dict.fromkeys(boundaries)))
    event_by_min = {e["at_min"]: e for e in events}

    for idx, b in enumerate(boundaries[:-1]):
        if b in event_by_min:
            e = event_by_min[b]
            for k, v in e.get("rate_multipliers", {}).items():
                if "." in k:
                    bg_rate_mult[k] = float(v)
                else:
                    flow_rate_mult[k] = float(v)
            for fid, m in e.get("latency_multipliers", {}).items():
                flow_lat_mult[fid] = {"p50": float(m["p50"]), "p95": float(m["p95"])}

        intervals.append(
            Interval(
                state="f",
                start_min=b,
                end_min=boundaries[idx + 1],
                flow_rate_mult=dict(flow_rate_mult),
                bg_rate_mult=dict(bg_rate_mult),
                flow_lat_mult=dict(flow_lat_mult),
            )
        )

    return intervals


def pick_host(comps: Dict[str, Any], comp_id: str, key: str, allowed: Optional[List[str]] = None) -> str:
    hosts = list(comps[comp_id].get("hosts", []))
    if allowed is not None:
        hosts = [h for h in hosts if h in allowed]
    if not hosts:
        return ""
    return choose_from(key + "|host", hosts)


def route_to_op(route: str) -> str:
    if route == "/dash":
        return "get_dashboard"
    if route == "/api/v1/query":
        return "query_timeseries"
    return "list_monitors"


def _int_domain_bounds(domain: Optional[Dict[str, Any]]) -> Optional[Tuple[int, int]]:
    if not domain:
        return None
    if domain.get("k") != "i":
        return None
    lo, hi = int(domain["v"][0]), int(domain["v"][1])
    return lo, hi


def step_delay_bounds_ms(
    templates: Dict[Tuple[str, str], Dict[str, Any]],
    comp_id: str,
    log_id: str,
    state: str,
) -> Tuple[int, int]:
    """
    Returns (min_ms, max_ms) bounds for the *inter-log delay* used for this emission, when that delay
    is coupled to an observed timing field in the message template.

    For web_frontend.access_* dur_ms, dur_ms is total request duration, not the per-hop delay, so we
    do not bound the delay using that domain (only >=1ms).
    """
    t = templates[(comp_id, log_id)]
    # Prefer explicit observed timing fields when present.
    if "timeout_ms" in t.get("vars", {}) or ("timeout_ms" in t.get("state_vars", {}).get(state, {})):
        b = _int_domain_bounds(get_domain(templates, comp_id, log_id, "timeout_ms", state))
        if b:
            return max(1, b[0]), b[1]
    if "ingest_ms" in t.get("vars", {}) or ("ingest_ms" in t.get("state_vars", {}).get(state, {})):
        b = _int_domain_bounds(get_domain(templates, comp_id, log_id, "ingest_ms", state))
        if b:
            return max(1, b[0]), b[1]
    if "dur_ms" in t.get("vars", {}) or ("dur_ms" in t.get("state_vars", {}).get(state, {})):
        if not (comp_id == "web_frontend" and log_id in ["access_ok", "access_err"]):
            b = _int_domain_bounds(get_domain(templates, comp_id, log_id, "dur_ms", state))
            if b:
                return max(1, b[0]), b[1]
    return 1, 10**9  # effectively unbounded


def clamp_int(val: int, lo: int, hi: int) -> int:
    if val < lo:
        return lo
    if val > hi:
        return hi
    return val


def render_log_message(
    templates: Dict[Tuple[str, str], Dict[str, Any]],
    placeholder_index: Dict[Tuple[str, str], List[str]],
    comp_id: str,
    log_id: str,
    state: str,
    base_key: str,
    bound: Dict[str, Any],
) -> str:
    t = templates[(comp_id, log_id)]
    vals: Dict[str, Any] = {}

    for name in placeholder_index[(comp_id, log_id)]:
        if name in bound:
            vals[name] = bound[name]
            continue
        dom = get_domain(templates, comp_id, log_id, name, state)
        if dom is None:
            vals[name] = ""
        else:
            vals[name] = sample_from_domain(dom, base_key + f"|{comp_id}.{log_id}|{name}")

    return t["msg"].format(**vals)


def adjust_web_access_total_duration(
    templates: Dict[Tuple[str, str], Dict[str, Any]],
    emit_refs: List[Tuple[str, str]],
    delays_ms: List[int],
    state: str,
) -> None:
    """
    Ensures that for chains with web_frontend.req_start and web_frontend.access_ok/access_err,
    the elapsed time between those emitted timestamps equals a value within the access_* dur_ms domain.

    This prevents contradictions where dur_ms is clamped but timestamps reflect a different duration.
    """
    req_idx = None
    end_idx = None
    end_log_id = None
    for i, (cid, lid) in enumerate(emit_refs):
        if cid == "web_frontend" and lid == "req_start":
            req_idx = i
        if cid == "web_frontend" and lid in ("access_ok", "access_err"):
            end_idx = i
            end_log_id = lid

    if req_idx is None or end_idx is None or end_idx <= req_idx:
        return

    b = _int_domain_bounds(get_domain(templates, "web_frontend", end_log_id, "dur_ms", state))
    if not b:
        return
    lo, hi = b

    slice_start = req_idx + 1
    slice_end = end_idx  # inclusive index for delays contributing to total duration
    mins = []
    for i in range(len(delays_ms)):
        cid, lid = emit_refs[i]
        mn, mx = step_delay_bounds_ms(templates, cid, lid, state)
        mins.append(mn)

    total = sum(delays_ms[slice_start : slice_end + 1])
    min_total = sum(mins[slice_start : slice_end + 1])

    # Target total duration within domain, but never below feasible minimum.
    target = total
    if target < lo:
        target = max(lo, min_total)
    if target > hi:
        # If domain upper bound is below feasible minimum, we cannot satisfy both.
        # In that pathological case, we keep the feasible minimum (timestamps coherent) even if it exceeds hi.
        if hi < min_total:
            target = min_total
        else:
            target = hi

    if target == total:
        return

    if target > total:
        # Increase last delay in the slice (typically the final access log's delay) to match target.
        delta = target - total
        delays_ms[slice_end] += delta
        return

    # target < total: reduce delays from the end backwards, never violating per-step minimums.
    delta = total - target
    for i in range(slice_end, slice_start - 1, -1):
        reducible = delays_ms[i] - mins[i]
        if reducible <= 0:
            continue
        take = reducible if reducible < delta else delta
        delays_ms[i] -= take
        delta -= take
        if delta <= 0:
            break
    # If delta remains, target was infeasible due to mins; we already guarded via min_total,
    # but keep a final safety net.
    if delta > 0:
        # Force all to mins; timestamps remain coherent.
        for i in range(slice_start, slice_end + 1):
            delays_ms[i] = max(delays_ms[i], mins[i])


def simulate_flow_instance(
    comps: Dict[str, Any],
    templates: Dict[Tuple[str, str], Dict[str, Any]],
    placeholder_index: Dict[Tuple[str, str], List[str]],
    interval: Interval,
    flow_def: Dict[str, Any],
    start_ts: datetime,
    inst_key: str,
) -> List[Dict[str, Any]]:
    state = interval.state
    emit_refs = [parse_ref(r) for r in flow_def["emit"]]
    latency_pairs = flow_def["latency_ms"]
    assert len(emit_refs) == len(latency_pairs)

    trace_id = ""
    if SYSTEM["tracing"]["on"] and flow_def.get("trace", False):
        trace_id = hex_str(inst_key + "|trace", 32)

    host_for_comp: Dict[str, str] = {}
    for (cid, _) in emit_refs:
        if cid not in host_for_comp:
            host_for_comp[cid] = pick_host(comps, cid, inst_key + f"|{cid}")

    ctx: Dict[str, Any] = {}
    fid = flow_def["id"]

    if fid.startswith("ui_widget_"):
        route = choose_from(inst_key + "|route", ["/", "/dash", "/api/v1/query"])
        method = "GET" if route in ["/", "/dash"] else choose_from(inst_key + "|method", ["GET", "POST"])
        req_id = hex_str(inst_key + "|req_id", 16)
        user_id = rand_int(inst_key + "|user_id", 10000, 99999)
        op = route_to_op(route)
        ctx.update({"route": route, "method": method, "req_id": req_id, "user_id": user_id, "op": op})

        if fid == "ui_widget_ok":
            pass
        elif fid == "ui_widget_backend_503":
            ctx.update({"err": "upstream_503", "status": 502})
            if state == "f":
                ctx["reason"] = choose_from(inst_key + "|reason", ["config_fetch_failed", "dependency_unresolved"])
            else:
                ctx["reason"] = choose_from(inst_key + "|reason", ["dependency_unresolved", "upstream_timeout"])
        elif fid == "ui_widget_lookup_failed":
            ctx.update({"err": "lookup_failed", "status": 504, "dep": "backend-api", "err_dep": "timeout"})
    elif fid == "payload_ingest":
        payload_id = hex_str(inst_key + "|payload_id", 24)
        ctx.update(
            {
                "payload_id": payload_id,
                "src_ip": ip_from_cidr(inst_key + "|src_ip", "10.0.0.0/8"),
                "size_bytes": rand_int(inst_key + "|size_bytes", 500, 60000),
            }
        )
    elif fid.startswith("latency_tracker_dns_"):
        attempt_id = hex_str(inst_key + "|attempt_id", 12)
        ctx.update({"attempt_id": attempt_id, "qname": "latency-tracker.service.local"})
        if fid == "latency_tracker_dns_nxdomain":
            ctx.update({"reason": "nxdomain"})
        elif fid == "latency_tracker_dns_servfail":
            ctx.update({"reason": "servfail"})
        elif fid == "latency_tracker_dns_timeout":
            ctx.update({"reason": "resolver_timeout"})

    mult = interval.flow_lat_mult.get(fid, {"p50": 1.0, "p95": 1.0}) if state == "f" else {"p50": 1.0, "p95": 1.0}

    # Sample per-log inter-emission delays, then enforce any per-delay bounds that are coupled to observed timing fields.
    delays_ms: List[int] = []
    for j, ((cid, lid), (p50, p95)) in enumerate(zip(emit_refs, latency_pairs)):
        p50_s = float(p50) * float(mult.get("p50", 1.0))
        p95_s = float(p95) * float(mult.get("p95", 1.0))
        d = lognormal_ms_from_p50_p95(inst_key + f"|lat|{j}|{cid}.{lid}", p50_s, p95_s, cap_mult=3.0)

        mn, mx = step_delay_bounds_ms(templates, cid, lid, state)
        d = clamp_int(int(d), mn, mx)
        delays_ms.append(max(1, int(d)))

    # Ensure web_frontend access_* dur_ms matches the emitted timestamp deltas by adjusting delays (not clamping dur_ms).
    adjust_web_access_total_duration(templates, emit_refs, delays_ms, state)

    rows: List[Dict[str, Any]] = []
    chain_t = start_ts
    req_start_ts: Optional[datetime] = None

    for j, (cid, lid) in enumerate(emit_refs):
        chain_t = chain_t + timedelta(milliseconds=delays_ms[j])
        t = templates[(cid, lid)]

        bound = dict(ctx)
        service = comps[cid].get("svc") or ""
        host = host_for_comp.get(cid, "")

        if cid == "web_frontend" and lid == "req_start":
            req_start_ts = chain_t

        # Observed timing fields must match the chosen chronology.
        if "timeout_ms" in t.get("vars", {}):
            bound["timeout_ms"] = delays_ms[j]
        if cid == "intake_cluster" and lid == "payload_accepted":
            bound["ingest_ms"] = delays_ms[j]

        if "dur_ms" in t.get("vars", {}):
            if cid == "web_frontend" and lid in ["access_ok", "access_err"]:
                # Total duration from req_start to access_*; do NOT clamp (timestamps were adjusted to domain).
                if req_start_ts is None:
                    req_start_ts = chain_t
                bound["dur_ms"] = int(round((chain_t - req_start_ts).total_seconds() * 1000.0))
            else:
                bound["dur_ms"] = delays_ms[j]

        # Additional coherence overrides for specific flows.
        if fid == "ui_widget_lookup_failed":
            if cid == "web_frontend" and lid == "dep_lookup_failed":
                bound["err"] = ctx.get("err_dep", "timeout")
            if cid == "web_frontend" and lid == "access_err":
                bound["err"] = "lookup_failed"
                bound["status"] = 504
        if fid == "ui_widget_backend_503":
            if cid == "web_frontend" and lid == "access_err":
                bound["err"] = "upstream_503"
                bound["status"] = 502
        if fid.startswith("latency_tracker_dns_") and cid == "intake_cluster" and lid == "tracking_skipped":
            bound["reason"] = ctx["reason"]

        msg = render_log_message(templates, placeholder_index, cid, lid, state, inst_key, bound)

        rows.append(
            {
                "timestamp_dt": chain_t,
                "level": t["lvl"],
                "message": msg,
                "trace_id": trace_id,
                "service": service,
                "host": host,
            }
        )

    return rows


def simulate_background(
    base_time: datetime,
    comps: Dict[str, Any],
    templates: Dict[Tuple[str, str], Dict[str, Any]],
    placeholder_index: Dict[Tuple[str, str], List[str]],
    interval: Interval,
) -> List[Dict[str, Any]]:
    state = interval.state
    rows: List[Dict[str, Any]] = []

    for cid, comp in comps.items():
        beh = comp.get("beh", {}).get(state, {})
        for src in beh.get("emit", []):
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope") or "per_host"
            bg_key = f"{cid}.{log_id}"
            mult = 1.0
            if state == "f":
                mult = float(interval.bg_rate_mult.get(bg_key, 1.0))
            eff_per_min = per_min * mult
            if eff_per_min <= 0:
                continue

            minutes = interval.end_min - interval.start_min
            expected = eff_per_min * minutes

            if scope == "global":
                cnt = det_round(expected, f"bg|{bg_key}|{state}|{interval.start_min}-{interval.end_min}")
                times = schedule_times(base_time, interval.start_min, interval.end_min, cnt, f"bg|{bg_key}|{state}|{interval.start_min}-{interval.end_min}")
                for i, ts in enumerate(times):
                    host = pick_host(comps, cid, f"bg|{bg_key}|{state}|{interval.start_min}-{interval.end_min}|{i}")
                    msg = render_log_message(templates, placeholder_index, cid, log_id, state, f"bg|{bg_key}|{state}|{interval.start_min}-{interval.end_min}|{i}", {})
                    rows.append({"timestamp_dt": ts, "level": templates[(cid, log_id)]["lvl"], "message": msg, "trace_id": "", "service": comp.get("svc") or "", "host": host})
            else:
                hosts = comp.get("hosts", [])
                for h in hosts:
                    cnt = det_round(expected, f"bg|{bg_key}|{state}|{interval.start_min}-{interval.end_min}|host={h}")
                    times = schedule_times(base_time, interval.start_min, interval.end_min, cnt, f"bg|{bg_key}|{state}|{interval.start_min}-{interval.end_min}|host={h}")
                    for i, ts in enumerate(times):
                        msg = render_log_message(templates, placeholder_index, cid, log_id, state, f"bg|{bg_key}|{state}|{interval.start_min}-{interval.end_min}|host={h}|{i}", {})
                        rows.append({"timestamp_dt": ts, "level": templates[(cid, log_id)]["lvl"], "message": msg, "trace_id": "", "service": comp.get("svc") or "", "host": h})

    return rows


def simulate_one_shots(
    base_time: datetime,
    comps: Dict[str, Any],
    templates: Dict[Tuple[str, str], Dict[str, Any]],
    placeholder_index: Dict[Tuple[str, str], List[str]],
    scenario: Dict[str, Any],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    events = sorted(scenario["scenario"]["phases"]["f"]["events"], key=lambda e: e["at_min"])
    for e in events:
        at_min = int(e["at_min"])
        for idx, os in enumerate(e.get("one_shots", [])):
            ref = os["ref"]
            cnt = int(os["count"])
            allowed_hosts = os.get("hosts", None)
            cid, lid = parse_ref(ref)
            for k in range(cnt):
                jitter_ms = int(round(900.0 * u01(f"oneshot|{ref}|{at_min}|{idx}|{k}|jit")))
                ts = base_time + timedelta(minutes=at_min, milliseconds=jitter_ms)
                host = pick_host(comps, cid, f"oneshot|{ref}|{at_min}|{idx}|{k}", allowed=allowed_hosts)
                msg = render_log_message(templates, placeholder_index, cid, lid, "f", f"oneshot|{ref}|{at_min}|{idx}|{k}", {})
                rows.append({"timestamp_dt": ts, "level": templates[(cid, lid)]["lvl"], "message": msg, "trace_id": "", "service": comps[cid].get("svc") or "", "host": host})
    return rows


def main() -> None:
    random.seed(0)
    np.random.seed(0)

    base_time = datetime(2020, 9, 24, 0, 0, 0, tzinfo=timezone.utc)

    comps, templates, placeholder_index = build_indices(SYSTEM)
    intervals = build_failure_intervals(SCENARIO)

    flows_by_state: Dict[str, Dict[str, Dict[str, Any]]] = {"n": {}, "f": {}}
    for st in ["n", "f"]:
        for f in SYSTEM["flows"][st]["req"]:
            flows_by_state[st][f["id"]] = f

    rows: List[Dict[str, Any]] = []

    for itv in intervals:
        rows.extend(simulate_background(base_time, comps, templates, placeholder_index, itv))

    for itv in intervals:
        st = itv.state
        minutes = itv.end_min - itv.start_min
        for fid, fdef in flows_by_state[st].items():
            rpm = float(fdef["rpm"])
            mult = 1.0
            if st == "f":
                mult = float(itv.flow_rate_mult.get(fid, 1.0))
            eff_rpm = rpm * mult
            if eff_rpm <= 0:
                continue

            expected_instances = eff_rpm * minutes
            cnt = det_round(expected_instances, f"flow|{st}|{fid}|{itv.start_min}-{itv.end_min}")
            start_times = schedule_times(base_time, itv.start_min, itv.end_min, cnt, f"flow|{st}|{fid}|{itv.start_min}-{itv.end_min}")

            for i, ts in enumerate(start_times):
                inst_key = f"flowinst|{st}|{fid}|{itv.start_min}-{itv.end_min}|{i}"
                rows.extend(simulate_flow_instance(comps, templates, placeholder_index, itv, fdef, ts, inst_key))

    rows.extend(simulate_one_shots(base_time, comps, templates, placeholder_index, SCENARIO))

    df = pd.DataFrame(rows)
    df = df.sort_values(by=["timestamp_dt", "service", "host", "level", "message", "trace_id"], kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["timestamp_dt"].map(fmt_ts)

    out = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    out.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
