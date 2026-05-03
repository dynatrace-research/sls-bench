import hashlib
import math
import random
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from string import Formatter
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "tiered_cdn_cache"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["edge_cache"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "edge_cache",
            "svc": "edge-cache",
            "hosts": ["lhr1", "fra1", "syd1"],
            "logs": {
                "access_hit_local": {
                    "lvl": "INFO",
                    "msg": "edge served {req_id} host={host} uri={uri} cache=HIT tiered=false status=200 dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "host": {"k": "ch", "v": ["example.com", "shop.example.com", "images.example.com"]},
                        "uri": {"k": "str", "v": "/assets/{file}.{ext}"},
                        "dur_ms": {"k": "i", "v": [2, 80]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_tiered_upper_ok": {
                    "lvl": "INFO",
                    "msg": "edge served {req_id} host={host} uri={uri} cache=MISS tiered=true upstream=upper_tier status=200 dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "host": {"k": "ch", "v": ["example.com", "shop.example.com", "images.example.com"]},
                        "uri": {"k": "str", "v": "/assets/{file}.{ext}"},
                        "dur_ms": {"k": "i", "v": [8, 260]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_tiered_origin_ok": {
                    "lvl": "INFO",
                    "msg": "edge served {req_id} host={host} uri={uri} cache=MISS tiered=true upstream=origin status=200 dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "host": {"k": "ch", "v": ["example.com", "shop.example.com", "images.example.com"]},
                        "uri": {"k": "str", "v": "/assets/{file}.{ext}"},
                        "dur_ms": {"k": "i", "v": [35, 950]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_tiered_530": {
                    "lvl": "WARN",
                    "msg": "edge failed {req_id} host={host} uri={uri} cache=MISS tiered=true upstream=dns status=530 err=dns_no_host dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "host": {"k": "ch", "v": ["example.com", "shop.example.com", "images.example.com"]},
                        "uri": {"k": "str", "v": "/assets/{file}.{ext}"},
                        "dur_ms": {"k": "i", "v": [5, 140]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "routing_change": {
                    "lvl": "INFO",
                    "msg": "routing change: removed_pop={pop} reason={reason}",
                    "vars": {
                        "pop": {"k": "ch", "v": ["lhr1", "fra1", "syd1"]},
                        "reason": {"k": "ch", "v": ["investigation", "mitigation"]},
                    },
                },
                "edge_cache_stats": {
                    "lvl": "INFO",
                    "msg": "cache_stats pop={pop} hit_rate={hit_rate} tiered_req_rpm={tiered_rpm} evictions={evictions}",
                    "vars": {
                        "pop": {"k": "ch", "v": ["lhr1", "fra1", "syd1"]},
                        "hit_rate": {"k": "f", "v": [0.70, 0.98]},
                        "tiered_rpm": {"k": "i", "v": [50, 400]},
                        "evictions": {"k": "i", "v": [0, 500]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "edge_cache_stats", "per_min": 2.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "edge_cache_stats", "per_min": 2.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "upper_tier_cache",
            "svc": "upper-tier-cache",
            "hosts": ["atl1", "iad1"],
            "logs": {
                "upper_recv_present": {
                    "lvl": "INFO",
                    "msg": "upper recv tiered {req_id} host={host} control_ctx=present trace={trace_id}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "host": {"k": "ch", "v": ["example.com", "shop.example.com", "images.example.com"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "upper_recv_missing": {
                    "lvl": "WARN",
                    "msg": "upper recv tiered {req_id} host=- control_ctx=missing trace={trace_id}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                },
                "upper_served_cache": {
                    "lvl": "INFO",
                    "msg": "upper served from cache key={cache_key} age_s={age_s} req_id={req_id}",
                    "vars": {
                        "cache_key": {"k": "str", "v": "cache:{host}:{uri}"},
                        "age_s": {"k": "i", "v": [0, 3600]},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "upper_cache_miss": {
                    "lvl": "INFO",
                    "msg": "upper cache miss key={cache_key} req_id={req_id}",
                    "vars": {"cache_key": {"k": "str", "v": "cache:{host}:{uri}"}, "req_id": {"k": "uuid", "v": None}},
                },
                "upper_no_host_error": {
                    "lvl": "ERROR",
                    "msg": "upper origin resolve failed req_id={req_id} cause=empty_qname_from_missing_control_host trace={trace_id}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                },
                "release_started": {
                    "lvl": "INFO",
                    "msg": "deploy started component=upper_tier_cache version={version} change=opentracing_wrap clear_control_headers=true rollout=canary",
                    "vars": {"version": {"k": "ch", "v": ["2022.10.25.1"]}},
                },
                "rollback_completed": {
                    "lvl": "INFO",
                    "msg": "rollback completed component=upper_tier_cache version={version} target=stable",
                    "vars": {"version": {"k": "ch", "v": ["2022.10.24.9"]}},
                },
                "accelerated_rollback_started": {
                    "lvl": "WARN",
                    "msg": "accelerated rollback initiated component=upper_tier_cache scope=upper_tiers",
                    "vars": {},
                },
                "upper_cache_stats": {
                    "lvl": "INFO",
                    "msg": "upper_cache_stats pop={pop} hit_rate={hit_rate} miss_rate={miss_rate} trace_wrap={trace_wrap}",
                    "vars": {
                        "pop": {"k": "ch", "v": ["atl1", "iad1"]},
                        "hit_rate": {"k": "f", "v": [0.50, 0.95]},
                        "miss_rate": {"k": "f", "v": [0.05, 0.50]},
                    },
                    "state_vars": {
                        "n": {"trace_wrap": {"k": "ch", "v": [False]}},
                        "f": {"trace_wrap": {"k": "ch", "v": [True, False]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "upper_cache_stats", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "upper_cache_stats", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "internal_dns",
            "svc": "internal-dns",
            "hosts": ["dns1", "dns2"],
            "logs": {
                "dns_resolve_ok": {
                    "lvl": "INFO",
                    "msg": "dns ok qname={qname} ip={ip} ttl_s={ttl_s} req_id={req_id} dur_ms={dur_ms}",
                    "vars": {
                        "qname": {"k": "ch", "v": ["example.com", "shop.example.com", "images.example.com"]},
                        "ip": {"k": "ip", "v": "10.0.0.0/8"},
                        "ttl_s": {"k": "i", "v": [30, 300]},
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [1, 25]},
                    },
                },
                "dns_err_empty_qname": {
                    "lvl": "ERROR",
                    "msg": "dns error req_id={req_id} rcode=SERVFAIL detail=empty_qname",
                    "vars": {"req_id": {"k": "uuid", "v": None}},
                },
                "dns_release_started": {
                    "lvl": "INFO",
                    "msg": "deploy started component=internal_dns version={version}",
                    "vars": {"version": {"k": "ch", "v": ["2022.10.25.5"]}},
                },
                "dns_health": {
                    "lvl": "INFO",
                    "msg": "dns_health node={node} qps={qps} servfail_pct={servfail_pct}",
                    "vars": {"node": {"k": "ch", "v": ["dns1", "dns2"]}, "qps": {"k": "i", "v": [0, 10]}},
                    "state_vars": {
                        "n": {"servfail_pct": {"k": "f", "v": [0.0, 0.1]}},
                        "f": {"servfail_pct": {"k": "f", "v": [0.0, 80.0]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "dns_health", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "dns_health", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "origin_connector",
            "svc": "origin-connector",
            "hosts": ["fetch1"],
            "logs": {
                "origin_fetch_ok": {
                    "lvl": "INFO",
                    "msg": "origin fetch ok host={host} bytes={bytes} req_id={req_id} dur_ms={dur_ms}",
                    "vars": {
                        "host": {"k": "ch", "v": ["example.com", "shop.example.com", "images.example.com"]},
                        "bytes": {"k": "i", "v": [500, 5000000]},
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [20, 900]},
                    },
                },
                "origin_pool_stats": {
                    "lvl": "INFO",
                    "msg": "origin_pool_stats conns_active={conns} timeouts={timeouts}",
                    "vars": {"conns": {"k": "i", "v": [50, 500]}, "timeouts": {"k": "i", "v": [0, 20]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "origin_pool_stats", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "origin_pool_stats", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        {
            "id": "observability",
            "svc": "obs-alert",
            "hosts": ["mon1"],
            "logs": {
                "sli_530_sample": {
                    "lvl": "INFO",
                    "msg": "sli_http_530 pct={pct_530} window_s=60 source=edge",
                    "vars": {},
                    "state_vars": {"n": {"pct_530": {"k": "f", "v": [0.0, 0.2]}}, "f": {"pct_530": {"k": "f", "v": [0.2, 6.0]}}},
                },
                "alert_fired": {
                    "lvl": "WARN",
                    "msg": "alert fired alert={alert} value={value} threshold={threshold}",
                    "vars": {
                        "alert": {"k": "ch", "v": ["customer_reported_5xx", "http_530_spike"]},
                        "value": {"k": "f", "v": [0.2, 6.0]},
                        "threshold": {"k": "f", "v": [0.2, 2.0]},
                    },
                },
                "incident_declared": {
                    "lvl": "ERROR",
                    "msg": "incident declared id={inc_id} severity={sev}",
                    "vars": {"inc_id": {"k": "str", "v": "INC-{digits}"}, "sev": {"k": "ch", "v": ["SEV-2", "SEV-1"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "sli_530_sample", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "sli_530_sample", "per_min": 1.0, "scope": "global"}]},
            },
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "client_asset_local_cache_hit",
                    "rpm": 900.0,
                    "emit": ["edge_cache.access_hit_local"],
                    "latency_ms": [[3, 30]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "client_asset_tiered_upper_cache_hit",
                    "rpm": 150.0,
                    "emit": [
                        "upper_tier_cache.upper_recv_present",
                        "upper_tier_cache.upper_served_cache",
                        "edge_cache.access_tiered_upper_ok",
                    ],
                    "latency_ms": [[2, 20], [1, 10], [8, 220]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "client_asset_tiered_upper_miss_origin_ok",
                    "rpm": 50.0,
                    "emit": [
                        "upper_tier_cache.upper_recv_present",
                        "upper_tier_cache.upper_cache_miss",
                        "internal_dns.dns_resolve_ok",
                        "origin_connector.origin_fetch_ok",
                        "edge_cache.access_tiered_origin_ok",
                    ],
                    "latency_ms": [[2, 25], [1, 15], [2, 25], [40, 850], [35, 950]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "client_asset_local_cache_hit",
                    "rpm": 900.0,
                    "emit": ["edge_cache.access_hit_local"],
                    "latency_ms": [[3, 30]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "client_asset_tiered_upper_cache_hit",
                    "rpm": 145.0,
                    "emit": [
                        "upper_tier_cache.upper_recv_present",
                        "upper_tier_cache.upper_served_cache",
                        "edge_cache.access_tiered_upper_ok",
                    ],
                    "latency_ms": [[2, 25], [1, 12], [8, 240]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "client_asset_tiered_upper_miss_origin_ok",
                    "rpm": 45.0,
                    "emit": [
                        "upper_tier_cache.upper_recv_present",
                        "upper_tier_cache.upper_cache_miss",
                        "internal_dns.dns_resolve_ok",
                        "origin_connector.origin_fetch_ok",
                        "edge_cache.access_tiered_origin_ok",
                    ],
                    "latency_ms": [[2, 25], [1, 15], [2, 25], [45, 900], [35, 950]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "client_asset_tiered_dns_error_530",
                    "rpm": 10.0,
                    "emit": [
                        "upper_tier_cache.upper_recv_missing",
                        "internal_dns.dns_err_empty_qname",
                        "upper_tier_cache.upper_no_host_error",
                        "edge_cache.access_tiered_530",
                    ],
                    "latency_ms": [[2, 20], [1, 10], [1, 8], [5, 140]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "tiered_cache_tracing_headers_530"},
    "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 50}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {},
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "upper_tier_cache.release_started", "count": 1, "hosts": ["atl1"]},
                        {"ref": "internal_dns.dns_release_started", "count": 1, "hosts": ["dns1"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 28,
                    "rate_multipliers": {
                        "client_asset_tiered_dns_error_530": 3.0,
                        "client_asset_tiered_upper_cache_hit": 0.9,
                        "client_asset_tiered_upper_miss_origin_ok": 0.9,
                        "observability.sli_530_sample": 2.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "observability.alert_fired", "count": 1, "hosts": ["mon1"]}],
                },
                {
                    "order": 3,
                    "at_min": 31,
                    "rate_multipliers": {
                        "client_asset_tiered_dns_error_530": 2.5,
                        "client_asset_tiered_upper_cache_hit": 0.92,
                        "client_asset_tiered_upper_miss_origin_ok": 0.92,
                        "observability.sli_530_sample": 2.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "edge_cache.routing_change", "count": 1, "hosts": ["lhr1"]}],
                },
                {
                    "order": 4,
                    "at_min": 34,
                    "rate_multipliers": {
                        "client_asset_tiered_dns_error_530": 5.5,
                        "client_asset_tiered_upper_cache_hit": 0.75,
                        "client_asset_tiered_upper_miss_origin_ok": 0.75,
                        "observability.sli_530_sample": 4.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "observability.alert_fired", "count": 1, "hosts": ["mon1"]},
                        {"ref": "observability.incident_declared", "count": 1, "hosts": ["mon1"]},
                    ],
                },
                {
                    "order": 5,
                    "at_min": 40,
                    "rate_multipliers": {
                        "client_asset_tiered_dns_error_530": 3.0,
                        "client_asset_tiered_upper_cache_hit": 0.8,
                        "client_asset_tiered_upper_miss_origin_ok": 0.8,
                        "observability.sli_530_sample": 3.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "upper_tier_cache.rollback_completed", "count": 1, "hosts": ["atl1"]}],
                },
                {
                    "order": 6,
                    "at_min": 45,
                    "rate_multipliers": {
                        "client_asset_tiered_dns_error_530": 2.0,
                        "client_asset_tiered_upper_cache_hit": 0.9,
                        "client_asset_tiered_upper_miss_origin_ok": 0.9,
                        "observability.sli_530_sample": 2.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "upper_tier_cache.accelerated_rollback_started", "count": 1, "hosts": ["iad1"]}],
                },
            ]
        }
    },
}

SEED = 1337
BASE_TIME = datetime(2022, 10, 25, 0, 0, 0, tzinfo=timezone.utc)

random.seed(SEED)
np.random.seed(SEED)


def _hash_bytes(key: str) -> bytes:
    h = hashlib.sha256()
    h.update(f"{SEED}|{key}".encode("utf-8"))
    return h.digest()


def stable_u64(key: str) -> int:
    return int.from_bytes(_hash_bytes(key)[:8], "big", signed=False)


def stable_uniform(key: str) -> float:
    return (stable_u64(key) & ((1 << 53) - 1)) / float(1 << 53)


def choose_from_list(values: List[Any], key: str) -> Any:
    if not values:
        return None
    idx = stable_u64(key) % len(values)
    return values[idx]


def deterministic_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    return base + (1 if stable_uniform(f"round:{key}") < frac else 0)


def iso8601_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    ms = int(dt.microsecond / 1000)
    return dt.replace(microsecond=ms * 1000).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def gen_hex(n: int, key: str) -> str:
    return hashlib.sha256(f"{SEED}|{key}".encode("utf-8")).hexdigest()[:n]


def gen_uuid(key: str) -> str:
    hx = gen_hex(32, f"uuid:{key}")
    return f"{hx[0:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"


def gen_ip_10_8(key: str) -> str:
    b = _hash_bytes(f"ip:{key}")
    return f"10.{b[0]}.{b[1]}.{b[2]}"


def schedule_times(start: datetime, end: datetime, n: int, key: str) -> List[datetime]:
    if n <= 0:
        return []
    total_s = (end - start).total_seconds()
    if total_s <= 0:
        return [start for _ in range(n)]
    step = total_s / n
    max_jitter = min(0.35, 0.2 * step)
    times = []
    for i in range(n):
        center = (i + 0.5) * step
        j = (stable_uniform(f"jitter:{key}:{i}") - 0.5) * 2.0 * max_jitter
        t = start + timedelta(seconds=center + j)
        if t < start:
            t = start + timedelta(milliseconds=i % 7)
        if t >= end:
            t = end - timedelta(milliseconds=1 + (i % 7))
        times.append(t)
    return times


def sample_latency_ms(p50: float, p95: float, key: str, mult: float = 1.0) -> int:
    u = stable_uniform(f"lat:{key}")
    q = 0.30 + 0.40 * u  # [0.30, 0.70]
    val = p50 + q * (p95 - p50)
    val *= mult
    val = max(1.0, val)
    return int(round(val))


@dataclass(frozen=True)
class LogTemplate:
    component_id: str
    log_id: str
    level: str
    msg: str
    vars: Dict[str, Any]
    state_vars: Dict[str, Any]


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, LogTemplate], Dict[str, Dict[str, Any]]]:
    comp_by_id: Dict[str, Any] = {c["id"]: c for c in system["components"]}
    log_by_ref: Dict[str, LogTemplate] = {}
    for comp in system["components"]:
        cid = comp["id"]
        for lid, tmpl in comp["logs"].items():
            ref = f"{cid}.{lid}"
            log_by_ref[ref] = LogTemplate(
                component_id=cid,
                log_id=lid,
                level=tmpl["lvl"],
                msg=tmpl["msg"],
                vars=tmpl.get("vars", {}) or {},
                state_vars=tmpl.get("state_vars", {}) or {},
            )
    flows = system["flows"]
    flows_by_state: Dict[str, Dict[str, Any]] = {"n": {}, "f": {}}
    for st in ("n", "f"):
        for fr in flows[st]["req"]:
            flows_by_state[st][fr["id"]] = fr
    return comp_by_id, log_by_ref, flows_by_state


COMP_BY_ID, LOG_BY_REF, FLOW_BY_STATE = build_indices(SYSTEM)


def extract_fields(fmt: str) -> List[str]:
    fields: List[str] = []
    for _, field, _, _ in Formatter().parse(fmt):
        if field:
            fields.append(field)
    return fields


def render_str_hint(hint: str, ctx: Dict[str, Any], key: str) -> str:
    if "{digits}" in hint:
        digits = str(10000 + (stable_u64(f"digits:{key}") % 90000))
        ctx2 = dict(ctx)
        ctx2["digits"] = digits
        return hint.format_map(DefaultDict(ctx2))
    if "{file}" in hint or "{ext}" in hint:
        exts = ["js", "css", "png", "jpg", "svg", "woff2"]
        ext = choose_from_list(exts, f"ext:{key}")
        file_n = 1000 + (stable_u64(f"file:{key}") % 9000)
        ctx2 = dict(ctx)
        ctx2["file"] = f"file{file_n}"
        ctx2["ext"] = ext
        return hint.format_map(DefaultDict(ctx2))
    if "{host}" in hint or "{uri}" in hint:
        return hint.format_map(DefaultDict(ctx))
    return hint


class DefaultDict(dict):
    def __missing__(self, k: str) -> str:
        return ""


def fmt_bool(v: Any) -> Any:
    if isinstance(v, bool):
        return "true" if v else "false"
    return v


def pick_value(domain: Dict[str, Any], key: str, ctx: Dict[str, Any]) -> Any:
    k = domain.get("k")
    v = domain.get("v")
    if k == "uuid":
        return gen_uuid(key)
    if k == "hex":
        return gen_hex(int(v), key)
    if k == "ip":
        return gen_ip_10_8(key)
    if k == "ch":
        return choose_from_list(list(v), key)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi <= lo:
            return lo
        u = stable_uniform(f"int:{key}")
        return lo + int(math.floor(u * (hi - lo + 1)))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        u = stable_uniform(f"float:{key}")
        val = lo + u * (hi - lo)
        return float(f"{val:.2f}")
    if k == "str":
        return render_str_hint(str(v), ctx, key)
    return ""


def render_message(tmpl: LogTemplate, bound: Dict[str, Any], state: str, key: str) -> str:
    fields = extract_fields(tmpl.msg)
    ctx = dict(bound)
    for f in fields:
        if f in ctx:
            continue
        domain = None
        if tmpl.state_vars and state in tmpl.state_vars and f in tmpl.state_vars[state]:
            domain = tmpl.state_vars[state][f]
        elif f in tmpl.vars:
            domain = tmpl.vars[f]
        if domain is None:
            ctx[f] = ""
        else:
            ctx[f] = pick_value(domain, f"{key}:{tmpl.component_id}.{tmpl.log_id}:{f}", ctx)

    for k, v in list(ctx.items()):
        ctx[k] = fmt_bool(v)

    return tmpl.msg.format_map(DefaultDict(ctx))


def minute_to_dt(minute: float) -> datetime:
    return BASE_TIME + timedelta(minutes=minute)


def build_failure_intervals() -> List[Dict[str, Any]]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    if not events or events[0]["at_min"] != f_start:
        events = [{"order": 0, "at_min": f_start, "rate_multipliers": {}, "latency_multipliers": {}, "one_shots": []}] + events

    intervals: List[Dict[str, Any]] = []
    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, float] = {}

    for i, ev in enumerate(events):
        t0 = ev["at_min"]
        t1 = events[i + 1]["at_min"] if i + 1 < len(events) else f_end
        for k, m in (ev.get("rate_multipliers") or {}).items():
            active_rate[k] = float(m)
        for k, m in (ev.get("latency_multipliers") or {}).items():
            active_lat[k] = float(m)
        if t1 > t0:
            intervals.append({"start_min": t0, "end_min": t1, "rate_mult": dict(active_rate), "lat_mult": dict(active_lat)})
    return intervals


FAILURE_INTERVALS = build_failure_intervals()


def active_multiplier(rate_mult: Dict[str, float], key: str) -> float:
    return float(rate_mult.get(key, 1.0))


def compute_dns_servfail_pct(state: str, dns_error_mult: float, key: str) -> float:
    if state == "n":
        u = stable_uniform(f"svf:{key}")
        return float(f"{(u * 0.1):.2f}")
    if dns_error_mult <= 1.1:
        lo, hi = 0.0, 5.0
    elif dns_error_mult <= 2.2:
        lo, hi = 4.0, 20.0
    elif dns_error_mult <= 3.2:
        lo, hi = 10.0, 35.0
    elif dns_error_mult <= 4.5:
        lo, hi = 20.0, 55.0
    else:
        lo, hi = 35.0, 75.0
    u = stable_uniform(f"svf:{key}")
    val = lo + u * (hi - lo)
    return float(f"{min(80.0, max(0.0, val)):.2f}")


def compute_sli_530_pct(state: str, dns_error_mult: float, key: str) -> float:
    if state == "n":
        u = stable_uniform(f"sli:{key}")
        return float(f"{(u * 0.2):.2f}")
    base = 0.2 + min(5.8, 0.7 * (dns_error_mult - 1.0))
    jitter = (stable_uniform(f"sli:{key}") - 0.5) * 0.6
    val = base + jitter
    val = min(6.0, max(0.2, val))
    return float(f"{val:.2f}")


def choose_component_host(component_id: str, req_key: str, forced: Optional[str] = None) -> str:
    comp = COMP_BY_ID[component_id]
    hosts = comp.get("hosts") or []
    if forced is not None:
        return forced
    if not hosts:
        return ""
    return str(choose_from_list(hosts, f"host:{component_id}:{req_key}"))


def emit_log(
    rows: List[Tuple[datetime, str, str, str, str, str]],
    when: datetime,
    tmpl_ref: str,
    state: str,
    trace_id: str,
    host: str,
    bound: Dict[str, Any],
    key: str,
):
    tmpl = LOG_BY_REF[tmpl_ref]
    comp = COMP_BY_ID[tmpl.component_id]
    message = render_message(tmpl, bound, state, key)
    rows.append((when, tmpl.level, message, trace_id, comp.get("svc", "") or "", host or ""))


def _tmpl_int_bounds(ref: str, field: str) -> Optional[Tuple[int, int]]:
    tmpl = LOG_BY_REF[ref]
    dom = (tmpl.vars or {}).get(field)
    if not dom or dom.get("k") != "i":
        return None
    v = dom.get("v")
    if not isinstance(v, list) or len(v) != 2:
        return None
    return int(v[0]), int(v[1])


def _adjust_latencies_to_total(step_ms: List[int], target_total: int) -> List[int]:
    # Keep positive ms per step; deterministically scale and then fix rounding drift on the last step.
    if not step_ms:
        return step_ms
    raw_total = sum(step_ms)
    if raw_total <= 0:
        return [1 for _ in step_ms]
    if target_total <= 0:
        target_total = 1

    scale = target_total / float(raw_total)
    scaled = [max(1, int(round(x * scale))) for x in step_ms]
    drift = target_total - sum(scaled)
    # Fix drift by adjusting the last step (bounded to stay >= 1)
    if drift != 0:
        last = scaled[-1] + drift
        if last < 1:
            # Borrow from previous steps if needed
            needed = 1 - last
            scaled[-1] = 1
            for j in range(len(scaled) - 2, -1, -1):
                take = min(needed, max(0, scaled[j] - 1))
                if take > 0:
                    scaled[j] -= take
                    needed -= take
                if needed <= 0:
                    break
        else:
            scaled[-1] = last
    # Final safety: ensure all >=1; if this changed total, fix last again
    for i in range(len(scaled)):
        if scaled[i] < 1:
            scaled[i] = 1
    drift2 = target_total - sum(scaled)
    if drift2 != 0:
        scaled[-1] = max(1, scaled[-1] + drift2)
    return scaled


def simulate_flow_instance(
    rows: List[Tuple[datetime, str, str, str, str, str]],
    flow: Dict[str, Any],
    state: str,
    start_time: datetime,
    interval_rate_mult: Dict[str, float],
    interval_lat_mult: Dict[str, float],
    inst_key: str,
    seq_num: int,
):
    flow_id = flow["id"]
    trace_on = bool(SYSTEM["tracing"]["on"]) and bool(flow.get("trace", False))
    trace_id = gen_hex(32, f"trace:{flow_id}:{seq_num}") if trace_on else ""

    req_id = gen_uuid(f"req:{flow_id}:{seq_num}")
    customer_host = choose_from_list(["example.com", "shop.example.com", "images.example.com"], f"custhost:{flow_id}:{seq_num}")
    uri = render_str_hint("/assets/{file}.{ext}", {"host": customer_host}, f"uri:{flow_id}:{seq_num}")

    comp_host_map: Dict[str, str] = {}
    for ref in flow["emit"]:
        cid, _ = ref.split(".", 1)
        if cid not in comp_host_map:
            comp_host_map[cid] = choose_component_host(cid, f"{flow_id}:{seq_num}")

    # Sample per-emit step latencies (delays since previous emitted log), but keep access-log dur_ms consistent
    # with the access-log template dur_ms domain by scaling the whole chain if needed.
    lat_mult = float(interval_lat_mult.get(flow_id, 1.0))
    step_ms: List[int] = []
    for i, (p50, p95) in enumerate(flow["latency_ms"]):
        step_ms.append(sample_latency_ms(float(p50), float(p95), f"{inst_key}:{flow_id}:{seq_num}:lat:{i}", mult=lat_mult))

    total_ms = sum(step_ms)
    last_ref = flow["emit"][-1]
    dur_bounds = _tmpl_int_bounds(last_ref, "dur_ms")
    if dur_bounds is not None:
        lo, hi = dur_bounds
        target_total = total_ms
        if total_ms < lo:
            target_total = lo
        elif total_ms > hi:
            target_total = hi
        if target_total != total_ms:
            step_ms = _adjust_latencies_to_total(step_ms, target_total)
            total_ms = sum(step_ms)

    cumulative_ms = 0
    for i, ref in enumerate(flow["emit"]):
        d_ms = step_ms[i]
        cumulative_ms += d_ms
        when = start_time + timedelta(milliseconds=cumulative_ms)

        cid, lid = ref.split(".", 1)

        bound: Dict[str, Any] = {"req_id": req_id, "trace_id": trace_id, "host": customer_host, "qname": customer_host, "uri": uri}

        if "cache_key" in extract_fields(LOG_BY_REF[ref].msg):
            bound["cache_key"] = render_str_hint("cache:{host}:{uri}", {"host": customer_host, "uri": uri}, f"ck:{flow_id}:{seq_num}")

        # Observed timing fields must follow the chosen chronology
        if lid in ("access_hit_local", "access_tiered_upper_ok", "access_tiered_origin_ok", "access_tiered_530"):
            bound["dur_ms"] = int(cumulative_ms)
        elif lid in ("dns_resolve_ok", "origin_fetch_ok"):
            bound["dur_ms"] = int(d_ms)

        emit_log(
            rows=rows,
            when=when,
            tmpl_ref=ref,
            state=state,
            trace_id=trace_id,
            host=comp_host_map.get(cid, ""),
            bound=bound,
            key=f"{inst_key}:{flow_id}:{seq_num}:{ref}",
        )


def simulate_background_interval(
    rows: List[Tuple[datetime, str, str, str, str, str]],
    state: str,
    start_min: int,
    end_min: int,
    rate_mult: Dict[str, float],
):
    start = minute_to_dt(start_min)
    end = minute_to_dt(end_min)
    duration_min = (end - start).total_seconds() / 60.0

    dns_error_mult = float(rate_mult.get("client_asset_tiered_dns_error_530", 1.0))

    for comp in SYSTEM["components"]:
        cid = comp["id"]
        beh = comp.get("beh", {}).get(state, {})
        for src in beh.get("emit", []):
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope") or "per_host"
            ref_key = f"{cid}.{log_id}"
            mult = 1.0
            if state == "f":
                mult = active_multiplier(rate_mult, ref_key)
            eff_per_min = per_min * mult
            expected = eff_per_min * duration_min

            if scope == "global":
                n = deterministic_round(expected, f"bg:{state}:{start_min}-{end_min}:{ref_key}:global")
                if n <= 0:
                    continue
                host = (comp.get("hosts") or [""])[0] if comp.get("hosts") else ""
                times = schedule_times(start, end, n, f"bg:{state}:{start_min}-{end_min}:{ref_key}:global")
                for i, t in enumerate(times):
                    bound: Dict[str, Any] = {}
                    if ref_key == "internal_dns.dns_health":
                        bound["node"] = host
                        bound["servfail_pct"] = compute_dns_servfail_pct(state, dns_error_mult, f"{start_min}:{i}:{host}")
                    if ref_key == "observability.sli_530_sample":
                        bound["pct_530"] = compute_sli_530_pct(state, dns_error_mult, f"{start_min}:{i}:global")
                    emit_log(rows, t, ref_key, state, "", host, bound, f"bg:{state}:{start_min}:{ref_key}:{i}")
            else:
                for host in (comp.get("hosts") or [""]):
                    n = deterministic_round(expected, f"bg:{state}:{start_min}-{end_min}:{ref_key}:{host}")
                    if n <= 0:
                        continue
                    times = schedule_times(start, end, n, f"bg:{state}:{start_min}-{end_min}:{ref_key}:{host}")
                    for i, t in enumerate(times):
                        bound = {}
                        if ref_key == "edge_cache.edge_cache_stats":
                            bound["pop"] = host
                        if ref_key == "upper_tier_cache.upper_cache_stats":
                            bound["pop"] = host
                            if state == "f":
                                u = stable_uniform(f"wrap:{start_min}:{host}:{i}")
                                if dns_error_mult >= 5.0:
                                    bound["trace_wrap"] = (u < 0.75)
                                elif dns_error_mult >= 3.0:
                                    bound["trace_wrap"] = (u < 0.55)
                                else:
                                    bound["trace_wrap"] = (u < 0.35)
                        if ref_key == "internal_dns.dns_health":
                            bound["node"] = host
                            bound["servfail_pct"] = compute_dns_servfail_pct(state, dns_error_mult, f"{start_min}:{i}:{host}")
                        emit_log(rows, t, ref_key, state, "", host, bound, f"bg:{state}:{start_min}:{ref_key}:{host}:{i}")


def simulate_flow_interval(
    rows: List[Tuple[datetime, str, str, str, str, str]],
    state: str,
    start_min: int,
    end_min: int,
    rate_mult: Dict[str, float],
    lat_mult: Dict[str, float],
    seq_start: int,
) -> int:
    start = minute_to_dt(start_min)
    end = minute_to_dt(end_min)
    duration_min = (end - start).total_seconds() / 60.0
    seq = seq_start

    for flow in SYSTEM["flows"][state]["req"]:
        flow_id = flow["id"]
        mult = 1.0
        if state == "f":
            mult = active_multiplier(rate_mult, flow_id)
        eff_rpm = float(flow["rpm"]) * mult
        expected_instances = eff_rpm * duration_min
        n_inst = deterministic_round(expected_instances, f"flow:{state}:{start_min}-{end_min}:{flow_id}")
        if n_inst <= 0:
            continue
        starts = schedule_times(start, end, n_inst, f"flow:{state}:{start_min}-{end_min}:{flow_id}")
        for st in starts:
            simulate_flow_instance(
                rows=rows,
                flow=flow,
                state=state,
                start_time=st,
                interval_rate_mult=rate_mult,
                interval_lat_mult=lat_mult,
                inst_key=f"flowinst:{state}:{start_min}-{end_min}:{flow_id}",
                seq_num=seq,
            )
            seq += 1
    return seq


def emit_one_shots(rows: List[Tuple[datetime, str, str, str, str, str]]):
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for ev in events:
        at = minute_to_dt(ev["at_min"])
        shots = ev.get("one_shots") or []
        for shot in shots:
            ref = shot["ref"]
            tmpl = LOG_BY_REF[ref]
            cid = tmpl.component_id
            comp = COMP_BY_ID[cid]
            allowed_hosts = shot.get("hosts") or (comp.get("hosts") or [""])
            count = int(shot["count"])
            times = [
                at
                + timedelta(milliseconds=50 * i)
                + timedelta(seconds=stable_uniform(f"oneshot:{ref}:{ev['at_min']}:{i}") * 2.0)
                for i in range(count)
            ]
            for i, t in enumerate(times):
                host = str(choose_from_list(allowed_hosts, f"oneshot_host:{ref}:{ev['at_min']}:{i}"))
                bound: Dict[str, Any] = {}

                if ref == "edge_cache.routing_change":
                    bound["pop"] = host
                    bound["reason"] = "investigation" if ev["order"] == 3 else "mitigation"
                elif ref == "observability.alert_fired":
                    bound["alert"] = "customer_reported_5xx" if ev["at_min"] == 28 else "http_530_spike"
                    dns_mult = float((ev.get("rate_multipliers") or {}).get("client_asset_tiered_dns_error_530", 3.0))
                    bound["threshold"] = 2.0 if ev["at_min"] >= 34 else 0.5
                    bound["value"] = min(6.0, max(0.2, 0.6 + 0.9 * dns_mult))
                    bound["value"] = float(f"{bound['value']:.2f}")
                    bound["threshold"] = float(f"{float(bound['threshold']):.2f}")
                elif ref == "observability.incident_declared":
                    bound["inc_id"] = render_str_hint("INC-{digits}", {}, f"inc:{ev['at_min']}:{i}")
                    bound["sev"] = "SEV-1" if ev["at_min"] >= 34 else "SEV-2"

                emit_log(rows, t, ref, "f", "", host, bound, f"oneshot:{ev['order']}:{ref}:{i}")


def main():
    rows: List[Tuple[datetime, str, str, str, str, str]] = []

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    simulate_background_interval(rows, "n", n_start, n_end, rate_mult={})
    seq = 0
    seq = simulate_flow_interval(rows, "n", n_start, n_end, rate_mult={}, lat_mult={}, seq_start=seq)

    for interval in FAILURE_INTERVALS:
        smin, emin = interval["start_min"], interval["end_min"]
        simulate_background_interval(rows, "f", smin, emin, rate_mult=interval["rate_mult"])
        seq = simulate_flow_interval(rows, "f", smin, emin, rate_mult=interval["rate_mult"], lat_mult=interval["lat_mult"], seq_start=seq)

    emit_one_shots(rows)

    df = pd.DataFrame(rows, columns=["_dt", "level", "message", "trace_id", "service", "host"])
    df.sort_values(by=["_dt", "service", "host", "level"], inplace=True, kind="mergesort")
    df["timestamp"] = df["_dt"].map(iso8601_ms)
    df.drop(columns=["_dt"], inplace=True)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    def clean_trace(x: Any) -> str:
        if not x:
            return ""
        s = str(x).lower()
        if re.fullmatch(r"[0-9a-f]{32}", s):
            return s
        return ""

    df["trace_id"] = df["trace_id"].map(clean_trace)
    df.to_csv("logs.csv", index=False)

    n_rows = len(df)
    assert 20000 <= n_rows <= 100000, f"Row count {n_rows} out of target range"


if __name__ == "__main__":
    main()
