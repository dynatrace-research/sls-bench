import math
import hashlib
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import numpy as np
import pandas as pd

# Deterministic seeding (even though the simulator uses hash-based determinism,
# verifier expects explicit seeding calls).
random.seed(0)
np.random.seed(0)

# ----------------------------
# Embedded normalized model data
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "id": "anycast_cdn_bgp_control_plane",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "netcfg_tool",
            "svc": "netcfg",
            "hosts": ["netcfg-1"],
            "logs": {
                "change_applied": {
                    "lvl": "INFO",
                    "msg": "change {change_id} applied on {device} iface={iface} policy=hkg_outbound_export_full",
                    "vars": {
                        "change_id": {"k": "hex", "v": 12},
                        "device": {"k": "ch", "v": ["rtr-hkg-1"]},
                        "iface": {"k": "ch", "v": ["inbound0"]},
                    },
                },
                "change_reverted": {
                    "lvl": "INFO",
                    "msg": "change {change_id} applied on {device} iface={iface} policy=hkg_inbound_filter_restore",
                    "vars": {
                        "change_id": {"k": "hex", "v": 12},
                        "device": {"k": "ch", "v": ["rtr-hkg-1"]},
                        "iface": {"k": "ch", "v": ["inbound0"]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "hkg_router",
            "svc": "edge-router",
            "hosts": ["rtr-hkg-1"],
            "logs": {
                "bgp_announce": {
                    "lvl": "WARN",
                    "msg": "BGP announced prefixes={prefix_count} to neighbor_as={neighbor_as} session={session}",
                    "vars": {
                        "prefix_count": {"k": "i", "v": [400, 900]},
                        "neighbor_as": {"k": "i", "v": [64500, 64500]},
                        "session": {"k": "ch", "v": ["BGP-HKG-TRANSIT1"]},
                    },
                },
                "bgp_withdraw": {
                    "lvl": "INFO",
                    "msg": "BGP withdrew prefixes={prefix_count} from neighbor_as={neighbor_as} session={session}",
                    "vars": {
                        "prefix_count": {"k": "i", "v": [400, 900]},
                        "neighbor_as": {"k": "i", "v": [64500, 64500]},
                        "session": {"k": "ch", "v": ["BGP-HKG-TRANSIT1"]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "upstream_transit",
            "svc": "transit",
            "hosts": ["transit-1"],
            "logs": {
                "bgp_update_received": {
                    "lvl": "INFO",
                    "msg": "BGP update from {peer} action={action} prefixes={prefix_count}",
                    "vars": {
                        "peer": {"k": "ch", "v": ["rtr-hkg-1"]},
                        "action": {"k": "ch", "v": ["announce", "withdraw"]},
                        "prefix_count": {"k": "i", "v": [400, 900]},
                    },
                },
                "bgp_propagation_notice": {
                    "lvl": "INFO",
                    "msg": "propagating BGP change action={action} prefixes={prefix_count} scope={scope}",
                    "vars": {
                        "action": {"k": "ch", "v": ["announce", "withdraw"]},
                        "prefix_count": {"k": "i", "v": [400, 900]},
                        "scope": {"k": "ch", "v": ["regional", "global"]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "direct_peer",
            "svc": "peering",
            "hosts": ["ix-1"],
            "logs": {
                "peer_stats": {
                    "lvl": "INFO",
                    "msg": "peer_stats site=global peer_rpm={peer_rpm} established_peers={established_peers}",
                    "vars": {
                        "peer_rpm": {"k": "i", "v": [240, 360]},
                        "established_peers": {"k": "i", "v": [18, 30]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "peer_stats", "per_min": 0.5, "scope": "global"}]},
                "f": {"emit": [{"id": "peer_stats", "per_min": 0.5, "scope": "global"}]},
            },
        },
        {
            "id": "edge_pop_other",
            "svc": "edge-proxy",
            "hosts": ["edge-sin-1", "edge-sin-2", "edge-tyo-1"],
            "logs": {
                "access_transit": {
                    "lvl": "INFO",
                    "msg": "served route=transit colo={colo} {method} {uri} status={status} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "colo": {"k": "ch", "v": ["SIN", "TYO"]},
                        "method": {"k": "ch", "v": ["GET", "HEAD"]},
                        "uri": {"k": "ch", "v": ["/static/app.js", "/", "/api/v1/ping"]},
                        "status": {"k": "i", "v": [200, 200]},
                        "dur_ms": {"k": "i", "v": [10, 180]},
                        "bytes": {"k": "i", "v": [200, 50000]},
                    },
                },
                "access_peer": {
                    "lvl": "INFO",
                    "msg": "served route=peer colo={colo} {method} {uri} status={status} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "colo": {"k": "ch", "v": ["SIN", "TYO"]},
                        "method": {"k": "ch", "v": ["GET", "HEAD"]},
                        "uri": {"k": "ch", "v": ["/static/app.js", "/", "/api/v1/ping"]},
                        "status": {"k": "i", "v": [200, 200]},
                        "dur_ms": {"k": "i", "v": [10, 160]},
                        "bytes": {"k": "i", "v": [200, 50000]},
                    },
                },
                "edge_stats": {
                    "lvl": "INFO",
                    "msg": "edge_stats colo={colo} req_per_min={req_per_min} active_conns={active_conns}",
                    "vars": {
                        "colo": {"k": "ch", "v": ["SIN", "TYO"]},
                        "req_per_min": {"k": "i", "v": [60, 520]},
                        "active_conns": {"k": "i", "v": [200, 1800]},
                    },
                },
                "cache_eviction_pressure": {
                    "lvl": "WARN",
                    "msg": "cache eviction pressure: evicted_keys={evicted_keys} in {window_s}s",
                    "vars": {
                        "evicted_keys": {"k": "i", "v": [100, 900]},
                        "window_s": {"k": "i", "v": [30, 60]},
                    },
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "edge_stats", "per_min": 1.0, "scope": "per_host"},
                        {"id": "cache_eviction_pressure", "per_min": 0.2, "scope": "per_host"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "edge_stats", "per_min": 1.0, "scope": "per_host"},
                        {"id": "cache_eviction_pressure", "per_min": 0.2, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "route_monitor",
            "svc": "monitor",
            "hosts": ["mon-1", "mon-2"],
            "logs": {
                "rib_summary": {
                    "lvl": "INFO",
                    "msg": "rib_summary best_origin={best_origin} observed_prefixes={observed_prefixes}",
                    "vars": {
                        "best_origin": {"k": "ch", "v": ["MIXED", "HKG", "OTHER"]},
                        "observed_prefixes": {"k": "i", "v": [400, 900]},
                    },
                },
                "route_leak_alert": {
                    "lvl": "ERROR",
                    "msg": "route_leak_alert origin={origin} prefixes={prefixes} severity={severity}",
                    "vars": {
                        "origin": {"k": "ch", "v": ["HKG"]},
                        "prefixes": {"k": "i", "v": [400, 900]},
                        "severity": {"k": "ch", "v": ["high"]},
                    },
                },
                "edge_traffic_drop_alert": {
                    "lvl": "WARN",
                    "msg": "edge_traffic_drop_alert observed_drop_pct={drop_pct} window_min={window_min}",
                    "vars": {"drop_pct": {"k": "i", "v": [50, 90]}, "window_min": {"k": "i", "v": [1, 5]}},
                },
                "probe_ok": {
                    "lvl": "INFO",
                    "msg": "probe_ok target={target} status=200 rtt_ms={rtt_ms}",
                    "vars": {"target": {"k": "ch", "v": ["anycast_http"]}, "rtt_ms": {"k": "i", "v": [25, 180]}},
                },
                "probe_timeout": {
                    "lvl": "WARN",
                    "msg": "probe_timeout target={target} code={code} timeout_ms={timeout_ms}",
                    "vars": {
                        "target": {"k": "ch", "v": ["anycast_http"]},
                        "code": {"k": "ch", "v": ["522", "504"]},
                        "timeout_ms": {"k": "i", "v": [800, 2000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "rib_summary", "per_min": 1.0, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "rib_summary", "per_min": 1.0, "scope": "global"},
                        {"id": "route_leak_alert", "per_min": 0.2, "scope": "global"},
                        {"id": "edge_traffic_drop_alert", "per_min": 0.1, "scope": "global"},
                    ]
                },
            },
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "user_http_transit_n",
                    "rpm": 900.0,
                    "emit": ["edge_pop_other.access_transit"],
                    "latency_ms": [[25, 90]],
                    "trace": False,
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                },
                {
                    "id": "user_http_peer_n",
                    "rpm": 300.0,
                    "emit": ["edge_pop_other.access_peer"],
                    "latency_ms": [[20, 70]],
                    "trace": False,
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                },
                {
                    "id": "synthetic_probe_n",
                    "rpm": 60.0,
                    "emit": ["route_monitor.probe_ok"],
                    "latency_ms": [[40, 120]],
                    "trace": False,
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "user_http_transit_f",
                    "rpm": 900.0,
                    "emit": ["edge_pop_other.access_transit"],
                    "latency_ms": [[30, 140]],
                    "trace": False,
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                },
                {
                    "id": "user_http_peer_f",
                    "rpm": 300.0,
                    "emit": ["edge_pop_other.access_peer"],
                    "latency_ms": [[20, 80]],
                    "trace": False,
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                },
                {
                    "id": "synthetic_probe_ok_f",
                    "rpm": 30.0,
                    "emit": ["route_monitor.probe_ok"],
                    "latency_ms": [[50, 180]],
                    "trace": False,
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                },
                {
                    "id": "synthetic_probe_timeout_f",
                    "rpm": 30.0,
                    "emit": ["route_monitor.probe_timeout"],
                    "latency_ms": [[900, 2000]],
                    "trace": False,
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "cf_route_leak_to_offline_hkg",
    "time": {"total_minutes": 31, "phases": {"n": {"start_min": 0, "end_min": 16}, "f": {"start_min": 16, "end_min": 31}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 16,
                    "rate_multipliers": {
                        "user_http_transit_f": 0.2,
                        "synthetic_probe_ok_f": 0.8,
                        "synthetic_probe_timeout_f": 1.2,
                        "route_monitor.route_leak_alert": 5.0,
                        "route_monitor.edge_traffic_drop_alert": 2.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "netcfg_tool.change_applied", "count": 1, "hosts": ["netcfg-1"]},
                        {"ref": "hkg_router.bgp_announce", "count": 1, "hosts": ["rtr-hkg-1"]},
                        {"ref": "upstream_transit.bgp_update_received", "count": 1, "hosts": ["transit-1"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 20,
                    "rate_multipliers": {
                        "user_http_transit_f": 0.02,
                        "synthetic_probe_ok_f": 0.2,
                        "synthetic_probe_timeout_f": 1.8,
                        "route_monitor.route_leak_alert": 8.0,
                        "route_monitor.edge_traffic_drop_alert": 6.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "upstream_transit.bgp_propagation_notice", "count": 1, "hosts": ["transit-1"]}],
                },
                {
                    "order": 3,
                    "at_min": 27,
                    "rate_multipliers": {
                        "user_http_transit_f": 0.4,
                        "synthetic_probe_ok_f": 1.8,
                        "synthetic_probe_timeout_f": 0.2,
                        "route_monitor.route_leak_alert": 2.0,
                        "route_monitor.edge_traffic_drop_alert": 3.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "netcfg_tool.change_reverted", "count": 1, "hosts": ["netcfg-1"]},
                        {"ref": "hkg_router.bgp_withdraw", "count": 1, "hosts": ["rtr-hkg-1"]},
                        {"ref": "upstream_transit.bgp_update_received", "count": 1, "hosts": ["transit-1"]},
                    ],
                },
                {
                    "order": 4,
                    "at_min": 30,
                    "rate_multipliers": {
                        "user_http_transit_f": 1.0,
                        "synthetic_probe_ok_f": 2.0,
                        "synthetic_probe_timeout_f": 0.0,
                        "route_monitor.route_leak_alert": 0.0,
                        "route_monitor.edge_traffic_drop_alert": 0.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [],
                },
            ]
        }
    },
}

# ----------------------------
# Deterministic helpers
# ----------------------------

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def hash_u01(s: str) -> float:
    h = hashlib.md5(s.encode("utf-8")).digest()
    x = int.from_bytes(h[:8], "big")
    return (x % (10**12)) / float(10**12)

def clamp(x: int, lo: int, hi: int) -> int:
    return lo if x < lo else hi if x > hi else x

def iso8601_ms(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

def choose_domain(dom: Dict[str, Any], seed: str) -> Any:
    k = dom["k"]
    v = dom["v"]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        r = int(hash_u01(seed) * (hi - lo + 1))
        return lo + (r % (hi - lo + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return lo + (hi - lo) * hash_u01(seed)
    if k == "ch":
        arr = list(v)
        if not arr:
            return ""
        idx = int(hash_u01(seed) * len(arr)) % len(arr)
        return arr[idx]
    if k == "hex":
        ln = int(v)
        return md5_hex(seed)[:ln]
    if k == "uuid":
        h = md5_hex(seed)
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
    return ""

def sample_latency_ms(p50: float, p95: float, seed: str, mult: float = 1.0) -> int:
    u = hash_u01(seed)
    u_skew = u * u  # skew toward p50
    ms = (p50 + (p95 - p50) * u_skew) * mult
    if ms < 1:
        ms = 1
    return int(round(ms))

def times_within_minute(min_start: datetime, n: int, seed_prefix: str, base_offset_s: float, span_s: float) -> List[datetime]:
    if n <= 0:
        return []
    amp = min(0.2, span_s / (n * 3.0)) if n > 1 else 0.2
    out = []
    for i in range(n):
        frac = (i + 0.5) / n
        t = base_offset_s + frac * span_s
        j = (hash_u01(f"{seed_prefix}|j|{i}") * 2.0 - 1.0) * amp
        t2 = t + j
        if t2 < base_offset_s:
            t2 = base_offset_s
        if t2 >= base_offset_s + span_s:
            t2 = (base_offset_s + span_s) - 1e-6
        out.append(min_start + timedelta(seconds=t2))
    return out

# ----------------------------
# Indices
# ----------------------------

COMP_BY_ID: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

LOG_BY_REF: Dict[str, Dict[str, Any]] = {}
for comp in SYSTEM["components"]:
    cid = comp["id"]
    for lid, tmpl in comp["logs"].items():
        LOG_BY_REF[f"{cid}.{lid}"] = {"component_id": cid, "log_id": lid, **tmpl}

FLOWS_BY_STATE: Dict[str, Dict[str, Dict[str, Any]]] = {"n": {}, "f": {}}
for st in ["n", "f"]:
    for f in SYSTEM["flows"][st]["req"]:
        FLOWS_BY_STATE[st][f["id"]] = f

# ----------------------------
# Failure control timeline
# ----------------------------

FAIL_START = SCENARIO["time"]["phases"]["f"]["start_min"]
FAIL_END = SCENARIO["time"]["phases"]["f"]["end_min"]

failure_events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

ACTIVE_RATE_MULT: Dict[int, Dict[str, float]] = {}
current: Dict[str, float] = {}
ev_i = 0
for m in range(FAIL_START, FAIL_END):
    while ev_i < len(failure_events) and failure_events[ev_i]["at_min"] <= m:
        for k, v in failure_events[ev_i].get("rate_multipliers", {}).items():
            current[k] = float(v)
        ev_i += 1
    ACTIVE_RATE_MULT[m] = dict(current)

def rate_mult_for(minute: int, source_key: str) -> float:
    if minute < FAIL_START or minute >= FAIL_END:
        return 1.0
    return float(ACTIVE_RATE_MULT.get(minute, {}).get(source_key, 1.0))

def failure_stage_best_origin(minute: int) -> str:
    if minute < FAIL_START:
        return "MIXED"
    if FAIL_START <= minute < 27:
        return "HKG"
    if 27 <= minute < 30:
        return "MIXED"
    return "OTHER"

# ----------------------------
# Stable count allocator with carry
# ----------------------------

CARRY: Dict[str, float] = {}

def alloc_count(expected: float, key: str) -> int:
    if expected <= 0.0:
        CARRY[key] = 0.0
        return 0
    c = CARRY.get(key, 0.0)
    x = expected + c
    n = int(math.floor(x + 1e-9))
    CARRY[key] = x - n
    return n

# ----------------------------
# Emission primitives
# ----------------------------

def emit_log(rows: List[Dict[str, Any]], ts: datetime, ref: str, seed: str, overrides: Dict[str, Any] = None, host_override: str = None):
    tmpl = LOG_BY_REF[ref]
    comp = COMP_BY_ID[tmpl["component_id"]]
    svc = comp.get("svc") or ""
    hosts = comp.get("hosts") or []
    if host_override is not None:
        host = host_override
    else:
        if not hosts:
            host = ""
        else:
            host = hosts[int(hash_u01(f"{seed}|host") * len(hosts)) % len(hosts)]
    vars_dom = tmpl.get("vars", {}) or {}
    vals: Dict[str, Any] = {}
    overrides = overrides or {}
    for k, dom in vars_dom.items():
        if k in overrides:
            vals[k] = overrides[k]
        else:
            vals[k] = choose_domain(dom, f"{seed}|var|{k}")
    msg = tmpl["msg"].format(**vals)
    rows.append(
        {
            "timestamp": ts,
            "level": tmpl["lvl"],
            "message": msg,
            "trace_id": "",
            "service": svc,
            "host": host,
        }
    )

def colo_from_edge_host(h: str) -> str:
    if "sin" in h:
        return "SIN"
    if "tyo" in h:
        return "TYO"
    return "SIN"

# ----------------------------
# Simulation
# ----------------------------

rows: List[Dict[str, Any]] = []

def served_edge_rpm_for_minute(minute: int) -> float:
    if minute < SCENARIO["time"]["phases"]["n"]["end_min"]:
        return FLOWS_BY_STATE["n"]["user_http_transit_n"]["rpm"] + FLOWS_BY_STATE["n"]["user_http_peer_n"]["rpm"]
    transit = FLOWS_BY_STATE["f"]["user_http_transit_f"]["rpm"] * rate_mult_for(minute, "user_http_transit_f")
    peer = FLOWS_BY_STATE["f"]["user_http_peer_f"]["rpm"]
    return transit + peer

# Background emissions per minute
for minute in range(SCENARIO["time"]["total_minutes"]):
    state = "n" if minute < SCENARIO["time"]["phases"]["n"]["end_min"] else "f"
    min_start = BASE_TIME + timedelta(minutes=minute)

    for comp in SYSTEM["components"]:
        cid = comp["id"]
        beh = comp.get("beh", {}).get(state, {}).get("emit", []) or []
        for src in beh:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host") or "per_host"
            ref = f"{cid}.{log_id}"

            eff = per_min
            if state == "f":
                eff *= rate_mult_for(minute, ref)

            if scope == "global":
                key = f"bg|{state}|{ref}|global"
                n = alloc_count(eff, key)
                tss = times_within_minute(min_start, n, f"{key}|{minute}", base_offset_s=2.0, span_s=56.0)
                for j, ts in enumerate(tss):
                    overrides: Dict[str, Any] = {}
                    if ref == "direct_peer.peer_stats":
                        wobble = int((hash_u01(f"{minute}|peer_wobble") * 21) - 10)
                        overrides["peer_rpm"] = clamp(300 + wobble, 240, 360)
                        overrides["established_peers"] = clamp(24 + int((hash_u01(f"{minute}|peers") * 5) - 2), 18, 30)
                    if ref == "route_monitor.rib_summary":
                        overrides["best_origin"] = failure_stage_best_origin(minute) if state == "f" else "MIXED"
                        overrides["observed_prefixes"] = clamp(780 + int((hash_u01(f"{minute}|pref") * 61) - 30), 400, 900)
                    if ref == "route_monitor.edge_traffic_drop_alert":
                        mult = rate_mult_for(minute, "user_http_transit_f")
                        drop = int(round((1.0 - mult) * 100.0))
                        overrides["drop_pct"] = clamp(drop, 50, 90)
                        overrides["window_min"] = 1 + (int(hash_u01(f"{minute}|dropwin") * 5) % 5)
                    if ref == "route_monitor.route_leak_alert":
                        overrides["origin"] = "HKG"
                        overrides["severity"] = "high"
                        overrides["prefixes"] = clamp(840 + int((hash_u01(f"{minute}|leakpref") * 81) - 40), 400, 900)
                    emit_log(rows, ts, ref, seed=f"{key}|{minute}|{j}", overrides=overrides)
            else:
                hosts = comp.get("hosts") or []
                for h in hosts:
                    key = f"bg|{state}|{ref}|{h}"
                    n = alloc_count(eff, key)
                    tss = times_within_minute(min_start, n, f"{key}|{minute}", base_offset_s=3.0, span_s=54.0)
                    for j, ts in enumerate(tss):
                        overrides = {}
                        if ref == "edge_pop_other.edge_stats":
                            total = served_edge_rpm_for_minute(minute)
                            per_host_req = int(round(total / max(1, len(hosts))))
                            overrides["req_per_min"] = clamp(per_host_req, 60, 520)
                            overrides["active_conns"] = clamp(int(round(overrides["req_per_min"] * 3.5 + 200)), 200, 1800)
                            overrides["colo"] = colo_from_edge_host(h)
                        emit_log(rows, ts, ref, seed=f"{key}|{minute}|{j}", overrides=overrides, host_override=h)

# Flow emissions per minute
HOST_RR: Dict[str, int] = {}

def rr_host(comp_id: str) -> str:
    comp = COMP_BY_ID[comp_id]
    hosts = comp.get("hosts") or []
    if not hosts:
        return ""
    k = f"rr|{comp_id}"
    i = HOST_RR.get(k, 0)
    HOST_RR[k] = i + 1
    return hosts[i % len(hosts)]

for minute in range(SCENARIO["time"]["total_minutes"]):
    state = "n" if minute < SCENARIO["time"]["phases"]["n"]["end_min"] else "f"
    min_start = BASE_TIME + timedelta(minutes=minute)

    flow_list = SYSTEM["flows"][state]["req"]
    for flow in flow_list:
        flow_id = flow["id"]
        base_rpm = float(flow["rpm"])
        eff_rpm = base_rpm
        if state == "f":
            eff_rpm *= rate_mult_for(minute, flow_id)

        key = f"flow|{state}|{flow_id}"
        nreq = alloc_count(eff_rpm, key)
        start_times = times_within_minute(min_start, nreq, f"{key}|{minute}", base_offset_s=1.0, span_s=58.0)

        emit_ref = flow["emit"][0]
        tmpl = LOG_BY_REF[emit_ref]
        emit_comp_id = tmpl["component_id"]

        p50, p95 = flow["latency_ms"][0]
        for i, st_dt in enumerate(start_times):
            seed = f"{flow_id}|{minute}|{i}"
            dur_ms = sample_latency_ms(p50, p95, seed=f"{seed}|lat", mult=1.0)
            emit_ts = st_dt + timedelta(milliseconds=dur_ms)

            overrides: Dict[str, Any] = {}
            h = rr_host(emit_comp_id)

            if emit_ref in ("edge_pop_other.access_transit", "edge_pop_other.access_peer"):
                overrides["dur_ms"] = clamp(dur_ms, 10, 180 if emit_ref.endswith("transit") else 160)
                overrides["bytes"] = clamp(200 + int(hash_u01(f"{seed}|bytes") * (50000 - 200 + 1)), 200, 50000)
                overrides["method"] = "GET" if (hash_u01(f"{seed}|m") < 0.85) else "HEAD"
                overrides["uri"] = choose_domain(LOG_BY_REF[emit_ref]["vars"]["uri"], f"{seed}|uri")
                overrides["status"] = 200
                overrides["colo"] = colo_from_edge_host(h)
            elif emit_ref == "route_monitor.probe_ok":
                overrides["rtt_ms"] = clamp(dur_ms, 25, 180)
                overrides["target"] = "anycast_http"
            elif emit_ref == "route_monitor.probe_timeout":
                overrides["timeout_ms"] = clamp(dur_ms, 800, 2000)
                overrides["target"] = "anycast_http"
                overrides["code"] = "522" if (hash_u01(f"{seed}|code") < 0.7) else "504"

            emit_log(rows, emit_ts, emit_ref, seed=seed, overrides=overrides, host_override=h)

# One-shots at event time (exact counts)
for ev in failure_events:
    at_min = int(ev["at_min"])
    ev_time = BASE_TIME + timedelta(minutes=at_min)

    ev_prefixes = clamp(820 + int((hash_u01(f"event|{ev['order']}|pref") * 121) - 60), 400, 900)

    for os_i, os in enumerate(ev.get("one_shots", [])):
        ref = os["ref"]
        cnt = int(os["count"])
        allowed_hosts = os.get("hosts") or []
        for k in range(cnt):
            ts = ev_time + timedelta(milliseconds=10 * (os_i * 3 + k) + int(hash_u01(f"{ref}|{at_min}|{k}|jit") * 50))
            overrides: Dict[str, Any] = {}
            if ref == "upstream_transit.bgp_update_received":
                if at_min == 16:
                    overrides["action"] = "announce"
                elif at_min == 27:
                    overrides["action"] = "withdraw"
                overrides["prefix_count"] = ev_prefixes
                overrides["peer"] = "rtr-hkg-1"
            if ref == "upstream_transit.bgp_propagation_notice":
                overrides["action"] = "announce"
                overrides["prefix_count"] = ev_prefixes
                overrides["scope"] = "global"
            if ref in ("hkg_router.bgp_announce", "hkg_router.bgp_withdraw"):
                overrides["prefix_count"] = ev_prefixes
                overrides["neighbor_as"] = 64500
                overrides["session"] = "BGP-HKG-TRANSIT1"

            host_override = allowed_hosts[0] if allowed_hosts else None
            emit_log(rows, ts, ref, seed=f"oneshot|{ref}|{at_min}|{k}", overrides=overrides, host_override=host_override)

# ----------------------------
# Build and save CSV
# ----------------------------

rows_sorted = sorted(rows, key=lambda r: (r["timestamp"], r["service"], r["host"], r["level"], r["message"]))

df = pd.DataFrame(
    {
        "timestamp": [iso8601_ms(r["timestamp"]) for r in rows_sorted],
        "level": [r["level"] for r in rows_sorted],
        "message": [r["message"] for r in rows_sorted],
        "trace_id": [r["trace_id"] for r in rows_sorted],
        "service": [r["service"] for r in rows_sorted],
        "host": [r["host"] for r in rows_sorted],
    }
)

assert list(df.columns) == ["timestamp", "level", "message", "trace_id", "service", "host"]
assert 20000 <= len(df) <= 100000
ts_series = pd.to_datetime(df["timestamp"], utc=True)
assert (ts_series.diff().dropna() >= pd.Timedelta(0)).all()

df.to_csv("logs.csv", index=False)
