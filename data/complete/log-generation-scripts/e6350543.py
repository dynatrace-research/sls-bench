from __future__ import annotations

import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "maps_tile_delivery_kartotherian"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["edge_ats"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "edge_ats",
            "svc": "ats",
            "hosts": ["ats-edge1", "ats-edge2"],
            "logs": {
                "webrequest_ok": {
                    "lvl": "INFO",
                    "msg": "webrequest host={host} method=GET uri={uri} status=200 backend={backend} bytes={bytes} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "host": {"k": "ch", "v": ["maps.wikimedia.org"]},
                        "uri": {"k": "ch", "v": ["/osm-intl/<z>/<x>/<y>.png", "/osm-intl/<z>/<x>/<y>.pbf", "/styles/<style>/<z>/<x>/<y>.png"]},
                        "backend": {"k": "ch", "v": ["kart-bm1", "kart-bm2"]},
                        "bytes": {"k": "i", "v": [800, 90000]},
                        "dur_ms": {"k": "i", "v": [8, 220]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "origin_tls_handshake_fail": {
                    "lvl": "WARN",
                    "msg": 'origin tls handshake failed backend={backend} sni={sni} err="x509: certificate is valid for {peer_san_sample}, not {sni}" handshake_ms={handshake_ms} trace_id={trace_id}',
                    "vars": {
                        "backend": {"k": "ch", "v": ["kart-k8s1", "kart-k8s2", "kart-k8s3"]},
                        "sni": {"k": "ch", "v": ["maps.wikimedia.org"]},
                        "peer_san_sample": {"k": "ch", "v": ["kartotherian.svc.cluster.local", "kartotherian.discovery.local", "*.wikikube.internal"]},
                        "handshake_ms": {"k": "i", "v": [1, 35]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "webrequest_502": {
                    "lvl": "ERROR",
                    "msg": "webrequest host={host} method=GET uri={uri} status=502 backend={backend} origin_error=tls_handshake_failed dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "host": {"k": "ch", "v": ["maps.wikimedia.org"]},
                        "uri": {"k": "ch", "v": ["/osm-intl/<z>/<x>/<y>.png", "/osm-intl/<z>/<x>/<y>.pbf", "/styles/<style>/<z>/<x>/<y>.png"]},
                        "backend": {"k": "ch", "v": ["kart-k8s1", "kart-k8s2", "kart-k8s3"]},
                        "dur_ms": {"k": "i", "v": [3, 140]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "conn_summary_ok": {
                    "lvl": "INFO",
                    "msg": "ats origin summary origin_rpm={origin_rpm} origin_tls_fail_rpm={origin_tls_fail_rpm}",
                    "vars": {"origin_rpm": {"k": "i", "v": [500, 1100]}, "origin_tls_fail_rpm": {"k": "i", "v": [0, 2]}},
                },
                "conn_summary_elevated": {
                    "lvl": "INFO",
                    "msg": "ats origin summary origin_rpm={origin_rpm} origin_tls_fail_rpm={origin_tls_fail_rpm}",
                    "vars": {"origin_rpm": {"k": "i", "v": [500, 1100]}, "origin_tls_fail_rpm": {"k": "i", "v": [5, 80]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "conn_summary_ok", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "conn_summary_ok", "per_min": 0.5, "scope": "per_host"}, {"id": "conn_summary_elevated", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        {
            "id": "lvs_kart",
            "svc": "lvs-kartotherian",
            "hosts": ["lvs-a", "lvs-b"],
            "logs": {
                "pool_k8s_backend": {"lvl": "INFO", "msg": "confctl pool service=kartotherian backend={backend} reason=migration", "vars": {"backend": {"k": "ch", "v": ["kart-k8s1", "kart-k8s2", "kart-k8s3"]}}},
                "depool_k8s_backend": {"lvl": "INFO", "msg": "confctl depool service=kartotherian backend={backend} reason=rollback_tls_errors", "vars": {"backend": {"k": "ch", "v": ["kart-k8s1", "kart-k8s2", "kart-k8s3"]}}},
                "pool_status": {
                    "lvl": "INFO",
                    "msg": "lvs pool service=kartotherian baremetal_up={baremetal_up} k8s_up={k8s_up} conns={conns}",
                    "vars": {"baremetal_up": {"k": "i", "v": [2, 2]}, "conns": {"k": "i", "v": [800, 9000]}},
                    "state_vars": {"n": {"k8s_up": {"k": "i", "v": [0, 0]}}, "f": {"k8s_up": {"k": "i", "v": [0, 3]}}},
                },
            },
            "beh": {"n": {"emit": [{"id": "pool_status", "per_min": 1.0, "scope": "per_host"}]}, "f": {"emit": [{"id": "pool_status", "per_min": 1.0, "scope": "per_host"}]}},
        },
        {
            "id": "kart_baremetal",
            "svc": "kartotherian",
            "hosts": ["kart-bm1", "kart-bm2"],
            "logs": {
                "metrics": {
                    "lvl": "INFO",
                    "msg": "kartotherian metrics host={host} req_rps={req_rps} p95_ms={p95_ms} err5xx_rps={err5xx_rps}",
                    "vars": {"host": {"k": "ch", "v": ["kart-bm1", "kart-bm2"]}, "req_rps": {"k": "f", "v": [1.0, 30.0]}, "p95_ms": {"k": "i", "v": [20, 260]}, "err5xx_rps": {"k": "f", "v": [0.0, 1.0]}},
                }
            },
            "beh": {"n": {"emit": [{"id": "metrics", "per_min": 0.5, "scope": "per_host"}]}, "f": {"emit": [{"id": "metrics", "per_min": 0.5, "scope": "per_host"}]}},
        },
        {
            "id": "kart_k8s",
            "svc": "kartotherian",
            "hosts": ["kart-k8s1", "kart-k8s2", "kart-k8s3"],
            "logs": {"pod_ready": {"lvl": "INFO", "msg": "k8s pod ready pod={pod} node={node}", "vars": {"pod": {"k": "str", "v": "kartotherian-<hash>"}, "node": {"k": "str", "v": "worker-<id>"}}}},
            "beh": {"n": {"emit": [{"id": "pod_ready", "per_min": 0.05, "scope": "per_host"}]}, "f": {"emit": [{"id": "pod_ready", "per_min": 0.05, "scope": "per_host"}]}},
        },
        {
            "id": "logstash",
            "svc": "logstash",
            "hosts": ["logstash1"],
            "logs": {
                "webrequest_5xx_rate_ok": {"lvl": "INFO", "msg": "webrequest_5xx_dashboard service=maps rate_5xx_rpm={rate_5xx_rpm} window_s=300", "vars": {"rate_5xx_rpm": {"k": "i", "v": [0, 2]}}},
                "webrequest_5xx_rate_elevated": {"lvl": "INFO", "msg": "webrequest_5xx_dashboard service=maps rate_5xx_rpm={rate_5xx_rpm} window_s=300", "vars": {"rate_5xx_rpm": {"k": "i", "v": [5, 80]}}},
            },
            "beh": {"n": {"emit": [{"id": "webrequest_5xx_rate_ok", "per_min": 1.0, "scope": "global"}]}, "f": {"emit": [{"id": "webrequest_5xx_rate_ok", "per_min": 1.0, "scope": "global"}, {"id": "webrequest_5xx_rate_elevated", "per_min": 1.0, "scope": "global"}]}},
        },
        {
            "id": "ops_tooling",
            "svc": None,
            "hosts": ["sre-bastion1"],
            "logs": {
                "sal_pool_k8s_backends": {"lvl": "INFO", "msg": 'SAL user={user} action=pool_k8s_backends target=kartotherian_lvs note="{note}"', "vars": {"user": {"k": "ch", "v": ["elukey"]}, "note": {"k": "str", "v": "freeform"}}},
                "sal_check_5xx_dashboard": {"lvl": "INFO", "msg": 'SAL user={user} action=check_5xx_dashboard target=maps_ats note="{note}"', "vars": {"user": {"k": "ch", "v": ["elukey", "yiannis"]}, "note": {"k": "str", "v": "freeform"}}},
                "sal_depool_k8s_backends": {"lvl": "INFO", "msg": 'SAL user={user} action=depool_k8s_backends target=kartotherian_lvs note="{note}"', "vars": {"user": {"k": "ch", "v": ["elukey"]}, "note": {"k": "str", "v": "freeform"}}},
                "sal_verify_tls_san_issue": {"lvl": "INFO", "msg": 'SAL user={user} action=verify_tls_san_issue target=maps_ats note="{note}"', "vars": {"user": {"k": "ch", "v": ["elukey"]}, "note": {"k": "str", "v": "freeform"}}},
                "openssl_probe": {
                    "lvl": "INFO",
                    "msg": "probe openssl_s_client addr={addr} sni={sni} verify_result={verify_result} peer_cn={peer_cn} peer_sans={peer_sans}",
                    "vars": {"addr": {"k": "str", "v": "kart-k8s<id>:443"}, "sni": {"k": "ch", "v": ["maps.wikimedia.org"]}, "verify_result": {"k": "ch", "v": ["hostname_mismatch"]}, "peer_cn": {"k": "ch", "v": ["kartotherian.wikikube.internal", "*.wikikube.internal"]}, "peer_sans": {"k": "ch", "v": ["kartotherian.svc.cluster.local,kartotherian.discovery.local", "*.wikikube.internal"]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {"req": [{"id": "tile_request_ok", "rpm": 800.0, "emit": ["edge_ats.webrequest_ok"], "latency_ms": [[25, 90]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True}]},
        "f": {
            "req": [
                {"id": "tile_request_ok", "rpm": 788.0, "emit": ["edge_ats.webrequest_ok"], "latency_ms": [[25, 95]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
                {"id": "tile_request_tls_502", "rpm": 12.0, "emit": ["edge_ats.origin_tls_handshake_fail", "edge_ats.webrequest_502"], "latency_ms": [[6, 22], [18, 85]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "incident_2025_02_19_maps_tls_san",
        "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 50}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 20,
                        "rate_multipliers": {"edge_ats.conn_summary_ok": 0.0, "edge_ats.conn_summary_elevated": 1.0, "logstash.webrequest_5xx_rate_ok": 0.0, "logstash.webrequest_5xx_rate_elevated": 1.0},
                        "latency_multipliers": {},
                        "one_shots": [{"ref": "lvs_kart.pool_k8s_backend", "count": 1, "hosts": ["lvs-a"]}, {"ref": "ops_tooling.sal_pool_k8s_backends", "count": 1, "hosts": ["sre-bastion1"]}],
                    },
                    {
                        "order": 2,
                        "at_min": 29,
                        "rate_multipliers": {"tile_request_tls_502": 0.0, "tile_request_ok": 1.02, "edge_ats.conn_summary_ok": 1.0, "edge_ats.conn_summary_elevated": 0.0, "logstash.webrequest_5xx_rate_ok": 1.0, "logstash.webrequest_5xx_rate_elevated": 0.0},
                        "latency_multipliers": {},
                        "one_shots": [{"ref": "ops_tooling.sal_check_5xx_dashboard", "count": 1, "hosts": ["sre-bastion1"]}, {"ref": "lvs_kart.depool_k8s_backend", "count": 1, "hosts": ["lvs-a"]}, {"ref": "ops_tooling.sal_depool_k8s_backends", "count": 1, "hosts": ["sre-bastion1"]}],
                    },
                    {"order": 3, "at_min": 30, "rate_multipliers": {}, "latency_multipliers": {}, "one_shots": [{"ref": "ops_tooling.openssl_probe", "count": 1, "hosts": ["sre-bastion1"]}, {"ref": "ops_tooling.sal_verify_tls_san_issue", "count": 1, "hosts": ["sre-bastion1"]}]},
                ]
            }
        },
    }
}

SEED = "maps_tls_san_sim_v3_seed_2026_04_03"
NORMAL_DIST = NormalDist()


@dataclass(frozen=True)
class Interval:
    state: str  # 'n' or 'f'
    start_min: int
    end_min: int
    start_dt: datetime
    end_dt: datetime
    rate_mult: Dict[str, float]  # keys: flow id or "comp.log"
    lat_mult: Dict[str, float]  # keys: flow id (or potentially "comp.log")
    k8s_pooled: bool  # derived from events for plausibility only


def _md5_bytes(s: str) -> bytes:
    return hashlib.md5((SEED + "|" + s).encode("utf-8")).digest()


def u01(key: str) -> float:
    b = _md5_bytes(key)
    x = int.from_bytes(b[:8], "big", signed=False)
    return (x & ((1 << 64) - 1)) / float(1 << 64)


def hex32(key: str) -> str:
    return hashlib.md5((SEED + "|" + key).encode("utf-8")).hexdigest()


def jitter_ms(key: str, amp_ms: int) -> int:
    return int(round((u01("jitter|" + key) * 2.0 - 1.0) * amp_ms))


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def stable_int_count(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    return base + (1 if u01("round|" + key) < frac else 0)


def normal_ppf(u: float) -> float:
    u = float(clamp(u, 1e-12, 1.0 - 1e-12))
    return float(NORMAL_DIST.inv_cdf(u))


def sample_lognormal_ms(p50: float, p95: float, key: str, soft_cap: Optional[float] = None) -> float:
    p50 = max(0.1, float(p50))
    p95 = max(p50 * 1.0001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.645
    z = normal_ppf(u01("ln|" + key))
    x = math.exp(mu + sigma * z)
    cap = soft_cap if soft_cap is not None else (3.0 * p95)
    if x > cap:
        x = cap + (x - cap) * 0.15
    return x


def fmt_ts(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    s = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return s[:-3] + "Z"


def expand_str_template(s: str, key: str) -> str:
    out = s
    if "<hash>" in out:
        out = out.replace("<hash>", hex32("hash|" + key)[:8])
    if "<id>" in out:
        out = out.replace("<id>", str(1 + int(u01("id|" + key) * 99)))
    if "<style>" in out:
        out = out.replace("<style>", "osm-intl")
    if any(t in out for t in ["<z>", "<x>", "<y>"]):
        z = int(u01("z|" + key) * 19)
        max_xy = max(1, (1 << z))
        x = int(u01("x|" + key) * max_xy)
        y = int(u01("y|" + key) * max_xy)
        out = out.replace("<z>", str(z)).replace("<x>", str(x)).replace("<y>", str(y))
    return out


def gen_value_from_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom["k"]
    v = dom["v"]
    if k == "ch":
        lst = list(v)
        if not lst:
            return ""
        idx = int(u01("ch|" + key) * len(lst))
        if idx == len(lst):
            idx -= 1
        return lst[idx]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi <= lo:
            return lo
        x = lo + int(u01("i|" + key) * (hi - lo + 1))
        if x > hi:
            x = hi
        return x
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        if hi <= lo:
            return lo
        x = lo + u01("f|" + key) * (hi - lo)
        return float(f"{x:.2f}")
    if k == "hex":
        ln = int(v)
        h = hashlib.md5((SEED + "|hex|" + key).encode("utf-8")).hexdigest()
        while len(h) < ln:
            h += hashlib.md5((SEED + "|hex2|" + h).encode("utf-8")).hexdigest()
        return h[:ln]
    if k == "str":
        return expand_str_template(str(v), key)
    return str(v)


def merge_domains(tpl: Dict[str, Any], state: str) -> Dict[str, Dict[str, Any]]:
    doms: Dict[str, Dict[str, Any]] = dict(tpl.get("vars", {}))
    sv = tpl.get("state_vars", {})
    if isinstance(sv, dict) and state in sv:
        doms = dict(doms)
        doms.update(sv[state])
    return doms


def render_log_message(comp_id: str, log_id: str, state: str, key: str, overrides: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
    comp = COMPONENTS[comp_id]
    tpl = comp["logs"][log_id]
    doms = merge_domains(tpl, state)
    vals: Dict[str, Any] = {}
    for var, dom in doms.items():
        if overrides and var in overrides:
            vals[var] = overrides[var]
        else:
            vals[var] = gen_value_from_domain(dom, f"{comp_id}.{log_id}.{var}|{key}")
            if isinstance(vals[var], str) and ("<" in vals[var] and ">" in vals[var]):
                vals[var] = expand_str_template(vals[var], f"{comp_id}.{log_id}.{var}|{key}")

    if overrides:
        for k2, v2 in overrides.items():
            vals[k2] = v2
            if isinstance(v2, str) and ("<" in v2 and ">" in v2):
                vals[k2] = expand_str_template(v2, f"{comp_id}.{log_id}.{k2}|{key}")

    msg = tpl["msg"].format(**vals)
    lvl = tpl["lvl"]
    return lvl, msg


def schedule_times_uniform(start_dt: datetime, end_dt: datetime, count: int, key: str, jitter_amp_ms: int) -> List[datetime]:
    if count <= 0:
        return []
    dur_s = (end_dt - start_dt).total_seconds()
    if dur_s <= 0:
        return [start_dt for _ in range(count)]
    times: List[datetime] = []
    for i in range(count):
        frac = (i + 0.5) / count
        t = start_dt + timedelta(seconds=dur_s * frac)
        j = jitter_ms(f"{key}|{i}", jitter_amp_ms)
        t2 = t + timedelta(milliseconds=j)
        if t2 < start_dt:
            t2 = start_dt + timedelta(milliseconds=1)
        if t2 >= end_dt:
            t2 = end_dt - timedelta(milliseconds=1)
        times.append(t2)
    return times


def add_row(rows: List[Dict[str, Any]], ts: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append({"timestamp": fmt_ts(ts), "level": level, "message": message, "trace_id": trace_id, "service": service, "host": host})


def build_indices() -> Dict[str, Dict[str, Any]]:
    comps = {c["id"]: c for c in SYSTEM["components"]}
    for c in comps.values():
        if c.get("svc") is None:
            c["svc"] = ""
        if "hosts" not in c or c["hosts"] is None:
            c["hosts"] = []
    return comps


def derive_failure_intervals(base_time: datetime) -> List[Interval]:
    sc = SCENARIO["scenario"]
    f_start = sc["time"]["phases"]["f"]["start_min"]
    f_end = sc["time"]["phases"]["f"]["end_min"]
    events = list(sc["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e.get("order", 0)))

    boundaries = sorted(set([f_start, f_end] + [int(e["at_min"]) for e in events if f_start <= int(e["at_min"]) <= f_end]))
    if boundaries[0] != f_start:
        boundaries = [f_start] + boundaries
    if boundaries[-1] != f_end:
        boundaries = boundaries + [f_end]

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, float] = {}
    k8s_pooled = False
    idx_ev = 0

    intervals: List[Interval] = []
    for i in range(len(boundaries) - 1):
        seg_start = boundaries[i]
        seg_end = boundaries[i + 1]

        while idx_ev < len(events) and int(events[idx_ev]["at_min"]) <= seg_start:
            ev = events[idx_ev]
            for k, v in (ev.get("rate_multipliers") or {}).items():
                active_rate[k] = float(v)
            for k, v in (ev.get("latency_multipliers") or {}).items():
                active_lat[k] = float(v)

            ones = ev.get("one_shots") or []
            for os in ones:
                ref = os.get("ref", "")
                if ref == "lvs_kart.pool_k8s_backend":
                    k8s_pooled = True
                if ref == "lvs_kart.depool_k8s_backend":
                    k8s_pooled = False

            idx_ev += 1

        intervals.append(
            Interval(
                state="f",
                start_min=seg_start,
                end_min=seg_end,
                start_dt=base_time + timedelta(minutes=seg_start),
                end_dt=base_time + timedelta(minutes=seg_end),
                rate_mult=dict(active_rate),
                lat_mult=dict(active_lat),
                k8s_pooled=k8s_pooled,
            )
        )
    return intervals


def multiplier_for_background(rate_mult: Dict[str, float], comp_id: str, log_id: str) -> float:
    return float(rate_mult.get(f"{comp_id}.{log_id}", 1.0))


def multiplier_for_flow(rate_mult: Dict[str, float], flow_id: str) -> float:
    return float(rate_mult.get(flow_id, 1.0))


def latency_multiplier_for_flow(lat_mult: Dict[str, float], flow_id: str) -> float:
    return float(lat_mult.get(flow_id, 1.0))


def simulate_background(rows: List[Dict[str, Any]], interval: Interval) -> None:
    state = interval.state
    dur_min = (interval.end_dt - interval.start_dt).total_seconds() / 60.0

    for comp_id, comp in COMPONENTS.items():
        beh = comp.get("beh", {}).get(state, {}).get("emit", [])
        for src in beh:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            mult = 1.0
            if state == "f":
                mult = multiplier_for_background(interval.rate_mult, comp_id, log_id)
            eff_per_min = per_min * mult
            if eff_per_min <= 0.0 or dur_min <= 0.0:
                continue

            hosts = list(comp.get("hosts") or [])
            if scope == "global":
                expected = eff_per_min * dur_min
                count = stable_int_count(expected, f"bg|{state}|{interval.start_min}-{interval.end_min}|{comp_id}.{log_id}")
                if count <= 0:
                    continue
                times = schedule_times_uniform(interval.start_dt, interval.end_dt, count, f"bg|{comp_id}.{log_id}|{interval.start_min}", 250)
                for i, ts in enumerate(times):
                    host = hosts[int(u01(f"bg_host|{comp_id}.{log_id}|{interval.start_min}|{i}") * len(hosts))] if hosts else ""
                    service = comp.get("svc", "")
                    overrides: Dict[str, Any] = {}

                    if comp_id == "logstash" and log_id.startswith("webrequest_5xx_rate_"):
                        tls_rpm = 0.0
                        if state == "f":
                            tls_rpm = FLOW_EFFECTIVE_RPM.get((interval.start_min, "tile_request_tls_502"), 0.0)
                        dom = comp["logs"][log_id]["vars"]["rate_5xx_rpm"]["v"]
                        lo, hi = int(dom[0]), int(dom[1])
                        base = int(round(tls_rpm))
                        base = base + int(round((u01(f"dash_jit|{interval.start_min}|{i}") * 4.0) - 2.0))
                        overrides["rate_5xx_rpm"] = int(clamp(base, lo, hi))

                    level, msg = render_log_message(comp_id, log_id, state, f"bg|{interval.start_min}|{i}", overrides=overrides)
                    add_row(rows, ts, level, msg, "", service, host)
            else:
                if not hosts:
                    continue
                expected_per_host = eff_per_min * dur_min
                for h in hosts:
                    count = stable_int_count(expected_per_host, f"bg|{state}|{interval.start_min}-{interval.end_min}|{comp_id}.{log_id}|{h}")
                    if count <= 0:
                        continue
                    times = schedule_times_uniform(interval.start_dt, interval.end_dt, count, f"bg|{comp_id}.{log_id}|{h}|{interval.start_min}", 250)
                    for i, ts in enumerate(times):
                        service = comp.get("svc", "")
                        overrides: Dict[str, Any] = {}

                        if comp_id == "kart_baremetal" and log_id == "metrics":
                            overrides["host"] = h
                            base_rps = 6.7
                            if state == "f":
                                ok_rpm = FLOW_EFFECTIVE_RPM.get((interval.start_min, "tile_request_ok"), 788.0)
                                base_rps = max(1.0, (ok_rpm / 60.0) / 2.0)
                            rps = base_rps * (0.85 + 0.3 * u01(f"rps_var|{interval.start_min}|{h}|{i}"))
                            overrides["req_rps"] = float(f"{clamp(rps, 1.0, 30.0):.2f}")
                            overrides["p95_ms"] = int(clamp(60 + 120 * u01(f"p95_var|{interval.start_min}|{h}|{i}"), 20, 260))
                            overrides["err5xx_rps"] = float(f"{clamp(0.05 * u01(f"e5|{interval.start_min}|{h}|{i}"), 0.0, 1.0):.2f}")

                        if comp_id == "kart_k8s" and log_id == "pod_ready":
                            overrides["pod"] = expand_str_template("kartotherian-<hash>", f"pod|{interval.start_min}|{h}")
                            overrides["node"] = expand_str_template("worker-<id>", f"node|{h}")

                        if comp_id == "edge_ats" and log_id.startswith("conn_summary_"):
                            ok_rpm = 800.0
                            tls_rpm = 0.0
                            if state == "f":
                                ok_rpm = FLOW_EFFECTIVE_RPM.get((interval.start_min, "tile_request_ok"), 788.0)
                                tls_rpm = FLOW_EFFECTIVE_RPM.get((interval.start_min, "tile_request_tls_502"), 12.0)
                            origin_rpm = int(round(ok_rpm + tls_rpm))
                            origin_rpm = origin_rpm + int(round((u01(f"or_jit|{interval.start_min}|{h}|{i}") * 14.0) - 7.0))
                            overrides["origin_rpm"] = int(clamp(origin_rpm, 500, 1100))
                            if log_id == "conn_summary_ok":
                                overrides["origin_tls_fail_rpm"] = int(clamp(int(round(tls_rpm * 0.1)), 0, 2))
                            else:
                                base = int(round(tls_rpm))
                                base = base + int(round((u01(f"tls_jit|{interval.start_min}|{h}|{i}") * 6.0) - 3.0))
                                overrides["origin_tls_fail_rpm"] = int(clamp(base, 5, 80))

                        if comp_id == "lvs_kart" and log_id == "pool_status":
                            if state == "n":
                                overrides["k8s_up"] = 0
                            else:
                                overrides["k8s_up"] = 3 if interval.k8s_pooled else 0
                            conns_base = 2500 if interval.k8s_pooled else 3200
                            conns = int(conns_base + 1800 * u01(f"conns|{interval.start_min}|{h}|{i}"))
                            overrides["conns"] = int(clamp(conns, 800, 9000))

                        level, msg = render_log_message(comp_id, log_id, state, f"bg|{interval.start_min}|{h}|{i}", overrides=overrides)
                        add_row(rows, ts, level, msg, "", service, h)


def simulate_flows(rows: List[Dict[str, Any]], interval: Interval) -> None:
    state = interval.state
    dur_min = (interval.end_dt - interval.start_dt).total_seconds() / 60.0
    if dur_min <= 0:
        return
    flows = SYSTEM["flows"][state]["req"]

    for flow in flows:
        flow_id = flow["id"]
        base_rpm = float(flow["rpm"])
        rpm_mult = 1.0
        lat_mult = 1.0
        if state == "f":
            rpm_mult = multiplier_for_flow(interval.rate_mult, flow_id)
            lat_mult = latency_multiplier_for_flow(interval.lat_mult, flow_id)
        eff_rpm = base_rpm * rpm_mult
        FLOW_EFFECTIVE_RPM[(interval.start_min, flow_id)] = eff_rpm
        if eff_rpm <= 0.0:
            continue

        expected = eff_rpm * dur_min
        count = stable_int_count(expected, f"flow|{state}|{interval.start_min}-{interval.end_min}|{flow_id}")
        if count <= 0:
            continue

        starts = schedule_times_uniform(interval.start_dt, interval.end_dt, count, f"flow_start|{state}|{flow_id}|{interval.start_min}", 40)

        for i, start_ts in enumerate(starts):
            instance_key = f"{state}|{interval.start_min}|{flow_id}|{i}"
            trace_id = hex32("trace|" + instance_key) if flow.get("trace", False) and SYSTEM.get("tracing", {}).get("on", False) else ""
            ats_hosts = COMPONENTS["edge_ats"]["hosts"]
            ats_host = ats_hosts[int(u01("ats_host|" + trace_id) * len(ats_hosts))] if ats_hosts else ""

            if flow_id == "tile_request_ok":
                p50, p95 = flow["latency_ms"][0]
                dur = sample_lognormal_ms(p50 * lat_mult, p95 * lat_mult, f"dur|{instance_key}", soft_cap=3.0 * p95 * lat_mult)
                dur_ms = int(round(dur))
                dom = COMPONENTS["edge_ats"]["logs"]["webrequest_ok"]["vars"]["dur_ms"]["v"]
                dur_ms = int(clamp(dur_ms, int(dom[0]), int(dom[1])))
                end_ts = start_ts + timedelta(milliseconds=dur_ms)

                uri_tpl = gen_value_from_domain(COMPONENTS["edge_ats"]["logs"]["webrequest_ok"]["vars"]["uri"], f"uri|{instance_key}")
                uri = expand_str_template(uri_tpl, f"uri|{instance_key}")
                backend = gen_value_from_domain(COMPONENTS["edge_ats"]["logs"]["webrequest_ok"]["vars"]["backend"], f"be|{instance_key}")
                bytes_v = gen_value_from_domain(COMPONENTS["edge_ats"]["logs"]["webrequest_ok"]["vars"]["bytes"], f"bytes|{instance_key}")
                overrides = {"uri": uri, "backend": backend, "bytes": bytes_v, "dur_ms": dur_ms, "trace_id": trace_id}

                level, msg = render_log_message("edge_ats", "webrequest_ok", state, instance_key, overrides=overrides)
                add_row(rows, end_ts, level, msg, trace_id, COMPONENTS["edge_ats"]["svc"], ats_host)

            elif flow_id == "tile_request_tls_502":
                (p50a, p95a), (p50b, p95b) = flow["latency_ms"]
                h = sample_lognormal_ms(p50a * lat_mult, p95a * lat_mult, f"hs|{instance_key}", soft_cap=3.0 * p95a * lat_mult)
                handshake_ms = int(round(h))
                dom_h = COMPONENTS["edge_ats"]["logs"]["origin_tls_handshake_fail"]["vars"]["handshake_ms"]["v"]
                handshake_ms = int(clamp(handshake_ms, int(dom_h[0]), int(dom_h[1])))

                dom_total = COMPONENTS["edge_ats"]["logs"]["webrequest_502"]["vars"]["dur_ms"]["v"]
                total_max = int(dom_total[1])
                remaining_cap = max(1.0, float(total_max - handshake_ms))
                s = sample_lognormal_ms(p50b * lat_mult, p95b * lat_mult, f"seg2|{instance_key}", soft_cap=min(3.0 * p95b * lat_mult, remaining_cap))
                seg2_ms = int(round(clamp(s, 1.0, remaining_cap)))
                total_ms = handshake_ms + seg2_ms
                total_ms = int(clamp(total_ms, int(dom_total[0]), int(dom_total[1])))
                seg2_ms = max(1, total_ms - handshake_ms)

                ts1 = start_ts + timedelta(milliseconds=handshake_ms)
                ts2 = ts1 + timedelta(milliseconds=seg2_ms)

                uri_tpl = gen_value_from_domain(COMPONENTS["edge_ats"]["logs"]["webrequest_502"]["vars"]["uri"], f"uri|{instance_key}")
                uri = expand_str_template(uri_tpl, f"uri|{instance_key}")
                backend = gen_value_from_domain(COMPONENTS["edge_ats"]["logs"]["webrequest_502"]["vars"]["backend"], f"be|{instance_key}")
                peer_san = gen_value_from_domain(COMPONENTS["edge_ats"]["logs"]["origin_tls_handshake_fail"]["vars"]["peer_san_sample"], f"san|{instance_key}")

                overrides1 = {"backend": backend, "peer_san_sample": peer_san, "handshake_ms": handshake_ms, "trace_id": trace_id}
                level1, msg1 = render_log_message("edge_ats", "origin_tls_handshake_fail", state, instance_key + "|1", overrides=overrides1)
                add_row(rows, ts1, level1, msg1, trace_id, COMPONENTS["edge_ats"]["svc"], ats_host)

                overrides2 = {"uri": uri, "backend": backend, "dur_ms": total_ms, "trace_id": trace_id}
                level2, msg2 = render_log_message("edge_ats", "webrequest_502", state, instance_key + "|2", overrides=overrides2)
                add_row(rows, ts2, level2, msg2, trace_id, COMPONENTS["edge_ats"]["svc"], ats_host)


def simulate_one_shots(rows: List[Dict[str, Any]], base_time: datetime) -> None:
    events = list(SCENARIO["scenario"]["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e.get("order", 0)))

    for ev in events:
        at_min = int(ev["at_min"])
        t0 = base_time + timedelta(minutes=at_min)
        ones = ev.get("one_shots") or []
        for j, os in enumerate(ones):
            ref = os["ref"]
            count = int(os["count"])
            allowed_hosts = list(os.get("hosts") or [])
            comp_id, log_id = ref.split(".", 1)
            comp = COMPONENTS[comp_id]
            service = comp.get("svc", "")
            comp_hosts = list(comp.get("hosts") or [])
            for k in range(count):
                ts = t0 + timedelta(milliseconds=10 + 20 * k + (jitter_ms(f"oneshot|{ref}|{at_min}|{j}|{k}", 30)))
                if allowed_hosts:
                    host = allowed_hosts[k % len(allowed_hosts)]
                else:
                    host = comp_hosts[k % len(comp_hosts)] if comp_hosts else ""

                overrides: Dict[str, Any] = {}
                if comp_id == "ops_tooling":
                    if log_id == "sal_pool_k8s_backends":
                        overrides["note"] = "pooled k8s backends for migration"
                    elif log_id == "sal_check_5xx_dashboard":
                        overrides["note"] = "checked maps 5xx dashboard after noticing errors"
                    elif log_id == "sal_depool_k8s_backends":
                        overrides["note"] = "depooled k8s backends due to tls errors"
                    elif log_id == "sal_verify_tls_san_issue":
                        overrides["note"] = "verified missing SAN for maps.wikimedia.org on k8s cert"
                    elif log_id == "openssl_probe":
                        overrides["addr"] = expand_str_template("kart-k8s<id>:443", f"probe|{at_min}|{k}")
                if comp_id == "lvs_kart" and log_id in ("pool_k8s_backend", "depool_k8s_backend"):
                    overrides["backend"] = gen_value_from_domain(comp["logs"][log_id]["vars"]["backend"], f"{ref}|{at_min}|{k}")

                level, msg = render_log_message(comp_id, log_id, "f", f"oneshot|{at_min}|{ref}|{k}", overrides=overrides)
                add_row(rows, ts, level, msg, "", service, host)


def build_all_intervals(base_time: datetime) -> List[Interval]:
    sc = SCENARIO["scenario"]
    n_start = sc["time"]["phases"]["n"]["start_min"]
    n_end = sc["time"]["phases"]["n"]["end_min"]
    normal = Interval(state="n", start_min=n_start, end_min=n_end, start_dt=base_time + timedelta(minutes=n_start), end_dt=base_time + timedelta(minutes=n_end), rate_mult={}, lat_mult={}, k8s_pooled=False)
    failure_intervals = derive_failure_intervals(base_time)
    return [normal] + failure_intervals


COMPONENTS: Dict[str, Dict[str, Any]] = {}
FLOW_EFFECTIVE_RPM: Dict[Tuple[int, str], float] = {}


def main() -> None:
    # Determinism hooks required by verifier, even though this simulator primarily uses hash-based pseudo-randomness.
    random.seed(SEED)
    np_seed = int.from_bytes(hashlib.md5(SEED.encode("utf-8")).digest()[:8], "big", signed=False) % (2**32)
    np.random.seed(np_seed)

    global COMPONENTS
    COMPONENTS = build_indices()

    base_time = datetime(2025, 2, 19, 0, 0, 0, tzinfo=timezone.utc)
    intervals = build_all_intervals(base_time)

    rows: List[Dict[str, Any]] = []

    # Precompute effective rpms per interval start for background coherence fields.
    for interval in intervals:
        if interval.state == "n":
            for flow in SYSTEM["flows"]["n"]["req"]:
                FLOW_EFFECTIVE_RPM[(interval.start_min, flow["id"])] = float(flow["rpm"])
        else:
            for flow in SYSTEM["flows"]["f"]["req"]:
                base_rpm = float(flow["rpm"])
                eff = base_rpm * multiplier_for_flow(interval.rate_mult, flow["id"])
                FLOW_EFFECTIVE_RPM[(interval.start_min, flow["id"])] = eff

    for interval in intervals:
        simulate_background(rows, interval)

    for interval in intervals:
        simulate_flows(rows, interval)

    simulate_one_shots(rows, base_time)

    df = pd.DataFrame(rows, columns=["timestamp", "level", "message", "trace_id", "service", "host"])
    df.sort_values(by=["timestamp", "service", "host", "level"], inplace=True, kind="mergesort")
    df.reset_index(drop=True, inplace=True)

    if df.shape[0] < 20000 or df.shape[0] > 100000:
        raise RuntimeError(f"Row count {df.shape[0]} outside required [20000, 100000].")

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
