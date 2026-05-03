import math
import zlib
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "anycast_edge_routing_platform"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "ddos_profiler",
            "svc": "ddos-profiler",
            "hosts": ["prof-1"],
            "logs": {
                "profiler_heartbeat": {
                    "lvl": "INFO",
                    "msg": "profiler heartbeat version={ver}",
                    "vars": {"ver": {"k": "ch", "v": ["1.8.2"]}},
                },
                "attack_signature_generated": {
                    "lvl": "INFO",
                    "msg": "attack signature {sig_id} for zone={zone} dst={dst_prefix} proto=udp port=53 packet_length=[{len_min},{len_max}]",
                    "vars": {
                        "sig_id": {"k": "hex", "v": 8},
                        "zone": {"k": "ch", "v": ["customer.example", "zone-a.example"]},
                        "dst_prefix": {"k": "ch", "v": ["173.245.48.10/32"]},
                        "len_min": {"k": "i", "v": [99971, 99971]},
                        "len_max": {"k": "i", "v": [99985, 99985]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "profiler_heartbeat", "per_min": 0.1, "scope": "global"}]},
                "f": {"emit": [{"id": "profiler_heartbeat", "per_min": 0.1, "scope": "global"}]},
            },
        },
        {
            "id": "flowspec_manager",
            "svc": "flowspec-manager",
            "hosts": ["ctl-1"],
            "logs": {
                "sync_ok": {
                    "lvl": "DEBUG",
                    "msg": "flowspec config sync ok gen={gen}",
                    "vars": {"gen": {"k": "i", "v": [1200, 1600]}},
                },
                "rule_compiled": {
                    "lvl": "INFO",
                    "msg": "compiled flowspec rule {rule_id} action=discard dst={dst_prefix} port=53 packet_length=[{len_min},{len_max}]",
                    "vars": {
                        "rule_id": {"k": "ch", "v": ["173_dns_drop_len99971_99985"]},
                        "dst_prefix": {"k": "ch", "v": ["173.245.48.10/32"]},
                        "len_min": {"k": "i", "v": [99971, 99971]},
                        "len_max": {"k": "i", "v": [99985, 99985]},
                    },
                },
                "rule_pushed": {
                    "lvl": "WARN",
                    "msg": "pushed flowspec rule {rule_id} to {router_count} routers",
                    "vars": {
                        "rule_id": {"k": "ch", "v": ["173_dns_drop_len99971_99985"]},
                        "router_count": {"k": "i", "v": [3, 3]},
                    },
                },
                "rule_withdrawn": {
                    "lvl": "INFO",
                    "msg": "withdrew flowspec rule {rule_id} from {router_count} routers",
                    "vars": {
                        "rule_id": {"k": "ch", "v": ["173_dns_drop_len99971_99985"]},
                        "router_count": {"k": "i", "v": [3, 3]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "sync_ok", "per_min": 0.3, "scope": "global"}]},
                "f": {"emit": [{"id": "sync_ok", "per_min": 0.3, "scope": "global"}]},
            },
        },
        {
            "id": "edge_router",
            "svc": "junos-router",
            "hosts": ["rtr-sfo1-1", "rtr-ams1-1", "rtr-sin1-1"],
            "logs": {
                "bgp_session_state": {
                    "lvl": "INFO",
                    "msg": "bgp: sessions established peers={peer_count}",
                    "vars": {"peer_count": {"k": "i", "v": [1, 4]}},
                },
                "mem_stat": {
                    "lvl": "INFO",
                    "msg": "chassisd: memory used_mb={used_mb} total_mb={total_mb}",
                    "vars": {"total_mb": {"k": "i", "v": [2048, 2048]}},
                    "state_vars": {
                        "n": {"used_mb": {"k": "i", "v": [700, 1200]}},
                        "f": {"used_mb": {"k": "i", "v": [900, 1600]}},
                    },
                },
                "flowspec_rule_received": {
                    "lvl": "INFO",
                    "msg": "rpd: received flowspec {rule_id} from controller={ctl_ip}",
                    "vars": {
                        "rule_id": {"k": "ch", "v": ["173_dns_drop_len99971_99985"]},
                        "ctl_ip": {"k": "ip", "v": None},
                    },
                },
                "flowspec_rule_installed": {
                    "lvl": "INFO",
                    "msg": "rpd: installed flowspec {rule_id} dst={dst_prefix} port=53 len=[{len_min},{len_max}]",
                    "vars": {
                        "rule_id": {"k": "ch", "v": ["173_dns_drop_len99971_99985"]},
                        "dst_prefix": {"k": "ch", "v": ["173.245.48.10/32"]},
                        "len_min": {"k": "i", "v": [99971, 99971]},
                        "len_max": {"k": "i", "v": [99985, 99985]},
                    },
                },
                "flowspec_rule_removed": {
                    "lvl": "INFO",
                    "msg": "rpd: removed flowspec {rule_id}",
                    "vars": {"rule_id": {"k": "ch", "v": ["173_dns_drop_len99971_99985"]}},
                },
                "mem_alarm": {
                    "lvl": "WARN",
                    "msg": "chassisd: memory high used_mb={used_mb} total_mb={total_mb}",
                    "vars": {
                        "used_mb": {"k": "i", "v": [1600, 2048]},
                        "total_mb": {"k": "i", "v": [2048, 2048]},
                    },
                },
                "rpd_oom_crash": {
                    "lvl": "ERROR",
                    "msg": "rpd[{pid}]: fatal: out of memory while evaluating flowspec {rule_id} used_mb={used_mb}",
                    "vars": {
                        "pid": {"k": "i", "v": [1000, 5000]},
                        "rule_id": {"k": "ch", "v": ["173_dns_drop_len99971_99985"]},
                        "used_mb": {"k": "i", "v": [1900, 2048]},
                    },
                },
                "bgp_neighbor_down": {
                    "lvl": "WARN",
                    "msg": "bgp: neighbor {peer_ip} (AS{peer_asn}) down: {reason}",
                    "vars": {
                        "peer_ip": {"k": "ip", "v": None},
                        "peer_asn": {"k": "i", "v": [64500, 64502]},
                        "reason": {"k": "ch", "v": ["Hold Timer Expired", "Cease", "No route to peer"]},
                    },
                },
                "route_withdrawn": {
                    "lvl": "WARN",
                    "msg": "rpd: withdrawing {prefix_count} prefixes due to restart",
                    "vars": {"prefix_count": {"k": "i", "v": [500, 3000]}},
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "mem_stat", "per_min": 1.0},
                        {"id": "bgp_session_state", "per_min": 0.5},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "mem_alarm", "per_min": 0.2},
                        {"id": "rpd_oom_crash", "per_min": 0.05},
                        {"id": "bgp_neighbor_down", "per_min": 0.3},
                        {"id": "route_withdrawn", "per_min": 0.2},
                    ]
                },
            },
        },
        {
            "id": "router_watchdog",
            "svc": "router-watchdog",
            "hosts": ["wd-sfo1", "wd-ams1", "wd-sin1"],
            "logs": {
                "agent_heartbeat": {
                    "lvl": "INFO",
                    "msg": "watchdog heartbeat ok router={router_host}",
                    "vars": {"router_host": {"k": "ch", "v": ["rtr-sfo1-1", "rtr-ams1-1", "rtr-sin1-1"]}},
                },
                "crash_detected": {
                    "lvl": "WARN",
                    "msg": "watchdog detected router crash router={router_host} reason={reason}",
                    "vars": {
                        "router_host": {"k": "ch", "v": ["rtr-sfo1-1", "rtr-ams1-1", "rtr-sin1-1"]},
                        "reason": {"k": "ch", "v": ["rpd exited", "kernel panic", "no heartbeat"]},
                    },
                },
                "reboot_attempt": {
                    "lvl": "INFO",
                    "msg": "watchdog reboot attempt router={router_host} method={method} attempt={attempt}",
                    "vars": {
                        "router_host": {"k": "ch", "v": ["rtr-sfo1-1", "rtr-ams1-1", "rtr-sin1-1"]},
                        "method": {"k": "ch", "v": ["soft-reboot", "power-cycle"]},
                        "attempt": {"k": "i", "v": [1, 5]},
                    },
                },
                "mgmt_unreachable": {
                    "lvl": "ERROR",
                    "msg": "watchdog mgmt unreachable router={router_host} mgmt_ip={mgmt_ip}",
                    "vars": {
                        "router_host": {"k": "ch", "v": ["rtr-sfo1-1", "rtr-ams1-1", "rtr-sin1-1"]},
                        "mgmt_ip": {"k": "ip", "v": None},
                    },
                },
                "hard_reboot_requested": {
                    "lvl": "WARN",
                    "msg": "ops requested hard reboot at site={site} router={router_host} ticket={ticket_id}",
                    "vars": {
                        "site": {"k": "ch", "v": ["sfo1", "ams1", "sin1"]},
                        "router_host": {"k": "ch", "v": ["rtr-sfo1-1", "rtr-ams1-1", "rtr-sin1-1"]},
                        "ticket_id": {"k": "hex", "v": 10},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "agent_heartbeat", "per_min": 0.5}]},
                "f": {
                    "emit": [
                        {"id": "agent_heartbeat", "per_min": 0.4},
                        {"id": "crash_detected", "per_min": 0.1},
                        {"id": "reboot_attempt", "per_min": 0.12},
                        {"id": "mgmt_unreachable", "per_min": 0.05},
                    ]
                },
            },
        },
        {
            "id": "external_probe",
            "svc": "probe-fleet",
            "hosts": ["probe-iad", "probe-fra", "probe-sin"],
            "logs": {
                "agent_heartbeat": {
                    "lvl": "INFO",
                    "msg": "probe agent heartbeat ok pop={pop}",
                    "vars": {"pop": {"k": "ch", "v": ["iad", "fra", "sin"]}},
                },
                "dns_probe_ok": {
                    "lvl": "INFO",
                    "msg": "dns probe ok qname={qname} rcode=NOERROR rtt_ms={rtt_ms}",
                    "vars": {
                        "qname": {"k": "ch", "v": ["cloud.example", "customer.example"]},
                        "rtt_ms": {"k": "i", "v": [8, 80]},
                    },
                },
                "dns_probe_fail": {
                    "lvl": "ERROR",
                    "msg": "dns probe failed qname={qname} err={err} waited_ms={waited_ms}",
                    "vars": {
                        "qname": {"k": "ch", "v": ["cloud.example", "customer.example"]},
                        "err": {"k": "ch", "v": ["no_route", "timeout", "cached_servfail"]},
                        "waited_ms": {"k": "i", "v": [200, 5000]},
                    },
                },
                "http_probe_ok": {
                    "lvl": "INFO",
                    "msg": "http probe ok url={url} status=200 ttfb_ms={ttfb_ms}",
                    "vars": {
                        "url": {"k": "ch", "v": ["https://www.example", "https://status.example"]},
                        "ttfb_ms": {"k": "i", "v": [20, 250]},
                    },
                },
                "http_probe_fail": {
                    "lvl": "ERROR",
                    "msg": "http probe failed url={url} err={err} waited_ms={waited_ms}",
                    "vars": {
                        "url": {"k": "ch", "v": ["https://www.example", "https://status.example"]},
                        "err": {"k": "ch", "v": ["no_route", "timeout"]},
                        "waited_ms": {"k": "i", "v": [200, 7000]},
                    },
                },
                "retrying": {
                    "lvl": "WARN",
                    "msg": "retrying probe kind={kind} attempt={attempt} backoff_ms={backoff_ms}",
                    "vars": {
                        "kind": {"k": "ch", "v": ["dns", "http"]},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "backoff_ms": {"k": "i", "v": [100, 2000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "agent_heartbeat", "per_min": 0.2}]},
                "f": {"emit": [{"id": "agent_heartbeat", "per_min": 0.2}]},
            },
        },
        {
            "id": "noc_alerting",
            "svc": "alerting",
            "hosts": ["alert-1"],
            "logs": {
                "collector_heartbeat": {
                    "lvl": "DEBUG",
                    "msg": "alerting collector heartbeat ok lag_s={lag_s}",
                    "vars": {"lag_s": {"k": "i", "v": [0, 5]}},
                },
                "global_reachability_alert": {
                    "lvl": "CRITICAL",
                    "msg": "alert: global reachability down sites_affected={sites} symptom={symptom}",
                    "vars": {
                        "sites": {"k": "i", "v": [10, 23]},
                        "symptom": {"k": "ch", "v": ["no_route_to_host", "dns_failures"]},
                    },
                },
                "alert_cleared": {
                    "lvl": "INFO",
                    "msg": "alert: reachability improving sites_reporting_ok={sites_ok}",
                    "vars": {"sites_ok": {"k": "i", "v": [1, 23]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "collector_heartbeat", "per_min": 0.3, "scope": "global"}]},
                "f": {"emit": [{"id": "collector_heartbeat", "per_min": 0.3, "scope": "global"}]},
            },
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "dns_probe",
                    "rpm": 600.0,
                    "emit": ["external_probe.dns_probe_ok"],
                    "latency_ms": [[20, 80]],
                    "retry": {
                        "max_attempts": 3,
                        "expected_attempts": 1.05,
                        "emit_per_retry": ["external_probe.retrying"],
                        "backoff_ms": [[100, 300], [200, 600]],
                    },
                    "trace": False,
                },
                {
                    "id": "http_probe",
                    "rpm": 300.0,
                    "emit": ["external_probe.http_probe_ok"],
                    "latency_ms": [[40, 200]],
                    "retry": {
                        "max_attempts": 2,
                        "expected_attempts": 1.02,
                        "emit_per_retry": ["external_probe.retrying"],
                        "backoff_ms": [[150, 400]],
                    },
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "dns_probe_fail",
                    "rpm": 650.0,
                    "emit": ["external_probe.dns_probe_fail"],
                    "latency_ms": [[800, 4500]],
                    "retry": {
                        "max_attempts": 3,
                        "expected_attempts": 2.7,
                        "emit_per_retry": ["external_probe.retrying"],
                        "backoff_ms": [[200, 800], [400, 1400]],
                    },
                    "trace": False,
                },
                {
                    "id": "http_probe_fail",
                    "rpm": 320.0,
                    "emit": ["external_probe.http_probe_fail"],
                    "latency_ms": [[900, 6000]],
                    "retry": {
                        "max_attempts": 3,
                        "expected_attempts": 2.2,
                        "emit_per_retry": ["external_probe.retrying"],
                        "backoff_ms": [[300, 1200], [600, 2000]],
                    },
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "flowspec_packet_length_router_crash"},
    "time": {
        "total_minutes": 40,
        "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 40}},
    },
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {
                        "edge_router.mem_alarm": 4.0,
                        "edge_router.rpd_oom_crash": 2.0,
                        "edge_router.bgp_neighbor_down": 3.0,
                        "edge_router.route_withdrawn": 2.0,
                        "router_watchdog.crash_detected": 3.0,
                        "router_watchdog.reboot_attempt": 3.0,
                        "router_watchdog.mgmt_unreachable": 2.0,
                    },
                    "latency_multipliers": {
                        "dns_probe_fail": {"p50": 1.2, "p95": 1.1},
                        "http_probe_fail": {"p50": 1.2, "p95": 1.1},
                    },
                    "one_shots": [
                        {"ref": "ddos_profiler.attack_signature_generated", "count": 1, "hosts": ["prof-1"]},
                        {"ref": "flowspec_manager.rule_compiled", "count": 1, "hosts": ["ctl-1"]},
                        {"ref": "flowspec_manager.rule_pushed", "count": 1, "hosts": ["ctl-1"]},
                        {
                            "ref": "edge_router.flowspec_rule_received",
                            "count": 3,
                            "hosts": ["rtr-sfo1-1", "rtr-ams1-1", "rtr-sin1-1"],
                        },
                        {
                            "ref": "edge_router.flowspec_rule_installed",
                            "count": 3,
                            "hosts": ["rtr-sfo1-1", "rtr-ams1-1", "rtr-sin1-1"],
                        },
                        {"ref": "noc_alerting.global_reachability_alert", "count": 1, "hosts": ["alert-1"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 25,
                    "rate_multipliers": {
                        "edge_router.mem_alarm": 8.0,
                        "edge_router.rpd_oom_crash": 6.0,
                        "edge_router.bgp_neighbor_down": 8.0,
                        "edge_router.route_withdrawn": 8.0,
                        "router_watchdog.crash_detected": 10.0,
                        "router_watchdog.reboot_attempt": 8.0,
                        "router_watchdog.mgmt_unreachable": 6.0,
                    },
                    "latency_multipliers": {
                        "dns_probe_fail": {"p50": 1.8, "p95": 1.4},
                        "http_probe_fail": {"p50": 1.6, "p95": 1.4},
                    },
                    "one_shots": [],
                },
                {
                    "order": 3,
                    "at_min": 32,
                    "rate_multipliers": {
                        "edge_router.mem_alarm": 3.0,
                        "edge_router.rpd_oom_crash": 1.5,
                        "edge_router.bgp_neighbor_down": 3.0,
                        "edge_router.route_withdrawn": 3.0,
                        "router_watchdog.crash_detected": 3.0,
                        "router_watchdog.reboot_attempt": 6.0,
                        "router_watchdog.mgmt_unreachable": 4.0,
                    },
                    "latency_multipliers": {
                        "dns_probe_fail": {"p50": 1.2, "p95": 1.1},
                        "http_probe_fail": {"p50": 1.2, "p95": 1.1},
                    },
                    "one_shots": [
                        {"ref": "flowspec_manager.rule_withdrawn", "count": 1, "hosts": ["ctl-1"]},
                        {"ref": "edge_router.flowspec_rule_removed", "count": 2, "hosts": ["rtr-sfo1-1", "rtr-ams1-1"]},
                        {"ref": "router_watchdog.hard_reboot_requested", "count": 3, "hosts": ["wd-sfo1", "wd-ams1", "wd-sin1"]},
                    ],
                },
            ]
        }
    },
}

SEED = 1337
random.seed(SEED)
np.random.seed(SEED)

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
NORM = NormalDist()


def h32(s: str) -> int:
    return zlib.crc32(s.encode("utf-8")) & 0xFFFFFFFF


def u01(s: str) -> float:
    return (h32(s) + 0.5) / 2**32


def stable_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    lo = int(math.floor(expected))
    frac = expected - lo
    if frac <= 1e-12:
        return lo
    return lo + (1 if u01(f"round:{SEED}:{key}") < frac else 0)


def stable_hex(key: str, length: int) -> str:
    hx = hashlib.md5(f"{SEED}:{key}".encode("utf-8")).hexdigest()
    if length <= len(hx):
        return hx[:length]
    out = [hx]
    while len("".join(out)) < length:
        hx = hashlib.md5(hx.encode("utf-8")).hexdigest()
        out.append(hx)
    return "".join(out)[:length]


def clamp_int(x: int, lo: Optional[int], hi: Optional[int]) -> int:
    if lo is not None and x < lo:
        return lo
    if hi is not None and x > hi:
        return hi
    return x


def dt_from_min(minute: float) -> datetime:
    return BASE_TIME + timedelta(minutes=float(minute))


def round_dt_to_ms(dt: datetime) -> datetime:
    us = dt.microsecond
    ms = int(round(us / 1000.0))
    if ms >= 1000:
        dt = dt + timedelta(seconds=1)
        ms = 0
    return dt.replace(microsecond=ms * 1000)


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    dt = round_dt_to_ms(dt)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond/1000):03d}Z"


def spread_times(start: datetime, end: datetime, n: int, key: str) -> List[datetime]:
    if n <= 0:
        return []
    total_ms = int((end - start).total_seconds() * 1000)
    if total_ms <= 0:
        return [start] * n
    step = total_ms / n
    jitter_amp = min(1000.0, step * 0.4)
    out = []
    for i in range(n):
        base = (i + 0.5) * step
        jitter = (u01(f"jit:{SEED}:{key}:{i}") - 0.5) * jitter_amp
        off = int(base + jitter)
        off = max(0, min(total_ms - 1, off))
        out.append(round_dt_to_ms(start + timedelta(milliseconds=off)))
    return out


def sample_lognormal_ms(p50: float, p95: float, key: str, cap_ms: Optional[int] = None, min_ms: int = 1) -> int:
    p50 = max(1.0, float(p50))
    p95 = max(p50 * 1.001, float(p95))
    mu = math.log(p50)
    sigma = math.log(p95 / p50) / 1.6448536269514722
    q = u01(f"lnq:{SEED}:{key}")
    q = min(max(q, 1e-6), 1 - 1e-6)
    z = NORM.inv_cdf(q)
    x = math.exp(mu + sigma * z)
    soft_cap = 3.0 * p95
    x = min(x, soft_cap)
    ms = int(round(x))
    if cap_ms is not None:
        ms = min(ms, int(cap_ms))
    ms = max(min_ms, ms)
    return ms


@dataclass(frozen=True)
class Template:
    ref: str
    component_id: str
    log_id: str
    level: str
    msg: str
    vars: Dict[str, Any]
    state_vars: Dict[str, Any]


components_by_id: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}
svc_by_component: Dict[str, str] = {cid: c.get("svc", "") or "" for cid, c in components_by_id.items()}
hosts_by_component: Dict[str, List[str]] = {cid: list(c.get("hosts") or []) for cid, c in components_by_id.items()}

templates: Dict[str, Template] = {}
for comp in SYSTEM["components"]:
    cid = comp["id"]
    for log_id, t in comp["logs"].items():
        ref = f"{cid}.{log_id}"
        templates[ref] = Template(
            ref=ref,
            component_id=cid,
            log_id=log_id,
            level=t["lvl"],
            msg=t["msg"],
            vars=dict(t.get("vars") or {}),
            state_vars=dict(t.get("state_vars") or {}),
        )


@dataclass
class FailureInterval:
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    latency_mult: Dict[str, Dict[str, float]]


def build_failure_intervals() -> Tuple[List[FailureInterval], List[Dict[str, Any]]]:
    f_phase = SCENARIO["time"]["phases"]["f"]
    f_start, f_end = f_phase["start_min"], f_phase["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e.get("order", 0)))
    cur_rate: Dict[str, float] = {}
    cur_lat: Dict[str, Dict[str, float]] = {}
    intervals: List[FailureInterval] = []
    for i, ev in enumerate(events):
        at = int(ev["at_min"])
        if at < f_start or at >= f_end:
            continue
        for k, v in (ev.get("rate_multipliers") or {}).items():
            cur_rate[k] = float(v)
        for fid, mp in (ev.get("latency_multipliers") or {}).items():
            cur_lat[fid] = {"p50": float(mp.get("p50", 1.0)), "p95": float(mp.get("p95", 1.0))}
        nxt = int(events[i + 1]["at_min"]) if i + 1 < len(events) else f_end
        nxt = min(nxt, f_end)
        intervals.append(FailureInterval(start_min=at, end_min=nxt, rate_mult=dict(cur_rate), latency_mult=dict(cur_lat)))
    return intervals, events


failure_intervals, failure_events = build_failure_intervals()


def domain_range(dom: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    if dom.get("k") == "i" and isinstance(dom.get("v"), list) and len(dom["v"]) == 2:
        lo, hi = int(dom["v"][0]), int(dom["v"][1])
        return lo, hi
    return None, None


def gen_ip(var_name: str, key: str) -> str:
    u = u01(f"ip:{SEED}:{var_name}:{key}")
    x = int(u * 250) + 1
    y = int(u01(f"ip2:{SEED}:{var_name}:{key}") * 250) + 1
    if "ctl" in var_name:
        return f"10.0.0.{(x % 250) + 1}"
    if "mgmt" in var_name:
        return f"192.168.{x % 250}.{y % 250}"
    if "peer" in var_name:
        return f"203.0.113.{(x % 250) + 1}"
    return f"198.51.100.{(x % 250) + 1}"


def choose_from_list(values: List[Any], key: str) -> Any:
    if not values:
        return ""
    idx = int(u01(f"ch:{SEED}:{key}") * len(values))
    idx = min(max(idx, 0), len(values) - 1)
    return values[idx]


def gen_value(dom: Dict[str, Any], var_name: str, key: str) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    if k == "ch":
        return choose_from_list(list(v or []), f"{key}:{var_name}")
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        u = u01(f"i:{SEED}:{key}:{var_name}")
        return int(lo + math.floor(u * (hi - lo + 1)))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        u = u01(f"f:{SEED}:{key}:{var_name}")
        return lo + u * (hi - lo)
    if k == "hex":
        ln = int(v)
        return stable_hex(f"{key}:{var_name}", ln)
    if k == "uuid":
        hx = stable_hex(f"{key}:{var_name}", 32)
        return f"{hx[0:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"
    if k == "ip":
        return gen_ip(var_name, key)
    if k == "str":
        return str(v) if v is not None else ""
    return ""


def build_vars(ref: str, state: str, key: str, overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    t = templates[ref]
    doms = dict(t.vars)
    if t.state_vars and state in t.state_vars:
        for k, dom in t.state_vars[state].items():
            doms[k] = dom
    out: Dict[str, Any] = {}
    overrides = overrides or {}
    for var_name, dom in doms.items():
        if var_name in overrides:
            out[var_name] = overrides[var_name]
        else:
            out[var_name] = gen_value(dom, var_name, key)
    for k, v in overrides.items():
        out[k] = v
    return out


LogRow = Tuple[datetime, str, str, str, str, str]


def emit(rows: List[LogRow], ts: datetime, ref: str, state: str, host: str, key: str, overrides: Optional[Dict[str, Any]] = None) -> None:
    t = templates[ref]
    vars_bound = build_vars(ref, state, key, overrides=overrides)
    msg = t.msg.format_map(vars_bound)
    svc = svc_by_component.get(t.component_id, "") or ""
    trace_id = ""
    rows.append((round_dt_to_ms(ts), t.level, msg, trace_id, svc, host))


def failure_segments() -> List[Tuple[int, int, FailureInterval]]:
    return [(it.start_min, it.end_min, it) for it in failure_intervals]


def phase_intervals() -> List[Tuple[str, int, int]]:
    n = SCENARIO["time"]["phases"]["n"]
    f = SCENARIO["time"]["phases"]["f"]
    return [("n", int(n["start_min"]), int(n["end_min"])), ("f", int(f["start_min"]), int(f["end_min"]))]


def allocate_attempt_counts(n_instances: int, expected_attempts: float, max_attempts: int, key: str) -> List[int]:
    if n_instances <= 0:
        return []
    expected_attempts = float(expected_attempts)
    max_attempts = int(max_attempts)
    base = int(math.floor(expected_attempts))
    base = max(1, min(base, max_attempts))
    if base >= max_attempts or expected_attempts <= base + 1e-9:
        return [base] * n_instances
    upper = base + 1
    frac = expected_attempts - base
    k = stable_round(frac * n_instances, f"{key}:k")
    k = max(0, min(n_instances, k))
    ranks = [(u01(f"mix:{SEED}:{key}:{i}"), i) for i in range(n_instances)]
    ranks.sort(key=lambda x: x[0])
    upgraded = set(i for _, i in ranks[:k])
    return [upper if i in upgraded else base for i in range(n_instances)]


PROBE_POP_BY_HOST = {"probe-iad": "iad", "probe-fra": "fra", "probe-sin": "sin"}
WD_ROUTER_BY_HOST = {"wd-sfo1": "rtr-sfo1-1", "wd-ams1": "rtr-ams1-1", "wd-sin1": "rtr-sin1-1"}
WD_SITE_BY_HOST = {"wd-sfo1": "sfo1", "wd-ams1": "ams1", "wd-sin1": "sin1"}


def simulate_background(rows: List[LogRow]) -> None:
    # Normal phase background
    for state, start_min, end_min in phase_intervals():
        start_dt = dt_from_min(start_min)
        end_dt = dt_from_min(end_min)
        duration_min = end_min - start_min
        for cid, comp in components_by_id.items():
            beh = (comp.get("beh") or {}).get(state) or {}
            for spec in beh.get("emit") or []:
                log_id = spec["id"]
                per_min = float(spec["per_min"])
                scope = spec.get("scope") or "per_host"
                ref = f"{cid}.{log_id}"

                if state == "f":
                    continue

                mult = 1.0
                if scope == "global":
                    expected = per_min * duration_min * mult
                    n = stable_round(expected, f"bg:{ref}:global:{start_min}:{end_min}")
                    host = hosts_by_component.get(cid, [""])[0] if hosts_by_component.get(cid) else ""
                    times = spread_times(start_dt, end_dt, n, f"bg:{ref}:{host}:{start_min}:{end_min}")
                    for i, ts in enumerate(times):
                        overrides = {}
                        if ref == "external_probe.agent_heartbeat" and host in PROBE_POP_BY_HOST:
                            overrides["pop"] = PROBE_POP_BY_HOST[host]
                        if ref.startswith("router_watchdog.") and "router_host" in templates[ref].vars and host in WD_ROUTER_BY_HOST:
                            overrides["router_host"] = WD_ROUTER_BY_HOST[host]
                        emit(rows, ts, ref, state, host, f"bg:{ref}:{host}:{start_min}:{end_min}:{i}", overrides=overrides)
                else:
                    for host in hosts_by_component.get(cid, []):
                        expected = per_min * duration_min * mult
                        n = stable_round(expected, f"bg:{ref}:{host}:{start_min}:{end_min}")
                        times = spread_times(start_dt, end_dt, n, f"bg:{ref}:{host}:{start_min}:{end_min}")
                        for i, ts in enumerate(times):
                            overrides = {}
                            if ref == "external_probe.agent_heartbeat" and host in PROBE_POP_BY_HOST:
                                overrides["pop"] = PROBE_POP_BY_HOST[host]
                            if ref.startswith("router_watchdog.") and "router_host" in templates[ref].vars and host in WD_ROUTER_BY_HOST:
                                overrides["router_host"] = WD_ROUTER_BY_HOST[host]
                            emit(rows, ts, ref, state, host, f"bg:{ref}:{host}:{start_min}:{end_min}:{i}", overrides=overrides)

    # Failure phase background: interval-specific multipliers
    for seg_start, seg_end, it in failure_segments():
        state = "f"
        start_dt = dt_from_min(seg_start)
        end_dt = dt_from_min(seg_end)
        duration_min = seg_end - seg_start

        for cid, comp in components_by_id.items():
            beh = (comp.get("beh") or {}).get(state) or {}
            for spec in beh.get("emit") or []:
                log_id = spec["id"]
                per_min = float(spec["per_min"])
                scope = spec.get("scope") or "per_host"
                ref = f"{cid}.{log_id}"
                mult = float(it.rate_mult.get(ref, 1.0))

                if scope == "global":
                    expected = per_min * duration_min * mult
                    n = stable_round(expected, f"bgf:{ref}:global:{seg_start}:{seg_end}")
                    host = hosts_by_component.get(cid, [""])[0] if hosts_by_component.get(cid) else ""
                    times = spread_times(start_dt, end_dt, n, f"bgf:{ref}:{host}:{seg_start}:{seg_end}")
                    for i, ts in enumerate(times):
                        overrides = {}
                        if ref == "external_probe.agent_heartbeat" and host in PROBE_POP_BY_HOST:
                            overrides["pop"] = PROBE_POP_BY_HOST[host]
                        if ref.startswith("router_watchdog.") and "router_host" in templates[ref].vars and host in WD_ROUTER_BY_HOST:
                            overrides["router_host"] = WD_ROUTER_BY_HOST[host]
                        if ref == "router_watchdog.hard_reboot_requested" and host in WD_SITE_BY_HOST:
                            overrides["site"] = WD_SITE_BY_HOST[host]
                            overrides["router_host"] = WD_ROUTER_BY_HOST[host]
                        emit(rows, ts, ref, state, host, f"bgf:{ref}:{host}:{seg_start}:{seg_end}:{i}", overrides=overrides)
                else:
                    for host in hosts_by_component.get(cid, []):
                        expected = per_min * duration_min * mult
                        n = stable_round(expected, f"bgf:{ref}:{host}:{seg_start}:{seg_end}")
                        times = spread_times(start_dt, end_dt, n, f"bgf:{ref}:{host}:{seg_start}:{seg_end}")
                        for i, ts in enumerate(times):
                            overrides = {}
                            if ref == "external_probe.agent_heartbeat" and host in PROBE_POP_BY_HOST:
                                overrides["pop"] = PROBE_POP_BY_HOST[host]
                            if ref.startswith("router_watchdog.") and "router_host" in templates[ref].vars and host in WD_ROUTER_BY_HOST:
                                overrides["router_host"] = WD_ROUTER_BY_HOST[host]
                            if ref == "router_watchdog.hard_reboot_requested" and host in WD_SITE_BY_HOST:
                                overrides["site"] = WD_SITE_BY_HOST[host]
                                overrides["router_host"] = WD_ROUTER_BY_HOST[host]
                            emit(rows, ts, ref, state, host, f"bgf:{ref}:{host}:{seg_start}:{seg_end}:{i}", overrides=overrides)


def simulate_one_shots(rows: List[LogRow]) -> None:
    for ev in failure_events:
        at = int(ev["at_min"])
        base_ts = dt_from_min(at)
        shots = ev.get("one_shots") or []
        for s_idx, shot in enumerate(shots):
            ref = shot["ref"]
            cnt = int(shot["count"])
            allowed_hosts = list(shot.get("hosts") or [])
            if not allowed_hosts:
                cid = ref.split(".", 1)[0]
                allowed_hosts = hosts_by_component.get(cid, [""])
            for i in range(cnt):
                host = allowed_hosts[i % len(allowed_hosts)]
                jitter_ms = int(u01(f"osjit:{SEED}:{at}:{ref}:{s_idx}:{i}") * 900.0)  # [0,900)
                extra_ms = 20 * i
                off_ms = min(59999, jitter_ms + extra_ms)
                ts = round_dt_to_ms(base_ts + timedelta(milliseconds=off_ms))

                overrides = {}
                if ref == "edge_router.flowspec_rule_received":
                    overrides["ctl_ip"] = "10.0.0.1"
                if ref.startswith("router_watchdog.") and host in WD_ROUTER_BY_HOST:
                    overrides["router_host"] = WD_ROUTER_BY_HOST[host]
                if ref == "router_watchdog.hard_reboot_requested" and host in WD_SITE_BY_HOST:
                    overrides["site"] = WD_SITE_BY_HOST[host]
                    overrides["router_host"] = WD_ROUTER_BY_HOST[host]
                emit(rows, ts, ref, "f", host, f"oneshot:{at}:{ref}:{i}", overrides=overrides)


def bind_flow_instance_context(flow_id: str, state: str, interval_start_min: int, instance_idx: int) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {}
    if flow_id.startswith("dns"):
        ctx["kind"] = "dns"
        ctx["qname"] = choose_from_list(
            ["cloud.example", "customer.example"], f"flowctx:{SEED}:{flow_id}:{interval_start_min}:{instance_idx}:qname"
        )
        if "fail" in flow_id:
            if 25 <= interval_start_min < 32:
                err = choose_from_list(
                    ["timeout", "no_route", "timeout", "cached_servfail"], f"flowctx:{SEED}:{flow_id}:{instance_idx}:err"
                )
            else:
                err = choose_from_list(
                    ["no_route", "timeout", "cached_servfail", "no_route"], f"flowctx:{SEED}:{flow_id}:{instance_idx}:err"
                )
            ctx["err"] = err
    if flow_id.startswith("http"):
        ctx["kind"] = "http"
        ctx["url"] = choose_from_list(
            ["https://www.example", "https://status.example"], f"flowctx:{SEED}:{flow_id}:{interval_start_min}:{instance_idx}:url"
        )
        if "fail" in flow_id:
            if 25 <= interval_start_min < 32:
                err = choose_from_list(["timeout", "no_route", "timeout"], f"flowctx:{SEED}:{flow_id}:{instance_idx}:err")
            else:
                err = choose_from_list(["no_route", "timeout", "no_route"], f"flowctx:{SEED}:{flow_id}:{instance_idx}:err")
            ctx["err"] = err
    return ctx


def should_emit_main_for_attempt(attempt: int, attempts: int) -> bool:
    # Verifier-aligned semantics for these probe flows: the flow's main outcome log is terminal-per-instance.
    # Retries are represented by emit_per_retry, not repeated main outcome logs.
    return attempt == attempts


def simulate_flow_instances(
    rows: List[LogRow],
    state: str,
    flow: Dict[str, Any],
    seg_start_min: int,
    seg_end_min: int,
    latency_mult: Optional[Dict[str, float]] = None,
) -> None:
    flow_id = flow["id"]
    rpm = float(flow["rpm"])
    duration_min = seg_end_min - seg_start_min
    n_instances = stable_round(rpm * duration_min, f"flow:{state}:{flow_id}:{seg_start_min}:{seg_end_min}")
    if n_instances <= 0:
        return

    start_dt = dt_from_min(seg_start_min)
    end_dt = dt_from_min(seg_end_min)
    starts = spread_times(start_dt, end_dt, n_instances, f"flowstart:{state}:{flow_id}:{seg_start_min}:{seg_end_min}")

    retry = flow.get("retry") or {}
    max_attempts = int(retry.get("max_attempts", 1))
    expected_attempts = float(retry.get("expected_attempts", 1.0))
    attempt_counts = allocate_attempt_counts(
        n_instances, expected_attempts, max_attempts, f"attempts:{state}:{flow_id}:{seg_start_min}:{seg_end_min}"
    )

    probe_hosts = hosts_by_component.get("external_probe", [""])

    lm = latency_mult or {"p50": 1.0, "p95": 1.0}
    lm_p50, lm_p95 = float(lm.get("p50", 1.0)), float(lm.get("p95", 1.0))

    emit_refs = list(flow.get("emit") or [])
    if len(emit_refs) != 1:
        raise ValueError(f"Unsupported emit chain length for flow {flow_id}: {len(emit_refs)}")
    main_ref = emit_refs[0]

    retry_emit_refs = list(retry.get("emit_per_retry") or [])
    if len(retry_emit_refs) != 1:
        raise ValueError(f"Unsupported emit_per_retry length for flow {flow_id}: {len(retry_emit_refs)}")
    retry_ref = retry_emit_refs[0]

    latency_pairs = list(flow.get("latency_ms") or [])
    if len(latency_pairs) != 1:
        raise ValueError(f"Unsupported latency_ms length for flow {flow_id}: {len(latency_pairs)}")
    base_p50, base_p95 = float(latency_pairs[0][0]), float(latency_pairs[0][1])

    backoff_pairs = list(retry.get("backoff_ms") or [])

    for i in range(n_instances):
        inst_start = starts[i]
        attempts = attempt_counts[i]
        inst_ctx = bind_flow_instance_context(flow_id, state, seg_start_min, i)

        probe_host = probe_hosts[i % len(probe_hosts)] if probe_hosts else ""
        t_attempt_start = inst_start

        for a in range(1, attempts + 1):
            p50 = base_p50 * lm_p50
            p95 = base_p95 * lm_p95

            overrides_main: Dict[str, Any] = {}
            if flow_id.startswith("dns"):
                overrides_main["qname"] = inst_ctx.get("qname", "cloud.example")
                if "fail" in flow_id:
                    overrides_main["err"] = inst_ctx.get("err", "no_route")
            if flow_id.startswith("http"):
                overrides_main["url"] = inst_ctx.get("url", "https://www.example")
                if "fail" in flow_id:
                    overrides_main["err"] = inst_ctx.get("err", "no_route")

            cap_ms: Optional[int] = None
            timing_field: Optional[str] = None
            if "rtt_ms" in templates[main_ref].vars:
                timing_field = "rtt_ms"
                _, hi = domain_range(templates[main_ref].vars["rtt_ms"])
                cap_ms = hi
            elif "ttfb_ms" in templates[main_ref].vars:
                timing_field = "ttfb_ms"
                _, hi = domain_range(templates[main_ref].vars["ttfb_ms"])
                cap_ms = hi
            elif "waited_ms" in templates[main_ref].vars:
                timing_field = "waited_ms"
                _, hi = domain_range(templates[main_ref].vars["waited_ms"])
                cap_ms = hi

            lat_ms = sample_lognormal_ms(p50, p95, f"lat:{state}:{flow_id}:{seg_start_min}:{i}:{a}", cap_ms=cap_ms)
            if timing_field is not None and should_emit_main_for_attempt(a, attempts):
                lo, hi = domain_range(templates[main_ref].vars[timing_field])
                lat_ms = clamp_int(lat_ms, lo, hi)
                overrides_main[timing_field] = int(lat_ms)

            t_end = round_dt_to_ms(t_attempt_start + timedelta(milliseconds=int(lat_ms)))

            # Terminal-per-instance main outcome log
            if should_emit_main_for_attempt(a, attempts):
                emit(
                    rows,
                    t_end,
                    main_ref,
                    state,
                    probe_host,
                    f"flow:{state}:{flow_id}:{seg_start_min}:{i}:main:{a}",
                    overrides=overrides_main,
                )

            # Retry/backoff + retrying log: emitted once per retry (attempts 2..A), anchored at the end of the prior attempt
            if a < attempts:
                bo_pair = backoff_pairs[a - 1] if a - 1 < len(backoff_pairs) else backoff_pairs[-1]
                bo_p50, bo_p95 = float(bo_pair[0]), float(bo_pair[1])

                bo_dom = templates[retry_ref].vars.get("backoff_ms")
                bo_cap: Optional[int] = None
                if bo_dom:
                    _, hi = domain_range(bo_dom)
                    bo_cap = hi

                backoff_ms = sample_lognormal_ms(
                    bo_p50, bo_p95, f"bo:{state}:{flow_id}:{seg_start_min}:{i}:{a}", cap_ms=bo_cap, min_ms=1
                )
                if bo_dom:
                    lo, hi = domain_range(bo_dom)
                    backoff_ms = clamp_int(backoff_ms, lo, hi)

                overrides_retry = {
                    "kind": inst_ctx.get("kind", choose_from_list(["dns", "http"], f"kind:{flow_id}:{i}")),
                    "attempt": a + 1,
                    "backoff_ms": int(backoff_ms),
                }
                emit(
                    rows,
                    t_end,
                    retry_ref,
                    state,
                    probe_host,
                    f"flow:{state}:{flow_id}:{seg_start_min}:{i}:retry:{a+1}",
                    overrides=overrides_retry,
                )

                t_attempt_start = round_dt_to_ms(t_end + timedelta(milliseconds=int(backoff_ms)))


def simulate_flows(rows: List[LogRow]) -> None:
    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    for flow in SYSTEM["flows"]["n"]["req"]:
        simulate_flow_instances(rows, "n", flow, int(n_start), int(n_end), latency_mult={"p50": 1.0, "p95": 1.0})

    for seg_start, seg_end, it in failure_segments():
        for flow in SYSTEM["flows"]["f"]["req"]:
            lm = it.latency_mult.get(flow["id"], {"p50": 1.0, "p95": 1.0})
            simulate_flow_instances(rows, "f", flow, int(seg_start), int(seg_end), latency_mult=lm)


def main() -> None:
    rows: List[LogRow] = []

    simulate_background(rows)
    simulate_flows(rows)
    simulate_one_shots(rows)

    rows.sort(key=lambda r: r[0])
    out = pd.DataFrame(
        {
            "timestamp": [fmt_ts(r[0]) for r in rows],
            "level": [r[1] for r in rows],
            "message": [r[2] for r in rows],
            "trace_id": [r[3] for r in rows],
            "service": [r[4] for r in rows],
            "host": [r[5] for r in rows],
        }
    )

    assert list(out.columns) == ["timestamp", "level", "message", "trace_id", "service", "host"]
    assert out["timestamp"].is_monotonic_increasing
    n_rows = len(out)
    assert 20000 <= n_rows <= 100000, f"Row count {n_rows} outside required range"

    out.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
