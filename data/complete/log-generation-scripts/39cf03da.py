import hashlib
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


SYSTEM: Dict[str, Any] = {
    "id": "cdn_edge_transit_failover",
    "tracing": {"on": True, "trace_id_len": 32},
    "components": {
        "edge_proxy": {
            "svc": "edge-proxy",
            "hosts": ["edge-ams-1", "edge-ams-2", "edge-fra-1"],
            "logs": {
                "access_hit_200": {
                    "lvl": "INFO",
                    "msg": "req {req_id} pop={pop} {method} {host}{uri} -> 200 hit dur={dur_ms}ms bytes={bytes}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "pop": {"k": "ch", "v": ["ams", "fra", "lhr", "iad"]},
                        "method": {"k": "ch", "v": ["GET", "HEAD"]},
                        "host": {"k": "ch", "v": ["app1.example", "app2.example", "api.example"]},
                        "uri": {"k": "ch", "v": ["/", "/login", "/api/v1/items", "/static/app.js"]},
                        "dur_ms": {"k": "i", "v": [5, 80]},
                        "bytes": {"k": "i", "v": [200, 90000]},
                    },
                },
                "origin_fetch_start": {
                    "lvl": "DEBUG",
                    "msg": "origin connect start pop={pop} host={host} provider={provider} origin_ip={origin_ip} timeout={timeout_ms}ms",
                    "vars": {
                        "pop": {"k": "ch", "v": ["ams", "fra", "lhr", "iad"]},
                        "host": {"k": "ch", "v": ["app1.example", "app2.example", "api.example"]},
                        "provider": {"k": "ch", "v": ["telia", "cogent", "level3"]},
                        "origin_ip": {"k": "ip", "v": None},
                        "timeout_ms": {"k": "i", "v": [2000, 7000]},
                    },
                },
                "access_miss_200": {
                    "lvl": "INFO",
                    "msg": "req {req_id} pop={pop} {method} {host}{uri} -> 200 miss provider={provider} dur={dur_ms}ms bytes={bytes}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "pop": {"k": "ch", "v": ["ams", "fra", "lhr", "iad"]},
                        "method": {"k": "ch", "v": ["GET", "HEAD"]},
                        "host": {"k": "ch", "v": ["app1.example", "app2.example", "api.example"]},
                        "uri": {"k": "ch", "v": ["/", "/login", "/api/v1/items", "/static/app.js"]},
                        "provider": {"k": "ch", "v": ["telia", "cogent", "level3"]},
                        "dur_ms": {"k": "i", "v": [60, 3500]},
                        "bytes": {"k": "i", "v": [200, 90000]},
                    },
                },
                "origin_fetch_start_telia": {
                    "lvl": "DEBUG",
                    "msg": "origin connect start pop={pop} host={host} provider=telia origin_ip={origin_ip} timeout={timeout_ms}ms",
                    "vars": {
                        "pop": {"k": "ch", "v": ["ams", "fra", "lhr", "iad"]},
                        "host": {"k": "ch", "v": ["app1.example", "app2.example", "api.example"]},
                        "origin_ip": {"k": "ip", "v": None},
                        "timeout_ms": {"k": "i", "v": [2000, 7000]},
                    },
                },
                "access_miss_200_telia": {
                    "lvl": "INFO",
                    "msg": "req {req_id} pop={pop} {method} {host}{uri} -> 200 miss provider=telia dur={dur_ms}ms bytes={bytes}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "pop": {"k": "ch", "v": ["ams", "fra", "lhr", "iad"]},
                        "method": {"k": "ch", "v": ["GET", "HEAD"]},
                        "host": {"k": "ch", "v": ["app1.example", "app2.example", "api.example"]},
                        "uri": {"k": "ch", "v": ["/", "/login", "/api/v1/items", "/static/app.js"]},
                        "dur_ms": {"k": "i", "v": [120, 8000]},
                        "bytes": {"k": "i", "v": [200, 90000]},
                    },
                },
                "origin_fetch_start_alt": {
                    "lvl": "DEBUG",
                    "msg": "origin connect start pop={pop} host={host} provider={provider} origin_ip={origin_ip} timeout={timeout_ms}ms",
                    "vars": {
                        "pop": {"k": "ch", "v": ["ams", "fra", "lhr", "iad"]},
                        "host": {"k": "ch", "v": ["app1.example", "app2.example", "api.example"]},
                        "provider": {"k": "ch", "v": ["cogent", "level3"]},
                        "origin_ip": {"k": "ip", "v": None},
                        "timeout_ms": {"k": "i", "v": [2000, 7000]},
                    },
                },
                "access_miss_200_alt": {
                    "lvl": "INFO",
                    "msg": "req {req_id} pop={pop} {method} {host}{uri} -> 200 miss provider={provider} dur={dur_ms}ms bytes={bytes}",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "pop": {"k": "ch", "v": ["ams", "fra", "lhr", "iad"]},
                        "method": {"k": "ch", "v": ["GET", "HEAD"]},
                        "host": {"k": "ch", "v": ["app1.example", "app2.example", "api.example"]},
                        "uri": {"k": "ch", "v": ["/", "/login", "/api/v1/items", "/static/app.js"]},
                        "provider": {"k": "ch", "v": ["cogent", "level3"]},
                        "dur_ms": {"k": "i", "v": [120, 9000]},
                        "bytes": {"k": "i", "v": [200, 90000]},
                    },
                },
                "origin_connect_timeout_telia": {
                    "lvl": "ERROR",
                    "msg": "origin connect timeout pop={pop} host={host} provider=telia waited={waited_ms}ms",
                    "vars": {
                        "pop": {"k": "ch", "v": ["ams", "fra", "lhr", "iad"]},
                        "host": {"k": "ch", "v": ["app1.example", "app2.example", "api.example"]},
                        "waited_ms": {"k": "i", "v": [2000, 12000]},
                    },
                },
                "access_522_telia": {
                    "lvl": "INFO",
                    "msg": "req {req_id} pop={pop} {method} {host}{uri} -> 522 origin_unreachable provider=telia dur={dur_ms}ms",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "pop": {"k": "ch", "v": ["ams", "fra", "lhr", "iad"]},
                        "method": {"k": "ch", "v": ["GET", "HEAD"]},
                        "host": {"k": "ch", "v": ["app1.example", "app2.example", "api.example"]},
                        "uri": {"k": "ch", "v": ["/", "/login", "/api/v1/items", "/static/app.js"]},
                        "dur_ms": {"k": "i", "v": [3000, 15000]},
                    },
                },
                "origin_connect_timeout_alt": {
                    "lvl": "ERROR",
                    "msg": "origin connect timeout pop={pop} host={host} provider={provider} waited={waited_ms}ms",
                    "vars": {
                        "pop": {"k": "ch", "v": ["ams", "fra", "lhr", "iad"]},
                        "host": {"k": "ch", "v": ["app1.example", "app2.example", "api.example"]},
                        "provider": {"k": "ch", "v": ["cogent", "level3"]},
                        "waited_ms": {"k": "i", "v": [2000, 12000]},
                    },
                },
                "access_522_alt": {
                    "lvl": "INFO",
                    "msg": "req {req_id} pop={pop} {method} {host}{uri} -> 522 origin_unreachable provider={provider} dur={dur_ms}ms",
                    "vars": {
                        "req_id": {"k": "uuid", "v": None},
                        "pop": {"k": "ch", "v": ["ams", "fra", "lhr", "iad"]},
                        "method": {"k": "ch", "v": ["GET", "HEAD"]},
                        "host": {"k": "ch", "v": ["app1.example", "app2.example", "api.example"]},
                        "uri": {"k": "ch", "v": ["/", "/login", "/api/v1/items", "/static/app.js"]},
                        "provider": {"k": "ch", "v": ["cogent", "level3"]},
                        "dur_ms": {"k": "i", "v": [3000, 15000]},
                    },
                },
                "cache_metrics": {
                    "lvl": "INFO",
                    "msg": "cache stats pop={pop} hit_rate={hit_rate}% evictions={evictions} fill_q={fill_q}",
                    "vars": {
                        "pop": {"k": "ch", "v": ["ams", "fra", "lhr", "iad"]},
                        "hit_rate": {"k": "i", "v": [60, 98]},
                        "evictions": {"k": "i", "v": [0, 250]},
                        "fill_q": {"k": "i", "v": [0, 80]},
                    },
                },
                "egress_queue_high": {
                    "lvl": "WARN",
                    "msg": "egress queueing pop={pop} queue_depth={queue_depth} p95_wait_ms={p95_wait_ms}",
                    "vars": {
                        "pop": {"k": "ch", "v": ["ams", "fra", "lhr", "iad"]},
                        "queue_depth": {"k": "i", "v": [50, 4000]},
                        "p95_wait_ms": {"k": "i", "v": [20, 1200]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "cache_metrics", "per_min": 0.5, "scope": "per_host"}],
                "f": [
                    {"id": "cache_metrics", "per_min": 0.5, "scope": "per_host"},
                    {"id": "egress_queue_high", "per_min": 4.0, "scope": "per_host"},
                ],
            },
        },
        "egress_router": {
            "svc": "egress-router",
            "hosts": ["rtr-ams-1", "rtr-ams-2"],
            "logs": {
                "iface_counters": {
                    "lvl": "INFO",
                    "msg": "egress counters iface={iface} provider={provider} tx_mbps={tx_mbps} drops={drops}",
                    "vars": {
                        "iface": {"k": "ch", "v": ["ae2"]},
                        "provider": {"k": "ch", "v": ["cogent", "level3"]},
                    },
                    "state_vars": {
                        "n": {"tx_mbps": {"k": "i", "v": [1, 12]}, "drops": {"k": "i", "v": [0, 10]}},
                        "f": {"tx_mbps": {"k": "i", "v": [3, 25]}, "drops": {"k": "i", "v": [0, 40]}},
                    },
                },
                "iface_counters_telia": {
                    "lvl": "INFO",
                    "msg": "egress counters iface={iface} provider=telia tx_mbps={tx_mbps} drops={drops}",
                    "vars": {"iface": {"k": "ch", "v": ["ae1"]}},
                    "state_vars": {
                        "n": {"tx_mbps": {"k": "i", "v": [1, 12]}, "drops": {"k": "i", "v": [0, 10]}},
                        "f": {"tx_mbps": {"k": "i", "v": [2, 20]}, "drops": {"k": "i", "v": [300, 5000]}},
                    },
                },
                "admin_down_telia": {
                    "lvl": "INFO",
                    "msg": "admin down: provider=telia iface={iface} reason={reason}",
                    "vars": {"iface": {"k": "ch", "v": ["ae1"]}, "reason": {"k": "ch", "v": ["high_packet_loss"]}},
                },
                "policy_update": {
                    "lvl": "INFO",
                    "msg": "egress policy updated: prefer={prefer} avoid=telia",
                    "vars": {"prefer": {"k": "ch", "v": ["cogent", "level3"]}},
                },
            },
            "beh": {
                "n": [
                    {"id": "iface_counters", "per_min": 1.0, "scope": "per_host"},
                    {"id": "iface_counters_telia", "per_min": 1.0, "scope": "per_host"},
                ],
                "f": [
                    {"id": "iface_counters", "per_min": 1.0, "scope": "per_host"},
                    {"id": "iface_counters_telia", "per_min": 1.0, "scope": "per_host"},
                ],
            },
        },
        "net_monitor": {
            "svc": "net-monitor",
            "hosts": ["netmon-1"],
            "logs": {
                "probe_ok": {
                    "lvl": "INFO",
                    "msg": "probe provider={provider} dst={dst} loss={loss_pct}% rtt={rtt_ms}ms",
                    "vars": {
                        "provider": {"k": "ch", "v": ["cogent", "level3"]},
                        "dst": {"k": "ch", "v": ["8.8.8.8", "1.1.1.1", "93.184.216.34"]},
                        "loss_pct": {"k": "f", "v": [0.0, 2.0]},
                        "rtt_ms": {"k": "i", "v": [10, 180]},
                    },
                },
                "probe_ok_telia": {
                    "lvl": "INFO",
                    "msg": "probe provider=telia dst={dst} loss={loss_pct}% rtt={rtt_ms}ms",
                    "vars": {
                        "dst": {"k": "ch", "v": ["8.8.8.8", "1.1.1.1", "93.184.216.34"]},
                        "loss_pct": {"k": "f", "v": [0.0, 2.0]},
                        "rtt_ms": {"k": "i", "v": [10, 180]},
                    },
                },
                "probe_high_loss_telia": {
                    "lvl": "WARN",
                    "msg": "probe high loss provider=telia dst={dst} loss={loss_pct}% rtt={rtt_ms}ms",
                    "vars": {
                        "dst": {"k": "ch", "v": ["8.8.8.8", "1.1.1.1", "93.184.216.34"]},
                        "loss_pct": {"k": "f", "v": [20.0, 85.0]},
                        "rtt_ms": {"k": "i", "v": [40, 350]},
                    },
                },
                "packet_loss_alarm": {
                    "lvl": "CRITICAL",
                    "msg": "alarm: high packet loss provider=telia pop={pop} loss={loss_pct}%",
                    "vars": {"pop": {"k": "ch", "v": ["ams", "fra", "lhr", "iad"]}, "loss_pct": {"k": "f", "v": [30.0, 85.0]}},
                },
            },
            "beh": {
                "n": [
                    {"id": "probe_ok", "per_min": 8.0, "scope": "global"},
                    {"id": "probe_ok_telia", "per_min": 4.0, "scope": "global"},
                ],
                "f": [
                    {"id": "probe_ok", "per_min": 6.0, "scope": "global"},
                    {"id": "probe_high_loss_telia", "per_min": 12.0, "scope": "global"},
                ],
            },
        },
        "traffic_engineering": {
            "svc": "noc-te",
            "hosts": ["noc-1"],
            "logs": {
                "noc_heartbeat": {
                    "lvl": "DEBUG",
                    "msg": "noc-te heartbeat leader={leader}",
                    "vars": {"leader": {"k": "ch", "v": ["noc-1"]}},
                },
                "mitigation_command": {
                    "lvl": "INFO",
                    "msg": "runbook execute action={action} provider=telia requested_by={user}",
                    "vars": {"action": {"k": "ch", "v": ["drain_ports", "disable_bgp_sessions"]}, "user": {"k": "ch", "v": ["oncall1", "oncall2"]}},
                },
            },
            "beh": {"n": [{"id": "noc_heartbeat", "per_min": 0.2, "scope": "per_host"}], "f": [{"id": "noc_heartbeat", "per_min": 0.2, "scope": "per_host"}]},
        },
        "analytics": {
            "svc": "analytics",
            "hosts": ["analytics-1"],
            "logs": {
                "http_522_summary_normal": {
                    "lvl": "INFO",
                    "msg": "http error summary status=522 count={count} pct={pct}% scope=global",
                    "vars": {"count": {"k": "i", "v": [0, 3]}, "pct": {"k": "f", "v": [0.0, 0.1]}},
                },
                "http_522_summary_high": {
                    "lvl": "INFO",
                    "msg": "http error summary status=522 count={count} pct={pct}% scope=global",
                    "vars": {"count": {"k": "i", "v": [30, 80]}, "pct": {"k": "f", "v": [5.0, 15.0]}},
                },
                "http_522_summary_reduced": {
                    "lvl": "INFO",
                    "msg": "http error summary status=522 count={count} pct={pct}% scope=global",
                    "vars": {"count": {"k": "i", "v": [5, 20]}, "pct": {"k": "f", "v": [1.0, 5.0]}},
                },
            },
            "beh": {
                "n": [{"id": "http_522_summary_normal", "per_min": 1.0, "scope": "global"}],
                "f": [
                    {"id": "http_522_summary_high", "per_min": 1.0, "scope": "global"},
                    {"id": "http_522_summary_reduced", "per_min": 1.0, "scope": "global"},
                ],
            },
        },
    },
    "flows": {
        "n": {
            "http_cache_hit": {
                "rpm": 350.0,
                "emit": ["edge_proxy.access_hit_200"],
                "latency_ms": [[5, 35]],
                "trace": False,
            },
            "http_cache_miss_ok": {
                "rpm": 150.0,
                "emit": ["edge_proxy.origin_fetch_start", "edge_proxy.access_miss_200"],
                "latency_ms": [[1, 10], [80, 2200]],
                "trace": True,
            },
        },
        "f": {
            "http_cache_hit_f": {
                "rpm": 330.0,
                "emit": ["edge_proxy.access_hit_200"],
                "latency_ms": [[5, 60]],
                "trace": False,
            },
            "http_cache_miss_ok_telia": {
                "rpm": 90.0,
                "emit": ["edge_proxy.origin_fetch_start_telia", "edge_proxy.access_miss_200_telia"],
                "latency_ms": [[1, 20], [150, 7000]],
                "trace": True,
            },
            "http_cache_miss_ok_alt": {
                "rpm": 30.0,
                "emit": ["edge_proxy.origin_fetch_start_alt", "edge_proxy.access_miss_200_alt"],
                "latency_ms": [[1, 20], [180, 8000]],
                "trace": True,
            },
            "http_cache_miss_522_telia": {
                "rpm": 50.0,
                "emit": ["edge_proxy.origin_fetch_start_telia", "edge_proxy.origin_connect_timeout_telia", "edge_proxy.access_522_telia"],
                "latency_ms": [[1, 20], [2500, 12000], [1, 20]],
                "trace": True,
            },
            "http_cache_miss_522_alt": {
                "rpm": 5.0,
                "emit": ["edge_proxy.origin_fetch_start_alt", "edge_proxy.origin_connect_timeout_alt", "edge_proxy.access_522_alt"],
                "latency_ms": [[1, 20], [2500, 12000], [1, 20]],
                "trace": True,
            },
        },
    },
}


SCENARIO: Dict[str, Any] = {
    "id": "jun20_telia_packet_loss_522",
    "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}}},
    "events": [
        {
            "order": 1,
            "at_min": 25,
            "rate_multipliers": {
                "http_cache_miss_522_alt": 0.0,
                "edge_proxy.egress_queue_high": 0.0,
                "analytics.http_522_summary_reduced": 0.0,
                "net_monitor.probe_ok": 0.5,
            },
            "latency_multipliers": {
                "http_cache_miss_ok_telia": {"p50": 1.0, "p95": 1.0},
                "http_cache_miss_522_telia": {"p50": 1.0, "p95": 1.0},
            },
            "one_shots": [{"ref": "net_monitor.packet_loss_alarm", "count": 1, "hosts": ["netmon-1"]}],
        },
        {
            "order": 2,
            "at_min": 35,
            "rate_multipliers": {
                "http_cache_miss_ok_telia": 0.0,
                "http_cache_miss_522_telia": 0.0,
                "http_cache_miss_ok_alt": 4.0,
                "http_cache_miss_522_alt": 2.0,
                "http_cache_hit_f": 0.9,
                "net_monitor.probe_high_loss_telia": 0.0,
                "analytics.http_522_summary_high": 0.0,
                "analytics.http_522_summary_reduced": 1.0,
                "edge_proxy.egress_queue_high": 1.0,
                "egress_router.iface_counters_telia": 0.0,
            },
            "latency_multipliers": {
                "http_cache_miss_ok_alt": {"p50": 1.2, "p95": 1.4},
                "http_cache_miss_522_alt": {"p50": 1.1, "p95": 1.2},
            },
            "one_shots": [
                {"ref": "traffic_engineering.mitigation_command", "count": 1, "hosts": ["noc-1"]},
                {"ref": "egress_router.admin_down_telia", "count": 2, "hosts": ["rtr-ams-1", "rtr-ams-2"]},
                {"ref": "egress_router.policy_update", "count": 1, "hosts": ["rtr-ams-1"]},
            ],
        },
    ],
}


BASE_SEED = 1337
BASE_TIME = datetime(2026, 6, 20, 12, 0, 0, tzinfo=timezone.utc)


def stable_hash_int(s: str) -> int:
    h = hashlib.md5(s.encode("utf-8")).hexdigest()
    return int(h[:8], 16)


def clamp_int(x: int, lo: int, hi: int) -> int:
    return lo if x < lo else hi if x > hi else x


def pop_from_host(host: str) -> str:
    for p in ("ams", "fra", "lhr", "iad"):
        if p in host:
            return p
    return "ams"


def subrng(key: str) -> random.Random:
    return random.Random((BASE_SEED << 16) + stable_hash_int(key))


def allocate_counts_per_minute(rate_per_min: float, minutes: int, key: str) -> List[int]:
    if minutes <= 0 or rate_per_min <= 0.0:
        return [0] * max(0, minutes)
    rr = subrng(f"alloc|{key}")
    acc = rr.random()  # deterministic offset to de-phase series across hosts/log_ids
    out: List[int] = []
    for _ in range(minutes):
        acc += rate_per_min
        c = int(acc)
        acc -= c
        out.append(c)
    return out


def schedule_within_minute(minute_start_ms: int, count: int, key: str) -> List[int]:
    if count <= 0:
        return []
    rr = subrng(f"sched|{key}|{minute_start_ms}|{count}")
    spacing_ms = 60000 / count
    jitter_cap = int(min(80, max(0, spacing_ms * 0.2)))
    times: List[int] = []
    for i in range(count):
        base_pos = int((i + 0.5) * 60000 / count)
        jitter = int((rr.random() * 2.0 - 1.0) * jitter_cap) if jitter_cap > 0 else 0
        pos = base_pos + jitter
        if pos < 0:
            pos = 0
        elif pos > 59999:
            pos = 59999
        times.append(minute_start_ms + pos)
    times.sort()
    return times


def sample_between_int(p50: float, p95: float, rr: random.Random, skew: float = 1.35) -> int:
    if p95 < p50:
        p50, p95 = p95, p50
    u = rr.random()
    x = p50 + (p95 - p50) * (u**skew)
    return max(1, int(round(x)))


def sample_var(spec: Dict[str, Any], rr: random.Random) -> Any:
    k = spec["k"]
    v = spec.get("v")
    if k == "ch":
        return v[int(rr.random() * len(v))] if v else ""
    if k == "i":
        lo, hi = v
        return lo if lo == hi else int(lo + rr.random() * (hi - lo + 1))
    if k == "f":
        lo, hi = v
        x = lo + rr.random() * (hi - lo)
        return round(x, 1)
    if k == "hex":
        n = int(v)
        return "".join(rr.choice("0123456789abcdef") for _ in range(n))
    if k == "uuid":
        bits = rr.getrandbits(128)
        return str(uuid.UUID(int=bits))
    if k == "ip":
        return f"203.0.113.{1 + int(rr.random() * 254)}"
    if k == "str":
        return "x"
    return ""


def get_var_spec(comp_id: str, log_id: str, var_name: str, state: str) -> Optional[Dict[str, Any]]:
    tmpl = SYSTEM["components"][comp_id]["logs"][log_id]
    if "vars" in tmpl and var_name in tmpl["vars"]:
        return tmpl["vars"][var_name]
    if "state_vars" in tmpl and state in tmpl["state_vars"] and var_name in tmpl["state_vars"][state]:
        return tmpl["state_vars"][state][var_name]
    return None


def build_failure_intervals() -> List[Dict[str, Any]]:
    f = SCENARIO["time"]["phases"]["f"]
    start = f["start_min"]
    end = f["end_min"]
    events = sorted(SCENARIO["events"], key=lambda e: (e["at_min"], e["order"]))

    flow_rate_mult: Dict[str, float] = {}
    bg_rate_mult: Dict[str, float] = {}
    flow_lat_mult: Dict[str, Dict[str, float]] = {}

    intervals: List[Dict[str, Any]] = []
    for i, ev in enumerate(events):
        at = ev["at_min"]
        for k, m in ev.get("rate_multipliers", {}).items():
            if "." in k:
                bg_rate_mult[k] = float(m)
            else:
                flow_rate_mult[k] = float(m)
        for fid, lm in ev.get("latency_multipliers", {}).items():
            flow_lat_mult[fid] = {"p50": float(lm["p50"]), "p95": float(lm["p95"])}

        interval_start = at
        interval_end = events[i + 1]["at_min"] if i + 1 < len(events) else end
        if interval_end <= interval_start:
            continue
        intervals.append(
            {
                "start_min": interval_start,
                "end_min": interval_end,
                "flow_rate_mult": dict(flow_rate_mult),
                "bg_rate_mult": dict(bg_rate_mult),
                "flow_lat_mult": dict(flow_lat_mult),
                "one_shots": ev.get("one_shots", []),
                "event_at_min": at,
            }
        )

    if not intervals or intervals[0]["start_min"] > start:
        intervals.insert(
            0,
            {
                "start_min": start,
                "end_min": intervals[0]["start_min"] if intervals else end,
                "flow_rate_mult": {},
                "bg_rate_mult": {},
                "flow_lat_mult": {},
                "one_shots": [],
                "event_at_min": start,
            },
        )
    return intervals


@dataclass
class Row:
    ts_ms: int
    level: str
    message: str
    trace_id: str
    service: str
    host: str


def render_log(comp_id: str, log_id: str, state: str, rr: random.Random, ctx: Dict[str, Any], host: str, trace_id: str) -> Tuple[str, str]:
    comp = SYSTEM["components"][comp_id]
    tmpl = comp["logs"][log_id]
    values: Dict[str, Any] = {}

    for name, spec in tmpl.get("vars", {}).items():
        values[name] = ctx[name] if name in ctx else sample_var(spec, rr)

    for name, spec in tmpl.get("state_vars", {}).get(state, {}).items():
        values[name] = ctx[name] if name in ctx else sample_var(spec, rr)

    if "pop" in values and "pop" not in ctx:
        values["pop"] = pop_from_host(host)

    msg = tmpl["msg"].format(**values)
    return tmpl["lvl"], msg


def make_trace_id(rr: random.Random) -> str:
    return "".join(rr.choice("0123456789abcdef") for _ in range(SYSTEM["tracing"]["trace_id_len"]))


def plan_flow_instance(
    flow_id: str,
    flow_def: Dict[str, Any],
    state: str,
    start_ts_ms: int,
    edge_host: str,
    latency_mult: Dict[str, float],
    rr: random.Random,
) -> List[Tuple[int, str, str, str, str, str]]:
    emit_refs: List[str] = flow_def["emit"]
    lat_pairs: List[List[float]] = flow_def["latency_ms"]
    assert len(emit_refs) == len(lat_pairs)

    ctx: Dict[str, Any] = {}
    ctx["req_id"] = str(uuid.UUID(int=rr.getrandbits(128)))
    ctx["pop"] = pop_from_host(edge_host)
    ctx["method"] = "GET" if rr.random() < 0.86 else "HEAD"
    ctx["host"] = ["app1.example", "app2.example", "api.example"][int(rr.random() * 3)]
    ctx["uri"] = ["/", "/login", "/api/v1/items", "/static/app.js"][int(rr.random() * 4)]
    ctx["bytes"] = int(200 + rr.random() * (90000 - 200))

    if "ok_alt" in flow_id or "522_alt" in flow_id:
        ctx["provider"] = "cogent" if rr.random() < 0.62 else "level3"
    elif "telia" in flow_id and "alt" not in flow_id:
        ctx["provider"] = "telia"
    else:
        r = rr.random()
        ctx["provider"] = "telia" if r < 0.45 else ("cogent" if r < 0.75 else "level3")

    ctx["origin_ip"] = f"203.0.113.{10 + int(rr.random() * 200)}"

    delays: List[int] = []
    for p50, p95 in lat_pairs:
        p50_s = p50 * latency_mult.get("p50", 1.0)
        p95_s = p95 * latency_mult.get("p95", 1.0)
        delays.append(sample_between_int(p50_s, p95_s, rr))

    timeout_idx = None
    access_idx = None
    for j, ref in enumerate(emit_refs):
        _, lid = ref.split(".", 1)
        if "origin_connect_timeout" in lid:
            timeout_idx = j
        if lid.startswith("access_"):
            access_idx = j

    if timeout_idx is not None:
        comp_id, lid = emit_refs[timeout_idx].split(".", 1)
        waited_spec = get_var_spec(comp_id, lid, "waited_ms", state)
        if waited_spec and waited_spec["k"] == "i":
            lo, hi = waited_spec["v"]
            delays[timeout_idx] = clamp_int(delays[timeout_idx], lo, hi)

    if access_idx is not None:
        comp_id, lid = emit_refs[access_idx].split(".", 1)
        dur_spec = get_var_spec(comp_id, lid, "dur_ms", state)
        if dur_spec and dur_spec["k"] == "i":
            dur_lo, dur_hi = dur_spec["v"]
            total = sum(delays[: access_idx + 1])
            if access_idx == 0:
                adj_idx = 0
            elif timeout_idx is not None and access_idx >= 2:
                adj_idx = timeout_idx
            else:
                adj_idx = access_idx

            if total < dur_lo:
                delays[adj_idx] += (dur_lo - total)
            elif total > dur_hi:
                delays[adj_idx] = max(1, delays[adj_idx] - (total - dur_hi))

            if timeout_idx is not None and adj_idx == timeout_idx:
                comp_id2, lid2 = emit_refs[timeout_idx].split(".", 1)
                waited_spec2 = get_var_spec(comp_id2, lid2, "waited_ms", state)
                if waited_spec2 and waited_spec2["k"] == "i":
                    lo2, hi2 = waited_spec2["v"]
                    delays[timeout_idx] = clamp_int(delays[timeout_idx], lo2, hi2)

    for j, ref in enumerate(emit_refs):
        comp_id, lid = ref.split(".", 1)
        if lid.startswith("origin_fetch_start"):
            timeout_spec = get_var_spec(comp_id, lid, "timeout_ms", state)
            if timeout_spec and timeout_spec["k"] == "i":
                lo, hi = timeout_spec["v"]
                if timeout_idx is not None:
                    timeout_ms = int(round(delays[timeout_idx] * 0.9))
                else:
                    approx_total = sum(delays)
                    timeout_ms = int(round(approx_total * 1.3))
                ctx["timeout_ms"] = clamp_int(timeout_ms, lo, hi)

    trace_id = ""
    if SYSTEM["tracing"]["on"] and flow_def.get("trace", False):
        trace_id = make_trace_id(rr)

    out: List[Tuple[int, str, str, str, str, str]] = []
    t = start_ts_ms
    for j, ref in enumerate(emit_refs):
        comp_id, lid = ref.split(".", 1)
        t += delays[j]

        if "origin_connect_timeout" in lid:
            ctx["waited_ms"] = delays[j]
        if lid.startswith("access_") and "dur_ms" in SYSTEM["components"][comp_id]["logs"][lid].get("vars", {}):
            ctx["dur_ms"] = t - start_ts_ms

        lvl, msg = render_log(comp_id, lid, state, rr, ctx, host=edge_host, trace_id=trace_id)
        out.append((t, lvl, msg, trace_id, SYSTEM["components"][comp_id]["svc"], edge_host))
    return out


def emit_background_interval(rows: List[Row], state: str, start_min: int, end_min: int, bg_mult: Dict[str, float]) -> None:
    minutes = end_min - start_min
    for comp_id, comp in SYSTEM["components"].items():
        emits = comp.get("beh", {}).get(state, [])
        for em in emits:
            log_id = em["id"]
            base_rate = float(em["per_min"])
            scope = em.get("scope", "per_host")
            mult = 1.0
            if state == "f":
                mult = float(bg_mult.get(f"{comp_id}.{log_id}", 1.0))
            eff_rate = base_rate * mult
            if eff_rate <= 0.0:
                continue

            if scope == "global":
                counts = allocate_counts_per_minute(eff_rate, minutes, key=f"bg|{state}|{comp_id}.{log_id}")
                rr = subrng(f"bg_rr|{state}|{comp_id}.{log_id}|{start_min}-{end_min}")
                for mi, c in enumerate(counts):
                    if c <= 0:
                        continue
                    minute_idx = start_min + mi
                    minute_start_ms = minute_idx * 60000
                    ts_list = schedule_within_minute(minute_start_ms, c, key=f"bg|{comp_id}.{log_id}|m{minute_idx}")
                    for k, ts in enumerate(ts_list):
                        host = comp["hosts"][(k + minute_idx) % len(comp["hosts"])] if comp.get("hosts") else ""
                        lvl, msg = render_log(comp_id, log_id, state, rr, ctx={}, host=host, trace_id="")
                        rows.append(Row(ts, lvl, msg, "", comp.get("svc", "") or "", host))
            else:
                for host in comp.get("hosts", []):
                    counts = allocate_counts_per_minute(eff_rate, minutes, key=f"bg|{state}|{comp_id}.{log_id}|{host}")
                    rr = subrng(f"bg_rr|{state}|{comp_id}.{log_id}|{host}|{start_min}-{end_min}")
                    for mi, c in enumerate(counts):
                        if c <= 0:
                            continue
                        minute_idx = start_min + mi
                        minute_start_ms = minute_idx * 60000
                        ts_list = schedule_within_minute(minute_start_ms, c, key=f"bg|{comp_id}.{log_id}|{host}|m{minute_idx}")
                        for ts in ts_list:
                            ctx = {}
                            if "pop" in SYSTEM["components"][comp_id]["logs"][log_id].get("vars", {}):
                                ctx["pop"] = pop_from_host(host)
                            lvl, msg = render_log(comp_id, log_id, state, rr, ctx=ctx, host=host, trace_id="")
                            rows.append(Row(ts, lvl, msg, "", comp.get("svc", "") or "", host))


def emit_flows_interval(
    rows: List[Row],
    state: str,
    start_min: int,
    end_min: int,
    flow_rate_mult: Dict[str, float],
    flow_lat_mult: Dict[str, Dict[str, float]],
) -> None:
    minutes = end_min - start_min
    flows = SYSTEM["flows"][state]
    edge_hosts = SYSTEM["components"]["edge_proxy"]["hosts"]

    for flow_id, flow_def in flows.items():
        base_rpm = float(flow_def["rpm"])
        mult = 1.0
        if state == "f":
            mult = float(flow_rate_mult.get(flow_id, 1.0))
        eff_rpm = base_rpm * mult
        if eff_rpm <= 0.0:
            continue

        counts = allocate_counts_per_minute(eff_rpm, minutes, key=f"flow|{state}|{flow_id}|{start_min}-{end_min}")
        for mi, c in enumerate(counts):
            if c <= 0:
                continue
            minute_idx = start_min + mi
            minute_start_ms = minute_idx * 60000
            starts = schedule_within_minute(minute_start_ms, c, key=f"flow|{flow_id}|m{minute_idx}")
            for j, st in enumerate(starts):
                edge_host = edge_hosts[(minute_idx + j) % len(edge_hosts)]
                rr = subrng(f"flow_rr|{state}|{flow_id}|m{minute_idx}|i{j}|t{st}")
                latm = flow_lat_mult.get(flow_id, {"p50": 1.0, "p95": 1.0}) if state == "f" else {"p50": 1.0, "p95": 1.0}
                emissions = plan_flow_instance(flow_id, flow_def, state, st, edge_host, latm, rr)
                for ts_ms, lvl, msg, trace_id, svc, host in emissions:
                    rows.append(Row(ts_ms, lvl, msg, trace_id, svc, host))


def emit_one_shots(rows: List[Row], intervals: List[Dict[str, Any]]) -> None:
    for interval in intervals:
        at_min = interval["event_at_min"]
        for os in interval.get("one_shots", []):
            ref = os["ref"]
            count = int(os["count"])
            hosts = os.get("hosts", [])
            comp_id, log_id = ref.split(".", 1)
            comp = SYSTEM["components"][comp_id]
            rr = subrng(f"oneshot|{ref}|{at_min}")
            base_ms = at_min * 60000
            for i in range(count):
                host = ""
                if hosts:
                    host = hosts[i % len(hosts)]
                elif comp.get("hosts"):
                    host = comp["hosts"][i % len(comp["hosts"])]
                jitter = int(200 + rr.random() * 800) + i * 25
                ts = base_ms + clamp_int(jitter, 0, 59999)
                ctx = {}
                if "pop" in comp["logs"][log_id].get("vars", {}):
                    ctx["pop"] = "ams"
                lvl, msg = render_log(comp_id, log_id, "f", rr, ctx=ctx, host=host, trace_id="")
                rows.append(Row(ts, lvl, msg, "", comp.get("svc", "") or "", host))


def isoformat_ms_utc(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def main() -> None:
    # Ensure global RNG is seeded for verifier reproducibility expectations.
    # The simulator itself uses subrng() everywhere, but we seed anyway.
    random.seed(BASE_SEED)

    rows: List[Row] = []

    n = SCENARIO["time"]["phases"]["n"]
    emit_background_interval(rows, "n", n["start_min"], n["end_min"], bg_mult={})
    emit_flows_interval(rows, "n", n["start_min"], n["end_min"], flow_rate_mult={}, flow_lat_mult={})

    failure_intervals = build_failure_intervals()
    for interval in failure_intervals:
        s = interval["start_min"]
        e = interval["end_min"]
        emit_background_interval(rows, "f", s, e, bg_mult=interval["bg_rate_mult"])
        emit_flows_interval(rows, "f", s, e, flow_rate_mult=interval["flow_rate_mult"], flow_lat_mult=interval["flow_lat_mult"])

    emit_one_shots(rows, failure_intervals)

    rows.sort(key=lambda r: (r.ts_ms, r.service, r.host, r.level, r.message))
    timestamps = [isoformat_ms_utc(BASE_TIME + timedelta(milliseconds=r.ts_ms)) for r in rows]
    df = pd.DataFrame(
        {
            "timestamp": timestamps,
            "level": [r.level for r in rows],
            "message": [r.message for r in rows],
            "trace_id": [r.trace_id for r in rows],
            "service": [r.service for r in rows],
            "host": [r.host for r in rows],
        }
    )

    assert list(df.columns) == ["timestamp", "level", "message", "trace_id", "service", "host"]
    assert 20000 <= len(df) <= 100000, f"Row count {len(df)} outside target range"
    assert df["timestamp"].is_monotonic_increasing

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
