import re
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# ----------------------------
# Embedded executable spec
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "id": "kartotherian_maps_migration",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "ats_edge": {
            "svc": "ats",
            "hosts": ["ats1001.eqiad.wmnet", "ats1002.eqiad.wmnet"],
            "logs": {
                "ats_txn_200": {
                    "lvl": "INFO",
                    "msg": "txn={txn} {method} {url} status=200 pool=bm dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "txn": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET"]},
                        "url": {"k": "str", "v": "https://maps.wikimedia.org/* (tile/render request)"},
                        "dur_ms": {"k": "i", "v": [15, 2500]},
                        "bytes": {"k": "i", "v": [800, 20000]},
                    },
                },
                "ats_txn_fail_k8s": {
                    "lvl": "WARN",
                    "msg": "txn={txn} {method} {url} status={status} pool=k8s err={err} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "txn": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET"]},
                        "url": {"k": "str", "v": "https://maps.wikimedia.org/* (tile/render request)"},
                        "status": {"k": "ch", "v": [502, 503]},
                        "err": {"k": "ch", "v": ["connect_timeout", "tls_handshake_timeout"]},
                        "dur_ms": {"k": "i", "v": [300, 8000]},
                        "bytes": {"k": "i", "v": [0, 1500]},
                    },
                },
                "ats_txn_fail_bm": {
                    "lvl": "WARN",
                    "msg": "txn={txn} {method} {url} status={status} pool=bm err={err} dur_ms={dur_ms} bytes={bytes}",
                    "vars": {
                        "txn": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET"]},
                        "url": {"k": "str", "v": "https://maps.wikimedia.org/* (tile/render request)"},
                        "status": {"k": "ch", "v": [503, 504]},
                        "err": {"k": "ch", "v": ["origin_timeout", "origin_busy"]},
                        "dur_ms": {"k": "i", "v": [1500, 25000]},
                        "bytes": {"k": "i", "v": [0, 2500]},
                    },
                },
                "ats_origin_pool_connect_fail": {
                    "lvl": "INFO",
                    "msg": "origin_pool connect_fail vip={vip} pool=k8s backend={backend} err={err} timeout_ms={timeout_ms}",
                    "vars": {
                        "vip": {"k": "ch", "v": ["kartotherian-ssl_443"]},
                        "backend": {
                            "k": "ch",
                            "v": [
                                "k8s-worker01.eqiad.wmnet",
                                "k8s-worker02.eqiad.wmnet",
                                "k8s-worker03.eqiad.wmnet",
                                "k8s-worker04.eqiad.wmnet",
                            ],
                        },
                        "err": {"k": "ch", "v": ["connect_timeout", "no_response"]},
                        "timeout_ms": {"k": "i", "v": [250, 2500]},
                    },
                },
            },
            "beh": {"n": [], "f": [{"id": "ats_origin_pool_connect_fail", "per_min": 0.6, "scope": "per_host"}]},
        },
        "lvs_lb": {
            "svc": "lvs-kartotherian",
            "hosts": ["lvs1019.eqiad.wmnet"],
            "logs": {
                "lvs_dispatch_bm": {
                    "lvl": "INFO",
                    "msg": "vip={vip} dispatch real={real} pool=bm conn_ms={conn_ms}",
                    "vars": {
                        "vip": {"k": "ch", "v": ["kartotherian-ssl_443"]},
                        "real": {"k": "ch", "v": ["maps1005.eqiad.wmnet", "maps1006.eqiad.wmnet", "maps1007.eqiad.wmnet", "maps1010.eqiad.wmnet"]},
                        "conn_ms": {"k": "i", "v": [0, 15]},
                    },
                },
                "lvs_dispatch_k8s": {
                    "lvl": "INFO",
                    "msg": "vip={vip} dispatch real={real} pool=k8s conn_ms={conn_ms}",
                    "vars": {
                        "vip": {"k": "ch", "v": ["kartotherian-ssl_443"]},
                        "real": {"k": "ch", "v": ["k8s-worker01.eqiad.wmnet", "k8s-worker02.eqiad.wmnet", "k8s-worker03.eqiad.wmnet", "k8s-worker04.eqiad.wmnet"]},
                        "conn_ms": {"k": "i", "v": [0, 15]},
                    },
                },
                "lvs_conntrack_gc": {
                    "lvl": "INFO",
                    "msg": "conntrack_gc scanned={scanned} freed={freed}",
                    "vars": {"scanned": {"k": "i", "v": [1000, 8000]}, "freed": {"k": "i", "v": [0, 800]}},
                },
            },
            "beh": {
                "n": [{"id": "lvs_conntrack_gc", "per_min": 0.15, "scope": "global"}],
                "f": [{"id": "lvs_conntrack_gc", "per_min": 0.25, "scope": "global"}],
            },
        },
        "kartotherian_bm": {
            "svc": "kartotherian",
            "hosts": ["maps1005.eqiad.wmnet", "maps1006.eqiad.wmnet", "maps1007.eqiad.wmnet", "maps1010.eqiad.wmnet"],
            "logs": {
                "krt_access_200": {
                    "lvl": "INFO",
                    "msg": "req={req} layer={layer} z={z} x={x} y={y} status=200 dur_ms={dur_ms}",
                    "vars": {
                        "req": {"k": "hex", "v": 16},
                        "layer": {"k": "ch", "v": ["osm-intl", "wikivoyage", "hillshade"]},
                        "z": {"k": "i", "v": [0, 18]},
                        "x": {"k": "i", "v": [0, 262144]},
                        "y": {"k": "i", "v": [0, 262144]},
                        "dur_ms": {"k": "i", "v": [10, 3000]},
                    },
                },
                "krt_render_timeout": {
                    "lvl": "WARN",
                    "msg": "req={req} layer={layer} z={z} x={x} y={y} timeout_ms={timeout_ms} waited_ms={waited_ms}",
                    "vars": {
                        "req": {"k": "hex", "v": 16},
                        "layer": {"k": "ch", "v": ["osm-intl", "wikivoyage", "hillshade"]},
                        "z": {"k": "i", "v": [0, 18]},
                        "x": {"k": "i", "v": [0, 262144]},
                        "y": {"k": "i", "v": [0, 262144]},
                        "timeout_ms": {"k": "i", "v": [3000, 15000]},
                        "waited_ms": {"k": "i", "v": [3000, 25000]},
                    },
                },
                "krt_worker_queue": {
                    "lvl": "WARN",
                    "msg": "workers_active={active} queue_depth={queue} drops_1m={drops}",
                    "vars": {
                        "active": {"k": "i", "v": [10, 300]},
                        "queue": {"k": "i", "v": [0, 800]},
                        "drops": {"k": "i", "v": [0, 400]},
                    },
                },
                "krt_restart": {"lvl": "INFO", "msg": "kartotherian restarted reason={reason}", "vars": {"reason": {"k": "ch", "v": ["rolling_restart"]}}},
            },
            "beh": {"n": [{"id": "krt_worker_queue", "per_min": 0.6, "scope": "per_host"}], "f": [{"id": "krt_worker_queue", "per_min": 1.4, "scope": "per_host"}]},
        },
        "kartotherian_k8s": {
            "svc": "kartotherian",
            "hosts": ["k8s-worker01.eqiad.wmnet", "k8s-worker02.eqiad.wmnet", "k8s-worker03.eqiad.wmnet", "k8s-worker04.eqiad.wmnet"],
            "logs": {
                "vip_presence_check": {
                    "lvl": "WARN",
                    "msg": "vip_check vip={vip} present_on_lo={present} svc={svc}",
                    "vars": {"vip": {"k": "ch", "v": ["kartotherian-ssl_443"]}, "svc": {"k": "ch", "v": ["kartotherian"]}},
                    "state_vars": {"n": {"present": {"k": "ch", "v": ["true"]}}, "f": {"present": {"k": "ch", "v": ["false"]}}},
                }
            },
            "beh": {"n": [], "f": [{"id": "vip_presence_check", "per_min": 0.4, "scope": "per_host"}]},
        },
        "prometheus": {
            "svc": "monitoring",
            "hosts": ["prometheus01.eqiad.wmnet"],
            "logs": {
                "scrape_summary": {
                    "lvl": "INFO",
                    "msg": "kartotherian_rps bm_rps={bm_rps} k8s_targets={k8s_targets} k8s_rps={k8s_rps} sample_window_s={win_s}",
                    "vars": {"bm_rps": {"k": "f", "v": [3.0, 22.0]}, "win_s": {"k": "i", "v": [30, 60]}},
                    "state_vars": {
                        "n": {"k8s_targets": {"k": "i", "v": [0, 0]}, "k8s_rps": {"k": "f", "v": [0.0, 0.0]}},
                        "f": {"k8s_targets": {"k": "i", "v": [4, 4]}, "k8s_rps": {"k": "f", "v": [0.0, 0.0]}},
                    },
                }
            },
            "beh": {"n": [{"id": "scrape_summary", "per_min": 1.0, "scope": "global"}], "f": [{"id": "scrape_summary", "per_min": 1.0, "scope": "global"}]},
        },
        "pybal": {
            "svc": "pybal",
            "hosts": ["lvs1019.eqiad.wmnet"],
            "logs": {
                "pybal_backend_down": {
                    "lvl": "CRITICAL",
                    "msg": "service={service} server={server} state=down reason={reason} pooled=yes",
                    "vars": {
                        "service": {"k": "ch", "v": ["kartotherian-ssl_443", "kartotherian-k8s-ssl_6543"]},
                        "server": {
                            "k": "ch",
                            "v": [
                                "maps1005.eqiad.wmnet",
                                "maps1006.eqiad.wmnet",
                                "maps1007.eqiad.wmnet",
                                "maps1010.eqiad.wmnet",
                                "k8s-worker01.eqiad.wmnet",
                                "k8s-worker02.eqiad.wmnet",
                                "k8s-worker03.eqiad.wmnet",
                                "k8s-worker04.eqiad.wmnet",
                            ],
                        },
                        "reason": {"k": "ch", "v": ["healthcheck_timeout", "no_response"]},
                    },
                }
            },
            "beh": {"n": [{"id": "pybal_backend_down", "per_min": 0.03, "scope": "global"}], "f": [{"id": "pybal_backend_down", "per_min": 1.0, "scope": "global"}]},
        },
        "alertmanager": {
            "svc": "monitoring",
            "hosts": ["alert01.eqiad.wmnet"],
            "logs": {
                "alert_firing": {
                    "lvl": "WARN",
                    "msg": "ALERT name={name} severity={severity} source={source} status=firing",
                    "vars": {
                        "name": {"k": "ch", "v": ["ATSBackendErrorsHigh", "PyBalBackendsHealthCritical"]},
                        "severity": {"k": "ch", "v": ["page, critical".split(", ")[0], "critical"]},
                        "source": {"k": "ch", "v": ["ats_edge", "pybal"]},
                    },
                }
            },
            "beh": {"n": [{"id": "alert_firing", "per_min": 0.02, "scope": "global"}], "f": [{"id": "alert_firing", "per_min": 0.25, "scope": "global"}]},
        },
        "ops_console": {
            "svc": "ops",
            "hosts": ["ops01.eqiad.wmnet"],
            "logs": {
                "ops_change": {
                    "lvl": "INFO",
                    "msg": "user={user} action={action} service={service} details={details}",
                    "vars": {
                        "user": {"k": "ch", "v": ["elukey"]},
                        "action": {"k": "ch", "v": ["pool_in", "pool_out", "deploy", "roll_restart"]},
                        "service": {"k": "ch", "v": ["kartotherian"]},
                        "details": {"k": "str", "v": "freeform short description (hosts/counts)"},
                    },
                }
            },
            "beh": {"n": [], "f": []},
        },
    },
    "flows": {
        "n": {
            "map_tile_render_bm": {
                "rpm": 600.0,
                "emit": ["lvs_lb.lvs_dispatch_bm", "kartotherian_bm.krt_access_200", "ats_edge.ats_txn_200"],
                "latency_ms": [[1, 4], [20, 140], [25, 220]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            }
        },
        "f": {
            "map_tile_ok_bm": {
                "rpm": 580.0,
                "emit": ["lvs_lb.lvs_dispatch_bm", "kartotherian_bm.krt_access_200", "ats_edge.ats_txn_200"],
                "latency_ms": [[1, 5], [25, 250], [40, 500]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            "map_tile_k8s_connect_fail": {
                "rpm": 2.0,
                "emit": ["lvs_lb.lvs_dispatch_k8s", "ats_edge.ats_txn_fail_k8s"],
                "latency_ms": [[1, 5], [400, 6000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            "map_tile_bm_overload_timeout": {
                "rpm": 20.0,
                "emit": ["lvs_lb.lvs_dispatch_bm", "kartotherian_bm.krt_render_timeout", "ats_edge.ats_txn_fail_bm"],
                "latency_ms": [[1, 6], [3000, 15000], [6000, 22000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "incident_2025_02_17_maps_kartotherian_migration",
    "time": {"total_minutes": 36, "phases": {"n": {"start_min": 0, "end_min": 18}, "f": {"start_min": 18, "end_min": 36}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 18,
                    "rate_multipliers": {"map_tile_k8s_connect_fail": 0.0, "map_tile_bm_overload_timeout": 0.0, "alertmanager.alert_firing": 0.0},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "ops_console.ops_change", "count": 1, "hosts": ["ops01.eqiad.wmnet"]}],
                },
                {"order": 2, "at_min": 22, "rate_multipliers": {}, "latency_multipliers": {}, "one_shots": [{"ref": "ops_console.ops_change", "count": 1, "hosts": ["ops01.eqiad.wmnet"]}]},
                {
                    "order": 3,
                    "at_min": 24,
                    "rate_multipliers": {
                        "map_tile_ok_bm": 0.85,
                        "map_tile_k8s_connect_fail": 10.0,
                        "map_tile_bm_overload_timeout": 0.6,
                        "ats_edge.ats_origin_pool_connect_fail": 2.5,
                        "kartotherian_bm.krt_worker_queue": 1.4,
                    },
                    "latency_multipliers": {"map_tile_ok_bm": {"p50": 1.2, "p95": 1.5}},
                    "one_shots": [{"ref": "ops_console.ops_change", "count": 1, "hosts": ["ops01.eqiad.wmnet"]}],
                },
                {
                    "order": 4,
                    "at_min": 28,
                    "rate_multipliers": {
                        "map_tile_ok_bm": 0.5,
                        "map_tile_k8s_connect_fail": 25.0,
                        "map_tile_bm_overload_timeout": 10.0,
                        "ats_edge.ats_origin_pool_connect_fail": 6.0,
                        "pybal.pybal_backend_down": 6.0,
                        "kartotherian_bm.krt_worker_queue": 2.2,
                        "alertmanager.alert_firing": 10.0,
                    },
                    "latency_multipliers": {"map_tile_ok_bm": {"p50": 1.6, "p95": 2.2}, "map_tile_bm_overload_timeout": {"p50": 1.6, "p95": 2.3}},
                    "one_shots": [],
                },
                {
                    "order": 5,
                    "at_min": 33,
                    "rate_multipliers": {
                        "map_tile_ok_bm": 0.95,
                        "map_tile_k8s_connect_fail": 0.3,
                        "map_tile_bm_overload_timeout": 0.3,
                        "ats_edge.ats_origin_pool_connect_fail": 0.7,
                        "pybal.pybal_backend_down": 2.0,
                        "kartotherian_bm.krt_worker_queue": 1.05,
                        "alertmanager.alert_firing": 0.0,
                    },
                    "latency_multipliers": {"map_tile_ok_bm": {"p50": 1.05, "p95": 1.15}, "map_tile_bm_overload_timeout": {"p50": 1.1, "p95": 1.2}},
                    "one_shots": [
                        {"ref": "ops_console.ops_change", "count": 2, "hosts": ["ops01.eqiad.wmnet"]},
                        {"ref": "kartotherian_bm.krt_restart", "count": 4, "hosts": ["maps1005.eqiad.wmnet", "maps1006.eqiad.wmnet", "maps1007.eqiad.wmnet", "maps1010.eqiad.wmnet"]},
                    ],
                },
            ]
        }
    },
}

# ----------------------------
# Deterministic simulator
# ----------------------------

SEED = 1337
random.seed(SEED)
rng = np.random.default_rng(SEED)

BASE_TIME = datetime(2025, 2, 17, 0, 0, 0, tzinfo=timezone.utc)

PLACEHOLDER_RE = re.compile(r"{([a-zA-Z0-9_]+)}")


def dt_at_min(minute: float) -> datetime:
    return BASE_TIME + timedelta(minutes=float(minute))


def isoformat_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def stable_round(x: float) -> int:
    if x <= 0:
        return 0
    return int(math.floor(x + 0.5))


def rand_hex_len(n: int) -> str:
    if n <= 0:
        return ""
    chunks = []
    while len("".join(chunks)) < n:
        v = int(rng.integers(0, 2**64, dtype=np.uint64))
        chunks.append(f"{v:016x}")
    return ("".join(chunks))[:n]


def sample_lognormal_ms(p50_ms: float, p95_ms: float, soft_cap: float) -> float:
    p50 = max(0.001, float(p50_ms))
    p95 = max(p50 * 1.001, float(p95_ms))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.645
    val = float(rng.lognormal(mean=mu, sigma=max(1e-6, sigma)))
    if soft_cap > 0:
        val = min(val, soft_cap)
    return max(0.0, val)


def schedule_times(start_dt: datetime, end_dt: datetime, n: int, jitter_ms: int = 300) -> List[datetime]:
    if n <= 0:
        return []
    span_s = (end_dt - start_dt).total_seconds()
    if span_s <= 0:
        return [start_dt for _ in range(n)]
    times: List[datetime] = []
    step = span_s / n
    for i in range(n):
        base = start_dt + timedelta(seconds=(i + 0.5) * step)
        jitter = (float(rng.random()) - 0.5) * (jitter_ms / 1000.0)
        t = base + timedelta(seconds=jitter)
        if t < start_dt:
            t = start_dt
        if t >= end_dt:
            t = end_dt - timedelta(milliseconds=1)
        times.append(t)
    return times


def get_log_template(component_id: str, log_id: str) -> Dict[str, Any]:
    return SYSTEM["components"][component_id]["logs"][log_id]


def get_var_spec(template: Dict[str, Any], var: str, state: str) -> Optional[Dict[str, Any]]:
    sv = template.get("state_vars", {})
    if state in sv and var in sv[state]:
        return sv[state][var]
    return template.get("vars", {}).get(var)


def get_int_range_for_var(component_id: str, log_id: str, var: str, state: str) -> Optional[Tuple[int, int]]:
    template = get_log_template(component_id, log_id)
    spec = get_var_spec(template, var, state)
    if not spec:
        return None
    if spec.get("k") != "i":
        return None
    lo, hi = int(spec["v"][0]), int(spec["v"][1])
    return lo, hi


def sample_var(spec: Dict[str, Any]) -> Any:
    k = spec["k"]
    v = spec["v"]
    if k == "ch":
        return v[int(rng.integers(0, len(v)))]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return int(rng.integers(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return float(rng.uniform(lo, hi))
    if k == "hex":
        return rand_hex_len(int(v))
    if k == "uuid":
        h = rand_hex_len(32)
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"
    if k == "ip":
        return "127.0.0.1"
    if k == "str":
        return str(v)
    return str(v)


def render_message(component_id: str, log_id: str, state: str, overrides: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
    template = get_log_template(component_id, log_id)
    msg = template["msg"]
    overrides = overrides or {}
    needed = set(PLACEHOLDER_RE.findall(msg))
    values: Dict[str, Any] = {}
    for var in needed:
        if var in overrides:
            values[var] = overrides[var]
            continue
        spec = get_var_spec(template, var, state)
        if spec is None:
            values[var] = ""
            continue
        values[var] = sample_var(spec)

    for k, val in list(values.items()):
        if isinstance(val, float):
            values[k] = f"{val:.2f}"
        else:
            values[k] = str(val)
    rendered = msg
    for k, v in values.items():
        rendered = rendered.replace("{" + k + "}", v)
    return template["lvl"], rendered


def emit_row(rows: List[Dict[str, Any]], ts: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append({"ts": ts, "timestamp": "", "level": level, "message": message, "trace_id": trace_id, "service": service or "", "host": host or ""})


def build_failure_intervals() -> List[Dict[str, Any]]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

    boundaries = [f_start] + sorted({e["at_min"] for e in events if f_start < e["at_min"] < f_end}) + [f_end]

    intervals: List[Dict[str, Any]] = []
    flow_mult: Dict[str, float] = {}
    bg_mult: Dict[str, float] = {}
    lat_mult: Dict[str, Dict[str, float]] = {}

    idx = 0
    while idx < len(events) and events[idx]["at_min"] == f_start:
        ev = events[idx]
        for k, v in ev.get("rate_multipliers", {}).items():
            if "." in k:
                bg_mult[k] = float(v)
            else:
                flow_mult[k] = float(v)
        for k, v in ev.get("latency_multipliers", {}).items():
            lat_mult[k] = {"p50": float(v.get("p50", 1.0)), "p95": float(v.get("p95", 1.0))}
        idx += 1

    for bi in range(len(boundaries) - 1):
        start_m = boundaries[bi]
        end_m = boundaries[bi + 1]

        if start_m != f_start:
            while idx < len(events) and events[idx]["at_min"] == start_m:
                ev = events[idx]
                for k, v in ev.get("rate_multipliers", {}).items():
                    if "." in k:
                        bg_mult[k] = float(v)
                    else:
                        flow_mult[k] = float(v)
                for k, v in ev.get("latency_multipliers", {}).items():
                    lat_mult[k] = {"p50": float(v.get("p50", 1.0)), "p95": float(v.get("p95", 1.0))}
                idx += 1

        intervals.append({"start_min": start_m, "end_min": end_m, "flow_mult": dict(flow_mult), "bg_mult": dict(bg_mult), "lat_mult": {k: dict(v) for k, v in lat_mult.items()}})
    return intervals


FAILURE_INTERVALS = build_failure_intervals()


def find_failure_interval_for_ts(ts: datetime) -> Optional[Dict[str, Any]]:
    rel_min = (ts - BASE_TIME).total_seconds() / 60.0
    for itv in FAILURE_INTERVALS:
        if itv["start_min"] <= rel_min < itv["end_min"]:
            return itv
    return None


def effective_failure_flow_rpm(flow_id: str, itv: Dict[str, Any]) -> float:
    base = float(SYSTEM["flows"]["f"][flow_id]["rpm"])
    mult = float(itv["flow_mult"].get(flow_id, 1.0))
    return base * mult


def compute_bm_rps_for_ts(ts: datetime, state: str) -> float:
    if state == "n":
        rpm = float(SYSTEM["flows"]["n"]["map_tile_render_bm"]["rpm"])
        rps = rpm / 60.0
    else:
        itv = find_failure_interval_for_ts(ts) or FAILURE_INTERVALS[-1]
        ok_rpm = effective_failure_flow_rpm("map_tile_ok_bm", itv)
        overload_rpm = effective_failure_flow_rpm("map_tile_bm_overload_timeout", itv)
        rps = (ok_rpm + overload_rpm) / 60.0

    noise = (float(rng.random()) - 0.5) * 0.6
    return max(3.0, min(22.0, rps + noise))


def pick_pybal_server(ts: datetime) -> str:
    rel_min = (ts - BASE_TIME).total_seconds() / 60.0
    bm = SYSTEM["components"]["kartotherian_bm"]["hosts"]
    k8s = SYSTEM["components"]["kartotherian_k8s"]["hosts"]
    if rel_min < 28:
        p_bm = 0.10
    elif rel_min < 33:
        p_bm = 0.60
    else:
        p_bm = 0.20
    if float(rng.random()) < p_bm:
        return bm[int(rng.integers(0, len(bm)))]
    return k8s[int(rng.integers(0, len(k8s)))]


def build_queue_metrics(ts: datetime, state: str, severity: float) -> Dict[str, int]:
    severity = max(0.0, float(severity))
    if state == "n":
        active = int(min(300, max(10, 40 + rng.integers(0, 40))))
        queue = int(min(800, max(0, rng.integers(0, 30))))
        drops = int(min(400, max(0, rng.integers(0, 3))))
    else:
        active = int(min(300, max(10, 90 + int(severity * 30) + rng.integers(0, 80))))
        queue = int(min(800, max(0, int(severity * 120) + rng.integers(20, 220))))
        drops = int(min(400, max(0, int(severity * 40) + rng.integers(0, 90))))
    return {"active": active, "queue": queue, "drops": drops}


def render_background_message(component_id: str, log_id: str, state: str, ts: datetime, meta: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
    meta = meta or {}
    overrides: Dict[str, Any] = {}

    if component_id == "prometheus" and log_id == "scrape_summary":
        overrides["bm_rps"] = compute_bm_rps_for_ts(ts, state)
        overrides["win_s"] = 60

    elif component_id == "kartotherian_bm" and log_id == "krt_worker_queue":
        severity = float(meta.get("severity", 1.0))
        overrides.update(build_queue_metrics(ts, state, severity))

    elif component_id == "pybal" and log_id == "pybal_backend_down":
        server = pick_pybal_server(ts) if state == "f" else SYSTEM["components"]["kartotherian_bm"]["hosts"][0]
        svc = "kartotherian-k8s-ssl_6543" if server.startswith("k8s-") else "kartotherian-ssl_443"
        reason = "healthcheck_timeout" if float(rng.random()) < 0.7 else "no_response"
        overrides.update({"server": server, "service": svc, "reason": reason})

    elif component_id == "alertmanager" and log_id == "alert_firing":
        if float(rng.random()) < 0.55:
            overrides.update({"name": "ATSBackendErrorsHigh", "source": "ats_edge", "severity": "page"})
        else:
            overrides.update({"name": "PyBalBackendsHealthCritical", "source": "pybal", "severity": "critical"})

    elif component_id == "lvs_lb" and log_id == "lvs_conntrack_gc":
        if state == "n":
            overrides.update({"scanned": int(rng.integers(1000, 4500)), "freed": int(rng.integers(0, 120))})
        else:
            overrides.update({"scanned": int(rng.integers(2500, 8000)), "freed": int(rng.integers(50, 650))})

    level, msg = render_message(component_id, log_id, state, overrides=overrides)
    return level, msg


def _per_step_min_ms(ref: str) -> int:
    # Enforce coherence for logs that carry observed timing fields (dur_ms / waited_ms).
    if ref == "kartotherian_bm.krt_access_200":
        return 10
    if ref == "kartotherian_bm.krt_render_timeout":
        return 3000
    # ATS final log's dur_ms is total; per-step minimum can be near-zero.
    return 0


def simulate_flow_instance(rows: List[Dict[str, Any]], flow_state: str, flow_id: str, start_ts: datetime, controls: Optional[Dict[str, Any]] = None) -> None:
    flow = SYSTEM["flows"][flow_state][flow_id]
    emit_chain: List[str] = flow["emit"]
    lat_pairs: List[List[float]] = flow["latency_ms"]
    assert len(emit_chain) == len(lat_pairs)

    lat_mult = {"p50": 1.0, "p95": 1.0}
    if flow_state == "f" and controls is not None:
        lm = controls.get("lat_mult", {}).get(flow_id, lat_mult)
        lat_mult = {"p50": float(lm.get("p50", 1.0)), "p95": float(lm.get("p95", 1.0))}

    # Host stickiness per component in this request chain.
    ats_host = SYSTEM["components"]["ats_edge"]["hosts"][int(rng.integers(0, 2))]
    lvs_host = SYSTEM["components"]["lvs_lb"]["hosts"][0]
    bm_hosts = SYSTEM["components"]["kartotherian_bm"]["hosts"]
    k8s_hosts = SYSTEM["components"]["kartotherian_k8s"]["hosts"]

    uses_bm_backend = any(ref.startswith("kartotherian_bm.") for ref in emit_chain) or any(ref == "lvs_lb.lvs_dispatch_bm" for ref in emit_chain)
    uses_k8s_backend = any(ref == "lvs_lb.lvs_dispatch_k8s" for ref in emit_chain)

    backend_host = None
    if uses_bm_backend:
        backend_host = bm_hosts[int(rng.integers(0, len(bm_hosts)))]
    elif uses_k8s_backend:
        backend_host = k8s_hosts[int(rng.integers(0, len(k8s_hosts)))]

    # Stable per-request identifiers.
    req_id = rand_hex_len(16)
    txn_id = rand_hex_len(16)
    layer = ["osm-intl", "wikivoyage", "hillshade"][int(rng.integers(0, 3))]
    z = int(rng.integers(0, 19))
    x = int(rng.integers(0, 262145))
    y = int(rng.integers(0, 262145))

    # Sample per-step delays, then convert to integer ms and enforce template/domain coherence.
    deltas_ms: List[int] = []
    per_step_min: List[int] = []
    for ref, (p50, p95) in zip(emit_chain, lat_pairs):
        sp50 = float(p50) * lat_mult["p50"]
        sp95 = float(p95) * lat_mult["p95"]
        sampled = sample_lognormal_ms(sp50, sp95, soft_cap=3.0 * sp95)
        ms = int(round(sampled))
        # Ref-specific caps consistent with templates.
        if ref.startswith("lvs_lb.lvs_dispatch"):
            ms = min(ms, 15)
        if ref == "kartotherian_bm.krt_render_timeout":
            ms = min(ms, 25000)
        deltas_ms.append(max(0, ms))
        per_step_min.append(_per_step_min_ms(ref))

    # Enforce per-step minimums (prevents waited_ms/dur_ms placeholders from exceeding timestamp gaps).
    for i in range(len(deltas_ms)):
        if deltas_ms[i] < per_step_min[i]:
            deltas_ms[i] = per_step_min[i]

    # Bind total duration range from the final ATS log template (if present).
    total_min, total_max = 0, 10**9
    final_ref = emit_chain[-1]
    if final_ref.startswith("ats_edge."):
        comp_id, log_id = final_ref.split(".", 1)
        dur_range = get_int_range_for_var(comp_id, log_id, "dur_ms", flow_state)
        if dur_range is not None:
            total_min, total_max = dur_range

    # Adjust total duration (integer ms) by reducing/expanding deltas while respecting per-step minimums.
    def total() -> int:
        return int(sum(deltas_ms))

    # Cap to total_max by shaving from the tail without violating per-step mins.
    cur_total = total()
    if cur_total > total_max:
        excess = cur_total - total_max
        for j in range(len(deltas_ms) - 1, -1, -1):
            if excess <= 0:
                break
            reducible = deltas_ms[j] - per_step_min[j]
            if reducible <= 0:
                continue
            take = min(reducible, excess)
            deltas_ms[j] -= take
            excess -= take

    # Lift to total_min by adding to the last step (keeps known-at-this-time ordering and timestamps coherent).
    cur_total = total()
    if cur_total < total_min:
        add = total_min - cur_total
        deltas_ms[-1] += add

    # Final rounding safety (in case caps + mins interact).
    cur_total = total()
    if cur_total > total_max:
        excess = cur_total - total_max
        for j in range(len(deltas_ms) - 1, -1, -1):
            if excess <= 0:
                break
            reducible = deltas_ms[j] - per_step_min[j]
            if reducible <= 0:
                continue
            take = min(reducible, excess)
            deltas_ms[j] -= take
            excess -= take

    total_ms = total()

    # Emit logs in order using these exact deltas.
    t = start_ts
    for idx, ref in enumerate(emit_chain):
        comp_id, log_id = ref.split(".", 1)
        delta_ms = int(deltas_ms[idx])
        t = t + timedelta(milliseconds=delta_ms)

        service = SYSTEM["components"][comp_id]["svc"]
        if comp_id == "ats_edge":
            host = ats_host
        elif comp_id == "lvs_lb":
            host = lvs_host
        elif comp_id == "kartotherian_bm":
            host = backend_host or bm_hosts[0]
        elif comp_id == "kartotherian_k8s":
            host = backend_host or k8s_hosts[0]
        else:
            hosts = SYSTEM["components"][comp_id].get("hosts", [])
            host = hosts[0] if hosts else ""

        overrides: Dict[str, Any] = {}

        if comp_id == "lvs_lb" and log_id in ("lvs_dispatch_bm", "lvs_dispatch_k8s"):
            overrides["vip"] = "kartotherian-ssl_443"
            overrides["real"] = backend_host or (bm_hosts[0] if log_id == "lvs_dispatch_bm" else k8s_hosts[0])
            overrides["conn_ms"] = int(max(0, min(15, delta_ms)))

        elif comp_id == "kartotherian_bm" and log_id == "krt_access_200":
            overrides.update({"req": req_id, "layer": layer, "z": z, "x": x, "y": y, "dur_ms": int(max(10, min(3000, delta_ms)))})

        elif comp_id == "kartotherian_bm" and log_id == "krt_render_timeout":
            waited_ms = int(max(3000, min(25000, delta_ms)))
            timeout_ms = int(min(15000, max(3000, int(round(waited_ms * 0.8)))))
            overrides.update({"req": req_id, "layer": layer, "z": z, "x": x, "y": y, "waited_ms": waited_ms, "timeout_ms": timeout_ms})

        elif comp_id == "ats_edge" and log_id == "ats_txn_200":
            bytes_out = int(rng.integers(800, 20001))
            overrides.update({"txn": txn_id, "dur_ms": total_ms, "bytes": bytes_out})

        elif comp_id == "ats_edge" and log_id == "ats_txn_fail_k8s":
            if total_ms >= 4500:
                status = 503
                err = "connect_timeout"
            else:
                status = 502
                err = "tls_handshake_timeout"
            bytes_out = 0 if float(rng.random()) < 0.85 else int(rng.integers(1, 700))
            overrides.update({"txn": txn_id, "dur_ms": total_ms, "bytes": bytes_out, "status": status, "err": err})

        elif comp_id == "ats_edge" and log_id == "ats_txn_fail_bm":
            waited = None
            for j, r2 in enumerate(emit_chain):
                if r2 == "kartotherian_bm.krt_render_timeout":
                    waited = int(deltas_ms[j])
                    break
            waited = waited if waited is not None else int(round(total_ms * 0.7))
            if waited >= 12000:
                status = 504
                err = "origin_timeout"
            else:
                status = 503
                err = "origin_busy"
            bytes_out = 0 if float(rng.random()) < 0.9 else int(rng.integers(1, 900))
            overrides.update({"txn": txn_id, "dur_ms": total_ms, "bytes": bytes_out, "status": status, "err": err})

        level, msg = render_message(comp_id, log_id, flow_state, overrides=overrides)
        emit_row(rows, t, level, msg, trace_id="", service=service, host=host)


def schedule_background(rows: List[Dict[str, Any]], state: str, start_min: int, end_min: int, itv_meta: Optional[Dict[str, Any]] = None) -> None:
    itv_meta = itv_meta or {}
    start_dt = dt_at_min(start_min)
    end_dt = dt_at_min(end_min)

    for comp_id, comp in SYSTEM["components"].items():
        emissions = comp.get("beh", {}).get(state, [])
        if not emissions:
            continue

        for em in emissions:
            log_id = em["id"]
            per_min = float(em["per_min"])
            scope = em.get("scope", "per_host")

            if state == "f":
                mult = float(itv_meta.get("bg_mult", {}).get(f"{comp_id}.{log_id}", 1.0))
                eff_per_min = per_min * mult
                severity = mult
            else:
                eff_per_min = per_min
                severity = 1.0

            if scope == "global":
                n = stable_round(eff_per_min * (end_min - start_min))
                times = schedule_times(start_dt, end_dt, n, jitter_ms=350)
                host = comp["hosts"][0] if comp.get("hosts") else ""
                for ts in times:
                    lvl, msg = render_background_message(comp_id, log_id, state, ts, meta={"severity": severity})
                    emit_row(rows, ts, lvl, msg, trace_id="", service=comp.get("svc", ""), host=host)
            else:
                hosts = comp.get("hosts", [])
                for h in hosts:
                    n = stable_round(eff_per_min * (end_min - start_min))
                    times = schedule_times(start_dt, end_dt, n, jitter_ms=350)
                    for ts in times:
                        lvl, msg = render_background_message(comp_id, log_id, state, ts, meta={"severity": severity})
                        emit_row(rows, ts, lvl, msg, trace_id="", service=comp.get("svc", ""), host=h)


def schedule_flows_normal(rows: List[Dict[str, Any]]) -> None:
    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    flow_id = "map_tile_render_bm"
    rpm = float(SYSTEM["flows"]["n"][flow_id]["rpm"])
    count = stable_round(rpm * (n_end - n_start))
    starts = schedule_times(dt_at_min(n_start), dt_at_min(n_end), count, jitter_ms=900)
    for ts in starts:
        simulate_flow_instance(rows, "n", flow_id, ts, controls=None)


def schedule_flows_failure(rows: List[Dict[str, Any]]) -> None:
    for itv in FAILURE_INTERVALS:
        start_dt = dt_at_min(itv["start_min"])
        end_dt = dt_at_min(itv["end_min"])
        dur_min = float(itv["end_min"] - itv["start_min"])
        controls = {"lat_mult": itv["lat_mult"]}

        for flow_id, flow in SYSTEM["flows"]["f"].items():
            base_rpm = float(flow["rpm"])
            mult = float(itv["flow_mult"].get(flow_id, 1.0))
            eff_rpm = base_rpm * mult
            count = stable_round(eff_rpm * dur_min)
            if count <= 0:
                continue
            starts = schedule_times(start_dt, end_dt, count, jitter_ms=900)
            for ts in starts:
                simulate_flow_instance(rows, "f", flow_id, ts, controls=controls)


def emit_one_shots(rows: List[Dict[str, Any]]) -> None:
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for ev in events:
        at_min = int(ev["at_min"])
        base_dt = dt_at_min(at_min)

        for one in ev.get("one_shots", []):
            ref = one["ref"]
            count = int(one["count"])
            allowed_hosts = list(one.get("hosts", []))
            comp_id, log_id = ref.split(".", 1)
            comp = SYSTEM["components"][comp_id]
            service = comp.get("svc", "")
            state = "f"

            def ops_overrides(i: int) -> Dict[str, Any]:
                if at_min == 18:
                    return {"user": "elukey", "action": "pool_in", "service": "kartotherian", "details": "pool_in k8s workers into LVS: 4 nodes (k8s-worker01..04) [misconfigured VIP on lo]"}
                if at_min == 22:
                    return {"user": "elukey", "action": "deploy", "service": "kartotherian", "details": "deploy/config change on k8s kartotherian (no effect on LVS VIP reachability)"}
                if at_min == 24:
                    return {"user": "elukey", "action": "pool_out", "service": "kartotherian", "details": "pool_out several maps bare-metal hosts (capacity reduction) [migration step]"}
                if at_min == 33:
                    if i == 0:
                        return {"user": "elukey", "action": "pool_in", "service": "kartotherian", "details": "re-pool bare-metal capacity into LVS service (restore maps hosts)"}
                    return {"user": "elukey", "action": "roll_restart", "service": "kartotherian", "details": "rolling restart kartotherian on bare-metal maps hosts (maps1005/6/7/1010)"}
                return {"user": "elukey", "action": "deploy", "service": "kartotherian", "details": "ops change"}

            for i in range(count):
                if allowed_hosts:
                    host = allowed_hosts[i] if i < len(allowed_hosts) else allowed_hosts[int(rng.integers(0, len(allowed_hosts)))]
                else:
                    host = comp["hosts"][0] if comp.get("hosts") else ""

                jitter_s = float(rng.uniform(0.0, 50.0))
                ts = base_dt + timedelta(seconds=jitter_s)

                overrides: Dict[str, Any] = {}
                if ref == "ops_console.ops_change":
                    overrides = ops_overrides(i)
                elif ref == "kartotherian_bm.krt_restart":
                    overrides = {"reason": "rolling_restart"}

                lvl, msg = render_message(comp_id, log_id, state, overrides=overrides)
                emit_row(rows, ts, lvl, msg, trace_id="", service=service, host=host)


def main() -> None:
    rows: List[Dict[str, Any]] = []

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    schedule_background(rows, "n", n_start, n_end, itv_meta=None)
    schedule_flows_normal(rows)

    for itv in FAILURE_INTERVALS:
        schedule_background(rows, "f", int(itv["start_min"]), int(itv["end_min"]), itv_meta=itv)

    schedule_flows_failure(rows)
    emit_one_shots(rows)

    rows.sort(key=lambda r: r["ts"])
    for r in rows:
        r["timestamp"] = isoformat_ms(r["ts"])
        del r["ts"]

    df = pd.DataFrame(rows, columns=["timestamp", "level", "message", "trace_id", "service", "host"])
    df.to_csv("logs.csv", index=False)

    n_rows = len(df)
    if not (20000 <= n_rows <= 100000):
        raise RuntimeError(f"Row count out of target range: {n_rows}")


if __name__ == "__main__":
    main()
