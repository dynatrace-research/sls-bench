import hashlib
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# Deterministic seeds
random.seed(0)
np.random.seed(0)

SYSTEM: Dict[str, Any] = {
    "id": "git_push_gateway",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["edge_router"], "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "edge_router": {
            "svc": "edge-router",
            "hosts": ["router-01", "router-02", "router-03"],
            "logs": {
                "http_req": {
                    "lvl": "INFO",
                    "msg": "ingress {method} {route} host={host} trace_id={trace_id} client_ip={client_ip}",
                    "vars": {
                        "method": {"k": "ch", "v": ["POST"]},
                        "route": {"k": "ch", "v": ["/git-receive-pack"]},
                        "host": {"k": "ch", "v": ["git.example.com"]},
                        "trace_id": {"k": "hex", "v": 32},
                        "client_ip": {"k": "ip", "v": "203.0.113.0/24"},
                    },
                },
                "http_resp_200": {
                    "lvl": "INFO",
                    "msg": "egress status=200 {route} trace_id={trace_id} bytes={bytes} dur_ms={dur_ms}",
                    "vars": {
                        "route": {"k": "ch", "v": ["/git-receive-pack"]},
                        "trace_id": {"k": "hex", "v": 32},
                        "bytes": {"k": "i", "v": [800, 60000]},
                        "dur_ms": {"k": "i", "v": [500, 3600000]},
                    },
                },
                "http_resp_401": {
                    "lvl": "WARN",
                    "msg": "egress status=401 {route} trace_id={trace_id} bytes={bytes} dur_ms={dur_ms}",
                    "vars": {
                        "route": {"k": "ch", "v": ["/git-receive-pack"]},
                        "trace_id": {"k": "hex", "v": 32},
                        "bytes": {"k": "i", "v": [200, 8000]},
                        "dur_ms": {"k": "i", "v": [50, 5000]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "git_gateway": {
            "svc": "git-gateway",
            "hosts": ["git-01", "git-02", "git-03", "git-04"],
            "logs": {
                "push_received": {
                    "lvl": "INFO",
                    "msg": "push start app={app} user={user} proto=http trace_id={trace_id}",
                    "vars": {
                        "app": {"k": "ch", "v": ["acme-web", "acme-api", "demo-shop", "payments", "docs", "internal-tools"]},
                        "user": {"k": "ch", "v": ["alice", "bob", "carol", "ci-bot"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "auth_token_missing": {
                    "lvl": "WARN",
                    "msg": "cannot authenticate internal api call auth_mode=new trace_id={trace_id}",
                    "vars": {"trace_id": {"k": "hex", "v": 32}},
                },
                "push_complete_200": {
                    "lvl": "INFO",
                    "msg": "push complete result=ok trace_id={trace_id} dur_ms={dur_ms}",
                    "vars": {"trace_id": {"k": "hex", "v": 32}, "dur_ms": {"k": "i", "v": [500, 3600000]}},
                },
                "push_complete_401": {
                    "lvl": "WARN",
                    "msg": "push failed result=unauthorized upstream_status=401 trace_id={trace_id} dur_ms={dur_ms}",
                    "vars": {"trace_id": {"k": "hex", "v": 32}, "dur_ms": {"k": "i", "v": [50, 5000]}},
                },
                "proc_restart": {
                    "lvl": "INFO",
                    "msg": "process restart reason={reason} code_ver={code_ver} cfg_rev={cfg_rev}",
                    "vars": {
                        "reason": {"k": "ch", "v": ["killed_for_cfg_sync", "deploy_roll"]},
                        "code_ver": {"k": "ch", "v": ["2020-09-03.1"]},
                        "cfg_rev": {"k": "i", "v": [41, 42]},
                    },
                },
                "health_ok": {
                    "lvl": "INFO",
                    "msg": "health ok goroutines={goroutines} open_conns={open_conns}",
                    "vars": {"goroutines": {"k": "i", "v": [50, 450]}, "open_conns": {"k": "i", "v": [5, 250]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "health_ok", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "health_ok", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        "internal_auth_api": {
            "svc": "internal-auth-api",
            "hosts": ["api-01", "api-02"],
            "logs": {
                "auth_ok": {
                    "lvl": "INFO",
                    "msg": "access {endpoint} status=200 auth=ok trace_id={trace_id} dur_ms={dur_ms}",
                    "vars": {
                        "endpoint": {"k": "ch", "v": ["/v2/git/authorize"]},
                        "trace_id": {"k": "hex", "v": 32},
                        "dur_ms": {"k": "i", "v": [5, 120]},
                    },
                },
                "auth_denied": {
                    "lvl": "WARN",
                    "msg": "access {endpoint} status=401 auth=missing trace_id={trace_id} dur_ms={dur_ms}",
                    "vars": {
                        "endpoint": {"k": "ch", "v": ["/v2/git/authorize"]},
                        "trace_id": {"k": "hex", "v": 32},
                        "dur_ms": {"k": "i", "v": [2, 80]},
                    },
                },
                "auth_metrics": {
                    "lvl": "INFO",
                    "msg": "auth_metrics endpoint={endpoint} ok={ok} denied={denied} window_s={window_s}",
                    "vars": {
                        "endpoint": {"k": "ch", "v": ["/v2/git/authorize"]},
                        "window_s": {"k": "i", "v": [60, 60]},
                    },
                    "state_vars": {
                        "n": {"ok": {"k": "i", "v": [120, 190]}, "denied": {"k": "i", "v": [0, 3]}},
                        "f": {"ok": {"k": "i", "v": [80, 180]}, "denied": {"k": "i", "v": [15, 120]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "auth_metrics", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "auth_metrics", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        "deploy_orchestrator": {
            "svc": "deploy-orchestrator",
            "hosts": ["deploy-01"],
            "logs": {
                "batch_deploy_start": {
                    "lvl": "INFO",
                    "msg": "deploy start service=git-server batch={batch} code_ver={code_ver} cfg_required_rev={cfg_required_rev}",
                    "vars": {
                        "batch": {"k": "ch", "v": ["batch-a"]},
                        "code_ver": {"k": "ch", "v": ["2020-09-03.1"]},
                        "cfg_required_rev": {"k": "i", "v": [42, 42]},
                    },
                },
                "instance_poisoned": {
                    "lvl": "INFO",
                    "msg": "poisoned instance={instance} old_code_ver={old_code_ver} batch={batch}",
                    "vars": {
                        "instance": {"k": "ch", "v": ["git-01", "git-02", "git-03", "git-04"]},
                        "old_code_ver": {"k": "ch", "v": ["2020-08-20.4"]},
                        "batch": {"k": "ch", "v": ["batch-a"]},
                    },
                },
                "deploy_status": {
                    "lvl": "INFO",
                    "msg": "deploy status batch={batch} updated={updated} poisoned={poisoned} pending={pending}",
                    "vars": {
                        "batch": {"k": "ch", "v": ["batch-a"]},
                        "updated": {"k": "i", "v": [0, 4]},
                        "poisoned": {"k": "i", "v": [0, 4]},
                        "pending": {"k": "i", "v": [0, 4]},
                    },
                },
                "deploy_pause": {
                    "lvl": "WARN",
                    "msg": "deploy paused batch={batch} reason={reason}",
                    "vars": {"batch": {"k": "ch", "v": ["batch-a"]}, "reason": {"k": "ch", "v": ["elevated_401", "manual_hold"]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": [{"id": "deploy_status", "per_min": 1.0, "scope": "global"}]}},
        },
        "config_distributor": {
            "svc": "config-distributor",
            "hosts": ["cfg-01"],
            "logs": {
                "config_sync_trigger": {
                    "lvl": "INFO",
                    "msg": "config sync triggered service=git-server cfg_rev={cfg_rev} targets={targets}",
                    "vars": {"cfg_rev": {"k": "i", "v": [42, 42]}, "targets": {"k": "i", "v": [4, 4]}},
                },
                "config_sync_complete": {
                    "lvl": "INFO",
                    "msg": "config sync complete service=git-server cfg_rev={cfg_rev} updated={updated} failed={failed}",
                    "vars": {"cfg_rev": {"k": "i", "v": [42, 42]}, "updated": {"k": "i", "v": [3, 4]}, "failed": {"k": "i", "v": [0, 1]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "alerting": {
            "svc": "alerting",
            "hosts": ["mon-01"],
            "logs": {
                "alert_401_rate_high": {
                    "lvl": "CRITICAL",
                    "msg": "ALERT name=git_push_401_rate_high rate_pct={rate_pct} window_min={window_min}",
                    "vars": {"rate_pct": {"k": "f", "v": [10.0, 60.0]}, "window_min": {"k": "i", "v": [5, 5]}},
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    },
    "flows": {
        "n": {
            "req": [
                {
                    "id": "git_push_http_success",
                    "rpm": 300.0,
                    "emit": [
                        "edge_router.http_req",
                        "git_gateway.push_received",
                        "internal_auth_api.auth_ok",
                        "git_gateway.push_complete_200",
                        "edge_router.http_resp_200",
                    ],
                    "latency_ms": [[1, 3], [2, 8], [5, 40], [12000, 900000], [2, 8]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                }
            ]
        },
        "f": {
            "req": [
                {
                    "id": "git_push_http_ok",
                    "rpm": 240.0,
                    "emit": [
                        "edge_router.http_req",
                        "git_gateway.push_received",
                        "internal_auth_api.auth_ok",
                        "git_gateway.push_complete_200",
                        "edge_router.http_resp_200",
                    ],
                    "latency_ms": [[1, 3], [2, 10], [5, 60], [15000, 1200000], [2, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "git_push_http_401",
                    "rpm": 60.0,
                    "emit": [
                        "edge_router.http_req",
                        "git_gateway.push_received",
                        "git_gateway.auth_token_missing",
                        "internal_auth_api.auth_denied",
                        "git_gateway.push_complete_401",
                        "edge_router.http_resp_401",
                    ],
                    "latency_ms": [[1, 3], [2, 8], [1, 5], [2, 30], [10, 500], [1, 5]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "incident_2105_git_push_401_partial_deploy",
    "time": {"total_minutes": 40, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 40}}},
    "failure_events": [
        {
            "order": 1,
            "at_min": 20,
            "rate_multipliers": {
                "git_push_http_ok": 1.0,
                "git_push_http_401": 1.0,
                "deploy_orchestrator.deploy_status": 1.0,
            },
            "latency_multipliers": {},
            "one_shots": [
                {"ref": "deploy_orchestrator.batch_deploy_start", "count": 1, "hosts": ["deploy-01"]},
                {"ref": "deploy_orchestrator.instance_poisoned", "count": 2, "hosts": ["deploy-01"]},
            ],
        },
        {
            "order": 2,
            "at_min": 24,
            "rate_multipliers": {"git_push_http_ok": 0.75, "git_push_http_401": 2.0},
            "latency_multipliers": {"git_push_http_ok": {"p50": 1.2, "p95": 1.5}},
            "one_shots": [{"ref": "alerting.alert_401_rate_high", "count": 1, "hosts": ["mon-01"]}],
        },
        {
            "order": 3,
            "at_min": 30,
            "rate_multipliers": {
                "git_push_http_ok": 1.125,
                "git_push_http_401": 0.5,
                "deploy_orchestrator.deploy_status": 0.2,
            },
            "latency_multipliers": {"git_push_http_ok": {"p50": 1.0, "p95": 1.0}},
            "one_shots": [
                {"ref": "config_distributor.config_sync_trigger", "count": 1, "hosts": ["cfg-01"]},
                {"ref": "config_distributor.config_sync_complete", "count": 1, "hosts": ["cfg-01"]},
                {"ref": "git_gateway.proc_restart", "count": 2, "hosts": ["git-01", "git-02"]},
                {"ref": "deploy_orchestrator.deploy_pause", "count": 1, "hosts": ["deploy-01"]},
            ],
        },
    ],
}

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def h_u32(*parts: Any) -> int:
    m = hashlib.md5()
    for p in parts:
        m.update(str(p).encode("utf-8"))
        m.update(b"|")
    return int(m.hexdigest()[:8], 16)


def rng_from(*parts: Any) -> random.Random:
    seed = int(md5_hex("|".join(str(p) for p in parts))[:16], 16)
    return random.Random(seed)


def fmt_ts(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond/1000):03d}Z"


def pick_from_domain(domain: Dict[str, Any], var_name: str, ctx: Tuple[Any, ...]) -> Any:
    k = domain["k"]
    v = domain["v"]
    if k == "ch":
        lst = list(v)
        return lst[h_u32("ch", var_name, *ctx) % len(lst)]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi <= lo:
            return lo
        return lo + (h_u32("i", var_name, *ctx) % (hi - lo + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        if hi <= lo:
            return lo
        u = (h_u32("f", var_name, *ctx) & 0xFFFFFFFF) / 2**32
        return round(lo + (hi - lo) * u, 1)
    if k == "hex":
        ln = int(v)
        return md5_hex("|".join(map(str, ("hex", var_name) + ctx)))[:ln]
    if k == "ip":
        cidr = str(v)
        base, prefix = cidr.split("/")
        prefix = int(prefix)
        if prefix != 24:
            return base
        a, b, c, _ = base.split(".")
        last = 1 + (h_u32("ip", var_name, *ctx) % 254)
        return f"{a}.{b}.{c}.{last}"
    if k == "uuid":
        hx = md5_hex("|".join(map(str, ("uuid", var_name) + ctx)))
        return f"{hx[:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"
    if k == "str":
        return f"{var_name}-{md5_hex('|'.join(map(str, ctx)))[:6]}"
    raise ValueError(f"Unknown domain kind: {k}")


def component_of_ref(ref: str) -> Tuple[str, str]:
    comp_id, log_id = ref.split(".", 1)
    return comp_id, log_id


def get_template(ref: str) -> Dict[str, Any]:
    comp_id, log_id = component_of_ref(ref)
    return SYSTEM["components"][comp_id]["logs"][log_id]


def choose_host_for_component(comp_id: str, chain_key: str) -> str:
    hosts = SYSTEM["components"][comp_id]["hosts"]
    if not hosts:
        return ""
    idx = h_u32("host", comp_id, chain_key) % len(hosts)
    return hosts[idx]


def lognormal_from_p50_p95(p50: float, p95: float) -> Tuple[float, float]:
    p50 = max(0.001, float(p50))
    p95 = max(p50 * 1.0001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.64485
    sigma = max(0.0001, sigma)
    return mu, sigma


def sample_lognormal_ms(p50: float, p95: float, seed_key: Tuple[Any, ...], cap_mult: float = 3.0) -> int:
    mu, sigma = lognormal_from_p50_p95(p50, p95)
    r = rng_from("ln", *seed_key)
    val = r.lognormvariate(mu, sigma)
    cap = cap_mult * float(p95)
    if val > cap:
        val = cap * (0.92 + 0.08 * r.random())
    return max(1, int(round(val)))


def jitter_ms(key: Tuple[Any, ...], max_abs_ms: int) -> int:
    if max_abs_ms <= 0:
        return 0
    j = (h_u32("jitter", *key) % (2 * max_abs_ms + 1)) - max_abs_ms
    return int(j)


def derive_failure_controls() -> List[Dict[str, Any]]:
    events = sorted(SCENARIO["failure_events"], key=lambda e: e["at_min"])
    active_rates: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}
    points: List[Dict[str, Any]] = []
    for ev in events:
        for k, v in ev.get("rate_multipliers", {}).items():
            active_rates[k] = float(v)
        for k, v in ev.get("latency_multipliers", {}).items():
            active_lat[k] = {"p50": float(v["p50"]), "p95": float(v["p95"])}
        points.append(
            {
                "at_min": int(ev["at_min"]),
                "rate_multipliers": dict(active_rates),
                "latency_multipliers": {k: dict(v) for k, v in active_lat.items()},
                "one_shots": list(ev.get("one_shots", [])),
            }
        )
    return points


def active_controls_at_minute(minute: int, control_points: List[Dict[str, Any]]) -> Dict[str, Any]:
    applicable = None
    for cp in control_points:
        if cp["at_min"] <= minute:
            applicable = cp
        else:
            break
    if applicable is None:
        return {"rate_multipliers": {}, "latency_multipliers": {}}
    return applicable


def add_row(rows: List[Dict[str, Any]], ts: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append({"timestamp": ts, "level": level, "message": message, "trace_id": trace_id, "service": service, "host": host})


def _monotonic_fix(timestamps: List[datetime]) -> None:
    for k in range(1, len(timestamps)):
        if timestamps[k] <= timestamps[k - 1]:
            need_ms = int(round((timestamps[k - 1] - timestamps[k]).total_seconds() * 1000.0)) + 1
            d = timedelta(milliseconds=int(need_ms))
            for j in range(k, len(timestamps)):
                timestamps[j] = timestamps[j] + d


def simulate_flow_instance(
    rows: List[Dict[str, Any]],
    ref_flow: Dict[str, Any],
    state: str,
    start_ts: datetime,
    flow_index: int,
    latency_mult: Optional[Dict[str, float]] = None,
    auth_counts_by_min_host: Optional[Dict[int, Dict[str, Dict[str, int]]]] = None,
) -> None:
    flow_id = ref_flow["id"]
    trace_id = ""
    if ref_flow.get("trace", False) and SYSTEM["tracing"]["on"]:
        trace_id = md5_hex(f"trace|{state}|{flow_id}|{flow_index}")[:32]

    chain_key = trace_id if trace_id else f"{state}|{flow_id}|{flow_index}"
    comp_host: Dict[str, str] = {}

    def host_for(comp_id: str) -> str:
        if comp_id not in comp_host:
            comp_host[comp_id] = choose_host_for_component(comp_id, chain_key + "|" + comp_id)
        return comp_host[comp_id]

    lat_mult_p50 = 1.0
    lat_mult_p95 = 1.0
    if latency_mult:
        lat_mult_p50 = float(latency_mult.get("p50", 1.0))
        lat_mult_p95 = float(latency_mult.get("p95", 1.0))

    emit_refs: List[str] = list(ref_flow["emit"])
    latency_pairs: List[List[float]] = list(ref_flow["latency_ms"])
    assert len(emit_refs) == len(latency_pairs)

    # Initial schedule from latency hints (per log = delay since previous emitted log)
    timestamps: List[datetime] = []
    t = start_ts
    for i, pair in enumerate(latency_pairs):
        p50, p95 = float(pair[0]), float(pair[1])
        p50 *= lat_mult_p50
        p95 *= lat_mult_p95
        dms = sample_lognormal_ms(p50, p95, seed_key=(flow_id, flow_index, "lat", i), cap_mult=3.0)
        dms = max(1, dms + jitter_ms((flow_id, flow_index, "latj", i), 1))
        t = t + timedelta(milliseconds=dms)
        timestamps.append(t)

    def gap_ms(i: int, j: int) -> int:
        return int(round((timestamps[j] - timestamps[i]).total_seconds() * 1000.0))

    def shift_from(idx: int, delta_ms: int) -> None:
        if delta_ms == 0:
            return
        d = timedelta(milliseconds=int(delta_ms))
        for k in range(idx, len(timestamps)):
            timestamps[k] = timestamps[k] + d

    def enforce_between(i: int, j: int, lo: int, hi: int, shift_idx: int) -> None:
        if i is None or j is None or i < 0 or j < 0 or i >= len(timestamps) or j >= len(timestamps) or i >= j:
            return
        cur = gap_ms(i, j)
        target = cur
        if cur < lo:
            target = lo
        elif cur > hi:
            target = hi
        delta = target - cur
        if delta != 0:
            if target <= 0:
                target = 1
                delta = target - cur
            shift_from(shift_idx, delta)

    first_edge_idx = None
    last_edge_idx = None
    push_recv_idx = None
    push_complete_idx = None
    auth_idx = None
    auth_prev_idx = None

    for i, ref in enumerate(emit_refs):
        if ref == "edge_router.http_req":
            first_edge_idx = i
        if ref.startswith("edge_router.http_resp_"):
            last_edge_idx = i
        if ref == "git_gateway.push_received":
            push_recv_idx = i
        if ref in ("git_gateway.push_complete_200", "git_gateway.push_complete_401"):
            push_complete_idx = i
        if ref in ("internal_auth_api.auth_ok", "internal_auth_api.auth_denied"):
            auth_idx = i
            auth_prev_idx = i - 1

    # Align timestamp gaps with any modeled observed timing fields (dur_ms) by shifting timestamps.
    # This keeps message-carried duration consistent with emitted chronology.
    def apply_domain_enforcement() -> None:
        if auth_idx is not None and auth_prev_idx is not None and auth_prev_idx >= 0:
            auth_ref = emit_refs[auth_idx]
            auth_tpl = get_template(auth_ref)
            if "dur_ms" in auth_tpl.get("vars", {}):
                lo, hi = auth_tpl["vars"]["dur_ms"]["v"]
                enforce_between(auth_prev_idx, auth_idx, int(lo), int(hi), shift_idx=auth_idx)

        if push_recv_idx is not None and push_complete_idx is not None:
            pc_ref = emit_refs[push_complete_idx]
            pc_tpl = get_template(pc_ref)
            if "dur_ms" in pc_tpl.get("vars", {}):
                lo, hi = pc_tpl["vars"]["dur_ms"]["v"]
                enforce_between(push_recv_idx, push_complete_idx, int(lo), int(hi), shift_idx=push_complete_idx)

        if first_edge_idx is not None and last_edge_idx is not None:
            resp_ref = emit_refs[last_edge_idx]
            resp_tpl = get_template(resp_ref)
            if "dur_ms" in resp_tpl.get("vars", {}):
                lo, hi = resp_tpl["vars"]["dur_ms"]["v"]
                lo_i, hi_i = int(lo), int(hi)
                cur = gap_ms(first_edge_idx, last_edge_idx)
                target = cur
                if cur < lo_i:
                    target = lo_i
                elif cur > hi_i:
                    target = hi_i
                delta = target - cur
                if delta != 0:
                    if delta < 0:
                        idx = max(1, last_edge_idx - 1)
                        shift_from(idx, delta)
                    else:
                        shift_from(last_edge_idx, delta)

    for _ in range(2):
        apply_domain_enforcement()
        _monotonic_fix(timestamps)

    router_dur_ms = None
    if first_edge_idx is not None and last_edge_idx is not None:
        router_dur_ms = max(1, gap_ms(first_edge_idx, last_edge_idx))

    gateway_dur_ms = None
    if push_recv_idx is not None and push_complete_idx is not None:
        gateway_dur_ms = max(1, gap_ms(push_recv_idx, push_complete_idx))

    auth_dur_ms = None
    if auth_idx is not None and auth_prev_idx is not None and auth_prev_idx >= 0:
        auth_dur_ms = max(1, gap_ms(auth_prev_idx, auth_idx))

    for i, ref in enumerate(emit_refs):
        comp_id, log_id = component_of_ref(ref)
        comp = SYSTEM["components"][comp_id]
        tpl = comp["logs"][log_id]
        lvl = tpl["lvl"]
        bound: Dict[str, Any] = {}
        vars_spec: Dict[str, Any] = dict(tpl.get("vars", {}))

        if "trace_id" in vars_spec:
            bound["trace_id"] = trace_id

        for var_name, dom in vars_spec.items():
            if var_name in bound:
                continue
            bound[var_name] = pick_from_domain(dom, var_name, (flow_id, flow_index, ref, i))

        if log_id in ("push_complete_200", "push_complete_401") and "dur_ms" in vars_spec and gateway_dur_ms is not None:
            bound["dur_ms"] = int(gateway_dur_ms)
        if log_id in ("http_resp_200", "http_resp_401") and "dur_ms" in vars_spec and router_dur_ms is not None:
            bound["dur_ms"] = int(router_dur_ms)
        if log_id in ("auth_ok", "auth_denied") and "dur_ms" in vars_spec and auth_dur_ms is not None:
            bound["dur_ms"] = int(auth_dur_ms)
            if auth_counts_by_min_host is not None:
                minute = int((timestamps[i] - BASE_TIME).total_seconds() // 60)
                host = host_for(comp_id)
                d = auth_counts_by_min_host.setdefault(minute, {}).setdefault(host, {"ok": 0, "denied": 0})
                if log_id == "auth_ok":
                    d["ok"] += 1
                else:
                    d["denied"] += 1

        msg = tpl["msg"].format(**bound)
        add_row(
            rows,
            timestamps[i],
            lvl,
            msg,
            trace_id,
            comp.get("svc", "") or "",
            host_for(comp_id) if comp.get("hosts") else "",
        )


def build_flow_lookup() -> Dict[Tuple[str, str], Dict[str, Any]]:
    lookup: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for st in ("n", "f"):
        for fd in SYSTEM["flows"][st]["req"]:
            lookup[(st, fd["id"])] = fd
    return lookup


def simulate_flows(rows: List[Dict[str, Any]]) -> Dict[int, Dict[str, Dict[str, int]]]:
    flow_lookup = build_flow_lookup()
    auth_counts_by_min_host: Dict[int, Dict[str, Dict[str, int]]] = {}
    flow_index = 0

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    n_flow = flow_lookup[("n", "git_push_http_success")]
    rpm = float(n_flow["rpm"])

    for minute in range(n_start, n_end):
        count = int(round(rpm))
        minute_start = BASE_TIME + timedelta(minutes=minute)
        for i in range(count):
            offset_s = (i + 0.5) * 60.0 / max(1, count)
            jms = jitter_ms(("n", minute, i), 20)
            start = minute_start + timedelta(seconds=offset_s, milliseconds=jms)
            if start < minute_start:
                start = minute_start
            if start >= minute_start + timedelta(minutes=1):
                start = minute_start + timedelta(minutes=1, milliseconds=-1)
            simulate_flow_instance(rows, n_flow, "n", start, flow_index, latency_mult=None, auth_counts_by_min_host=auth_counts_by_min_host)
            flow_index += 1

    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    control_points = derive_failure_controls()

    ok_flow = flow_lookup[("f", "git_push_http_ok")]
    e401_flow = flow_lookup[("f", "git_push_http_401")]
    base_ok = float(ok_flow["rpm"])
    base_401 = float(e401_flow["rpm"])

    for minute in range(f_start, f_end):
        ctrl = active_controls_at_minute(minute, control_points)
        rm = ctrl.get("rate_multipliers", {})
        lm = ctrl.get("latency_multipliers", {})

        ok_mult = float(rm.get("git_push_http_ok", 1.0))
        e401_mult = float(rm.get("git_push_http_401", 1.0))
        ok_count = int(round(base_ok * ok_mult))
        e401_count = int(round(base_401 * e401_mult))

        minute_start = BASE_TIME + timedelta(minutes=minute)

        for i in range(ok_count):
            offset_s = (i + 0.5) * 60.0 / max(1, ok_count)
            jms = jitter_ms(("f_ok", minute, i), 20)
            start = minute_start + timedelta(seconds=offset_s, milliseconds=jms)
            if start < minute_start:
                start = minute_start
            if start >= minute_start + timedelta(minutes=1):
                start = minute_start + timedelta(minutes=1, milliseconds=-1)
            simulate_flow_instance(
                rows,
                ok_flow,
                "f",
                start,
                flow_index,
                latency_mult=lm.get("git_push_http_ok"),
                auth_counts_by_min_host=auth_counts_by_min_host,
            )
            flow_index += 1

        for i in range(e401_count):
            offset_s = (i + 0.5) * 60.0 / max(1, e401_count)
            base_shift_ms = 50
            jms = jitter_ms(("f_401", minute, i), 20)
            start = minute_start + timedelta(seconds=offset_s, milliseconds=base_shift_ms + jms)
            if start < minute_start:
                start = minute_start
            if start >= minute_start + timedelta(minutes=1):
                start = minute_start + timedelta(minutes=1, milliseconds=-1)
            simulate_flow_instance(rows, e401_flow, "f", start, flow_index, latency_mult=None, auth_counts_by_min_host=auth_counts_by_min_host)
            flow_index += 1

    return auth_counts_by_min_host


def simulate_health_ok(rows: List[Dict[str, Any]]) -> None:
    comp_id = "git_gateway"
    comp = SYSTEM["components"][comp_id]
    tpl = comp["logs"]["health_ok"]
    svc = comp["svc"]
    hosts = comp["hosts"]
    total_minutes = SCENARIO["time"]["total_minutes"]

    for host in hosts:
        acc = 0.0
        for minute in range(0, total_minutes):
            acc += 0.5
            if acc >= 1.0 - 1e-9:
                acc -= 1.0
                minute_start = BASE_TIME + timedelta(minutes=minute)
                ts = minute_start + timedelta(seconds=10, milliseconds=jitter_ms(("health", host, minute), 200))
                bound: Dict[str, Any] = {}
                for var_name, dom in tpl["vars"].items():
                    bound[var_name] = pick_from_domain(dom, var_name, (comp_id, host, minute))
                msg = tpl["msg"].format(**bound)
                add_row(rows, ts, tpl["lvl"], msg, "", svc, host)


def simulate_deploy_status(rows: List[Dict[str, Any]]) -> None:
    comp_id = "deploy_orchestrator"
    comp = SYSTEM["components"][comp_id]
    tpl = comp["logs"]["deploy_status"]
    svc = comp["svc"]
    host = comp["hosts"][0] if comp["hosts"] else ""

    control_points = derive_failure_controls()
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]

    acc = 0.0
    for minute in range(f_start, f_end):
        ctrl = active_controls_at_minute(minute, control_points)
        rm = ctrl.get("rate_multipliers", {})
        mult = float(rm.get("deploy_orchestrator.deploy_status", 1.0))
        acc += 1.0 * mult
        emit = 0
        while acc >= 1.0 - 1e-9:
            acc -= 1.0
            emit += 1
        for k in range(emit):
            minute_start = BASE_TIME + timedelta(minutes=minute)
            ts = minute_start + timedelta(seconds=5, milliseconds=50 * k + jitter_ms(("deploy_status", minute, k), 120))
            updated = min(4, max(0, int(round((minute - f_start) / max(1, (f_end - f_start)) * 4))))
            poisoned = 2
            pending = max(0, 4 - updated)
            bound = {"batch": "batch-a", "updated": updated, "poisoned": poisoned, "pending": pending}
            msg = tpl["msg"].format(**bound)
            add_row(rows, ts, tpl["lvl"], msg, "", svc, host)


def _clip_int(x: int, lo: int, hi: int) -> int:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def simulate_auth_metrics(rows: List[Dict[str, Any]], auth_counts_by_min_host: Dict[int, Dict[str, Dict[str, int]]]) -> None:
    comp_id = "internal_auth_api"
    comp = SYSTEM["components"][comp_id]
    tpl = comp["logs"]["auth_metrics"]
    svc = comp["svc"]
    hosts = comp["hosts"]

    total_minutes = SCENARIO["time"]["total_minutes"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]

    for minute in range(0, total_minutes):
        state = "n" if minute < n_end else "f"
        sv = tpl.get("state_vars", {}).get(state, {})
        ok_dom = sv.get("ok")
        denied_dom = sv.get("denied")
        if ok_dom is None or denied_dom is None:
            raise ValueError("auth_metrics missing state_vars domains")

        ok_lo, ok_hi = int(ok_dom["v"][0]), int(ok_dom["v"][1])
        dn_lo, dn_hi = int(denied_dom["v"][0]), int(denied_dom["v"][1])

        minute_start = BASE_TIME + timedelta(minutes=minute)
        base_ts = minute_start + timedelta(seconds=59, milliseconds=700)
        for hi, host in enumerate(hosts):
            ts = base_ts + timedelta(milliseconds=hi * 30 + jitter_ms(("auth_metrics", minute, host), 80))
            counts = auth_counts_by_min_host.get(minute, {}).get(host, {"ok": 0, "denied": 0})
            raw_ok = int(counts.get("ok", 0))
            raw_dn = int(counts.get("denied", 0))

            ok_val = _clip_int(raw_ok, ok_lo, ok_hi)
            dn_val = _clip_int(raw_dn, dn_lo, dn_hi)

            if raw_ok == 0 and ok_lo > 0:
                ok_val = int(pick_from_domain(ok_dom, "ok", (minute, host, "metrics_sample")))
            if raw_dn == 0 and dn_lo > 0:
                dn_val = int(pick_from_domain(denied_dom, "denied", (minute, host, "metrics_sample")))

            bound = {"endpoint": "/v2/git/authorize", "window_s": 60, "ok": int(ok_val), "denied": int(dn_val)}
            msg = tpl["msg"].format(**bound)
            add_row(rows, ts, tpl["lvl"], msg, "", svc, host)


def emit_one_shot(rows: List[Dict[str, Any]], ref: str, at_min: int, count: int, hosts: List[str]) -> None:
    comp_id, log_id = component_of_ref(ref)
    comp = SYSTEM["components"][comp_id]
    tpl = comp["logs"][log_id]
    svc = comp.get("svc", "") or ""

    event_ts = BASE_TIME + timedelta(minutes=at_min)
    for i in range(count):
        host = hosts[min(i, len(hosts) - 1)] if hosts else (comp["hosts"][0] if comp.get("hosts") else "")
        ts = event_ts + timedelta(milliseconds=200 * i + jitter_ms(("one_shot", ref, at_min, i), 150))

        bound: Dict[str, Any] = {}

        if ref == "deploy_orchestrator.instance_poisoned":
            instances = ["git-03", "git-04"]
            bound["instance"] = instances[i % len(instances)]
            bound["old_code_ver"] = "2020-08-20.4"
            bound["batch"] = "batch-a"
        elif ref == "deploy_orchestrator.batch_deploy_start":
            bound["batch"] = "batch-a"
            bound["code_ver"] = "2020-09-03.1"
            bound["cfg_required_rev"] = 42
        elif ref == "deploy_orchestrator.deploy_pause":
            bound["batch"] = "batch-a"
            bound["reason"] = "elevated_401"
        elif ref == "config_distributor.config_sync_trigger":
            bound["cfg_rev"] = 42
            bound["targets"] = 4
        elif ref == "config_distributor.config_sync_complete":
            bound["cfg_rev"] = 42
            bound["updated"] = 4
            bound["failed"] = 0
        elif ref == "git_gateway.proc_restart":
            bound["reason"] = "killed_for_cfg_sync"
            bound["code_ver"] = "2020-09-03.1"
            bound["cfg_rev"] = 42
        elif ref == "alerting.alert_401_rate_high":
            bound["rate_pct"] = 35.0
            bound["window_min"] = 5

        for var_name, dom in tpl.get("vars", {}).items():
            if var_name not in bound:
                bound[var_name] = pick_from_domain(dom, var_name, (ref, at_min, i))

        msg = tpl["msg"].format(**bound)
        add_row(rows, ts, tpl["lvl"], msg, "", svc, host)


def simulate_one_shots(rows: List[Dict[str, Any]]) -> None:
    for ev in SCENARIO["failure_events"]:
        at_min = int(ev["at_min"])
        for ospec in ev.get("one_shots", []):
            emit_one_shot(rows, ref=ospec["ref"], at_min=at_min, count=int(ospec["count"]), hosts=list(ospec.get("hosts", [])))


def main() -> None:
    rows: List[Dict[str, Any]] = []

    auth_counts = simulate_flows(rows)

    simulate_health_ok(rows)
    simulate_deploy_status(rows)
    simulate_auth_metrics(rows, auth_counts)

    simulate_one_shots(rows)

    df = pd.DataFrame(rows)

    # Do not time-compress or clip request chains to the scenario end; chains may spill past total_minutes.
    df.sort_values(["timestamp", "service", "host", "level", "message"], inplace=True, kind="mergesort")
    df["timestamp"] = df["timestamp"].apply(fmt_ts)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    assert list(df.columns) == ["timestamp", "level", "message", "trace_id", "service", "host"]
    assert 20000 <= len(df) <= 100000, f"Row count {len(df)} out of target range"
    ts_parsed = pd.to_datetime(df["timestamp"], utc=True, format="%Y-%m-%dT%H:%M:%S.%fZ", errors="raise")
    assert ts_parsed.is_monotonic_increasing

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
