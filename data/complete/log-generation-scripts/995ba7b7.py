import math
import re
import hashlib
import random
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "id": "tq_cluster_startup_logging_stall",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32},
    },
    "components": [
        {
            "id": "login_gateway",
            "svc": "tq-login",
            "hosts": ["gw-01", "gw-02"],
            "logs": {
                "login_ok": {
                    "lvl": "INFO",
                    "msg": "login user={user_id} result=OK node={node} ms={ms}",
                    "vars": {
                        "user_id": {"k": "i", "v": [1000000, 9999999]},
                        "node": {"k": "i", "v": [1, 250]},
                        "ms": {"k": "i", "v": [5, 300]},
                    },
                },
                "login_maintenance": {
                    "lvl": "WARN",
                    "msg": "login user={user_id} rejected: maintenance",
                    "vars": {"user_id": {"k": "i", "v": [1000000, 9999999]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "ops_console",
            "svc": "tq-ops",
            "hosts": ["ops-01"],
            "logs": {
                "cmd_startup": {
                    "lvl": "INFO",
                    "msg": "ops command: startup cluster={cluster}",
                    "vars": {"cluster": {"k": "ch", "v": ["tq"]}},
                },
                "cmd_reboot": {
                    "lvl": "WARN",
                    "msg": "ops command: reboot cluster={cluster}",
                    "vars": {"cluster": {"k": "ch", "v": ["tq"]}},
                },
                "cmd_load_campaigns": {
                    "lvl": "WARN",
                    "msg": "ops console: load_campaigns scope=all_nodes",
                    "vars": {},
                },
                "audit_tick": {"lvl": "DEBUG", "msg": "ops audit tick ok", "vars": {}},
            },
            "beh": {
                "n": {"emit": [{"id": "audit_tick", "per_min": 0.2, "scope": "global"}]},
                "f": {"emit": [{"id": "audit_tick", "per_min": 0.3, "scope": "global"}]},
            },
        },
        {
            "id": "deployment_tool",
            "svc": "tq-deploy",
            "hosts": ["deploy-01"],
            "logs": {
                "deploy_timeout": {
                    "lvl": "ERROR",
                    "msg": "deploy failed: timeout uploading package={pkg} cluster={cluster}",
                    "vars": {"pkg": {"k": "ch", "v": ["aegis_hotfix_1"]}, "cluster": {"k": "ch", "v": ["tq"]}},
                },
                "deploy_complete": {
                    "lvl": "INFO",
                    "msg": "deploy complete package={pkg} cluster={cluster} ms={ms}",
                    "vars": {
                        "pkg": {"k": "ch", "v": ["aegis_hotfix_1"]},
                        "cluster": {"k": "ch", "v": ["tq"]},
                        "ms": {"k": "i", "v": [60000, 900000]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "polaris_master",
            "svc": "tq-polaris",
            "hosts": ["tq-polaris-01"],
            "logs": {
                "cluster_ready": {
                    "lvl": "INFO",
                    "msg": "cluster ready stage=0 online_nodes={online_nodes}",
                    "vars": {"online_nodes": {"k": "i", "v": [200, 250]}},
                },
                "stage_report_rcvd": {
                    "lvl": "INFO",
                    "msg": "stage report from node={node} stage={stage}",
                    "vars": {"node": {"k": "i", "v": [1, 250]}, "stage": {"k": "i", "v": [-4, 0]}},
                },
                "startup_waiting": {
                    "lvl": "WARN",
                    "msg": "startup waiting stage={stage} waiting_nodes={waiting_nodes} stuck_nodes={stuck_nodes} dead_nodes={dead_nodes}",
                    "vars": {
                        "stage": {"k": "i", "v": [-4, 0]},
                        "waiting_nodes": {"k": "i", "v": [0, 250]},
                        "stuck_nodes": {"k": "i", "v": [0, 250]},
                        "dead_nodes": {"k": "i", "v": [0, 250]},
                    },
                },
                "node_dead_detected": {
                    "lvl": "ERROR",
                    "msg": "node marked dead node={node} reason=heartbeat_timeout remap_systems={systems}",
                    "vars": {"node": {"k": "i", "v": [1, 250]}, "systems": {"k": "i", "v": [1, 80]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "cluster_ready", "per_min": 1.0, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "startup_waiting", "per_min": 1.0, "scope": "global"},
                        {"id": "node_dead_detected", "per_min": 0.1, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "game_node",
            "svc": "tq-node",
            "hosts": [
                "tq-node-01",
                "tq-node-02",
                "tq-node-03",
                "tq-node-04",
                "tq-node-05",
                "tq-node-06",
                "tq-node-07",
                "tq-node-08",
                "tq-node-09",
                "tq-node-10",
                "tq-node-11",
                "tq-node-12",
            ],
            "logs": {
                "heartbeat": {
                    "lvl": "DEBUG",
                    "msg": "heartbeat ok node={node}",
                    "vars": {"node": {"k": "i", "v": [1, 250]}},
                },
                "session_created": {
                    "lvl": "INFO",
                    "msg": "session created user={user_id} node={node}",
                    "vars": {"user_id": {"k": "i", "v": [1000000, 9999999]}, "node": {"k": "i", "v": [1, 250]}},
                },
                "stage_report_sent": {
                    "lvl": "INFO",
                    "msg": "sending stage report stage={stage} node={node}",
                    "vars": {"stage": {"k": "i", "v": [-4, 0]}, "node": {"k": "i", "v": [1, 250]}},
                },
                "campaign_log_line": {
                    "lvl": "WARN",
                    "msg": "campaign channel log action={action} campaign_id={campaign_id} alliance_id={alliance_id}",
                    "vars": {
                        "action": {"k": "ch", "v": ["load_campaign", "compute_window", "spawn_command_node", "refresh_cache"]},
                        "campaign_id": {"k": "i", "v": [1, 5000]},
                        "alliance_id": {"k": "i", "v": [1000, 9999]},
                    },
                },
                "logger_backpressure": {
                    "lvl": "WARN",
                    "msg": "logger backpressure channel={channel} blocked_ms={blocked_ms}",
                    "vars": {"blocked_ms": {"k": "i", "v": [0, 300000]}},
                    "state_vars": {
                        "n": {"channel": {"k": "ch", "v": ["generic"]}},
                        "f": {"channel": {"k": "ch", "v": ["campaign", "generic"]}},
                    },
                },
                "process_exit": {
                    "lvl": "ERROR",
                    "msg": "node process exited node={node} exit_code={exit_code}",
                    "vars": {"node": {"k": "i", "v": [1, 250]}, "exit_code": {"k": "i", "v": [1, 255]}},
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "heartbeat", "per_min": 6.0},
                        {"id": "logger_backpressure", "per_min": 0.05},
                        {"id": "process_exit", "per_min": 0.002},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "heartbeat", "per_min": 4.0},
                        {"id": "logger_backpressure", "per_min": 1.5},
                        {"id": "process_exit", "per_min": 0.03},
                    ]
                },
            },
        },
        {
            "id": "log_collector",
            "svc": "tq-logs",
            "hosts": ["splunk-01"],
            "logs": {
                "ingest_stats": {
                    "lvl": "INFO",
                    "msg": "ingest stats eps={eps} queue_depth={queue_depth} dropped={dropped}",
                    "vars": {
                        "eps": {"k": "i", "v": [5, 60]},
                        "queue_depth": {"k": "i", "v": [0, 150000]},
                        "dropped": {"k": "i", "v": [0, 20000]},
                    },
                },
                "queue_high": {
                    "lvl": "WARN",
                    "msg": "backpressure active channel={channel} queue_depth={queue_depth}",
                    "vars": {"queue_depth": {"k": "i", "v": [5000, 150000]}},
                    "state_vars": {"n": {"channel": {"k": "ch", "v": ["generic"]}}, "f": {"channel": {"k": "ch", "v": ["campaign"]}}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "ingest_stats", "per_min": 1.0, "scope": "global"}, {"id": "queue_high", "per_min": 0.1, "scope": "global"}]},
                "f": {"emit": [{"id": "ingest_stats", "per_min": 1.0, "scope": "global"}, {"id": "queue_high", "per_min": 1.0, "scope": "global"}]},
            },
        },
        {
            "id": "db_cluster",
            "svc": "tq-db",
            "hosts": ["db-01"],
            "logs": {
                "query_slow": {
                    "lvl": "WARN",
                    "msg": "slow query name={qname} ms={ms} rows={rows}",
                    "vars": {
                        "qname": {"k": "ch", "v": ["load_campaigns", "load_systems", "load_market"]},
                        "ms": {"k": "i", "v": [50, 5000]},
                        "rows": {"k": "i", "v": [1, 50000]},
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "query_slow", "per_min": 0.2, "scope": "global"}]},
                "f": {"emit": [{"id": "query_slow", "per_min": 0.4, "scope": "global"}]},
            },
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "player_login_ok",
                    "rpm": 250.0,
                    "path": ["login_gateway", "game_node"],
                    "emit": ["login_gateway.login_ok", "game_node.session_created"],
                    "latency_ms": [[10, 80], [5, 60]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                }
            ]
        },
        "f": {
            "req": [
                {
                    "id": "player_login_maintenance",
                    "rpm": 40.0,
                    "path": ["login_gateway"],
                    "emit": ["login_gateway.login_maintenance"],
                    "latency_ms": [[5, 40]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "node_stage_report",
                    "rpm": 80.0,
                    "path": ["game_node", "polaris_master"],
                    "emit": ["game_node.stage_report_sent", "polaris_master.stage_report_rcvd"],
                    "latency_ms": [[5, 40], [5, 40]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "campaign_log_line",
                    "rpm": 60.0,
                    "path": ["game_node"],
                    "emit": ["game_node.campaign_log_line"],
                    "latency_ms": [[1, 5]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "tq_long_downtime_campaign_log_channel_stall",
    "time": {"total_minutes": 40, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 40}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {
                        "node_stage_report": 1.8,
                        "campaign_log_line": 4.0,
                        "game_node.logger_backpressure": 4.0,
                        "log_collector.queue_high": 3.0,
                        "polaris_master.startup_waiting": 1.5,
                        "polaris_master.node_dead_detected": 0.0,
                        "game_node.process_exit": 0.0,
                    },
                    "latency_multipliers": {"node_stage_report": {"p50": 1.5, "p95": 4.0}},
                    "one_shots": [
                        {"ref": "deployment_tool.deploy_complete", "count": 1, "hosts": ["deploy-01"]},
                        {"ref": "ops_console.cmd_startup", "count": 1, "hosts": ["ops-01"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 26,
                    "rate_multipliers": {
                        "node_stage_report": 2.3,
                        "campaign_log_line": 6.0,
                        "game_node.logger_backpressure": 6.0,
                        "log_collector.queue_high": 5.0,
                        "polaris_master.startup_waiting": 2.0,
                    },
                    "latency_multipliers": {"node_stage_report": {"p50": 2.0, "p95": 6.0}},
                    "one_shots": [{"ref": "ops_console.cmd_reboot", "count": 1, "hosts": ["ops-01"]}],
                },
                {
                    "order": 3,
                    "at_min": 33,
                    "rate_multipliers": {
                        "node_stage_report": 1.2,
                        "campaign_log_line": 18.0,
                        "game_node.logger_backpressure": 15.0,
                        "game_node.process_exit": 4.0,
                        "log_collector.queue_high": 12.0,
                        "polaris_master.startup_waiting": 2.5,
                        "polaris_master.node_dead_detected": 6.0,
                    },
                    "latency_multipliers": {"node_stage_report": {"p50": 2.5, "p95": 10.0}},
                    "one_shots": [{"ref": "ops_console.cmd_load_campaigns", "count": 1, "hosts": ["ops-01"]}],
                },
            ]
        }
    },
}

SEED = 7
random.seed(SEED)
np.random.seed(SEED)

PLACEHOLDER_RE = re.compile(r"{([a-zA-Z_][a-zA-Z0-9_]*)}")
NORM = NormalDist()


def md5_int(s: str) -> int:
    return int.from_bytes(hashlib.md5(s.encode("utf-8")).digest()[:8], "big", signed=False)


def stable_u01(key: str) -> float:
    x = md5_int(f"{SEED}:{key}")
    return ((x % (10**12)) + 0.5) / (10**12 + 1.0)


def clamp_int(v: int, lo: int, hi: int) -> int:
    return lo if v < lo else hi if v > hi else v


def iso_utc_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond/1000):03d}Z"


def lognormal_ms_from_p50_p95(p50: float, p95: float, key: str, soft_cap_factor: float = 2.7) -> int:
    # Lognormal with median p50 and ~95th percentile p95.
    p50 = max(0.1, float(p50))
    p95 = max(p50 * 1.0001, float(p95))
    sigma = math.log(p95 / p50) / 1.645
    mu = math.log(p50)
    u = stable_u01(key)
    u = min(max(u, 1e-6), 1.0 - 1e-6)
    z = NORM.inv_cdf(u)
    x = math.exp(mu + sigma * z)
    cap = soft_cap_factor * p95
    x = min(x, cap)
    x = max(0.1, x)
    return int(round(x))


def spread_times(start_dt: datetime, end_dt: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    total_s = (end_dt - start_dt).total_seconds()
    if total_s <= 0:
        return [start_dt] * count
    step = total_s / count
    jitter_cap = min(step * 0.4, 0.8)  # seconds
    out = []
    for i in range(count):
        base = start_dt + timedelta(seconds=(i + 0.5) * step)
        j = (stable_u01(f"{key}:j:{i}") - 0.5) * jitter_cap
        t = base + timedelta(seconds=j)
        if t < start_dt:
            t = start_dt + timedelta(milliseconds=int(1000 * stable_u01(f"{key}:clipL:{i}")))
        if t >= end_dt:
            t = end_dt - timedelta(milliseconds=1 + int(1000 * stable_u01(f"{key}:clipR:{i}")))
        out.append(t)
    out.sort()
    return out


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    comps = {c["id"]: c for c in system["components"]}
    log_templates: Dict[str, Any] = {}
    for cid, c in comps.items():
        for lid, lt in c.get("logs", {}).items():
            log_templates[f"{cid}.{lid}"] = {"component_id": cid, "log_id": lid, **lt}
    return comps, log_templates


COMPONENTS, LOG_TEMPLATES = build_indices(SYSTEM)


def domain_for_var(lt: Dict[str, Any], state: str, varname: str) -> Optional[Dict[str, Any]]:
    if "state_vars" in lt and state in lt["state_vars"] and varname in lt["state_vars"][state]:
        return lt["state_vars"][state][varname]
    vars_base = lt.get("vars", {})
    if varname in vars_base:
        return vars_base[varname]
    return None


def render_from_template(template_ref: str, state: str, bound: Dict[str, Any], render_key: str) -> str:
    lt = LOG_TEMPLATES[template_ref]
    msg = lt["msg"]
    placeholders = PLACEHOLDER_RE.findall(msg)
    values: Dict[str, Any] = {}
    for p in placeholders:
        if p in bound:
            values[p] = bound[p]
            continue
        dom = domain_for_var(lt, state, p)
        if dom is None:
            values[p] = ""
            continue
        k = dom["k"]
        v = dom["v"]
        if k == "i":
            lo, hi = int(v[0]), int(v[1])
            values[p] = lo + (md5_int(f"{render_key}:{p}") % (hi - lo + 1))
        elif k == "f":
            lo, hi = float(v[0]), float(v[1])
            values[p] = lo + stable_u01(f"{render_key}:{p}") * (hi - lo)
        elif k == "ch":
            choices = list(v)
            values[p] = choices[md5_int(f"{render_key}:{p}") % len(choices)]
        elif k == "hex":
            n = int(v)
            h = hashlib.md5(f"{SEED}:{render_key}:{p}".encode("utf-8")).hexdigest()
            values[p] = h[:n]
        elif k == "uuid":
            h = hashlib.md5(f"{SEED}:{render_key}:{p}".encode("utf-8")).hexdigest()
            values[p] = f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"
        else:
            values[p] = str(v)
    return msg.format_map(values)


def allocate_int(expected: float, carry_key: str, carries: Dict[str, float]) -> int:
    c = carries.get(carry_key, 0.0)
    x = expected + c
    n = int(math.floor(x + 1e-12))
    carries[carry_key] = x - n
    return n


def game_node_base_id_for_host(host: str) -> int:
    hosts = COMPONENTS["game_node"]["hosts"]
    idx = hosts.index(host)
    return 1 + idx * 20  # 1,21,...,221


def choose_host(component_id: str, key: str) -> str:
    hosts = COMPONENTS[component_id].get("hosts", [])
    if not hosts:
        return ""
    return hosts[md5_int(f"{key}:{component_id}") % len(hosts)]


def get_active_controls_at_min(events: List[Dict[str, Any]], minute: int) -> Tuple[Dict[str, float], Dict[str, Dict[str, float]]]:
    rate_mult: Dict[str, float] = {}
    lat_mult: Dict[str, Dict[str, float]] = {}
    for ev in events:
        if ev["at_min"] <= minute:
            for k, v in ev.get("rate_multipliers", {}).items():
                rate_mult[k] = float(v)
            for flow_id, mult in ev.get("latency_multipliers", {}).items():
                lat_mult[flow_id] = {"p50": float(mult.get("p50", 1.0)), "p95": float(mult.get("p95", 1.0))}
    return rate_mult, lat_mult


def build_failure_intervals(phase_start: int, phase_end: int, events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    events_sorted = sorted(events, key=lambda e: (e["at_min"], e.get("order", 0)))
    bounds = [phase_start] + [e["at_min"] for e in events_sorted if phase_start <= e["at_min"] < phase_end] + [phase_end]
    seen = set()
    bounds2 = []
    for b in bounds:
        if b not in seen:
            seen.add(b)
            bounds2.append(b)
    bounds = bounds2
    intervals = []
    for i in range(len(bounds) - 1):
        s, e = bounds[i], bounds[i + 1]
        if s >= e:
            continue
        rate_mult, lat_mult = get_active_controls_at_min(events_sorted, s)
        intervals.append({"start_min": s, "end_min": e, "rate_mult": rate_mult, "lat_mult": lat_mult})
    return intervals


def expected_volume_per_minute(minute: int) -> float:
    n_phase = SCENARIO["time"]["phases"]["n"]
    if n_phase["start_min"] <= minute < n_phase["end_min"]:
        state = "n"
        rate_mult = {}
    else:
        state = "f"
        events = SCENARIO["phases"]["f"]["events"]
        rate_mult, _ = get_active_controls_at_min(events, minute)

    total = 0.0
    for flow in SYSTEM["flows"][state]["req"]:
        rpm = float(flow["rpm"])
        if state == "f":
            rpm *= float(rate_mult.get(flow["id"], 1.0))
        total += rpm * len(flow["emit"])

    for comp in SYSTEM["components"]:
        cid = comp["id"]
        beh = comp.get("beh", {}).get(state, {}).get("emit", [])
        for em in beh:
            lid = em["id"]
            per_min = float(em["per_min"])
            scope = em.get("scope", "per_host")
            if state == "f":
                per_min *= float(rate_mult.get(f"{cid}.{lid}", 1.0))
            if per_min <= 0:
                continue
            if scope == "global":
                total += per_min
            else:
                total += per_min * max(1, len(comp.get("hosts", [])))
    return total


def make_background_bound(component_id: str, log_id: str, state: str, host: str, t: datetime, minute: int, rate_mult: Dict[str, float]) -> Dict[str, Any]:
    bound: Dict[str, Any] = {}

    if component_id == "game_node":
        if log_id == "heartbeat":
            bound["node"] = game_node_base_id_for_host(host)
        elif log_id == "logger_backpressure":
            mult = float(rate_mult.get("game_node.logger_backpressure", 1.0)) if state == "f" else 1.0
            bound["channel"] = "campaign" if state == "f" else "generic"
            u = stable_u01(f"bp:{state}:{minute}:{host}:{iso_utc_ms(t)}")
            base = 800 if state == "n" else 15000
            blocked = int(base + (mult - 1.0) * 18000 + u * (5000 if state == "n" else 50000))
            bound["blocked_ms"] = clamp_int(blocked, 0, 300000)
        elif log_id == "process_exit":
            bound["node"] = game_node_base_id_for_host(host)
            bound["exit_code"] = 1 + (md5_int(f"exit:{minute}:{host}:{iso_utc_ms(t)}") % 120)

    elif component_id == "polaris_master":
        if log_id == "cluster_ready":
            bound["online_nodes"] = clamp_int(232 + (md5_int(f"on:{minute}") % 16) - 8, 200, 250)
        elif log_id == "startup_waiting":
            bound["stage"] = -1
            bp_mult = float(rate_mult.get("game_node.logger_backpressure", 1.0))
            dead_mult = float(rate_mult.get("polaris_master.node_dead_detected", 1.0))
            u = stable_u01(f"wait:{minute}:{iso_utc_ms(t)}")
            stuck = int(25 + 12 * bp_mult + u * 35 + (0 if dead_mult <= 0 else 10))
            stuck = clamp_int(stuck, 0, 250)
            waiting = int(120 + 8 * bp_mult + u * 40)
            waiting = clamp_int(max(waiting, stuck), 0, 250)
            dead = 0 if dead_mult <= 0 else clamp_int(int(2 + dead_mult * 2 + u * 6), 0, 250)
            bound["waiting_nodes"] = waiting
            bound["stuck_nodes"] = stuck
            bound["dead_nodes"] = dead
        elif log_id == "node_dead_detected":
            bound["node"] = 1 + (md5_int(f"dead:{minute}:{iso_utc_ms(t)}") % 250)
            bound["systems"] = 1 + (md5_int(f"sys:{minute}:{iso_utc_ms(t)}") % 80)

    elif component_id == "log_collector":
        if log_id == "ingest_stats":
            vol = expected_volume_per_minute(minute)
            eps = int(round(vol / 60.0))
            eps = clamp_int(eps, 5, 60)
            q_mult = float(rate_mult.get("log_collector.queue_high", 1.0)) if state == "f" else 1.0
            u = stable_u01(f"ing:{minute}:{iso_utc_ms(t)}")
            if state == "n":
                qd = int(50 + u * 600)
                dropped = int(u * 3)
            else:
                qd_base = int(min(150000, max(0, (vol - 500.0) * 90.0 + (q_mult - 1.0) * 8000.0)))
                qd = int(qd_base * (0.75 + 0.25 * u))
                qd = clamp_int(qd, 0, 150000)
                dropped = int(min(20000, max(0, (qd - 35000) / 6.0)))
            bound["eps"] = eps
            bound["queue_depth"] = int(qd)
            bound["dropped"] = int(dropped)
        elif log_id == "queue_high":
            if state == "n":
                bound["channel"] = "generic"
                u = stable_u01(f"qh:n:{minute}:{iso_utc_ms(t)}")
                bound["queue_depth"] = clamp_int(int(5000 + u * 15000), 5000, 150000)
            else:
                bound["channel"] = "campaign"
                q_mult = float(rate_mult.get("log_collector.queue_high", 1.0))
                u = stable_u01(f"qh:f:{minute}:{iso_utc_ms(t)}")
                base = 18000 + (q_mult - 1.0) * 9000
                bound["queue_depth"] = clamp_int(int(base + u * 90000), 5000, 150000)

    elif component_id == "db_cluster":
        if log_id == "query_slow":
            u = stable_u01(f"db:{state}:{minute}:{iso_utc_ms(t)}")
            bound["qname"] = ["load_campaigns", "load_systems", "load_market"][md5_int(f"dbq:{minute}") % 3]
            bound["ms"] = clamp_int(int((120 if state == "n" else 250) + u * (900 if state == "n" else 1600)), 50, 5000)
            bound["rows"] = clamp_int(int(50 + u * 12000), 1, 50000)

    return bound


def flow_attempt_counts(num_instances: int, expected_attempts: float, max_attempts: int, key: str) -> List[int]:
    if num_instances <= 0:
        return []
    expected_attempts = float(expected_attempts)
    max_attempts = int(max_attempts)
    if max_attempts <= 1 or expected_attempts <= 1.0:
        return [1] * num_instances
    lo = int(math.floor(expected_attempts))
    hi = int(math.ceil(expected_attempts))
    lo = max(1, min(lo, max_attempts))
    hi = max(1, min(hi, max_attempts))
    if lo == hi:
        return [lo] * num_instances
    frac = expected_attempts - lo
    n_hi = int(round(frac * num_instances))
    n_hi = clamp_int(n_hi, 0, num_instances)
    order = sorted(range(num_instances), key=lambda i: md5_int(f"{key}:mix:{i}"))
    hi_set = set(order[:n_hi])
    return [hi if i in hi_set else lo for i in range(num_instances)]


def simulate_flow_instance(
    flow: Dict[str, Any],
    state: str,
    start_t: datetime,
    instance_idx: int,
    interval_controls: Dict[str, Any],
    rows: List[Dict[str, Any]],
) -> None:
    flow_id = flow["id"]
    lat_mult = interval_controls.get("lat_mult", {})

    comp_host: Dict[str, str] = {}
    for cref in flow["emit"]:
        cid = cref.split(".", 1)[0]
        if cid not in comp_host:
            comp_host[cid] = choose_host(cid, f"flow:{flow_id}:{instance_idx}:{start_t.timestamp()}")

    ctx: Dict[str, Any] = {}
    if flow_id == "player_login_ok":
        user_id = 1000000 + (md5_int(f"uid:{instance_idx}:{start_t.timestamp()}") % 9000000)
        node_host = comp_host.get("game_node", choose_host("game_node", f"flow:{flow_id}:{instance_idx}"))
        base = game_node_base_id_for_host(node_host)
        node = clamp_int(base + (md5_int(f"nsel:{instance_idx}:{start_t.timestamp()}") % 20), 1, 250)
        ctx.update({"user_id": user_id, "node": node})
    elif flow_id == "player_login_maintenance":
        ctx["user_id"] = 1000000 + (md5_int(f"uidm:{instance_idx}:{start_t.timestamp()}") % 9000000)
    elif flow_id == "node_stage_report":
        node_host = comp_host.get("game_node", choose_host("game_node", f"flow:{flow_id}:{instance_idx}"))
        base = game_node_base_id_for_host(node_host)
        node = clamp_int(base + (md5_int(f"nstage:{instance_idx}:{start_t.timestamp()}") % 20), 1, 250)
        ctx.update({"node": node, "stage": -1})
    elif flow_id == "campaign_log_line":
        actions = COMPONENTS["game_node"]["logs"]["campaign_log_line"]["vars"]["action"]["v"]
        action = actions[md5_int(f"act:{instance_idx}:{start_t.timestamp()}") % len(actions)]
        campaign_id = 1 + (md5_int(f"cid:{instance_idx}:{start_t.timestamp()}") % 5000)
        alliance_id = 1000 + (md5_int(f"aid:{instance_idx}:{start_t.timestamp()}") % 9000)
        ctx.update({"action": action, "campaign_id": campaign_id, "alliance_id": alliance_id})

    retry = flow["retry"]
    attempts = flow_attempt_counts(1, retry.get("expected_attempts", 1.0), retry.get("max_attempts", 1), f"att:{flow_id}:{instance_idx}")[0]

    flow_lat_mult = lat_mult.get(flow_id, {"p50": 1.0, "p95": 1.0})
    p50m = float(flow_lat_mult.get("p50", 1.0))
    p95m = float(flow_lat_mult.get("p95", 1.0))

    t = start_t
    trace_id = ""

    for attempt in range(1, attempts + 1):
        for j, ref in enumerate(retry.get("emit_per_retry", [])):
            lt = LOG_TEMPLATES[ref]
            cid = lt["component_id"]
            host = comp_host.get(cid, choose_host(cid, f"retry:{flow_id}:{instance_idx}:{attempt}"))
            msg = render_from_template(ref, state, ctx, f"{flow_id}:{instance_idx}:retry:{attempt}:{j}")
            rows.append({"_dt": t, "level": lt["lvl"], "message": msg, "trace_id": trace_id, "service": COMPONENTS[cid].get("svc", "") or "", "host": host})

        for k, ref in enumerate(flow["emit"]):
            p50, p95 = flow["latency_ms"][k]
            if state == "f" and flow_id in lat_mult:
                p50 = p50 * p50m
                p95 = p95 * p95m
            delay = lognormal_ms_from_p50_p95(p50, p95, f"lat:{flow_id}:{instance_idx}:{attempt}:{k}")
            t = t + timedelta(milliseconds=delay)

            lt = LOG_TEMPLATES[ref]
            cid = lt["component_id"]
            host = comp_host.get(cid, choose_host(cid, f"emit:{flow_id}:{instance_idx}:{attempt}:{k}"))

            bound = dict(ctx)
            if ref == "login_gateway.login_ok":
                dom = domain_for_var(LOG_TEMPLATES[ref], state, "ms")
                lo, hi = int(dom["v"][0]), int(dom["v"][1])
                bound["ms"] = clamp_int(int(delay), lo, hi)

            msg = render_from_template(ref, state, bound, f"{flow_id}:{instance_idx}:{attempt}:{k}")
            rows.append({"_dt": t, "level": lt["lvl"], "message": msg, "trace_id": trace_id, "service": COMPONENTS[cid].get("svc", "") or "", "host": host})

        if attempt < attempts and retry.get("backoff_ms"):
            p50b, p95b = retry["backoff_ms"][attempt - 1]
            bo = lognormal_ms_from_p50_p95(p50b, p95b, f"bo:{flow_id}:{instance_idx}:{attempt}")
            t = t + timedelta(milliseconds=bo)


def simulate() -> pd.DataFrame:
    base_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    carries: Dict[str, float] = {}
    rows: List[Dict[str, Any]] = []

    n_phase = SCENARIO["time"]["phases"]["n"]
    f_phase = SCENARIO["time"]["phases"]["f"]

    normal_intervals = [{"state": "n", "start_min": n_phase["start_min"], "end_min": n_phase["end_min"], "rate_mult": {}, "lat_mult": {}}]

    failure_events = SCENARIO["phases"]["f"]["events"]
    failure_intervals = [{"state": "f", **it} for it in build_failure_intervals(f_phase["start_min"], f_phase["end_min"], failure_events)]

    all_intervals = normal_intervals + failure_intervals

    # Background emissions per interval
    for it in all_intervals:
        state = it["state"]
        start_dt = base_time + timedelta(minutes=it["start_min"])
        end_dt = base_time + timedelta(minutes=it["end_min"])
        duration_min = it["end_min"] - it["start_min"]
        rate_mult = it.get("rate_mult", {})

        for comp in SYSTEM["components"]:
            cid = comp["id"]
            beh_emit = comp.get("beh", {}).get(state, {}).get("emit", [])
            for em in beh_emit:
                lid = em["id"]
                per_min = float(em["per_min"])
                scope = em.get("scope", "per_host")
                if state == "f":
                    per_min *= float(rate_mult.get(f"{cid}.{lid}", 1.0))
                if per_min <= 0.0:
                    continue

                lt_ref = f"{cid}.{lid}"
                lt = LOG_TEMPLATES[lt_ref]

                if scope == "global":
                    expected = per_min * duration_min
                    ckey = f"bg:{state}:{cid}.{lid}:global:{it['start_min']}-{it['end_min']}"
                    count = allocate_int(expected, ckey, carries)
                    times = spread_times(start_dt, end_dt, count, ckey)
                    for idx, t in enumerate(times):
                        host = choose_host(cid, f"{ckey}:{idx}:{t.timestamp()}")
                        minute = int((t - base_time).total_seconds() // 60)
                        bound = make_background_bound(cid, lid, state, host, t, minute, rate_mult)
                        msg = render_from_template(lt_ref, state, bound, f"{ckey}:{idx}")
                        rows.append({"_dt": t, "level": lt["lvl"], "message": msg, "trace_id": "", "service": comp.get("svc", "") or "", "host": host})
                else:
                    hosts = comp.get("hosts", []) or [""]
                    for h in hosts:
                        expected = per_min * duration_min
                        ckey = f"bg:{state}:{cid}.{lid}:{h}:{it['start_min']}-{it['end_min']}"
                        count = allocate_int(expected, ckey, carries)
                        times = spread_times(start_dt, end_dt, count, ckey)
                        for idx, t in enumerate(times):
                            minute = int((t - base_time).total_seconds() // 60)
                            bound = make_background_bound(cid, lid, state, h, t, minute, rate_mult)
                            msg = render_from_template(lt_ref, state, bound, f"{ckey}:{idx}")
                            rows.append({"_dt": t, "level": lt["lvl"], "message": msg, "trace_id": "", "service": comp.get("svc", "") or "", "host": h})

    # Flow instances per interval
    for it in all_intervals:
        state = it["state"]
        start_dt = base_time + timedelta(minutes=it["start_min"])
        end_dt = base_time + timedelta(minutes=it["end_min"])
        duration_min = it["end_min"] - it["start_min"]
        rate_mult = it.get("rate_mult", {})
        lat_mult = it.get("lat_mult", {})

        for flow in SYSTEM["flows"][state]["req"]:
            rpm = float(flow["rpm"])
            if state == "f":
                rpm *= float(rate_mult.get(flow["id"], 1.0))
            if rpm <= 0.0:
                continue
            expected_instances = rpm * duration_min
            ckey = f"flow:{state}:{flow['id']}:{it['start_min']}-{it['end_min']}"
            num_instances = allocate_int(expected_instances, ckey, carries)
            starts = spread_times(start_dt, end_dt, num_instances, ckey)
            interval_controls = {"rate_mult": rate_mult, "lat_mult": lat_mult}
            for i, st in enumerate(starts):
                simulate_flow_instance(flow, state, st, i, interval_controls, rows)

    # One-shots at failure events (exact counts, not scaled)
    for ev in sorted(failure_events, key=lambda e: (e["at_min"], e.get("order", 0))):
        ev_dt = base_time + timedelta(minutes=ev["at_min"])
        active_rate_mult, _ = get_active_controls_at_min(failure_events, ev["at_min"])
        for os in ev.get("one_shots", []):
            ref = os["ref"]
            count = int(os["count"])
            allowed_hosts = os.get("hosts", [])
            lt = LOG_TEMPLATES[ref]
            cid = lt["component_id"]
            comp = COMPONENTS[cid]
            for i in range(count):
                j_ms = int(50 * i + 200 * (stable_u01(f"oneshot:{ref}:{ev['at_min']}:{i}") - 0.5))
                t = ev_dt + timedelta(milliseconds=j_ms)
                host = allowed_hosts[i % len(allowed_hosts)] if allowed_hosts else choose_host(cid, f"os:{ref}:{ev['at_min']}:{i}")
                minute = int((t - base_time).total_seconds() // 60)
                bound = make_background_bound(cid, lt["log_id"], "f", host, t, minute, active_rate_mult)
                msg = render_from_template(ref, "f", bound, f"os:{ref}:{ev['at_min']}:{i}")
                rows.append({"_dt": t, "level": lt["lvl"], "message": msg, "trace_id": "", "service": comp.get("svc", "") or "", "host": host})

    df = pd.DataFrame(rows)
    df.sort_values(by="_dt", inplace=True, kind="mergesort")
    df["timestamp"] = df["_dt"].apply(iso_utc_ms)
    df.drop(columns=["_dt"], inplace=True)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    for c in ["timestamp", "level", "message", "trace_id", "service", "host"]:
        df[c] = df[c].fillna("").astype(str)
    return df


def main() -> None:
    df = simulate()
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
