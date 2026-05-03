import math
import hashlib
import uuid
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd


SYSTEM: Dict[str, Any] = {
    "sys": {"id": "git_hosting_stack_dec22"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["web_app"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "web_app",
            "svc": "git-web",
            "hosts": ["web-1", "web-2"],
            "logs": {
                "storage_call": {
                    "lvl": "INFO",
                    "msg": "storage call op={op} repo={repo} server={fs_host} timeout_ms={timeout_ms} trace={trace_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["read_pack", "write_pack", "list_refs"]},
                        "repo": {"k": "ch", "v": ["octo/app", "octo/lib", "octo/site"]},
                        "timeout_ms": {"k": "i", "v": [800, 6000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                    "state_vars": {
                        "n": {"fs_host": {"k": "ch", "v": ["fs-a1", "fs-b1"]}},
                        "f": {"fs_host": {"k": "ch", "v": ["fs-a1", "fs-a2", "fs-b1", "fs-b2"]}},
                    },
                },
                "upstream_timeout": {
                    "lvl": "ERROR",
                    "msg": "storage timeout op={op} repo={repo} waited_ms={waited_ms} server={fs_host} trace={trace_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["read_pack", "write_pack", "list_refs"]},
                        "repo": {"k": "ch", "v": ["octo/app", "octo/lib", "octo/site"]},
                        "waited_ms": {"k": "i", "v": [900, 6000]},
                        "fs_host": {"k": "ch", "v": ["fs-a1", "fs-a2", "fs-b1", "fs-b2"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_ok": {
                    "lvl": "INFO",
                    "msg": "request completed status=200 method={method} route={route} repo={repo} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/git"]},
                        "repo": {"k": "ch", "v": ["octo/app", "octo/lib", "octo/site"]},
                        "dur_ms": {"k": "i", "v": [20, 450]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_error": {
                    "lvl": "WARN",
                    "msg": "request failed status={status} route={route} repo={repo} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "status": {"k": "ch", "v": ["500", "504"]},
                        "route": {"k": "ch", "v": ["/git"]},
                        "repo": {"k": "ch", "v": ["octo/app", "octo/lib", "octo/site"]},
                        "dur_ms": {"k": "i", "v": [600, 8000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "access_maintenance": {
                    "lvl": "INFO",
                    "msg": "request blocked by maintenance status=503 route={route} repo={repo} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "route": {"k": "ch", "v": ["/git"]},
                        "repo": {"k": "ch", "v": ["octo/app", "octo/lib", "octo/site"]},
                        "dur_ms": {"k": "i", "v": [2, 60]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "maintenance_mode_enabled": {
                    "lvl": "WARN",
                    "msg": "maintenance mode enabled={enabled} reason={reason} actor={actor}",
                    "vars": {
                        "enabled": {"k": "ch", "v": ["true"]},
                        "reason": {"k": "ch", "v": ["fileserver_recovery", "widespread_storage_errors"]},
                        "actor": {"k": "ch", "v": ["ops"]},
                    },
                },
                "app_health": {
                    "lvl": "INFO",
                    "msg": "health workers_busy={busy} queue_depth={q} mem_mb={mem}",
                    "vars": {
                        "busy": {"k": "i", "v": [0, 32]},
                        "q": {"k": "i", "v": [0, 80]},
                        "mem": {"k": "i", "v": [900, 2400]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "app_health", "per_min": 0.2}]},
                "f": {"emit": [{"id": "app_health", "per_min": 0.3}]},
            },
        },
        {
            "id": "fileserver_cluster",
            "svc": "repo-storage",
            "hosts": ["fs-a1", "fs-a2", "fs-b1", "fs-b2"],
            "logs": {
                "storage_op_ok": {
                    "lvl": "INFO",
                    "msg": "storage op ok op={op} repo={repo} node={node} bytes={bytes} dur_ms={dur_ms}",
                    "vars": {
                        "op": {"k": "ch", "v": ["read_pack", "write_pack", "list_refs"]},
                        "repo": {"k": "ch", "v": ["octo/app", "octo/lib", "octo/site"]},
                        "node": {"k": "ch", "v": ["fs-a1", "fs-b1"]},
                        "bytes": {"k": "i", "v": [512, 8000000]},
                        "dur_ms": {"k": "i", "v": [5, 300]},
                    },
                },
                "drbd_replication_ok": {
                    "lvl": "INFO",
                    "msg": "drbd status res={res} role={role} peer={peer} link={link} lag_kb={lag_kb}",
                    "vars": {
                        "res": {"k": "ch", "v": ["repo-volume-a", "repo-volume-b"]},
                        "role": {"k": "ch", "v": ["Primary", "Secondary"]},
                        "peer": {"k": "ch", "v": ["fs-a2", "fs-b2"]},
                        "link": {"k": "ch", "v": ["Connected"]},
                        "lag_kb": {"k": "i", "v": [0, 20480]},
                    },
                },
                "heartbeat_timeout": {
                    "lvl": "WARN",
                    "msg": "heartbeat timeout res={res} peer={peer} elapsed_ms={elapsed_ms} local={local}",
                    "vars": {
                        "res": {"k": "ch", "v": ["repo-volume-a", "repo-volume-b"]},
                        "peer": {"k": "ch", "v": ["fs-a1", "fs-a2", "fs-b1", "fs-b2"]},
                        "elapsed_ms": {"k": "i", "v": [1500, 20000]},
                        "local": {"k": "ch", "v": ["fs-a1", "fs-a2", "fs-b1", "fs-b2"]},
                    },
                },
                "stonith_sent": {
                    "lvl": "WARN",
                    "msg": "stonith request target={target} fence={fence} reason={reason} local={local}",
                    "vars": {
                        "target": {"k": "ch", "v": ["fs-a1", "fs-a2", "fs-b1", "fs-b2"]},
                        "fence": {"k": "ch", "v": ["ipmi", "pdu"]},
                        "reason": {"k": "ch", "v": ["heartbeat_lost", "peer_unresponsive"]},
                        "local": {"k": "ch", "v": ["fs-a1", "fs-a2", "fs-b1", "fs-b2"]},
                    },
                },
                "stonith_ack_timeout": {
                    "lvl": "ERROR",
                    "msg": "stonith ack timeout target={target} fence={fence} waited_ms={waited_ms} local={local}",
                    "vars": {
                        "target": {"k": "ch", "v": ["fs-a1", "fs-a2", "fs-b1", "fs-b2"]},
                        "fence": {"k": "ch", "v": ["ipmi", "pdu"]},
                        "waited_ms": {"k": "i", "v": [5000, 60000]},
                        "local": {"k": "ch", "v": ["fs-a1", "fs-a2", "fs-b1", "fs-b2"]},
                    },
                },
                "resource_stop": {
                    "lvl": "WARN",
                    "msg": "pacemaker stopping resource={res} node={node} reason={reason}",
                    "vars": {
                        "res": {"k": "ch", "v": ["repo-volume-a", "repo-volume-b"]},
                        "node": {"k": "ch", "v": ["fs-a1", "fs-a2", "fs-b1", "fs-b2"]},
                        "reason": {"k": "ch", "v": ["fencing_pending", "role_conflict", "quorum_lost"]},
                    },
                },
                "resource_start": {
                    "lvl": "INFO",
                    "msg": "pacemaker starting resource={res} node={node} mode={mode}",
                    "vars": {
                        "res": {"k": "ch", "v": ["repo-volume-a", "repo-volume-b"]},
                        "node": {"k": "ch", "v": ["fs-a1", "fs-a2", "fs-b1", "fs-b2"]},
                        "mode": {"k": "ch", "v": ["recover_readonly", "promote_primary"]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "drbd_replication_ok", "per_min": 0.25}]},
                "f": {
                    "emit": [
                        {"id": "drbd_replication_ok", "per_min": 0.2},
                        {"id": "heartbeat_timeout", "per_min": 0.5},
                        {"id": "stonith_sent", "per_min": 0.3},
                        {"id": "stonith_ack_timeout", "per_min": 0.1},
                        {"id": "resource_stop", "per_min": 0.2},
                        {"id": "resource_start", "per_min": 0.1},
                    ]
                },
            },
        },
        {
            "id": "agg_switch_pair",
            "svc": "net-agg",
            "hosts": ["agg-1", "agg-2"],
            "logs": {
                "upgrade_step": {
                    "lvl": "INFO",
                    "msg": "isssu step={step} switch={sw} target_version={ver}",
                    "vars": {
                        "step": {"k": "ch", "v": ["upload_image", "set_boot", "reload", "postcheck"]},
                        "sw": {"k": "ch", "v": ["agg-1", "agg-2"]},
                        "ver": {"k": "ch", "v": ["v9.2.1", "v9.1.7"]},
                    },
                },
                "agent_state_dump": {
                    "lvl": "WARN",
                    "msg": "terminating agent={agent} for state dump pid={pid} sw={sw}",
                    "vars": {
                        "agent": {"k": "ch", "v": ["mlag-agent"]},
                        "pid": {"k": "i", "v": [1200, 9800]},
                        "sw": {"k": "ch", "v": ["agg-1"]},
                    },
                },
                "mlag_heartbeat_missed": {
                    "lvl": "WARN",
                    "msg": "mlag heartbeat missed peer={peer} missed={missed} peer_link={peer_link}",
                    "vars": {
                        "peer": {"k": "ch", "v": ["agg-1", "agg-2"]},
                        "missed": {"k": "i", "v": [1, 12]},
                        "peer_link": {"k": "ch", "v": ["up"]},
                    },
                },
                "mlag_mode_change": {
                    "lvl": "ERROR",
                    "msg": "mlag mode change new_mode={mode} reason={reason} sw={sw}",
                    "vars": {
                        "mode": {"k": "ch", "v": ["standalone"]},
                        "reason": {"k": "ch", "v": ["peer_heartbeat_timeout_link_up"]},
                        "sw": {"k": "ch", "v": ["agg-2"]},
                    },
                },
                "stp_topology_change": {
                    "lvl": "INFO",
                    "msg": "stp topology change vlan={vlan} root={root} ports_blocked={ports}",
                    "vars": {
                        "vlan": {"k": "i", "v": [1, 120]},
                        "root": {"k": "ch", "v": ["agg-1", "agg-2"]},
                        "ports": {"k": "i", "v": [5, 180]},
                    },
                },
                "l2_forwarding_pause": {
                    "lvl": "ERROR",
                    "msg": "l2 forwarding paused duration_ms={duration_ms} cause={cause}",
                    "vars": {
                        "duration_ms": {"k": "i", "v": [60000, 120000]},
                        "cause": {"k": "ch", "v": ["stp_reconvergence_mlag_fallback"]},
                    },
                },
                "mlag_peer_ok": {
                    "lvl": "INFO",
                    "msg": "mlag peer ok peer={peer} rtt_ms={rtt}",
                    "vars": {
                        "peer": {"k": "ch", "v": ["agg-1", "agg-2"]},
                        "rtt": {"k": "i", "v": [1, 8]},
                    },
                },
                "rollback_completed": {
                    "lvl": "INFO",
                    "msg": "software rollback completed target_version={ver} sw={sw}",
                    "vars": {"ver": {"k": "ch", "v": ["v9.1.7"]}, "sw": {"k": "ch", "v": ["agg-1", "agg-2"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "mlag_peer_ok", "per_min": 0.5}, {"id": "upgrade_step", "per_min": 0.05}]},
                "f": {"emit": [{"id": "mlag_heartbeat_missed", "per_min": 1.0}, {"id": "stp_topology_change", "per_min": 1.0}]},
            },
        },
        {
            "id": "ops_control",
            "svc": "ops",
            "hosts": ["ops-1"],
            "logs": {
                "page_allhands": {
                    "lvl": "WARN",
                    "msg": "paging group={group} incident={incident}",
                    "vars": {
                        "group": {"k": "ch", "v": ["operations_all"]},
                        "incident": {"k": "ch", "v": ["net_mlag_churn_fileserver_ha"]},
                    },
                },
                "rollback_initiated": {
                    "lvl": "INFO",
                    "msg": "initiating agg rollback target_version={ver} reason={reason}",
                    "vars": {"ver": {"k": "ch", "v": ["v9.1.7"]}, "reason": {"k": "ch", "v": ["mlag_instability"]}},
                },
                "recovery_progress": {
                    "lvl": "INFO",
                    "msg": "recovery progress pairs_remaining={pairs} action={action}",
                    "vars": {
                        "pairs": {"k": "i", "v": [0, 40]},
                        "action": {"k": "ch", "v": ["identify_last_primary", "start_resources", "verify_drbd_sync"]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": [{"id": "recovery_progress", "per_min": 0.8, "scope": "global"}]}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "git_request_ok",
                    "rpm": 300.0,
                    "emit": ["web_app.storage_call", "fileserver_cluster.storage_op_ok", "web_app.access_ok"],
                    "latency_ms": [[5, 20], [5, 120], [10, 260]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                }
            ]
        },
        "f": {
            "req": [
                {
                    "id": "git_request_storage_timeout",
                    "rpm": 300.0,
                    "emit": ["web_app.storage_call", "web_app.upstream_timeout", "web_app.access_error"],
                    "latency_ms": [[10, 80], [900, 5500], [20, 140]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "maintenance_request",
                    "rpm": 300.0,
                    "emit": ["web_app.access_maintenance"],
                    "latency_ms": [[2, 30]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "dec22_mlag_churn_fileserver_split_brain",
        "time": {"total_minutes": 40, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 40}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 20,
                        "rate_multipliers": {
                            "maintenance_request": 0.0,
                            "agg_switch_pair.mlag_heartbeat_missed": 15.0,
                            "agg_switch_pair.stp_topology_change": 20.0,
                            "fileserver_cluster.heartbeat_timeout": 0.0,
                            "fileserver_cluster.stonith_sent": 0.0,
                            "fileserver_cluster.stonith_ack_timeout": 0.0,
                            "fileserver_cluster.resource_stop": 0.0,
                            "fileserver_cluster.resource_start": 0.0,
                            "ops_control.recovery_progress": 0.0,
                        },
                        "latency_multipliers": {"git_request_storage_timeout": {"p50": 1.05, "p95": 1.05}},
                        "one_shots": [
                            {"ref": "agg_switch_pair.agent_state_dump", "count": 1, "hosts": ["agg-1"]},
                            {"ref": "agg_switch_pair.mlag_mode_change", "count": 1, "hosts": ["agg-2"]},
                            {"ref": "agg_switch_pair.l2_forwarding_pause", "count": 2, "hosts": ["agg-2"]},
                        ],
                    },
                    {
                        "order": 2,
                        "at_min": 22,
                        "rate_multipliers": {
                            "fileserver_cluster.heartbeat_timeout": 12.0,
                            "fileserver_cluster.stonith_sent": 10.0,
                            "fileserver_cluster.stonith_ack_timeout": 6.0,
                            "fileserver_cluster.resource_stop": 8.0,
                            "fileserver_cluster.resource_start": 2.0,
                        },
                        "latency_multipliers": {"git_request_storage_timeout": {"p50": 1.02, "p95": 1.02}},
                        "one_shots": [],
                    },
                    {
                        "order": 3,
                        "at_min": 26,
                        "rate_multipliers": {
                            "git_request_storage_timeout": 0.0,
                            "maintenance_request": 1.0,
                            "ops_control.recovery_progress": 1.0,
                        },
                        "latency_multipliers": {"maintenance_request": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [
                            {"ref": "ops_control.page_allhands", "count": 1, "hosts": ["ops-1"]},
                            {"ref": "ops_control.rollback_initiated", "count": 1, "hosts": ["ops-1"]},
                            {"ref": "web_app.maintenance_mode_enabled", "count": 1, "hosts": ["web-1"]},
                        ],
                    },
                    {
                        "order": 4,
                        "at_min": 32,
                        "rate_multipliers": {
                            "agg_switch_pair.mlag_heartbeat_missed": 1.2,
                            "agg_switch_pair.stp_topology_change": 1.0,
                            "fileserver_cluster.heartbeat_timeout": 4.0,
                            "fileserver_cluster.stonith_sent": 4.0,
                            "fileserver_cluster.stonith_ack_timeout": 2.0,
                            "fileserver_cluster.resource_stop": 3.0,
                            "fileserver_cluster.resource_start": 4.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [{"ref": "agg_switch_pair.rollback_completed", "count": 2, "hosts": ["agg-1", "agg-2"]}],
                    },
                ]
            }
        },
    }
}


SEED = 13371337
BASE_TIME = datetime(2026, 4, 1, 0, 0, 0, tzinfo=timezone.utc)

random.seed(SEED)
np.random.seed(SEED)


def md5_int(s: str) -> int:
    h = hashlib.md5(s.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big", signed=False)


def rng_for(key: str) -> np.random.Generator:
    return np.random.default_rng(md5_int(f"{SEED}|{key}") % (2**63 - 1))


def trace_id_from(key: str) -> str:
    return hashlib.md5(f"{SEED}|trace|{key}".encode("utf-8")).hexdigest()[:32]


def iso_utc_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def round_expected(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    n = int(math.floor(expected))
    frac = expected - n
    if frac <= 1e-12:
        return n
    u = (md5_int(f"{SEED}|round|{key}") % 1_000_000) / 1_000_000.0
    return n + (1 if u < frac else 0)


def parse_placeholders(fmt: str) -> List[str]:
    import string

    fields: List[str] = []
    for _, field, _, _ in string.Formatter().parse(fmt):
        if field:
            fields.append(field)
    return fields


def sample_from_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    r = rng_for(f"dom|{key}")
    if k == "ch":
        choices = list(v)
        idx = int(r.integers(0, len(choices)))
        return choices[idx]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return int(r.integers(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return float(lo + (hi - lo) * r.random())
    if k == "uuid":
        hx = hashlib.md5(f"{SEED}|uuid|{key}".encode("utf-8")).hexdigest()
        return str(uuid.UUID(hx))
    if k == "hex":
        ln = int(v)
        return hashlib.md5(f"{SEED}|hex|{key}".encode("utf-8")).hexdigest()[:ln]
    if k == "ip":
        h = md5_int(f"{SEED}|ip|{key}")
        return f"10.{(h>>16)&255}.{(h>>8)&255}.{h&255}"
    if k == "str":
        return f"s-{hashlib.md5(f'{SEED}|str|{key}'.encode('utf-8')).hexdigest()[:8]}"
    return ""


def lognormal_ms_from_p50_p95(p50: float, p95: float, key: str, cap: Optional[float] = None, hard_min: float = 1.0) -> float:
    p50 = max(0.1, float(p50))
    p95 = max(p50, float(p95))
    if p95 == p50:
        val = p50
    else:
        sigma = math.log(p95 / p50) / 1.645
        mu = math.log(p50)
        r = rng_for(f"ln|{key}")
        val = float(r.lognormal(mean=mu, sigma=max(1e-6, sigma)))
    if cap is not None:
        val = min(val, float(cap))
    val = max(val, float(hard_min))
    return val


def evenly_spaced_times(start: datetime, end: datetime, count: int, key: str, jitter_ms: int = 500) -> List[datetime]:
    if count <= 0:
        return []
    total_s = (end - start).total_seconds()
    if total_s <= 0:
        return [start for _ in range(count)]
    times: List[datetime] = []
    for i in range(count):
        base = start + timedelta(seconds=(i + 0.5) * total_s / count)
        r = rng_for(f"ts|{key}|{i}")
        jitter = (r.random() - 0.5) * 2.0 * jitter_ms
        t = base + timedelta(milliseconds=jitter)
        if t < start:
            t = start + timedelta(milliseconds=(i % 7))
        if t >= end:
            t = end - timedelta(milliseconds=1 + (i % 7))
        times.append(t)
    return times


@dataclass(frozen=True)
class Interval:
    state: str  # 'n' or 'f'
    start: datetime
    end: datetime
    rate_mult: Dict[str, float]  # keys: flow_id or "component.log_id"
    latency_mult: Dict[str, Dict[str, float]]  # keys: flow_id -> {p50,p95}


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]], Dict[Tuple[str, str], Dict[str, Any]]]:
    comp_by_id = {c["id"]: c for c in system["components"]}
    log_by_ref: Dict[str, Dict[str, Any]] = {}
    for cid, comp in comp_by_id.items():
        for lid, tmpl in comp.get("logs", {}).items():
            log_by_ref[f"{cid}.{lid}"] = tmpl
    flow_by_state_id: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for st in ["n", "f"]:
        for fdef in system["flows"][st]["req"]:
            flow_by_state_id[(st, fdef["id"])] = fdef
    return comp_by_id, log_by_ref, flow_by_state_id


COMP_BY_ID, LOG_BY_REF, FLOW_BY_STATE_ID = build_indices(SYSTEM)


def split_ref(ref: str) -> Tuple[str, str]:
    a, b = ref.split(".", 1)
    return a, b


def get_domain_for(template: Dict[str, Any], state: str, var: str) -> Optional[Dict[str, Any]]:
    sv = template.get("state_vars", {}).get(state, {})
    if var in sv:
        return sv[var]
    return template.get("vars", {}).get(var)


def render_message(component_id: str, log_id: str, state: str, values: Dict[str, Any]) -> Tuple[str, str]:
    tmpl = COMP_BY_ID[component_id]["logs"][log_id]
    msg_fmt = tmpl["msg"]
    needed = parse_placeholders(msg_fmt)
    full: Dict[str, Any] = dict(values)

    for var in needed:
        if var in full:
            continue
        dom = get_domain_for(tmpl, state, var)
        if dom is None:
            full[var] = ""
        else:
            full[var] = sample_from_domain(dom, key=f"{component_id}.{log_id}|{state}|{var}|{values.get('_key','')}")
    return tmpl["lvl"], msg_fmt.format(**full)


def component_identity(component_id: str, host: str) -> Tuple[str, str]:
    comp = COMP_BY_ID[component_id]
    svc = comp.get("svc") or ""
    return svc, host or ""


def build_intervals() -> List[Interval]:
    scen = SCENARIO["scenario"]
    p = scen["time"]["phases"]
    n_start, n_end = int(p["n"]["start_min"]), int(p["n"]["end_min"])
    f_start, f_end = int(p["f"]["start_min"]), int(p["f"]["end_min"])
    events = list(sorted(scen["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"])))

    intervals: List[Interval] = []
    intervals.append(
        Interval(
            state="n",
            start=BASE_TIME + timedelta(minutes=n_start),
            end=BASE_TIME + timedelta(minutes=n_end),
            rate_mult={},
            latency_mult={},
        )
    )

    boundaries = [f_start] + [int(e["at_min"]) for e in events if f_start <= int(e["at_min"]) <= f_end] + [f_end]
    boundaries = sorted(set(boundaries))
    if boundaries[0] != f_start:
        boundaries = [f_start] + boundaries
    if boundaries[-1] != f_end:
        boundaries.append(f_end)

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}
    ev_i = 0
    for i in range(len(boundaries) - 1):
        s_min = boundaries[i]
        e_min = boundaries[i + 1]
        while ev_i < len(events) and int(events[ev_i]["at_min"]) == s_min:
            ev = events[ev_i]
            for k, v in ev.get("rate_multipliers", {}).items():
                active_rate[k] = float(v)
            for fk, mv in ev.get("latency_multipliers", {}).items():
                active_lat[fk] = {"p50": float(mv.get("p50", 1.0)), "p95": float(mv.get("p95", 1.0))}
            ev_i += 1

        intervals.append(
            Interval(
                state="f",
                start=BASE_TIME + timedelta(minutes=s_min),
                end=BASE_TIME + timedelta(minutes=e_min),
                rate_mult=dict(active_rate),
                latency_mult=dict(active_lat),
            )
        )
    return intervals


INTERVALS = build_intervals()


def bind_background_overrides(component_id: str, log_id: str, host: str, key: str) -> Dict[str, Any]:
    overrides: Dict[str, Any] = {"_key": key}

    if component_id == "fileserver_cluster":
        if log_id == "drbd_replication_ok":
            if host.startswith("fs-a"):
                overrides["res"] = "repo-volume-a"
                overrides["peer"] = "fs-a2"
            else:
                overrides["res"] = "repo-volume-b"
                overrides["peer"] = "fs-b2"
            overrides["role"] = "Primary" if host.endswith("1") else "Secondary"
            overrides["link"] = "Connected"
            r = rng_for(f"bg|{key}|lag")
            overrides["lag_kb"] = int(r.integers(0, 6000))
        elif log_id in ("heartbeat_timeout", "stonith_sent", "stonith_ack_timeout"):
            overrides["local"] = host
        elif log_id in ("resource_stop", "resource_start"):
            overrides["node"] = host

    if component_id == "agg_switch_pair":
        if log_id == "mlag_peer_ok":
            overrides["peer"] = "agg-2" if host == "agg-1" else "agg-1"
        elif log_id == "upgrade_step":
            overrides["sw"] = host
        elif log_id == "mlag_heartbeat_missed":
            overrides["peer"] = "agg-2" if host == "agg-1" else "agg-1"
            overrides["peer_link"] = "up"
        elif log_id == "rollback_completed":
            overrides["sw"] = host

    return overrides


def emit_row(
    rows: List[Dict[str, Any]],
    ts: datetime,
    component_id: str,
    log_id: str,
    state: str,
    host: str,
    trace_id: str,
    values: Dict[str, Any],
) -> None:
    lvl, msg = render_message(component_id, log_id, state, values)
    svc, h = component_identity(component_id, host)
    rows.append(
        {
            "timestamp_dt": ts,
            "timestamp": iso_utc_ms(ts),
            "level": lvl,
            "message": msg,
            "trace_id": trace_id,
            "service": svc,
            "host": h,
        }
    )


def simulate_background(rows: List[Dict[str, Any]], intervals: List[Interval]) -> None:
    for itv in intervals:
        state = itv.state
        for comp in SYSTEM["components"]:
            cid = comp["id"]
            beh = comp.get("beh", {}).get(state, {}).get("emit", [])
            if not beh:
                continue
            for src in beh:
                log_id = src["id"]
                per_min = float(src["per_min"])
                scope = src.get("scope", "per_host")
                mult_key = f"{cid}.{log_id}"
                mult = float(itv.rate_mult.get(mult_key, 1.0)) if state == "f" else 1.0
                eff = per_min * mult
                if eff <= 0:
                    continue

                duration_min = (itv.end - itv.start).total_seconds() / 60.0
                if duration_min <= 0:
                    continue

                if scope == "global":
                    expected = eff * duration_min
                    cnt = round_expected(expected, key=f"bg|{state}|{mult_key}|{itv.start.isoformat()}|global")
                    host = comp["hosts"][0] if comp.get("hosts") else ""
                    times = evenly_spaced_times(itv.start, itv.end, cnt, key=f"bg|{state}|{mult_key}|global|{itv.start.isoformat()}")
                    for j, ts in enumerate(times):
                        vals = bind_background_overrides(cid, log_id, host, key=f"bg|{mult_key}|{itv.start.isoformat()}|{j}")
                        emit_row(rows, ts, cid, log_id, state, host, "", vals)
                else:
                    for host in comp.get("hosts", []):
                        expected = eff * duration_min
                        cnt = round_expected(expected, key=f"bg|{state}|{mult_key}|{itv.start.isoformat()}|{host}")
                        if cnt <= 0:
                            continue
                        times = evenly_spaced_times(
                            itv.start, itv.end, cnt, key=f"bg|{state}|{mult_key}|{host}|{itv.start.isoformat()}"
                        )
                        for j, ts in enumerate(times):
                            vals = bind_background_overrides(cid, log_id, host, key=f"bg|{mult_key}|{host}|{itv.start.isoformat()}|{j}")
                            emit_row(rows, ts, cid, log_id, state, host, "", vals)


def pick_web_host(instance_key: str) -> str:
    hosts = COMP_BY_ID["web_app"]["hosts"]
    r = rng_for(f"webhost|{instance_key}")
    return hosts[int(r.integers(0, len(hosts)))]


def pick_fs_host(state: str, instance_key: str) -> str:
    tmpl = COMP_BY_ID["web_app"]["logs"]["storage_call"]
    dom = tmpl.get("state_vars", {}).get(state, {}).get("fs_host")
    if dom is None:
        choices = COMP_BY_ID["fileserver_cluster"]["hosts"]
        r = rng_for(f"fshost|{instance_key}")
        return choices[int(r.integers(0, len(choices)))]
    return str(sample_from_domain(dom, key=f"fshost|{state}|{instance_key}"))


def simulate_flow_instance(rows: List[Dict[str, Any]], flow: Dict[str, Any], state: str, start: datetime, itv: Interval, instance_seq: int) -> None:
    flow_id = flow["id"]
    instance_key = f"{state}|{flow_id}|{itv.start.isoformat()}|{instance_seq}"
    web_host = pick_web_host(instance_key)

    trace_id = ""
    if SYSTEM["tracing"]["on"] and flow.get("trace", False):
        trace_id = trace_id_from(instance_key)

    r = rng_for(f"ctx|{instance_key}")
    repo = ["octo/app", "octo/lib", "octo/site"][int(r.integers(0, 3))]
    op = ["read_pack", "write_pack", "list_refs"][int(r.integers(0, 3))]
    method = ["GET", "POST"][int(r.integers(0, 2))]
    route = "/git"

    fs_host = ""
    if "web_app.storage_call" in flow["emit"]:
        fs_host = pick_fs_host(state, instance_key)

    p50m, p95m = 1.0, 1.0
    if state == "f":
        lm = itv.latency_mult.get(flow_id)
        if lm:
            p50m = float(lm.get("p50", 1.0))
            p95m = float(lm.get("p95", 1.0))

    # Enforce per-hop hard minima that keep emitted timing fields within modeled variable domains and coherent with timestamps.
    # These minima mirror the smallest plausible latency between log lines in the flow definitions.
    hop_hard_min: Dict[str, List[float]] = {
        "git_request_ok": [5.0, 5.0, 10.0],  # ensures total >= 20ms (web_app.access_ok.dur_ms min)
        "git_request_storage_timeout": [10.0, 900.0, 20.0],  # ensures waited_ms min 900ms is coherent
        "maintenance_request": [2.0],
    }
    hard_mins = hop_hard_min.get(flow_id, [1.0 for _ in flow["latency_ms"]])

    pairs: List[List[float]] = flow["latency_ms"]
    dts: List[float] = []
    for j, (p50, p95) in enumerate(pairs):
        p50s, p95s = p50, p95
        if state == "f":
            p50s, p95s = p50 * p50m, p95 * p95m

        if flow_id == "git_request_ok":
            if j == 0:
                cap = 60.0
            elif j == 1:
                cap = 320.0
            else:
                cap = 600.0
        elif flow_id == "git_request_storage_timeout":
            if j == 0:
                cap = 250.0
            elif j == 1:
                cap = 6000.0
            else:
                cap = 500.0
        elif flow_id == "maintenance_request":
            cap = 60.0
        else:
            cap = 2000.0

        dt = lognormal_ms_from_p50_p95(
            p50s,
            p95s,
            key=f"{instance_key}|dt|{j}",
            cap=cap,
            hard_min=float(hard_mins[j]) if j < len(hard_mins) else 1.0,
        )
        dts.append(dt)

    # Keep normal-request total within access_ok domain (20..450ms) while staying timestamp-coherent.
    if flow_id == "git_request_ok":
        total_cap = 450.0
        total_min = 20.0
        if dts[0] + dts[1] > total_cap - 10.0:
            dts[1] = max(hard_mins[1], total_cap - 10.0 - dts[0])
        rem = max(hard_mins[2], total_cap - (dts[0] + dts[1]))
        dts[2] = min(dts[2], rem)

        cur_total = dts[0] + dts[1] + dts[2]
        if cur_total < total_min:
            # pad the last hop to make total reach the modeled min
            dts[2] += (total_min - cur_total)
    elif flow_id == "maintenance_request":
        dts[0] = min(dts[0], 60.0)

    ts_list: List[datetime] = []
    cur = start
    for dt in dts:
        cur = cur + timedelta(milliseconds=int(round(dt)))
        ts_list.append(cur)

    emits: List[str] = flow["emit"]
    total_ms = int(round((ts_list[-1] - start).total_seconds() * 1000.0)) if ts_list else 0

    for idx, ref in enumerate(emits):
        cid, lid = split_ref(ref)
        if cid == "web_app":
            host = web_host
        elif cid == "fileserver_cluster":
            host = fs_host if fs_host else COMP_BY_ID[cid]["hosts"][0]
        else:
            host = COMP_BY_ID[cid]["hosts"][0] if COMP_BY_ID[cid].get("hosts") else ""

        values: Dict[str, Any] = {"_key": instance_key, "trace_id": trace_id, "repo": repo}
        if lid in ("storage_call", "upstream_timeout", "storage_op_ok"):
            values["op"] = op
        if lid in ("access_ok",):
            values["method"] = method
            values["route"] = route
            values["dur_ms"] = int(max(20, min(450, total_ms)))
        if lid == "storage_op_ok":
            values["node"] = fs_host if fs_host else "fs-a1"
            values["dur_ms"] = int(max(5, min(300, int(round(dts[1])))))
            values["bytes"] = int(sample_from_domain(COMP_BY_ID[cid]["logs"][lid]["vars"]["bytes"], key=f"{instance_key}|bytes"))
        if lid == "storage_call":
            values["fs_host"] = fs_host if fs_host else pick_fs_host(state, instance_key)
            if flow_id == "git_request_storage_timeout":
                waited = int(round(dts[1]))
                timeout_ms = min(6000, max(800, waited + 200))
            else:
                timeout_ms = 800
            values["timeout_ms"] = int(timeout_ms)
        if lid == "upstream_timeout":
            values["fs_host"] = fs_host if fs_host else pick_fs_host(state, instance_key)
            values["waited_ms"] = int(max(900, min(6000, int(round(dts[1])))))
        if lid == "access_error":
            values["route"] = route
            values["dur_ms"] = int(max(600, min(8000, total_ms)))
            values["status"] = "504"
        if lid == "access_maintenance":
            values["route"] = route
            values["dur_ms"] = int(max(2, min(60, int(round(dts[0])))))

        emit_row(rows, ts_list[idx], cid, lid, state, host, trace_id, values)


def simulate_flows(rows: List[Dict[str, Any]], intervals: List[Interval]) -> None:
    instance_counters: Dict[Tuple[str, str, str], int] = {}
    for itv in intervals:
        state = itv.state
        flow_defs = SYSTEM["flows"][state]["req"]
        duration_min = (itv.end - itv.start).total_seconds() / 60.0
        if duration_min <= 0:
            continue
        for fdef in flow_defs:
            fid = fdef["id"]
            mult = float(itv.rate_mult.get(fid, 1.0)) if state == "f" else 1.0
            rpm_eff = float(fdef["rpm"]) * mult
            if rpm_eff <= 0:
                continue
            expected = rpm_eff * duration_min
            cnt = round_expected(expected, key=f"flow|{state}|{fid}|{itv.start.isoformat()}")
            if cnt <= 0:
                continue
            starts = evenly_spaced_times(itv.start, itv.end, cnt, key=f"flow|{state}|{fid}|{itv.start.isoformat()}", jitter_ms=700)
            for j, st in enumerate(starts):
                k = (state, fid, itv.start.isoformat())
                instance_seq = instance_counters.get(k, 0)
                instance_counters[k] = instance_seq + 1
                simulate_flow_instance(rows, fdef, state, st, itv, instance_seq)


def simulate_one_shots(rows: List[Dict[str, Any]]) -> None:
    events = list(sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"])))
    for ev in events:
        at_min = int(ev["at_min"])
        base_ts = BASE_TIME + timedelta(minutes=at_min)
        one_shots = ev.get("one_shots", [])
        for os_idx, os in enumerate(one_shots):
            ref = os["ref"]
            cnt = int(os["count"])
            allowed_hosts = list(os.get("hosts") or [])
            cid, lid = split_ref(ref)
            comp_hosts = COMP_BY_ID[cid].get("hosts", [])
            for k in range(cnt):
                if allowed_hosts:
                    host = allowed_hosts[k % len(allowed_hosts)]
                else:
                    host = comp_hosts[k % len(comp_hosts)] if comp_hosts else ""
                if comp_hosts and host not in comp_hosts:
                    host = comp_hosts[0]

                r = rng_for(f"oneshot|{ref}|{at_min}|{os_idx}|{k}")
                jitter_ms = int(r.integers(0, 5000))
                ts = base_ts + timedelta(milliseconds=jitter_ms)

                vals = bind_background_overrides(cid, lid, host, key=f"oneshot|{ref}|{at_min}|{os_idx}|{k}")
                emit_row(rows, ts, cid, lid, "f", host, "", vals)


def main() -> None:
    rows: List[Dict[str, Any]] = []

    simulate_background(rows, INTERVALS)
    simulate_flows(rows, INTERVALS)
    simulate_one_shots(rows)

    df = pd.DataFrame(rows)
    df = df.sort_values("timestamp_dt", kind="mergesort").drop(columns=["timestamp_dt"])
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
