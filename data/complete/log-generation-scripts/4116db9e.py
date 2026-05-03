import math
import hashlib
import uuid
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "cloud_us_backbone_control_plane"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "maintenance_automation": {
            "svc": "dc-maint-orch",
            "hosts": ["auto-1"],
            "logs": {
                "orchestrator_tick": {
                    "lvl": "DEBUG",
                    "msg": "tick loop_ok={loop_ok} queued_actions={queued_actions}",
                    "vars": {
                        "loop_ok": {"k": "ch", "v": ["true"]},
                        "queued_actions": {"k": "i", "v": [0, 5]},
                    },
                },
                "maintenance_start": {
                    "lvl": "INFO",
                    "msg": "maintenance started event_id={event_id} maint_type={maint_type} location={location} scope_clusters={scope_clusters}",
                    "vars": {
                        "event_id": {"k": "hex", "v": 16},
                        "maint_type": {"k": "ch", "v": ["rare_power_switchover"]},
                        "location": {"k": "ch", "v": ["loc-sfo1"]},
                        "scope_clusters": {"k": "i", "v": [5, 30]},
                    },
                },
                "automation_halted": {
                    "lvl": "WARN",
                    "msg": "automation halted event_id={event_id} reason={reason} operator={operator}",
                    "vars": {
                        "event_id": {"k": "hex", "v": 16},
                        "reason": {"k": "ch", "v": ["incident_mitigation"]},
                        "operator": {"k": "str", "v": "eng-oncall-*"},
                    },
                },
            },
            "beh": {
                "n": [{"id": "orchestrator_tick", "per_min": 0.5, "scope": "per_host"}],
                "f": [{"id": "orchestrator_tick", "per_min": 0.5, "scope": "per_host"}],
            },
        },
        "cluster_manager": {
            "svc": "cluster-mgr",
            "hosts": ["cm-1", "cm-2"],
            "logs": {
                "scheduler_tick": {
                    "lvl": "DEBUG",
                    "msg": "scheduler tick leader={leader} pending={pending} running={running}",
                    "vars": {
                        "leader": {"k": "ch", "v": ["true", "false"]},
                        "pending": {"k": "i", "v": [0, 50]},
                        "running": {"k": "i", "v": [500, 1200]},
                    },
                },
                "job_evicted": {
                    "lvl": "WARN",
                    "msg": "job evicted job={job} cluster={cluster} location={location} reason={reason}",
                    "vars": {
                        "job": {"k": "ch", "v": ["net-control-plane", "net-config-store"]},
                        "cluster": {"k": "ch", "v": ["netcp-a", "netcp-b"]},
                        "location": {"k": "ch", "v": ["loc-sfo1", "loc-ord1", "loc-iad1"]},
                        "reason": {"k": "ch", "v": ["maintenance_deschedule"]},
                    },
                },
                "job_rescheduled": {
                    "lvl": "INFO",
                    "msg": "job rescheduled job={job} cluster={cluster} location={location} placement_host={placement_host}",
                    "vars": {
                        "job": {"k": "ch", "v": ["net-control-plane", "net-config-store"]},
                        "cluster": {"k": "ch", "v": ["netcp-a", "netcp-b"]},
                        "location": {"k": "ch", "v": ["loc-sfo1", "loc-ord1", "loc-iad1"]},
                        "placement_host": {"k": "str", "v": "cm-node-[01-99]"},
                    },
                },
                "reschedule_batch_start": {
                    "lvl": "INFO",
                    "msg": "reschedule batch start batch_id={batch_id} targets={targets}",
                    "vars": {
                        "batch_id": {"k": "hex", "v": 12},
                        "targets": {"k": "i", "v": [2, 20]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "scheduler_tick", "per_min": 0.5, "scope": "per_host"}],
                "f": [
                    {"id": "scheduler_tick", "per_min": 0.5, "scope": "per_host"},
                    {"id": "job_evicted", "per_min": 0.2, "scope": "per_host"},
                    {"id": "job_rescheduled", "per_min": 0.2, "scope": "per_host"},
                ],
            },
        },
        "net_control_plane": {
            "svc": "net-control",
            "hosts": ["ncp-1", "ncp-2"],
            "logs": {
                "heartbeat": {
                    "lvl": "INFO",
                    "msg": "heartbeat instance={instance} location={location} routes_programmed={routes_programmed}",
                    "vars": {
                        "instance": {"k": "ch", "v": ["ncp-1", "ncp-2"]},
                        "location": {"k": "ch", "v": ["loc-sfo1", "loc-ord1", "loc-iad1"]},
                        "routes_programmed": {"k": "i", "v": [1000, 6000]},
                    },
                },
                "ncp_process_start": {
                    "lvl": "INFO",
                    "msg": "process start instance={instance} location={location} build={build}",
                    "vars": {
                        "instance": {"k": "ch", "v": ["ncp-1", "ncp-2"]},
                        "location": {"k": "ch", "v": ["loc-sfo1", "loc-ord1", "loc-iad1"]},
                        "build": {"k": "ch", "v": ["2019.06.02"]},
                    },
                },
                "config_rebuild_step": {
                    "lvl": "INFO",
                    "msg": "config rebuild location={location} step={step} progress_pct={progress_pct}",
                    "vars": {
                        "location": {"k": "ch", "v": ["loc-sfo1", "loc-ord1", "loc-iad1"]},
                        "step": {"k": "ch", "v": ["rebuild_kv", "regenerate_bgp", "distribute"]},
                        "progress_pct": {"k": "i", "v": [0, 100]},
                    },
                },
                "config_publish_started": {
                    "lvl": "INFO",
                    "msg": "config publish started location={location} version={version}",
                    "vars": {
                        "location": {"k": "ch", "v": ["loc-sfo1", "loc-ord1", "loc-iad1"]},
                        "version": {"k": "str", "v": "cfg-20190602-*"},
                    },
                },
            },
            "beh": {
                "n": [{"id": "heartbeat", "per_min": 1.0, "scope": "per_host"}],
                "f": [{"id": "config_rebuild_step", "per_min": 1.0, "scope": "per_host"}],
            },
        },
        "backbone_router": {
            "svc": "backbone-rtr",
            "hosts": ["rtr-a", "rtr-b"],
            "logs": {
                "link_utilization": {
                    "lvl": "INFO",
                    "msg": "link util link_id={link_id} util_pct={util_pct}",
                    "vars": {"link_id": {"k": "ch", "v": ["bb-central-east", "bb-central-northeast"]}},
                    "state_vars": {
                        "n": {"util_pct": {"k": "i", "v": [10, 60]}},
                        "f": {"util_pct": {"k": "i", "v": [30, 100]}},
                    },
                },
                "packet_loss_high": {
                    "lvl": "WARN",
                    "msg": "packet loss high region_pair={region_pair} loss_pct={loss_pct}",
                    "vars": {
                        "region_pair": {"k": "ch", "v": ["us-central1->us-east4", "us-central1->us-west2"]},
                        "loss_pct": {"k": "f", "v": [5.0, 40.0]},
                    },
                },
                "packet_loss_low": {
                    "lvl": "INFO",
                    "msg": "packet loss elevated region_pair={region_pair} loss_pct={loss_pct}",
                    "vars": {
                        "region_pair": {"k": "ch", "v": ["us-central1->us-east4", "us-central1->us-west2"]},
                        "loss_pct": {"k": "f", "v": [0.2, 5.0]},
                    },
                },
                "bgp_session_down": {
                    "lvl": "ERROR",
                    "msg": "bgp session down neighbor_ip={neighbor_ip} neighbor_asn={neighbor_asn} reason={reason}",
                    "vars": {
                        "neighbor_ip": {"k": "ip", "v": None},
                        "neighbor_asn": {"k": "i", "v": [64512, 65534]},
                        "reason": {"k": "ch", "v": ["hold_timer_expired", "route_withdrawn"]},
                    },
                },
                "qos_policy_update": {
                    "lvl": "WARN",
                    "msg": "qos policy update policy={policy} class={class} drop_pct={drop_pct}",
                    "vars": {
                        "policy": {"k": "ch", "v": ["deprioritize_best_effort"]},
                        "class": {"k": "ch", "v": ["besteffort"]},
                        "drop_pct": {"k": "i", "v": [5, 30]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "link_utilization", "per_min": 1.0, "scope": "per_host"}],
                "f": [
                    {"id": "link_utilization", "per_min": 1.0, "scope": "per_host"},
                    {"id": "packet_loss_high", "per_min": 1.0, "scope": "per_host"},
                    {"id": "packet_loss_low", "per_min": 1.0, "scope": "per_host"},
                ],
            },
        },
        "storage_api": {
            "svc": "storage-api",
            "hosts": ["st-api-1", "st-api-2", "st-api-3"],
            "logs": {
                "req_start": {
                    "lvl": "INFO",
                    "msg": "request start method={method} bucket={bucket} region={region} object={object} client_ip={client_ip} request_id={request_id} trace_id={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "PUT", "LIST"]},
                        "bucket": {"k": "ch", "v": ["img-prod", "logs-archive", "app-data"]},
                        "region": {"k": "ch", "v": ["us-east4"]},
                        "object": {"k": "str", "v": "obj/*"},
                        "client_ip": {"k": "ip", "v": None},
                        "request_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "req_end_200": {
                    "lvl": "INFO",
                    "msg": "request end request_id={request_id} status=200 bytes={bytes} dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "bytes": {"k": "i", "v": [200, 200000]},
                        "dur_ms": {"k": "i", "v": [5, 20000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "req_end_504": {
                    "lvl": "ERROR",
                    "msg": "request end request_id={request_id} status=504 err=upstream_timeout dur_ms={dur_ms} trace_id={trace_id}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [3000, 30000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "retry_attempt": {
                    "lvl": "WARN",
                    "msg": "retrying request_id={request_id} attempt={attempt} max_attempts={max_attempts} backoff_ms={backoff_ms}",
                    "vars": {
                        "request_id": {"k": "uuid", "v": None},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "max_attempts": {"k": "i", "v": [2, 3]},
                        "backoff_ms": {"k": "i", "v": [50, 1200]},
                    },
                },
                "request_stats": {
                    "lvl": "INFO",
                    "msg": "stats region={region} ok_rpm={ok_rpm} err_rpm={err_rpm} p95_ms={p95_ms}",
                    "vars": {"region": {"k": "ch", "v": ["us-east4"]}},
                    "state_vars": {
                        "n": {
                            "ok_rpm": {"k": "i", "v": [70, 130]},
                            "err_rpm": {"k": "i", "v": [0, 2]},
                            "p95_ms": {"k": "i", "v": [40, 140]},
                        },
                        "f": {
                            "ok_rpm": {"k": "i", "v": [20, 120]},
                            "err_rpm": {"k": "i", "v": [0, 80]},
                            "p95_ms": {"k": "i", "v": [120, 30000]},
                        },
                    },
                },
            },
            "beh": {
                "n": [{"id": "request_stats", "per_min": 1.0, "scope": "per_host"}],
                "f": [{"id": "request_stats", "per_min": 1.0, "scope": "per_host"}],
            },
        },
        "monitoring": {
            "svc": "mon-alert",
            "hosts": ["mon-1"],
            "logs": {
                "scrape_ok": {
                    "lvl": "INFO",
                    "msg": "scrape ok target={target} latency_ms={latency_ms}",
                    "vars": {
                        "target": {"k": "ch", "v": ["storage_api", "net_control_plane"]},
                        "latency_ms": {"k": "i", "v": [20, 400]},
                    },
                },
                "scrape_timeout": {
                    "lvl": "WARN",
                    "msg": "scrape timeout target={target} timeout_ms={timeout_ms}",
                    "vars": {
                        "target": {"k": "ch", "v": ["storage_api", "net_control_plane"]},
                        "timeout_ms": {"k": "i", "v": [500, 5000]},
                    },
                },
                "tool_query_timeout": {
                    "lvl": "WARN",
                    "msg": "tool query timeout tool={tool} dur_ms={dur_ms}",
                    "vars": {
                        "tool": {"k": "ch", "v": ["status_dashboard", "log_search"]},
                        "dur_ms": {"k": "i", "v": [800, 15000]},
                    },
                },
                "alert_open": {
                    "lvl": "CRITICAL",
                    "msg": "alert open alert={alert} severity={severity} details={details}",
                    "vars": {
                        "alert": {"k": "ch", "v": ["net_control_plane_heartbeat_missing", "packet_loss_high"]},
                        "severity": {"k": "ch", "v": ["SEV1"]},
                        "details": {"k": "str", "v": "summary:*"},
                    },
                },
            },
            "beh": {
                "n": [{"id": "scrape_ok", "per_min": 2.0, "scope": "global"}],
                "f": [
                    {"id": "scrape_timeout", "per_min": 2.0, "scope": "global"},
                    {"id": "tool_query_timeout", "per_min": 0.5, "scope": "global"},
                ],
            },
        },
    },
    "flows": {
        "n": [
            {
                "id": "storage_object_get_n",
                "rpm": 300.0,
                "emit": ["storage_api.req_start", "storage_api.req_end_200"],
                "latency_ms": [[5, 15], [20, 80]],
                "retry": {
                    "max_attempts": 2,
                    "expected_attempts": 1.05,
                    "emit_per_retry": ["storage_api.retry_attempt"],
                    "backoff_ms": [[50, 150]],
                },
                "trace": True,
            }
        ],
        "f": [
            {
                "id": "storage_object_get_ok_f",
                "rpm": 260.0,
                "emit": ["storage_api.req_start", "storage_api.req_end_200"],
                "latency_ms": [[5, 20], [60, 250]],
                "retry": {
                    "max_attempts": 3,
                    "expected_attempts": 1.2,
                    "emit_per_retry": ["storage_api.retry_attempt"],
                    "backoff_ms": [[100, 300], [200, 600]],
                },
                "trace": True,
            },
            {
                "id": "storage_object_get_timeout_f",
                "rpm": 10.0,
                "emit": ["storage_api.req_start", "storage_api.req_end_504"],
                "latency_ms": [[3000, 12000], [2000, 18000]],
                "retry": {
                    "max_attempts": 3,
                    "expected_attempts": 1.8,
                    "emit_per_retry": ["storage_api.retry_attempt"],
                    "backoff_ms": [[200, 600], [400, 1200]],
                },
                "trace": True,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "us_backbone_control_plane_deschedule_20190602",
        "time": {
            "total_minutes": 60,
            "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 60}},
        },
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 25,
                        "rate_multipliers": {
                            "storage_object_get_timeout_f": 0.2,
                            "cluster_manager.job_evicted": 15.0,
                            "cluster_manager.job_rescheduled": 0.0,
                            "net_control_plane.config_rebuild_step": 0.0,
                            "backbone_router.packet_loss_high": 0.0,
                            "backbone_router.packet_loss_low": 0.0,
                            "monitoring.scrape_timeout": 0.0,
                            "monitoring.tool_query_timeout": 0.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "maintenance_automation.maintenance_start", "count": 1, "hosts": ["auto-1"]}
                        ],
                    },
                    {
                        "order": 2,
                        "at_min": 27,
                        "rate_multipliers": {},
                        "latency_multipliers": {},
                        "one_shots": [{"ref": "monitoring.alert_open", "count": 1, "hosts": ["mon-1"]}],
                    },
                    {
                        "order": 3,
                        "at_min": 29,
                        "rate_multipliers": {
                            "storage_object_get_ok_f": 0.8,
                            "storage_object_get_timeout_f": 12.0,
                            "cluster_manager.job_evicted": 1.5,
                            "backbone_router.packet_loss_high": 1.0,
                            "backbone_router.packet_loss_low": 0.0,
                            "monitoring.scrape_timeout": 1.0,
                            "monitoring.tool_query_timeout": 1.0,
                        },
                        "latency_multipliers": {"storage_object_get_ok_f": {"p50": 3.0, "p95": 4.0}},
                        "one_shots": [
                            {"ref": "backbone_router.bgp_session_down", "count": 8, "hosts": ["rtr-a", "rtr-b"]},
                            {"ref": "monitoring.alert_open", "count": 1, "hosts": ["mon-1"]},
                        ],
                    },
                    {
                        "order": 4,
                        "at_min": 40,
                        "rate_multipliers": {
                            "storage_object_get_ok_f": 0.9,
                            "storage_object_get_timeout_f": 6.0,
                            "cluster_manager.job_evicted": 0.0,
                            "cluster_manager.job_rescheduled": 5.0,
                            "net_control_plane.config_rebuild_step": 1.0,
                            "monitoring.tool_query_timeout": 1.2,
                        },
                        "latency_multipliers": {"storage_object_get_ok_f": {"p50": 2.5, "p95": 3.5}},
                        "one_shots": [
                            {"ref": "maintenance_automation.automation_halted", "count": 1, "hosts": ["auto-1"]},
                            {"ref": "cluster_manager.reschedule_batch_start", "count": 1, "hosts": ["cm-1"]},
                            {"ref": "net_control_plane.ncp_process_start", "count": 2, "hosts": ["ncp-1", "ncp-2"]},
                            {"ref": "backbone_router.qos_policy_update", "count": 1, "hosts": ["rtr-a"]},
                        ],
                    },
                    {
                        "order": 5,
                        "at_min": 50,
                        "rate_multipliers": {
                            "storage_object_get_ok_f": 1.0,
                            "storage_object_get_timeout_f": 3.0,
                            "backbone_router.packet_loss_high": 0.2,
                            "backbone_router.packet_loss_low": 1.0,
                            "cluster_manager.job_rescheduled": 1.5,
                            "monitoring.scrape_timeout": 0.6,
                            "monitoring.tool_query_timeout": 0.6,
                        },
                        "latency_multipliers": {"storage_object_get_ok_f": {"p50": 1.8, "p95": 2.5}},
                        "one_shots": [
                            {"ref": "net_control_plane.config_publish_started", "count": 3, "hosts": ["ncp-1", "ncp-2"]}
                        ],
                    },
                ]
            }
        },
    }
}

SEED = 1337
BASE_TIME = datetime(2019, 6, 2, 0, 0, 0, tzinfo=timezone.utc)


def stable_u32(s: str) -> int:
    h = hashlib.md5(s.encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big", signed=False)


def rng_for(*parts: Any) -> np.random.Generator:
    s = "|".join(str(p) for p in parts)
    return np.random.default_rng(stable_u32(f"{SEED}|{s}"))


def iso_ms(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond/1000):03d}Z"


def det_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    fl = int(math.floor(expected))
    frac = expected - fl
    if frac <= 0:
        return fl
    r = stable_u32(f"round|{key}") / 2**32
    return fl + (1 if r < frac else 0)


def schedule_times(start: datetime, end: datetime, n: int, key: str, jitter_ms: int = 400) -> List[datetime]:
    if n <= 0:
        return []
    dur_ms = max(1.0, (end - start).total_seconds() * 1000.0)
    times: List[datetime] = []
    for i in range(n):
        base_ms = (i + 0.5) * dur_ms / n
        j = (stable_u32(f"jit|{key}|{i}") / 2**32 - 0.5) * 2.0 * jitter_ms
        ms = min(max(0.0, base_ms + j), dur_ms - 1.0)
        times.append(start + timedelta(milliseconds=ms))
    return times


def sample_lognormal_ms(p50: float, p95: float, rng: np.random.Generator, cap_mult: float = 3.0) -> float:
    p50 = max(0.1, float(p50))
    p95 = max(p50 * 1.01, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - mu) / 1.645
    z = float(rng.normal())
    x = math.exp(mu + sigma * z)
    cap = cap_mult * p95
    if x > cap:
        x = cap * (0.90 + 0.10 * float(rng.random()))
    return x


def gen_hex(rng: np.random.Generator, n: int) -> str:
    b = rng.integers(0, 256, size=(math.ceil(n / 2),), dtype=np.uint8).tobytes()
    return b.hex()[:n]


def gen_uuid_str(rng: np.random.Generator) -> str:
    b = rng.integers(0, 256, size=(16,), dtype=np.uint8).tobytes()
    return str(uuid.UUID(bytes=b))


def gen_ip_str(rng: np.random.Generator, pool: str = "198.51.100") -> str:
    last = int(rng.integers(1, 255))
    return f"{pool}.{last}"


def gen_hint_str(rng: np.random.Generator, hint: str) -> str:
    if hint == "obj/*":
        return "obj/" + gen_hex(rng, 10)
    if hint == "cm-node-[01-99]":
        n = int(rng.integers(1, 100))
        return f"cm-node-{n:02d}"
    if hint == "eng-oncall-*":
        who = ["alice", "bob", "carol", "dana"][int(rng.integers(0, 4))]
        return f"eng-oncall-{who}"
    if hint == "cfg-20190602-*":
        return "cfg-20190602-" + gen_hex(rng, 8)
    if hint == "summary:*":
        tail = ["packet loss high", "heartbeat missing", "api timeouts", "backbone congestion"][int(rng.integers(0, 4))]
        return "summary:" + tail
    if "*" in hint:
        return hint.replace("*", gen_hex(rng, 6))
    return hint


def generate_from_domain(spec: Dict[str, Any], rng: np.random.Generator) -> Any:
    k = spec.get("k")
    v = spec.get("v")
    if k == "ch":
        lst = list(v)
        return lst[int(rng.integers(0, len(lst)))]
    if k == "i":
        a, b = int(v[0]), int(v[1])
        return int(rng.integers(a, b + 1))
    if k == "f":
        a, b = float(v[0]), float(v[1])
        x = float(rng.random()) * (b - a) + a
        return float(f"{x:.1f}")
    if k == "uuid":
        return gen_uuid_str(rng)
    if k == "hex":
        return gen_hex(rng, int(v))
    if k == "ip":
        return gen_ip_str(rng)
    if k == "str":
        return gen_hint_str(rng, str(v))
    return ""


def parse_ref(ref: str) -> Tuple[str, str]:
    comp_id, log_id = ref.split(".", 1)
    return comp_id, log_id


def emit_log_row(
    rows: List[Dict[str, str]],
    ts: datetime,
    ref: str,
    state: str,
    trace_id: str,
    host: str,
    overrides: Optional[Dict[str, Any]] = None,
    rng_key: Optional[str] = None,
) -> None:
    comp_id, log_id = parse_ref(ref)
    comp = SYSTEM["components"][comp_id]
    tpl = comp["logs"][log_id]
    vars_spec = dict(tpl.get("vars", {}))
    state_vars = tpl.get("state_vars", {})
    if state in state_vars:
        for k, v in state_vars[state].items():
            vars_spec[k] = v

    overrides = dict(overrides or {})

    # Bind net_control_plane.instance to emitting host for coherence.
    if comp_id == "net_control_plane" and "instance" in vars_spec and "instance" not in overrides:
        if host in (comp.get("hosts") or []):
            overrides["instance"] = host

    rk = rng_key or f"{ref}|{iso_ms(ts)}|{state}|{host}"
    rng = rng_for("row", rk)

    vals: Dict[str, Any] = {}
    for k, spec in vars_spec.items():
        if k in overrides:
            vals[k] = overrides[k]
        else:
            vals[k] = generate_from_domain(spec, rng)

    msg = tpl["msg"].format(**vals)

    rows.append(
        {
            "timestamp": iso_ms(ts),
            "level": tpl["lvl"],
            "message": msg,
            "trace_id": trace_id,
            "service": comp.get("svc", "") or "",
            "host": host or "",
        }
    )


def build_failure_intervals() -> List[Dict[str, Any]]:
    f_phase = SCENARIO["scenario"]["time"]["phases"]["f"]
    start_min, end_min = int(f_phase["start_min"]), int(f_phase["end_min"])
    events = list(SCENARIO["scenario"]["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e["order"]))
    bounds = [start_min] + [int(e["at_min"]) for e in events if start_min <= int(e["at_min"]) < end_min] + [end_min]
    bounds = sorted(set(bounds))

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}
    intervals: List[Dict[str, Any]] = []

    ev_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        ev_by_min.setdefault(int(e["at_min"]), []).append(e)

    for i in range(len(bounds) - 1):
        a, b = bounds[i], bounds[i + 1]
        if a in ev_by_min:
            for e in sorted(ev_by_min[a], key=lambda x: x["order"]):
                for k, v in e.get("rate_multipliers", {}).items():
                    active_rate[k] = float(v)
                for k, v in e.get("latency_multipliers", {}).items():
                    active_lat[k] = {"p50": float(v["p50"]), "p95": float(v["p95"])}
        intervals.append({"start_min": a, "end_min": b, "rate_mult": dict(active_rate), "lat_mult": dict(active_lat)})
    return intervals


def get_flow_by_id(state: str, flow_id: str) -> Dict[str, Any]:
    for f in SYSTEM["flows"][state]:
        if f["id"] == flow_id:
            return f
    raise KeyError(flow_id)


def allocate_attempt_counts(n: int, expected: float, max_attempts: int, key: str) -> List[int]:
    if n <= 0:
        return []
    expected = max(1.0, min(float(expected), float(max_attempts)))

    if max_attempts == 1:
        return [1] * n

    if max_attempts == 2:
        p2 = max(0.0, min(1.0, expected - 1.0))
        n2 = det_round(p2 * n, f"{key}|n2")
        n2 = max(0, min(n, n2))
        counts = [2] * n2 + [1] * (n - n2)
        order_rng = rng_for("attempts", key, n)
        order = np.arange(n)
        order_rng.shuffle(order)
        out = [0] * n
        for j, idx in enumerate(order):
            out[idx] = counts[j]
        return out

    disc = max(0.0, 4.0 * expected - 3.0)
    r = max(0.0, min(1.0, (-1.0 + math.sqrt(disc)) / 2.0))
    q = 1.0 - r
    p2 = r * q
    p3 = r * r
    n3 = det_round(p3 * n, f"{key}|n3")
    n2 = det_round(p2 * n, f"{key}|n2")
    n3 = max(0, min(n, n3))
    n2 = max(0, min(n - n3, n2))
    n1 = n - n2 - n3

    counts = [3] * n3 + [2] * n2 + [1] * n1
    order_rng = rng_for("attempts", key, n, "v2")
    order = np.arange(n)
    order_rng.shuffle(order)
    out = [0] * n
    for j, idx in enumerate(order):
        out[idx] = counts[j]
    return out


def get_int_domain_range(ref: str, var_name: str) -> Optional[Tuple[int, int]]:
    comp_id, log_id = parse_ref(ref)
    tpl = SYSTEM["components"][comp_id]["logs"][log_id]
    spec = (tpl.get("vars", {}) or {}).get(var_name)
    if not spec:
        return None
    if spec.get("k") != "i":
        return None
    v = spec.get("v")
    if not (isinstance(v, (list, tuple)) and len(v) == 2):
        return None
    return int(v[0]), int(v[1])


def clamp_int(x: int, lo: Optional[int], hi: Optional[int]) -> int:
    if lo is not None and x < lo:
        return lo
    if hi is not None and x > hi:
        return hi
    return x


def scale_attempt_durations_to_total(
    attempt_durs_ms: List[int],
    backoffs_ms: List[int],
    total_range: Optional[Tuple[int, int]],
    key: str,
) -> Tuple[List[int], int]:
    """
    Ensures total duration (sum attempts + sum backoffs) lands within total_range by scaling attempt durations only.
    Then deterministically adjusts the tail to hit the target exactly (subject to attempt durations staying >= 1ms).
    """
    total_back = int(sum(max(0, b) for b in backoffs_ms))
    attempt_durs = [max(1, int(d)) for d in attempt_durs_ms] if attempt_durs_ms else [1]
    total_att = int(sum(attempt_durs))
    cur_total = total_att + total_back

    if not total_range:
        return attempt_durs, cur_total

    dmin, dmax = int(total_range[0]), int(total_range[1])
    target_total = max(dmin, min(dmax, cur_total))

    # Minimum achievable total is total_back + len(attempts) (since each attempt >= 1ms)
    min_total = total_back + len(attempt_durs)
    if target_total < min_total:
        target_total = min_total

    target_att = target_total - total_back
    if total_att <= 0:
        scaled = [max(1, target_att // len(attempt_durs))] * len(attempt_durs)
    else:
        factor = float(target_att) / float(total_att)
        scaled = [max(1, int(round(d * factor))) for d in attempt_durs]

    # Adjust deterministically to hit target_total exactly.
    cur2 = int(sum(scaled)) + total_back
    diff = target_total - cur2
    if diff != 0:
        if diff > 0:
            scaled[-1] += diff
        else:
            need = -diff
            # reduce from the end backwards, keeping each >=1
            for idx in range(len(scaled) - 1, -1, -1):
                if need <= 0:
                    break
                can = scaled[idx] - 1
                take = min(can, need)
                scaled[idx] -= take
                need -= take
            # If we couldn't reduce enough due to >=1 constraints, we accept the best effort.
    final_total = int(sum(scaled)) + total_back
    return scaled, final_total


def simulate_flow_instances(
    rows: List[Dict[str, str]],
    state: str,
    interval_start: datetime,
    interval_end: datetime,
    flow: Dict[str, Any],
    rate_multiplier: float,
    lat_multiplier: Optional[Dict[str, float]],
    instance_seq_start: int,
) -> int:
    dur_min = (interval_end - interval_start).total_seconds() / 60.0
    eff_rpm = float(flow["rpm"]) * float(rate_multiplier)
    n_instances = det_round(eff_rpm * dur_min, f"flowcount|{state}|{flow['id']}|{iso_ms(interval_start)}")
    if n_instances <= 0:
        return instance_seq_start

    start_times = schedule_times(
        interval_start,
        interval_end,
        n_instances,
        f"flowstarts|{state}|{flow['id']}|{iso_ms(interval_start)}",
        jitter_ms=900,
    )

    retry = flow.get("retry", {}) or {}
    max_attempts = int(retry.get("max_attempts", 1))
    expected_attempts = float(retry.get("expected_attempts", 1.0))
    attempt_counts = allocate_attempt_counts(
        n_instances,
        expected_attempts,
        max_attempts,
        f"attempt_alloc|{state}|{flow['id']}|{iso_ms(interval_start)}",
    )

    flow_emit = list(flow["emit"])
    if len(flow_emit) < 2:
        return instance_seq_start + n_instances

    # Bind end ref and its dur_ms domain to total request duration (start -> final end).
    end_ref = flow_emit[-1]
    total_dur_range = get_int_domain_range(end_ref, "dur_ms")

    backoff_range = get_int_domain_range("storage_api.retry_attempt", "backoff_ms")
    backoff_min = backoff_range[0] if backoff_range else None
    backoff_max = backoff_range[1] if backoff_range else None

    lat_pairs: List[List[float]] = [list(x) for x in flow["latency_ms"]]
    if lat_multiplier:
        mp50 = float(lat_multiplier.get("p50", 1.0))
        mp95 = float(lat_multiplier.get("p95", 1.0))
        lat_pairs = [[x[0] * mp50, x[1] * mp95] for x in lat_pairs]

    api_hosts = SYSTEM["components"]["storage_api"]["hosts"]
    backoff_pairs = retry.get("backoff_ms", []) or []

    for i, chain_start in enumerate(start_times):
        inst_seq = instance_seq_start + i
        inst_rng = rng_for("flowinst", state, flow["id"], inst_seq)

        trace_id = gen_hex(inst_rng, 32) if (SYSTEM["tracing"]["on"] and flow.get("trace", False)) else ""
        request_id = gen_uuid_str(inst_rng)

        api_host = api_hosts[stable_u32(f"h|{request_id}") % len(api_hosts)]
        method = ["GET", "PUT", "LIST"][stable_u32(f"m|{request_id}") % 3]
        bucket = ["img-prod", "logs-archive", "app-data"][stable_u32(f"b|{request_id}") % 3]
        obj = "obj/" + gen_hex(inst_rng, 12)
        client_ip = gen_ip_str(inst_rng, pool="203.0.113")
        bytes_val = int(inst_rng.integers(200, 200000 + 1))

        A = int(attempt_counts[i])
        A = max(1, min(max_attempts, A))

        # --- Plan attempt durations and retry backoffs deterministically ---
        attempt_durs: List[int] = []
        for a in range(1, A + 1):
            att_rng = rng_for("attempt", state, flow["id"], inst_seq, a)
            segs = [
                max(0, int(round(sample_lognormal_ms(float(p50), float(p95), att_rng, cap_mult=3.0))))
                for (p50, p95) in lat_pairs
            ]
            # Interpret YAML per-log latency pairs as internal segments within an attempt; sum gives attempt duration.
            dur_a = max(1, int(sum(segs)))
            attempt_durs.append(dur_a)

        backoffs: List[int] = []
        for a in range(2, A + 1):
            idx = min(a - 2, len(backoff_pairs) - 1) if backoff_pairs else 0
            bo_rng = rng_for("attempt", state, flow["id"], inst_seq, a, "backoff")
            if backoff_pairs:
                p50, p95 = backoff_pairs[idx]
                bo = sample_lognormal_ms(float(p50), float(p95), bo_rng, cap_mult=3.0)
                bo_ms = int(round(bo))
            else:
                bo_ms = 0
            bo_ms = clamp_int(bo_ms, backoff_min, backoff_max)
            bo_ms = max(0, int(bo_ms))
            backoffs.append(bo_ms)

        # Scale attempt durations so that total request duration fits the terminal log dur_ms domain.
        attempt_durs, total_ms = scale_attempt_durations_to_total(
            attempt_durs, backoffs, total_dur_range, key=f"{state}|{flow['id']}|{inst_seq}"
        )

        # --- Emit req_start once, then retry markers, then one terminal req_end_* once ---
        emit_log_row(
            rows,
            chain_start,
            "storage_api.req_start",
            state,
            trace_id,
            api_host,
            overrides={
                "method": method,
                "bucket": bucket,
                "region": "us-east4",
                "object": obj,
                "client_ip": client_ip,
                "request_id": request_id,
                "trace_id": trace_id,
            },
            rng_key=f"start|{flow['id']}|{inst_seq}",
        )

        # Compute attempt boundaries for retry logs and final end timestamp.
        t = chain_start
        for a in range(1, A + 1):
            # attempt runs for attempt_durs[a-1]
            t_end = t + timedelta(milliseconds=int(attempt_durs[a - 1]))
            if a < A:
                bo = backoffs[a - 1]  # backoff between a and a+1
                t_next = t_end + timedelta(milliseconds=int(bo))
                # retry marker at the start of the retry attempt
                if retry.get("emit_per_retry"):
                    emit_log_row(
                        rows,
                        t_next,
                        "storage_api.retry_attempt",
                        state,
                        trace_id,
                        api_host,
                        overrides={
                            "request_id": request_id,
                            "attempt": a + 1,
                            "max_attempts": max_attempts,
                            "backoff_ms": bo,
                        },
                        rng_key=f"retry|{flow['id']}|{inst_seq}|{a+1}",
                    )
                t = t_next
            else:
                # final attempt end; emit terminal response log
                end_ts = chain_start + timedelta(milliseconds=int(total_ms))
                # Ensure end_ts matches our computed timeline end; if rounding drift exists, prefer total_ms for dur_ms/message.
                # (End timestamp is the observable; dur_ms should match it.)
                dur_ms = int(round((end_ts - chain_start).total_seconds() * 1000.0))
                if total_dur_range is not None:
                    dur_ms = clamp_int(dur_ms, total_dur_range[0], total_dur_range[1])

                overrides: Dict[str, Any] = {"request_id": request_id, "dur_ms": dur_ms, "trace_id": trace_id}
                if end_ref == "storage_api.req_end_200":
                    overrides["bytes"] = bytes_val

                emit_log_row(
                    rows,
                    end_ts,
                    end_ref,
                    state,
                    trace_id,
                    api_host,
                    overrides=overrides,
                    rng_key=f"end|{flow['id']}|{inst_seq}|{end_ref}",
                )

    return instance_seq_start + n_instances


def emit_background_interval(
    rows: List[Dict[str, str]],
    state: str,
    start: datetime,
    end: datetime,
    rate_mult: Dict[str, float],
    lat_mult: Dict[str, Dict[str, float]],
) -> None:
    dur_min = (end - start).total_seconds() / 60.0
    for comp_id, comp in SYSTEM["components"].items():
        beh = comp.get("beh", {}).get(state, [])
        for src in beh:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            key = f"{comp_id}.{log_id}"
            mult = float(rate_mult.get(key, 1.0)) if state == "f" else 1.0
            eff_per_min = per_min * mult
            if eff_per_min <= 0:
                continue

            if scope == "global":
                n = det_round(eff_per_min * dur_min, f"bg|{state}|{key}|{iso_ms(start)}")
                host = comp["hosts"][0] if comp["hosts"] else ""
                times = schedule_times(start, end, n, f"bgts|{state}|{key}|{iso_ms(start)}", jitter_ms=800)
                for t in times:
                    overrides: Dict[str, Any] = {}
                    if comp_id == "storage_api" and log_id == "request_stats" and state == "f":
                        ok_flow = get_flow_by_id("f", "storage_object_get_ok_f")
                        to_flow = get_flow_by_id("f", "storage_object_get_timeout_f")
                        okm = float(rate_mult.get(ok_flow["id"], 1.0))
                        tom = float(rate_mult.get(to_flow["id"], 1.0))
                        ok_total = ok_flow["rpm"] * okm
                        to_total = to_flow["rpm"] * tom
                        nh = max(1, len(comp["hosts"]))
                        ok_ph = int(round(ok_total / nh))
                        err_ph = int(round(to_total / nh))
                        lm = lat_mult.get(ok_flow["id"], {"p50": 1.0, "p95": 1.0})
                        p95_ms = int(
                            min(30000, max(120, round(180 * float(lm.get("p95", 1.0)) * (1.0 + to_total / 60.0))))
                        )
                        overrides = {"ok_rpm": ok_ph, "err_rpm": err_ph, "p95_ms": p95_ms}
                    emit_log_row(
                        rows,
                        t,
                        f"{comp_id}.{log_id}",
                        state,
                        "",
                        host,
                        overrides=overrides,
                        rng_key=f"bg|{state}|{key}|{iso_ms(t)}",
                    )
            else:
                for host in comp.get("hosts", []):
                    n = det_round(eff_per_min * dur_min, f"bg|{state}|{key}|{host}|{iso_ms(start)}")
                    times = schedule_times(start, end, n, f"bgts|{state}|{key}|{host}|{iso_ms(start)}", jitter_ms=800)
                    for t in times:
                        overrides = {}
                        if comp_id == "storage_api" and log_id == "request_stats" and state == "f":
                            ok_flow = get_flow_by_id("f", "storage_object_get_ok_f")
                            to_flow = get_flow_by_id("f", "storage_object_get_timeout_f")
                            okm = float(rate_mult.get(ok_flow["id"], 1.0))
                            tom = float(rate_mult.get(to_flow["id"], 1.0))
                            ok_total = ok_flow["rpm"] * okm
                            to_total = to_flow["rpm"] * tom
                            nh = max(1, len(comp["hosts"]))
                            ok_ph = int(round(ok_total / nh))
                            err_ph = int(round(to_total / nh))
                            lm = lat_mult.get(ok_flow["id"], {"p50": 1.0, "p95": 1.0})
                            p95_ms = int(
                                min(30000, max(120, round(180 * float(lm.get("p95", 1.0)) * (1.0 + to_total / 60.0))))
                            )
                            overrides = {"ok_rpm": ok_ph, "err_rpm": err_ph, "p95_ms": p95_ms}
                        emit_log_row(
                            rows,
                            t,
                            f"{comp_id}.{log_id}",
                            state,
                            "",
                            host,
                            overrides=overrides,
                            rng_key=f"bg|{state}|{key}|{host}|{iso_ms(t)}",
                        )


def emit_one_shots(rows: List[Dict[str, str]], maintenance_event_id: str) -> None:
    events = list(SCENARIO["scenario"]["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e["order"]))
    for e in events:
        at_min = int(e["at_min"])
        if not e.get("one_shots"):
            continue
        win_start = BASE_TIME + timedelta(minutes=at_min)
        win_end = win_start + timedelta(minutes=1)
        for s_idx, shot in enumerate(e["one_shots"]):
            ref = shot["ref"]
            count = int(shot["count"])
            hosts = list(shot.get("hosts") or [])
            times = schedule_times(win_start, win_end, count, f"oneshot|{ref}|{at_min}|{s_idx}", jitter_ms=650)
            for i, t in enumerate(times):
                comp_id, _ = parse_ref(ref)
                if hosts:
                    host = hosts[i % len(hosts)]
                else:
                    host = SYSTEM["components"][comp_id]["hosts"][0] if SYSTEM["components"][comp_id]["hosts"] else ""

                overrides: Dict[str, Any] = {}
                if ref in ("maintenance_automation.maintenance_start", "maintenance_automation.automation_halted"):
                    overrides["event_id"] = maintenance_event_id

                emit_log_row(rows, t, ref, "f", "", host, overrides=overrides, rng_key=f"oneshot|{ref}|{at_min}|{i}")


def main() -> None:
    random.seed(SEED)
    np.random.seed(SEED)

    rows: List[Dict[str, str]] = []

    maint_rng = rng_for("maintenance_event")
    maintenance_event_id = gen_hex(maint_rng, 16)

    n_start = BASE_TIME + timedelta(minutes=int(SCENARIO["scenario"]["time"]["phases"]["n"]["start_min"]))
    n_end = BASE_TIME + timedelta(minutes=int(SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"]))

    emit_background_interval(rows, "n", n_start, n_end, rate_mult={}, lat_mult={})

    inst_seq = 0
    flow_n = get_flow_by_id("n", "storage_object_get_n")
    inst_seq = simulate_flow_instances(
        rows, "n", n_start, n_end, flow_n, rate_multiplier=1.0, lat_multiplier=None, instance_seq_start=inst_seq
    )

    f_intervals = build_failure_intervals()
    for itv in f_intervals:
        a = BASE_TIME + timedelta(minutes=int(itv["start_min"]))
        b = BASE_TIME + timedelta(minutes=int(itv["end_min"]))
        rate_mult = itv["rate_mult"]
        lat_mult = itv["lat_mult"]

        emit_background_interval(rows, "f", a, b, rate_mult=rate_mult, lat_mult=lat_mult)

        for flow in SYSTEM["flows"]["f"]:
            fm = float(rate_mult.get(flow["id"], 1.0))
            lm = lat_mult.get(flow["id"])
            inst_seq = simulate_flow_instances(
                rows, "f", a, b, flow, rate_multiplier=fm, lat_multiplier=lm, instance_seq_start=inst_seq
            )

    emit_one_shots(rows, maintenance_event_id=maintenance_event_id)

    df = pd.DataFrame(rows, columns=["timestamp", "level", "message", "trace_id", "service", "host"])
    for c in ["timestamp", "level", "message", "trace_id", "service", "host"]:
        df[c] = df[c].fillna("").astype(str)
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
