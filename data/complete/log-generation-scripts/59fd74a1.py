import hashlib
import math
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "travis_osx_ci_vsphere"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["build_scheduler"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "build_scheduler",
            "svc": "travis-dispatcher",
            "hosts": ["sched-1", "sched-2"],
            "logs": {
                "dequeue_build": {
                    "lvl": "INFO",
                    "msg": "Dequeued build {build_id} repo={repo} queue={queue}",
                    "vars": {
                        "build_id": {"k": "uuid", "v": None},
                        "repo": {"k": "str", "v": "owner/repo"},
                        "queue": {"k": "ch", "v": ["com", "org"]},
                    },
                },
                "build_started": {
                    "lvl": "INFO",
                    "msg": "Build {build_id} started vm={vm_id} queue={queue}",
                    "vars": {
                        "build_id": {"k": "uuid", "v": None},
                        "vm_id": {"k": "hex", "v": 12},
                        "queue": {"k": "ch", "v": ["com", "org"]},
                    },
                },
                "build_requeued": {
                    "lvl": "WARN",
                    "msg": "Re-queued build {build_id} reason={reason} queue={queue}",
                    "vars": {
                        "build_id": {"k": "uuid", "v": None},
                        "reason": {"k": "ch", "v": ["vm_boot_failed", "capacity_paused"]},
                        "queue": {"k": "ch", "v": ["com", "org"]},
                    },
                },
                "queue_backlog_metric": {
                    "lvl": "INFO",
                    "msg": "Queue backlog queue={queue} depth={depth}",
                    "vars": {"queue": {"k": "ch", "v": ["com", "org"]}},
                    "state_vars": {
                        "n": {"depth": {"k": "i", "v": [0, 200]}},
                        "f": {"depth": {"k": "i", "v": [800, 5000]}},
                    },
                },
                "pause_toggle": {
                    "lvl": "INFO",
                    "msg": "OSX builds {action} by={actor} scope={scope}",
                    "vars": {
                        "action": {"k": "ch", "v": ["paused", "resumed"]},
                        "actor": {"k": "ch", "v": ["oncall", "infra"]},
                        "scope": {"k": "ch", "v": ["com+org", "com", "org"]},
                    },
                },
                "capacity_limit_set": {
                    "lvl": "INFO",
                    "msg": "Set OS X capacity limit max_concurrent={max_concurrent} scope={scope}",
                    "vars": {
                        "max_concurrent": {"k": "i", "v": [20, 200]},
                        "scope": {"k": "ch", "v": ["com+org", "com", "org"]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "queue_backlog_metric", "per_min": 2.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "queue_backlog_metric", "per_min": 3.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "osx_vm_manager",
            "svc": "osx-vm-manager",
            "hosts": ["vm-mgr-1", "vm-mgr-2"],
            "logs": {
                "vm_create_req": {
                    "lvl": "INFO",
                    "msg": "Create VM request build={build_id} template={template} reservation_mhz={res_mhz}",
                    "vars": {
                        "build_id": {"k": "uuid", "v": None},
                        "template": {"k": "ch", "v": ["xcode6_3"]},
                        "res_mhz": {"k": "i", "v": [800, 2000]},
                    },
                },
                "vm_poweron_ok": {
                    "lvl": "INFO",
                    "msg": "VM {vm_id} powered on host={esx_host} boot_ms={boot_ms}",
                    "vars": {
                        "vm_id": {"k": "hex", "v": 12},
                        "esx_host": {"k": "ch", "v": ["esx01", "esx02", "esx03", "esx04"]},
                        "boot_ms": {"k": "i", "v": [20000, 180000]},
                    },
                },
                "vm_poweron_failed_admission": {
                    "lvl": "ERROR",
                    "msg": "Power on failed vm={vm_id} err=reservation_unmet needed_res_mhz={need_mhz} available_res_mhz={avail_mhz}",
                    "vars": {
                        "vm_id": {"k": "hex", "v": 12},
                        "need_mhz": {"k": "i", "v": [800, 2000]},
                        "avail_mhz": {"k": "i", "v": [0, 700]},
                    },
                },
                "vm_poweron_failed_cpu_reservation": {
                    "lvl": "ERROR",
                    "msg": "Power on failed vm={vm_id} err=cpu_reservation_limit needed_res_mhz={need_mhz} available_res_mhz={avail_mhz}",
                    "vars": {
                        "vm_id": {"k": "hex", "v": 12},
                        "need_mhz": {"k": "i", "v": [800, 2000]},
                        "avail_mhz": {"k": "i", "v": [0, 700]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "vsphere_janitor",
            "svc": "vsphere-janitor",
            "hosts": ["janitor-1"],
            "logs": {
                "cleanup_cycle_normal": {"lvl": "INFO", "msg": "Cleanup cycle started mode=normal", "vars": {}},
                "cleanup_cycle_aggressive": {"lvl": "INFO", "msg": "Cleanup cycle started mode=aggressive", "vars": {}},
                "auth_failed": {
                    "lvl": "ERROR",
                    "msg": "vSphere auth failed user={user} err={err}",
                    "vars": {
                        "user": {"k": "ch", "v": ["travis-janitor"]},
                        "err": {"k": "ch", "v": ["invalid_credentials", "account_locked"]},
                    },
                },
                "vm_total_report_live": {
                    "lvl": "INFO",
                    "msg": "Reported vm_total={vm_total} cluster={cluster}",
                    "vars": {"vm_total": {"k": "i", "v": [0, 7000]}, "cluster": {"k": "ch", "v": ["osx-cluster"]}},
                },
                "vm_total_report_stale": {
                    "lvl": "INFO",
                    "msg": "Reported vm_total={vm_total} cluster={cluster} (stale)",
                    "vars": {"vm_total": {"k": "i", "v": [150, 220]}, "cluster": {"k": "ch", "v": ["osx-cluster"]}},
                },
                "delete_vm_ok_normal": {
                    "lvl": "INFO",
                    "msg": "Deleted VM vm={vm_id} reason=build_finished",
                    "vars": {"vm_id": {"k": "hex", "v": 12}},
                },
                "delete_vm_ok_aggressive": {
                    "lvl": "INFO",
                    "msg": "Deleted VM vm={vm_id} reason=aggressive_sweep",
                    "vars": {"vm_id": {"k": "hex", "v": 12}},
                },
                "delete_vm_skipped": {
                    "lvl": "WARN",
                    "msg": "Skipped VM vm={vm_id} state={vm_state}",
                    "vars": {"vm_id": {"k": "hex", "v": 12}, "vm_state": {"k": "ch", "v": ["poweron_failed", "orphaned", "unknown"]}},
                },
                "janitor_restart": {
                    "lvl": "INFO",
                    "msg": "Service restarted reason={reason}",
                    "vars": {"reason": {"k": "ch", "v": ["config_update", "manual_restart"]}},
                },
                "cleanup_mode_set": {
                    "lvl": "INFO",
                    "msg": "Set cleanup mode={mode}",
                    "vars": {"mode": {"k": "ch", "v": ["aggressive"]}},
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "cleanup_cycle_normal", "per_min": 1.0, "scope": "global"},
                        {"id": "vm_total_report_live", "per_min": 1.0, "scope": "global"},
                        {"id": "delete_vm_ok_normal", "per_min": 20.0, "scope": "global"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "cleanup_cycle_normal", "per_min": 1.0, "scope": "global"},
                        {"id": "cleanup_cycle_aggressive", "per_min": 1.0, "scope": "global"},
                        {"id": "auth_failed", "per_min": 1.0, "scope": "global"},
                        {"id": "vm_total_report_live", "per_min": 1.0, "scope": "global"},
                        {"id": "vm_total_report_stale", "per_min": 1.0, "scope": "global"},
                        {"id": "delete_vm_ok_normal", "per_min": 25.0, "scope": "global"},
                        {"id": "delete_vm_ok_aggressive", "per_min": 550.0, "scope": "global"},
                        {"id": "delete_vm_skipped", "per_min": 8.0, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "vsphere_cluster",
            "svc": "vsphere",
            "hosts": ["vcenter-1"],
            "logs": {
                "admission_denied_reservation": {
                    "lvl": "WARN",
                    "msg": "Admission control denied powerOn vm={vm_id} reason=insufficient_reservations cpu_mhz_needed={cpu_mhz_needed} cpu_mhz_free={cpu_mhz_free}",
                    "vars": {"vm_id": {"k": "hex", "v": 12}, "cpu_mhz_needed": {"k": "i", "v": [800, 2000]}, "cpu_mhz_free": {"k": "i", "v": [0, 700]}},
                },
                "admission_denied_cpu": {
                    "lvl": "WARN",
                    "msg": "Admission control denied powerOn vm={vm_id} reason=reservation_limit_reached cpu_mhz_needed={cpu_mhz_needed} cpu_mhz_free={cpu_mhz_free}",
                    "vars": {"vm_id": {"k": "hex", "v": 12}, "cpu_mhz_needed": {"k": "i", "v": [800, 2000]}, "cpu_mhz_free": {"k": "i", "v": [0, 700]}},
                },
                "cluster_inventory_high": {
                    "lvl": "INFO",
                    "msg": "Cluster inventory total_vms={total_vms} powered_on={powered_on} reserved_cpu_mhz={reserved_cpu_mhz} cpu_mhz_capacity={cpu_mhz_capacity}",
                    "vars": {
                        "total_vms": {"k": "i", "v": [4000, 6500]},
                        "powered_on": {"k": "i", "v": [1200, 4200]},
                        "reserved_cpu_mhz": {"k": "i", "v": [1500000, 7000000]},
                        "cpu_mhz_capacity": {"k": "i", "v": [2500000, 8000000]},
                    },
                },
                "cluster_inventory_low": {
                    "lvl": "INFO",
                    "msg": "Cluster inventory total_vms={total_vms} powered_on={powered_on} reserved_cpu_mhz={reserved_cpu_mhz} cpu_mhz_capacity={cpu_mhz_capacity}",
                    "vars": {
                        "total_vms": {"k": "i", "v": [150, 800]},
                        "powered_on": {"k": "i", "v": [80, 450]},
                        "reserved_cpu_mhz": {"k": "i", "v": [200000, 1200000]},
                        "cpu_mhz_capacity": {"k": "i", "v": [2500000, 8000000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "cluster_inventory_low", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "cluster_inventory_high", "per_min": 1.0, "scope": "global"}, {"id": "cluster_inventory_low", "per_min": 1.0, "scope": "global"}]},
            },
        },
        {
            "id": "metrics_alerting",
            "svc": "metrics",
            "hosts": ["metrics-1"],
            "logs": {
                "ingest_vm_total_live": {
                    "lvl": "INFO",
                    "msg": "Ingested gauge osx_vm_total={value} source=vsphere-janitor",
                    "vars": {"value": {"k": "i", "v": [0, 7000]}},
                },
                "ingest_vm_total_stale": {
                    "lvl": "INFO",
                    "msg": "Ingested gauge osx_vm_total={value} source=vsphere-janitor (stale)",
                    "vars": {"value": {"k": "i", "v": [150, 220]}},
                },
                "alert_eval_ok": {
                    "lvl": "DEBUG",
                    "msg": "Alert eval name=osx_vm_total_high value={value} status=ok",
                    "vars": {"value": {"k": "i", "v": [0, 7000]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "ingest_vm_total_live", "per_min": 1.0, "scope": "global"}, {"id": "alert_eval_ok", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "ingest_vm_total_live", "per_min": 1.0, "scope": "global"}, {"id": "ingest_vm_total_stale", "per_min": 1.0, "scope": "global"}, {"id": "alert_eval_ok", "per_min": 1.0, "scope": "global"}]},
            },
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "osx_build_start_ok",
                    "rpm": 20.0,
                    "emit": ["build_scheduler.dequeue_build", "osx_vm_manager.vm_create_req", "osx_vm_manager.vm_poweron_ok", "build_scheduler.build_started"],
                    "latency_ms": [[1, 5], [5, 20], [60000, 120000], [10, 50]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                }
            ]
        },
        "f": {
            "req": [
                {
                    "id": "osx_build_start_ok",
                    "rpm": 20.0,
                    "emit": ["build_scheduler.dequeue_build", "osx_vm_manager.vm_create_req", "osx_vm_manager.vm_poweron_ok", "build_scheduler.build_started"],
                    "latency_ms": [[1, 5], [5, 25], [90000, 180000], [10, 80]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "osx_build_start_fail_leak",
                    "rpm": 160.0,
                    "emit": [
                        "build_scheduler.dequeue_build",
                        "osx_vm_manager.vm_create_req",
                        "vsphere_cluster.admission_denied_reservation",
                        "osx_vm_manager.vm_poweron_failed_admission",
                        "build_scheduler.build_requeued",
                    ],
                    "latency_ms": [[1, 5], [5, 25], [30000, 90000], [1, 10], [5, 60]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "osx_build_start_fail_cpu_resv",
                    "rpm": 70.0,
                    "emit": [
                        "build_scheduler.dequeue_build",
                        "osx_vm_manager.vm_create_req",
                        "vsphere_cluster.admission_denied_cpu",
                        "osx_vm_manager.vm_poweron_failed_cpu_reservation",
                        "build_scheduler.build_requeued",
                    ],
                    "latency_ms": [[1, 5], [5, 25], [20000, 60000], [1, 10], [5, 60]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "travis_osx_vm_leak_and_cpu_reservation",
        "time": {"total_minutes": 55, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 55}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 20,
                        "rate_multipliers": {
                            "osx_build_start_fail_cpu_resv": 0.0,
                            "vsphere_janitor.vm_total_report_live": 0.0,
                            "vsphere_janitor.delete_vm_ok_normal": 0.0,
                            "vsphere_janitor.delete_vm_ok_aggressive": 0.0,
                            "vsphere_janitor.delete_vm_skipped": 0.0,
                            "vsphere_janitor.cleanup_cycle_aggressive": 0.0,
                            "vsphere_cluster.cluster_inventory_low": 0.0,
                            "vsphere_cluster.cluster_inventory_high": 1.0,
                            "metrics_alerting.ingest_vm_total_live": 0.0,
                            "metrics_alerting.ingest_vm_total_stale": 1.0,
                        },
                        "latency_multipliers": {"osx_build_start_ok": {"p50": 1.2, "p95": 1.3}, "osx_build_start_fail_leak": {"p50": 1.3, "p95": 1.4}},
                        "one_shots": [],
                    },
                    {
                        "order": 2,
                        "at_min": 30,
                        "rate_multipliers": {
                            "osx_build_start_ok": 0.0,
                            "osx_build_start_fail_leak": 0.0,
                            "osx_build_start_fail_cpu_resv": 0.0,
                            "vsphere_janitor.auth_failed": 0.0,
                            "vsphere_janitor.vm_total_report_stale": 0.0,
                            "vsphere_janitor.vm_total_report_live": 1.0,
                            "vsphere_janitor.delete_vm_ok_normal": 1.0,
                            "vsphere_janitor.delete_vm_skipped": 1.0,
                            "metrics_alerting.ingest_vm_total_stale": 0.0,
                            "metrics_alerting.ingest_vm_total_live": 1.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "build_scheduler.pause_toggle", "count": 1, "hosts": ["sched-1"]},
                            {"ref": "vsphere_janitor.janitor_restart", "count": 1, "hosts": ["janitor-1"]},
                        ],
                    },
                    {
                        "order": 3,
                        "at_min": 38,
                        "rate_multipliers": {
                            "vsphere_janitor.cleanup_cycle_normal": 0.0,
                            "vsphere_janitor.cleanup_cycle_aggressive": 1.0,
                            "vsphere_janitor.delete_vm_ok_normal": 0.2,
                            "vsphere_janitor.delete_vm_ok_aggressive": 2.0,
                            "vsphere_janitor.delete_vm_skipped": 0.2,
                        },
                        "latency_multipliers": {},
                        "one_shots": [{"ref": "vsphere_janitor.cleanup_mode_set", "count": 1, "hosts": ["janitor-1"]}],
                    },
                    {
                        "order": 4,
                        "at_min": 44,
                        "rate_multipliers": {
                            "osx_build_start_ok": 1.0,
                            "osx_build_start_fail_leak": 0.0,
                            "osx_build_start_fail_cpu_resv": 1.0,
                            "vsphere_cluster.cluster_inventory_high": 0.0,
                            "vsphere_cluster.cluster_inventory_low": 1.0,
                            "vsphere_janitor.cleanup_cycle_aggressive": 0.0,
                            "vsphere_janitor.cleanup_cycle_normal": 1.0,
                            "vsphere_janitor.delete_vm_ok_aggressive": 0.0,
                            "vsphere_janitor.delete_vm_ok_normal": 1.0,
                            "vsphere_janitor.delete_vm_skipped": 0.5,
                        },
                        "latency_multipliers": {"osx_build_start_ok": {"p50": 1.1, "p95": 1.2}, "osx_build_start_fail_cpu_resv": {"p50": 1.0, "p95": 1.1}},
                        "one_shots": [{"ref": "build_scheduler.pause_toggle", "count": 1, "hosts": ["sched-1"]}],
                    },
                    {
                        "order": 5,
                        "at_min": 48,
                        "rate_multipliers": {"osx_build_start_ok": 0.6, "osx_build_start_fail_cpu_resv": 0.3},
                        "latency_multipliers": {"osx_build_start_ok": {"p50": 0.9, "p95": 0.9}, "osx_build_start_fail_cpu_resv": {"p50": 0.8, "p95": 0.8}},
                        "one_shots": [{"ref": "build_scheduler.capacity_limit_set", "count": 1, "hosts": ["sched-1"]}],
                    },
                ]
            }
        },
    }
}

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def md5_bytes(s: str) -> bytes:
    return hashlib.md5(s.encode("utf-8")).digest()


def stable_u(s: str) -> float:
    x = int.from_bytes(md5_bytes(s), "big")
    u = (x % (10**12)) / (10**12)
    return min(1.0 - 1e-12, max(1e-12, u))


def stable_int(s: str) -> int:
    return int.from_bytes(md5_bytes(s), "big")


def choose_from_list(values: List[Any], key: str) -> Any:
    if not values:
        return None
    return values[stable_int(key) % len(values)]


def choose_int(lo: int, hi: int, key: str) -> int:
    if hi <= lo:
        return lo
    span = hi - lo + 1
    return lo + (stable_int(key) % span)


def choose_hex(n: int, key: str) -> str:
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    if n <= 32:
        return h[:n]
    out = []
    while len("".join(out)) < n:
        out.append(hashlib.md5((key + ":" + str(len(out))).encode("utf-8")).hexdigest())
    return "".join(out)[:n]


def choose_uuid_str(key: str) -> str:
    b = hashlib.md5(key.encode("utf-8")).digest()
    u = uuid.UUID(bytes=b)
    return str(u)


def inv_norm_ppf(p: float) -> float:
    p = min(1.0 - 1e-15, max(1e-15, float(p)))
    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02, 1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]
    plow = 0.02425
    phigh = 1.0 - plow
    if p < plow:
        q = math.sqrt(-2.0 * math.log(p))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return num / den
    if p > phigh:
        q = math.sqrt(-2.0 * math.log(1.0 - p))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return -(num / den)
    q = p - 0.5
    r = q * q
    num = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    den = (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    return num / den


def sample_lognormal_from_p50_p95(p50: float, p95: float, u: float, cap_mult: float = 3.0) -> float:
    p50 = max(0.001, float(p50))
    p95 = max(p50 * 1.001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.6448536269514722
    z = inv_norm_ppf(u)
    x = math.exp(mu + sigma * z)
    cap = cap_mult * p95
    return float(min(cap, max(0.0, x)))


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def fmt_ts(ms_since_epoch: int) -> str:
    dt = datetime.fromtimestamp(ms_since_epoch / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond / 1000):03d}Z"


@dataclass(frozen=True)
class Interval:
    start_min: int
    end_min: int
    state: str  # "n" or "f"
    rate_mult: Dict[str, float]  # persistent
    lat_mult: Dict[str, Dict[str, float]]  # flow_id -> {p50,p95}


class AccumulatorRounding:
    def __init__(self):
        self.totals: Dict[str, float] = {}

    def alloc(self, key: str, expected: float) -> int:
        prev = self.totals.get(key, 0.0)
        new = prev + expected
        self.totals[key] = new
        return int(math.floor(new + 1e-12) - math.floor(prev + 1e-12))


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[Tuple[str, str], Dict[str, Any]], Dict[Tuple[str, str], Dict[str, Any]]]:
    comp_by_id = {c["id"]: c for c in system["components"]}
    flow_by_state_id: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for state in ["n", "f"]:
        for f in system["flows"][state]["req"]:
            flow_by_state_id[(state, f["id"])] = f
    log_tpl: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for cid, c in comp_by_id.items():
        for lid, tpl in c.get("logs", {}).items():
            log_tpl[(cid, lid)] = tpl
    return comp_by_id, flow_by_state_id, log_tpl


COMPONENTS, FLOWS, LOG_TPLS = build_indices(SYSTEM)


def derive_failure_intervals(scenario: Dict[str, Any]) -> List[Interval]:
    f_phase = scenario["scenario"]["time"]["phases"]["f"]
    f_start = int(f_phase["start_min"])
    f_end = int(f_phase["end_min"])
    events = sorted(scenario["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = [f_start] + sorted({e["at_min"] for e in events if f_start <= e["at_min"] < f_end}) + [f_end]
    boundaries = sorted(boundaries)

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}
    intervals: List[Interval] = []

    idx = 0
    for i in range(len(boundaries) - 1):
        s = boundaries[i]
        e = boundaries[i + 1]
        while idx < len(events) and events[idx]["at_min"] == s:
            ev = events[idx]
            for k, v in ev.get("rate_multipliers", {}).items():
                active_rate[k] = float(v)
            for fid, mm in ev.get("latency_multipliers", {}).items():
                active_lat[fid] = {"p50": float(mm["p50"]), "p95": float(mm["p95"])}
            idx += 1
        intervals.append(Interval(start_min=s, end_min=e, state="f", rate_mult=dict(active_rate), lat_mult=dict(active_lat)))
    return intervals


FAILURE_INTERVALS = derive_failure_intervals(SCENARIO)
NORMAL_INTERVAL = Interval(
    start_min=int(SCENARIO["scenario"]["time"]["phases"]["n"]["start_min"]),
    end_min=int(SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"]),
    state="n",
    rate_mult={},
    lat_mult={},
)


def interval_to_ms(i: Interval) -> Tuple[int, int]:
    return ms(BASE_TIME + timedelta(minutes=i.start_min)), ms(BASE_TIME + timedelta(minutes=i.end_min))


def schedule_evenly(start_ms: int, end_ms: int, n: int, key: str, jitter_ms: int = 250) -> List[int]:
    if n <= 0:
        return []
    span = max(1, end_ms - start_ms)
    out = []
    for j in range(n):
        frac = (j + 0.5) / n
        base = start_ms + int(frac * span)
        uj = stable_u(f"{key}:jitter:{j}")
        jitter = int((uj - 0.5) * 2 * jitter_ms)
        t = min(end_ms - 1, max(start_ms, base + jitter))
        out.append(t)
    return out


def component_host_for_flow(trace_id: str, component_id: str) -> str:
    comp = COMPONENTS[component_id]
    hosts = comp.get("hosts", []) or []
    if not hosts:
        return ""
    if not trace_id:
        return hosts[0]
    return hosts[stable_int(f"{trace_id}:{component_id}") % len(hosts)]


def host_for_background(component_id: str, host_hint: Optional[str], seq: int) -> str:
    comp = COMPONENTS[component_id]
    hosts = comp.get("hosts", []) or []
    if not hosts:
        return ""
    if host_hint:
        return host_hint
    return hosts[seq % len(hosts)]


def get_rate_multiplier(interval: Interval, key: str) -> float:
    return float(interval.rate_mult.get(key, 1.0))


def get_latency_multiplier(interval: Interval, flow_id: str) -> Tuple[float, float]:
    m = interval.lat_mult.get(flow_id)
    if not m:
        return 1.0, 1.0
    return float(m.get("p50", 1.0)), float(m.get("p95", 1.0))


def gen_backlog_depth(minute: int, queue: str, key: str) -> int:
    if minute < 20:
        base = choose_int(5, 120, f"{key}:n:{queue}:{minute}")
        return base
    if 20 <= minute < 30:
        base = 1200 + (minute - 20) * 320
    elif 30 <= minute < 44:
        base = 4400 + (minute - 30) * 40
    elif 44 <= minute < 48:
        base = 4600 - (minute - 44) * 220
    else:
        base = 3600 - (minute - 48) * 170
    base += 60 if queue == "org" else 0
    base += int((stable_u(f"{key}:noise:{minute}:{queue}") - 0.5) * 220)
    return int(max(800, min(5000, base)))


def custom_background_values(component_id: str, log_id: str, state: str, ts_ms: int, key: str) -> Dict[str, Any]:
    minute = int((ts_ms - ms(BASE_TIME)) // 60000)

    if component_id == "build_scheduler" and log_id == "queue_backlog_metric":
        queue = choose_from_list(["com", "org"], f"{key}:queue:{minute}")
        depth = gen_backlog_depth(minute, queue, key)
        return {"queue": queue, "depth": depth}

    if component_id == "vsphere_janitor" and log_id in ("vm_total_report_live", "vm_total_report_stale"):
        if log_id == "vm_total_report_stale":
            vm_total = 180 + choose_int(-20, 20, f"{key}:stale:{minute}")
            vm_total = int(max(150, min(220, vm_total)))
        else:
            if minute < 20:
                vm_total = 220 + choose_int(-40, 90, f"{key}:live:n:{minute}")
                vm_total = int(max(0, min(7000, vm_total)))
            elif 30 <= minute < 44:
                vm_total = 5600 + choose_int(-800, 700, f"{key}:live:high:{minute}")
            elif 20 <= minute < 30:
                vm_total = 6000 + choose_int(-600, 600, f"{key}:live:suppressed:{minute}")
            else:
                vm_total = 420 + choose_int(-200, 220, f"{key}:live:low:{minute}")
            vm_total = int(max(0, min(7000, vm_total)))
        return {"vm_total": vm_total, "cluster": "osx-cluster"}

    if component_id == "metrics_alerting" and log_id in ("ingest_vm_total_live", "ingest_vm_total_stale", "alert_eval_ok"):
        if log_id == "ingest_vm_total_stale":
            value = 180 + choose_int(-20, 20, f"{key}:stale:{minute}")
            value = int(max(150, min(220, value)))
        elif log_id == "ingest_vm_total_live":
            if minute < 20:
                value = 240 + choose_int(-60, 120, f"{key}:live:n:{minute}")
            elif 30 <= minute < 44:
                value = 5700 + choose_int(-900, 600, f"{key}:live:high:{minute}")
            else:
                value = 450 + choose_int(-220, 250, f"{key}:live:low:{minute}")
            value = int(max(0, min(7000, value)))
        else:
            if minute < 20:
                value = 260 + choose_int(-80, 160, f"{key}:eval:n:{minute}")
            elif minute < 30:
                value = 200 + choose_int(-30, 30, f"{key}:eval:stale:{minute}")
            elif minute < 44:
                value = 5900 + choose_int(-700, 500, f"{key}:eval:high:{minute}")
            else:
                value = 520 + choose_int(-200, 200, f"{key}:eval:low:{minute}")
            value = int(max(0, min(7000, value)))
        return {"value": value}

    if component_id == "vsphere_cluster" and log_id in ("cluster_inventory_high", "cluster_inventory_low"):
        if log_id == "cluster_inventory_high":
            if minute < 38:
                total_vms = 6100 + choose_int(-500, 350, f"{key}:t:{minute}")
            else:
                total_vms = 6400 - int((minute - 38) * 320) + choose_int(-220, 120, f"{key}:t:{minute}")
            total_vms = int(max(4000, min(6500, total_vms)))
            powered_on = int(max(1200, min(4200, int(total_vms * 0.62) + choose_int(-200, 200, f"{key}:p:{minute}"))))
            cpu_mhz_capacity = 6500000
            reserved_cpu_mhz = int(max(1500000, min(7000000, cpu_mhz_capacity - choose_int(20000, 280000, f"{key}:r:{minute}"))))
        else:
            total_vms = 380 + choose_int(-160, 240, f"{key}:t:{minute}")
            total_vms = int(max(150, min(800, total_vms)))
            powered_on = int(max(80, min(450, int(total_vms * 0.55) + choose_int(-50, 50, f"{key}:p:{minute}"))))
            cpu_mhz_capacity = 6500000
            reserved_cpu_mhz = int(max(200000, min(1200000, 780000 + choose_int(-250000, 250000, f"{key}:r:{minute}"))))
        return {"total_vms": total_vms, "powered_on": powered_on, "reserved_cpu_mhz": reserved_cpu_mhz, "cpu_mhz_capacity": cpu_mhz_capacity}

    return {}


def domain_value(dom: Dict[str, Any], key: str) -> Any:
    k = dom["k"]
    v = dom.get("v")
    if k == "uuid":
        return choose_uuid_str(key)
    if k == "hex":
        return choose_hex(int(v), key)
    if k == "ch":
        return choose_from_list(list(v), key)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return choose_int(lo, hi, key)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        u = stable_u(key)
        return lo + (hi - lo) * u
    if k == "str":
        return str(v) if v is not None else ""
    return ""


def render_log(component_id: str, log_id: str, state: str, ts_ms: int, key: str, overrides: Dict[str, Any]) -> Tuple[str, str]:
    tpl = LOG_TPLS[(component_id, log_id)]
    vals: Dict[str, Any] = {}
    vals.update(custom_background_values(component_id, log_id, state, ts_ms, key))
    vals.update(overrides)

    for var, dom in tpl.get("vars", {}).items():
        if var not in vals:
            vals[var] = domain_value(dom, f"{key}:{component_id}.{log_id}:{var}")

    sv = tpl.get("state_vars", {}).get(state, {})
    for var, dom in sv.items():
        if var not in vals:
            vals[var] = domain_value(dom, f"{key}:{component_id}.{log_id}:{var}:{state}")

    msg = tpl["msg"].format(**vals)
    lvl = tpl["lvl"]
    return lvl, msg


def emit_row(rows: List[Dict[str, Any]], ts_ms: int, level: str, message: str, trace_id: str, service: str, host: str, seq: int):
    rows.append(
        {"ts_ms": ts_ms, "seq": seq, "timestamp": "", "level": level, "message": message, "trace_id": trace_id, "service": service or "", "host": host or ""}
    )


def simulate_background(interval: Interval, rounding: AccumulatorRounding, rows: List[Dict[str, Any]], seq_start: int) -> int:
    seq = seq_start
    start_ms, end_ms = interval_to_ms(interval)
    dur_min = (interval.end_min - interval.start_min)

    for comp in SYSTEM["components"]:
        cid = comp["id"]
        beh = comp.get("beh", {}).get(interval.state, {}).get("emit", [])
        for e in beh:
            log_id = e["id"]
            per_min = float(e["per_min"])
            scope = e.get("scope", "per_host")
            ref_key = f"{cid}.{log_id}"

            per_min_eff = per_min * (get_rate_multiplier(interval, ref_key) if interval.state == "f" else 1.0)
            if per_min_eff <= 0.0 or dur_min <= 0:
                continue

            if scope == "per_host":
                for h in comp.get("hosts", []) or [""]:
                    exp = per_min_eff * dur_min
                    count = rounding.alloc(f"bg:{ref_key}:{h}:{interval.start_min}", exp)
                    if count <= 0:
                        continue
                    times = schedule_evenly(start_ms, end_ms, count, f"bg:{ref_key}:{h}:{interval.start_min}")
                    for idx, t in enumerate(times):
                        lvl, msg = render_log(cid, log_id, interval.state, t, f"bg:{ref_key}:{h}:{interval.start_min}:{idx}", {})
                        emit_row(rows, t, lvl, msg, "", comp.get("svc", ""), h, seq)
                        seq += 1
            else:  # global
                exp = per_min_eff * dur_min
                count = rounding.alloc(f"bg:{ref_key}:{interval.start_min}", exp)
                if count <= 0:
                    continue
                times = schedule_evenly(start_ms, end_ms, count, f"bg:{ref_key}:{interval.start_min}")
                for idx, t in enumerate(times):
                    h = host_for_background(cid, None, idx)
                    lvl, msg = render_log(cid, log_id, interval.state, t, f"bg:{ref_key}:{interval.start_min}:{idx}", {})
                    emit_row(rows, t, lvl, msg, "", comp.get("svc", ""), h, seq)
                    seq += 1
    return seq


def flow_instance_context(flow_id: str, state: str, inst_key: str) -> Dict[str, Any]:
    build_id = choose_uuid_str(f"{inst_key}:build_id")
    queue = choose_from_list(["com", "org"], f"{inst_key}:queue")
    vm_id = choose_hex(12, f"{inst_key}:vm_id")
    res_mhz = choose_int(800, 2000, f"{inst_key}:res_mhz")
    esx_host = choose_from_list(["esx01", "esx02", "esx03", "esx04"], f"{inst_key}:esx_host")

    ctx: Dict[str, Any] = {
        "build_id": build_id,
        "repo": "owner/repo",
        "queue": queue,
        "vm_id": vm_id,
        "template": "xcode6_3",
        "res_mhz": res_mhz,
        "esx_host": esx_host,
    }

    if flow_id in ("osx_build_start_fail_leak", "osx_build_start_fail_cpu_resv"):
        need = res_mhz
        free = choose_int(0, 700, f"{inst_key}:free_mhz")
        ctx.update({"cpu_mhz_needed": need, "cpu_mhz_free": free, "need_mhz": need, "avail_mhz": free, "reason": "vm_boot_failed"})
    return ctx


def _get_int_range_for_var(component_id: str, log_id: str, var: str) -> Optional[Tuple[int, int]]:
    tpl = LOG_TPLS.get((component_id, log_id))
    if not tpl:
        return None
    dom = tpl.get("vars", {}).get(var)
    if not dom or dom.get("k") != "i":
        return None
    lo, hi = int(dom["v"][0]), int(dom["v"][1])
    return lo, hi


def simulate_flow_instances(interval: Interval, rounding: AccumulatorRounding, rows: List[Dict[str, Any]], seq_start: int) -> int:
    seq = seq_start
    start_ms, end_ms = interval_to_ms(interval)
    dur_min = (interval.end_min - interval.start_min)
    if dur_min <= 0:
        return seq

    for flow in SYSTEM["flows"][interval.state]["req"]:
        fid = flow["id"]
        rpm = float(flow["rpm"])
        rpm_eff = rpm * (get_rate_multiplier(interval, fid) if interval.state == "f" else 1.0)
        if rpm_eff <= 0.0:
            continue

        expected = rpm_eff * dur_min
        n_inst = rounding.alloc(f"flow:{interval.state}:{fid}:{interval.start_min}", expected)
        if n_inst <= 0:
            continue

        starts = schedule_evenly(start_ms, end_ms, n_inst, f"flowstart:{interval.state}:{fid}:{interval.start_min}", jitter_ms=400)
        lat_p50_mul, lat_p95_mul = get_latency_multiplier(interval, fid)

        for j, st in enumerate(starts):
            inst_key = f"{interval.state}:{fid}:{interval.start_min}:{j}:{st}"
            trace_id = choose_hex(32, f"trace:{inst_key}") if (SYSTEM["tracing"]["on"] and flow.get("trace", False)) else ""

            ctx = flow_instance_context(fid, interval.state, inst_key)

            comp_host_cache: Dict[str, str] = {}
            t = st
            sampled_latencies: List[int] = []
            for li, (p50, p95) in enumerate(flow["latency_ms"]):
                p50_eff = float(p50) * lat_p50_mul
                p95_eff = float(p95) * lat_p95_mul
                u = stable_u(f"{inst_key}:lat:{li}")
                dt = int(round(sample_lognormal_from_p50_p95(p50_eff, p95_eff, u)))
                sampled_latencies.append(dt)

            # Bind boot_ms from the actual applied timestamp gap to keep message fields coherent.
            # Also clamp that gap to the boot_ms variable domain so the message doesn't contradict itself.
            if "osx_vm_manager.vm_poweron_ok" in flow["emit"]:
                idx_poweron = flow["emit"].index("osx_vm_manager.vm_poweron_ok")
                rng = _get_int_range_for_var("osx_vm_manager", "vm_poweron_ok", "boot_ms")
                if rng and 0 <= idx_poweron < len(sampled_latencies):
                    lo, hi = rng
                    boot_gap = int(max(lo, min(hi, sampled_latencies[idx_poweron])))
                    sampled_latencies[idx_poweron] = boot_gap
                    ctx["boot_ms"] = boot_gap

            for li, ref in enumerate(flow["emit"]):
                cid, log_id = ref.split(".", 1)
                if cid not in comp_host_cache:
                    comp_host_cache[cid] = component_host_for_flow(trace_id, cid)
                t = t + sampled_latencies[li]

                overrides = dict(ctx)
                if cid == "build_scheduler" and log_id == "build_requeued":
                    overrides["reason"] = ctx.get("reason", "vm_boot_failed")

                lvl, msg = render_log(cid, log_id, interval.state, t, f"flow:{inst_key}:{li}", overrides)
                emit_row(rows, t, lvl, msg, trace_id, COMPONENTS[cid].get("svc", ""), comp_host_cache[cid], seq)
                seq += 1

    return seq


def emit_one_shots(rows: List[Dict[str, Any]], seq_start: int) -> int:
    seq = seq_start
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for ev in events:
        at_min = int(ev["at_min"])
        base_ms = ms(BASE_TIME + timedelta(minutes=at_min))
        for os_idx, os in enumerate(ev.get("one_shots", [])):
            ref = os["ref"]
            cid, log_id = ref.split(".", 1)
            count = int(os["count"])
            hosts = os.get("hosts", []) or []
            for k in range(count):
                jitter = int((stable_u(f"oneshot:{ref}:{at_min}:{os_idx}:{k}") - 0.5) * 800)
                t = base_ms + jitter
                h = hosts[k % len(hosts)] if hosts else host_for_background(cid, None, k)

                overrides: Dict[str, Any] = {}
                if ref == "build_scheduler.pause_toggle":
                    overrides["action"] = "paused" if at_min == 30 else "resumed"
                    overrides["actor"] = "oncall"
                    overrides["scope"] = "com+org"
                elif ref == "vsphere_janitor.janitor_restart":
                    overrides["reason"] = "config_update"
                elif ref == "vsphere_janitor.cleanup_mode_set":
                    overrides["mode"] = "aggressive"
                elif ref == "build_scheduler.capacity_limit_set":
                    overrides["max_concurrent"] = 80
                    overrides["scope"] = "com+org"

                lvl, msg = render_log(cid, log_id, "f", t, f"oneshot:{ref}:{at_min}:{os_idx}:{k}", overrides)
                emit_row(rows, t, lvl, msg, "", COMPONENTS[cid].get("svc", ""), h, seq)
                seq += 1
    return seq


def main():
    random.seed(0)
    np.random.seed(0)

    rounding = AccumulatorRounding()
    rows: List[Dict[str, Any]] = []
    seq = 0

    seq = simulate_background(NORMAL_INTERVAL, rounding, rows, seq)
    seq = simulate_flow_instances(NORMAL_INTERVAL, rounding, rows, seq)

    for iv in FAILURE_INTERVALS:
        seq = simulate_background(iv, rounding, rows, seq)
        seq = simulate_flow_instances(iv, rounding, rows, seq)

    seq = emit_one_shots(rows, seq)

    df = pd.DataFrame(rows)
    if df.empty:
        df = pd.DataFrame(columns=["timestamp", "level", "message", "trace_id", "service", "host"])
        df.to_csv("logs.csv", index=False)
        return

    df = df.sort_values(["ts_ms", "seq"], ascending=[True, True]).reset_index(drop=True)
    df["timestamp"] = df["ts_ms"].apply(fmt_ts)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    assert list(df.columns) == ["timestamp", "level", "message", "trace_id", "service", "host"]
    ts_list = df["timestamp"].tolist()
    assert ts_list == sorted(ts_list)

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
