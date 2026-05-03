from __future__ import annotations

import math
import hashlib
import ipaddress
import uuid
import random
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# ----------------------------
# Embedded executable spec
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "us_east_1_az_power_control_planes"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["control_plane_api", "elb_control_plane"], "trace_id": {"k": "hex", "v": 32}},
    "components": {
        "client": {
            "svc": None,
            "hosts": ["external"],
            "logs": {
                "client_call": {
                    "lvl": "DEBUG",
                    "msg": "client call kind={kind} target={target} req_id={req_id}",
                    "vars": {
                        "kind": {"k": "ch", "v": ["api", "dns", "connect"]},
                        "target": {"k": "ch", "v": ["ec2", "ebs", "elb", "dns"]},
                        "req_id": {"k": "uuid", "v": None},
                    },
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "dc_power_az1": {
            "svc": None,
            "hosts": ["use1-az1-facility"],
            "logs": {
                "utility_event": {
                    "lvl": "CRITICAL",
                    "msg": "utility power event state={state} az={az}",
                    "vars": {"state": {"k": "ch", "v": ["fluctuation", "loss", "restore"]}, "az": {"k": "ch", "v": ["use1-az1"]}},
                },
                "gen_sync_fail": {
                    "lvl": "ERROR",
                    "msg": "generator sync failed voltage_v={voltage_v} freq_hz={freq_hz} az={az}",
                    "vars": {"voltage_v": {"k": "f", "v": [0.0, 520.0]}, "freq_hz": {"k": "f", "v": [0.0, 70.0]}, "az": {"k": "ch", "v": ["use1-az1"]}},
                },
                "ups_metric": {
                    "lvl": "INFO",
                    "msg": "UPS on_battery={on_battery} charge_pct={charge_pct} az={az}",
                    "vars": {"on_battery": {"k": "ch", "v": ["true", "false"]}, "charge_pct": {"k": "i", "v": [0, 100]}, "az": {"k": "ch", "v": ["use1-az1"]}},
                },
                "rack_power_loss": {
                    "lvl": "CRITICAL",
                    "msg": "rack power lost racks_offline={racks_offline} az={az}",
                    "vars": {"racks_offline": {"k": "i", "v": [10, 900]}, "az": {"k": "ch", "v": ["use1-az1"]}},
                },
                "power_restore": {
                    "lvl": "INFO",
                    "msg": "facility power stabilized source={source} az={az}",
                    "vars": {"source": {"k": "ch", "v": ["utility", "generator"]}, "az": {"k": "ch", "v": ["use1-az1"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "ups_metric", "per_min": 0.5, "scope": "global"}]},
                "f": {"emit": [{"id": "ups_metric", "per_min": 2.0, "scope": "global"}]},
            },
        },
        "metadata_store": {
            "svc": "metadata-store",
            "hosts": ["meta-az1", "meta-az2", "meta-az3"],
            "logs": {
                "write_commit": {
                    "lvl": "INFO",
                    "msg": "write committed op={op} req_id={req_id} leader={leader} dur_ms={dur_ms}",
                    "vars": {
                        "op": {"k": "ch", "v": ["RunInstances", "AttachVolume"]},
                        "req_id": {"k": "uuid", "v": None},
                        "leader": {"k": "ch", "v": ["meta-az1", "meta-az2", "meta-az3"]},
                        "dur_ms": {"k": "i", "v": [5, 5000]},
                    },
                },
                "write_rejected_readonly": {
                    "lvl": "WARN",
                    "msg": "write rejected (read-only replica) op={op} req_id={req_id} leader={leader}",
                    "vars": {
                        "op": {"k": "ch", "v": ["RunInstances", "AttachVolume"]},
                        "req_id": {"k": "uuid", "v": None},
                        "leader": {"k": "ch", "v": ["meta-az1", "meta-az2", "meta-az3"]},
                    },
                },
                "leader_change": {
                    "lvl": "INFO",
                    "msg": "replica role change old={old_role} new={new_role} leader={leader}",
                    "vars": {
                        "old_role": {"k": "ch", "v": ["primary", "replica-rw", "replica-ro", "down"]},
                        "new_role": {"k": "ch", "v": ["primary", "replica-rw", "replica-ro", "down"]},
                        "leader": {"k": "ch", "v": ["meta-az1", "meta-az2", "meta-az3"]},
                    },
                },
                "replication_health": {
                    "lvl": "INFO",
                    "msg": "replication health role={role} leader={leader} lag_ms={lag_ms}",
                    "vars": {
                        "role": {"k": "ch", "v": ["primary", "replica-rw", "replica-ro"]},
                        "leader": {"k": "ch", "v": ["meta-az1", "meta-az2", "meta-az3"]},
                        "lag_ms": {"k": "i", "v": [0, 60000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "replication_health", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "replication_health", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        "control_plane_api": {
            "svc": "compute-storage-api",
            "hosts": ["api-1", "api-2"],
            "logs": {
                "api_request": {
                    "lvl": "INFO",
                    "msg": "API {api} request received req_id={req_id} account={account} region={region}",
                    "vars": {"api": {"k": "ch", "v": ["RunInstances", "AttachVolume"]}, "req_id": {"k": "uuid", "v": None}, "account": {"k": "i", "v": [100000000000, 999999999999]}, "region": {"k": "ch", "v": ["us-east-1"]}},
                },
                "api_response_ok": {
                    "lvl": "INFO",
                    "msg": "API {api} succeeded req_id={req_id} status=200 latency_ms={latency_ms}",
                    "vars": {"api": {"k": "ch", "v": ["RunInstances", "AttachVolume"]}, "req_id": {"k": "uuid", "v": None}, "latency_ms": {"k": "i", "v": [10, 120000]}},
                },
                "api_response_err": {
                    "lvl": "WARN",
                    "msg": "API {api} failed req_id={req_id} status=409 err=ReadOnlyMode latency_ms={latency_ms}",
                    "vars": {"api": {"k": "ch", "v": ["RunInstances", "AttachVolume"]}, "req_id": {"k": "uuid", "v": None}, "latency_ms": {"k": "i", "v": [10, 120000]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "elb_control_plane": {
            "svc": "elb-control-plane",
            "hosts": ["elbcp-1", "elbcp-2"],
            "logs": {
                "mgmt_request": {
                    "lvl": "INFO",
                    "msg": "ELB mgmt request action={action} lb={lb} req_id={req_id}",
                    "vars": {"action": {"k": "ch", "v": ["RegisterTargets", "DeregisterTargets", "ScaleLoadBalancer"]}, "lb": {"k": "str", "v": "lb-[a-z]{4}-[0-9]{2}"}, "req_id": {"k": "uuid", "v": None}},
                },
                "enqueue_task": {
                    "lvl": "DEBUG",
                    "msg": "enqueued task type={task} lb={lb} queue=region-main qdepth={qdepth}",
                    "vars": {"task": {"k": "ch", "v": ["traffic_shift", "scale", "register_targets"]}, "lb": {"k": "str", "v": "lb-[a-z]{4}-[0-9]{2}"}, "qdepth": {"k": "i", "v": [0, 250000]}},
                },
                "task_processed": {
                    "lvl": "INFO",
                    "msg": "processed task type={task} lb={lb} result=ok latency_ms={latency_ms}",
                    "vars": {"task": {"k": "ch", "v": ["traffic_shift", "scale", "register_targets"]}, "lb": {"k": "str", "v": "lb-[a-z]{4}-[0-9]{2}"}, "latency_ms": {"k": "i", "v": [10, 600000]}},
                },
                "queue_metric": {
                    "lvl": "INFO",
                    "msg": "queue region-main depth={depth} oldest_age_s={oldest_age_s} rate_in={rate_in} rate_out={rate_out}",
                    "vars": {"depth": {"k": "i", "v": [0, 300000]}, "oldest_age_s": {"k": "i", "v": [0, 7200]}, "rate_in": {"k": "i", "v": [0, 8000]}, "rate_out": {"k": "i", "v": [0, 8000]}},
                },
                "bug_scale_trigger": {
                    "lvl": "ERROR",
                    "msg": "unexpected scaling evaluation for lb={lb} reason={reason} desired_size={desired_size}",
                    "vars": {"lb": {"k": "str", "v": "lb-[a-z]{4}-[0-9]{2}"}, "reason": {"k": "ch", "v": ["recovered_state_mismatch", "missing_health_snapshot"]}, "desired_size": {"k": "ch", "v": ["large", "xlarge"]}},
                },
                "operator_action": {
                    "lvl": "INFO",
                    "msg": "operator action={action} detail={detail}",
                    "vars": {"action": {"k": "ch", "v": ["apply_rate_limit", "disable_auto_scale_eval"]}, "detail": {"k": "ch", "v": ["region-main", "scale_tasks"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "queue_metric", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "queue_metric", "per_min": 1.5, "scope": "per_host"}, {"id": "bug_scale_trigger", "per_min": 0.2, "scope": "per_host"}]},
            },
        },
        "dns_service": {
            "svc": "dns",
            "hosts": ["dns-1", "dns-2"],
            "logs": {
                "record_update": {
                    "lvl": "INFO",
                    "msg": "updated DNS A records name={name} action={action} ip={ip} ttl_s={ttl_s}",
                    "vars": {"name": {"k": "str", "v": "lb-[a-z]{4}-[0-9]{2}\\.elb\\.use1\\.example\\.com"}, "action": {"k": "ch", "v": ["add", "remove"]}, "ip": {"k": "ip", "v": "54.0.0.0/8"}, "ttl_s": {"k": "i", "v": [30, 300]}},
                },
                "dns_query": {
                    "lvl": "DEBUG",
                    "msg": "DNS query name={name} client_type={client_type} answered_ips={answered_ips}",
                    "vars": {"name": {"k": "str", "v": "lb-[a-z]{4}-[0-9]{2}\\.elb\\.use1\\.example\\.com"}, "client_type": {"k": "ch", "v": ["browser", "console", "iot", "sdk"]}, "answered_ips": {"k": "i", "v": [1, 4]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "elb_edge": {
            "svc": "elb-edge",
            "hosts": ["elb-az1", "elb-az2", "elb-az3"],
            "logs": {
                "connect_ok": {
                    "lvl": "INFO",
                    "msg": "client request served lb={lb} az={az} code=200 latency_ms={latency_ms}",
                    "vars": {"lb": {"k": "str", "v": "lb-[a-z]{4}-[0-9]{2}"}, "az": {"k": "ch", "v": ["use1-az1", "use1-az2", "use1-az3"]}, "latency_ms": {"k": "i", "v": [5, 30000]}},
                },
                "connect_fail": {
                    "lvl": "WARN",
                    "msg": "client connection failed lb={lb} az={az} reason={reason}",
                    "vars": {"lb": {"k": "str", "v": "lb-[a-z]{4}-[0-9]{2}"}, "az": {"k": "ch", "v": ["use1-az1", "use1-az2", "use1-az3"]}, "reason": {"k": "ch", "v": ["timeout", "no_route", "conn_refused"]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "ebs_recovery": {
            "svc": "ebs-recovery",
            "hosts": ["ebsrec-1, ebsrec-2".replace(",", ""), "ebsrec-2"],  # keep deterministic; normalized below
            "logs": {
                "recovery_progress": {
                    "lvl": "INFO",
                    "msg": "ebs recovery backlog={backlog} processing_rate_vpm={processing_rate_vpm} completed={completed}",
                    "vars": {"backlog": {"k": "i", "v": [0, 40000]}, "processing_rate_vpm": {"k": "i", "v": [10, 4000]}, "completed": {"k": "i", "v": [0, 40000]}},
                },
                "volume_state_impaired": {
                    "lvl": "WARN",
                    "msg": "volume {vol_id} opened in impaired state io_paused=true reason={reason}",
                    "vars": {"vol_id": {"k": "str", "v": "vol-[0-9a-f]{8,12}"}, "reason": {"k": "ch", "v": ["in_flight_writes", "journal_replay"]}},
                },
                "volume_recovered": {
                    "lvl": "INFO",
                    "msg": "volume {vol_id} recovery complete io_paused=false",
                    "vars": {"vol_id": {"k": "str", "v": "vol-[0-9a-f]{8,12}"}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "recovery_progress", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "recovery_progress", "per_min": 1.0, "scope": "per_host"}, {"id": "volume_state_impaired", "per_min": 2.5, "scope": "per_host"}, {"id": "volume_recovered", "per_min": 2.0, "scope": "per_host"}]},
            },
        },
    },
    "flows": {
        "n": [
            {"id": "ec2_run_instances_n", "rpm": 70.0, "emit": ["control_plane_api.api_request", "metadata_store.write_commit", "control_plane_api.api_response_ok"], "latency_ms": [[1, 3], [5, 30], [20, 250]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "ebs_attach_volume_n", "rpm": 40.0, "emit": ["control_plane_api.api_request", "metadata_store.write_commit", "control_plane_api.api_response_ok"], "latency_ms": [[1, 3], [5, 35], [25, 350]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "elb_register_targets_n", "rpm": 30.0, "emit": ["elb_control_plane.mgmt_request", "elb_control_plane.enqueue_task", "elb_control_plane.task_processed", "dns_service.record_update"], "latency_ms": [[1, 2], [1, 5], [20, 200], [5, 50]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "elb_shift_traffic_n", "rpm": 50.0, "emit": ["elb_control_plane.task_processed", "dns_service.record_update"], "latency_ms": [[15, 150], [5, 40]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "elb_client_request_n", "rpm": 120.0, "emit": ["dns_service.dns_query", "elb_edge.connect_ok"], "latency_ms": [[1, 3], [20, 250]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
        ],
        "f": [
            {"id": "ec2_run_instances_f_readonly", "rpm": 80.0, "emit": ["control_plane_api.api_request", "metadata_store.write_rejected_readonly", "control_plane_api.api_response_err"], "latency_ms": [[1, 3], [2, 15], [30, 1200]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "ec2_run_instances_f_ok", "rpm": 40.0, "emit": ["control_plane_api.api_request", "metadata_store.write_commit", "control_plane_api.api_response_ok"], "latency_ms": [[1, 3], [10, 80], [40, 800]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "ebs_attach_volume_f_readonly", "rpm": 50.0, "emit": ["control_plane_api.api_request", "metadata_store.write_rejected_readonly", "control_plane_api.api_response_err"], "latency_ms": [[1, 3], [2, 15], [30, 1500]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "ebs_attach_volume_f_ok", "rpm": 25.0, "emit": ["control_plane_api.api_request", "metadata_store.write_commit", "control_plane_api.api_response_ok"], "latency_ms": [[1, 3], [10, 100], [50, 1200]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "elb_register_targets_f", "rpm": 60.0, "emit": ["elb_control_plane.mgmt_request", "elb_control_plane.enqueue_task", "elb_control_plane.task_processed", "dns_service.record_update"], "latency_ms": [[1, 3], [1, 10], [200, 60000], [5, 200]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "elb_shift_traffic_f", "rpm": 70.0, "emit": ["elb_control_plane.task_processed", "dns_service.record_update"], "latency_ms": [[200, 60000], [5, 200]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "elb_client_request_f_ok", "rpm": 110.0, "emit": ["dns_service.dns_query", "elb_edge.connect_ok"], "latency_ms": [[1, 5], [30, 600]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
            {"id": "elb_client_request_f_fail", "rpm": 2.0, "emit": ["dns_service.dns_query", "elb_edge.connect_fail"], "latency_ms": [[1, 5], [200, 5000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
        ],
    },
}

# Normalize a small typo introduced in embedding above (keeps deterministic output stable).
SYSTEM["components"]["ebs_recovery"]["hosts"] = ["ebsrec-1", "ebsrec-2"]

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "use1_az1_power_loss_control_plane_backlog"},
    "states": {"n": "normal", "f": "failure"},
    "time": {"total_minutes": 60, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 60}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 25,
                    "rate_multipliers": {
                        "ec2_run_instances_f_ok": 0.0,
                        "ebs_attach_volume_f_ok": 0.0,
                        "ec2_run_instances_f_readonly": 1.3,
                        "ebs_attach_volume_f_readonly": 1.3,
                        "elb_client_request_f_fail": 2.0,
                        "dc_power_az1.ups_metric": 2.5,
                        "elb_control_plane.bug_scale_trigger": 0.0,
                        "ebs_recovery.recovery_progress": 0.0,
                        "ebs_recovery.volume_state_impaired": 0.0,
                        "ebs_recovery.volume_recovered": 0.0,
                    },
                    "latency_multipliers": {"elb_shift_traffic_f": {"p50": 1.3, "p95": 1.8}},
                    "one_shots": [
                        {"ref": "dc_power_az1.utility_event", "count": 1, "hosts": ["use1-az1-facility"]},
                        {"ref": "dc_power_az1.gen_sync_fail", "count": 3, "hosts": ["use1-az1-facility"]},
                        {"ref": "dc_power_az1.rack_power_loss", "count": 1, "hosts": ["use1-az1-facility"]},
                        {"ref": "metadata_store.leader_change", "count": 2, "hosts": ["meta-az2", "meta-az3"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 35,
                    "rate_multipliers": {
                        "elb_register_targets_f": 1.6,
                        "elb_shift_traffic_f": 1.2,
                        "elb_client_request_f_fail": 3.0,
                        "elb_control_plane.queue_metric": 2.5,
                        "elb_control_plane.bug_scale_trigger": 10.0,
                        "ebs_recovery.recovery_progress": 2.0,
                        "ebs_recovery.volume_state_impaired": 2.0,
                        "ebs_recovery.volume_recovered": 1.0,
                    },
                    "latency_multipliers": {"elb_register_targets_f": {"p50": 4.0, "p95": 6.0}, "elb_shift_traffic_f": {"p50": 5.0, "p95": 7.0}},
                    "one_shots": [],
                },
                {
                    "order": 3,
                    "at_min": 42,
                    "rate_multipliers": {"ec2_run_instances_f_ok": 1.0, "ebs_attach_volume_f_ok": 1.0, "ec2_run_instances_f_readonly": 0.2, "ebs_attach_volume_f_readonly": 0.2},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "metadata_store.leader_change", "count": 1, "hosts": ["meta-az2"]}],
                },
                {
                    "order": 4,
                    "at_min": 52,
                    "rate_multipliers": {"elb_control_plane.bug_scale_trigger": 2.0, "elb_control_plane.queue_metric": 1.8, "elb_client_request_f_fail": 1.3, "ebs_recovery.volume_recovered": 1.4},
                    "latency_multipliers": {"elb_register_targets_f": {"p50": 2.5, "p95": 4.0}, "elb_shift_traffic_f": {"p50": 3.0, "p95": 5.0}},
                    "one_shots": [{"ref": "elb_control_plane.operator_action", "count": 1, "hosts": ["elbcp-1"]}],
                },
            ]
        }
    },
}

# ----------------------------
# Deterministic helpers
# ----------------------------

SEED = 1337
random.seed(SEED)
RNG = np.random.default_rng(SEED)


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def md5_int(s: str) -> int:
    return int(md5_hex(s)[:16], 16)


def unit_float(s: str) -> float:
    return (md5_int(s) % 10_000_000) / 10_000_000.0


def det_uuid(salt: str) -> str:
    b = bytearray(hashlib.md5(salt.encode("utf-8")).digest())
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    u = uuid.UUID(bytes=bytes(b))
    return str(u)


def det_hex(salt: str, n: int) -> str:
    h = hashlib.md5(salt.encode("utf-8")).hexdigest()
    if n <= 32:
        return h[:n]
    out = h
    i = 1
    while len(out) < n:
        out += hashlib.md5(f"{salt}:{i}".encode("utf-8")).hexdigest()
        i += 1
    return out[:n]


def fmt_ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def clamp(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def lognorm_ms(p50_ms: float, p95_ms: float, salt: str) -> int:
    p50 = max(1e-3, float(p50_ms))
    p95 = max(p50 * 1.001, float(p95_ms))
    mu = math.log(p50)
    sigma = max(1e-6, (math.log(p95) - mu) / 1.645)

    u = unit_float(f"delay_u:{salt}")

    def erfinv(y: float) -> float:
        a = 0.147
        sgn = 1.0 if y >= 0 else -1.0
        ln = math.log(1.0 - y * y)
        tt1 = 2.0 / (math.pi * a) + ln / 2.0
        tt2 = 1.0 / a * ln
        return sgn * math.sqrt(max(0.0, math.sqrt(tt1 * tt1 - tt2) - tt1))

    z = math.sqrt(2.0) * erfinv(clamp(2.0 * u - 1.0, -0.999999, 0.999999))
    x = math.exp(mu + sigma * z)
    soft_cap = 3.0 * p95
    x = min(x, soft_cap * (0.9 + 0.2 * unit_float(f"delay_cap_jitter:{salt}")))
    return int(max(1, round(x)))


def schedule_evenly(start: datetime, end: datetime, count: int, salt: str) -> List[datetime]:
    if count <= 0:
        return []
    total_s = (end - start).total_seconds()
    if total_s <= 0:
        return [start for _ in range(count)]
    step = total_s / count
    jitter_amp = min(0.5, 0.15 * step)
    out = []
    for i in range(count):
        base = (i + 0.5) * step
        j = (unit_float(f"{salt}:j:{i}") - 0.5) * 2.0 * jitter_amp
        t = start + timedelta(seconds=base + j)
        if t < start:
            t = start + timedelta(milliseconds=i % 100)
        if t >= end:
            t = end - timedelta(milliseconds=(count - i) % 100 + 1)
        out.append(t)
    out.sort()
    return out


# ----------------------------
# Indices / lookups
# ----------------------------

COMP = SYSTEM["components"]
FLOW_BY_STATE_ID: Dict[Tuple[str, str], Dict[str, Any]] = {}
for st, flows in SYSTEM["flows"].items():
    for f in flows:
        FLOW_BY_STATE_ID[(st, f["id"])] = f

LOG_TPL: Dict[str, Dict[str, Any]] = {}
LOG_META: Dict[str, Dict[str, Any]] = {}
for cid, c in COMP.items():
    for lid, tpl in c["logs"].items():
        ref = f"{cid}.{lid}"
        LOG_TPL[ref] = tpl
        LOG_META[ref] = {"component_id": cid, "log_id": lid, "svc": c.get("svc"), "hosts": c.get("hosts", [])}


def int_var_range(ref: str, var: str, default: Tuple[int, int]) -> Tuple[int, int]:
    tpl = LOG_TPL.get(ref, {})
    vs = tpl.get("vars", {}).get(var)
    if not vs:
        return default
    if vs.get("k") != "i":
        return default
    v = vs.get("v")
    if not (isinstance(v, list) and len(v) == 2):
        return default
    return int(v[0]), int(v[1])


# ----------------------------
# Failure control intervals
# ----------------------------

@dataclass(frozen=True)
class Interval:
    state: str
    start_min: int
    end_min: int
    rate_flow: Dict[str, float]
    rate_bg: Dict[str, float]
    lat_flow: Dict[str, Dict[str, float]]


def build_failure_intervals() -> Tuple[List[Interval], List[Dict[str, Any]]]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: e["at_min"])
    event_by_min = {e["at_min"]: e for e in events}
    bounds = [f_start] + [e["at_min"] for e in events if f_start < e["at_min"] < f_end] + [f_end]
    bounds = sorted(set(bounds))

    rate_flow: Dict[str, float] = {}
    rate_bg: Dict[str, float] = {}
    lat_flow: Dict[str, Dict[str, float]] = {}

    intervals: List[Interval] = []
    for i in range(len(bounds) - 1):
        s = bounds[i]
        e = bounds[i + 1]
        if s in event_by_min:
            ev = event_by_min[s]
            for k, v in ev.get("rate_multipliers", {}).items():
                if "." in k:
                    rate_bg[k] = float(v)
                else:
                    rate_flow[k] = float(v)
            for fid, mm in ev.get("latency_multipliers", {}).items():
                lat_flow[fid] = {"p50": float(mm["p50"]), "p95": float(mm["p95"])}

        intervals.append(
            Interval(
                state="f",
                start_min=s,
                end_min=e,
                rate_flow=dict(rate_flow),
                rate_bg=dict(rate_bg),
                lat_flow=deepcopy(lat_flow),
            )
        )
    return intervals, events


FAIL_INTERVALS, FAIL_EVENTS = build_failure_intervals()


# ----------------------------
# Variable generation
# ----------------------------

DNS_NAME_HINT = "lb-[a-z]{4}-[0-9]{2}\\.elb\\.use1\\.example\\.com"
VOL_HINT = "vol-[0-9a-f]{8,12}"


def gen_lb_name(salt: str) -> str:
    letters = "abcdefghijklmnopqrstuvwxyz"
    h = md5_hex(f"lb:{salt}")
    a = "".join(letters[int(h[i:i + 2], 16) % 26] for i in range(0, 8, 2))
    d = int(h[8:10], 16) % 100
    return f"lb-{a}-{d:02d}"


def gen_dns_name_from_lb(lb: str) -> str:
    return f"{lb}.elb.use1.example.com"


def gen_vol_id(salt: str) -> str:
    h = md5_hex(f"vol:{salt}")
    return f"vol-{h[:10]}"


def gen_ip_in_cidr(cidr: str, salt: str) -> str:
    net = ipaddress.ip_network(cidr, strict=False)
    host_bits = net.max_prefixlen - net.prefixlen
    if host_bits <= 1:
        return str(net.network_address)
    n_hosts = 1 << host_bits
    x = md5_int(f"ip:{salt}") % n_hosts
    return str(net.network_address + x)


def choose_from_list(vals: List[Any], salt: str) -> Any:
    if not vals:
        return ""
    idx = md5_int(f"ch:{salt}") % len(vals)
    return vals[idx]


def gen_int(lo: int, hi: int, salt: str) -> int:
    if hi < lo:
        lo, hi = hi, lo
    span = hi - lo + 1
    return lo + (md5_int(f"i:{salt}") % span)


def gen_float(lo: float, hi: float, salt: str) -> float:
    if hi < lo:
        lo, hi = hi, lo
    u = unit_float(f"f:{salt}")
    v = lo + (hi - lo) * u
    return round(v, 1)


def fill_vars(domains: Dict[str, Any], ctx: Dict[str, Any], salt: str) -> Dict[str, Any]:
    out = {}
    for k, spec in domains.items():
        if k in ctx:
            out[k] = ctx[k]
            continue
        kind = spec["k"]
        v = spec.get("v")
        if kind == "ch":
            out[k] = choose_from_list(list(v), f"{salt}:{k}")
        elif kind == "i":
            out[k] = int(gen_int(int(v[0]), int(v[1]), f"{salt}:{k}"))
        elif kind == "f":
            out[k] = float(gen_float(float(v[0]), float(v[1]), f"{salt}:{k}"))
        elif kind == "uuid":
            out[k] = det_uuid(f"{salt}:{k}")
        elif kind == "hex":
            out[k] = det_hex(f"{salt}:{k}", int(v))
        elif kind == "ip":
            out[k] = gen_ip_in_cidr(str(v), f"{salt}:{k}")
        elif kind == "str":
            hint = str(v)
            if hint == "lb-[a-z]{4}-[0-9]{2}":
                out[k] = gen_lb_name(f"{salt}:{k}")
            elif hint == DNS_NAME_HINT:
                lb = ctx.get("lb", gen_lb_name(f"{salt}:{k}:lb"))
                out[k] = gen_dns_name_from_lb(lb)
            elif hint == VOL_HINT:
                out[k] = gen_vol_id(f"{salt}:{k}")
            else:
                out[k] = f"{hint}:{det_hex(f'{salt}:{k}', 8)}"
        else:
            out[k] = ""
    return out


def render_log(ref: str, ctx: Dict[str, Any], salt: str) -> Tuple[str, str]:
    tpl = LOG_TPL[ref]
    lvl = tpl["lvl"]
    vars_spec = tpl.get("vars", {})
    vals = fill_vars(vars_spec, ctx, salt)
    msg = tpl["msg"].format(**vals)
    return lvl, msg


def pick_host(component_id: str, salt: str, allowed_hosts: Optional[List[str]] = None) -> str:
    hosts = list(COMP[component_id].get("hosts", []))
    if allowed_hosts is not None:
        allowed = set(allowed_hosts)
        hosts = [h for h in hosts if h in allowed]
    if not hosts:
        return ""
    return hosts[md5_int(f"host:{salt}:{component_id}") % len(hosts)]


def az_to_elb_edge_host(az: str) -> str:
    if not az:
        return ""
    m = az.strip().lower()
    if m.endswith("az1"):
        return "elb-az1"
    if m.endswith("az2"):
        return "elb-az2"
    if m.endswith("az3"):
        return "elb-az3"
    return ""


# ----------------------------
# Background value shaping
# ----------------------------

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def minute_of(dt: datetime) -> float:
    return (dt - BASE_TIME).total_seconds() / 60.0


def active_meta_leader(minute: float) -> str:
    if minute < 25:
        return "meta-az1"
    if minute < 42:
        return "meta-az3"
    return "meta-az2"


def queue_shape(minute: float) -> Tuple[int, int, int, int]:
    if minute < 25:
        depth = int(200 + 1800 * (0.3 + 0.7 * math.sin(minute / 3.0) ** 2))
        oldest = int(1 + 8 * (0.2 + 0.8 * math.sin(minute) ** 2))
        rate_in = int(900 + 300 * (0.5 + 0.5 * math.sin(minute / 2.0)))
        rate_out = int(rate_in * (0.98 + 0.02 * math.sin(minute)))
    elif minute < 35:
        t = (minute - 25) / 10.0
        depth = int(5000 + 8000 * t + 3000 * math.sin(minute) ** 2)
        oldest = int(30 + 220 * t)
        rate_in = int(1300 + 500 * t)
        rate_out = int(rate_in * (0.9 - 0.1 * t))
    elif minute < 42:
        t = (minute - 35) / 7.0
        depth = int(60000 + 140000 * t + 15000 * math.sin(minute / 1.5) ** 2)
        oldest = int(600 + 1600 * t)
        rate_in = int(4500 + 2200 * t)
        rate_out = int(3200 + 300 * (1.0 - t))
    elif minute < 52:
        t = (minute - 42) / 10.0
        depth = int(130000 + 60000 * (0.5 + 0.5 * math.sin(minute / 2.2) ** 2) + 25000 * t)
        oldest = int(1200 + 2200 * (0.3 + 0.7 * t))
        rate_in = int(5200 + 800 * math.sin(minute / 2.5))
        rate_out = int(4200 + 200 * math.sin(minute / 3.5))
    else:
        t = (minute - 52) / 8.0
        depth = int(150000 - 45000 * t + 20000 * math.sin(minute / 2.0) ** 2)
        oldest = int(2200 - 900 * t)
        rate_in = int(3600 - 500 * t)
        rate_out = int(3400 - 200 * t)
    depth = int(clamp(depth, 0, 300000))
    oldest = int(clamp(oldest, 0, 7200))
    rate_in = int(clamp(rate_in, 0, 8000))
    rate_out = int(clamp(rate_out, 0, 8000))
    return depth, oldest, rate_in, rate_out


def ups_shape(minute: float) -> Tuple[str, int]:
    if minute < 25:
        return "false", 100
    if minute < 35:
        t = (minute - 25) / 10.0
        charge = int(clamp(95 - 70 * t - 8 * math.sin(minute) ** 2, 0, 100))
        return "true", charge
    if minute < 42:
        t = (minute - 35) / 7.0
        charge = int(clamp(25 - 10 * t + 3 * math.sin(minute) ** 2, 0, 100))
        return "true", charge
    t = min(1.0, (minute - 42) / 18.0)
    charge = int(clamp(20 + 70 * t + 4 * math.sin(minute / 2.0) ** 2, 0, 100))
    on_battery = "false" if minute >= 44 else "true"
    return on_battery, charge


def ebs_recovery_shape(minute: float) -> Tuple[int, int, int]:
    if minute < 35:
        return 0, 0, 0
    t = minute - 35
    backlog = int(clamp(32000 - 650 * t + 1200 * math.sin(minute / 3.0) ** 2, 0, 40000))
    rate = int(clamp(800 + 120 * math.sin(minute / 2.8) + 60 * t, 10, 4000))
    completed = int(clamp(1000 * t + 1500 * math.sin(minute / 4.0) ** 2, 0, 40000))
    return backlog, rate, completed


# ----------------------------
# Count allocation
# ----------------------------

_CARRY: Dict[str, float] = defaultdict(float)


def alloc_count(expected: float, salt: str) -> int:
    x = expected + _CARRY[salt]
    n = int(math.floor(x + 1e-9))
    _CARRY[salt] = x - n
    return max(0, n)


# ----------------------------
# Emission
# ----------------------------

def emit_row(rows: List[Dict[str, Any]], when: datetime, ref: str, trace_id: str, ctx: Dict[str, Any], salt: str, host_override: Optional[str] = None) -> None:
    meta = LOG_META[ref]
    cid = meta["component_id"]
    lvl, msg = render_log(ref, ctx, salt)
    svc = COMP[cid].get("svc") or ""
    host = host_override if host_override is not None else pick_host(cid, salt)
    rows.append(
        {
            "timestamp_dt": when,
            "level": lvl,
            "message": msg,
            "trace_id": trace_id,
            "service": svc,
            "host": host,
        }
    )


def bind_flow_context(flow_id: str, state: str, start_dt: datetime, instance_idx: int) -> Dict[str, Any]:
    m = minute_of(start_dt)
    ctx: Dict[str, Any] = {}

    ctx["req_id"] = det_uuid(f"req:{state}:{flow_id}:{instance_idx}:{fmt_ts(start_dt)}")

    if flow_id.startswith("ec2_run_instances"):
        ctx["api"] = "RunInstances"
        ctx["op"] = "RunInstances"
    elif flow_id.startswith("ebs_attach_volume"):
        ctx["api"] = "AttachVolume"
        ctx["op"] = "AttachVolume"

    if "api" in ctx:
        ctx["region"] = "us-east-1"
        ctx["account"] = 100000000000 + (md5_int(f"acct:{ctx['req_id']}") % (999999999999 - 100000000000 + 1))

    ctx["leader"] = active_meta_leader(m)

    if flow_id.startswith("elb_"):
        lb = gen_lb_name(f"lb:{state}:{flow_id}:{instance_idx}")
        ctx["lb"] = lb
        ctx["name"] = gen_dns_name_from_lb(lb)

    if flow_id.startswith("elb_client_request"):
        ctx["client_type"] = choose_from_list(["browser", "console", "iot", "sdk"], f"ctype:{state}:{flow_id}:{instance_idx}")
        if flow_id.endswith("_fail"):
            ctx["answered_ips"] = 1
        else:
            ctx["answered_ips"] = 3 if state == "n" else 2

        if flow_id == "elb_client_request_n":
            ctx["az"] = choose_from_list(["use1-az1", "use1-az2", "use1-az3"], f"az:n:{flow_id}:{instance_idx}")

    return ctx


def compute_attempts(retry: Dict[str, Any], salt: str) -> int:
    max_attempts = int(retry.get("max_attempts", 1))
    expected = float(retry.get("expected_attempts", 1.0))
    if max_attempts <= 1:
        return 1
    lo = int(math.floor(expected))
    hi = int(math.ceil(expected))
    lo = max(1, min(lo, max_attempts))
    hi = max(1, min(hi, max_attempts))
    if lo == hi:
        return lo
    frac = expected - math.floor(expected)
    return hi if unit_float(f"att:{salt}") < frac else lo


def enforce_timing_domains_for_attempt(delays_ms: List[int], emit_refs: List[str]) -> None:
    if "metadata_store.write_commit" in emit_refs:
        i_wc = emit_refs.index("metadata_store.write_commit")
        lo, hi = int_var_range("metadata_store.write_commit", "dur_ms", (5, 5000))
        delays_ms[i_wc] = int(clamp(delays_ms[i_wc], lo, hi))

    if "elb_control_plane.task_processed" in emit_refs:
        i_tp = emit_refs.index("elb_control_plane.task_processed")
        lo, hi = int_var_range("elb_control_plane.task_processed", "latency_ms", (10, 600000))
        delays_ms[i_tp] = int(clamp(delays_ms[i_tp], lo, hi))

    if "dns_service.dns_query" in emit_refs and "elb_edge.connect_ok" in emit_refs:
        i_q = emit_refs.index("dns_service.dns_query")
        i_ok = emit_refs.index("elb_edge.connect_ok")
        if i_ok > i_q:
            lo, hi = int_var_range("elb_edge.connect_ok", "latency_ms", (5, 30000))
            total = sum(delays_ms[i_q + 1 : i_ok + 1])
            if total < lo:
                delays_ms[i_ok] += int(lo - total)
            elif total > hi:
                reduc = min(delays_ms[i_ok] - 1, int(total - hi))
                if reduc > 0:
                    delays_ms[i_ok] -= reduc

    if "control_plane_api.api_request" in emit_refs:
        i_req = emit_refs.index("control_plane_api.api_request")
        i_resp = None
        resp_ref = None
        for cand in ["control_plane_api.api_response_ok", "control_plane_api.api_response_err"]:
            if cand in emit_refs:
                i_resp = emit_refs.index(cand)
                resp_ref = cand
                break
        if i_resp is not None and i_resp > i_req and resp_ref is not None:
            lo, hi = int_var_range(resp_ref, "latency_ms", (10, 120000))
            total = sum(delays_ms[i_req + 1 : i_resp + 1])
            if total < lo:
                delays_ms[i_resp] += int(lo - total)
            elif total > hi:
                reduc = min(delays_ms[i_resp] - 1, int(total - hi))
                if reduc > 0:
                    delays_ms[i_resp] -= reduc


def simulate_flow_instance(
    rows: List[Dict[str, Any]],
    state: str,
    flow: Dict[str, Any],
    start_dt: datetime,
    instance_idx: int,
    rate_mult_latency: Dict[str, float],
) -> None:
    flow_id = flow["id"]
    emit_refs: List[str] = list(flow["emit"])
    latency_pairs: List[List[float]] = list(flow.get("latency_ms", []))
    retry = flow.get("retry", {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []})
    trace_on = bool(SYSTEM["tracing"]["on"]) and bool(flow.get("trace", False))
    trace_id = det_hex(f"trace:{state}:{flow_id}:{instance_idx}:{fmt_ts(start_dt)}", 32) if trace_on else ""

    attempts = compute_attempts(retry, f"{flow_id}:{instance_idx}")
    backoff_pairs = list(retry.get("backoff_ms", []))
    per_retry_refs = list(retry.get("emit_per_retry", []))

    comp_host_map: Dict[str, str] = {}

    def default_host_for_ref(ref: str) -> str:
        cid = LOG_META[ref]["component_id"]
        if cid not in comp_host_map:
            comp_host_map[cid] = pick_host(cid, f"flowhost:{trace_id or flow_id}:{instance_idx}:{cid}")
        return comp_host_map[cid]

    def host_override_for_ref(ref: str, ctx: Dict[str, Any]) -> Optional[str]:
        cid = LOG_META[ref]["component_id"]
        if cid == "metadata_store" and "leader" in ctx:
            leader = str(ctx["leader"])
            if leader in COMP["metadata_store"]["hosts"]:
                comp_host_map["metadata_store"] = leader
                return leader
        if cid == "elb_edge":
            az = str(ctx.get("az", "") or "")
            h = az_to_elb_edge_host(az)
            if h and h in COMP["elb_edge"]["hosts"]:
                comp_host_map["elb_edge"] = h
                return h
        return None

    ctx_base = bind_flow_context(flow_id, state, start_dt, instance_idx)

    p50m = float(rate_mult_latency.get("p50", 1.0))
    p95m = float(rate_mult_latency.get("p95", 1.0))

    TASK_PROCESSED_MAX_MS = 600000

    t0 = start_dt
    for att in range(1, attempts + 1):
        ctx = dict(ctx_base)

        delays_ms: List[int] = []
        for j, pair in enumerate(latency_pairs):
            base_p50, base_p95 = float(pair[0]), float(pair[1])
            sp50 = base_p50 * p50m
            sp95 = base_p95 * p95m
            d = lognorm_ms(sp50, sp95, f"flowdelay:{state}:{flow_id}:{instance_idx}:a{att}:s{j}")
            if j < len(emit_refs) and emit_refs[j] == "elb_control_plane.task_processed":
                d = min(d, TASK_PROCESSED_MAX_MS)
            delays_ms.append(d)

        enforce_timing_domains_for_attempt(delays_ms, emit_refs)

        times: List[datetime] = []
        tt = t0
        for d in delays_ms:
            tt = tt + timedelta(milliseconds=int(d))
            times.append(tt)

        if "control_plane_api.api_request" in emit_refs:
            i_req = emit_refs.index("control_plane_api.api_request")
            i_resp = None
            resp_ref = None
            for cand in ["control_plane_api.api_response_ok", "control_plane_api.api_response_err"]:
                if cand in emit_refs:
                    i_resp = emit_refs.index(cand)
                    resp_ref = cand
                    break
            if i_resp is not None and i_resp > i_req and resp_ref is not None:
                total_ms = int(round((times[i_resp] - times[i_req]).total_seconds() * 1000.0))
                lo, hi = int_var_range(resp_ref, "latency_ms", (10, 120000))
                ctx["latency_ms"] = int(clamp(total_ms, lo, hi))

        if "metadata_store.write_commit" in emit_refs:
            i_wc = emit_refs.index("metadata_store.write_commit")
            if i_wc > 0:
                dur_ms = int(round((times[i_wc] - times[i_wc - 1]).total_seconds() * 1000.0))
                lo, hi = int_var_range("metadata_store.write_commit", "dur_ms", (5, 5000))
                ctx["dur_ms"] = int(clamp(dur_ms, lo, hi))

        if "elb_control_plane.task_processed" in emit_refs:
            i_tp = emit_refs.index("elb_control_plane.task_processed")
            lo, hi = int_var_range("elb_control_plane.task_processed", "latency_ms", (10, TASK_PROCESSED_MAX_MS))
            ctx["latency_ms"] = int(clamp(delays_ms[i_tp], lo, min(hi, TASK_PROCESSED_MAX_MS)))

        if "elb_edge.connect_ok" in emit_refs and "dns_service.dns_query" in emit_refs:
            i_q = emit_refs.index("dns_service.dns_query")
            i_ok = emit_refs.index("elb_edge.connect_ok")
            if i_ok > i_q:
                gap_ms = int(round((times[i_ok] - times[i_q]).total_seconds() * 1000.0))
                lo, hi = int_var_range("elb_edge.connect_ok", "latency_ms", (5, 30000))
                ctx["latency_ms"] = int(clamp(gap_ms, lo, hi))

        if flow_id.startswith("elb_register_targets"):
            ctx["action"] = "RegisterTargets"
            ctx["task"] = "register_targets"
        if flow_id.startswith("elb_shift_traffic"):
            ctx["task"] = "traffic_shift"

        if flow_id.startswith("elb_client_request"):
            if flow_id.endswith("_fail"):
                ctx["az"] = "use1-az1"
                ctx["reason"] = choose_from_list(["timeout", "no_route", "conn_refused"], f"failreason:{flow_id}:{instance_idx}")
            elif flow_id.endswith("_ok"):
                if state == "f":
                    ctx["az"] = choose_from_list(["use1-az2", "use1-az3"], f"okazf:{flow_id}:{instance_idx}")
                else:
                    ctx["az"] = choose_from_list(["use1-az1", "use1-az2", "use1-az3"], f"okazn:{flow_id}:{instance_idx}")

        if att >= 2:
            for rr_ref in per_retry_refs:
                hov = host_override_for_ref(rr_ref, ctx)
                emit_row(rows, t0, rr_ref, trace_id, ctx, f"retryonly:{flow_id}:{instance_idx}:a{att}", host_override=hov or default_host_for_ref(rr_ref))

        for j, ref in enumerate(emit_refs):
            if ref == "dns_service.record_update":
                if flow_id.startswith("elb_register_targets"):
                    ctx["action"] = "add"
                elif flow_id.startswith("elb_shift_traffic"):
                    ctx["action"] = "remove"
                else:
                    ctx["action"] = choose_from_list(["add", "remove"], f"dnsact:{flow_id}:{instance_idx}")
                ip_salt = f"dnsip:{flow_id}:{instance_idx}:{ctx.get('action', '')}"
                if ctx.get("action") == "remove":
                    ip_salt += ":az1"
                ctx["ip"] = gen_ip_in_cidr("54.0.0.0/8", ip_salt)
                ctx["ttl_s"] = gen_int(30, 300, f"ttl:{flow_id}:{instance_idx}")
                ctx["name"] = gen_dns_name_from_lb(ctx.get("lb", gen_lb_name(f"lb:{flow_id}:{instance_idx}")))

            if ref == "elb_control_plane.enqueue_task":
                m = minute_of(times[j])
                depth, _, _, _ = queue_shape(m)
                ctx["qdepth"] = int(clamp(depth + gen_int(-500, 500, f"qdepthj:{flow_id}:{instance_idx}:{j}"), 0, 250000))

            hov = host_override_for_ref(ref, ctx)
            emit_row(
                rows,
                times[j],
                ref,
                trace_id,
                ctx,
                f"flow:{flow_id}:{instance_idx}:a{att}:s{j}",
                host_override=hov or default_host_for_ref(ref),
            )

        if att < attempts:
            if backoff_pairs:
                b_idx = min(att - 1, len(backoff_pairs) - 1)
                bp50, bp95 = backoff_pairs[b_idx]
                bms = lognorm_ms(float(bp50), float(bp95), f"backoff:{flow_id}:{instance_idx}:a{att}")
            else:
                bms = int(50 + 150 * unit_float(f"backoffdf:{flow_id}:{instance_idx}:a{att}"))
            t0 = times[-1] + timedelta(milliseconds=bms)


def emit_background(
    rows: List[Dict[str, Any]],
    state: str,
    interval_start: datetime,
    interval_end: datetime,
    rate_bg_mult: Dict[str, float],
) -> None:
    dur_min = (interval_end - interval_start).total_seconds() / 60.0
    for cid in sorted(COMP.keys()):
        beh = COMP[cid].get("beh", {}).get(state, {})
        emits = list(beh.get("emit", []))
        for e in emits:
            log_id = e["id"]
            per_min = float(e["per_min"])
            scope = e.get("scope", "per_host")
            ref = f"{cid}.{log_id}"
            mult = float(rate_bg_mult.get(ref, 1.0)) if state == "f" else 1.0
            eff = per_min * mult
            if eff <= 0:
                continue

            if scope == "global":
                key = f"bg:{state}:{ref}:global"
                cnt = alloc_count(eff * dur_min, key)
                times = schedule_evenly(interval_start, interval_end, cnt, key)
                for i, t in enumerate(times):
                    ctx: Dict[str, Any] = {}
                    m = minute_of(t)
                    if ref == "dc_power_az1.ups_metric":
                        on_batt, chg = ups_shape(m)
                        ctx["on_battery"] = on_batt
                        ctx["charge_pct"] = chg
                        ctx["az"] = "use1-az1"
                    emit_row(rows, t, ref, "", ctx, f"{key}:i{i}", host_override=pick_host(cid, f"{key}:i{i}"))

            else:
                for host in COMP[cid].get("hosts", []):
                    key = f"bg:{state}:{ref}:host:{host}"
                    cnt = alloc_count(eff * dur_min, key)
                    times = schedule_evenly(interval_start, interval_end, cnt, key)
                    for i, t in enumerate(times):
                        ctx = {}
                        m = minute_of(t)
                        if ref == "metadata_store.replication_health":
                            leader = active_meta_leader(m)
                            ctx["leader"] = leader
                            if m < 25:
                                role = "primary" if host == "meta-az1" else "replica-rw"
                                lag = int(clamp(10 + 80 * unit_float(f"lag:{host}:{i}"), 0, 60000))
                            elif m < 42:
                                role = "replica-ro"
                                lag = int(clamp(800 + 4200 * unit_float(f"lagf1:{host}:{i}") + 400 * math.sin(m) ** 2, 0, 60000))
                            else:
                                role = "primary" if host == "meta-az2" else "replica-rw"
                                lag = int(clamp(100 + 1200 * unit_float(f"lagf2:{host}:{i}") + 200 * math.sin(m / 2.0) ** 2, 0, 60000))
                            ctx["role"] = role
                            ctx["lag_ms"] = lag

                        if ref == "elb_control_plane.queue_metric":
                            depth, oldest, rin, rout = queue_shape(m)
                            ctx["depth"] = depth
                            ctx["oldest_age_s"] = oldest
                            ctx["rate_in"] = rin
                            ctx["rate_out"] = rout

                        if ref == "elb_control_plane.bug_scale_trigger":
                            ctx["lb"] = gen_lb_name(f"buglb:{state}:{host}:{fmt_ts(t)}")
                            ctx["reason"] = choose_from_list(["recovered_state_mismatch", "missing_health_snapshot"], f"bugreason:{host}:{i}")
                            ctx["desired_size"] = choose_from_list(["large", "xlarge"], f"bugsize:{host}:{i}")

                        if ref == "ebs_recovery.recovery_progress":
                            backlog, pr, comp = ebs_recovery_shape(m)
                            ctx["backlog"] = int(clamp(backlog + gen_int(-250, 250, f"brj:{host}:{i}"), 0, 40000))
                            ctx["processing_rate_vpm"] = int(clamp(pr + gen_int(-60, 60, f"prj:{host}:{i}"), 10, 4000))
                            ctx["completed"] = int(clamp(comp + gen_int(-400, 400, f"ccj:{host}:{i}"), 0, 40000))

                        if ref == "ebs_recovery.volume_state_impaired":
                            ctx["vol_id"] = gen_vol_id(f"imp:{host}:{fmt_ts(t)}")
                            ctx["reason"] = choose_from_list(["in_flight_writes", "journal_replay"], f"impreason:{host}:{i}")

                        if ref == "ebs_recovery.volume_recovered":
                            ctx["vol_id"] = gen_vol_id(f"rec:{host}:{fmt_ts(t)}")

                        emit_row(rows, t, ref, "", ctx, f"{key}:i{i}", host_override=host)


def emit_one_shots(rows: List[Dict[str, Any]]) -> None:
    for ev in sorted(FAIL_EVENTS, key=lambda e: e["at_min"]):
        at_min = int(ev["at_min"])
        base_t = BASE_TIME + timedelta(minutes=at_min)
        end_t = base_t + timedelta(seconds=59.900)
        for shot in ev.get("one_shots", []):
            ref = shot["ref"]
            cnt = int(shot["count"])
            allowed_hosts = list(shot.get("hosts", []))
            cid = LOG_META[ref]["component_id"]
            for i in range(cnt):
                # Non-negative jitter to ensure one-shots never precede the event minute.
                j = unit_float(f"oneshot:{at_min}:{ref}:{i}") * 8.0
                t = base_t + timedelta(seconds=j + 0.2 * i)
                if t < base_t:
                    t = base_t
                if t > end_t:
                    t = end_t - timedelta(milliseconds=(i % 50))
                ctx: Dict[str, Any] = {}
                m = minute_of(t)

                if ref == "dc_power_az1.utility_event":
                    ctx["state"] = "loss"
                    ctx["az"] = "use1-az1"
                elif ref == "dc_power_az1.gen_sync_fail":
                    ctx["az"] = "use1-az1"
                    ctx["voltage_v"] = float(clamp(140.0 + 220.0 * unit_float(f"v:{at_min}:{i}"), 0.0, 520.0))
                    ctx["freq_hz"] = float(clamp(35.0 + 20.0 * unit_float(f"f:{at_min}:{i}"), 0.0, 70.0))
                    ctx["voltage_v"] = round(ctx["voltage_v"], 1)
                    ctx["freq_hz"] = round(ctx["freq_hz"], 1)
                elif ref == "dc_power_az1.rack_power_loss":
                    ctx["az"] = "use1-az1"
                    ctx["racks_offline"] = int(clamp(600 + 200 * unit_float(f"racks:{at_min}:{i}"), 10, 900))
                elif ref == "metadata_store.leader_change":
                    ctx["leader"] = allowed_hosts[i % len(allowed_hosts)] if allowed_hosts else active_meta_leader(m)
                    if at_min == 25:
                        ctx["old_role"] = "replica-rw"
                        ctx["new_role"] = "replica-ro"
                    elif at_min == 42:
                        ctx["old_role"] = "replica-ro"
                        ctx["new_role"] = "primary"
                    else:
                        ctx["old_role"] = "replica-ro"
                        ctx["new_role"] = "replica-rw"
                elif ref == "elb_control_plane.operator_action":
                    ctx["action"] = "apply_rate_limit"
                    ctx["detail"] = "region-main"

                host = allowed_hosts[i % len(allowed_hosts)] if allowed_hosts else pick_host(cid, f"oneshot:{at_min}:{ref}:{i}")
                emit_row(rows, t, ref, "", ctx, f"oneshot:{at_min}:{ref}:{i}", host_override=host)


# ----------------------------
# Main simulation
# ----------------------------

def simulate() -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    n0 = BASE_TIME + timedelta(minutes=SCENARIO["time"]["phases"]["n"]["start_min"])
    n1 = BASE_TIME + timedelta(minutes=SCENARIO["time"]["phases"]["n"]["end_min"])
    emit_background(rows, "n", n0, n1, rate_bg_mult={})

    dur_min_n = (n1 - n0).total_seconds() / 60.0
    for flow in sorted(SYSTEM["flows"]["n"], key=lambda f: f["id"]):
        exp_instances = float(flow["rpm"]) * dur_min_n
        cnt = alloc_count(exp_instances, f"flowcnt:n:{flow['id']}")
        starts = schedule_evenly(n0, n1, cnt, f"flowstart:n:{flow['id']}")
        for idx, st in enumerate(starts):
            simulate_flow_instance(rows, "n", flow, st, idx, rate_mult_latency={"p50": 1.0, "p95": 1.0})

    for interval in FAIL_INTERVALS:
        sdt = BASE_TIME + timedelta(minutes=interval.start_min)
        edt = BASE_TIME + timedelta(minutes=interval.end_min)
        emit_background(rows, "f", sdt, edt, rate_bg_mult=interval.rate_bg)

        dur_min = (edt - sdt).total_seconds() / 60.0
        for flow in sorted(SYSTEM["flows"]["f"], key=lambda f: f["id"]):
            mult = float(interval.rate_flow.get(flow["id"], 1.0))
            if mult <= 0:
                continue
            exp_instances = float(flow["rpm"]) * mult * dur_min
            cnt = alloc_count(exp_instances, f"flowcnt:f:{interval.start_min}:{flow['id']}")
            starts = schedule_evenly(sdt, edt, cnt, f"flowstart:f:{interval.start_min}:{flow['id']}")
            lm = interval.lat_flow.get(flow["id"], {"p50": 1.0, "p95": 1.0})
            for idx, st in enumerate(starts):
                simulate_flow_instance(rows, "f", flow, st, idx, rate_mult_latency=lm)

    emit_one_shots(rows)

    df = pd.DataFrame(rows)
    df.sort_values("timestamp_dt", inplace=True, kind="mergesort")
    df["timestamp"] = df["timestamp_dt"].apply(fmt_ts)
    df.drop(columns=["timestamp_dt"], inplace=True)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    assert list(df.columns) == ["timestamp", "level", "message", "trace_id", "service", "host"]
    assert df["timestamp"].is_monotonic_increasing

    nrows = len(df)
    if not (20000 <= nrows <= 100000):
        raise RuntimeError(f"Row count {nrows} out of bounds [20000, 100000].")

    return df


def main() -> None:
    df = simulate()
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
