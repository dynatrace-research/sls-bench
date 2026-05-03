import math
import hashlib
import uuid
import ipaddress
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# ----------------------------
# Determinism seeds
# ----------------------------
SEED = 0
random.seed(SEED)
np.random.seed(SEED)

# ----------------------------
# Embedded normalized semantics
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "public_ip_programming_pipeline"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "resource_manager",
            "svc": "rm",
            "hosts": ["rm-1", "rm-2"],
            "logs": {
                "rm_create_request": {
                    "lvl": "INFO",
                    "msg": "create {resource_type} request received resource_id={resource_id} zone={zone} public_ip={public_ip} req_id={req_id}",
                    "vars": {
                        "resource_type": {"k": "ch", "v": ["gce_instance", "cloud_vpn", "net_lb"]},
                        "resource_id": {"k": "hex", "v": 12},
                        "zone": {"k": "ch", "v": ["us-west1-a", "us-west1-b", "us-west1-c"]},
                        "public_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "rm_net_program_submit": {
                    "lvl": "INFO",
                    "msg": "submitting public address programming resource_id={resource_id} req_id={req_id}",
                    "vars": {
                        "resource_id": {"k": "hex", "v": 12},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "rm_create_ready": {
                    "lvl": "INFO",
                    "msg": "resource network ready resource_id={resource_id} public_ip={public_ip} elapsed_ms={elapsed_ms} req_id={req_id}",
                    "vars": {
                        "resource_id": {"k": "hex", "v": 12},
                        "public_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "elapsed_ms": {"k": "i", "v": [500, 30000]},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "rm_create_timeout": {
                    "lvl": "ERROR",
                    "msg": "resource network not ready (timeout) resource_id={resource_id} waited_ms={waited_ms} req_id={req_id}",
                    "vars": {
                        "resource_id": {"k": "hex", "v": 12},
                        "waited_ms": {"k": "i", "v": [60000, 180000]},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "rm_pending_network": {
                    "lvl": "INFO",
                    "msg": "pending network programming requests={pending} oldest_age_s={oldest_age_s}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "pending": {"k": "i", "v": [0, 50]},
                            "oldest_age_s": {"k": "i", "v": [0, 15]},
                        },
                        "f": {
                            "pending": {"k": "i", "v": [500, 8000]},
                            "oldest_age_s": {"k": "i", "v": [60, 3600]},
                        },
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "rm_pending_network", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "rm_pending_network", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "net_control_plane",
            "svc": "ncp",
            "hosts": ["ncp-1", "ncp-2"],
            "logs": {
                "ncp_config_batch_received": {
                    "lvl": "WARN",
                    "msg": "received config update batch config={config_name} updates={updates} change_id={change_id}",
                    "vars": {
                        "config_name": {"k": "ch", "v": ["rare_l2_extension"]},
                        "updates": {"k": "i", "v": [1000, 8000]},
                        "change_id": {"k": "hex", "v": 10},
                    },
                },
                "ncp_enqueue_update": {
                    "lvl": "INFO",
                    "msg": "enqueued address update config={config_name} resource_id={resource_id} queue_depth={queue_depth} job_id={job_id}",
                    "vars": {
                        "resource_id": {"k": "hex", "v": 12},
                        "job_id": {"k": "str", "v": "ncp-job-[1-9]"},
                    },
                    "state_vars": {
                        "n": {
                            "config_name": {"k": "ch", "v": ["standard_ipv4", "standard_ipv6"]},
                            "queue_depth": {"k": "i", "v": [0, 200]},
                        },
                        "f": {
                            "config_name": {"k": "ch", "v": ["rare_l2_extension"]},
                            "queue_depth": {"k": "i", "v": [500, 15000]},
                        },
                    },
                },
                "ncp_apply_success": {
                    "lvl": "INFO",
                    "msg": "applied address update to dataplane config={config_name} batch_id={batch_id} updated={updated} queue_depth={queue_depth}",
                    "vars": {
                        "batch_id": {"k": "hex", "v": 8},
                        "updated": {"k": "i", "v": [1, 300]},
                    },
                    "state_vars": {
                        "n": {
                            "config_name": {"k": "ch", "v": ["standard_ipv4", "standard_ipv6"]},
                            "queue_depth": {"k": "i", "v": [0, 200]},
                        },
                        "f": {
                            "config_name": {"k": "ch", "v": ["rare_l2_extension"]},
                            "queue_depth": {"k": "i", "v": [200, 15000]},
                        },
                    },
                },
                "ncp_worker_restart": {
                    "lvl": "WARN",
                    "msg": "worker restart requested zone={zone} reason={reason}",
                    "vars": {
                        "zone": {"k": "ch", "v": ["us-west1-a", "us-west1-b", "us-west1-c"]},
                        "reason": {"k": "ch", "v": ["stuck_canary", "backlog_drain"]},
                    },
                },
                "ncp_queue_status_normal": {
                    "lvl": "INFO",
                    "msg": "queue status depth={queue_depth} oldest_s={oldest_s} canary_state={canary_state}",
                    "vars": {
                        "queue_depth": {"k": "i", "v": [0, 200]},
                        "oldest_s": {"k": "i", "v": [0, 30]},
                        "canary_state": {"k": "ch", "v": ["passing"]},
                    },
                },
                "ncp_queue_status_timeout": {
                    "lvl": "WARN",
                    "msg": "queue status depth={queue_depth} oldest_s={oldest_s} canary_state={canary_state}",
                    "vars": {
                        "queue_depth": {"k": "i", "v": [2000, 15000]},
                        "oldest_s": {"k": "i", "v": [300, 3600]},
                        "canary_state": {"k": "ch", "v": ["timing_out"]},
                    },
                },
                "ncp_queue_status_draining": {
                    "lvl": "INFO",
                    "msg": "queue status depth={queue_depth} oldest_s={oldest_s} canary_state={canary_state}",
                    "vars": {
                        "queue_depth": {"k": "i", "v": [500, 9000]},
                        "oldest_s": {"k": "i", "v": [60, 2400]},
                        "canary_state": {"k": "ch", "v": ["draining"]},
                    },
                },
                "ncp_queue_snapshot_high": {
                    "lvl": "WARN",
                    "msg": "queue snapshot depth={queue_depth} oldest_s={oldest_s}",
                    "vars": {
                        "queue_depth": {"k": "i", "v": [9000, 15000]},
                        "oldest_s": {"k": "i", "v": [1200, 3600]},
                    },
                },
                "ncp_queue_snapshot_draining": {
                    "lvl": "INFO",
                    "msg": "queue snapshot depth={queue_depth} oldest_s={oldest_s}",
                    "vars": {
                        "queue_depth": {"k": "i", "v": [2000, 8000]},
                        "oldest_s": {"k": "i", "v": [300, 2400]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "ncp_queue_status_normal", "per_min": 1.0, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "ncp_queue_status_timeout", "per_min": 1.0, "scope": "global"},
                        {"id": "ncp_queue_status_draining", "per_min": 1.0, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "canary_tester",
            "svc": "canary",
            "hosts": ["canary-1"],
            "logs": {
                "canary_pass": {
                    "lvl": "INFO",
                    "msg": "canary succeeded config={config_name} elapsed_ms={elapsed_ms} canary_id={canary_id}",
                    "vars": {"elapsed_ms": {"k": "i", "v": [100, 12000]}, "canary_id": {"k": "hex", "v": 12}},
                    "state_vars": {
                        "n": {"config_name": {"k": "ch", "v": ["standard_ipv4", "standard_ipv6"]}},
                        "f": {"config_name": {"k": "ch", "v": ["rare_l2_extension"]}},
                    },
                },
                "canary_timeout": {
                    "lvl": "ERROR",
                    "msg": "canary timed out config={config_name} timeout_ms={timeout_ms} elapsed_ms={elapsed_ms} canary_id={canary_id}",
                    "vars": {
                        "config_name": {"k": "ch", "v": ["rare_l2_extension"]},
                        "timeout_ms": {"k": "i", "v": [30000, 30000]},
                        "elapsed_ms": {"k": "i", "v": [30000, 45000]},
                        "canary_id": {"k": "hex", "v": 12},
                    },
                },
            },
        },
        {
            "id": "l2lb_edge",
            "svc": "l2lb",
            "hosts": ["l2lb-1", "l2lb-2", "l2lb-3"],
            "logs": {
                "l2lb_program_apply": {
                    "lvl": "INFO",
                    "msg": "programmed public ip mapping public_ip={public_ip} resource_id={resource_id} zone={zone} version={version}",
                    "vars": {
                        "public_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "resource_id": {"k": "hex", "v": 12},
                        "zone": {"k": "ch", "v": ["us-west1-a", "us-west1-b", "us-west1-c"]},
                        "version": {"k": "ch", "v": ["r20170130"]},
                    },
                },
                "l2lb_drop_nomap": {
                    "lvl": "WARN",
                    "msg": "dropped inbound packet public_ip={public_ip} reason={reason}",
                    "vars": {
                        "public_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "reason": {"k": "ch", "v": ["no_mapping", "pending_programming"]},
                    },
                },
                "l2lb_stats": {
                    "lvl": "INFO",
                    "msg": "dataplane stats packets_in={packets_in} drops_nomap={drops_nomap}",
                    "vars": {},
                    "state_vars": {
                        "n": {"packets_in": {"k": "i", "v": [10000, 50000]}, "drops_nomap": {"k": "i", "v": [0, 20]}},
                        "f": {"packets_in": {"k": "i", "v": [8000, 45000]}, "drops_nomap": {"k": "i", "v": [300, 7000]}},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "l2lb_stats", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "l2lb_stats", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "health_checker",
            "svc": "hc",
            "hosts": ["hc-1", "hc-2"],
            "logs": {
                "hc_ok": {
                    "lvl": "INFO",
                    "msg": "health check ok target={target} public_ip={public_ip} rtt_ms={rtt_ms}",
                    "vars": {
                        "target": {"k": "ch", "v": ["net_lb", "gce_instance"]},
                        "public_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "rtt_ms": {"k": "i", "v": [5, 80]},
                    },
                },
                "hc_fail": {
                    "lvl": "WARN",
                    "msg": "health check failed target={target} public_ip={public_ip} error={error} waited_ms={waited_ms}",
                    "vars": {
                        "target": {"k": "ch", "v": ["net_lb", "gce_instance"]},
                        "public_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "error": {"k": "ch", "v": ["timeout", "no_route", "connection_refused"]},
                        "waited_ms": {"k": "i", "v": [1000, 6000]},
                    },
                },
            },
        },
        {
            "id": "guest_agent",
            "svc": "guest-agent",
            "hosts": ["guest-1", "guest-2", "guest-3"],
            "logs": {
                "ga_egress_ok": {
                    "lvl": "INFO",
                    "msg": "egress connection ok dest={dest} public_ip={public_ip} latency_ms={latency_ms}",
                    "vars": {
                        "dest": {"k": "str", "v": "external endpoint"},
                        "public_ip": {"k": "ip", "v": "203.0.113.0/24"},
                        "latency_ms": {"k": "i", "v": [20, 300]},
                    },
                },
                "ga_egress_fail": {
                    "lvl": "WARN",
                    "msg": "egress connection failed dest={dest} error={error} public_ip={public_ip}",
                    "vars": {
                        "dest": {"k": "str", "v": "external endpoint"},
                        "error": {"k": "ch", "v": ["no_route", "timeout"]},
                        "public_ip": {"k": "ip", "v": "203.0.113.0/24"},
                    },
                },
            },
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "create_resource_ok",
                    "rpm": 120.0,
                    "emit": [
                        "resource_manager.rm_create_request",
                        "resource_manager.rm_net_program_submit",
                        "net_control_plane.ncp_enqueue_update",
                        "canary_tester.canary_pass",
                        "l2lb_edge.l2lb_program_apply",
                        "net_control_plane.ncp_apply_success",
                        "resource_manager.rm_create_ready",
                    ],
                    "latency_ms": [[2, 5], [10, 30], [20, 80], [200, 1500], [20, 120], [20, 120], [5, 20]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "health_check_ok",
                    "rpm": 250.0,
                    "emit": ["health_checker.hc_ok"],
                    "latency_ms": [[10, 60]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "egress_probe_ok",
                    "rpm": 30.0,
                    "emit": ["guest_agent.ga_egress_ok"],
                    "latency_ms": [[30, 200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "create_resource_blocked",
                    "rpm": 120.0,
                    "emit": [
                        "resource_manager.rm_create_request",
                        "resource_manager.rm_net_program_submit",
                        "net_control_plane.ncp_enqueue_update",
                        "canary_tester.canary_timeout",
                        "resource_manager.rm_create_timeout",
                    ],
                    "latency_ms": [[2, 5], [10, 30], [50, 200], [30000, 45000], [60000, 180000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "create_resource_recovered",
                    "rpm": 120.0,
                    "emit": [
                        "resource_manager.rm_create_request",
                        "resource_manager.rm_net_program_submit",
                        "net_control_plane.ncp_enqueue_update",
                        "canary_tester.canary_pass",
                        "l2lb_edge.l2lb_program_apply",
                        "net_control_plane.ncp_apply_success",
                        "resource_manager.rm_create_ready",
                    ],
                    "latency_ms": [[2, 5], [20, 60], [50, 200], [500, 4000], [50, 250], [100, 500], [20, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "health_check_fail",
                    "rpm": 250.0,
                    "emit": ["l2lb_edge.l2lb_drop_nomap", "health_checker.hc_fail"],
                    "latency_ms": [[1, 5], [1000, 6000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "health_check_ok_in_failure",
                    "rpm": 250.0,
                    "emit": ["health_checker.hc_ok"],
                    "latency_ms": [[10, 80]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "inbound_public_drop",
                    "rpm": 80.0,
                    "emit": ["l2lb_edge.l2lb_drop_nomap"],
                    "latency_ms": [[1, 5]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "egress_probe_fail",
                    "rpm": 30.0,
                    "emit": ["guest_agent.ga_egress_fail"],
                    "latency_ms": [[200, 5000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "egress_probe_ok_in_failure",
                    "rpm": 30.0,
                    "emit": ["guest_agent.ga_egress_ok"],
                    "latency_ms": [[30, 400]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "canary_timeout_blocks_public_ip_updates",
        "time": {"total_minutes": 45, "phases": {"n": {"start_min": 0, "end_min": 18}, "f": {"start_min": 18, "end_min": 45}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 18,
                        "rate_multipliers": {
                            "create_resource_recovered": 0.0,
                            "health_check_ok_in_failure": 0.0,
                            "egress_probe_ok_in_failure": 0.0,
                            "net_control_plane.ncp_queue_status_timeout": 1.0,
                            "net_control_plane.ncp_queue_status_draining": 0.0,
                        },
                        "latency_multipliers": {"create_resource_blocked": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [
                            {"ref": "net_control_plane.ncp_config_batch_received", "count": 1, "hosts": ["ncp-1"]},
                            {"ref": "net_control_plane.ncp_queue_snapshot_high", "count": 1, "hosts": ["ncp-1"]},
                        ],
                    },
                    {
                        "order": 2,
                        "at_min": 26,
                        "rate_multipliers": {
                            "create_resource_blocked": 0.7,
                            "create_resource_recovered": 0.3,
                            "health_check_fail": 0.7,
                            "health_check_ok_in_failure": 0.3,
                            "inbound_public_drop": 0.7,
                            "egress_probe_fail": 0.7,
                            "egress_probe_ok_in_failure": 0.3,
                            "net_control_plane.ncp_queue_status_timeout": 0.7,
                            "net_control_plane.ncp_queue_status_draining": 0.3,
                        },
                        "latency_multipliers": {"create_resource_recovered": {"p50": 1.8, "p95": 1.5}},
                        "one_shots": [{"ref": "net_control_plane.ncp_worker_restart", "count": 1, "hosts": ["ncp-1"]}],
                    },
                    {
                        "order": 3,
                        "at_min": 38,
                        "rate_multipliers": {
                            "create_resource_blocked": 0.4,
                            "create_resource_recovered": 0.6,
                            "health_check_fail": 0.4,
                            "health_check_ok_in_failure": 0.6,
                            "inbound_public_drop": 0.4,
                            "egress_probe_fail": 0.4,
                            "egress_probe_ok_in_failure": 0.6,
                            "net_control_plane.ncp_queue_status_timeout": 0.4,
                            "net_control_plane.ncp_queue_status_draining": 0.6,
                        },
                        "latency_multipliers": {"create_resource_recovered": {"p50": 1.3, "p95": 1.2}},
                        "one_shots": [
                            {"ref": "net_control_plane.ncp_worker_restart", "count": 2, "hosts": ["ncp-1", "ncp-2"]},
                            {"ref": "net_control_plane.ncp_queue_snapshot_draining", "count": 1, "hosts": ["ncp-2"]},
                        ],
                    },
                ]
            }
        },
    }
}

# ----------------------------
# Deterministic helpers
# ----------------------------

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def md5_bytes(s: str) -> bytes:
    return hashlib.md5(s.encode("utf-8")).digest()


def md5_int(s: str) -> int:
    return int.from_bytes(md5_bytes(s), "big", signed=False)


def u01(s: str) -> float:
    return (md5_int(s) % 10_000_000) / 10_000_000.0


def iso_utc_ms(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def det_choice(values: List[Any], key: str) -> Any:
    if not values:
        return None
    return values[md5_int(key) % len(values)]


def det_int(lo: int, hi: int, key: str) -> int:
    if hi < lo:
        lo, hi = hi, lo
    span = hi - lo + 1
    return lo + (md5_int(key) % span)


def det_hex(length: int, key: str) -> str:
    h = hashlib.md5((key + "|hex").encode("utf-8")).hexdigest()
    while len(h) < length:
        h += hashlib.md5((key + "|" + h).encode("utf-8")).hexdigest()
    return h[:length]


def det_uuid4(key: str) -> str:
    b = bytearray(md5_bytes(key + "|uuid"))
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(b)))


def det_ip(cidr: str, key: str) -> str:
    net = ipaddress.ip_network(cidr, strict=False)
    n = net.num_addresses
    off = md5_int(key + "|ip") % n
    ip = net.network_address + off
    return str(ip)


def det_str_hint(hint: str, key: str) -> str:
    if hint == "ncp-job-[1-9]":
        d = 1 + (md5_int(key + "|job") % 9)
        return f"ncp-job-{d}"
    return hint


def sample_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    if k == "ch":
        return det_choice(list(v), key)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return det_int(lo, hi, key)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return lo + (hi - lo) * u01(key + "|f")
    if k == "hex":
        return det_hex(int(v), key)
    if k == "uuid":
        return det_uuid4(key)
    if k == "ip":
        return det_ip(str(v), key)
    if k == "str":
        return det_str_hint(str(v), key)
    return str(v)


def bounded_latency_ms(p50: float, p95: float, key: str, cap_max: Optional[int] = None) -> int:
    a = float(p50)
    b = float(p95)
    if b < a:
        a, b = b, a
    t = u01(key + "|lat")
    val = a + t * (b - a)
    if cap_max is not None:
        val = min(val, float(cap_max))
    return max(1, int(round(val)))


# ----------------------------
# Indices
# ----------------------------

COMP_BY_ID: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}


def get_template(ref: str) -> Tuple[str, str, Dict[str, Any]]:
    comp_id, log_id = ref.split(".", 1)
    comp = COMP_BY_ID[comp_id]
    tmpl = comp["logs"][log_id]
    return comp_id, log_id, tmpl


# ----------------------------
# Scenario control tables
# ----------------------------

PH_N = SCENARIO["scenario"]["time"]["phases"]["n"]
PH_F = SCENARIO["scenario"]["time"]["phases"]["f"]
FAIL_EVENTS = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))


def build_failure_controls() -> Dict[int, Dict[str, Any]]:
    rate_map: Dict[str, float] = {}
    lat_map: Dict[str, Dict[str, float]] = {}
    controls_by_minute: Dict[int, Dict[str, Any]] = {}
    events_by_min = {e["at_min"]: e for e in FAIL_EVENTS}

    for m in range(PH_F["start_min"], PH_F["end_min"]):
        if m in events_by_min:
            e = events_by_min[m]
            for k, mult in e.get("rate_multipliers", {}).items():
                rate_map[k] = float(mult)
            for fid, mults in e.get("latency_multipliers", {}).items():
                lat_map[fid] = {"p50": float(mults["p50"]), "p95": float(mults["p95"])}
        controls_by_minute[m] = {"rate": dict(rate_map), "lat": dict(lat_map)}
    return controls_by_minute


FAIL_CONTROLS_BY_MIN = build_failure_controls()


def get_rate_mult(state: str, minute: int, source_key: str) -> float:
    if state != "f":
        return 1.0
    c = FAIL_CONTROLS_BY_MIN.get(minute)
    if not c:
        return 1.0
    return float(c["rate"].get(source_key, 1.0))


def get_latency_mult(state: str, minute: int, flow_id: str) -> Tuple[float, float]:
    if state != "f":
        return 1.0, 1.0
    c = FAIL_CONTROLS_BY_MIN.get(minute)
    if not c:
        return 1.0, 1.0
    m = c["lat"].get(flow_id)
    if not m:
        return 1.0, 1.0
    return float(m.get("p50", 1.0)), float(m.get("p95", 1.0))


# ----------------------------
# Deterministic count allocation
# ----------------------------

class CarryAllocator:
    def __init__(self):
        self.carry: Dict[str, float] = {}

    def alloc(self, key: str, expected: float) -> int:
        v = self.carry.get(key, 0.0) + float(expected)
        n = int(math.floor(v + 1e-12))
        self.carry[key] = v - n
        return n


ALLOC = CarryAllocator()

# ----------------------------
# Scheduling helpers
# ----------------------------

def schedule_within_minute(minute_start: datetime, n: int, key_prefix: str) -> List[datetime]:
    if n <= 0:
        return []
    out: List[datetime] = []
    step = 60.0 / float(n)
    amp = min(0.12, 0.25 * step)  # seconds
    for i in range(n):
        base_off = (i + 0.5) * step
        jitter = (u01(f"{key_prefix}:j:{i}") - 0.5) * 2.0 * amp
        off = base_off + jitter
        if off < 0.0:
            off = 0.0
        if off >= 60.0:
            off = 59.999
        out.append(minute_start + timedelta(seconds=off))
    return out


def choose_host(comp_id: str, key: str) -> str:
    hosts = COMP_BY_ID[comp_id].get("hosts", []) or []
    if not hosts:
        return ""
    return det_choice(hosts, key + "|host")


def choose_host_round_robin(comp_id: str, idx: int) -> str:
    hosts = COMP_BY_ID[comp_id].get("hosts", []) or []
    if not hosts:
        return ""
    return hosts[idx % len(hosts)]


# ----------------------------
# Emission/rendering
# ----------------------------

def build_template_domains(tmpl: Dict[str, Any], state: str) -> Dict[str, Dict[str, Any]]:
    doms = dict(tmpl.get("vars", {}) or {})
    sv = tmpl.get("state_vars", None)
    if sv and state in sv:
        for k, dom in sv[state].items():
            doms[k] = dom
    return doms


def render_message(tmpl: Dict[str, Any], vals: Dict[str, Any]) -> str:
    return tmpl["msg"].format(**vals)


def get_domain_cap_max_for_var(tmpl: Dict[str, Any], var: str, state: str) -> Optional[int]:
    doms = build_template_domains(tmpl, state)
    dom = doms.get(var)
    if not dom:
        return None
    if dom.get("k") == "i":
        return int(dom["v"][1])
    return None


def emit_row(rows: List[Dict[str, Any]], ts: datetime, lvl: str, msg: str, trace_id: str, svc: str, host: str) -> None:
    rows.append(
        {
            "timestamp": iso_utc_ms(ts),
            "level": lvl,
            "message": msg,
            "trace_id": trace_id,
            "service": svc or "",
            "host": host or "",
        }
    )


# ----------------------------
# Flow simulation
# ----------------------------

FLOW_BY_STATE_ID: Dict[Tuple[str, str], Dict[str, Any]] = {}
for st in ("n", "f"):
    for f in SYSTEM["flows"][st]["req"]:
        FLOW_BY_STATE_ID[(st, f["id"])] = f

INSTANCE_SEQ: Dict[str, int] = {}


def simulate_flow_instance(rows: List[Dict[str, Any]], state: str, minute: int, flow_id: str, start_ts: datetime) -> None:
    flow = FLOW_BY_STATE_ID[(state, flow_id)]
    emit_refs: List[str] = flow["emit"]
    lat_pairs: List[List[float]] = flow["latency_ms"]

    seq = INSTANCE_SEQ.get(flow_id, 0)
    INSTANCE_SEQ[flow_id] = seq + 1

    inst_key = f"{state}:{minute}:{flow_id}:{seq}"
    p50m, p95m = get_latency_mult(state, minute, flow_id)

    comp_host: Dict[str, str] = {}
    ctx: Dict[str, Any] = {}

    delays_ms: List[int] = []
    p50s: List[int] = []
    p95s: List[int] = []

    for j, ref in enumerate(emit_refs):
        _, log_id, tmpl = get_template(ref)
        p50, p95 = float(lat_pairs[j][0]) * p50m, float(lat_pairs[j][1]) * p95m

        cap = None
        if log_id == "hc_ok":
            cap = get_domain_cap_max_for_var(tmpl, "rtt_ms", state)
        elif log_id == "hc_fail":
            cap = get_domain_cap_max_for_var(tmpl, "waited_ms", state)
        elif log_id == "ga_egress_ok":
            cap = get_domain_cap_max_for_var(tmpl, "latency_ms", state)
        elif log_id == "canary_timeout":
            cap = get_domain_cap_max_for_var(tmpl, "elapsed_ms", state)
        elif log_id == "canary_pass":
            cap = get_domain_cap_max_for_var(tmpl, "elapsed_ms", state)
        elif log_id == "rm_create_timeout":
            # NOTE: This cap is for the final-step delay; the logged waited_ms for rm_create_timeout is handled
            # separately as total time since rm_create_request.
            cap = get_domain_cap_max_for_var(tmpl, "waited_ms", state)

        d = bounded_latency_ms(p50, p95, f"{inst_key}:lat:{j}", cap_max=cap)
        delays_ms.append(d)
        p50s.append(max(1, int(round(p50))))
        p95s.append(max(1, int(round(p95 if cap is None else min(p95, cap)))))

    # If ends with rm_create_ready, keep elapsed_ms (since rm_create_request) within its domain by shaping per-step delays.
    if emit_refs and emit_refs[-1].endswith(".rm_create_ready"):
        _, _, last_tmpl = get_template(emit_refs[-1])
        doms_last = build_template_domains(last_tmpl, state)
        dom_elapsed = doms_last.get("elapsed_ms")
        if dom_elapsed and dom_elapsed.get("k") == "i" and len(delays_ms) >= 2:
            min_elapsed = int(dom_elapsed["v"][0])
            max_elapsed = int(dom_elapsed["v"][1])
            curr = int(sum(delays_ms[1:]))  # elapsed since rm_create_request
            if curr < min_elapsed:
                need = min_elapsed - curr
                candidates: List[Tuple[int, int]] = []
                for idx in range(1, len(delays_ms)):
                    headroom = max(0, p95s[idx] - delays_ms[idx])
                    if headroom > 0:
                        candidates.append((headroom, idx))
                candidates.sort(reverse=True)
                for headroom, idx in candidates:
                    if need <= 0:
                        break
                    add = min(need, headroom)
                    delays_ms[idx] += add
                    need -= add
            curr2 = int(sum(delays_ms[1:]))
            if curr2 > max_elapsed:
                need_down = curr2 - max_elapsed
                candidates2: List[Tuple[int, int]] = []
                for idx in range(1, len(delays_ms)):
                    reducible = max(0, delays_ms[idx] - p50s[idx])
                    if reducible > 0:
                        candidates2.append((reducible, idx))
                candidates2.sort(reverse=True)
                for reducible, idx in candidates2:
                    if need_down <= 0:
                        break
                    sub = min(need_down, reducible)
                    delays_ms[idx] -= sub
                    need_down -= sub

    # If ends with rm_create_timeout, keep waited_ms (total since rm_create_request) within its domain.
    if emit_refs and emit_refs[-1].endswith(".rm_create_timeout"):
        _, _, last_tmpl = get_template(emit_refs[-1])
        doms_last = build_template_domains(last_tmpl, state)
        dom_waited = doms_last.get("waited_ms")
        if dom_waited and dom_waited.get("k") == "i" and len(delays_ms) >= 2:
            min_waited = int(dom_waited["v"][0])
            max_waited = int(dom_waited["v"][1])

            # waited_ms is modeled as total elapsed from rm_create_request to rm_create_timeout.
            curr = int(sum(delays_ms[1:]))
            if curr < min_waited:
                need = min_waited - curr
                # Prefer increasing the final "wait" step first, then others.
                order = list(range(len(delays_ms) - 1, 0, -1))
                for idx in order:
                    if need <= 0:
                        break
                    headroom = max(0, p95s[idx] - delays_ms[idx])
                    if headroom <= 0:
                        continue
                    add = min(need, headroom)
                    delays_ms[idx] += add
                    need -= add

            curr2 = int(sum(delays_ms[1:]))
            if curr2 > max_waited:
                need_down = curr2 - max_waited
                # Prefer reducing the final "wait" step first, then any others above their p50.
                order = list(range(len(delays_ms) - 1, 0, -1))
                for idx in order:
                    if need_down <= 0:
                        break
                    reducible = max(0, delays_ms[idx] - p50s[idx])
                    if reducible <= 0:
                        continue
                    sub = min(need_down, reducible)
                    delays_ms[idx] -= sub
                    need_down -= sub

    trace_id = ""
    t = start_ts

    ts_create_request: Optional[datetime] = None

    for j, ref in enumerate(emit_refs):
        comp_id, log_id, tmpl = get_template(ref)
        if comp_id not in comp_host:
            comp_host[comp_id] = choose_host(comp_id, f"{inst_key}:{comp_id}")
        host = comp_host[comp_id]
        svc = COMP_BY_ID[comp_id].get("svc", "") or ""

        # advance time
        t = t + timedelta(milliseconds=delays_ms[j])

        doms = build_template_domains(tmpl, state)
        vals: Dict[str, Any] = {}
        for var, dom in doms.items():
            if var in ctx:
                vals[var] = ctx[var]
            else:
                vals[var] = sample_domain(dom, f"{inst_key}:{ref}:{var}")
                ctx[var] = vals[var]

        # Bind observed timing fields to the same chronology used for timestamps.
        if log_id == "canary_pass":
            vals["elapsed_ms"] = int(delays_ms[j])
            ctx["elapsed_ms"] = vals["elapsed_ms"]
        elif log_id == "canary_timeout":
            vals["timeout_ms"] = 30000
            vals["elapsed_ms"] = int(delays_ms[j])
            ctx["timeout_ms"] = vals["timeout_ms"]
            ctx["elapsed_ms"] = vals["elapsed_ms"]
        elif log_id == "hc_ok":
            vals["rtt_ms"] = int(delays_ms[j])
            ctx["rtt_ms"] = vals["rtt_ms"]
        elif log_id == "hc_fail":
            vals["waited_ms"] = int(delays_ms[j])
            ctx["waited_ms"] = vals["waited_ms"]
        elif log_id == "ga_egress_ok":
            vals["latency_ms"] = int(delays_ms[j])
            ctx["latency_ms"] = vals["latency_ms"]
        elif log_id == "rm_create_timeout":
            # waited_ms reflects total time RM waited since the create request was received.
            if ts_create_request is None:
                # Fallback: sum since first log (shouldn't happen given emit ordering).
                waited_total = int(sum(delays_ms[1 : j + 1])) if j >= 1 else int(delays_ms[j])
            else:
                waited_total = int(round((t - ts_create_request).total_seconds() * 1000.0))
            vals["waited_ms"] = waited_total
            ctx["waited_ms"] = vals["waited_ms"]
        elif log_id == "rm_create_ready":
            # elapsed_ms represents time from rm_create_request to rm_create_ready
            if ts_create_request is None:
                elapsed_total = int(sum(delays_ms[1 : j + 1])) if j >= 1 else int(delays_ms[j])
            else:
                elapsed_total = int(round((t - ts_create_request).total_seconds() * 1000.0))
            vals["elapsed_ms"] = elapsed_total
            ctx["elapsed_ms"] = vals["elapsed_ms"]

        msg = render_message(tmpl, vals)
        emit_row(rows, t, tmpl["lvl"], msg, trace_id, svc, host)

        if log_id == "rm_create_request":
            ts_create_request = t


# ----------------------------
# Background simulation
# ----------------------------

def simulate_background_for_minute(rows: List[Dict[str, Any]], state: str, minute: int) -> None:
    minute_start = BASE_TIME + timedelta(minutes=minute)
    for comp in SYSTEM["components"]:
        comp_id = comp["id"]
        beh = comp.get("beh", {}).get(state, {})
        emits = beh.get("emit", []) or []
        for e in emits:
            log_id = e["id"]
            per_min = float(e["per_min"])
            scope = e.get("scope") or "per_host"

            source_key = f"{comp_id}.{log_id}"
            eff = per_min
            if state == "f":
                eff *= get_rate_mult(state, minute, source_key)

            _, _, tmpl = get_template(source_key)

            if scope == "global":
                count = ALLOC.alloc(f"bg|{state}|{source_key}", eff)
                if count <= 0:
                    continue
                ts_list = schedule_within_minute(minute_start, count, f"bg|{state}|{source_key}|m{minute}")
                for i, ts in enumerate(ts_list):
                    host = choose_host_round_robin(comp_id, md5_int(f"{source_key}|{minute}|{i}") % 1000000)
                    svc = comp.get("svc", "") or ""
                    doms = build_template_domains(tmpl, state)
                    vals = {var: sample_domain(dom, f"bg:{state}:{source_key}:{minute}:{i}:{var}") for var, dom in doms.items()}
                    msg = render_message(tmpl, vals)
                    emit_row(rows, ts, tmpl["lvl"], msg, "", svc, host)
            else:
                hosts = comp.get("hosts", []) or []
                for h in hosts:
                    count = ALLOC.alloc(f"bg|{state}|{source_key}|{h}", eff)
                    if count <= 0:
                        continue
                    ts_list = schedule_within_minute(minute_start, count, f"bg|{state}|{source_key}|{h}|m{minute}")
                    svc = comp.get("svc", "") or ""
                    for i, ts in enumerate(ts_list):
                        doms = build_template_domains(tmpl, state)
                        vals = {var: sample_domain(dom, f"bg:{state}:{source_key}:{h}:{minute}:{i}:{var}") for var, dom in doms.items()}
                        msg = render_message(tmpl, vals)
                        emit_row(rows, ts, tmpl["lvl"], msg, "", svc, h)


# ----------------------------
# One-shots
# ----------------------------

def simulate_one_shots(rows: List[Dict[str, Any]]) -> None:
    for ev in FAIL_EVENTS:
        m = int(ev["at_min"])
        event_ts = BASE_TIME + timedelta(minutes=m)
        one_shots = ev.get("one_shots", []) or []
        for sidx, shot in enumerate(one_shots):
            ref = shot["ref"]
            count = int(shot["count"])
            hosts_allowed = shot.get("hosts", None)
            comp_id, _, tmpl = get_template(ref)
            comp = COMP_BY_ID[comp_id]
            svc = comp.get("svc", "") or ""
            hosts = hosts_allowed if hosts_allowed is not None else (comp.get("hosts", []) or [""])
            if not hosts:
                hosts = [""]

            for i in range(count):
                # Must not precede the event boundary; jitter is non-negative.
                jitter_ms = int(round(u01(f"oneshot:{m}:{ref}:{sidx}:{i}") * 900.0))  # [0, 900]ms
                ts = event_ts + timedelta(milliseconds=jitter_ms + i * 10)
                host = hosts[i % len(hosts)]
                doms = build_template_domains(tmpl, "f")
                vals = {var: sample_domain(dom, f"oneshot:{m}:{ref}:{i}:{var}") for var, dom in doms.items()}
                msg = render_message(tmpl, vals)
                emit_row(rows, ts, tmpl["lvl"], msg, "", svc, host)


# ----------------------------
# Main simulation
# ----------------------------

def main() -> None:
    rows: List[Dict[str, Any]] = []
    total_minutes = int(SCENARIO["scenario"]["time"]["total_minutes"])

    for minute in range(total_minutes):
        state = "n" if minute < PH_N["end_min"] else "f"
        simulate_background_for_minute(rows, state, minute)

    for minute in range(total_minutes):
        state = "n" if minute < PH_N["end_min"] else "f"
        minute_start = BASE_TIME + timedelta(minutes=minute)

        for flow in SYSTEM["flows"][state]["req"]:
            flow_id = flow["id"]
            rpm = float(flow["rpm"])
            eff = rpm
            if state == "f":
                eff *= get_rate_mult(state, minute, flow_id)

            count = ALLOC.alloc(f"flow|{state}|{flow_id}", eff)
            if count <= 0:
                continue

            starts = schedule_within_minute(minute_start, count, f"flow|{state}|{flow_id}|m{minute}")
            for s in starts:
                simulate_flow_instance(rows, state, minute, flow_id, s)

    simulate_one_shots(rows)

    df = pd.DataFrame(rows, columns=["timestamp", "level", "message", "trace_id", "service", "host"])
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
