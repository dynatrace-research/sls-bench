import math
import re
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# -----------------------------
# Embedded executable spec (normalized)
# -----------------------------
SYSTEM: Dict[str, Any] = {
    "sys": {"id": "joyent_sdc_us_east_1"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["sdc_api"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "ops_tool",
            "svc": "sdc-ops-tool",
            "hosts": ["ops1"],
            "logs": {
                "cmd_invoke": {
                    "lvl": "INFO",
                    "msg": "Executing action={action} target={target} requested_by={user}",
                    "vars": {
                        "action": {"k": "ch", "v": ["reboot"]},
                        "target": {"k": "ch", "v": ["all_us_east_1"]},
                        "user": {"k": "ch", "v": ["oncall_ops"]},
                    },
                },
                "target_expanded": {
                    "lvl": "WARN",
                    "msg": "Target expanded to servers={server_count} az={az} selector={selector}",
                    "vars": {
                        "server_count": {"k": "i", "v": [35, 60]},
                        "az": {"k": "ch", "v": ["us-east-1"]},
                        "selector": {"k": "ch", "v": ["role=all"]},
                    },
                },
                "reboot_dispatched": {
                    "lvl": "ERROR",
                    "msg": "Issued reboot batch_id={batch_id} servers={server_count} az={az}",
                    "vars": {
                        "batch_id": {"k": "hex", "v": 12},
                        "server_count": {"k": "i", "v": [35, 60]},
                        "az": {"k": "ch", "v": ["us-east-1"]},
                    },
                },
                "cmd_invoke_targeted": {
                    "lvl": "INFO",
                    "msg": "Executing action={action} target={target} requested_by={user} reason={reason}",
                    "vars": {
                        "action": {"k": "ch", "v": ["reboot"]},
                        "target": {"k": "ch", "v": ["lagging_nodes"]},
                        "user": {"k": "ch", "v": ["oncall_ops"]},
                        "reason": {"k": "ch", "v": ["legacy_dhcp_lease_failure", "slow_pxe_boot"]},
                    },
                },
                "target_expanded_targeted": {
                    "lvl": "INFO",
                    "msg": "Target expanded to servers={server_count} az={az} selector={selector}",
                    "vars": {
                        "server_count": {"k": "i", "v": [1, 10]},
                        "az": {"k": "ch", "v": ["us-east-1"]},
                        "selector": {"k": "ch", "v": ["role=lagging"]},
                    },
                },
                "reboot_dispatched_targeted": {
                    "lvl": "WARN",
                    "msg": "Issued reboot batch_id={batch_id} servers={server_count} az={az} target={target}",
                    "vars": {
                        "batch_id": {"k": "hex", "v": 12},
                        "server_count": {"k": "i", "v": [1, 10]},
                        "az": {"k": "ch", "v": ["us-east-1"]},
                        "target": {"k": "ch", "v": ["lagging_nodes"]},
                    },
                },
                "audit_tick": {
                    "lvl": "INFO",
                    "msg": "ops_tool audit ok run_id={run_id}",
                    "vars": {"run_id": {"k": "hex", "v": 8}},
                },
            },
            "beh": {
                "n": [{"id": "audit_tick", "per_min": 0.05, "scope": "per_host"}],
                "f": [{"id": "audit_tick", "per_min": 0.1, "scope": "per_host"}],
            },
        },
        {
            "id": "sdc_api",
            "svc": "sdc-api",
            "hosts": ["api1", "api2", "api3"],
            "logs": {
                "http_200_access": {
                    "lvl": "INFO",
                    "msg": "HTTP {method} {req_path} status=200 dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "req_path": {"k": "ch", "v": ["/v1/instances", "/v1/account", "/v1/ping"]},
                        "dur_ms": {"k": "i", "v": [5, 1500]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_503_access": {
                    "lvl": "WARN",
                    "msg": "HTTP {method} {req_path} status=503 dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "req_path": {"k": "ch", "v": ["/v1/instances", "/v1/account", "/v1/ping"]},
                        "dur_ms": {"k": "i", "v": [1, 200]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "startup_wait": {
                    "lvl": "WARN",
                    "msg": "API starting; waiting_for_quorum={quorum} last_err={last_err}",
                    "vars": {
                        "quorum": {"k": "ch", "v": ["absent", "partial"]},
                        "last_err": {"k": "ch", "v": ["no_leader", "connect_timeout"]},
                    },
                },
                "restart": {
                    "lvl": "INFO",
                    "msg": "Restarting API service reason={reason} ticket={ticket}",
                    "vars": {
                        "reason": {"k": "ch", "v": ["control_plane_recovery", "host_reboot"]},
                        "ticket": {"k": "hex", "v": 10},
                    },
                },
            },
            "beh": {"n": [], "f": [{"id": "startup_wait", "per_min": 0.6, "scope": "per_host"}]},
        },
        {
            "id": "control_store",
            "svc": "sdc-control-store",
            "hosts": ["cs1", "cs2", "cs3"],
            "logs": {
                "store_metrics": {
                    "lvl": "INFO",
                    "msg": "store_metrics role={role} peers_visible={peers} commit_lag_ms={lag_ms}",
                    "vars": {"role": {"k": "ch", "v": ["leader", "follower", "unknown"]}, "peers": {"k": "i", "v": [0, 3]}},
                    "state_vars": {"n": {"lag_ms": {"k": "i", "v": [0, 200]}}, "f": {"lag_ms": {"k": "i", "v": [0, 20000]}}},
                },
                "quorum_lost": {
                    "lvl": "ERROR",
                    "msg": "Quorum lost; peers_visible={peers} term={term}",
                    "vars": {"peers": {"k": "i", "v": [0, 2]}, "term": {"k": "i", "v": [1, 2000]}},
                },
                "election": {
                    "lvl": "INFO",
                    "msg": "Leader election term={term} votes={votes} peers_visible={peers}",
                    "vars": {"term": {"k": "i", "v": [1, 2000]}, "votes": {"k": "i", "v": [0, 3]}, "peers": {"k": "i", "v": [0, 3]}},
                },
                "recovery_blocked": {
                    "lvl": "WARN",
                    "msg": "Recovery blocked history_entries={hist} required={required} safe_mode={safe_mode}",
                    "vars": {"hist": {"k": "i", "v": [0, 800]}, "required": {"k": "i", "v": [500, 2500]}, "safe_mode": {"k": "ch", "v": ["true"]}},
                },
                "manual_recover": {
                    "lvl": "INFO",
                    "msg": "Manual recovery applied op={op} new_term={term} peers_bootstrapped={peers}",
                    "vars": {"op": {"k": "ch", "v": ["reseed", "force_leader"]}, "term": {"k": "i", "v": [1, 2000]}, "peers": {"k": "i", "v": [1, 3]}},
                },
            },
            "beh": {
                "n": [{"id": "store_metrics", "per_min": 1.0, "scope": "per_host"}],
                "f": [
                    {"id": "store_metrics", "per_min": 2.0, "scope": "per_host"},
                    {"id": "election", "per_min": 0.6, "scope": "per_host"},
                    {"id": "quorum_lost", "per_min": 0.15, "scope": "per_host"},
                    {"id": "recovery_blocked", "per_min": 0.4, "scope": "per_host"},
                ],
            },
        },
        {
            "id": "boot_service",
            "svc": "sdc-boot",
            "hosts": ["boot1", "boot2"],
            "logs": {
                "pxe_req": {
                    "lvl": "INFO",
                    "msg": "PXE request node={node} lease_ip={lease_ip} cache={cache} rtt_ms={rtt_ms}",
                    "vars": {
                        "node": {"k": "str", "v": "cn{001..030}"},
                        "lease_ip": {"k": "ip", "v": "10.1.0.0/16"},
                        "cache": {"k": "ch", "v": ["hit", "stale"]},
                        "rtt_ms": {"k": "i", "v": [1, 1500]},
                    },
                },
                "dhcp_no_offer": {
                    "lvl": "WARN",
                    "msg": "DHCP no offer node={node} err={err} waited_ms={waited_ms}",
                    "vars": {
                        "node": {"k": "str", "v": "cn{001..030}"},
                        "err": {"k": "ch", "v": ["no_offer", "timeout"]},
                        "waited_ms": {"k": "i", "v": [500, 15000]},
                    },
                },
                "throttle_drop": {
                    "lvl": "WARN",
                    "msg": "Throttling PXE qlen={qlen} dropped={dropped} max_qps={max_qps}",
                    "vars": {"qlen": {"k": "i", "v": [0, 800]}, "dropped": {"k": "i", "v": [1, 200]}, "max_qps": {"k": "i", "v": [50, 500]}},
                },
                "throttle_update": {
                    "lvl": "INFO",
                    "msg": "Updated throttles max_qps={max_qps} tftp_workers={workers}",
                    "vars": {"max_qps": {"k": "i", "v": [200, 1200]}, "workers": {"k": "i", "v": [8, 64]}},
                },
                "tftp_metrics": {
                    "lvl": "INFO",
                    "msg": "boot_metrics sessions_per_min={sessions_per_min} active_transfers={active} avg_rtt_ms={avg_rtt_ms}",
                    "vars": {"active": {"k": "i", "v": [0, 30]}},
                    "state_vars": {
                        "n": {"sessions_per_min": {"k": "i", "v": [0, 2]}, "avg_rtt_ms": {"k": "i", "v": [1, 40]}},
                        "f": {"sessions_per_min": {"k": "i", "v": [1, 8]}, "avg_rtt_ms": {"k": "i", "v": [5, 1200]}},
                    },
                },
                "cache_refresh_warn": {
                    "lvl": "WARN",
                    "msg": "Control-plane unavailable; serving from cache age_s={age_s} cache_state={cache_state}",
                    "vars": {"age_s": {"k": "i", "v": [0, 7200]}, "cache_state": {"k": "ch", "v": ["stale"]}},
                },
            },
            "beh": {
                "n": [{"id": "tftp_metrics", "per_min": 1.0, "scope": "global"}],
                "f": [
                    {"id": "tftp_metrics", "per_min": 2.0, "scope": "global"},
                    {"id": "cache_refresh_warn", "per_min": 0.6, "scope": "global"},
                ],
            },
        },
        {
            "id": "node_agent",
            "svc": "sdc-agent",
            "hosts": [f"cn{str(i).zfill(3)}" for i in range(1, 31)],
            "logs": {
                "heartbeat": {
                    "lvl": "INFO",
                    "msg": "node_heartbeat node={node} vms={vms} agent_state={agent_state}",
                    "vars": {"node": {"k": "str", "v": "cn{001..030}"}, "vms": {"k": "i", "v": [0, 48]}, "agent_state": {"k": "ch", "v": ["ready", "draining"]}},
                },
                "pxe_attempt_start": {
                    "lvl": "INFO",
                    "msg": "PXE attempt start node={node} attempt={attempt} reason={reason}",
                    "vars": {"node": {"k": "str", "v": "cn{001..030}"}, "attempt": {"k": "i", "v": [1, 5]}, "reason": {"k": "ch", "v": ["reboot", "power_on"]}},
                },
                "pxe_retry": {
                    "lvl": "WARN",
                    "msg": "PXE retry node={node} next_attempt={attempt} backoff_ms={backoff_ms}",
                    "vars": {"node": {"k": "str", "v": "cn{001..030}"}, "attempt": {"k": "i", "v": [2, 5]}, "backoff_ms": {"k": "i", "v": [200, 4000]}},
                },
                "boot_complete": {
                    "lvl": "INFO",
                    "msg": "Boot complete node={node} took_ms={took_ms} net_ok={net_ok}",
                    "vars": {"node": {"k": "str", "v": "cn{001..030}"}, "took_ms": {"k": "i", "v": [5000, 240000]}, "net_ok": {"k": "ch", "v": ["true"]}},
                },
                "dhcp_lease_failed": {
                    "lvl": "ERROR",
                    "msg": "DHCP lease failed node={node} nic={nic} err={err}",
                    "vars": {"node": {"k": "str", "v": "cn{001..030}"}, "nic": {"k": "ch", "v": ["legacy_nic"]}, "err": {"k": "ch", "v": ["timeout", "no_offer"]}},
                },
            },
            "beh": {
                "n": [{"id": "heartbeat", "per_min": 0.5, "scope": "per_host"}],
                "f": [{"id": "heartbeat", "per_min": 0.2, "scope": "per_host"}],
            },
        },
    ],
    "flows": {
        "n": [
            {
                "id": "api_requests_ok",
                "rpm": 900.0,
                "emit": ["sdc_api.http_200_access"],
                "latency_ms": [[25, 120]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "pxe_boot_normal",
                "rpm": 0.2,
                "emit": ["node_agent.pxe_attempt_start", "boot_service.pxe_req", "node_agent.boot_complete"],
                "latency_ms": [[5, 30], [5, 30], [20000, 60000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
        "f": [
            {
                "id": "api_requests_503",
                "rpm": 650.0,
                "emit": ["sdc_api.http_503_access"],
                "latency_ms": [[15, 80]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "api_requests_partial_200",
                "rpm": 70.0,
                "emit": ["sdc_api.http_200_access"],
                "latency_ms": [[60, 400]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "pxe_boot_success",
                "rpm": 1.2,
                "emit": ["node_agent.pxe_attempt_start", "boot_service.pxe_req", "node_agent.boot_complete"],
                "latency_ms": [[5, 40], [20, 800], [30000, 180000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "pxe_boot_throttled",
                "rpm": 0.8,
                "emit": ["node_agent.pxe_attempt_start", "boot_service.throttle_drop"],
                "latency_ms": [[5, 30], [10, 300]],
                "retry": {
                    "max_attempts": 5,
                    "expected_attempts": 2.2,
                    "emit_per_retry": ["node_agent.pxe_retry"],
                    "backoff_ms": [[400, 900], [800, 1500], [1500, 2500], [2500, 4000]],
                },
                "trace": False,
            },
            {
                "id": "legacy_nic_dhcp_fail",
                "rpm": 0.4,
                "emit": ["node_agent.pxe_attempt_start", "boot_service.dhcp_no_offer", "node_agent.dhcp_lease_failed"],
                "latency_ms": [[5, 30], [500, 15000], [5000, 30000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "us_east_1_mass_reboot_20140527"},
    "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 25,
                    "rate_multipliers": {
                        "api_requests_partial_200": 0.0,
                        "legacy_nic_dhcp_fail": 0.0,
                        "control_store.quorum_lost": 6.0,
                        "control_store.election": 3.0,
                        "sdc_api.startup_wait": 2.0,
                    },
                    "latency_multipliers": {
                        "pxe_boot_success": {"p50": 1.3, "p95": 1.6},
                        "pxe_boot_throttled": {"p50": 1.2, "p95": 1.5},
                    },
                    "one_shots": [
                        {"ref": "ops_tool.cmd_invoke", "count": 1, "hosts": ["ops1"]},
                        {"ref": "ops_tool.target_expanded", "count": 1, "hosts": ["ops1"]},
                        {"ref": "ops_tool.reboot_dispatched", "count": 1, "hosts": ["ops1"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 30,
                    "rate_multipliers": {"pxe_boot_throttled": 0.3, "pxe_boot_success": 1.5},
                    "latency_multipliers": {
                        "pxe_boot_success": {"p50": 0.8, "p95": 0.7},
                        "pxe_boot_throttled": {"p50": 0.9, "p95": 0.8},
                    },
                    "one_shots": [{"ref": "boot_service.throttle_update", "count": 1, "hosts": ["boot1"]}],
                },
                {
                    "order": 3,
                    "at_min": 38,
                    "rate_multipliers": {"legacy_nic_dhcp_fail": 1.0, "pxe_boot_success": 1.8},
                    "latency_multipliers": {"pxe_boot_success": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [
                        {"ref": "ops_tool.cmd_invoke_targeted", "count": 1, "hosts": ["ops1"]},
                        {"ref": "ops_tool.target_expanded_targeted", "count": 1, "hosts": ["ops1"]},
                        {"ref": "ops_tool.reboot_dispatched_targeted", "count": 1, "hosts": ["ops1"]},
                    ],
                },
                {
                    "order": 4,
                    "at_min": 44,
                    "rate_multipliers": {
                        "api_requests_partial_200": 1.0,
                        "api_requests_503": 0.8,
                        "control_store.election": 0.5,
                        "control_store.quorum_lost": 0.4,
                        "control_store.recovery_blocked": 0.6,
                        "sdc_api.startup_wait": 0.7,
                    },
                    "latency_multipliers": {"api_requests_partial_200": {"p50": 1.8, "p95": 2.2}},
                    "one_shots": [
                        {"ref": "control_store.manual_recover", "count": 1, "hosts": ["cs1"]},
                        {"ref": "sdc_api.restart", "count": 1, "hosts": ["api1"]},
                    ],
                },
            ]
        }
    },
}

# -----------------------------
# Helpers
# -----------------------------
SEED = 1337
random.seed(SEED)
rng = np.random.RandomState(SEED)

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def ms_to_iso(ms_from_base: int) -> str:
    dt = BASE_TIME + timedelta(milliseconds=int(ms_from_base))
    s = dt.strftime("%Y-%m-%dT%H:%M:%S.%f")
    return s[:23] + "Z"


def stable_u32(s: str) -> int:
    h = hashlib.md5(s.encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big")


def rand_hex(n: int, rs: np.random.RandomState) -> str:
    b = rs.bytes((n + 1) // 2)
    hx = b.hex()
    return hx[:n]


def parse_cidr(cidr: str) -> Tuple[int, int, int, int]:
    ip, _pfx = cidr.split("/")
    octs = [int(x) for x in ip.split(".")]
    return octs[0], octs[1], octs[2], octs[3]


def node_to_num(node: str) -> int:
    m = re.match(r"cn(\d+)$", node)
    return int(m.group(1)) if m else 1


def ip_from_cidr_and_node(cidr: str, node: str, salt: int = 0) -> str:
    a, b, _, _ = parse_cidr(cidr)
    n = node_to_num(node)
    v = (n * 97 + salt * 53) % 65534 + 1
    c = v // 256
    d = v % 256
    if d == 0:
        d = 1
    return f"{a}.{b}.{c}.{d}"


def sample_lognormal_ms(
    p50: float,
    p95: float,
    rs: np.random.RandomState,
    cap_mult: float = 3.0,
    hard_max: Optional[int] = None,
    hard_min: int = 1,
) -> int:
    p50 = max(1e-6, float(p50))
    p95 = max(p50 * 1.000001, float(p95))
    mu = math.log(p50)
    sigma = math.log(p95 / p50) / 1.645
    z = rs.normal(0.0, 1.0)
    x = math.exp(mu + sigma * z)
    cap = cap_mult * p95
    if hard_max is not None:
        cap = min(cap, float(hard_max))
    x = min(x, cap)
    x = max(float(hard_min), x)
    return int(round(x))


def jitter_ms(rs: np.random.RandomState, max_abs_ms: int) -> int:
    if max_abs_ms <= 0:
        return 0
    return int(rs.randint(-max_abs_ms, max_abs_ms + 1))


def schedule_evenly(start_ms: int, end_ms: int, count: int, rs: np.random.RandomState, jitter_abs_ms: int) -> List[int]:
    if count <= 0:
        return []
    dur = max(1, end_ms - start_ms)
    if count == 1:
        t = start_ms + dur // 2 + jitter_ms(rs, jitter_abs_ms)
        return [max(start_ms, min(end_ms - 1, t))]
    step = dur / count
    out: List[int] = []
    for i in range(count):
        base = start_ms + int((i + 0.5) * step)
        t = base + jitter_ms(rs, jitter_abs_ms)
        if t < start_ms:
            t = start_ms
        if t >= end_ms:
            t = end_ms - 1
        out.append(t)
    out.sort()
    return out


def choose_from_list(values: List[Any], rs: np.random.RandomState) -> Any:
    if not values:
        return ""
    return values[int(rs.randint(0, len(values)))]


def parse_str_domain(v: str) -> Optional[List[str]]:
    m = re.match(r"^([a-zA-Z_]+)\{(\d+)\.\.(\d+)\}$", v)
    if not m:
        return None
    prefix = m.group(1)
    a = m.group(2)
    b = m.group(3)
    lo = int(a)
    hi = int(b)
    width = len(a)
    return [f"{prefix}{str(i).zfill(width)}" for i in range(lo, hi + 1)]


def sample_domain(domain: Dict[str, Any], rs: np.random.RandomState, hint_ctx: Optional[Dict[str, Any]] = None) -> Any:
    k = domain["k"]
    v = domain["v"]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return int(rs.randint(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return float(lo + (hi - lo) * rs.rand())
    if k == "ch":
        return choose_from_list(list(v), rs)
    if k == "uuid":
        hx = rand_hex(32, rs)
        return f"{hx[:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:]}"
    if k == "hex":
        return rand_hex(int(v), rs)
    if k == "ip":
        cidr = str(v)
        node = (hint_ctx or {}).get("node")
        salt = int((hint_ctx or {}).get("_salt", 0))
        if node:
            return ip_from_cidr_and_node(cidr, str(node), salt=salt)
        a, b, _, _ = parse_cidr(cidr)
        c = int(rs.randint(0, 256))
        d = int(rs.randint(1, 255))
        return f"{a}.{b}.{c}.{d}"
    if k == "str":
        dom = parse_str_domain(str(v))
        if dom:
            return choose_from_list(dom, rs)
        return str(v)
    return str(v)


def split_ref(ref: str) -> Tuple[str, str]:
    comp_id, log_id = ref.split(".", 1)
    return comp_id, log_id


# -----------------------------
# Indices
# -----------------------------
COMP: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}
LOGS: Dict[Tuple[str, str], Dict[str, Any]] = {}
for c in SYSTEM["components"]:
    for log_id, t in c["logs"].items():
        LOGS[(c["id"], log_id)] = t

FLOWS: Dict[Tuple[str, str], Dict[str, Any]] = {}
for st in ["n", "f"]:
    for f in SYSTEM["flows"][st]:
        FLOWS[(st, f["id"])] = f


def field_bounds(comp_id: str, log_id: str, field: str, state: str) -> Tuple[Optional[int], Optional[int]]:
    tmpl = LOGS.get((comp_id, log_id), {})
    dom = None
    if "vars" in tmpl and field in tmpl["vars"]:
        dom = tmpl["vars"][field]
    if "state_vars" in tmpl and state in tmpl["state_vars"] and field in tmpl["state_vars"][state]:
        dom = tmpl["state_vars"][state][field]
    if dom and dom.get("k") == "i":
        lo, hi = int(dom["v"][0]), int(dom["v"][1])
        return lo, hi
    return None, None


# -----------------------------
# Controller derivation for failure phase
# -----------------------------
@dataclass(frozen=True)
class Interval:
    state: str
    start_min: int
    end_min: int
    rate_mult_flow: Dict[str, float]
    rate_mult_bg: Dict[str, float]
    lat_mult_flow: Dict[str, Tuple[float, float]]


def build_failure_intervals() -> List[Interval]:
    fstart = SCENARIO["time"]["phases"]["f"]["start_min"]
    fend = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    at_mins = sorted({fstart} | {e["at_min"] for e in events if fstart <= e["at_min"] <= fend} | {fend})
    flow_rm: Dict[str, float] = {}
    bg_rm: Dict[str, float] = {}
    flow_lm: Dict[str, Tuple[float, float]] = {}
    intervals: List[Interval] = []
    events_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        events_by_min.setdefault(e["at_min"], []).append(e)

    def apply_event(e: Dict[str, Any]) -> None:
        for k, v in (e.get("rate_multipliers") or {}).items():
            if "." in k:
                bg_rm[k] = float(v)
            else:
                flow_rm[k] = float(v)
        for k, v in (e.get("latency_multipliers") or {}).items():
            flow_lm[k] = (float(v.get("p50", 1.0)), float(v.get("p95", 1.0)))

    for i in range(len(at_mins) - 1):
        a = at_mins[i]
        b = at_mins[i + 1]
        for e in events_by_min.get(a, []):
            apply_event(e)
        intervals.append(
            Interval(
                state="f",
                start_min=a,
                end_min=b,
                rate_mult_flow=dict(flow_rm),
                rate_mult_bg=dict(bg_rm),
                lat_mult_flow=dict(flow_lm),
            )
        )
    return intervals


FAIL_INTERVALS = build_failure_intervals()

FAIL_ONE_SHOTS: List[Dict[str, Any]] = []
for e in SCENARIO["phases"]["f"]["events"]:
    for os in (e.get("one_shots") or []):
        FAIL_ONE_SHOTS.append({"at_min": e["at_min"], **os})

# -----------------------------
# Deterministic count allocator (carry-based)
# -----------------------------
class CarryAllocator:
    def __init__(self) -> None:
        self.carry: Dict[str, float] = {}

    def alloc(self, key: str, expected: float) -> int:
        x = float(expected) + float(self.carry.get(key, 0.0))
        n = int(math.floor(x + 1e-12))
        self.carry[key] = x - n
        return max(0, n)


ALLOC = CarryAllocator()

# -----------------------------
# Rendering
# -----------------------------
def render_log(comp_id: str, log_id: str, state: str, rs: np.random.RandomState, bound: Dict[str, Any]) -> Tuple[str, str]:
    tmpl = LOGS[(comp_id, log_id)]
    vars_all: Dict[str, Dict[str, Any]] = {}
    vars_all.update(tmpl.get("vars") or {})
    state_vars = (tmpl.get("state_vars") or {}).get(state) or {}
    vars_all.update(state_vars)

    vals: Dict[str, Any] = dict(bound)
    for k, dom in vars_all.items():
        if k not in vals:
            vals[k] = sample_domain(dom, rs, hint_ctx=vals)

    msg = tmpl["msg"].format(**vals)
    lvl = tmpl["lvl"]
    return lvl, msg


def pick_host_for_component(comp_id: str, rr_index: int) -> str:
    hosts = COMP[comp_id].get("hosts") or []
    if not hosts:
        return ""
    return hosts[rr_index % len(hosts)]


def flow_latency_multiplier(interval: Optional[Interval], flow_id: str) -> Tuple[float, float]:
    if interval is None:
        return (1.0, 1.0)
    return interval.lat_mult_flow.get(flow_id, (1.0, 1.0))


def flow_rate_multiplier(interval: Optional[Interval], flow_id: str) -> float:
    if interval is None:
        return 1.0
    return float(interval.rate_mult_flow.get(flow_id, 1.0))


def bg_rate_multiplier(interval: Optional[Interval], comp_id: str, log_id: str) -> float:
    if interval is None:
        return 1.0
    key = f"{comp_id}.{log_id}"
    return float(interval.rate_mult_bg.get(key, 1.0))


def plan_attempt_counts(flow_id: str, start_min: int, n_instances: int, expected_attempts: float, max_attempts: int) -> List[int]:
    if n_instances <= 0:
        return []
    if max_attempts <= 1:
        return [1] * n_instances
    e = float(expected_attempts)
    lo = int(math.floor(e))
    hi = int(math.ceil(e))
    lo = max(1, min(max_attempts, lo))
    hi = max(1, min(max_attempts, hi))
    if lo == hi:
        return [lo] * n_instances

    frac = max(0.0, min(1.0, e - math.floor(e)))
    k_hi = int(round(frac * n_instances))
    seed = stable_u32(f"attempts|{flow_id}|{start_min}|{n_instances}|{expected_attempts}|{max_attempts}")
    rs = np.random.RandomState(seed)
    perm = rs.permutation(n_instances)
    hi_set = set(int(i) for i in perm[:k_hi])

    out: List[int] = []
    for i in range(n_instances):
        out.append(hi if i in hi_set else lo)
    return out


# -----------------------------
# Simulation
# -----------------------------
rows: List[Dict[str, Any]] = []


def emit_row(ts_ms: int, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append(
        {
            "timestamp_ms": int(ts_ms),
            "level": str(level),
            "message": str(message),
            "trace_id": str(trace_id),
            "service": str(service),
            "host": str(host),
        }
    )


def simulate_background_interval(state: str, start_min: int, end_min: int, interval: Optional[Interval]) -> None:
    start_ms = start_min * 60_000
    end_ms = end_min * 60_000
    dur_min = float(end_min - start_min)

    for comp_id, comp in COMP.items():
        beh = comp.get("beh", {}).get(state, [])
        for src in beh:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope") or "per_host"
            mult = 1.0
            if state == "f":
                mult = bg_rate_multiplier(interval, comp_id, log_id)
            eff = per_min * mult

            if scope == "global":
                expected = eff * dur_min
                key = f"bg|{state}|{comp_id}.{log_id}"
                count = ALLOC.alloc(key, expected)
                rs_local = np.random.RandomState(stable_u32(f"bg_sched|{key}|{start_min}|{end_min}"))
                times = schedule_evenly(start_ms, end_ms, count, rs_local, jitter_abs_ms=250)
                host = (comp.get("hosts") or [""])[0] if comp.get("hosts") else ""
                for j, t in enumerate(times):
                    bound: Dict[str, Any] = {"_salt": (start_min * 100000 + j)}
                    if comp_id == "boot_service" and log_id == "tftp_metrics":
                        if state == "n":
                            bound["sessions_per_min"] = int(0 + (j % 3 == 0))
                            bound["avg_rtt_ms"] = int(5 + (j % 7) * 3)
                        else:
                            minute_offset = (t - start_ms) / 60_000.0
                            prog = (start_min + minute_offset - 25.0) / 25.0
                            prog = max(0.0, min(1.0, prog))
                            bound["sessions_per_min"] = int(1 + round(6 * prog))
                            bound["avg_rtt_ms"] = int(80 + round(600 * (0.2 + 0.8 * prog)))
                    if comp_id == "control_store" and log_id == "store_metrics":
                        if state == "n":
                            bound["role"] = "leader" if host == "cs1" else "follower"
                            bound["peers"] = 3
                        else:
                            bound["role"] = choose_from_list(["unknown", "follower", "unknown"], rs_local)
                            bound["peers"] = int(rs_local.randint(0, 3))
                    if comp_id == "sdc_api" and log_id == "startup_wait":
                        if state == "f":
                            minute_global = (t // 60_000)
                            bound["quorum"] = "absent" if minute_global < 44 else "partial"
                            bound["last_err"] = "no_leader" if minute_global < 44 else choose_from_list(
                                ["no_leader", "connect_timeout"], rs_local
                            )

                    lvl, msg = render_log(comp_id, log_id, state, rs_local, bound)
                    emit_row(t, lvl, msg, "", comp.get("svc", "") or "", host)
            else:
                hosts = comp.get("hosts") or []
                for h_i, host in enumerate(hosts):
                    expected = eff * dur_min
                    key = f"bg|{state}|{comp_id}.{log_id}|{host}"
                    count = ALLOC.alloc(key, expected)
                    rs_local = np.random.RandomState(stable_u32(f"bg_sched|{key}|{start_min}|{end_min}"))
                    times = schedule_evenly(start_ms, end_ms, count, rs_local, jitter_abs_ms=250)
                    for j, t in enumerate(times):
                        bound = {"_salt": (h_i * 1000000 + start_min * 100000 + j)}
                        if comp_id == "node_agent" and log_id == "heartbeat":
                            bound["node"] = host
                            bound["agent_state"] = "ready" if state == "n" else choose_from_list(
                                ["ready", "draining", "ready"], rs_local
                            )
                        if comp_id == "control_store" and log_id == "store_metrics":
                            if state == "n":
                                bound["role"] = "leader" if host == "cs1" else "follower"
                                bound["peers"] = 3
                            else:
                                bound["role"] = choose_from_list(["unknown", "follower", "unknown"], rs_local)
                                bound["peers"] = int(rs_local.randint(0, 3))

                        lvl, msg = render_log(comp_id, log_id, state, rs_local, bound)
                        emit_row(t, lvl, msg, "", comp.get("svc", "") or "", host)


def simulate_one_shots() -> None:
    for idx, os in enumerate(FAIL_ONE_SHOTS):
        at_min = int(os["at_min"])
        ref = os["ref"]
        count = int(os["count"])
        hosts = list(os.get("hosts") or [])
        comp_id, log_id = split_ref(ref)
        comp = COMP[comp_id]
        base_ms = at_min * 60_000
        rs_local = np.random.RandomState(stable_u32(f"oneshot|{at_min}|{ref}|{idx}|{count}"))
        for j in range(count):
            t = base_ms + int(500 + 200 * j + abs(jitter_ms(rs_local, 200)))
            host = choose_from_list(hosts, rs_local) if hosts else pick_host_for_component(comp_id, j)
            bound: Dict[str, Any] = {"_salt": (idx * 1000 + j)}
            lvl, msg = render_log(comp_id, log_id, "f", rs_local, bound)
            emit_row(t, lvl, msg, "", comp.get("svc", "") or "", host)


def simulate_flow_instance(
    state: str,
    flow: Dict[str, Any],
    interval: Optional[Interval],
    start_ms: int,
    inst_index: int,
    attempt_count: int,
    host_rr: Dict[str, int],
) -> None:
    flow_id = flow["id"]
    emit_refs: List[str] = list(flow["emit"])
    latency_pairs: List[List[float]] = list(flow["latency_ms"])
    retry = flow["retry"]
    emit_per_retry: List[str] = list(retry.get("emit_per_retry") or [])
    backoff_pairs: List[List[float]] = list(retry.get("backoff_ms") or [])

    traced = bool(flow.get("trace", False)) and bool(SYSTEM["tracing"]["on"])
    trace_id = rand_hex(32, rng) if traced else ""

    # component-local host stickiness
    comp_host: Dict[str, str] = {}
    node_host = ""
    if any(ref.startswith("node_agent.") for ref in emit_refs) or any(ref.startswith("node_agent.") for ref in emit_per_retry):
        rr = host_rr.get("node_agent", 0)
        node_host = pick_host_for_component("node_agent", rr)
        host_rr["node_agent"] = rr + 1
        comp_host["node_agent"] = node_host
    for ref in emit_refs + emit_per_retry:
        c_id, _ = split_ref(ref)
        if c_id not in comp_host:
            rr = host_rr.get(c_id, 0)
            comp_host[c_id] = pick_host_for_component(c_id, rr)
            host_rr[c_id] = rr + 1

    lm50, lm95 = (1.0, 1.0)
    if state == "f":
        lm50, lm95 = flow_latency_multiplier(interval, flow_id)

    rs_ctx = np.random.RandomState(stable_u32(f"flowctx|{state}|{flow_id}|{start_ms}|{inst_index}"))
    method = choose_from_list(["GET", "POST"], rs_ctx)
    req_path = choose_from_list(["/v1/instances", "/v1/account", "/v1/ping"], rs_ctx)
    pxe_reason = "power_on" if state == "n" else "reboot"

    # Pre-sample retry backoffs per inter-attempt gap so the logged backoff_ms matches the timestamp spacing.
    gap_backoffs: List[int] = []
    if attempt_count > 1:
        for g in range(1, attempt_count):
            idx = g - 1
            if idx < len(backoff_pairs):
                bp50, bp95 = backoff_pairs[idx]
            else:
                bp50, bp95 = backoff_pairs[-1] if backoff_pairs else (200.0, 500.0)
            bmin, bmax = field_bounds("node_agent", "pxe_retry", "backoff_ms", state)
            gap_backoffs.append(
                sample_lognormal_ms(
                    bp50,
                    bp95,
                    rng,
                    cap_mult=3.0,
                    hard_max=int(bmax) if bmax is not None else 4000,
                    hard_min=int(bmin) if bmin is not None else 200,
                )
            )

    # Determine whether this flow includes the legacy DHCP failure chain and thus needs a bound err across components.
    needs_dhcp_err = ("boot_service.dhcp_no_offer" in emit_refs) or ("node_agent.dhcp_lease_failed" in emit_refs)

    attempt_start_ms = int(start_ms)
    for attempt in range(1, attempt_count + 1):
        # Bind per-attempt categorical context to keep outcome-bearing fields coherent across components.
        rs_attempt = np.random.RandomState(stable_u32(f"attemptctx|{state}|{flow_id}|{start_ms}|{inst_index}|{attempt}"))
        dhcp_err: Optional[str] = None
        if needs_dhcp_err:
            # Must be consistent across boot_service.dhcp_no_offer and node_agent.dhcp_lease_failed for this attempt.
            dhcp_err = choose_from_list(["no_offer", "timeout"], rs_attempt)

        sampled_lat: List[int] = []
        for li, (p50, p95) in enumerate(latency_pairs):
            sp50 = float(p50) * lm50
            sp95 = float(p95) * lm95
            hard_min = 1
            hard_max: Optional[int] = None

            ref = emit_refs[li]
            c_id, l_id = split_ref(ref)

            if c_id == "sdc_api" and l_id in ("http_200_access", "http_503_access"):
                mn, mx = field_bounds(c_id, l_id, "dur_ms", state)
                if mn is not None:
                    hard_min = int(mn)
                if mx is not None:
                    hard_max = int(mx)

            if c_id == "boot_service" and l_id == "pxe_req":
                mn, mx = field_bounds(c_id, l_id, "rtt_ms", state)
                if mn is not None:
                    hard_min = int(mn)
                if mx is not None:
                    hard_max = int(mx)

            if c_id == "boot_service" and l_id == "dhcp_no_offer":
                mn, mx = field_bounds(c_id, l_id, "waited_ms", state)
                if mn is not None:
                    hard_min = int(mn)
                if mx is not None:
                    hard_max = int(mx)

            sampled_lat.append(sample_lognormal_ms(sp50, sp95, rng, cap_mult=3.0, hard_max=hard_max, hard_min=hard_min))

        # Ensure boot_complete.took_ms aligns with timestamp gaps and within domain bounds.
        if "node_agent.boot_complete" in emit_refs:
            idx_bc = emit_refs.index("node_agent.boot_complete")
            took_ms = sum(sampled_lat[: idx_bc + 1])
            dom = LOGS[("node_agent", "boot_complete")]["vars"]["took_ms"]["v"]
            max_took = int(dom[1])
            min_took = int(dom[0])
            if took_ms > max_took and idx_bc >= 1:
                excess = took_ms - max_took
                sampled_lat[idx_bc] = max(1, sampled_lat[idx_bc] - excess)
            took_ms = sum(sampled_lat[: idx_bc + 1])
            if took_ms < min_took and idx_bc >= 1:
                sampled_lat[idx_bc] += (min_took - took_ms)

        t = attempt_start_ms
        attempt_end_ms = attempt_start_ms
        for li, ref in enumerate(emit_refs):
            c_id, l_id = split_ref(ref)
            host = comp_host.get(c_id, "")
            t = t + sampled_lat[li]
            attempt_end_ms = t

            bound: Dict[str, Any] = {"_salt": (inst_index * 1000 + attempt * 10 + li)}

            if c_id == "sdc_api" and l_id in ("http_200_access", "http_503_access"):
                bound["method"] = method
                bound["req_path"] = req_path
                bound["dur_ms"] = sampled_lat[li]
                bound["trace_id"] = trace_id

            if c_id == "node_agent" and l_id == "pxe_attempt_start":
                bound["node"] = node_host
                bound["attempt"] = attempt
                bound["reason"] = pxe_reason

            if c_id == "boot_service" and l_id == "pxe_req":
                bound["node"] = node_host
                bound["lease_ip"] = ip_from_cidr_and_node("10.1.0.0/16", node_host, salt=attempt)
                bound["cache"] = "hit" if state == "n" else "stale"
                bound["rtt_ms"] = sampled_lat[li]

            if c_id == "node_agent" and l_id == "boot_complete":
                took = t - attempt_start_ms
                bound["node"] = node_host
                bound["took_ms"] = int(took)
                bound["net_ok"] = "true"

            if c_id == "boot_service" and l_id == "dhcp_no_offer":
                bound["node"] = node_host
                bound["waited_ms"] = sampled_lat[li]
                if dhcp_err is not None:
                    bound["err"] = dhcp_err

            if c_id == "node_agent" and l_id == "dhcp_lease_failed":
                bound["node"] = node_host
                if dhcp_err is not None:
                    bound["err"] = dhcp_err

            lvl, msg = render_log(c_id, l_id, state, rs_ctx, bound)
            emit_row(t, lvl, msg, trace_id if traced else "", COMP[c_id].get("svc", "") or "", host)

        # Between attempts: schedule backoff and emit retry-only logs using the SAME backoff value.
        if attempt < attempt_count and emit_per_retry:
            backoff_ms = gap_backoffs[attempt - 1] if (attempt - 1) < len(gap_backoffs) else gap_backoffs[-1]
            retry_log_time = attempt_end_ms + 1
            for ref in emit_per_retry:
                c_id, l_id = split_ref(ref)
                host = comp_host.get(c_id, "")
                bound = {"_salt": inst_index * 100 + attempt}
                if c_id == "node_agent":
                    bound["node"] = node_host
                    bound["attempt"] = attempt + 1
                    bound["backoff_ms"] = backoff_ms
                lvl, msg = render_log(c_id, l_id, state, rs_ctx, bound)
                emit_row(retry_log_time, lvl, msg, trace_id if traced else "", COMP[c_id].get("svc", "") or "", host)

        if attempt < attempt_count:
            backoff_ms_next = gap_backoffs[attempt - 1] if (attempt - 1) < len(gap_backoffs) else gap_backoffs[-1]
            attempt_start_ms = attempt_end_ms + backoff_ms_next
        else:
            attempt_start_ms = attempt_end_ms


def simulate_flows_normal() -> None:
    start_min = SCENARIO["time"]["phases"]["n"]["start_min"]
    end_min = SCENARIO["time"]["phases"]["n"]["end_min"]
    start_ms = start_min * 60_000
    end_ms = end_min * 60_000
    dur_min = float(end_min - start_min)

    host_rr: Dict[str, int] = {"sdc_api": 0, "node_agent": 0, "boot_service": 0}

    for flow in SYSTEM["flows"]["n"]:
        flow_id = flow["id"]
        rpm = float(flow["rpm"])
        expected_instances = rpm * dur_min
        count = ALLOC.alloc(f"flow|n|{flow_id}", expected_instances)
        rs_sched = np.random.RandomState(stable_u32(f"flow_sched|n|{flow_id}|{start_min}|{end_min}|{count}"))
        starts = schedule_evenly(start_ms, end_ms, count, rs_sched, jitter_abs_ms=200)
        attempts = plan_attempt_counts(flow_id, start_min, count, flow["retry"]["expected_attempts"], flow["retry"]["max_attempts"])
        for i, st in enumerate(starts):
            simulate_flow_instance("n", flow, None, st, i, attempts[i], host_rr)


def simulate_flows_failure() -> None:
    host_rr: Dict[str, int] = {"sdc_api": 0, "node_agent": 0, "boot_service": 0, "control_store": 0, "ops_tool": 0}

    for interval in FAIL_INTERVALS:
        start_min = interval.start_min
        end_min = interval.end_min
        start_ms = start_min * 60_000
        end_ms = end_min * 60_000
        dur_min = float(end_min - start_min)
        for flow in SYSTEM["flows"]["f"]:
            flow_id = flow["id"]
            base_rpm = float(flow["rpm"])
            mult = flow_rate_multiplier(interval, flow_id)
            eff_rpm = base_rpm * mult
            expected_instances = eff_rpm * dur_min
            count = ALLOC.alloc(f"flow|f|{flow_id}", expected_instances)
            rs_sched = np.random.RandomState(stable_u32(f"flow_sched|f|{flow_id}|{start_min}|{end_min}|{count}"))
            starts = schedule_evenly(start_ms, end_ms, count, rs_sched, jitter_abs_ms=200)

            attempts = plan_attempt_counts(flow_id, start_min, count, flow["retry"]["expected_attempts"], flow["retry"]["max_attempts"])
            for i, st in enumerate(starts):
                simulate_flow_instance("f", flow, interval, st, i, attempts[i], host_rr)


# -----------------------------
# Run simulation
# -----------------------------
simulate_background_interval("n", SCENARIO["time"]["phases"]["n"]["start_min"], SCENARIO["time"]["phases"]["n"]["end_min"], None)
simulate_flows_normal()

for interval in FAIL_INTERVALS:
    simulate_background_interval("f", interval.start_min, interval.end_min, interval)
simulate_flows_failure()
simulate_one_shots()

# -----------------------------
# Output CSV
# -----------------------------
df = pd.DataFrame(rows)
df.sort_values(["timestamp_ms", "service", "host", "level"], inplace=True, kind="mergesort")
df["timestamp"] = df["timestamp_ms"].apply(ms_to_iso)

out = df[["timestamp", "level", "message", "trace_id", "service", "host"]].copy()
for c in ["level", "message", "trace_id", "service", "host", "timestamp"]:
    out[c] = out[c].fillna("").astype(str)

out.to_csv("logs.csv", index=False)
