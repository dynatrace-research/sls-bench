import math
import uuid
import hashlib
import ipaddress
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional, DefaultDict
from collections import defaultdict

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "aws_ebs_az_degradation_2012_10_22"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "api_frontend",
            "svc": "control-plane-api",
            "hosts": ["api-1", "api-2"],
            "logs": {
                "api_req_describe": {
                    "lvl": "INFO",
                    "msg": "api request op={op} acct={acct} req_id={req_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["DescribeInstances", "DescribeVolumes", "DescribeImages"]},
                        "acct": {"k": "ch", "v": ["acct_001", "acct_002", "acct_003", "acct_004", "acct_005", "acct_006"]},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "api_req_mutation": {
                    "lvl": "INFO",
                    "msg": "api request op={op} acct={acct} req_id={req_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["RunInstances", "TerminateInstances", "CreateVolume", "AttachVolume"]},
                        "acct": {"k": "ch", "v": ["acct_001", "acct_002", "acct_003", "acct_004", "acct_005", "acct_006"]},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "api_resp_200": {
                    "lvl": "INFO",
                    "msg": "api response 200 op={op} acct={acct} req_id={req_id} dur_ms={dur_ms}",
                    "vars": {
                        "op": {
                            "k": "ch",
                            "v": [
                                "DescribeInstances",
                                "DescribeVolumes",
                                "DescribeImages",
                                "RunInstances",
                                "TerminateInstances",
                                "CreateVolume",
                                "AttachVolume",
                            ],
                        },
                        "acct": {"k": "ch", "v": ["acct_001", "acct_002", "acct_003", "acct_004", "acct_005", "acct_006"]},
                        "req_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [10, 1200]},
                    },
                },
                "api_resp_429_aggressive": {
                    "lvl": "WARN",
                    "msg": "api response 429 op={op} acct={acct} req_id={req_id} policy=aggressive retry_after_ms={retry_after_ms}",
                    "vars": {
                        "op": {"k": "ch", "v": ["DescribeInstances", "DescribeVolumes", "DescribeImages"]},
                        "acct": {"k": "ch", "v": ["acct_001", "acct_002", "acct_003", "acct_004", "acct_005", "acct_006"]},
                        "req_id": {"k": "uuid", "v": None},
                        "retry_after_ms": {"k": "i", "v": [100, 2000]},
                    },
                },
                "api_resp_429_relaxed": {
                    "lvl": "WARN",
                    "msg": "api response 429 op={op} acct={acct} req_id={req_id} policy=relaxed retry_after_ms={retry_after_ms}",
                    "vars": {
                        "op": {"k": "ch", "v": ["DescribeInstances", "DescribeVolumes", "DescribeImages"]},
                        "acct": {"k": "ch", "v": ["acct_001", "acct_002", "acct_003", "acct_004", "acct_005", "acct_006"]},
                        "req_id": {"k": "uuid", "v": None},
                        "retry_after_ms": {"k": "i", "v": [100, 2000]},
                    },
                },
                "api_agg_1m": {
                    "lvl": "INFO",
                    "msg": "api agg 1m total={total} throttled={throttled} p95_ms={p95_ms}",
                    "vars": {
                        "total": {"k": "i", "v": [200, 2000]},
                        "throttled": {"k": "i", "v": [0, 600]},
                        "p95_ms": {"k": "i", "v": [40, 2500]},
                    },
                },
                "throttle_policy_set_aggressive": {"lvl": "WARN", "msg": "throttle policy set to aggressive reason=recovery", "vars": {}},
                "throttle_policy_set_relaxed": {"lvl": "WARN", "msg": "throttle policy set to relaxed reason=rollback", "vars": {}},
            },
            "beh": {
                "n": {"emit": [{"id": "api_agg_1m", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "api_agg_1m", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "ebs_failover_mgr",
            "svc": "ebs-control",
            "hosts": ["failover-1"],
            "logs": {
                "failover_task_enqueued": {
                    "lvl": "INFO",
                    "msg": "failover task enqueued vol=vol-{vol_hex} src={src_host} dst={dst_host} reason={reason}",
                    "vars": {
                        "vol_hex": {"k": "hex", "v": 8},
                        "src_host": {"k": "ch", "v": ["ebs-a1", "ebs-a2", "ebs-a3"]},
                        "dst_host": {"k": "ch", "v": ["ebs-a4", "ebs-a5", "ebs-a6"]},
                        "reason": {"k": "ch", "v": ["stuck_io", "degraded_perf"]},
                    },
                },
                "failover_metrics_1m": {
                    "lvl": "INFO",
                    "msg": "failover metrics stuck_vols={stuck_vols} failover_q={failover_q} healthy_hosts={healthy_hosts}",
                    "vars": {},
                    "state_vars": {
                        "n": {"stuck_vols": {"k": "i", "v": [0, 5]}, "failover_q": {"k": "i", "v": [0, 10]}, "healthy_hosts": {"k": "i", "v": [18, 25]}},
                        "f": {"stuck_vols": {"k": "i", "v": [10, 600]}, "failover_q": {"k": "i", "v": [5, 450]}, "healthy_hosts": {"k": "i", "v": [3, 22]}},
                    },
                },
                "failover_rate_limited": {
                    "lvl": "WARN",
                    "msg": "failover rate limited skipped={skipped} q_depth={q_depth}",
                    "vars": {"skipped": {"k": "i", "v": [5, 200]}, "q_depth": {"k": "i", "v": [30, 600]}},
                },
                "failover_rate_limit_reduced": {
                    "lvl": "WARN",
                    "msg": "updated failover rate limit old_per_min={old_per_min} new_per_min={new_per_min}",
                    "vars": {"old_per_min": {"k": "i", "v": [80, 140]}, "new_per_min": {"k": "i", "v": [10, 50]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "failover_task_enqueued", "per_min": 0.2, "scope": "global"}, {"id": "failover_metrics_1m", "per_min": 0.2, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "failover_task_enqueued", "per_min": 5.0, "scope": "global"},
                        {"id": "failover_metrics_1m", "per_min": 0.2, "scope": "global"},
                        {"id": "failover_rate_limited", "per_min": 0.05, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "ebs_storage",
            "svc": "ebs-data",
            "hosts": ["ebs-a1", "ebs-a2", "ebs-a3"],
            "logs": {
                "host_mem_sample": {
                    "lvl": "INFO",
                    "msg": "mem sample free_mb={free_mb} agent_rss_mb={agent_rss_mb} io_q_depth={io_q_depth}",
                    "vars": {"io_q_depth": {"k": "i", "v": [0, 250]}},
                    "state_vars": {
                        "n": {"free_mb": {"k": "i", "v": [4000, 16000]}, "agent_rss_mb": {"k": "i", "v": [40, 140]}},
                        "f": {"free_mb": {"k": "i", "v": [50, 3500]}, "agent_rss_mb": {"k": "i", "v": [300, 4500]}},
                    },
                },
                "low_free_mem": {
                    "lvl": "WARN",
                    "msg": "low free memory free_mb={free_mb} top_proc=collection-agent rss_mb={agent_rss_mb}",
                    "vars": {},
                    "state_vars": {
                        "n": {"free_mb": {"k": "i", "v": [1500, 4000]}, "agent_rss_mb": {"k": "i", "v": [80, 250]}},
                        "f": {"free_mb": {"k": "i", "v": [20, 1200]}, "agent_rss_mb": {"k": "i", "v": [800, 5000]}},
                    },
                },
                "agent_report_ok": {
                    "lvl": "INFO",
                    "msg": "collection-agent reported collector={collector} resolved_ip={resolved_ip} dur_ms={dur_ms} status=OK",
                    "vars": {"collector": {"k": "ch", "v": ["collector-b"]}, "resolved_ip": {"k": "ip", "v": "10.0.20.0/24"}, "dur_ms": {"k": "i", "v": [20, 400]}},
                },
                "agent_report_err": {
                    "lvl": "WARN",
                    "msg": "collection-agent failed collector={collector} resolved_ip={resolved_ip} err={err} retry_in_ms={retry_in_ms}",
                    "vars": {"err": {"k": "ch", "v": ["timeout", "conn_refused", "host_unreachable"]}, "retry_in_ms": {"k": "i", "v": [100, 5000]}},
                    "state_vars": {"n": {"collector": {"k": "ch", "v": ["collector-b"]}, "resolved_ip": {"k": "ip", "v": "10.0.20.0/24"}}, "f": {"collector": {"k": "ch", "v": ["collector-a"]}, "resolved_ip": {"k": "ip", "v": "10.0.10.0/24"}}},
                },
                "io_ok": {
                    "lvl": "INFO",
                    "msg": "volume io completed vol=vol-{vol_hex} op={op} bytes={bytes} dur_ms={dur_ms} req_id={req_id}",
                    "vars": {"vol_hex": {"k": "hex", "v": 8}, "op": {"k": "ch", "v": ["read", "write"]}, "bytes": {"k": "i", "v": [4096, 1048576]}, "req_id": {"k": "uuid", "v": None}},
                    "state_vars": {"n": {"dur_ms": {"k": "i", "v": [1, 25]}}, "f": {"dur_ms": {"k": "i", "v": [10, 500]}}},
                },
                "io_timeout": {
                    "lvl": "ERROR",
                    "msg": "volume io timeout vol=vol-{vol_hex} op={op} waited_ms={waited_ms} io_q_depth={io_q_depth} req_id={req_id}",
                    "vars": {"vol_hex": {"k": "hex", "v": 8}, "op": {"k": "ch", "v": ["read", "write"]}, "waited_ms": {"k": "i", "v": [500, 12000]}, "io_q_depth": {"k": "i", "v": [20, 400]}, "req_id": {"k": "uuid", "v": None}},
                },
                "agent_restart": {"lvl": "INFO", "msg": "ops restarted collection-agent pid={pid} freed_mb={freed_mb}", "vars": {"pid": {"k": "i", "v": [1000, 65000]}, "freed_mb": {"k": "i", "v": [200, 5000]}}},
            },
            "beh": {
                "n": {"emit": [{"id": "host_mem_sample", "per_min": 1.0}, {"id": "agent_report_ok", "per_min": 1.0}, {"id": "agent_report_err", "per_min": 0.02}, {"id": "low_free_mem", "per_min": 0.02}]},
                "f": {"emit": [{"id": "host_mem_sample", "per_min": 1.0}, {"id": "agent_report_ok", "per_min": 0.2}, {"id": "agent_report_err", "per_min": 1.0}, {"id": "low_free_mem", "per_min": 0.2}]},
            },
        },
        {
            "id": "internal_dns",
            "svc": "internal-dns",
            "hosts": ["dns-1", "dns-2"],
            "logs": {
                "dns_stats": {
                    "lvl": "INFO",
                    "msg": "dns stats queries_1m={queries_1m} nxdomain_1m={nxdomain_1m} stale_rrsets={stale_rrsets}",
                    "vars": {"queries_1m": {"k": "i", "v": [500, 5000]}, "nxdomain_1m": {"k": "i", "v": [0, 50]}},
                    "state_vars": {"n": {"stale_rrsets": {"k": "i", "v": [0, 2]}}, "f": {"stale_rrsets": {"k": "i", "v": [5, 80]}}},
                }
            },
            "beh": {"n": {"emit": [{"id": "dns_stats", "per_min": 0.5}]}, "f": {"emit": [{"id": "dns_stats", "per_min": 0.5}]}},
        },
        {
            "id": "fleet_collector",
            "svc": "fleet-collector",
            "hosts": ["collector-1"],
            "logs": {"ingest_ok": {"lvl": "INFO", "msg": "collector ingest ok reports_1m={reports_1m} lag_ms={lag_ms}", "vars": {"reports_1m": {"k": "i", "v": [600, 2200]}, "lag_ms": {"k": "i", "v": [10, 500]}}}},
            "beh": {"n": {"emit": [{"id": "ingest_ok", "per_min": 0.5, "scope": "global"}]}, "f": {"emit": [{"id": "ingest_ok", "per_min": 0.5, "scope": "global"}]}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {"id": "ebs_io_ok", "rpm": 400.0, "emit": ["ebs_storage.io_ok"], "latency_ms": [[3, 15]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "api_describe_ok", "rpm": 180.0, "emit": ["api_frontend.api_req_describe", "api_frontend.api_resp_200"], "latency_ms": [[2, 6], [25, 90]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "api_mutation_ok", "rpm": 40.0, "emit": ["api_frontend.api_req_mutation", "api_frontend.api_resp_200"], "latency_ms": [[2, 6], [40, 160]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
            ]
        },
        "f": {
            "req": [
                {"id": "ebs_io_ok", "rpm": 260.0, "emit": ["ebs_storage.io_ok"], "latency_ms": [[30, 180]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "ebs_io_timeout", "rpm": 30.0, "emit": ["ebs_storage.io_timeout"], "latency_ms": [[900, 8000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "api_describe_ok", "rpm": 160.0, "emit": ["api_frontend.api_req_describe", "api_frontend.api_resp_200"], "latency_ms": [[2, 6], [60, 250]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "api_describe_throttled_aggressive", "rpm": 0.05, "emit": ["api_frontend.api_req_describe", "api_frontend.api_resp_429_aggressive"], "latency_ms": [[2, 6], [5, 25]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "api_describe_throttled_relaxed", "rpm": 0.05, "emit": ["api_frontend.api_req_describe", "api_frontend.api_resp_429_relaxed"], "latency_ms": [[2, 6], [5, 25]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "api_mutation_ok", "rpm": 40.0, "emit": ["api_frontend.api_req_mutation", "api_frontend.api_resp_200"], "latency_ms": [[2, 6], [70, 320]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "us_east_az_ebs_memory_leak_and_api_throttling_2012_10_22"},
    "time": {"total_minutes": 60, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 60}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 25,
                    "rate_multipliers": {
                        "api_describe_throttled_aggressive": 0.0,
                        "api_describe_throttled_relaxed": 0.0,
                        "ebs_io_timeout": 0.8,
                        "ebs_failover_mgr.failover_task_enqueued": 0.3,
                        "ebs_failover_mgr.failover_metrics_1m": 0.0,
                        "ebs_failover_mgr.failover_rate_limited": 0.0,
                        "ebs_storage.low_free_mem": 0.6,
                    },
                    "latency_multipliers": {},
                    "one_shots": [],
                },
                {
                    "order": 2,
                    "at_min": 35,
                    "rate_multipliers": {"ebs_io_timeout": 2.5, "ebs_failover_mgr.failover_task_enqueued": 20.0, "ebs_failover_mgr.failover_metrics_1m": 5.0, "ebs_storage.low_free_mem": 4.0},
                    "latency_multipliers": {"ebs_io_ok": {"p50": 1.6, "p95": 2.0}, "api_describe_ok": {"p50": 1.3, "p95": 1.4}},
                    "one_shots": [],
                },
                {
                    "order": 3,
                    "at_min": 40,
                    "rate_multipliers": {"ebs_failover_mgr.failover_task_enqueued": 6.0, "ebs_io_timeout": 1.2, "ebs_failover_mgr.failover_rate_limited": 20.0},
                    "latency_multipliers": {"ebs_io_ok": {"p50": 1.2, "p95": 1.3}},
                    "one_shots": [{"ref": "ebs_failover_mgr.failover_rate_limit_reduced", "count": 1, "hosts": ["failover-1"]}],
                },
                {
                    "order": 4,
                    "at_min": 45,
                    "rate_multipliers": {"api_describe_throttled_aggressive": 1600.0, "api_describe_throttled_relaxed": 0.0, "api_describe_ok": 0.7},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "api_frontend.throttle_policy_set_aggressive", "count": 1, "hosts": ["api-1"]}],
                },
                {
                    "order": 5,
                    "at_min": 53,
                    "rate_multipliers": {"api_describe_throttled_aggressive": 0.0, "api_describe_throttled_relaxed": 320.0, "api_describe_ok": 0.9},
                    "latency_multipliers": {},
                    "one_shots": [{"ref": "api_frontend.throttle_policy_set_relaxed", "count": 1, "hosts": ["api-2"]}],
                },
                {
                    "order": 6,
                    "at_min": 55,
                    "rate_multipliers": {
                        "ebs_storage.agent_report_err": 0.4,
                        "ebs_storage.low_free_mem": 0.5,
                        "ebs_io_timeout": 0.6,
                        "ebs_failover_mgr.failover_task_enqueued": 1.5,
                        "ebs_failover_mgr.failover_rate_limited": 10.0,
                        "api_describe_ok": 0.95,
                    },
                    "latency_multipliers": {"ebs_io_ok": {"p50": 0.9, "p95": 0.9}, "api_describe_ok": {"p50": 0.95, "p95": 0.95}},
                    "one_shots": [{"ref": "ebs_storage.agent_restart", "count": 3, "hosts": ["ebs-a1", "ebs-a2", "ebs-a3"]}],
                },
            ]
        }
    },
}

GLOBAL_SEED = 1337
random.seed(GLOBAL_SEED)
np.random.seed(GLOBAL_SEED)

BASE_TIME = datetime(2012, 10, 22, 0, 0, 0, tzinfo=timezone.utc)


def stable_hash_int(s: str) -> int:
    h = hashlib.sha256(s.encode("utf-8")).digest()
    return int.from_bytes(h[:8], byteorder="big", signed=False)


def seed_from_key(key: str) -> int:
    return (stable_hash_int(key) ^ GLOBAL_SEED) & ((1 << 64) - 1)


def rng_for(key: str) -> np.random.Generator:
    return np.random.default_rng(seed_from_key(key))


def iso8601ms(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc)
    ms = int(dt.microsecond / 1000)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def lognormal_ms(p50: float, p95: float, key: str, soft_cap_mult: float = 3.0) -> float:
    p50 = max(0.1, float(p50))
    p95 = max(p50 * 1.05, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - mu) / 1.6448536269514722  # norminv(0.95)
    sigma = max(1e-6, sigma)
    rng = rng_for(key)
    v = float(rng.lognormal(mean=mu, sigma=sigma))
    cap = soft_cap_mult * p95
    v = min(v, cap)
    return max(1.0, v)


def sample_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom["k"]
    v = dom.get("v", None)
    r = rng_for(key)
    if k == "ch":
        vals = list(v)
        return vals[int(r.integers(0, len(vals)))]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return int(r.integers(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return float(lo + (hi - lo) * r.random())
    if k == "uuid":
        hi = int(r.integers(0, 1 << 64, dtype=np.uint64))
        lo = int(r.integers(0, 1 << 64, dtype=np.uint64))
        bits = (hi << 64) | lo
        return str(uuid.UUID(int=bits, version=4))
    if k == "hex":
        n = int(v)
        nbytes = (n + 1) // 2
        b = bytes(int(x) for x in r.integers(0, 256, size=nbytes, dtype=np.uint8))
        hx = b.hex()[:n]
        return hx
    if k == "ip":
        net = ipaddress.ip_network(v, strict=False)
        usable = max(1, net.num_addresses - 2)
        idx = int(r.integers(1, usable + 1))
        return str(net.network_address + idx)
    if k == "str":
        return f"{v or 's'}"
    raise ValueError(f"Unknown domain kind: {k}")


class DeterministicAllocator:
    def __init__(self):
        self.carry: DefaultDict[str, float] = defaultdict(float)

    def alloc(self, key: str, expected: float) -> int:
        x = float(expected) + self.carry[key]
        n = int(math.floor(x + 1e-12))
        self.carry[key] = x - n
        return n


@dataclass(frozen=True)
class LogRef:
    component_id: str
    log_id: str

    @property
    def key(self) -> str:
        return f"{self.component_id}.{self.log_id}"


def parse_logref(s: str) -> LogRef:
    comp, log_id = s.split(".", 1)
    return LogRef(comp, log_id)


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[Tuple[str, str], Dict[str, Any]]]:
    comps = {c["id"]: c for c in system["components"]}
    flows = {"n": {}, "f": {}}
    for st in ["n", "f"]:
        for f in system["flows"][st]["req"]:
            flows[st][f["id"]] = f
    templates: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for cid, c in comps.items():
        for lid, tmpl in c["logs"].items():
            templates[(cid, lid)] = tmpl
    return comps, flows, templates


COMPONENTS, FLOWS, TEMPLATES = build_indices(SYSTEM)


def get_domains(component_id: str, log_id: str, state: str) -> Dict[str, Dict[str, Any]]:
    tmpl = TEMPLATES[(component_id, log_id)]
    base = dict(tmpl.get("vars", {}) or {})
    sv = tmpl.get("state_vars", None)
    if sv and state in sv:
        for k, dom in (sv[state] or {}).items():
            base[k] = dom
    return base


def pick_component_host(component_id: str, key: str) -> str:
    c = COMPONENTS[component_id]
    hosts = c.get("hosts", []) or []
    if not hosts:
        return ""
    r = rng_for(key)
    return hosts[int(r.integers(0, len(hosts)))]


def schedule_times_in_minute(minute_start: datetime, count: int, key: str, guard_s: float = 0.0) -> List[datetime]:
    if count <= 0:
        return []
    duration = 60.0
    times: List[datetime] = []
    for i in range(count):
        r = rng_for(f"{key}|i={i}")
        base = (i + 0.5) / count * duration
        jitter = (r.random() - 0.5) * 0.4  # +/- 0.2s
        tsec = clamp(base + jitter, 0.0, duration - max(0.001, guard_s))
        ms_jitter = int(r.integers(0, 1000))
        dt = minute_start + timedelta(seconds=tsec, milliseconds=ms_jitter / 1000.0)
        times.append(dt)
    return times


def current_state_for_minute(minute: int) -> str:
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    return "n" if minute < n_end else "f"


def build_failure_controls() -> Dict[int, Dict[str, Any]]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    idx = 0

    rate_flow: Dict[str, float] = {}
    rate_bg: Dict[str, float] = {}
    lat_flow: Dict[str, Tuple[float, float]] = {}

    controls_by_min: Dict[int, Dict[str, Any]] = {}
    for m in range(f_start, f_end):
        while idx < len(events) and events[idx]["at_min"] <= m:
            ev = events[idx]
            for k, mult in (ev.get("rate_multipliers", {}) or {}).items():
                if "." in k:
                    rate_bg[k] = float(mult)
                else:
                    rate_flow[k] = float(mult)
            for fid, mm in (ev.get("latency_multipliers", {}) or {}).items():
                lat_flow[fid] = (float(mm.get("p50", 1.0)), float(mm.get("p95", 1.0)))
            idx += 1
        controls_by_min[m] = {"rate_flow": dict(rate_flow), "rate_bg": dict(rate_bg), "lat_flow": dict(lat_flow)}
    return controls_by_min


FAILURE_CONTROLS_BY_MIN = build_failure_controls()


def get_rate_multiplier(state: str, minute: int, source_key: str) -> float:
    if state != "f":
        return 1.0
    c = FAILURE_CONTROLS_BY_MIN.get(minute, None)
    if not c:
        return 1.0
    return float(c["rate_bg"].get(source_key, 1.0))


def get_flow_rate_multiplier(state: str, minute: int, flow_id: str) -> float:
    if state != "f":
        return 1.0
    c = FAILURE_CONTROLS_BY_MIN.get(minute, None)
    if not c:
        return 1.0
    return float(c["rate_flow"].get(flow_id, 1.0))


def get_flow_latency_multiplier(state: str, minute: int, flow_id: str) -> Tuple[float, float]:
    if state != "f":
        return (1.0, 1.0)
    c = FAILURE_CONTROLS_BY_MIN.get(minute, None)
    if not c:
        return (1.0, 1.0)
    return tuple(c["lat_flow"].get(flow_id, (1.0, 1.0)))


def severity_factor_for_storage(minute: int) -> float:
    if minute < 25:
        return 0.0
    if 25 <= minute < 35:
        return (minute - 25) / 10.0 * 0.45
    if 35 <= minute < 40:
        return 0.45 + (minute - 35) / 5.0 * 0.45
    if 40 <= minute < 55:
        return 0.90
    if 55 <= minute < 60:
        return 0.35 + (minute - 55) / 5.0 * 0.10
    return 0.35


def intensity_factor_for_failover(minute: int) -> float:
    if minute < 25:
        return 0.0
    c = FAILURE_CONTROLS_BY_MIN.get(minute, {})
    rm = float(c.get("rate_bg", {}).get("ebs_failover_mgr.failover_task_enqueued", 1.0))
    return float(clamp((rm - 0.3) / (20.0 - 0.3), 0.0, 1.0))


def render_message(component_id: str, log_id: str, state: str, key: str, overrides: Optional[Dict[str, Any]] = None) -> Tuple[str, str]:
    tmpl = TEMPLATES[(component_id, log_id)]
    domains = get_domains(component_id, log_id, state)
    vals: Dict[str, Any] = {}
    if overrides:
        vals.update(overrides)
    for var, dom in domains.items():
        if var in vals:
            continue
        vals[var] = sample_domain(dom, f"{key}|var={var}")
    msg = tmpl["msg"].format(**vals)
    lvl = tmpl["lvl"]
    return lvl, msg


def bind_background_vars(component_id: str, log_id: str, state: str, minute: int, host: str, idx: int, flow_counts: Dict[int, Dict[str, int]]) -> Dict[str, Any]:
    key = f"bg|{component_id}.{log_id}|st={state}|m={minute}|h={host}|i={idx}"
    if component_id == "ebs_storage" and log_id in ("host_mem_sample", "low_free_mem"):
        s = severity_factor_for_storage(minute)
        h_off = (stable_hash_int(host) % 97) / 96.0
        r = rng_for(key)
        if state == "n":
            free = int(clamp(12000 - h_off * 4000 + (r.random() - 0.5) * 600, 4000, 16000))
            rss = int(clamp(70 + h_off * 50 + (r.random() - 0.5) * 15, 40, 140))
            qd = int(clamp(5 + h_off * 20 + (r.random() - 0.5) * 6, 0, 250))
        else:
            free = int(clamp(3200 - s * 3000 - h_off * 200 + (r.random() - 0.5) * 180, 50, 3500))
            rss = int(clamp(450 + s * 3700 + h_off * 250 + (r.random() - 0.5) * 250, 300, 4500))
            qd = int(clamp(30 + s * 200 + h_off * 40 + (r.random() - 0.5) * 20, 0, 250))
        if log_id == "host_mem_sample":
            return {"free_mb": free, "agent_rss_mb": rss, "io_q_depth": qd}
        if state == "n":
            free2 = int(clamp(3500 - h_off * 1200 + (r.random() - 0.5) * 300, 1500, 4000))
            rss2 = int(clamp(120 + h_off * 90 + (r.random() - 0.5) * 20, 80, 250))
        else:
            free2 = int(clamp(900 - s * 800 - h_off * 120 + (r.random() - 0.5) * 120, 20, 1200))
            rss2 = int(clamp(1200 + s * 3200 + h_off * 300 + (r.random() - 0.5) * 250, 800, 5000))
        return {"free_mb": free2, "agent_rss_mb": rss2}

    if component_id == "internal_dns" and log_id == "dns_stats":
        r = rng_for(key)
        queries = int(clamp(1800 + (r.random() - 0.5) * 800, 500, 5000))
        nx = int(clamp(5 + (r.random() - 0.5) * 10, 0, 50))
        if state == "n":
            stale = int(clamp((r.random() * 2.2), 0, 2))
        else:
            s = severity_factor_for_storage(minute)
            host_bias = 0.6 if host.endswith("1") else 0.3
            stale = int(clamp(8 + s * 55 + host_bias * 8 + (r.random() - 0.5) * 8, 5, 80))
        return {"queries_1m": queries, "nxdomain_1m": nx, "stale_rrsets": stale}

    if component_id == "ebs_failover_mgr" and log_id == "failover_metrics_1m":
        r = rng_for(key)
        if state == "n":
            return {
                "stuck_vols": int(clamp(r.integers(0, 6), 0, 5)),
                "failover_q": int(clamp(r.integers(0, 11), 0, 10)),
                "healthy_hosts": int(clamp(22 + (r.random() - 0.5) * 4, 18, 25)),
            }
        inten = intensity_factor_for_failover(minute)
        stuck = int(clamp(10 + inten * 520 + (r.random() - 0.5) * 30, 10, 600))
        fq = int(clamp(5 + inten * 380 + (r.random() - 0.5) * 25, 5, 450))
        healthy = int(clamp(22 - inten * 17 + (r.random() - 0.5) * 2.5, 3, 22))
        return {"stuck_vols": stuck, "failover_q": fq, "healthy_hosts": healthy}

    if component_id == "api_frontend" and log_id == "api_agg_1m":
        r = rng_for(key)
        minute_flows = flow_counts.get(minute, {})
        total_api = int(
            minute_flows.get("api_describe_ok", 0)
            + minute_flows.get("api_mutation_ok", 0)
            + minute_flows.get("api_describe_throttled_aggressive", 0)
            + minute_flows.get("api_describe_throttled_relaxed", 0)
        )
        throttled_total = int(minute_flows.get("api_describe_throttled_aggressive", 0) + minute_flows.get("api_describe_throttled_relaxed", 0))
        host_idx = 0 if host.endswith("1") else 1
        total_host = int(clamp(total_api / 2.0 + (host_idx - 0.5) * 6 + (r.random() - 0.5) * 10, 200, 2000))
        throttled_host = int(clamp(throttled_total / 2.0 + (r.random() - 0.5) * 8, 0, 600))
        if state == "n":
            p95 = int(clamp(75 + (r.random() - 0.5) * 20, 40, 2500))
        else:
            lm_p50, lm_p95 = get_flow_latency_multiplier("f", minute, "api_describe_ok")
            p95_est = 250.0 * lm_p95
            p95 = int(clamp(p95_est + (r.random() - 0.5) * 60 + throttled_total * 0.7, 40, 2500))
        return {"total": total_host, "throttled": throttled_host, "p95_ms": p95}

    if component_id == "fleet_collector" and log_id == "ingest_ok":
        r = rng_for(key)
        reports = int(clamp(1500 + (r.random() - 0.5) * 500, 600, 2200))
        lag = int(clamp(60 + (r.random() - 0.5) * 80, 10, 500))
        return {"reports_1m": reports, "lag_ms": lag}

    if component_id == "ebs_failover_mgr" and log_id == "failover_rate_limited":
        r = rng_for(key)
        inten = intensity_factor_for_failover(minute)
        skipped = int(clamp(15 + inten * 120 + (r.random() - 0.5) * 30, 5, 200))
        qd = int(clamp(60 + inten * 420 + (r.random() - 0.5) * 60, 30, 600))
        return {"skipped": skipped, "q_depth": qd}

    if component_id == "ebs_storage" and log_id == "agent_report_err":
        r = rng_for(key)
        if state == "n":
            err = "timeout" if r.random() < 0.4 else "conn_refused"
            retry_in = int(clamp(800 + (r.random() - 0.5) * 400, 100, 5000))
            return {"err": err, "retry_in_ms": retry_in}
        s = severity_factor_for_storage(minute)
        if s > 0.75:
            err = "timeout"
        elif s > 0.35:
            err = "host_unreachable" if r.random() < 0.5 else "timeout"
        else:
            err = "conn_refused"
        retry_in = int(clamp(400 + s * 1800 + (r.random() - 0.5) * 250, 100, 5000))
        return {"err": err, "retry_in_ms": retry_in}

    return {}


def sample_flow_delays(flow: Dict[str, Any], state: str, minute: int, inst_key: str) -> List[int]:
    lm_p50, lm_p95 = get_flow_latency_multiplier(state, minute, flow["id"])
    delays: List[int] = []
    for j, (p50, p95) in enumerate(flow.get("latency_ms", []) or []):
        sp50 = float(p50) * lm_p50
        sp95 = float(p95) * lm_p95
        sp95 = max(sp95, sp50 * 1.05)
        d = lognormal_ms(sp50, sp95, f"{inst_key}|delay={j}")
        delays.append(int(round(d)))
    return delays


def simulate_flow_instance(
    start_dt: datetime,
    state: str,
    minute: int,
    flow_id: str,
    inst_idx: int,
    rows: List[Dict[str, Any]],
) -> None:
    flow = FLOWS[state][flow_id]
    inst_key = f"flow|st={state}|m={minute}|fid={flow_id}|i={inst_idx}"
    trace_id = ""

    emit_refs = [parse_logref(x) for x in flow["emit"]]
    n_logs = len(emit_refs)

    # Component-local host stickiness within this chain
    comp_host: Dict[str, str] = {}
    for ref in emit_refs:
        if ref.component_id not in comp_host:
            comp_host[ref.component_id] = pick_component_host(ref.component_id, f"{inst_key}|host|cid={ref.component_id}")

    ctx: Dict[str, Any] = {}

    # Bind coherent ids/fields for the chain
    if flow_id.startswith("api_"):
        if "describe" in flow_id:
            ops = COMPONENTS["api_frontend"]["logs"]["api_req_describe"]["vars"]["op"]["v"]
        else:
            ops = COMPONENTS["api_frontend"]["logs"]["api_req_mutation"]["vars"]["op"]["v"]
        ctx["op"] = ops[int(rng_for(f"{inst_key}|op").integers(0, len(ops)))]
        accts = COMPONENTS["api_frontend"]["logs"]["api_req_describe"]["vars"]["acct"]["v"]
        ctx["acct"] = accts[int(rng_for(f"{inst_key}|acct").integers(0, len(accts)))]
        ctx["req_id"] = sample_domain({"k": "uuid", "v": None}, f"{inst_key}|req_id")
        if "throttled" in flow_id:
            ctx["retry_after_ms"] = int(clamp(250 + rng_for(f"{inst_key}|retry_after").random() * 1400, 100, 2000))
    elif flow_id.startswith("ebs_io_"):
        ctx["vol_hex"] = sample_domain({"k": "hex", "v": 8}, f"{inst_key}|vol")
        ops = ["read", "write"]
        ctx["op"] = ops[int(rng_for(f"{inst_key}|op").integers(0, 2))]
        ctx["req_id"] = sample_domain({"k": "uuid", "v": None}, f"{inst_key}|req_id")
        if flow_id == "ebs_io_ok":
            ctx["bytes"] = int(clamp(4096 * (2 ** int(rng_for(f"{inst_key}|bytes_pow").integers(0, 9))), 4096, 1048576))

    delays = sample_flow_delays(flow, state, minute, inst_key)

    offsets_ms: List[int] = []
    if n_logs <= 0:
        return
    if n_logs == 1:
        op_ms = delays[0] if delays else 0
        offsets_ms = [int(max(0, op_ms))]
    else:
        cum = 0
        offsets_ms = [0]
        for j in range(1, n_logs):
            gap = delays[j] if j < len(delays) else 0
            gap = int(max(0, gap))
            cum += gap
            offsets_ms.append(cum)

    end_ms = int(offsets_ms[-1])

    # Bind observed timing fields to the planned chronology (message values agree with emitted gaps).
    if flow_id in ("api_describe_ok", "api_mutation_ok"):
        # IMPORTANT: dur_ms must agree with the request->response timestamp gap.
        # We clamp the bound value, then set the final offset to that same value.
        dur = int(clamp(end_ms, 10, 1200))
        ctx["dur_ms"] = dur
        offsets_ms[-1] = dur
        # Defensive monotonicity in case of future multi-log API flows
        for k in range(1, len(offsets_ms)):
            if offsets_ms[k] < offsets_ms[k - 1]:
                offsets_ms[k] = offsets_ms[k - 1]

    if flow_id == "ebs_io_ok":
        d = int(clamp(end_ms, 1, 25)) if state == "n" else int(clamp(end_ms, 10, 500))
        offsets_ms[0] = d
        ctx["dur_ms"] = d
    if flow_id == "ebs_io_timeout":
        d = int(clamp(end_ms if end_ms > 0 else (delays[0] if delays else 1000), 500, 12000))
        offsets_ms[0] = d
        ctx["waited_ms"] = d
        qd = int(clamp(20 + (d - 500) / (12000 - 500) * 380, 20, 400))
        qd += int((rng_for(f"{inst_key}|qd").random() - 0.5) * 30)
        ctx["io_q_depth"] = int(clamp(qd, 20, 400))

    for j, ref in enumerate(emit_refs):
        ts = start_dt + timedelta(milliseconds=int(offsets_ms[j]))
        lvl, msg = render_message(ref.component_id, ref.log_id, state, f"{inst_key}|emit={j}", overrides=ctx)
        rows.append(
            {
                "_ts": ts,
                "timestamp": "",
                "level": lvl,
                "message": msg,
                "trace_id": trace_id,
                "service": COMPONENTS[ref.component_id].get("svc", "") or "",
                "host": comp_host.get(ref.component_id, "") or "",
            }
        )


def simulate_background_for_minute(
    minute: int,
    state: str,
    allocator: DeterministicAllocator,
    rows: List[Dict[str, Any]],
    flow_counts: Dict[int, Dict[str, int]],
) -> None:
    minute_start = BASE_TIME + timedelta(minutes=minute)
    for cid, comp in COMPONENTS.items():
        beh = comp.get("beh", {}).get(state, {})
        for emit in beh.get("emit", []) or []:
            log_id = emit["id"]
            per_min = float(emit["per_min"])
            scope = emit.get("scope", "per_host")
            mult = get_rate_multiplier(state, minute, f"{cid}.{log_id}")
            eff = per_min * mult
            if eff <= 0.0:
                continue

            if scope == "global":
                key = f"bgcount|st={state}|m={minute}|cid={cid}|lid={log_id}|scope=global"
                n = allocator.alloc(key, eff)
                times = schedule_times_in_minute(minute_start, n, f"bgsched|{key}")
                hosts = comp.get("hosts", []) or []
                for i, ts in enumerate(times):
                    host = hosts[i % len(hosts)] if hosts else ""
                    overrides = bind_background_vars(cid, log_id, state, minute, host, i, flow_counts)
                    lvl, msg = render_message(cid, log_id, state, f"{key}|emit={i}", overrides=overrides)
                    rows.append(
                        {
                            "_ts": ts,
                            "timestamp": "",
                            "level": lvl,
                            "message": msg,
                            "trace_id": "",
                            "service": comp.get("svc", "") or "",
                            "host": host,
                        }
                    )
            else:
                hosts = comp.get("hosts", []) or []
                for h in hosts:
                    key = f"bgcount|st={state}|m={minute}|cid={cid}|lid={log_id}|h={h}|scope=per_host"
                    n = allocator.alloc(key, eff)
                    times = schedule_times_in_minute(minute_start, n, f"bgsched|{key}")
                    for i, ts in enumerate(times):
                        overrides = bind_background_vars(cid, log_id, state, minute, h, i, flow_counts)
                        lvl, msg = render_message(cid, log_id, state, f"{key}|emit={i}", overrides=overrides)
                        rows.append(
                            {
                                "_ts": ts,
                                "timestamp": "",
                                "level": lvl,
                                "message": msg,
                                "trace_id": "",
                                "service": comp.get("svc", "") or "",
                                "host": h,
                            }
                        )


def plan_flow_counts() -> Dict[int, Dict[str, int]]:
    allocator = DeterministicAllocator()
    total_minutes = SCENARIO["time"]["total_minutes"]
    flow_counts: Dict[int, Dict[str, int]] = {}

    for minute in range(total_minutes):
        state = current_state_for_minute(minute)
        minute_counts: Dict[str, int] = {}
        for fid, flow in FLOWS[state].items():
            base_rpm = float(flow["rpm"])
            mult = get_flow_rate_multiplier(state, minute, fid)
            eff = base_rpm * mult
            if eff <= 0.0:
                minute_counts[fid] = 0
                continue
            key = f"flowcount|st={state}|m={minute}|fid={fid}"
            n = allocator.alloc(key, eff)
            minute_counts[fid] = n
        flow_counts[minute] = minute_counts
    return flow_counts


def simulate_all() -> pd.DataFrame:
    total_minutes = SCENARIO["time"]["total_minutes"]
    flow_counts = plan_flow_counts()

    rows: List[Dict[str, Any]] = []
    bg_allocator = DeterministicAllocator()

    for minute in range(total_minutes):
        state = current_state_for_minute(minute)
        simulate_background_for_minute(minute, state, bg_allocator, rows, flow_counts)

    for minute in range(total_minutes):
        state = current_state_for_minute(minute)
        minute_start = BASE_TIME + timedelta(minutes=minute)
        for fid, count in flow_counts[minute].items():
            if count <= 0:
                continue
            guard_s = 0.0
            if fid == "ebs_io_timeout":
                guard_s = 10.0
            starts = schedule_times_in_minute(minute_start, count, f"flowsched|st={state}|m={minute}|fid={fid}", guard_s=guard_s)
            for i, st_dt in enumerate(starts):
                simulate_flow_instance(st_dt, state, minute, fid, i, rows)

    for ev in SCENARIO["phases"]["f"]["events"]:
        at_min = int(ev["at_min"])
        event_time = BASE_TIME + timedelta(minutes=at_min)
        for os in ev.get("one_shots", []) or []:
            ref = parse_logref(os["ref"])
            cnt = int(os["count"])
            allowed_hosts = list(os.get("hosts", []) or [])
            for i in range(cnt):
                r = rng_for(f"oneshot|{os['ref']}|m={at_min}|i={i}")
                offset_s = float(clamp(r.random() * 6.0, 0.0, 6.0))
                ts = event_time + timedelta(seconds=offset_s, milliseconds=int(r.integers(0, 1000)))
                if allowed_hosts:
                    host = allowed_hosts[i % len(allowed_hosts)]
                else:
                    host = pick_component_host(ref.component_id, f"oneshot|{os['ref']}|m={at_min}|i={i}|host")
                overrides: Dict[str, Any] = {}
                if ref.component_id == "ebs_failover_mgr" and ref.log_id == "failover_rate_limit_reduced":
                    rr = rng_for(f"oneshot|rate_limit_reduced|m={at_min}")
                    old = int(clamp(120 + (rr.random() - 0.5) * 20, 80, 140))
                    new = int(clamp(30 + (rr.random() - 0.5) * 12, 10, 50))
                    if new >= old:
                        new = max(10, old - 40)
                    overrides = {"old_per_min": old, "new_per_min": new}
                lvl, msg = render_message(ref.component_id, ref.log_id, "f", f"oneshot|{os['ref']}|m={at_min}|i={i}", overrides=overrides)
                rows.append(
                    {
                        "_ts": ts,
                        "timestamp": "",
                        "level": lvl,
                        "message": msg,
                        "trace_id": "",
                        "service": COMPONENTS[ref.component_id].get("svc", "") or "",
                        "host": host,
                    }
                )

    for idx, row in enumerate(rows):
        row["_seq"] = idx
        row["timestamp"] = iso8601ms(row["_ts"])

    df = pd.DataFrame(rows)
    df = df.sort_values(by=["_ts", "_seq"], ascending=True).reset_index(drop=True)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    if not (20000 <= len(df) <= 100000):
        raise RuntimeError(f"Row count {len(df)} outside target [20000, 100000].")
    if not df["timestamp"].is_monotonic_increasing:
        raise RuntimeError("Timestamps are not sorted ascending.")
    return df


def main() -> None:
    df = simulate_all()
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
