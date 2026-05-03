import hashlib
import math
import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from string import Formatter
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# -----------------------------
# Embedded executable spec (normalized)
# -----------------------------

SYSTEM: Dict[str, Any] = {
    "id": "ebs_ec2_rds_useast_ebs_stuck_2011",
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["ec2_hypervisor", "ebs_control_plane"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "net_ops",
            "svc": None,
            "hosts": ["netops-1"],
            "logs": {
                "change_start": {
                    "lvl": "INFO",
                    "msg": "Applying network change {change_id} on {router} in {az}",
                    "vars": {"change_id": {"k": "hex", "v": 12}, "router": {"k": "ch", "v": ["rtr-a", "rtr-b"]}, "az": {"k": "ch", "v": ["use1a"]}},
                },
                "misroute_detected": {
                    "lvl": "ERROR",
                    "msg": "Unexpected traffic shift: primary traffic routed to replication network; affected_nodes={affected_nodes}",
                    "vars": {"affected_nodes": {"k": "i", "v": [50, 250]}},
                },
                "rollback_complete": {
                    "lvl": "WARN",
                    "msg": "Rollback completed; primary routing restored; duration_s={duration_s}",
                    "vars": {"duration_s": {"k": "i", "v": [30, 240]}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "ec2_hypervisor",
            "svc": "ec2",
            "hosts": ["ip-10-0-1-10", "ip-10-0-1-11"],
            "logs": {
                "io_submit": {
                    "lvl": "DEBUG",
                    "msg": "Submitting EBS I/O req={req_id} vol={vol_id} op={op}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "vol_id": {"k": "uuid", "v": None}, "op": {"k": "ch", "v": ["READ", "WRITE"]}},
                },
                "io_timeout": {
                    "lvl": "ERROR",
                    "msg": "EBS I/O timeout req={req_id} vol={vol_id} waited_ms={waited_ms}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "vol_id": {"k": "uuid", "v": None}, "waited_ms": {"k": "i", "v": [500, 20000]}},
                },
                "io_health": {
                    "lvl": "INFO",
                    "msg": "io_health stuck_ios={stuck_ios} p95_wait_ms={p95_wait_ms} affected_instances={affected_instances}",
                    "vars": {},
                    "state_vars": {
                        "n": {"stuck_ios": {"k": "i", "v": [0, 5]}, "p95_wait_ms": {"k": "i", "v": [2, 25]}, "affected_instances": {"k": "i", "v": [0, 1]}},
                        "f": {"stuck_ios": {"k": "i", "v": [20, 400]}, "p95_wait_ms": {"k": "i", "v": [200, 15000]}, "affected_instances": {"k": "i", "v": [1, 200]}},
                    },
                },
            },
            "beh": {"n": {"emit": [{"id": "io_health", "per_min": 0.5}]}, "f": {"emit": [{"id": "io_health", "per_min": 1.0}]}},
        },
        {
            "id": "ebs_node_cluster",
            "svc": "ebs-data",
            "hosts": ["ebs-node-01", "ebs-node-02", "ebs-node-03"],
            "logs": {
                "io_start": {
                    "lvl": "DEBUG",
                    "msg": "I/O request {req_id} vol={vol_id} op={op} bytes={bytes}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "vol_id": {"k": "uuid", "v": None}, "op": {"k": "ch", "v": ["READ", "WRITE"]}, "bytes": {"k": "i", "v": [4096, 1048576]}},
                },
                "io_complete": {
                    "lvl": "INFO",
                    "msg": "I/O request {req_id} vol={vol_id} op={op} status=OK latency_ms={latency_ms}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "vol_id": {"k": "uuid", "v": None}, "op": {"k": "ch", "v": ["READ", "WRITE"]}, "latency_ms": {"k": "i", "v": [1, 2000]}},
                },
                "io_blocked_remirror": {"lvl": "WARN", "msg": "I/O blocked vol={vol_id} reason=REMIRROR_PRIMARY_ELECTION waited_ms={waited_ms}", "vars": {"vol_id": {"k": "uuid", "v": None}, "waited_ms": {"k": "i", "v": [200, 20000]}}},
                "cluster_health": {
                    "lvl": "INFO",
                    "msg": "cluster_health stuck_vol_pct={stuck_vol_pct} remirror_q={remirror_q} free_gb={free_gb} node_crashes_5m={node_crashes_5m}",
                    "vars": {},
                    "state_vars": {
                        "n": {"stuck_vol_pct": {"k": "f", "v": [0.0, 0.5]}, "remirror_q": {"k": "i", "v": [0, 30]}, "free_gb": {"k": "i", "v": [5000, 20000]}, "node_crashes_5m": {"k": "i", "v": [0, 1]}},
                        "f": {"stuck_vol_pct": {"k": "f", "v": [10.0, 20.0]}, "remirror_q": {"k": "i", "v": [500, 6000]}, "free_gb": {"k": "i", "v": [0, 1500]}, "node_crashes_5m": {"k": "i", "v": [0, 40]}},
                    },
                },
                "remirror_activity": {
                    "lvl": "INFO",
                    "msg": "remirror_activity searches_per_s={searches_per_s} elections_per_s={elections_per_s}",
                    "vars": {},
                    "state_vars": {"n": {"searches_per_s": {"k": "i", "v": [0, 5]}, "elections_per_s": {"k": "i", "v": [0, 3]}}, "f": {"searches_per_s": {"k": "i", "v": [50, 1200]}, "elections_per_s": {"k": "i", "v": [20, 600]}}},
                },
                "node_crash_race": {"lvl": "ERROR", "msg": "Node crash during replication request close; crash_id={crash_id} open_conns={open_conns}", "vars": {"crash_id": {"k": "hex", "v": 10}, "open_conns": {"k": "i", "v": [100, 20000]}}},
                "remirror_throttle_enabled": {"lvl": "WARN", "msg": "Updated remirror backoff policy mode={mode} base_ms={base_ms} max_ms={max_ms}", "vars": {"mode": {"k": "ch", "v": ["aggressive_backoff"]}, "base_ms": {"k": "i", "v": [200, 2000]}, "max_ms": {"k": "i", "v": [5000, 60000]}}},
            },
            "beh": {
                "n": {"emit": [{"id": "cluster_health", "per_min": 0.5}, {"id": "remirror_activity", "per_min": 0.2}]},
                "f": {"emit": [{"id": "cluster_health", "per_min": 1.0}, {"id": "remirror_activity", "per_min": 1.0}, {"id": "node_crash_race", "per_min": 0.02}]},
            },
        },
        {
            "id": "ebs_control_plane",
            "svc": "ebs-control",
            "hosts": ["ebs-cp-01", "ebs-cp-02"],
            "logs": {
                "create_volume_req": {"lvl": "INFO", "msg": "CreateVolume request_id={req_id} az={az} size_gb={size_gb}", "vars": {"req_id": {"k": "hex", "v": 16}, "az": {"k": "ch", "v": ["use1a", "use1b", "use1c"]}, "size_gb": {"k": "i", "v": [1, 1024]}}},
                "create_volume_ok": {"lvl": "INFO", "msg": "CreateVolume request_id={req_id} status=200 vol={vol_id} latency_ms={latency_ms}", "vars": {"req_id": {"k": "hex", "v": 16}, "vol_id": {"k": "uuid", "v": None}, "latency_ms": {"k": "i", "v": [20, 5000]}}},
                "create_volume_err": {"lvl": "ERROR", "msg": "CreateVolume request_id={req_id} status={status} err={err} latency_ms={latency_ms}", "vars": {"req_id": {"k": "hex", "v": 16}, "status": {"k": "ch", "v": ["503", "504"]}, "err": {"k": "ch", "v": ["UpstreamTimeout"]}, "latency_ms": {"k": "i", "v": [200, 90000]}}},
                "create_volume_reject_disabled": {"lvl": "ERROR", "msg": "CreateVolume request_id={req_id} status=403 err=ApiDisabledInAz latency_ms={latency_ms}", "vars": {"req_id": {"k": "hex", "v": 16}, "latency_ms": {"k": "i", "v": [10, 1500]}}},
                "attach_volume_req": {"lvl": "INFO", "msg": "AttachVolume request_id={req_id} instance={instance_id} vol={vol_id} az={az}", "vars": {"req_id": {"k": "hex", "v": 16}, "instance_id": {"k": "str", "v": "i-[a-f0-9]{8,17}"}, "vol_id": {"k": "uuid", "v": None}, "az": {"k": "ch", "v": ["use1a", "use1b", "use1c"]}}},
                "attach_volume_ok": {"lvl": "INFO", "msg": "AttachVolume request_id={req_id} status=200 latency_ms={latency_ms}", "vars": {"req_id": {"k": "hex", "v": 16}, "latency_ms": {"k": "i", "v": [20, 20000]}}},
                "attach_volume_err": {"lvl": "ERROR", "msg": "AttachVolume request_id={req_id} status={status} err={err} latency_ms={latency_ms}", "vars": {"req_id": {"k": "hex", "v": 16}, "status": {"k": "ch", "v": ["500", "503"]}, "err": {"k": "ch", "v": ["ThreadPoolExhausted", "UpstreamTimeout"]}, "latency_ms": {"k": "i", "v": [200, 60000]}}},
                "elect_primary_req": {"lvl": "INFO", "msg": "ElectPrimary election_id={election_id} vol={vol_id} az={az}", "vars": {"election_id": {"k": "hex", "v": 16}, "vol_id": {"k": "uuid", "v": None}, "az": {"k": "ch", "v": ["use1a"]}}},
                "elect_primary_ok": {"lvl": "INFO", "msg": "ElectPrimary election_id={election_id} status=OK latency_ms={latency_ms}", "vars": {"election_id": {"k": "hex", "v": 16}, "latency_ms": {"k": "i", "v": [10, 20000]}}},
                "elect_primary_err": {"lvl": "ERROR", "msg": "ElectPrimary election_id={election_id} status={status} err={err} latency_ms={latency_ms}", "vars": {"election_id": {"k": "hex", "v": 16}, "status": {"k": "ch", "v": ["403", "504"]}, "err": {"k": "ch", "v": ["ClusterPartitioned", "UpstreamTimeout"]}, "latency_ms": {"k": "i", "v": [200, 20000]}}},
                "thread_pool_saturated": {"lvl": "ERROR", "msg": "thread_pool_saturated active={active} queued={queued} pool={pool}", "vars": {"active": {"k": "i", "v": [100, 2000]}, "queued": {"k": "i", "v": [500, 50000]}, "pool": {"k": "i", "v": [200, 2000]}}},
                "cp_metrics": {
                    "lvl": "INFO",
                    "msg": "cp_metrics api_5xx_rate={api_5xx_rate} p95_ms={p95_ms} active_threads={active_threads} queued={queued}",
                    "vars": {},
                    "state_vars": {
                        "n": {"api_5xx_rate": {"k": "f", "v": [0.0, 0.5]}, "p95_ms": {"k": "i", "v": [20, 200]}, "active_threads": {"k": "i", "v": [50, 200]}, "queued": {"k": "i", "v": [0, 200]}},
                        "f": {"api_5xx_rate": {"k": "f", "v": [1.0, 60.0]}, "p95_ms": {"k": "i", "v": [100, 60000]}, "active_threads": {"k": "i", "v": [200, 2000]}, "queued": {"k": "i", "v": [100, 50000]}},
                    },
                },
                "create_volume_disabled": {"lvl": "WARN", "msg": "CreateVolume disabled in {az} mode={mode}", "vars": {"az": {"k": "ch", "v": ["use1a"]}, "mode": {"k": "ch", "v": ["reject_new"]}}},
                "cluster_comm_blocked": {"lvl": "WARN", "msg": "Blocked control-plane comms to cluster {az} reason={reason}", "vars": {"az": {"k": "ch", "v": ["use1a"]}, "reason": {"k": "ch", "v": ["isolate_degraded_cluster"]}}},
                "partition_status": {"lvl": "INFO", "msg": "partition_status az={az} blocked={blocked}", "vars": {"az": {"k": "ch", "v": ["use1a"]}, "blocked": {"k": "ch", "v": ["true"]}}},
            },
            "beh": {"n": {"emit": [{"id": "cp_metrics", "per_min": 1.0}]}, "f": {"emit": [{"id": "cp_metrics", "per_min": 1.0}, {"id": "thread_pool_saturated", "per_min": 2.0}, {"id": "partition_status", "per_min": 0.5}]}},
        },
        {
            "id": "rds_service",
            "svc": "rds",
            "hosts": ["rds-ctl-01"],
            "logs": {
                "txn_commit": {"lvl": "INFO", "msg": "txn_commit db={db_id} latency_ms={latency_ms}", "vars": {"db_id": {"k": "str", "v": "db-[a-z0-9]{6}"}, "latency_ms": {"k": "i", "v": [5, 2000]}}},
                "db_io_wait": {"lvl": "WARN", "msg": "db_io_wait db={db_id} vol={vol_id} waited_ms={waited_ms}", "vars": {"db_id": {"k": "str", "v": "db-[a-z0-9]{6}"}, "vol_id": {"k": "uuid", "v": None}, "waited_ms": {"k": "i", "v": [200, 20000]}}},
                "txn_error": {"lvl": "ERROR", "msg": "txn_error db={db_id} err=StorageIOStuck waited_ms={waited_ms}", "vars": {"db_id": {"k": "str", "v": "db-[a-z0-9]{6}"}, "waited_ms": {"k": "i", "v": [500, 30000]}}},
                "failover_blocked": {"lvl": "ERROR", "msg": "automatic_failover_blocked db={db_id} reason={reason}", "vars": {"db_id": {"k": "str", "v": "db-[a-z0-9]{6}"}, "reason": {"k": "ch", "v": ["safety_check_failed_after_partition_and_io_stall"]}}},
                "rds_health": {
                    "lvl": "INFO",
                    "msg": "rds_health stuck_pct={stuck_pct} failovers_blocked_5m={failovers_blocked_5m}",
                    "vars": {},
                    "state_vars": {"n": {"stuck_pct": {"k": "f", "v": [0.0, 0.5]}, "failovers_blocked_5m": {"k": "i", "v": [0, 0]}}, "f": {"stuck_pct": {"k": "f", "v": [5.0, 50.0]}, "failovers_blocked_5m": {"k": "i", "v": [0, 5]}}},
                },
            },
            "beh": {"n": {"emit": [{"id": "rds_health", "per_min": 0.5}]}, "f": {"emit": [{"id": "rds_health", "per_min": 1.0}]}},
        },
    ],
    "flows": {
        "n": [
            {"id": "ec2_volume_io_ok", "rpm": 300, "emit": ["ec2_hypervisor.io_submit", "ebs_node_cluster.io_start", "ebs_node_cluster.io_complete"], "latency_ms": [[0, 1], [0, 2], [2, 8]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "ebs_api_create_volume_ok", "rpm": 15, "emit": ["ebs_control_plane.create_volume_req", "ebs_control_plane.create_volume_ok"], "latency_ms": [[0, 2], [50, 250]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "ebs_api_attach_volume_ok", "rpm": 25, "emit": ["ebs_control_plane.attach_volume_req", "ebs_control_plane.attach_volume_ok"], "latency_ms": [[0, 2], [40, 200]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "ebs_elect_primary_ok", "rpm": 20, "emit": ["ebs_control_plane.elect_primary_req", "ebs_control_plane.elect_primary_ok"], "latency_ms": [[0, 2], [20, 120]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
            {"id": "rds_txn_ok", "rpm": 60, "emit": ["rds_service.txn_commit"], "latency_ms": [[10, 80]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
        ],
        "f": [
            {"id": "ec2_volume_io_ok", "rpm": 240, "emit": ["ec2_hypervisor.io_submit", "ebs_node_cluster.io_start", "ebs_node_cluster.io_complete"], "latency_ms": [[0, 1], [0, 2], [4, 40]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "ec2_volume_io_stuck", "rpm": 60, "emit": ["ec2_hypervisor.io_submit", "ebs_node_cluster.io_start", "ebs_node_cluster.io_blocked_remirror", "ec2_hypervisor.io_timeout"], "latency_ms": [[0, 1], [0, 2], [500, 8000], [1000, 20000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "ebs_api_create_volume_timeout", "rpm": 10, "emit": ["ebs_control_plane.create_volume_req", "ebs_control_plane.create_volume_err"], "latency_ms": [[0, 2], [10000, 90000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "ebs_api_create_volume_disabled_fast", "rpm": 10, "emit": ["ebs_control_plane.create_volume_req", "ebs_control_plane.create_volume_reject_disabled"], "latency_ms": [[0, 2], [15, 250]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "ebs_api_attach_volume_ok_slow", "rpm": 14, "emit": ["ebs_control_plane.attach_volume_req", "ebs_control_plane.attach_volume_ok"], "latency_ms": [[0, 2], [200, 4000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "ebs_api_attach_volume_err", "rpm": 6, "emit": ["ebs_control_plane.attach_volume_req", "ebs_control_plane.attach_volume_err"], "latency_ms": [[0, 2], [500, 60000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            {"id": "ebs_elect_primary_ok_load", "rpm": 90, "emit": ["ebs_control_plane.elect_primary_req", "ebs_control_plane.elect_primary_ok"], "latency_ms": [[0, 2], [200, 8000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
            {"id": "ebs_elect_primary_err_blocked", "rpm": 30, "emit": ["ebs_control_plane.elect_primary_req", "ebs_control_plane.elect_primary_err"], "latency_ms": [[0, 2], [200, 5000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
            {"id": "rds_txn_stuck", "rpm": 30, "emit": ["rds_service.db_io_wait", "rds_service.txn_error"], "latency_ms": [[200, 8000], [500, 15000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "id": "useast_2011_ebs_remirror_storm_control_plane_brownout",
    "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 50}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {"ebs_api_create_volume_disabled_fast": 0.0, "ebs_control_plane.partition_status": 0.0, "ebs_control_plane.thread_pool_saturated": 0.8, "ebs_elect_primary_err_blocked": 0.0},
                    "latency_multipliers": {"ebs_api_create_volume_timeout": {"p50": 1.2, "p95": 1.2}},
                    "one_shots": [{"ref": "net_ops.change_start", "count": 1, "hosts": ["netops-1"]}, {"ref": "net_ops.misroute_detected", "count": 1, "hosts": ["netops-1"]}, {"ref": "net_ops.rollback_complete", "count": 1, "hosts": ["netops-1"]}],
                },
                {
                    "order": 2,
                    "at_min": 28,
                    "rate_multipliers": {"ebs_api_create_volume_timeout": 0.0, "ebs_api_create_volume_disabled_fast": 1.0, "ebs_control_plane.thread_pool_saturated": 0.2, "ebs_api_attach_volume_err": 0.3},
                    "latency_multipliers": {"ebs_api_attach_volume_ok_slow": {"p50": 0.7, "p95": 0.7}},
                    "one_shots": [{"ref": "ebs_control_plane.create_volume_disabled", "count": 1, "hosts": ["ebs-cp-01"]}],
                },
                {
                    "order": 3,
                    "at_min": 36,
                    "rate_multipliers": {"ec2_volume_io_stuck": 1.5, "ebs_elect_primary_ok_load": 1.8, "ebs_node_cluster.node_crash_race": 10.0, "ebs_control_plane.thread_pool_saturated": 1.3, "ebs_api_attach_volume_err": 1.5},
                    "latency_multipliers": {"ebs_elect_primary_ok_load": {"p50": 1.4, "p95": 1.4}},
                    "one_shots": [{"ref": "rds_service.failover_blocked", "count": 2, "hosts": ["rds-ctl-01"]}],
                },
                {
                    "order": 4,
                    "at_min": 42,
                    "rate_multipliers": {"ebs_elect_primary_ok_load": 0.0, "ebs_elect_primary_err_blocked": 1.0, "ebs_control_plane.thread_pool_saturated": 0.1, "ebs_control_plane.partition_status": 1.0, "ebs_api_attach_volume_err": 0.2},
                    "latency_multipliers": {"ebs_api_attach_volume_ok_slow": {"p50": 0.6, "p95": 0.6}},
                    "one_shots": [{"ref": "ebs_control_plane.cluster_comm_blocked", "count": 1, "hosts": ["ebs-cp-02"]}],
                },
                {"order": 5, "at_min": 46, "rate_multipliers": {"ebs_node_cluster.remirror_activity": 0.4, "ebs_node_cluster.node_crash_race": 2.0, "ec2_volume_io_stuck": 1.1}, "latency_multipliers": {}, "one_shots": [{"ref": "ebs_node_cluster.remirror_throttle_enabled", "count": 1, "hosts": ["ebs-node-01"]}]},
            ]
        }
    },
}

# -----------------------------
# Helpers
# -----------------------------

GLOBAL_SEED = 1337
random.seed(GLOBAL_SEED)
np.random.seed(GLOBAL_SEED)

# Global emission sequence number (used as a stable tie-breaker for equal timestamps)
EMIT_SEQ = 0


def stable_hash_u32(s: str) -> int:
    h = hashlib.sha256(s.encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big", signed=False)


def stable_uniform01(s: str) -> float:
    return stable_hash_u32(s) / 2**32


def make_rng(key: str) -> np.random.RandomState:
    return np.random.RandomState((stable_hash_u32(f"{GLOBAL_SEED}:{key}") ^ GLOBAL_SEED) & 0xFFFFFFFF)


def isoformat_ms(dt: datetime) -> str:
    dtu = dt.astimezone(timezone.utc)
    return dtu.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def stable_round_expected(E: float, key: str) -> int:
    if E <= 0:
        return 0
    n = int(math.floor(E))
    frac = E - n
    if frac <= 1e-12:
        return n
    u = stable_uniform01(f"{key}:round")
    return n + (1 if u < frac else 0)


def schedule_evenly(start: datetime, end: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    dur = (end - start).total_seconds()
    if dur <= 0:
        return [start] * count
    stride = dur / count
    times: List[datetime] = []
    for i in range(count):
        base = (i + 0.5) * stride
        jitter = (stable_uniform01(f"{key}:jit:{i}") - 0.5) * 0.4 * stride  # +/-0.2 stride
        off = clamp(base + jitter, 0.0, max(0.0, dur - 0.001))
        times.append(start + timedelta(seconds=off))
    times.sort()
    return times


def parse_placeholders(msg: str) -> List[str]:
    names: List[str] = []
    for _, field_name, _, _ in Formatter().parse(msg):
        if field_name:
            names.append(field_name.split("!")[0].split(":")[0])
    return names


def domain_for(template: Dict[str, Any], state: str, name: str) -> Optional[Dict[str, Any]]:
    if "vars" in template and name in template["vars"]:
        return template["vars"][name]
    if "state_vars" in template and state in template["state_vars"] and name in template["state_vars"][state]:
        return template["state_vars"][state][name]
    return None


ALNUM = "abcdefghijklmnopqrstuvwxyz0123456789"


def gen_hex(rng: np.random.RandomState, length: int) -> str:
    nbytes = (length + 1) // 2
    b = rng.bytes(nbytes)
    s = b.hex()
    return s[:length]


def gen_uuid(rng: np.random.RandomState) -> str:
    b = rng.bytes(16)
    return str(uuid.UUID(bytes=b))


def gen_str_from_hint(rng: np.random.RandomState, hint: str) -> str:
    if hint == "i-[a-f0-9]{8,17}":
        ln = int(rng.randint(8, 18))
        return "i-" + gen_hex(rng, ln)
    if hint == "db-[a-z0-9]{6}":
        chars = [ALNUM[int(rng.randint(0, len(ALNUM)))] for _ in range(6)]
        return "db-" + "".join(chars)
    return "s-" + gen_hex(rng, 12)


def gen_value(rng: np.random.RandomState, spec: Dict[str, Any]) -> Any:
    k = spec["k"]
    v = spec["v"]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return int(rng.randint(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return float(f"{(lo + (hi - lo) * rng.rand()):.2f}")
    if k == "ch":
        arr = list(v)
        return arr[int(rng.randint(0, len(arr)))]
    if k == "uuid":
        return gen_uuid(rng)
    if k == "hex":
        return gen_hex(rng, int(v))
    if k == "str":
        return gen_str_from_hint(rng, str(v))
    if k == "ip":
        return "127.0.0.1"
    return str(v)


def sample_lognormal_ms(rng: np.random.RandomState, p50: float, p95: float, soft_cap: float) -> int:
    if p95 <= 0 and p50 <= 0:
        return 0
    p50 = float(p50)
    p95 = float(p95)
    if p95 < 1e-9:
        return 0
    if p50 <= 0:
        p50 = max(0.2, p95 / 5.0)
    mu = math.log(p50)
    sigma = 0.0
    if p95 > p50:
        sigma = (math.log(p95) - math.log(p50)) / 1.6448536269514722
        sigma = max(0.05, sigma)
    x = float(rng.lognormal(mean=mu, sigma=sigma))
    x = min(x, soft_cap)
    return int(max(0, round(x)))


# -----------------------------
# Indices
# -----------------------------

COMP_BY_ID: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}
LOG_TPL: Dict[Tuple[str, str], Dict[str, Any]] = {}
LOG_FIELDS: Dict[Tuple[str, str], List[str]] = {}

for comp in SYSTEM["components"]:
    cid = comp["id"]
    for lid, tpl in comp["logs"].items():
        LOG_TPL[(cid, lid)] = tpl
        LOG_FIELDS[(cid, lid)] = parse_placeholders(tpl["msg"])

FLOW_BY_STATE: Dict[str, Dict[str, Dict[str, Any]]] = {"n": {}, "f": {}}
for st in ["n", "f"]:
    for fl in SYSTEM["flows"][st]:
        FLOW_BY_STATE[st][fl["id"]] = fl


def numeric_range_for_var(comp_id: str, log_id: str, state: str, var_name: str) -> Optional[Tuple[float, float]]:
    tpl = LOG_TPL[(comp_id, log_id)]
    dom = domain_for(tpl, state, var_name)
    if not dom:
        return None
    if dom["k"] == "i":
        lo, hi = int(dom["v"][0]), int(dom["v"][1])
        return float(lo), float(hi)
    if dom["k"] == "f":
        lo, hi = float(dom["v"][0]), float(dom["v"][1])
        return lo, hi
    return None


# -----------------------------
# Failure control intervals
# -----------------------------


@dataclass(frozen=True)
class Interval:
    state: str
    start_min: int
    end_min: int
    flow_rate_mult: Dict[str, float]
    bg_rate_mult: Dict[str, float]  # key: "component.log"
    flow_latency_mult: Dict[str, Tuple[float, float]]  # key flow -> (p50mult,p95mult)


def build_failure_intervals() -> List[Interval]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

    flow_rate_mult: Dict[str, float] = {}
    bg_rate_mult: Dict[str, float] = {}
    flow_latency_mult: Dict[str, Tuple[float, float]] = {}

    boundaries = [f_start] + [e["at_min"] for e in events if f_start <= e["at_min"] < f_end] + [f_end]
    boundaries = sorted(set(boundaries))

    ev_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        ev_by_min.setdefault(e["at_min"], []).append(e)

    intervals: List[Interval] = []
    for i in range(len(boundaries) - 1):
        smin = boundaries[i]
        emin = boundaries[i + 1]

        if smin in ev_by_min:
            for e in sorted(ev_by_min[smin], key=lambda x: x["order"]):
                for k, v in (e.get("rate_multipliers") or {}).items():
                    if "." in k:
                        bg_rate_mult[k] = float(v)
                    else:
                        flow_rate_mult[k] = float(v)
                for k, v in (e.get("latency_multipliers") or {}).items():
                    flow_latency_mult[k] = (float(v.get("p50", 1.0)), float(v.get("p95", 1.0)))

        intervals.append(Interval(state="f", start_min=smin, end_min=emin, flow_rate_mult=dict(flow_rate_mult), bg_rate_mult=dict(bg_rate_mult), flow_latency_mult=dict(flow_latency_mult)))
    return intervals


# -----------------------------
# Simulation core
# -----------------------------

BASE_TIME = datetime(2011, 4, 21, 0, 0, 0, tzinfo=timezone.utc)


def dt_at_minute(minute: int) -> datetime:
    return BASE_TIME + timedelta(minutes=int(minute))


def get_component_identity(comp_id: str) -> Tuple[str, List[str]]:
    comp = COMP_BY_ID[comp_id]
    svc = comp.get("svc") or ""
    hosts = comp.get("hosts") or []
    return svc, hosts


def pick_host_for_component(comp_id: str, key: str) -> str:
    _, hosts = get_component_identity(comp_id)
    if not hosts:
        return ""
    idx = stable_hash_u32(f"{key}:{comp_id}:host") % len(hosts)
    return hosts[idx]


def emit_row(rows: List[Dict[str, Any]], ts: datetime, level: str, message: str, trace_id: str, service: str, host: str):
    # Use a monotonically increasing sequence id so stable sorting by [timestamp, seq]
    # preserves emission order even when multiple rows share identical timestamps.
    global EMIT_SEQ
    EMIT_SEQ += 1
    rows.append({"timestamp": ts, "seq": EMIT_SEQ, "level": level, "message": message, "trace_id": trace_id, "service": service, "host": host})


def render_log_message(comp_id: str, log_id: str, state: str, bound: Dict[str, Any], rng: np.random.RandomState) -> Tuple[str, str]:
    tpl = LOG_TPL[(comp_id, log_id)]
    msg_tmpl = tpl["msg"]
    need = LOG_FIELDS[(comp_id, log_id)]
    vals: Dict[str, Any] = {}
    for name in need:
        if name in bound:
            vals[name] = bound[name]
            continue
        dom = domain_for(tpl, state, name)
        if dom is None:
            vals[name] = ""
        else:
            vals[name] = gen_value(rng, dom)
    return tpl["lvl"], msg_tmpl.format(**vals)


def flow_field_overrides(flow_id: str, state: str, field: str) -> Optional[Any]:
    if state == "f" and field == "az":
        if flow_id in {"ebs_api_create_volume_timeout", "ebs_api_create_volume_disabled_fast", "ebs_api_attach_volume_ok_slow", "ebs_api_attach_volume_err"}:
            return "use1a"
    if state == "f" and flow_id == "ebs_elect_primary_err_blocked":
        if field == "status":
            return "403"
        if field == "err":
            return "ClusterPartitioned"
    return None


def _adjust_last_delay_to_fit_total(delays_ms: List[int], idx_from_exclusive: int, idx_to_inclusive: int, lo: int, hi: int):
    """
    Adjust the last delay (idx_to_inclusive) so that sum(delays[idx_from_exclusive:idx_to_inclusive+1])
    falls within [lo, hi]. If negative spill occurs, reduce earlier delays in the segment.
    """
    if idx_to_inclusive < 0 or idx_to_inclusive >= len(delays_ms):
        return
    if idx_from_exclusive > idx_to_inclusive:
        return

    seg_sum = int(sum(delays_ms[idx_from_exclusive : idx_to_inclusive + 1]))
    desired = seg_sum
    if hi >= 0:
        desired = min(desired, hi)
    if lo >= 0:
        desired = max(desired, lo)
    delta = desired - seg_sum
    if delta == 0:
        return

    new_last = delays_ms[idx_to_inclusive] + delta
    if new_last >= 0:
        delays_ms[idx_to_inclusive] = int(new_last)
        return

    deficit = -int(new_last)
    delays_ms[idx_to_inclusive] = 0

    for k in range(idx_to_inclusive - 1, idx_from_exclusive - 1, -1):
        if deficit <= 0:
            break
        take = min(deficit, delays_ms[k])
        delays_ms[k] -= int(take)
        deficit -= int(take)


def simulate_flow_instance(rows: List[Dict[str, Any]], flow: Dict[str, Any], state: str, start_ts: datetime, interval_key: str, flow_latency_mult: Tuple[float, float]):
    flow_id = flow["id"]
    instance_key = f"{interval_key}:flow:{state}:{flow_id}:{isoformat_ms(start_ts)}"
    rng = make_rng(instance_key)

    trace_id = ""
    if SYSTEM["tracing"]["on"] and flow.get("trace", False):
        trace_id = gen_hex(rng, 32)

    emit_refs: List[Tuple[str, str]] = []
    for ref in flow["emit"]:
        comp_id, log_id = ref.split(".", 1)
        emit_refs.append((comp_id, log_id))

    involved_comps = sorted({cid for cid, _ in emit_refs})
    comp_host: Dict[str, str] = {cid: pick_host_for_component(cid, instance_key) for cid in involved_comps}

    exclude_shared = {"latency_ms", "waited_ms", "duration_ms", "total_ms", "backoff_ms"}
    field_counts: Dict[str, int] = {}
    field_domain_tpl: Dict[str, Tuple[str, str]] = {}
    for (cid, lid) in emit_refs:
        for nm in LOG_FIELDS[(cid, lid)]:
            field_counts[nm] = field_counts.get(nm, 0) + 1
            if nm not in field_domain_tpl:
                field_domain_tpl[nm] = (cid, lid)

    shared: Dict[str, Any] = {}
    for nm, cnt in field_counts.items():
        if cnt <= 1 or nm in exclude_shared:
            continue
        override = flow_field_overrides(flow_id, state, nm)
        if override is not None:
            shared[nm] = override
            continue
        (cid, lid) = field_domain_tpl[nm]
        tpl = LOG_TPL[(cid, lid)]
        dom = domain_for(tpl, state, nm)
        if dom is not None:
            shared[nm] = gen_value(rng, dom)

    for nm in ["az", "status", "err"]:
        if nm in field_counts and nm not in shared:
            ov = flow_field_overrides(flow_id, state, nm)
            if ov is not None:
                shared[nm] = ov

    p50m, p95m = flow_latency_mult

    delay_constraints: List[Optional[Tuple[int, int]]] = [None] * len(emit_refs)
    for j, (cid, lid) in enumerate(emit_refs):
        need = LOG_FIELDS[(cid, lid)]

        if "latency_ms" in need:
            r = numeric_range_for_var(cid, lid, state, "latency_ms")
            if r:
                delay_constraints[j] = (int(r[0]), int(r[1]))

        if "waited_ms" in need and j == 0:
            r = numeric_range_for_var(cid, lid, state, "waited_ms")
            if r:
                lo, hi = int(r[0]), int(r[1])
                if delay_constraints[j] is None:
                    delay_constraints[j] = (lo, hi)
                else:
                    delay_constraints[j] = (max(delay_constraints[j][0], lo), min(delay_constraints[j][1], hi))

        if cid == "ebs_node_cluster" and lid == "io_blocked_remirror" and "waited_ms" in need:
            r = numeric_range_for_var(cid, lid, state, "waited_ms")
            if r:
                lo, hi = int(r[0]), int(r[1])
                if delay_constraints[j] is None:
                    delay_constraints[j] = (lo, hi)
                else:
                    delay_constraints[j] = (max(delay_constraints[j][0], lo), min(delay_constraints[j][1], hi))

    delays_ms: List[int] = []
    for j, (p50, p95) in enumerate(flow["latency_ms"]):
        sp50 = float(p50) * p50m
        sp95 = float(p95) * p95m
        soft_cap = max(1.0, 3.0 * sp95)

        lo_hi = delay_constraints[j]
        if lo_hi is not None:
            dlo, dhi = lo_hi
            cap = min(soft_cap, float(max(0, dhi)))
        else:
            cap = soft_cap

        dj = sample_lognormal_ms(make_rng(f"{instance_key}:delay:{j}"), sp50, sp95, cap)

        if lo_hi is not None:
            dlo, dhi = lo_hi
            if dhi < 0:
                dhi = 0
            if dj < dlo:
                dj = dlo
            if dj > dhi:
                dj = dhi

        delays_ms.append(int(dj))

    idx_submit = None
    idx_timeout = None
    for k, (cid, lid) in enumerate(emit_refs):
        if cid == "ec2_hypervisor" and lid == "io_submit" and idx_submit is None:
            idx_submit = k
        if cid == "ec2_hypervisor" and lid == "io_timeout":
            idx_timeout = k

    if idx_submit is not None and idx_timeout is not None and idx_timeout > idx_submit:
        r = numeric_range_for_var("ec2_hypervisor", "io_timeout", state, "waited_ms")
        if r:
            lo, hi = int(r[0]), int(r[1])
            _adjust_last_delay_to_fit_total(delays_ms, idx_submit + 1, idx_timeout, lo, hi)

    idx_txn_error = None
    for k, (cid, lid) in enumerate(emit_refs):
        if cid == "rds_service" and lid == "txn_error":
            idx_txn_error = k
            break
    if idx_txn_error is not None:
        r = numeric_range_for_var("rds_service", "txn_error", state, "waited_ms")
        if r:
            lo, hi = int(r[0]), int(r[1])
            _adjust_last_delay_to_fit_total(delays_ms, 0, idx_txn_error, lo, hi)

    log_ts_list: List[datetime] = []
    cur = start_ts
    for dms in delays_ms:
        cur = cur + timedelta(milliseconds=int(dms))
        log_ts_list.append(cur)

    for j, (cid, lid) in enumerate(emit_refs):
        need = LOG_FIELDS[(cid, lid)]
        bound = dict(shared)

        if "latency_ms" in need:
            prev = start_ts if j == 0 else log_ts_list[j - 1]
            d = int(round((log_ts_list[j] - prev).total_seconds() * 1000.0))
            bound["latency_ms"] = max(0, d)

        if "waited_ms" in need:
            if cid == "ebs_node_cluster" and lid == "io_blocked_remirror":
                idx_start = None
                for k, (cc, ll) in enumerate(emit_refs):
                    if cc == "ebs_node_cluster" and ll == "io_start":
                        idx_start = k
                        break
                if idx_start is None or idx_start >= j:
                    bound["waited_ms"] = max(0, delays_ms[j])
                else:
                    d = int(round((log_ts_list[j] - log_ts_list[idx_start]).total_seconds() * 1000.0))
                    bound["waited_ms"] = max(0, d)
            elif cid == "ec2_hypervisor" and lid == "io_timeout":
                idx_sub = None
                for k, (cc, ll) in enumerate(emit_refs):
                    if cc == "ec2_hypervisor" and ll == "io_submit":
                        idx_sub = k
                        break
                if idx_sub is None or idx_sub >= j:
                    bound["waited_ms"] = max(0, delays_ms[j])
                else:
                    d = int(round((log_ts_list[j] - log_ts_list[idx_sub]).total_seconds() * 1000.0))
                    bound["waited_ms"] = max(0, d)
            elif cid == "rds_service" and lid in {"db_io_wait", "txn_error"}:
                bound["waited_ms"] = max(0, int(round((log_ts_list[j] - start_ts).total_seconds() * 1000.0)))
            else:
                bound["waited_ms"] = max(0, delays_ms[j])

        lvl, msg = render_log_message(cid, lid, state, bound, make_rng(f"{instance_key}:msg:{j}:{cid}.{lid}"))

        svc, _hosts = get_component_identity(cid)
        host = comp_host.get(cid, "") if _hosts else ""
        emit_row(rows, log_ts_list[j], lvl, msg, trace_id if flow.get("trace", False) else "", svc, host)


def simulate_background(rows: List[Dict[str, Any]], comp_id: str, state: str, start_ts: datetime, end_ts: datetime, bg_mult: Dict[str, float], interval_key: str):
    comp = COMP_BY_ID[comp_id]
    beh = comp["beh"][state]["emit"]
    hosts = comp.get("hosts") or []
    svc = comp.get("svc") or ""
    for src in beh:
        log_id = src["id"]
        per_min = float(src["per_min"])
        scope = src.get("scope", "per_host")
        mult_key = f"{comp_id}.{log_id}"
        mult = float(bg_mult.get(mult_key, 1.0))
        rate = per_min * mult

        minutes = (end_ts - start_ts).total_seconds() / 60.0
        if minutes <= 0:
            continue

        if scope == "global":
            expected = rate * minutes
            count = stable_round_expected(expected, f"{interval_key}:bg:{comp_id}.{log_id}:global")
            times = schedule_evenly(start_ts, end_ts, count, f"{interval_key}:bg:{comp_id}.{log_id}:global")
            for i, ts in enumerate(times):
                host = ""
                if hosts:
                    host = hosts[i % len(hosts)]
                lvl, msg = render_log_message(comp_id, log_id, state, {}, make_rng(f"{interval_key}:bgmsg:{comp_id}.{log_id}:g:{i}"))
                emit_row(rows, ts, lvl, msg, "", svc, host)
        else:
            for h in hosts:
                expected = rate * minutes
                count = stable_round_expected(expected, f"{interval_key}:bg:{comp_id}.{log_id}:{h}")
                times = schedule_evenly(start_ts, end_ts, count, f"{interval_key}:bg:{comp_id}.{log_id}:{h}")
                for i, ts in enumerate(times):
                    lvl, msg = render_log_message(comp_id, log_id, state, {}, make_rng(f"{interval_key}:bgmsg:{comp_id}.{log_id}:{h}:{i}"))
                    emit_row(rows, ts, lvl, msg, "", svc, h)


def simulate_one_shots(rows: List[Dict[str, Any]]):
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for e in events:
        at = int(e["at_min"])
        base = dt_at_minute(at)
        shots = e.get("one_shots") or []
        for sidx, shot in enumerate(shots):
            ref = shot["ref"]
            comp_id, log_id = ref.split(".", 1)
            hosts = shot.get("hosts") or []
            count = int(shot["count"])
            svc = COMP_BY_ID[comp_id].get("svc") or ""
            tpl = LOG_TPL[(comp_id, log_id)]
            lvl = tpl["lvl"]

            for i in range(count):
                jitter_s = 0.5 + 4.0 * stable_uniform01(f"oneshot:{ref}:{at}:{sidx}:{i}")
                jitter_ms = int(10 * stable_uniform01(f"oneshotms:{ref}:{at}:{sidx}:{i}"))
                ts = base + timedelta(seconds=jitter_s, milliseconds=jitter_ms + i)
                host = ""
                if hosts:
                    host = hosts[i % len(hosts)]
                else:
                    chosts = COMP_BY_ID[comp_id].get("hosts") or []
                    if chosts:
                        host = chosts[0]
                _lvl, msg = render_log_message(comp_id, log_id, "f", {}, make_rng(f"oneshot:{ref}:{at}:{sidx}:{i}"))
                emit_row(rows, ts, lvl, msg, "", svc, host)


def simulate():
    global EMIT_SEQ
    EMIT_SEQ = 0

    rows: List[Dict[str, Any]] = []

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    n_start_ts = dt_at_minute(n_start)
    n_end_ts = dt_at_minute(n_end)
    n_minutes = float(n_end - n_start)

    interval_key_n = f"n:{n_start}-{n_end}"

    for comp_id in COMP_BY_ID.keys():
        simulate_background(rows, comp_id, "n", n_start_ts, n_end_ts, bg_mult={}, interval_key=interval_key_n)

    for flow_id, flow in FLOW_BY_STATE["n"].items():
        expected_instances = float(flow["rpm"]) * n_minutes
        count = stable_round_expected(expected_instances, f"{interval_key_n}:flow:{flow_id}")
        start_times = schedule_evenly(n_start_ts, n_end_ts, count, f"{interval_key_n}:flow:{flow_id}:starts")
        for st in start_times:
            simulate_flow_instance(rows, flow, "n", st, interval_key_n, flow_latency_mult=(1.0, 1.0))

    f_intervals = build_failure_intervals()
    for iv in f_intervals:
        start_ts = dt_at_minute(iv.start_min)
        end_ts = dt_at_minute(iv.end_min)
        minutes = float(iv.end_min - iv.start_min)
        interval_key = f"f:{iv.start_min}-{iv.end_min}"

        for comp_id in COMP_BY_ID.keys():
            simulate_background(rows, comp_id, "f", start_ts, end_ts, bg_mult=iv.bg_rate_mult, interval_key=interval_key)

        for flow_id, flow in FLOW_BY_STATE["f"].items():
            mult = float(iv.flow_rate_mult.get(flow_id, 1.0))
            rpm_eff = float(flow["rpm"]) * mult
            expected_instances = rpm_eff * minutes
            count = stable_round_expected(expected_instances, f"{interval_key}:flow:{flow_id}")
            start_times = schedule_evenly(start_ts, end_ts, count, f"{interval_key}:flow:{flow_id}:starts")
            lat_mult = iv.flow_latency_mult.get(flow_id, (1.0, 1.0))
            for st in start_times:
                simulate_flow_instance(rows, flow, "f", st, interval_key, flow_latency_mult=lat_mult)

    simulate_one_shots(rows)

    df = pd.DataFrame(rows)
    # Sort by timestamp, then by emission sequence to preserve encoded emit[] ordering for equal timestamps.
    df.sort_values(["timestamp", "seq"], inplace=True, kind="mergesort")
    df["timestamp"] = df["timestamp"].apply(isoformat_ms)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    nrows = len(df)
    if not (20000 <= nrows <= 100000):
        raise RuntimeError(f"Row-count out of expected bounds: {nrows}")

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    simulate()
