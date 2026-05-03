import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# ----------------------------
# Embedded normalized spec data
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "cirrussearch_eqiad_opensearch"},
    "tracing": {"on": True, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "traffic_router",
            "svc": "traffic-router",
            "hosts": ["router1"],
            "logs": {
                "pool_state_eqiad_pooled": {
                    "lvl": "INFO",
                    "msg": "pool state eqiad=true codfw=true",
                    "vars": {},
                },
                "pool_state_eqiad_depooled": {
                    "lvl": "INFO",
                    "msg": "pool state eqiad=false codfw=true",
                    "vars": {},
                },
                "pool_change": {
                    "lvl": "WARN",
                    "msg": "pool change dc=eqiad pooled=false actor=inflatador reason=incident_mitigation",
                    "vars": {},
                },
                "repool_change": {
                    "lvl": "WARN",
                    "msg": "pool change dc=eqiad pooled=true actor=inflatador reason=restore_service",
                    "vars": {},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "pool_state_eqiad_pooled", "per_min": 1.0, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "pool_state_eqiad_pooled", "per_min": 1.0, "scope": "global"},
                        {"id": "pool_state_eqiad_depooled", "per_min": 1.0, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "search_gateway",
            "svc": "cirrussearch-api",
            "hosts": ["cirrussearch-api1", "cirrussearch-api2"],
            "logs": {
                "req_in_eqiad": {
                    "lvl": "INFO",
                    "msg": "request GET /w/api.php q={q} req_id={req_id} dc=eqiad",
                    "vars": {"q": {"k": "str", "v": "search term"}, "req_id": {"k": "hex", "v": 16}},
                },
                "req_in_codfw": {
                    "lvl": "INFO",
                    "msg": "request GET /w/api.php q={q} req_id={req_id} dc=codfw",
                    "vars": {"q": {"k": "str", "v": "search term"}, "req_id": {"k": "hex", "v": 16}},
                },
                "upstream_ok_eqiad": {
                    "lvl": "INFO",
                    "msg": "opensearch eqiad responded status=200 upstream_id={upstream_id} dur_ms={dur_ms}",
                    "vars": {"upstream_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [5, 300]}},
                },
                "upstream_ok_codfw": {
                    "lvl": "INFO",
                    "msg": "opensearch codfw responded status=200 upstream_id={upstream_id} dur_ms={dur_ms}",
                    "vars": {"upstream_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [5, 350]}},
                },
                "upstream_err_503_eqiad": {
                    "lvl": "ERROR",
                    "msg": "opensearch eqiad error status=503 err={err} upstream_id={upstream_id} dur_ms={dur_ms}",
                    "vars": {
                        "err": {"k": "ch", "v": ["no_cluster_manager", "timeout_waiting_for_cluster_state"]},
                        "upstream_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [800, 7000]},
                    },
                },
                "retrying_eqiad": {
                    "lvl": "WARN",
                    "msg": "retrying opensearch eqiad attempt={attempt} upstream_id={upstream_id} backoff_ms={backoff_ms}",
                    "vars": {
                        "attempt": {"k": "i", "v": [2, 3]},
                        "upstream_id": {"k": "hex", "v": 16},
                        "backoff_ms": {"k": "i", "v": [50, 600]},
                    },
                },
                "req_done_200": {
                    "lvl": "INFO",
                    "msg": "response status=200 req_id={req_id} dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [10, 500]}},
                },
                "req_done_503_eqiad": {
                    "lvl": "WARN",
                    "msg": "response status=503 req_id={req_id} dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [900, 9000]}},
                },
                "worker_stats": {
                    "lvl": "INFO",
                    "msg": "worker stats in_flight={in_flight} p95_ms={p95_ms}",
                    "vars": {},
                    "state_vars": {
                        "n": {"in_flight": {"k": "i", "v": [0, 80]}, "p95_ms": {"k": "i", "v": [20, 400]}},
                        "f": {
                            "in_flight": {"k": "i", "v": [10, 220]},
                            "p95_ms": {"k": "i", "v": [80, 9000]},
                        },
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "worker_stats", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "worker_stats", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "opensearch_eqiad",
            "svc": "opensearch",
            "hosts": ["cirrussearch1101", "cirrussearch1102", "cirrussearch1103", "cirrussearch1104"],
            "logs": {
                "cluster_health": {
                    "lvl": "INFO",
                    "msg": "cluster health status={status} active_nodes={active} master={master}",
                    "vars": {
                        "active": {"k": "i", "v": [3, 5]},
                        "master": {
                            "k": "ch",
                            "v": [
                                "cirrussearch1100",
                                "cirrussearch1101",
                                "cirrussearch1102",
                                "cirrussearch1103",
                                "cirrussearch1104",
                            ],
                        },
                    },
                    "state_vars": {
                        "n": {"status": {"k": "ch", "v": ["green", "yellow"]}},
                        "f": {"status": {"k": "ch", "v": ["yellow", "red"]}},
                    },
                },
                "no_cluster_manager": {
                    "lvl": "WARN",
                    "msg": "cluster_manager not discovered yet, retrying; node={node} elapsed_s={elapsed_s}",
                    "vars": {
                        "node": {"k": "ch", "v": ["cirrussearch1101", "cirrussearch1102", "cirrussearch1103", "cirrussearch1104"]},
                        "elapsed_s": {"k": "i", "v": [5, 180]},
                    },
                },
                "cluster_manager_elected": {
                    "lvl": "INFO",
                    "msg": "elected cluster_manager node={node} term={term}",
                    "vars": {
                        "node": {"k": "ch", "v": ["cirrussearch1101", "cirrussearch1102", "cirrussearch1103", "cirrussearch1104"]},
                        "term": {"k": "i", "v": [100, 250]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "cluster_health", "per_min": 0.3, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "cluster_health", "per_min": 0.2, "scope": "per_host"},
                        {"id": "no_cluster_manager", "per_min": 3.0, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "opensearch_eqiad_node1100",
            "svc": "opensearch",
            "hosts": ["cirrussearch1100"],
            "logs": {
                "cluster_health": {
                    "lvl": "INFO",
                    "msg": "cluster health status={status} active_nodes={active} master={master}",
                    "vars": {
                        "active": {"k": "i", "v": [3, 5]},
                        "master": {
                            "k": "ch",
                            "v": [
                                "cirrussearch1100",
                                "cirrussearch1101",
                                "cirrussearch1102",
                                "cirrussearch1103",
                                "cirrussearch1104",
                            ],
                        },
                    },
                    "state_vars": {
                        "n": {"status": {"k": "ch", "v": ["green", "yellow"]}},
                        "f": {"status": {"k": "ch", "v": ["yellow", "red"]}},
                    },
                },
                "no_cluster_manager": {
                    "lvl": "WARN",
                    "msg": "cluster_manager not discovered yet, retrying; node=cirrussearch1100 elapsed_s={elapsed_s}",
                    "vars": {"elapsed_s": {"k": "i", "v": [5, 180]}},
                },
                "cluster_state_mismatch": {
                    "lvl": "WARN",
                    "msg": "cluster state mismatch on join; node=cirrussearch1100 local_uuid={local_uuid} remote_uuid={remote_uuid}",
                    "vars": {"local_uuid": {"k": "uuid", "v": None}, "remote_uuid": {"k": "uuid", "v": None}},
                },
                "node_start": {
                    "lvl": "INFO",
                    "msg": "starting OpenSearch node=cirrussearch1100 data_path={data_path}",
                    "vars": {"data_path": {"k": "ch", "v": ["/srv/opensearch"]}},
                },
                "node_stop": {"lvl": "INFO", "msg": "stopping OpenSearch node=cirrussearch1100 reason=operator_stop", "vars": {}},
                "data_wipe": {
                    "lvl": "WARN",
                    "msg": "removed data path {data_path} on node=cirrussearch1100",
                    "vars": {"data_path": {"k": "ch", "v": ["/srv/opensearch"]}},
                },
                "node_join": {
                    "lvl": "INFO",
                    "msg": "node joined cluster node=cirrussearch1100 cluster_uuid={cluster_uuid}",
                    "vars": {"cluster_uuid": {"k": "uuid", "v": None}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "cluster_health", "per_min": 0.3, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "cluster_health", "per_min": 0.2, "scope": "per_host"},
                        {"id": "no_cluster_manager", "per_min": 3.0, "scope": "per_host"},
                        {"id": "cluster_state_mismatch", "per_min": 1.0, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "opensearch_codfw",
            "svc": "opensearch",
            "hosts": ["cirrussearch2100", "cirrussearch2101", "cirrussearch2102", "cirrussearch2103", "cirrussearch2104"],
            "logs": {
                "cluster_health": {
                    "lvl": "INFO",
                    "msg": "cluster health status={status} active_nodes={active} master={master}",
                    "vars": {
                        "status": {"k": "ch", "v": ["green", "yellow"]},
                        "active": {"k": "i", "v": [4, 5]},
                        "master": {"k": "ch", "v": ["cirrussearch2100", "cirrussearch2101", "cirrussearch2102", "cirrussearch2103", "cirrussearch2104"]},
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "cluster_health", "per_min": 0.3, "scope": "per_host"}]},
                "f": {"emit": [{"id": "cluster_health", "per_min": 0.3, "scope": "per_host"}]},
            },
        },
        {
            "id": "monitoring",
            "svc": "monitoring",
            "hosts": ["mon1"],
            "logs": {
                "probe_ok": {"lvl": "INFO", "msg": "probe opensearch_eqiad OK status=200 latency_ms={lat_ms}", "vars": {"lat_ms": {"k": "i", "v": [5, 120]}}},
                "probe_fail": {
                    "lvl": "WARN",
                    "msg": "probe opensearch_eqiad FAIL status=503 err={err} latency_ms={lat_ms}",
                    "vars": {"err": {"k": "ch", "v": ["no_cluster_manager"]}, "lat_ms": {"k": "i", "v": [800, 5000]}},
                },
                "alert_search_timeouts": {"lvl": "CRITICAL", "msg": "ALERT SearchBackendUnavailable dc=eqiad error=upstream_503_no_cluster_manager firing=true", "vars": {}},
                "scrape": {"lvl": "INFO", "msg": "scrape search_gateway metrics success=true targets={targets}", "vars": {"targets": {"k": "i", "v": [10, 20]}}},
            },
            "beh": {"n": {"emit": [{"id": "scrape", "per_min": 0.5, "scope": "global"}]}, "f": {"emit": [{"id": "scrape", "per_min": 0.5, "scope": "global"}]}},
        },
        {
            "id": "ops_tooling",
            "svc": "ops-tooling",
            "hosts": ["ops1"],
            "logs": {
                "rolling_restart_step": {"lvl": "INFO", "msg": "rolling restart step host=cirrussearch1100 action=restart svc=opensearch", "vars": {}},
                "investigation_uuid": {
                    "lvl": "INFO",
                    "msg": "investigation: host=cirrussearch1100 local_cluster_uuid={local_uuid} cluster_uuid={remote_uuid}",
                    "vars": {"local_uuid": {"k": "uuid", "v": None}, "remote_uuid": {"k": "uuid", "v": None}},
                },
                "cmd_depool_eqiad": {"lvl": "INFO", "msg": "operator command 'depool eqiad' target=traffic_router", "vars": {}},
                "cmd_repool_eqiad": {"lvl": "INFO", "msg": "operator command 'repool eqiad' target=traffic_router", "vars": {}},
                "cmd_stop_opensearch": {"lvl": "INFO", "msg": "operator command 'systemctl stop opensearch' target=cirrussearch1100", "vars": {}},
                "cmd_wipe_datadir": {"lvl": "INFO", "msg": "operator command 'rm -rf /srv/opensearch/*' target=cirrussearch1100", "vars": {}},
                "cmd_start_opensearch": {"lvl": "INFO", "msg": "operator command 'systemctl start opensearch' target=cirrussearch1100", "vars": {}},
                "sal_heartbeat": {"lvl": "INFO", "msg": "SAL note: {note}", "vars": {"note": {"k": "ch", "v": ["rolling restart in progress", "incident mitigation ongoing"]}}},
            },
            "beh": {"n": {"emit": [{"id": "sal_heartbeat", "per_min": 0.05, "scope": "global"}]}, "f": {"emit": [{"id": "sal_heartbeat", "per_min": 0.05, "scope": "global"}]}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "search_query_eqiad_ok",
                    "rpm": 140.0,
                    "emit": ["search_gateway.req_in_eqiad", "search_gateway.upstream_ok_eqiad", "search_gateway.req_done_200"],
                    "latency_ms": [[1, 3], [25, 120], [2, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "search_query_codfw_ok",
                    "rpm": 90.0,
                    "emit": ["search_gateway.req_in_codfw", "search_gateway.upstream_ok_codfw", "search_gateway.req_done_200"],
                    "latency_ms": [[1, 3], [30, 140], [2, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "health_check_eqiad_ok",
                    "rpm": 6.0,
                    "emit": ["monitoring.probe_ok"],
                    "latency_ms": [[10, 80]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "search_query_eqiad_fail",
                    "rpm": 140.0,
                    "emit": ["search_gateway.upstream_err_503_eqiad"],
                    "latency_ms": [[2000, 6500]],
                    "retry": {
                        "max_attempts": 3,
                        "expected_attempts": 2.2,
                        "emit_per_retry": ["search_gateway.retrying_eqiad"],
                        "backoff_ms": [[100, 300], [200, 600]],
                    },
                    "trace": True,
                },
                {
                    "id": "search_query_eqiad_client_503",
                    "rpm": 140.0,
                    "emit": ["search_gateway.req_in_eqiad", "search_gateway.req_done_503_eqiad"],
                    "latency_ms": [[1, 3], [900, 9000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "search_query_eqiad_ok_recovered",
                    "rpm": 140.0,
                    "emit": ["search_gateway.req_in_eqiad", "search_gateway.upstream_ok_eqiad", "search_gateway.req_done_200"],
                    "latency_ms": [[1, 3], [25, 140], [2, 12]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "search_query_codfw_ok",
                    "rpm": 90.0,
                    "emit": ["search_gateway.req_in_codfw", "search_gateway.upstream_ok_codfw", "search_gateway.req_done_200"],
                    "latency_ms": [[1, 3], [35, 180], [2, 12]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "health_check_eqiad_fail",
                    "rpm": 6.0,
                    "emit": ["monitoring.probe_fail"],
                    "latency_ms": [[900, 5000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "health_check_eqiad_ok",
                    "rpm": 6.0,
                    "emit": ["monitoring.probe_ok"],
                    "latency_ms": [[10, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "cirrussearch_outage_2025_07_07"},
    "time": {"total_minutes": 42, "phases": {"n": {"start_min": 0, "end_min": 21}, "f": {"start_min": 21, "end_min": 42}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 21,
                    "rate_multipliers": {
                        "search_query_eqiad_ok_recovered": 0.0,
                        "health_check_eqiad_ok": 0.0,
                        "traffic_router.pool_state_eqiad_depooled": 0.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "ops_tooling.rolling_restart_step", "count": 1, "hosts": ["ops1"]},
                        {"ref": "opensearch_eqiad_node1100.node_start", "count": 1, "hosts": ["cirrussearch1100"]},
                        {"ref": "monitoring.alert_search_timeouts", "count": 1, "hosts": ["mon1"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 24,
                    "rate_multipliers": {
                        "search_query_eqiad_fail": 0.0,
                        "search_query_eqiad_client_503": 0.0,
                        "search_query_codfw_ok": 2.56,
                        "health_check_eqiad_ok": 0.0,
                        "traffic_router.pool_state_eqiad_pooled": 0.0,
                        "traffic_router.pool_state_eqiad_depooled": 1.0,
                    },
                    "latency_multipliers": {"search_query_codfw_ok": {"p50": 1.1, "p95": 1.4}},
                    "one_shots": [
                        {"ref": "traffic_router.pool_change", "count": 1, "hosts": ["router1"]},
                        {"ref": "ops_tooling.cmd_depool_eqiad", "count": 1, "hosts": ["ops1"]},
                    ],
                },
                {"order": 3, "at_min": 28, "rate_multipliers": {"health_check_eqiad_ok": 0.0}, "latency_multipliers": {}, "one_shots": [{"ref": "ops_tooling.investigation_uuid", "count": 1, "hosts": ["ops1"]}]},
                {
                    "order": 4,
                    "at_min": 37,
                    "rate_multipliers": {
                        "opensearch_eqiad_node1100.cluster_health": 0.0,
                        "opensearch_eqiad_node1100.no_cluster_manager": 0.0,
                        "opensearch_eqiad_node1100.cluster_state_mismatch": 0.0,
                        "opensearch_eqiad.no_cluster_manager": 0.2,
                        "health_check_eqiad_fail": 0.2,
                        "health_check_eqiad_ok": 0.8,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "ops_tooling.cmd_stop_opensearch", "count": 1, "hosts": ["ops1"]},
                        {"ref": "opensearch_eqiad_node1100.node_stop", "count": 1, "hosts": ["cirrussearch1100"]},
                        {"ref": "opensearch_eqiad.cluster_manager_elected", "count": 1, "hosts": ["cirrussearch1101"]},
                    ],
                },
                {
                    "order": 5,
                    "at_min": 40,
                    "rate_multipliers": {
                        "opensearch_eqiad_node1100.cluster_health": 1.0,
                        "opensearch_eqiad_node1100.no_cluster_manager": 0.0,
                        "opensearch_eqiad_node1100.cluster_state_mismatch": 0.0,
                        "opensearch_eqiad.no_cluster_manager": 0.05,
                        "health_check_eqiad_fail": 0.0,
                        "health_check_eqiad_ok": 1.0,
                    },
                    "latency_multipliers": {},
                    "one_shots": [
                        {"ref": "ops_tooling.cmd_wipe_datadir", "count": 1, "hosts": ["ops1"]},
                        {"ref": "ops_tooling.cmd_start_opensearch", "count": 1, "hosts": ["ops1"]},
                        {"ref": "opensearch_eqiad_node1100.data_wipe", "count": 1, "hosts": ["cirrussearch1100"]},
                        {"ref": "opensearch_eqiad_node1100.node_start", "count": 1, "hosts": ["cirrussearch1100"]},
                        {"ref": "opensearch_eqiad_node1100.node_join", "count": 1, "hosts": ["cirrussearch1100"]},
                    ],
                },
                {
                    "order": 6,
                    "at_min": 41,
                    "rate_multipliers": {
                        "search_query_eqiad_ok_recovered": 1.0,
                        "search_query_codfw_ok": 1.0,
                        "search_query_eqiad_fail": 0.0,
                        "search_query_eqiad_client_503": 0.0,
                        "opensearch_eqiad.no_cluster_manager": 0.0,
                        "traffic_router.pool_state_eqiad_pooled": 1.0,
                        "traffic_router.pool_state_eqiad_depooled": 0.0,
                    },
                    "latency_multipliers": {"search_query_codfw_ok": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [
                        {"ref": "traffic_router.repool_change", "count": 1, "hosts": ["router1"]},
                        {"ref": "ops_tooling.cmd_repool_eqiad", "count": 1, "hosts": ["ops1"]},
                    ],
                },
            ]
        }
    },
}

# ----------------------------
# Helpers
# ----------------------------

SEED = 1337
random.seed(SEED)
np.random.seed(SEED)


def _sha256_int(s: str) -> int:
    return int(hashlib.sha256(s.encode("utf-8")).hexdigest(), 16)


def hash_uniform(*parts: Any) -> float:
    s = "|".join(str(p) for p in parts)
    return _sha256_int(s) / float(2**256)


def hash_hex(length: int, *parts: Any) -> str:
    h = hashlib.sha256("|".join(str(p) for p in parts).encode("utf-8")).hexdigest()
    while len(h) < length:
        h += hashlib.sha256(h.encode("utf-8")).hexdigest()
    return h[:length]


def hash_uuid(*parts: Any) -> str:
    h = hash_hex(32, "uuid", *parts)
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


# Acklam's inverse normal CDF approximation (deterministic, no SciPy needed)
def norm_ppf(p: float) -> float:
    if p <= 0.0:
        return -float("inf")
    if p >= 1.0:
        return float("inf")

    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02, 1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]

    plow = 0.02425
    phigh = 1 - plow

    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        return num / den
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        num = -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        return num / den

    q = p - 0.5
    r = q * q
    num = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    den = (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    return num / den


def lognormal_sample_ms(p50_ms: float, p95_ms: float, u: float, soft_cap: float) -> float:
    p50_ms = max(0.1, float(p50_ms))
    p95_ms = max(p50_ms * 1.001, float(p95_ms))
    mu = math.log(p50_ms)
    sigma = max(1e-9, (math.log(p95_ms) - mu) / 1.645)
    u = min(max(u, 1e-6), 1 - 1e-6)
    z = norm_ppf(u)
    x = math.exp(mu + sigma * z)
    # Soft cap by compressing tail rather than hard clamping.
    if x > soft_cap:
        x = soft_cap + (x - soft_cap) * 0.15
    return float(x)


def alloc_int(expected: float, key: str) -> int:
    expected = max(0.0, float(expected))
    n = int(math.floor(expected))
    frac = expected - n
    u = hash_uniform("alloc", key)
    return n + (1 if u < frac else 0)


def dt_to_iso_millis(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def schedule_times(count: int, start_dt: datetime, end_dt: datetime, key: str) -> List[datetime]:
    if count <= 0:
        return []
    dur = (end_dt - start_dt).total_seconds()
    if dur <= 0:
        return []
    spacing = dur / count
    out: List[datetime] = []
    for i in range(count):
        base = (i + 0.5) * spacing
        u = hash_uniform("jitter", key, i)
        jitter = (u - 0.5) * 0.6 * spacing
        t = base + jitter
        t = min(max(t, 0.0), max(0.0, dur - 0.001))
        out.append(start_dt + timedelta(seconds=t))
    return out


def parse_ref(ref: str) -> Tuple[str, str]:
    comp, log_id = ref.split(".", 1)
    return comp, log_id


def gen_value(dom_spec: Dict[str, Any], key: str, state: str, context_key: str) -> Any:
    k = dom_spec.get("k")
    v = dom_spec.get("v")
    u = hash_uniform("var", context_key, state, key)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi <= lo:
            return lo
        return int(lo + math.floor(u * (hi - lo + 1)))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return lo + u * (hi - lo)
    if k == "ch":
        choices = list(v)
        if not choices:
            return ""
        idx = int(math.floor(u * len(choices))) % len(choices)
        return choices[idx]
    if k == "hex":
        ln = int(v)
        return hash_hex(ln, context_key, state, key)
    if k == "uuid":
        return hash_uuid(context_key, state, key)
    if k == "str":
        base = str(v) if v is not None else "value"
        terms = [
            "search term",
            "wikipedia",
            "cirrussearch",
            "opensearch",
            "leader election",
            "datacenter",
            "rolling restart",
            "cluster state",
        ]
        idx = int(math.floor(u * len(terms))) % len(terms)
        if base.strip().lower() == "search term":
            return terms[idx]
        return base
    if k == "ip":
        return "127.0.0.1"
    return str(v) if v is not None else ""


@dataclass(frozen=True)
class LogTemplate:
    level: str
    message: str
    vars: Dict[str, Any]
    state_vars: Dict[str, Any]


def clamp_int(x: int, lo: int, hi: int) -> int:
    return lo if x < lo else hi if x > hi else x


# ----------------------------
# Build indices
# ----------------------------

components: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

log_templates: Dict[str, LogTemplate] = {}
for comp_id, comp in components.items():
    for log_id, t in comp.get("logs", {}).items():
        log_templates[f"{comp_id}.{log_id}"] = LogTemplate(
            level=t["lvl"],
            message=t["msg"],
            vars=t.get("vars", {}) or {},
            state_vars=t.get("state_vars", {}) or {},
        )

flows_by_state: Dict[str, Dict[str, Dict[str, Any]]] = {"n": {}, "f": {}}
for st in ["n", "f"]:
    for fl in SYSTEM["flows"][st]["req"]:
        flows_by_state[st][fl["id"]] = fl


def get_int_var_range(ref: str, var_name: str, state: str) -> Optional[Tuple[int, int]]:
    tmpl = log_templates.get(ref)
    if not tmpl:
        return None
    dom = None
    if var_name in (tmpl.vars or {}):
        dom = (tmpl.vars or {}).get(var_name)
    if tmpl.state_vars and state in tmpl.state_vars and var_name in (tmpl.state_vars[state] or {}):
        dom = (tmpl.state_vars[state] or {}).get(var_name)
    if not dom:
        return None
    if dom.get("k") != "i":
        return None
    v = dom.get("v")
    if not isinstance(v, list) or len(v) != 2:
        return None
    return int(v[0]), int(v[1])


# Pre-bind incident-wide UUIDs for "stale local" vs "current cluster"
INCIDENT_LOCAL_UUID = hash_uuid("incident", "local_uuid")
INCIDENT_REMOTE_UUID = hash_uuid("incident", "remote_uuid")

# ----------------------------
# Failure controls: piecewise intervals
# ----------------------------

f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

boundaries = [f_start] + sorted({e["at_min"] for e in events if f_start <= e["at_min"] <= f_end}) + [f_end]
boundaries = [boundaries[0]] + [m for i, m in enumerate(boundaries[1:], start=1) if m > boundaries[i - 1]]

active_rate_mul: Dict[str, float] = {}
active_lat_mul: Dict[str, Dict[str, float]] = {}

event_map: Dict[int, List[Dict[str, Any]]] = {}
for e in events:
    event_map.setdefault(e["at_min"], []).append(e)

interval_controls: Dict[int, Dict[str, Any]] = {}
for b in boundaries[:-1]:
    for e in event_map.get(b, []):
        for k, v in (e.get("rate_multipliers", {}) or {}).items():
            active_rate_mul[k] = float(v)
        for k, v in (e.get("latency_multipliers", {}) or {}).items():
            active_lat_mul[k] = {"p50": float(v.get("p50", 1.0)), "p95": float(v.get("p95", 1.0))}
    interval_controls[b] = {"rate_mul": dict(active_rate_mul), "lat_mul": dict(active_lat_mul)}

# ----------------------------
# Simulation: emit rows
# ----------------------------

BASE_TIME = datetime(2025, 7, 7, 0, 0, 0, tzinfo=timezone.utc)

rows: List[Dict[str, Any]] = []


def emit_log(
    ts: datetime,
    ref: str,
    state: str,
    overrides: Optional[Dict[str, Any]] = None,
    trace_id: str = "",
    host_override: Optional[str] = None,
    context_key: str = "",
) -> None:
    comp_id, _ = parse_ref(ref)
    comp = components[comp_id]
    tmpl = log_templates[ref]
    domains: Dict[str, Any] = {}
    domains.update(tmpl.vars or {})
    if tmpl.state_vars and state in tmpl.state_vars:
        domains.update(tmpl.state_vars[state] or {})

    ctx_key = context_key or f"{ref}|{dt_to_iso_millis(ts)}"
    vals: Dict[str, Any] = {}
    for k, dom in domains.items():
        vals[k] = gen_value(dom, k, state, ctx_key)

    if overrides:
        vals.update(overrides)

    # Special coherence bindings for modeled UUID mismatch/join evidence
    if ref == "opensearch_eqiad_node1100.cluster_state_mismatch":
        vals["local_uuid"] = INCIDENT_LOCAL_UUID
        vals["remote_uuid"] = INCIDENT_REMOTE_UUID
    if ref == "ops_tooling.investigation_uuid":
        vals["local_uuid"] = INCIDENT_LOCAL_UUID
        vals["remote_uuid"] = INCIDENT_REMOTE_UUID
    if ref == "opensearch_eqiad_node1100.node_join":
        vals["cluster_uuid"] = INCIDENT_REMOTE_UUID

    # Make node field match emitting host when appropriate for coherence
    if ref == "opensearch_eqiad.no_cluster_manager":
        vals["node"] = host_override or vals.get("node", "")
    if ref == "opensearch_eqiad.cluster_manager_elected":
        vals["node"] = host_override or vals.get("node", "")

    msg = tmpl.message.format(**vals)

    host = host_override if host_override is not None else (comp["hosts"][0] if comp.get("hosts") else "")
    rows.append(
        {
            "ts": ts,
            "timestamp": "",
            "level": tmpl.level,
            "message": msg,
            "trace_id": trace_id,
            "service": comp.get("svc", "") or "",
            "host": host or "",
        }
    )


def get_failure_rate_mul(start_min: int, key: str) -> float:
    ctrl = interval_controls.get(start_min, {"rate_mul": {}})
    return float(ctrl["rate_mul"].get(key, 1.0))


def get_failure_latency_mul(start_min: int, flow_id: str) -> Tuple[float, float]:
    ctrl = interval_controls.get(start_min, {"lat_mul": {}})
    d = ctrl["lat_mul"].get(flow_id)
    if not d:
        return 1.0, 1.0
    return float(d.get("p50", 1.0)), float(d.get("p95", 1.0))


def sample_latency_pair(pair: List[float], ukey: str, p50_mul: float = 1.0, p95_mul: float = 1.0, soft_cap_mult: float = 2.8) -> float:
    p50, p95 = float(pair[0]) * p50_mul, float(pair[1]) * p95_mul
    u = hash_uniform("lat", ukey)
    soft_cap = max(p95 * soft_cap_mult, p50 * 4.0)
    return lognormal_sample_ms(p50, p95, u, soft_cap=soft_cap)


def choose_component_host(comp_id: str, instance_key: str) -> str:
    hosts = components[comp_id].get("hosts") or [""]
    if len(hosts) == 1:
        return hosts[0]
    u = hash_uniform("host", comp_id, instance_key)
    idx = int(math.floor(u * len(hosts))) % len(hosts)
    return hosts[idx]


def attempt_counts_for_batch(n: int, expected_attempts: float, max_attempts: int, key: str) -> List[int]:
    if n <= 0:
        return []
    expected_attempts = max(1.0, min(float(expected_attempts), float(max_attempts)))
    lo = int(math.floor(expected_attempts))
    hi = int(math.ceil(expected_attempts))
    lo = max(1, min(lo, max_attempts))
    hi = max(1, min(hi, max_attempts))
    if lo == hi:
        return [lo] * n
    frac = expected_attempts - lo
    k = int(round(frac * n))
    k = max(0, min(n, k))
    rot = int(math.floor(hash_uniform("mixrot", key) * n)) if n > 0 else 0
    out = [lo] * n
    for j in range(k):
        out[(rot + j) % n] = hi
    return out


# ----------------------------
# Background emissions
# ----------------------------

def simulate_background_interval(state: str, start_min: int, end_min: int) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    dur_min = (end_dt - start_dt).total_seconds() / 60.0

    for comp_id, comp in components.items():
        beh = (comp.get("beh", {}) or {}).get(state, {}) or {}
        emits = beh.get("emit", []) or []
        for e in emits:
            log_id = e["id"]
            per_min = float(e["per_min"])
            scope = e.get("scope") or "per_host"
            ref = f"{comp_id}.{log_id}"

            mul = 1.0
            if state == "f":
                mul = get_failure_rate_mul(start_min, ref)

            eff = per_min * mul
            if eff <= 0.0:
                continue

            if scope == "global":
                expected = eff * dur_min
                cnt = alloc_int(expected, f"bg|{state}|{start_min}-{end_min}|{ref}|global")
                times = schedule_times(cnt, start_dt, end_dt, f"bg|{state}|{start_min}-{end_min}|{ref}|global")
                host = comp.get("hosts", [""])[0] if comp.get("hosts") else ""
                for i, ts in enumerate(times):
                    emit_log(ts, ref, state, overrides=None, trace_id="", host_override=host, context_key=f"bg|{ref}|{state}|{start_min}|{i}")
            else:
                for host in comp.get("hosts", []) or [""]:
                    expected = eff * dur_min
                    cnt = alloc_int(expected, f"bg|{state}|{start_min}-{end_min}|{ref}|{host}")
                    times = schedule_times(cnt, start_dt, end_dt, f"bg|{state}|{start_min}-{end_min}|{ref}|{host}")
                    for i, ts in enumerate(times):
                        emit_log(ts, ref, state, overrides=None, trace_id="", host_override=host, context_key=f"bg|{ref}|{state}|{start_min}|{host}|{i}")


# ----------------------------
# Flow emissions
# ----------------------------

def simulate_flow_instances(state: str, interval_start_min: int, interval_end_min: int) -> None:
    start_dt = BASE_TIME + timedelta(minutes=interval_start_min)
    end_dt = BASE_TIME + timedelta(minutes=interval_end_min)
    dur_min = (end_dt - start_dt).total_seconds() / 60.0

    flows = flows_by_state[state]
    for flow_id, flow in flows.items():
        rpm = float(flow["rpm"])
        mul = 1.0
        if state == "f":
            mul = get_failure_rate_mul(interval_start_min, flow_id)
        eff_rpm = rpm * mul
        if eff_rpm <= 0.0:
            continue

        expected_instances = eff_rpm * dur_min
        n_instances = alloc_int(expected_instances, f"flow|{state}|{interval_start_min}-{interval_end_min}|{flow_id}")
        if n_instances <= 0:
            continue

        starts = schedule_times(n_instances, start_dt, end_dt, f"flow|{state}|{interval_start_min}-{interval_end_min}|{flow_id}")

        retry = flow.get("retry", {}) or {}
        max_attempts = int(retry.get("max_attempts", 1))
        expected_attempts = float(retry.get("expected_attempts", 1.0))
        attempts_list = attempt_counts_for_batch(n_instances, expected_attempts, max_attempts, f"attempts|{state}|{interval_start_min}|{flow_id}")

        for idx, (inst_start, attempts) in enumerate(zip(starts, attempts_list)):
            instance_key = f"{state}|{interval_start_min}|{flow_id}|{idx}"
            trace_id = hash_hex(32, "trace", instance_key) if (SYSTEM["tracing"]["on"] and flow.get("trace", False)) else ""

            emit_refs: List[str] = list(flow.get("emit", []) or [])
            retry_refs: List[str] = list((retry.get("emit_per_retry", []) or []))
            comps_involved = sorted({parse_ref(r)[0] for r in (emit_refs + retry_refs)})
            host_by_comp = {cid: choose_component_host(cid, instance_key) for cid in comps_involved}

            req_id = hash_hex(16, "req", instance_key)
            upstream_id = hash_hex(16, "upstream", instance_key)

            p50_mul, p95_mul = (1.0, 1.0)
            if state == "f":
                p50_mul, p95_mul = get_failure_latency_mul(interval_start_min, flow_id)

            q = gen_value({"k": "str", "v": "search term"}, "q", state, f"q|{instance_key}")
            err_choice = ["no_cluster_manager", "timeout_waiting_for_cluster_state"][idx % 2]

            backoff_pairs: List[List[float]] = list(retry.get("backoff_ms", []) or [])

            # Retry chronology:
            # - attempt_start advances to the end of the prior attempt + backoff
            # - retry marker is emitted on retry attempts (2..A) at the start of that attempt
            #   with backoff_ms matching the gap since prior attempt's last log.
            attempt_start = inst_start
            backoff_before_attempt_ms = 0  # only used for attempts >= 2

            for attempt in range(1, attempts + 1):
                if attempt >= 2 and retry_refs:
                    retry_ref = retry_refs[0]
                    r_comp, _ = parse_ref(retry_ref)
                    emit_log(
                        attempt_start,
                        retry_ref,
                        state,
                        overrides={"attempt": attempt, "upstream_id": upstream_id, "backoff_ms": backoff_before_attempt_ms},
                        trace_id=trace_id,
                        host_override=host_by_comp.get(r_comp, ""),
                        context_key=f"retry|{instance_key}|a{attempt}",
                    )

                prev_ts = attempt_start
                req_in_ts: Optional[datetime] = None

                lat_pairs: List[List[float]] = list(flow.get("latency_ms", []) or [])
                for li, ref in enumerate(emit_refs):
                    pair = lat_pairs[li] if li < len(lat_pairs) else [1.0, 3.0]
                    delay_ms = sample_latency_pair(pair, f"lat|{instance_key}|a{attempt}|l{li}", p50_mul, p95_mul)

                    # Convert to an integer millisecond delay for stable timestamp/value coupling.
                    delay_int = max(1, int(round(delay_ms)))

                    # If the log exposes a per-step timing field, clamp to that log's modeled domain.
                    timing_field: Optional[str] = None
                    if ref in ("search_gateway.upstream_ok_eqiad", "search_gateway.upstream_ok_codfw", "search_gateway.upstream_err_503_eqiad"):
                        timing_field = "dur_ms"
                    elif ref in ("monitoring.probe_ok", "monitoring.probe_fail"):
                        timing_field = "lat_ms"
                    elif ref == "search_gateway.req_done_503_eqiad" and len(emit_refs) == 2 and li == 1:
                        timing_field = "dur_ms"

                    if timing_field is not None:
                        rng = get_int_var_range(ref, timing_field, state)
                        if rng is not None:
                            lo, hi = rng
                            delay_int = clamp_int(delay_int, lo, hi)

                    # Special case: req_done_200 carries end-to-end dur_ms computed from timestamps.
                    # Ensure the final step delay yields a total duration within req_done_200's modeled domain.
                    if ref == "search_gateway.req_done_200" and req_in_ts is not None:
                        rng_total = get_int_var_range(ref, "dur_ms", state)
                        if rng_total is not None:
                            lo_t, hi_t = rng_total
                            elapsed_before = int(round((prev_ts - req_in_ts).total_seconds() * 1000.0))
                            # Make total at least lo_t by extending the final step delay if needed.
                            min_needed = lo_t - elapsed_before
                            if min_needed > delay_int:
                                delay_int = min_needed
                            # Keep total at most hi_t (should not occur with the encoded latencies, but keep deterministic).
                            max_allowed = hi_t - elapsed_before
                            if max_allowed >= 1:
                                delay_int = min(delay_int, max_allowed)

                    ts = prev_ts + timedelta(milliseconds=delay_int)

                    comp_id, _ = parse_ref(ref)
                    overrides: Dict[str, Any] = {}

                    if ref in ("search_gateway.req_in_eqiad", "search_gateway.req_in_codfw"):
                        overrides["req_id"] = req_id
                        overrides["q"] = q
                        req_in_ts = ts
                    elif ref in ("search_gateway.upstream_ok_eqiad", "search_gateway.upstream_ok_codfw"):
                        overrides["upstream_id"] = upstream_id
                        overrides["dur_ms"] = delay_int
                    elif ref == "search_gateway.upstream_err_503_eqiad":
                        overrides["upstream_id"] = upstream_id
                        overrides["err"] = err_choice
                        overrides["dur_ms"] = delay_int
                    elif ref == "search_gateway.req_done_200":
                        overrides["req_id"] = req_id
                        if req_in_ts is not None:
                            overrides["dur_ms"] = int(round((ts - req_in_ts).total_seconds() * 1000.0))
                    elif ref == "search_gateway.req_done_503_eqiad":
                        overrides["req_id"] = req_id
                        if req_in_ts is not None:
                            overrides["dur_ms"] = int(round((ts - req_in_ts).total_seconds() * 1000.0))
                    elif ref == "monitoring.probe_ok":
                        overrides["lat_ms"] = delay_int
                    elif ref == "monitoring.probe_fail":
                        overrides["err"] = "no_cluster_manager"
                        overrides["lat_ms"] = delay_int

                    emit_log(
                        ts,
                        ref,
                        state,
                        overrides=overrides,
                        trace_id=trace_id,
                        host_override=host_by_comp.get(comp_id, ""),
                        context_key=f"flow|{instance_key}|a{attempt}|{ref}",
                    )
                    prev_ts = ts

                attempt_end = prev_ts

                if attempt < attempts:
                    if backoff_pairs:
                        bo_pair = backoff_pairs[attempt - 1] if (attempt - 1) < len(backoff_pairs) else backoff_pairs[-1]
                    else:
                        bo_pair = [100.0, 300.0]

                    bo_ms = sample_latency_pair(bo_pair, f"bo|{instance_key}|to_a{attempt+1}", 1.0, 1.0, soft_cap_mult=3.0)
                    # Keep backoff in the modeled log domain [50,600] and use the same value for spacing.
                    bo_int = clamp_int(max(1, int(round(bo_ms))), 50, 600)
                    backoff_before_attempt_ms = bo_int
                    attempt_start = attempt_end + timedelta(milliseconds=bo_int)


# ----------------------------
# One-shots
# ----------------------------

def simulate_one_shots() -> None:
    for e in events:
        at_min = int(e["at_min"])
        base = BASE_TIME + timedelta(minutes=at_min)
        shots = e.get("one_shots", []) or []
        for si, s in enumerate(shots):
            ref = s["ref"]
            count = int(s["count"])
            hosts = s.get("hosts") or []
            comp_id, _ = parse_ref(ref)
            comp_hosts = components[comp_id].get("hosts", []) or [""]
            if not hosts:
                hosts = comp_hosts

            for j in range(count):
                u = hash_uniform("oneshot", at_min, ref, si, j)
                ts = base + timedelta(seconds=min(5.0, u * 5.0))
                host = hosts[j % len(hosts)] if hosts else (comp_hosts[0] if comp_hosts else "")
                emit_log(ts, ref, "f", overrides=None, trace_id="", host_override=host, context_key=f"oneshot|{at_min}|{ref}|{si}|{j}")


# ----------------------------
# Run simulation
# ----------------------------

n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
simulate_background_interval("n", n_start, n_end)
simulate_flow_instances("n", n_start, n_end)

for i in range(len(boundaries) - 1):
    a = boundaries[i]
    b = boundaries[i + 1]
    simulate_background_interval("f", a, b)
    simulate_flow_instances("f", a, b)

simulate_one_shots()

# ----------------------------
# Finalize CSV
# ----------------------------

df = pd.DataFrame(rows)
if df.empty:
    df = pd.DataFrame(columns=["timestamp", "level", "message", "trace_id", "service", "host"])
else:
    df = df.sort_values("ts", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["ts"].apply(dt_to_iso_millis)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

row_count = len(df)
if not (20000 <= row_count <= 100000):
    raise RuntimeError(f"Log volume out of target range: {row_count} rows (expected 20k..100k)")

bad_trace = df[(df["trace_id"] != "") & (~df["trace_id"].str.match(r"^[0-9a-f]{32}$", na=False))]
if len(bad_trace) > 0:
    raise RuntimeError("Invalid trace_id format detected")

if not df["timestamp"].is_monotonic_increasing:
    raise RuntimeError("Timestamps not sorted ascending")

df.to_csv("logs.csv", index=False)
