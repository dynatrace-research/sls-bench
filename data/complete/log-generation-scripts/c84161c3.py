import math
import random
import uuid
import zlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# Global deterministic seeds (even though we mostly use local Random instances).
GLOBAL_SEED = 1337
random.seed(GLOBAL_SEED)
np.random.seed(GLOBAL_SEED)

# -----------------------------
# Embedded normalized model data
# -----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "azure_compute_cluster_control"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["service_mgmt_api", "acs_service"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "fabric_controller",
            "svc": "fabric-controller",
            "hosts": ["fc-01", "fc-02"],
            "logs": {
                "fc_cluster_health": {
                    "lvl": "INFO",
                    "msg": "cluster={cluster} fc_ver={fc_ver} hi_nodes={hi_nodes} auto_heal={auto_heal} servicing_updates={servicing}",
                    "vars": {
                        "cluster": {"k": "ch", "v": ["cluster-a"]},
                        "fc_ver": {"k": "ch", "v": ["5.12", "5.13"]},
                        "auto_heal": {"k": "ch", "v": ["true", "false"]},
                        "servicing": {"k": "ch", "v": ["running", "stopped"]},
                    },
                    "state_vars": {
                        "n": {"hi_nodes": {"k": "i", "v": [0, 10]}},
                        "f": {"hi_nodes": {"k": "i", "v": [10, 450]}},
                    },
                },
                "fc_vm_op": {
                    "lvl": "INFO",
                    "msg": "vm_op op={op} deployment={deployment} target_vm={vm_id} ud={ud}",
                    "vars": {
                        "op": {"k": "ch", "v": ["deploy", "scale_out", "service_heal"]},
                        "deployment": {"k": "ch", "v": ["dep-app", "dep-identity", "dep-bus"]},
                        "vm_id": {"k": "ch", "v": ["vm-a1", "vm-a2", "vm-a3", "vm-a4", "vm-a5"]},
                        "ud": {"k": "i", "v": [0, 4]},
                    },
                },
                "fc_hi_threshold_hit": {
                    "lvl": "ERROR",
                    "msg": "HI threshold exceeded: hi_nodes={hi_nodes} threshold={threshold}; entering cluster HI mode",
                    "vars": {"hi_nodes": {"k": "i", "v": [100, 450]}, "threshold": {"k": "i", "v": [80, 150]}},
                },
                "fc_autonomic_change_hi": {
                    "lvl": "WARN",
                    "msg": "autonomic_ops auto_heal=false service_heal=disabled reason=hi_threshold",
                    "vars": {},
                },
                "fc_manual_repair": {
                    "lvl": "INFO",
                    "msg": "manual_repair initiated scope={scope} ticket={ticket}",
                    "vars": {"scope": {"k": "ch", "v": ["cluster-a"]}, "ticket": {"k": "str", "v": "INC-<digits>"}},
                },
            },
            "beh": {
                "n": [{"id": "fc_cluster_health", "per_min": 1.0, "scope": "per_host"}],
                "f": [{"id": "fc_cluster_health", "per_min": 2.0, "scope": "per_host"}],
            },
        },
        {
            "id": "host_agent",
            "svc": "host-agent",
            "hosts": ["host-101", "host-102", "host-103", "host-104", "host-105", "host-106", "host-107"],
            "logs": {
                "ha_host_status": {
                    "lvl": "INFO",
                    "msg": "ha_status ha_ver={ha_ver} plugin={plugin} vms={vm_count} state={state}",
                    "vars": {
                        "ha_ver": {"k": "ch", "v": ["1.4", "1.5"]},
                        "plugin": {"k": "ch", "v": ["np-1.4", "np-1.5"]},
                        "vm_count": {"k": "i", "v": [0, 60]},
                        "state": {"k": "ch", "v": ["ok", "hi", "updating", "corrupted"]},
                    },
                },
                "ha_vm_start": {
                    "lvl": "INFO",
                    "msg": "vm_start vm={vm_id} image={image} ud={ud}",
                    "vars": {
                        "vm_id": {"k": "ch", "v": ["vm-a1", "vm-a2", "vm-a3", "vm-a4", "vm-a5"]},
                        "image": {"k": "ch", "v": ["wa-2012.02", "wa-2012.03"]},
                        "ud": {"k": "i", "v": [0, 4]},
                    },
                },
                "ha_ga_timeout": {
                    "lvl": "WARN",
                    "msg": "ga_connect_timeout vm={vm_id} waited_s={waited_s} attempt={attempt} action={action}",
                    "vars": {
                        "vm_id": {"k": "ch", "v": ["vm-a1", "vm-a2", "vm-a3", "vm-a4", "vm-a5"]},
                        "waited_s": {"k": "i", "v": [300, 1200]},
                        "attempt": {"k": "ch", "v": ["1", "2", "3"]},
                        "action": {"k": "ch", "v": ["restart", "reimage"]},
                    },
                },
                "ha_vm_reimage": {
                    "lvl": "INFO",
                    "msg": "vm_reimage vm={vm_id} reason={reason}",
                    "vars": {
                        "vm_id": {"k": "ch", "v": ["vm-a1", "vm-a2", "vm-a3", "vm-a4", "vm-a5"]},
                        "reason": {"k": "ch", "v": ["ga_timeout", "operator_repair"]},
                    },
                },
                "ha_boot_retry": {
                    "lvl": "INFO",
                    "msg": "retrying_vm_boot vm={vm_id} next_attempt={attempt} backoff_s={backoff_s}",
                    "vars": {
                        "vm_id": {"k": "ch", "v": ["vm-a1", "vm-a2", "vm-a3", "vm-a4", "vm-a5"]},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "backoff_s": {"k": "i", "v": [30, 120]},
                    },
                },
                "ha_network_plugin_incompatible": {
                    "lvl": "ERROR",
                    "msg": "network_plugin_error combo={combo} vm={vm_id} impact={impact}",
                    "vars": {
                        "combo": {"k": "ch", "v": ["ha-1.4+np-1.5"]},
                        "vm_id": {"k": "ch", "v": ["vm-a1", "vm-a2", "vm-a3", "vm-a4", "vm-a5"]},
                        "impact": {"k": "ch", "v": ["no_vnet", "no_connectivity"]},
                    },
                },
                "ha_blast_update_start_rollback": {
                    "lvl": "WARN",
                    "msg": "blast_update start package=rollback_ha_pkg_v1 target_ha=1.4 target_ga=2.2-fixed",
                    "vars": {},
                },
                "ha_blast_update_start_corrected": {
                    "lvl": "WARN",
                    "msg": "blast_update start package=corrected_ha_pkg_v2 target_ha=1.4 target_ga=2.2-fixed",
                    "vars": {},
                },
                "ha_blast_update_complete_corrected": {
                    "lvl": "INFO",
                    "msg": "blast_update complete package=corrected_ha_pkg_v2 result=partial",
                    "vars": {},
                },
            },
            "beh": {
                "n": [{"id": "ha_host_status", "per_min": 0.5, "scope": "per_host"}],
                "f": [
                    {"id": "ha_host_status", "per_min": 1.0, "scope": "per_host"},
                    {"id": "ha_network_plugin_incompatible", "per_min": 0.3, "scope": "per_host"},
                ],
            },
        },
        {
            "id": "guest_agent",
            "svc": "guest-agent",
            "hosts": ["vm-a1", "vm-a2", "vm-a3", "vm-a4", "vm-a5"],
            "logs": {
                "ga_init_start": {
                    "lvl": "INFO",
                    "msg": "ga_init vm={vm_id} utc_date={utc_date}",
                    "vars": {"vm_id": {"k": "ch", "v": ["vm-a1", "vm-a2", "vm-a3", "vm-a4", "vm-a5"]}},
                    "state_vars": {
                        "n": {"utc_date": {"k": "ch", "v": ["2012-02-28"]}},
                        "f": {"utc_date": {"k": "ch", "v": ["2012-02-29"]}},
                    },
                },
                "ga_transfer_cert_error": {
                    "lvl": "ERROR",
                    "msg": "transfer_cert_create_failed vm={vm_id} valid_from={valid_from} valid_to={valid_to} err={err}",
                    "vars": {
                        "vm_id": {"k": "ch", "v": ["vm-a1", "vm-a2", "vm-a3", "vm-a4", "vm-a5"]},
                        "valid_from": {"k": "ch", "v": ["2012-02-29T00:00:00Z"]},
                        "valid_to": {"k": "ch", "v": ["2013-02-29T00:00:00Z"]},
                        "err": {"k": "ch", "v": ["invalid_date"]},
                    },
                },
                "ga_connect_success": {
                    "lvl": "INFO",
                    "msg": "connected_to_host_agent vm={vm_id} transfer_cert_thumbprint={thumb}",
                    "vars": {
                        "vm_id": {"k": "ch", "v": ["vm-a1", "vm-a2", "vm-a3", "vm-a4", "vm-a5"]},
                        "thumb": {"k": "hex", "v": 40},
                    },
                },
            },
            "beh": {"n": [], "f": []},
        },
        {
            "id": "service_mgmt_api",
            "svc": "service-management",
            "hosts": ["sm-01", "sm-02"],
            "logs": {
                "sm_request": {
                    "lvl": "INFO",
                    "msg": "req {method} {route} caller={caller} rid={rid}",
                    "vars": {
                        "method": {"k": "ch", "v": ["POST", "PUT"]},
                        "route": {"k": "ch", "v": ["/deployments", "/scale"]},
                        "caller": {"k": "ch", "v": ["customer", "portal"]},
                        "rid": {"k": "uuid", "v": None},
                    },
                },
                "sm_response_success": {
                    "lvl": "INFO",
                    "msg": "resp rid={rid} status=202 fc_cluster={cluster} took_ms={took_ms}",
                    "vars": {
                        "rid": {"k": "uuid", "v": None},
                        "cluster": {"k": "ch", "v": ["cluster-a"]},
                        "took_ms": {"k": "i", "v": [200, 4000]},
                    },
                },
                "sm_response_disabled": {
                    "lvl": "WARN",
                    "msg": "resp rid={rid} status=503 code=ServiceManagementDisabled took_ms={took_ms}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "took_ms": {"k": "i", "v": [10, 200]}},
                },
                "sm_disable_action_global": {
                    "lvl": "WARN",
                    "msg": "service_management disabled=true scope=global reason=protect_running_services",
                    "vars": {},
                },
                "sm_metric": {
                    "lvl": "INFO",
                    "msg": "metric req_rate_rpm={req_rpm} rejected_rpm={rej_rpm}",
                    "vars": {"req_rpm": {"k": "i", "v": [0, 400]}, "rej_rpm": {"k": "i", "v": [0, 400]}},
                },
            },
            "beh": {
                "n": [{"id": "sm_metric", "per_min": 0.5, "scope": "per_host"}],
                "f": [{"id": "sm_metric", "per_min": 1.0, "scope": "per_host"}],
            },
        },
        {
            "id": "acs_service",
            "svc": "acs",
            "hosts": ["acs-01", "acs-02", "acs-03"],
            "logs": {
                "acs_req": {
                    "lvl": "INFO",
                    "msg": "token_request client={client_ip} scope={scope} rid={rid}",
                    "vars": {
                        "client_ip": {"k": "ip", "v": "10.0.0.0/8"},
                        "scope": {"k": "ch", "v": ["openid", "sb", "sql"]},
                        "rid": {"k": "uuid", "v": None},
                    },
                },
                "acs_resp_200": {
                    "lvl": "INFO",
                    "msg": "token_response rid={rid} status=200 took_ms={took_ms}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "took_ms": {"k": "i", "v": [20, 400]}},
                },
                "acs_upstream_unreachable": {
                    "lvl": "ERROR",
                    "msg": "backend_unreachable rid={rid} reason={reason} waited_ms={waited_ms}",
                    "vars": {
                        "rid": {"k": "uuid", "v": None},
                        "reason": {"k": "ch", "v": ["vm_no_network", "backend_timeout"]},
                        "waited_ms": {"k": "i", "v": [500, 5000]},
                    },
                },
                "acs_resp_503": {
                    "lvl": "WARN",
                    "msg": "token_response rid={rid} status=503 error={error} took_ms={took_ms}",
                    "vars": {
                        "rid": {"k": "uuid", "v": None},
                        "error": {"k": "ch", "v": ["ServiceUnavailable", "GatewayTimeout"]},
                        "took_ms": {"k": "i", "v": [500, 8000]},
                    },
                },
                "acs_metric": {
                    "lvl": "INFO",
                    "msg": "metric active_nodes={nodes} error_rate_pct={err_pct}",
                    "vars": {"nodes": {"k": "i", "v": [0, 10]}, "err_pct": {"k": "f", "v": [0.0, 60.0]}},
                },
            },
            "beh": {
                "n": [{"id": "acs_metric", "per_min": 0.3, "scope": "per_host"}],
                "f": [{"id": "acs_metric", "per_min": 0.6, "scope": "per_host"}],
            },
        },
    ],
    "flows": {
        "n": [
            {
                "id": "service_mgmt_deploy",
                "rpm": 40.0,
                "emit": ["service_mgmt_api.sm_request", "fabric_controller.fc_vm_op", "service_mgmt_api.sm_response_success"],
                "latency_ms": [[5, 20], [20, 120], [50, 400]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "vm_bootstrap_success_n",
                "rpm": 60.0,
                "emit": ["host_agent.ha_vm_start", "guest_agent.ga_init_start", "guest_agent.ga_connect_success"],
                "latency_ms": [[200, 1200], [50, 300], [100, 800]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "acs_token_success_n",
                "rpm": 250.0,
                "emit": ["acs_service.acs_req", "acs_service.acs_resp_200"],
                "latency_ms": [[10, 50], [20, 400]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
        "f": [
            {
                "id": "vm_bootstrap_leapday_fail",
                "rpm": 120.0,
                "emit": [
                    "host_agent.ha_vm_start",
                    "guest_agent.ga_init_start",
                    "guest_agent.ga_transfer_cert_error",
                    "host_agent.ha_ga_timeout",
                    "host_agent.ha_vm_reimage",
                ],
                "latency_ms": [[200, 1200], [50, 250], [5, 30], [450000, 600000], [10000, 30000]],
                "retry": {
                    "max_attempts": 3,
                    "expected_attempts": 2.5,
                    "emit_per_retry": ["host_agent.ha_boot_retry"],
                    "backoff_ms": [[60000, 120000], [60000, 180000]],
                },
                "trace": False,
            },
            {
                "id": "vm_bootstrap_fixed_success_f",
                "rpm": 80.0,
                "emit": ["host_agent.ha_vm_start", "guest_agent.ga_init_start", "guest_agent.ga_connect_success"],
                "latency_ms": [[200, 1500], [50, 400], [150, 1200]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "service_mgmt_request_disabled",
                "rpm": 80.0,
                "emit": ["service_mgmt_api.sm_request", "service_mgmt_api.sm_response_disabled"],
                "latency_ms": [[2, 20], [10, 200]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "acs_token_success_f",
                "rpm": 250.0,
                "emit": ["acs_service.acs_req", "acs_service.acs_resp_200"],
                "latency_ms": [[10, 50], [20, 400]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            {
                "id": "acs_token_timeout_f",
                "rpm": 20.0,
                "emit": ["acs_service.acs_req", "acs_service.acs_upstream_unreachable", "acs_service.acs_resp_503"],
                "latency_ms": [[10, 50], [500, 5000], [500, 8000]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "leap_day_ga_cert_and_bad_ha_plugin"},
    "time": {"total_minutes": 55, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 55}}},
    "events": [
        {
            "order": 1,
            "at_min": 25,
            "rate_multipliers": {
                "vm_bootstrap_leapday_fail": 1.2,
                "vm_bootstrap_fixed_success_f": 0.0,
                "service_mgmt_request_disabled": 0.0,
                "acs_token_timeout_f": 0.0,
                "host_agent.ha_network_plugin_incompatible": 0.0,
            },
            "latency_multipliers": {"vm_bootstrap_leapday_fail": {"p50": 1.0, "p95": 1.0}},
            "one_shots": [],
        },
        {
            "order": 2,
            "at_min": 35,
            "rate_multipliers": {
                "vm_bootstrap_leapday_fail": 0.5,
                "vm_bootstrap_fixed_success_f": 0.0,
                "service_mgmt_request_disabled": 1.5,
                "acs_token_timeout_f": 0.0,
                "host_agent.ha_network_plugin_incompatible": 0.0,
                "acs_token_success_f": 0.9,
            },
            "latency_multipliers": {"vm_bootstrap_leapday_fail": {"p50": 1.0, "p95": 1.0}},
            "one_shots": [
                {"ref": "fabric_controller.fc_hi_threshold_hit", "count": 1, "hosts": ["fc-01"]},
                {"ref": "fabric_controller.fc_autonomic_change_hi", "count": 1, "hosts": ["fc-01"]},
                {"ref": "service_mgmt_api.sm_disable_action_global", "count": 1, "hosts": ["sm-01"]},
            ],
        },
        {
            "order": 3,
            "at_min": 43,
            "rate_multipliers": {
                "vm_bootstrap_leapday_fail": 0.0,
                "vm_bootstrap_fixed_success_f": 1.0,
                "service_mgmt_request_disabled": 1.5,
                "host_agent.ha_network_plugin_incompatible": 30.0,
                "acs_token_timeout_f": 20.0,
                "acs_token_success_f": 0.2,
            },
            "latency_multipliers": {"acs_token_timeout_f": {"p50": 1.4, "p95": 1.3}},
            "one_shots": [
                {
                    "ref": "host_agent.ha_blast_update_start_rollback",
                    "count": 7,
                    "hosts": ["host-101", "host-102", "host-103", "host-104", "host-105", "host-106", "host-107"],
                }
            ],
        },
        {
            "order": 4,
            "at_min": 50,
            "rate_multipliers": {
                "vm_bootstrap_leapday_fail": 0.0,
                "vm_bootstrap_fixed_success_f": 1.0,
                "service_mgmt_request_disabled": 1.2,
                "host_agent.ha_network_plugin_incompatible": 0.5,
                "acs_token_timeout_f": 4.0,
                "acs_token_success_f": 0.6,
            },
            "latency_multipliers": {"acs_token_timeout_f": {"p50": 1.1, "p95": 1.1}},
            "one_shots": [
                {
                    "ref": "host_agent.ha_blast_update_start_corrected",
                    "count": 7,
                    "hosts": ["host-101", "host-102", "host-103", "host-104", "host-105", "host-106", "host-107"],
                },
                {
                    "ref": "host_agent.ha_blast_update_complete_corrected",
                    "count": 7,
                    "hosts": ["host-101", "host-102", "host-103", "host-104", "host-105", "host-106", "host-107"],
                },
                {"ref": "fabric_controller.fc_manual_repair", "count": 1, "hosts": ["fc-02"]},
            ],
        },
    ],
}

# -----------------------------
# Deterministic helpers
# -----------------------------

BASE_TIME = datetime(2012, 2, 28, 23, 35, 0, tzinfo=timezone.utc)  # minute 25 => 2012-02-29T00:00:00Z
BASE_EPOCH = BASE_TIME.timestamp()


def hash32(s: str) -> int:
    return zlib.crc32(s.encode("utf-8")) & 0xFFFFFFFF


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def minutes_to_epoch_s(minute: float) -> float:
    return BASE_EPOCH + float(minute) * 60.0


SCENARIO_END_S = minutes_to_epoch_s(SCENARIO["time"]["total_minutes"])
HORIZON_EPS_S = 0.0005

# Acklam's inverse normal CDF approximation (deterministic, no SciPy).
def inv_norm_cdf(p: float) -> float:
    p = clamp(p, 1e-12, 1.0 - 1e-12)
    a = [
        -3.969683028665376e01,
        2.209460984245205e02,
        -2.759285104469687e02,
        1.383577518672690e02,
        -3.066479806614716e01,
        2.506628277459239e00,
    ]
    b = [
        -5.447609879822406e01,
        1.615858368580409e02,
        -1.556989798598866e02,
        6.680131188771972e01,
        -1.328068155288572e01,
    ]
    c = [
        -7.784894002430293e-03,
        -3.223964580411365e-01,
        -2.400758277161838e00,
        -2.549732539343734e00,
        4.374664141464968e00,
        2.938163982698783e00,
    ]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]
    plow = 0.02425
    phigh = 1 - plow
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / (
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
        )
    q = p - 0.5
    r = q * q
    return (
        (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
        / (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    )


Z95 = 1.6448536269514722


def lognormal_sample_ms(p50_ms: float, p95_ms: float, u: float, cap_mult: float) -> int:
    p50_ms = max(1.0, float(p50_ms))
    p95_ms = max(p50_ms * 1.0001, float(p95_ms))
    mu = math.log(p50_ms)
    sigma = (math.log(p95_ms) - mu) / Z95
    z = inv_norm_cdf(clamp(u, 1e-6, 1 - 1e-6))
    val = math.exp(mu + sigma * z)
    cap = cap_mult * p95_ms
    val = clamp(val, 1.0, cap)
    return int(round(val))


def sample_delay_ms(
    p50_ms: float,
    p95_ms: float,
    r: random.Random,
    p50_mult: float = 1.0,
    p95_mult: float = 1.0,
    cap_mult: float = 3.0,
    min_ms: Optional[int] = None,
    max_ms: Optional[int] = None,
) -> int:
    ms = lognormal_sample_ms(p50_ms * p50_mult, p95_ms * p95_mult, u=r.random(), cap_mult=cap_mult)
    if min_ms is not None:
        ms = max(int(min_ms), int(ms))
    if max_ms is not None:
        ms = min(int(max_ms), int(ms))
    return int(max(1, ms))


def schedule_uniform(start_s: float, end_s: float, n: int, key: str) -> List[float]:
    if n <= 0:
        return []
    # Guard against zero/negative windows to avoid generating times before start_s.
    end_s = max(float(end_s), float(start_s) + 0.001)
    dur = max(0.001, end_s - start_s)
    out: List[float] = []
    hi = end_s - HORIZON_EPS_S
    if hi < start_s:
        hi = start_s
    for i in range(n):
        frac = (i + 0.5) / n
        jitter_ms = (hash32(f"{key}|j|{i}") % 201) - 100  # [-100, 100] ms
        t = start_s + frac * dur + (jitter_ms / 1000.0)
        t = clamp(t, start_s, hi)
        out.append(t)
    return out


def iso_utc_ms(epoch_s: float) -> str:
    dt = datetime.fromtimestamp(epoch_s, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond/1000):03d}Z"


def rnd_hex(r: random.Random, n: int) -> str:
    alphabet = "0123456789abcdef"
    return "".join(alphabet[r.randrange(16)] for _ in range(n))


def rnd_uuid(r: random.Random) -> str:
    u = uuid.UUID(int=r.getrandbits(128))
    return str(u)


def rnd_ip_10_8(r: random.Random) -> str:
    return f"10.{r.randrange(0,256)}.{r.randrange(0,256)}.{r.randrange(1,255)}"


def stable_round(expected: float, carry: float) -> Tuple[int, float]:
    x = expected + carry
    n = int(math.floor(x + 1e-12))
    carry = x - n
    return n, carry


@dataclass
class ControlInterval:
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    latency_mult: Dict[str, Dict[str, float]]


# -----------------------------
# Build indices
# -----------------------------

components: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

log_defs: Dict[str, Dict[str, Any]] = {}
for cid, c in components.items():
    for lid, ld in c["logs"].items():
        log_defs[f"{cid}.{lid}"] = {"component_id": cid, "log_id": lid, **ld}

flows_by_state: Dict[str, Dict[str, Any]] = {"n": {}, "f": {}}
for st in ["n", "f"]:
    for f in SYSTEM["flows"][st]:
        flows_by_state[st][f["id"]] = f


def comp_host_for_instance(comp_id: str, seed_key: str) -> str:
    hosts = components[comp_id].get("hosts", []) or []
    if not hosts:
        return ""
    idx = hash32(seed_key + "|" + comp_id) % len(hosts)
    return hosts[idx]


# -----------------------------
# Derive failure control intervals
# -----------------------------

f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
events = sorted(SCENARIO["events"], key=lambda e: (e["at_min"], e["order"]))

control_intervals: List[ControlInterval] = []
active_rate: Dict[str, float] = {}
active_lat: Dict[str, Dict[str, float]] = {}

boundaries = [f_start] + [e["at_min"] for e in events] + [f_end]
boundaries = sorted(set(boundaries))

for i, b in enumerate(boundaries[:-1]):
    for e in [ev for ev in events if ev["at_min"] == b]:
        for k, v in e.get("rate_multipliers", {}).items():
            active_rate[k] = float(v)
        for k, v in e.get("latency_multipliers", {}).items():
            active_lat[k] = {"p50": float(v.get("p50", 1.0)), "p95": float(v.get("p95", 1.0))}
    control_intervals.append(
        ControlInterval(start_min=b, end_min=boundaries[i + 1], rate_mult=dict(active_rate), latency_mult=dict(active_lat))
    )


def controls_for_minute(minute: int) -> ControlInterval:
    for ci in control_intervals:
        if ci.start_min <= minute < ci.end_min:
            return ci
    return ControlInterval(start_min=minute, end_min=minute + 1, rate_mult={}, latency_mult={})


# -----------------------------
# Emission rendering
# -----------------------------

def choose_from_domain(dom: Dict[str, Any], r: random.Random) -> Any:
    k = dom["k"]
    v = dom["v"]
    if k == "ch":
        return v[r.randrange(len(v))]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return int(lo + (hi - lo) * r.random())
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        return round(lo + (hi - lo) * r.random(), 1)
    if k == "uuid":
        return rnd_uuid(r)
    if k == "hex":
        return rnd_hex(r, int(v))
    if k == "ip":
        return rnd_ip_10_8(r)
    if k == "str":
        if isinstance(v, str) and "<digits>" in v:
            return v.replace("<digits>", f"{r.randrange(100000, 999999)}")
        return str(v)
    return str(v)


def render_message(ref: str, state: str, bound: Dict[str, Any], seed_key: str) -> Tuple[str, str, str]:
    ld = log_defs[ref]
    cid = ld["component_id"]
    service = components[cid].get("svc", "") or ""
    r = random.Random(hash32("msg|" + seed_key + "|" + ref))
    vars_all: Dict[str, Any] = {}

    for k, dom in ld.get("vars", {}).items():
        vars_all[k] = bound[k] if k in bound else choose_from_domain(dom, r)

    sv = ld.get("state_vars", {}).get(state, {})
    for k, dom in sv.items():
        vars_all[k] = bound[k] if k in bound else choose_from_domain(dom, r)

    msg = ld["msg"].format(**vars_all)
    lvl = ld["lvl"]
    return lvl, msg, service


# -----------------------------
# Count planning (stable rounding)
# -----------------------------

def latency_multiplier_for_flow(flow_id: str, controls: Optional[ControlInterval]) -> Tuple[float, float]:
    if controls is None:
        return 1.0, 1.0
    m = controls.latency_mult.get(flow_id)
    if not m:
        return 1.0, 1.0
    return float(m.get("p50", 1.0)), float(m.get("p95", 1.0))


def estimate_chain_guard_s(flow: Dict[str, Any], controls: Optional[ControlInterval]) -> float:
    """
    Soft guard used only to bias start-time scheduling earlier so long retrying chains
    are less likely to spill past scenario end. This does not change retry/latency semantics,
    it only influences where starts are placed within each interval.
    """
    flow_id = flow["id"]
    exp_a = float(flow["retry"]["expected_attempts"])
    p50m, _p95m = latency_multiplier_for_flow(flow_id, controls)

    per_attempt_ms = 0.0
    for p50, _p95 in flow.get("latency_ms", []):
        per_attempt_ms += float(p50) * p50m

    backoff_ms_total = 0.0
    backoffs = max(0.0, exp_a - 1.0)
    pairs = flow["retry"].get("backoff_ms", []) or []
    if backoffs > 0 and pairs:
        full = int(math.floor(backoffs + 1e-12))
        frac = clamp(backoffs - full, 0.0, 1.0)
        for i in range(full):
            j = min(i, len(pairs) - 1)
            backoff_ms_total += float(pairs[j][0])
        if frac > 0:
            j = min(full, len(pairs) - 1)
            backoff_ms_total += frac * float(pairs[j][0])

    # Apply a small safety factor so scheduling has margin but doesn't eliminate long chains entirely.
    guard_s = 1.15 * ((per_attempt_ms * exp_a) + backoff_ms_total) / 1000.0
    return float(max(0.0, guard_s))


def plan_flow_instances(state: str, start_min: int, end_min: int, controls: Optional[ControlInterval]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    dur_min = end_min - start_min
    carry_per_flow: Dict[str, float] = {}

    flows = SYSTEM["flows"][state]
    for f in flows:
        flow_id = f["id"]
        base_rpm = float(f["rpm"])
        mult = 1.0
        if state == "f" and controls is not None:
            mult = float(controls.rate_mult.get(flow_id, 1.0))
        eff_rpm = base_rpm * mult
        expected = eff_rpm * dur_min
        carry = carry_per_flow.get(flow_id, 0.0)
        n, carry = stable_round(expected, carry)
        carry_per_flow[flow_id] = carry

        if n <= 0:
            continue

        max_a = int(f["retry"]["max_attempts"])
        exp_a = float(f["retry"]["expected_attempts"])
        base_a = int(math.floor(exp_a + 1e-12))
        frac = clamp(exp_a - base_a, 0.0, 1.0)
        n_plus = int(round(frac * n))
        base_a = int(clamp(base_a, 1, max_a))

        start_s = minutes_to_epoch_s(start_min)
        end_s = minutes_to_epoch_s(end_min)

        # Softly constrain start times so long chains tend to begin earlier, reducing post-scenario spillover.
        guard_s = estimate_chain_guard_s(f, controls if state == "f" else None)
        soft_end_s = min(end_s, SCENARIO_END_S - guard_s)
        if soft_end_s <= start_s + 0.25:
            # If the interval is too close to the scenario end for this flow's expected chain,
            # still schedule within the interval but biased to the first ~minute of the interval.
            soft_end_s = min(end_s, start_s + min(60.0, max(0.25, end_s - start_s)))

        times = schedule_uniform(start_s, soft_end_s, n, key=f"flow|{state}|{flow_id}|{start_min}-{end_min}")

        for i, t in enumerate(times):
            attempts = int(base_a + (1 if i < n_plus else 0))
            attempts = int(clamp(attempts, 1, max_a))
            out.append(
                {
                    "state": state,
                    "flow_id": flow_id,
                    "start_s": float(min(t, SCENARIO_END_S - HORIZON_EPS_S)),
                    "batch_start_min": start_min,
                    "batch_end_min": end_min,
                    "batch_index": i,
                    "batch_size": n,
                    "attempts": attempts,
                }
            )
    return out


def plan_background(state: str, start_min: int, end_min: int, controls: Optional[ControlInterval]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    dur_min = end_min - start_min
    for cid, comp in components.items():
        beh = comp.get("beh", {}).get(state, [])
        for be in beh:
            log_id = be["id"]
            per_min = float(be["per_min"])
            scope = be.get("scope", "per_host")
            ref = f"{cid}.{log_id}"
            mult = 1.0
            if state == "f" and controls is not None:
                mult = float(controls.rate_mult.get(ref, 1.0))
            eff_per_min = per_min * mult
            if eff_per_min <= 0:
                continue

            start_s = minutes_to_epoch_s(start_min)
            end_s = min(minutes_to_epoch_s(end_min), SCENARIO_END_S)

            if scope == "global":
                expected = eff_per_min * dur_min
                n, _ = stable_round(expected, 0.0)
                times = schedule_uniform(start_s, end_s, n, key=f"bg|{state}|{ref}|global|{start_min}-{end_min}")
                for i, t in enumerate(times):
                    out.append({"state": state, "ref": ref, "ts_s": t, "host": comp_host_for_instance(cid, f"bg|{ref}|{start_min}|{i}")})
            else:
                hosts = comp.get("hosts", []) or [""]
                for h in hosts:
                    expected = eff_per_min * dur_min
                    n, _ = stable_round(expected, 0.0)
                    times = schedule_uniform(start_s, end_s, n, key=f"bg|{state}|{ref}|{h}|{start_min}-{end_min}")
                    for i, t in enumerate(times):
                        out.append({"state": state, "ref": ref, "ts_s": t, "host": h})
    return out


# -----------------------------
# Flow simulation
# -----------------------------

def sample_delays_ms(
    lat_pairs: List[List[float]], p50_mult: float, p95_mult: float, r: random.Random, cap_mult: float = 3.0
) -> List[int]:
    out: List[int] = []
    for (p50, p95) in lat_pairs:
        ms = lognormal_sample_ms(p50 * p50_mult, p95 * p95_mult, u=r.random(), cap_mult=cap_mult)
        out.append(ms)
    return out


def sample_backoff_ms(pair: List[float], r: random.Random, hard_cap_ms: Optional[int] = None) -> int:
    p50, p95 = float(pair[0]), float(pair[1])
    ms = lognormal_sample_ms(p50, p95, u=r.random(), cap_mult=2.5)
    if hard_cap_ms is not None:
        ms = int(min(ms, hard_cap_ms))
    return int(max(1, ms))


# Internal row tuple includes an index for stable sorting: (ts_s, idx, level, message, trace_id, service, host)
Row = Tuple[float, int, str, str, str, str, str]


def emit(rows: List[Row], ts_s: float, ref: str, state: str, trace_id: str, host: str, bound: Dict[str, Any], seed_key: str) -> bool:
    # Hard scenario horizon: never emit logs beyond scenario end.
    if ts_s > SCENARIO_END_S - HORIZON_EPS_S:
        return False
    lvl, msg, svc = render_message(ref, state, bound, seed_key)
    rows.append((ts_s, len(rows), lvl, msg, trace_id, svc, host))
    return True


def gen_vm_id(idx: int) -> str:
    return ["vm-a1", "vm-a2", "vm-a3", "vm-a4", "vm-a5"][idx % 5]


def gen_ud(idx: int) -> int:
    return idx % 5


def simulate_flow_instance(inst: Dict[str, Any], rows: List[Row]) -> None:
    state = inst["state"]
    flow_id = inst["flow_id"]
    flow = flows_by_state[state][flow_id]
    start_s = float(inst["start_s"])
    attempts = int(inst["attempts"])
    batch_key = f"{state}|{flow_id}|{inst['batch_start_min']}-{inst['batch_end_min']}|{inst['batch_index']}"
    r = random.Random(hash32("flow|" + batch_key))

    if start_s > SCENARIO_END_S - HORIZON_EPS_S:
        return

    minute = int(math.floor((start_s - BASE_EPOCH) / 60.0))
    ci = controls_for_minute(minute) if state == "f" else None
    p50m, p95m = latency_multiplier_for_flow(flow_id, ci)

    trace_id = ""
    if flow.get("trace", False):
        trace_id = rnd_hex(random.Random(hash32("trace|" + batch_key)), 32)

    host_map: Dict[str, str] = {}

    def host_for_ref(ref: str, vm_id: Optional[str] = None) -> str:
        cid = log_defs[ref]["component_id"]
        if cid == "guest_agent" and vm_id is not None:
            return vm_id
        if cid not in host_map:
            host_map[cid] = comp_host_for_instance(cid, "host|" + batch_key)
        return host_map[cid]

    lat_pairs = flow["latency_ms"]

    # ---- Service management flows (prefer all-or-nothing within horizon) ----
    if flow_id in ("service_mgmt_deploy", "service_mgmt_request_disabled"):
        rid = rnd_uuid(random.Random(hash32("rid|" + batch_key)))
        route = "/deployments" if (inst["batch_index"] % 3 != 2) else "/scale"
        method = "POST" if route == "/deployments" else "PUT"
        caller = "customer" if (inst["batch_index"] % 2 == 0) else "portal"
        op = "deploy" if route == "/deployments" else "scale_out"
        deployment = "dep-app" if op == "scale_out" else ("dep-identity" if (inst["batch_index"] % 5 == 1) else "dep-app")
        vm_id = gen_vm_id(inst["batch_index"])
        ud = gen_ud(inst["batch_index"])

        if flow_id == "service_mgmt_deploy":
            d0 = sample_delay_ms(lat_pairs[0][0], lat_pairs[0][1], r, p50m, p95m, cap_mult=2.0)
            d1 = sample_delay_ms(lat_pairs[1][0], lat_pairs[1][1], r, p50m, p95m, cap_mult=2.0)
            d2 = sample_delay_ms(lat_pairs[2][0], lat_pairs[2][1], r, p50m, p95m, cap_mult=2.0)

            took = d1 + d2
            if took < 200:
                d2 += (200 - took)
                took = d1 + d2
            if took > 4000:
                scale = 4000.0 / float(took)
                d1 = max(1, int(round(d1 * scale)))
                d2 = max(1, int(round(d2 * scale)))
                took = d1 + d2

            t_req = start_s + d0 / 1000.0
            t_fc = t_req + d1 / 1000.0
            t_resp = t_fc + d2 / 1000.0
            if t_resp > SCENARIO_END_S - HORIZON_EPS_S:
                return

            if not emit(
                rows,
                t_req,
                "service_mgmt_api.sm_request",
                state,
                trace_id,
                host_for_ref("service_mgmt_api.sm_request"),
                {"method": method, "route": route, "caller": caller, "rid": rid},
                batch_key + "|1",
            ):
                return
            if not emit(
                rows,
                t_fc,
                "fabric_controller.fc_vm_op",
                state,
                trace_id,
                host_for_ref("fabric_controller.fc_vm_op"),
                {"op": op, "deployment": deployment, "vm_id": vm_id, "ud": ud},
                batch_key + "|2",
            ):
                return
            took_ms = int(round((t_resp - t_req) * 1000.0))
            emit(
                rows,
                t_resp,
                "service_mgmt_api.sm_response_success",
                state,
                trace_id,
                host_for_ref("service_mgmt_api.sm_response_success"),
                {"rid": rid, "cluster": "cluster-a", "took_ms": took_ms},
                batch_key + "|3",
            )
        else:
            d0 = sample_delay_ms(lat_pairs[0][0], lat_pairs[0][1], r, p50m, p95m, cap_mult=2.0)
            d1 = sample_delay_ms(lat_pairs[1][0], lat_pairs[1][1], r, p50m, p95m, cap_mult=2.0, min_ms=10, max_ms=200)
            t_req = start_s + d0 / 1000.0
            t_resp = t_req + d1 / 1000.0
            if t_resp > SCENARIO_END_S - HORIZON_EPS_S:
                return

            if not emit(
                rows,
                t_req,
                "service_mgmt_api.sm_request",
                state,
                trace_id,
                host_for_ref("service_mgmt_api.sm_request"),
                {"method": method, "route": route, "caller": caller, "rid": rid},
                batch_key + "|1",
            ):
                return
            took_ms = int(round((t_resp - t_req) * 1000.0))
            emit(
                rows,
                t_resp,
                "service_mgmt_api.sm_response_disabled",
                state,
                trace_id,
                host_for_ref("service_mgmt_api.sm_response_disabled"),
                {"rid": rid, "took_ms": took_ms},
                batch_key + "|2",
            )
        return

    # ---- ACS flows (prefer all-or-nothing within horizon) ----
    if flow_id in ("acs_token_success_n", "acs_token_success_f", "acs_token_timeout_f"):
        rid = rnd_uuid(random.Random(hash32("rid|" + batch_key)))
        rr = random.Random(hash32("acsreq|" + batch_key))
        client_ip = rnd_ip_10_8(rr)
        scope = ["openid", "sb", "sql"][inst["batch_index"] % 3]

        if flow_id in ("acs_token_success_n", "acs_token_success_f"):
            d0 = sample_delay_ms(lat_pairs[0][0], lat_pairs[0][1], r, p50m, p95m, cap_mult=3.0)
            d1 = sample_delay_ms(lat_pairs[1][0], lat_pairs[1][1], r, p50m, p95m, cap_mult=2.0, min_ms=20, max_ms=400)
            t_req = start_s + d0 / 1000.0
            t_resp = t_req + d1 / 1000.0
            if t_resp > SCENARIO_END_S - HORIZON_EPS_S:
                return
            if not emit(
                rows,
                t_req,
                "acs_service.acs_req",
                state,
                trace_id,
                host_for_ref("acs_service.acs_req"),
                {"client_ip": client_ip, "scope": scope, "rid": rid},
                batch_key + "|1",
            ):
                return
            took_ms = int(round((t_resp - t_req) * 1000.0))
            emit(
                rows,
                t_resp,
                "acs_service.acs_resp_200",
                state,
                trace_id,
                host_for_ref("acs_service.acs_resp_200"),
                {"rid": rid, "took_ms": took_ms},
                batch_key + "|2",
            )
            return

        d0 = sample_delay_ms(lat_pairs[0][0], lat_pairs[0][1], r, p50m, p95m, cap_mult=3.0)
        waited_ms = sample_delay_ms(lat_pairs[1][0], lat_pairs[1][1], r, p50m, p95m, cap_mult=2.5, min_ms=500, max_ms=5000)

        max_total = 8000
        max_d2 = max(1, max_total - waited_ms)
        d2 = sample_delay_ms(lat_pairs[2][0], lat_pairs[2][1], r, p50m, p95m, cap_mult=2.5, min_ms=1, max_ms=max_d2)

        reason = "vm_no_network" if (ci is not None and ci.start_min >= 43) else "backend_timeout"
        error = "ServiceUnavailable" if reason == "vm_no_network" else "GatewayTimeout"

        t_req = start_s + d0 / 1000.0
        t_un = t_req + waited_ms / 1000.0
        t_resp = t_un + d2 / 1000.0
        if t_resp > SCENARIO_END_S - HORIZON_EPS_S:
            return

        if not emit(
            rows,
            t_req,
            "acs_service.acs_req",
            state,
            trace_id,
            host_for_ref("acs_service.acs_req"),
            {"client_ip": client_ip, "scope": scope, "rid": rid},
            batch_key + "|1",
        ):
            return
        if not emit(
            rows,
            t_un,
            "acs_service.acs_upstream_unreachable",
            state,
            trace_id,
            host_for_ref("acs_service.acs_upstream_unreachable"),
            {"rid": rid, "reason": reason, "waited_ms": waited_ms},
            batch_key + "|2",
        ):
            return
        took_ms = int(round((t_resp - t_req) * 1000.0))
        took_ms = int(clamp(took_ms, 500, 8000))
        emit(
            rows,
            t_resp,
            "acs_service.acs_resp_503",
            state,
            trace_id,
            host_for_ref("acs_service.acs_resp_503"),
            {"rid": rid, "error": error, "took_ms": took_ms},
            batch_key + "|3",
        )
        return

    # ---- VM bootstrap success flows ----
    if flow_id in ("vm_bootstrap_success_n", "vm_bootstrap_fixed_success_f"):
        vm_id = gen_vm_id(inst["batch_index"])
        ud = gen_ud(inst["batch_index"])
        image = "wa-2012.02" if (inst["batch_index"] % 2 == 0) else "wa-2012.03"
        thumb = rnd_hex(random.Random(hash32("thumb|" + batch_key)), 40)

        delays = sample_delays_ms(lat_pairs, p50m, p95m, r, cap_mult=3.0)

        t1 = start_s + delays[0] / 1000.0
        t2 = t1 + delays[1] / 1000.0
        t3 = t2 + delays[2] / 1000.0
        if t3 > SCENARIO_END_S - HORIZON_EPS_S:
            return

        if not emit(
            rows,
            t1,
            "host_agent.ha_vm_start",
            state,
            "",
            host_for_ref("host_agent.ha_vm_start"),
            {"vm_id": vm_id, "image": image, "ud": ud},
            batch_key + "|1",
        ):
            return
        if not emit(
            rows,
            t2,
            "guest_agent.ga_init_start",
            state,
            "",
            host_for_ref("guest_agent.ga_init_start", vm_id=vm_id),
            {"vm_id": vm_id},
            batch_key + "|2",
        ):
            return
        emit(
            rows,
            t3,
            "guest_agent.ga_connect_success",
            state,
            "",
            host_for_ref("guest_agent.ga_connect_success", vm_id=vm_id),
            {"vm_id": vm_id, "thumb": thumb},
            batch_key + "|3",
        )
        return

    # ---- VM bootstrap leap-day failure with retries (may partially spill; horizon stops emissions) ----
    if flow_id == "vm_bootstrap_leapday_fail":
        vm_id = gen_vm_id(inst["batch_index"])
        ud = gen_ud(inst["batch_index"])
        image = "wa-2012.02" if (inst["batch_index"] % 2 == 0) else "wa-2012.03"

        attempt_start = start_s
        for a in range(1, attempts + 1):
            if attempt_start > SCENARIO_END_S - HORIZON_EPS_S:
                break

            if a >= 2:
                backoff_pair = flow["retry"]["backoff_ms"][a - 2]
                backoff_ms = sample_backoff_ms(backoff_pair, r, hard_cap_ms=180000)
                backoff_s = int(clamp(int(round(backoff_ms / 1000.0)), 30, 120))
                attempt_start = attempt_start + float(backoff_s)
                if not emit(
                    rows,
                    attempt_start,
                    "host_agent.ha_boot_retry",
                    state,
                    "",
                    host_for_ref("host_agent.ha_boot_retry"),
                    {"vm_id": vm_id, "attempt": a, "backoff_s": backoff_s},
                    batch_key + f"|retry|{a}",
                ):
                    break

            delays = sample_delays_ms(lat_pairs, p50m, p95m, r, cap_mult=3.0)

            waited_s = int(clamp(int(round(delays[3] / 1000.0)), 300, 1200))
            delays[3] = waited_s * 1000  # enforce schedule/message agreement

            action = "restart" if a < attempts else "reimage"

            t = attempt_start + delays[0] / 1000.0
            if not emit(
                rows,
                t,
                "host_agent.ha_vm_start",
                state,
                "",
                host_for_ref("host_agent.ha_vm_start"),
                {"vm_id": vm_id, "image": image, "ud": ud},
                batch_key + f"|a{a}|1",
            ):
                break
            t += delays[1] / 1000.0
            if not emit(
                rows,
                t,
                "guest_agent.ga_init_start",
                state,
                "",
                host_for_ref("guest_agent.ga_init_start", vm_id=vm_id),
                {"vm_id": vm_id},
                batch_key + f"|a{a}|2",
            ):
                break
            t += delays[2] / 1000.0
            if not emit(
                rows,
                t,
                "guest_agent.ga_transfer_cert_error",
                state,
                "",
                host_for_ref("guest_agent.ga_transfer_cert_error", vm_id=vm_id),
                {"vm_id": vm_id, "valid_from": "2012-02-29T00:00:00Z", "valid_to": "2013-02-29T00:00:00Z", "err": "invalid_date"},
                batch_key + f"|a{a}|3",
            ):
                break
            t += delays[3] / 1000.0
            if not emit(
                rows,
                t,
                "host_agent.ha_ga_timeout",
                state,
                "",
                host_for_ref("host_agent.ha_ga_timeout"),
                {"vm_id": vm_id, "waited_s": waited_s, "attempt": str(a), "action": action},
                batch_key + f"|a{a}|4",
            ):
                break
            t += delays[4] / 1000.0
            if not emit(
                rows,
                t,
                "host_agent.ha_vm_reimage",
                state,
                "",
                host_for_ref("host_agent.ha_vm_reimage"),
                {"vm_id": vm_id, "reason": "ga_timeout"},
                batch_key + f"|a{a}|5",
            ):
                break

            attempt_start = t + 0.001
        return

    # Generic fallback (not expected to be hit in this model).
    delays = sample_delays_ms(lat_pairs, p50m, p95m, r, cap_mult=3.0)
    t = start_s
    for j, ref in enumerate(flow["emit"]):
        t += delays[j] / 1000.0
        if not emit(rows, t, ref, state, trace_id, host_for_ref(ref), {}, batch_key + f"|gen|{j}"):
            break


# -----------------------------
# Background simulation and one-shots
# -----------------------------

def compute_hi_nodes(minute: int) -> int:
    if minute < f_start:
        return int(clamp(2 + (minute % 9), 0, 10))
    frac = clamp((minute - f_start) / max(1, (f_end - f_start)), 0.0, 1.0)
    return int(clamp(round(10 + frac * 430), 10, 450))


def emit_background_entry(bg: Dict[str, Any], rows: List[Row]) -> None:
    state = bg["state"]
    ref = bg["ref"]
    ts_s = float(bg["ts_s"])
    host = bg["host"]
    seed_key = f"bg|{state}|{ref}|{host}|{int(ts_s*1000)}"
    bound: Dict[str, Any] = {}

    minute = int(math.floor((ts_s - BASE_EPOCH) / 60.0))

    if ref == "fabric_controller.fc_cluster_health":
        bound["cluster"] = "cluster-a"
        bound["fc_ver"] = "5.13" if (host.endswith("02") or minute >= 35) else "5.12"
        hi = compute_hi_nodes(minute) if state == "f" else int(clamp(minute % 11, 0, 10))
        bound["hi_nodes"] = hi
        if state == "f" and minute >= 35:
            bound["auto_heal"] = "false"
            bound["servicing"] = "stopped"
        else:
            bound["auto_heal"] = "true"
            bound["servicing"] = "running"

    elif ref == "host_agent.ha_host_status":
        bound["ha_ver"] = "1.4" if minute >= 43 else ("1.5" if (hash32(host) % 4 == 0 and state == "n") else "1.4")
        bound["plugin"] = "np-1.5" if minute >= 43 else "np-1.4"
        bound["vm_count"] = int(clamp(20 + (hash32(host) % 25), 0, 60))
        if state == "n":
            bound["state"] = "ok" if (hash32(host) % 10 != 0) else "updating"
        else:
            if minute < 35:
                bound["state"] = "hi" if (hash32(host) % 3 != 0) else "ok"
            elif minute < 43:
                bound["state"] = "hi" if (hash32(host) % 2 == 0) else "corrupted"
            elif minute < 50:
                bound["state"] = "updating" if (hash32(host) % 2 == 0) else "corrupted"
            else:
                bound["state"] = "hi" if (hash32(host) % 2 == 0) else "corrupted"

    elif ref == "host_agent.ha_network_plugin_incompatible":
        bound["combo"] = "ha-1.4+np-1.5"
        bound["vm_id"] = gen_vm_id(hash32(host + str(minute)) % 1000)
        bound["impact"] = "no_vnet" if (hash32(host + "|imp") % 3 != 0) else "no_connectivity"

    elif ref == "service_mgmt_api.sm_metric":
        ci = controls_for_minute(minute) if minute >= f_start else None
        mult = 0.0
        if state == "f" and ci is not None:
            mult = float(ci.rate_mult.get("service_mgmt_request_disabled", 0.0))
        if state == "n":
            req = 40
            rej = 0
        else:
            req = int(clamp(round(80.0 * mult), 0, 400))
            rej = req if req > 0 else 0
        bound["req_rpm"] = req
        bound["rej_rpm"] = rej

    elif ref == "acs_service.acs_metric":
        if state == "n":
            nodes = 10 - (minute % 2)
            err = 1.5 + (minute % 3) * 0.7
        else:
            if minute < 35:
                nodes = 9
                err = 8.0
            elif minute < 43:
                nodes = 7
                err = 15.0
            elif minute < 50:
                nodes = 3
                err = 55.0
            else:
                nodes = 6
                err = 28.0
        bound["nodes"] = int(clamp(nodes, 0, 10))
        bound["err_pct"] = float(clamp(err, 0.0, 60.0))

    emit(rows, ts_s, ref, state, "", host, bound, seed_key)


def emit_one_shots(rows: List[Row]) -> None:
    for e in events:
        at_min = int(e["at_min"])
        at_s = minutes_to_epoch_s(at_min)
        for os in e.get("one_shots", []):
            ref = os["ref"]
            count = int(os["count"])
            hosts = os.get("hosts", []) or [""]
            times = schedule_uniform(at_s, min(at_s + 0.25, SCENARIO_END_S), count, key=f"oneshot|{ref}|{at_min}|{count}")
            for i, ts_s in enumerate(times):
                if ts_s > SCENARIO_END_S - HORIZON_EPS_S:
                    continue
                host = hosts[i % len(hosts)]
                cid = log_defs[ref]["component_id"]
                svc = components[cid].get("svc", "") or ""
                seed_key = f"oneshot|{ref}|{at_min}|{i}|{host}"
                bound: Dict[str, Any] = {}
                if ref == "fabric_controller.fc_hi_threshold_hit":
                    bound["hi_nodes"] = int(clamp(compute_hi_nodes(at_min), 100, 450))
                    bound["threshold"] = 100
                elif ref == "fabric_controller.fc_manual_repair":
                    rr = random.Random(hash32("repair|" + seed_key))
                    bound["scope"] = "cluster-a"
                    bound["ticket"] = f"INC-{rr.randrange(100000, 999999)}"
                lvl, msg, _ = render_message(ref, "f", bound, seed_key)
                rows.append((ts_s, len(rows), lvl, msg, "", svc, host))


# -----------------------------
# Main simulation
# -----------------------------

def main() -> None:
    rows: List[Row] = []

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    normal_flows = plan_flow_instances("n", n_start, n_end, None)
    normal_bg = plan_background("n", n_start, n_end, None)

    failure_flows: List[Dict[str, Any]] = []
    failure_bg: List[Dict[str, Any]] = []
    for ci in control_intervals:
        failure_flows.extend(plan_flow_instances("f", ci.start_min, ci.end_min, ci))
        failure_bg.extend(plan_background("f", ci.start_min, ci.end_min, ci))

    for bg in normal_bg:
        emit_background_entry(bg, rows)
    for bg in failure_bg:
        emit_background_entry(bg, rows)

    for inst in normal_flows:
        simulate_flow_instance(inst, rows)
    for inst in failure_flows:
        simulate_flow_instance(inst, rows)

    emit_one_shots(rows)

    # Hard horizon safety: ensure nothing beyond scenario end.
    rows = [r for r in rows if r[0] <= SCENARIO_END_S - HORIZON_EPS_S]

    rows.sort(key=lambda x: (x[0], x[1]))

    df = pd.DataFrame(
        {
            "timestamp": [iso_utc_ms(ts) for ts, _, _, _, _, _, _ in rows],
            "level": [lvl for _, _, lvl, _, _, _, _ in rows],
            "message": [msg for _, _, _, msg, _, _, _ in rows],
            "trace_id": [tid for _, _, _, _, tid, _, _ in rows],
            "service": [svc for _, _, _, _, _, svc, _ in rows],
            "host": [host for _, _, _, _, _, _, host in rows],
        }
    )

    assert list(df.columns) == ["timestamp", "level", "message", "trace_id", "service", "host"]
    assert 20000 <= len(df) <= 100000, f"row count out of target range: {len(df)}"
    ts_vals = df["timestamp"].tolist()
    assert ts_vals == sorted(ts_vals), "timestamps not sorted"

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
