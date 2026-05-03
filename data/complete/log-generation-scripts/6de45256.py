import math
import hashlib
import uuid
import ipaddress
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd


# -----------------------------
# Determinism seed (verifier-required)
# -----------------------------
SEED = 1337
random.seed(SEED)


# -----------------------------
# Embedded normalized model data
# -----------------------------
SYSTEM: Dict[str, Any] = {
    "sys": {"id": "cloud_https_load_balancer"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "deploy_manager",
            "svc": "deploy-manager",
            "hosts": ["deploy-01", "deploy-02"],
            "logs": {
                "deploy_start_ok": {
                    "lvl": "INFO",
                    "msg": "deploy start app={app} env={env} change_id={change_id}",
                    "vars": {
                        "app": {"k": "ch", "v": ["payments", "storefront", "api"]},
                        "env": {"k": "ch", "v": ["prod", "staging"]},
                        "change_id": {"k": "uuid", "v": None},
                    },
                },
                "deploy_done_ok": {
                    "lvl": "INFO",
                    "msg": "deploy success app={app} change_id={change_id} duration_ms={duration_ms}",
                    "vars": {
                        "app": {"k": "ch", "v": ["payments", "storefront", "api"]},
                        "change_id": {"k": "uuid", "v": None},
                        "duration_ms": {"k": "i", "v": [2000, 45000]},
                    },
                },
                "deploy_start_paused": {
                    "lvl": "INFO",
                    "msg": "deploy start app={app} env={env} change_id={change_id}",
                    "vars": {
                        "app": {"k": "ch", "v": ["payments", "storefront", "api"]},
                        "env": {"k": "ch", "v": ["prod", "staging"]},
                        "change_id": {"k": "uuid", "v": None},
                    },
                },
                "deploy_done_paused": {
                    "lvl": "ERROR",
                    "msg": "deploy failed app={app} change_id={change_id} reason=lb_config_updates_paused",
                    "vars": {
                        "app": {"k": "ch", "v": ["payments", "storefront", "api"]},
                        "change_id": {"k": "uuid", "v": None},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "config_master",
            "svc": "lb-config-master",
            "hosts": ["cm-01", "cm-02", "cm-03"],
            "logs": {
                "heartbeat": {
                    "lvl": "INFO",
                    "msg": "master heartbeat host={host} role={role} leader={leader}",
                    "vars": {
                        "host": {"k": "ch", "v": ["cm-01", "cm-02", "cm-03"]},
                        "role": {"k": "ch", "v": ["leader", "follower"]},
                        "leader": {"k": "ch", "v": ["cm-01", "cm-02", "cm-03"]},
                    },
                },
                "gfs_read_fail": {
                    "lvl": "ERROR",
                    "msg": "gfs read failed leader={leader} cfg_path={cfg_path} err={err} since_s={since_s}",
                    "vars": {
                        "leader": {"k": "ch", "v": ["cm-01", "cm-02", "cm-03"]},
                        "cfg_path": {"k": "str", "v": "gfs:///lb-config/snapshots/<ver>.json"},
                        "err": {"k": "ch", "v": ["io_timeout", "permission_denied", "stale_handle"]},
                        "since_s": {"k": "i", "v": [1, 1800]},
                    },
                },
                "pause_state": {
                    "lvl": "WARN",
                    "msg": "config updates paused=true reason={reason}",
                    "vars": {"reason": {"k": "ch", "v": ["incident_investigation", "safety_freeze"]}},
                },
                "push_start": {
                    "lvl": "INFO",
                    "msg": "config push start push_id={push_id} leader={leader} cfg_version={cfg_version} cfg_age_h={cfg_age_h} stage=canary",
                    "vars": {
                        "push_id": {"k": "uuid", "v": None},
                        "leader": {"k": "ch", "v": ["cm-01", "cm-02", "cm-03"]},
                    },
                    "state_vars": {
                        "n": {"cfg_version": {"k": "i", "v": [120, 140]}, "cfg_age_h": {"k": "f", "v": [0.0, 2.0]}},
                        "f": {"cfg_version": {"k": "i", "v": [100, 130]}, "cfg_age_h": {"k": "f", "v": [8.0, 48.0]}},
                    },
                },
                "canary_result": {
                    "lvl": "WARN",
                    "msg": "config canary result push_id={push_id} leader={leader} result={result} failed_checks={failed_checks}",
                    "vars": {
                        "push_id": {"k": "uuid", "v": None},
                        "leader": {"k": "ch", "v": ["cm-01", "cm-02", "cm-03"]},
                        "failed_checks": {"k": "i", "v": [0, 10]},
                    },
                    "state_vars": {"n": {"result": {"k": "ch", "v": ["pass"]}}, "f": {"result": {"k": "ch", "v": ["fail"]}}},
                },
                "push_complete": {
                    "lvl": "INFO",
                    "msg": "config push complete push_id={push_id} leader={leader} action={action} global_version={cfg_version}",
                    "vars": {"push_id": {"k": "uuid", "v": None}, "leader": {"k": "ch", "v": ["cm-01", "cm-02", "cm-03"]}},
                    "state_vars": {
                        "n": {"action": {"k": "ch", "v": ["apply"]}, "cfg_version": {"k": "i", "v": [120, 140]}},
                        "f": {"action": {"k": "ch", "v": ["revert_to_known_good"]}, "cfg_version": {"k": "i", "v": [100, 130]}},
                    },
                },
                "deploy_change_accepted": {
                    "lvl": "INFO",
                    "msg": "deploy change accepted change_id={change_id} leader={leader}",
                    "vars": {"change_id": {"k": "uuid", "v": None}, "leader": {"k": "ch", "v": ["cm-01", "cm-02", "cm-03"]}},
                },
                "deploy_change_rejected_paused": {
                    "lvl": "WARN",
                    "msg": "deploy change rejected change_id={change_id} reason=updates_paused leader={leader}",
                    "vars": {"change_id": {"k": "uuid", "v": None}, "leader": {"k": "ch", "v": ["cm-01", "cm-02", "cm-03"]}},
                },
                "leader_elected": {
                    "lvl": "INFO",
                    "msg": "leader elected new_leader={leader} prev_leader={prev_leader}",
                    "vars": {
                        "leader": {"k": "ch", "v": ["cm-01", "cm-02", "cm-03"]},
                        "prev_leader": {"k": "ch", "v": ["cm-01", "cm-02", "cm-03"]},
                    },
                },
                "rollback_to_known_good": {
                    "lvl": "WARN",
                    "msg": "rollback initiated leader={leader} revert_to_version={revert_to_version} observed_latest_readable={latest_readable_version}",
                    "vars": {
                        "leader": {"k": "ch", "v": ["cm-01", "cm-02", "cm-03"]},
                        "revert_to_version": {"k": "i", "v": [100, 130]},
                        "latest_readable_version": {"k": "i", "v": [100, 130]},
                    },
                },
                "ops_switch_master": {
                    "lvl": "INFO",
                    "msg": "ops switched config master from={from_leader} to={to_leader}",
                    "vars": {
                        "from_leader": {"k": "ch", "v": ["cm-01", "cm-02", "cm-03"]},
                        "to_leader": {"k": "ch", "v": ["cm-01", "cm-02", "cm-03"]},
                    },
                },
                "ops_force_config_push": {
                    "lvl": "INFO",
                    "msg": "ops forced config push leader={leader} cfg_version={cfg_version} cfg_age_h={cfg_age_h} stage=global",
                    "vars": {
                        "leader": {"k": "ch", "v": ["cm-01", "cm-02", "cm-03"]},
                        "cfg_version": {"k": "i", "v": [120, 140]},
                        "cfg_age_h": {"k": "f", "v": [0.0, 2.0]},
                    },
                },
                "ops_pause_updates": {
                    "lvl": "WARN",
                    "msg": "ops paused config updates=true duration_expected_h={duration_expected_h}",
                    "vars": {"duration_expected_h": {"k": "f", "v": [1.0, 6.0]}},
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "heartbeat", "per_min": 1.0, "scope": "per_host"},
                        {"id": "gfs_read_fail", "per_min": 0.02, "scope": "per_host"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "heartbeat", "per_min": 1.0, "scope": "per_host"},
                        {"id": "gfs_read_fail", "per_min": 2.0, "scope": "per_host"},
                        {"id": "pause_state", "per_min": 0.5, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "gfs",
            "svc": "gfs",
            "hosts": ["gfs-01"],
            "logs": {
                "rpc_warn": {
                    "lvl": "WARN",
                    "msg": "storage rpc warn client={client} op={op} latency_ms={latency_ms}",
                    "vars": {
                        "client": {"k": "ch", "v": ["lb-config-master"]},
                        "op": {"k": "ch", "v": ["read", "list"]},
                        "latency_ms": {"k": "i", "v": [20, 2000]},
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "rpc_warn", "per_min": 0.05, "scope": "global"}]},
                "f": {"emit": [{"id": "rpc_warn", "per_min": 0.05, "scope": "global"}]},
            },
        },
        {
            "id": "gfe",
            "svc": "gfe",
            "hosts": ["gfe-01", "gfe-02", "gfe-03", "gfe-04", "gfe-05", "gfe-06"],
            "logs": {
                "access_200": {
                    "lvl": "INFO",
                    "msg": "http access server={server} lb_id={lb_id} vip={vip} method={method} uri={uri} status=200 latency_ms={latency_ms} bytes_out={bytes_out}",
                    "vars": {
                        "server": {"k": "ch", "v": ["gfe-01", "gfe-02", "gfe-03", "gfe-04", "gfe-05", "gfe-06"]},
                        "lb_id": {"k": "ch", "v": ["lb-az1", "lb-az2", "lb-az3"]},
                        "vip": {"k": "ip", "v": "203.0.113.0/24"},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "uri": {"k": "ch", "v": ["/", "/api", "/login", "/healthz"]},
                        "bytes_out": {"k": "i", "v": [200, 40000]},
                    },
                    "state_vars": {"n": {"latency_ms": {"k": "i", "v": [8, 140]}}, "f": {"latency_ms": {"k": "i", "v": [10, 320]}}},
                },
                "access_502_restart": {
                    "lvl": "INFO",
                    "msg": "http access server={server} lb_id={lb_id} vip={vip} method={method} uri={uri} status=502 latency_ms={latency_ms} error=frontend_restarting",
                    "vars": {
                        "server": {"k": "ch", "v": ["gfe-01", "gfe-02", "gfe-03", "gfe-04", "gfe-05", "gfe-06"]},
                        "lb_id": {"k": "ch", "v": ["lb-az1", "lb-az2", "lb-az3"]},
                        "vip": {"k": "ip", "v": "203.0.113.0/24"},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "uri": {"k": "ch", "v": ["/", "/api", "/login", "/healthz"]},
                        "latency_ms": {"k": "i", "v": [5, 800]},
                    },
                },
                "proxy_error_restart": {
                    "lvl": "ERROR",
                    "msg": "proxy error server={server} lb_id={lb_id} status=502 reason=process_restarting restart_reason={restart_reason}",
                    "vars": {
                        "server": {"k": "ch", "v": ["gfe-01", "gfe-02", "gfe-03", "gfe-04", "gfe-05", "gfe-06"]},
                        "lb_id": {"k": "ch", "v": ["lb-az1", "lb-az2", "lb-az3"]},
                        "restart_reason": {"k": "ch", "v": ["healthcheck_failed", "watchdog"]},
                    },
                },
                "access_502_config": {
                    "lvl": "INFO",
                    "msg": "http access server={server} lb_id={lb_id} vip={vip} method={method} uri={uri} status=502 latency_ms={latency_ms} error=config_not_found",
                    "vars": {
                        "server": {"k": "ch", "v": ["gfe-01", "gfe-02", "gfe-03", "gfe-04", "gfe-05", "gfe-06"]},
                        "lb_id": {"k": "ch", "v": ["lb-new-7f", "lb-new-a3"]},
                        "vip": {"k": "ip", "v": "203.0.113.0/24"},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "uri": {"k": "ch", "v": ["/", "/api", "/login", "/healthz"]},
                        "latency_ms": {"k": "i", "v": [2, 120]},
                    },
                },
                "proxy_error_config": {
                    "lvl": "ERROR",
                    "msg": "proxy error server={server} lb_id={lb_id} status=502 reason=config_missing active_cfg_version={active_cfg_version}",
                    "vars": {
                        "server": {"k": "ch", "v": ["gfe-01", "gfe-02", "gfe-03", "gfe-04", "gfe-05", "gfe-06"]},
                        "lb_id": {"k": "ch", "v": ["lb-new-7f", "lb-new-a3"]},
                        "active_cfg_version": {"k": "i", "v": [100, 130]},
                    },
                },
                "config_apply": {
                    "lvl": "INFO",
                    "msg": "config applied server={server} cfg_version={cfg_version} cfg_age_h={cfg_age_h} entries={entries}",
                    "vars": {
                        "server": {"k": "ch", "v": ["gfe-01", "gfe-02", "gfe-03", "gfe-04", "gfe-05", "gfe-06"]},
                        "entries": {"k": "i", "v": [200, 12000]},
                    },
                    "state_vars": {
                        "n": {"cfg_version": {"k": "i", "v": [120, 140]}, "cfg_age_h": {"k": "f", "v": [0.0, 2.0]}},
                        "f": {"cfg_version": {"k": "i", "v": [100, 130]}, "cfg_age_h": {"k": "f", "v": [8.0, 48.0]}},
                    },
                },
                "config_apply_forced": {
                    "lvl": "INFO",
                    "msg": "config applied server={server} cfg_version={cfg_version} cfg_age_h={cfg_age_h} entries={entries} source=forced_reload",
                    "vars": {
                        "server": {"k": "ch", "v": ["gfe-01", "gfe-02", "gfe-03", "gfe-04", "gfe-05", "gfe-06"]},
                        "cfg_version": {"k": "i", "v": [120, 140]},
                        "cfg_age_h": {"k": "f", "v": [0.0, 2.0]},
                        "entries": {"k": "i", "v": [3000, 12000]},
                    },
                },
                "gc_stats": {
                    "lvl": "WARN",
                    "msg": "gc stats server={server} deleted_cfg={deleted_cfg} gc_cpu_pct={gc_cpu_pct} heap_mb={heap_mb}",
                    "vars": {"server": {"k": "ch", "v": ["gfe-01", "gfe-02", "gfe-03", "gfe-04", "gfe-05", "gfe-06"]}},
                    "state_vars": {
                        "n": {
                            "deleted_cfg": {"k": "i", "v": [0, 30]},
                            "gc_cpu_pct": {"k": "i", "v": [0, 15]},
                            "heap_mb": {"k": "i", "v": [600, 1600]},
                        },
                        "f": {
                            "deleted_cfg": {"k": "i", "v": [500, 6000]},
                            "gc_cpu_pct": {"k": "i", "v": [20, 95]},
                            "heap_mb": {"k": "i", "v": [900, 4200]},
                        },
                    },
                },
                "process_restart": {
                    "lvl": "ERROR",
                    "msg": "process restart server={server} reason={reason} downtime_ms={downtime_ms}",
                    "vars": {
                        "server": {"k": "ch", "v": ["gfe-01", "gfe-02", "gfe-03", "gfe-04", "gfe-05", "gfe-06"]},
                        "reason": {"k": "ch", "v": ["healthcheck_failed", "watchdog"]},
                        "downtime_ms": {"k": "i", "v": [10000, 90000]},
                    },
                },
                "cpu_sample": {
                    "lvl": "INFO",
                    "msg": "cpu sample server={server} cpu_pct={cpu_pct}",
                    "vars": {"server": {"k": "ch", "v": ["gfe-01", "gfe-02", "gfe-03", "gfe-04", "gfe-05", "gfe-06"]}, "cpu_pct": {"k": "i", "v": [5, 98]}},
                },
                "bulk_config_reload": {
                    "lvl": "WARN",
                    "msg": "bulk config reload initiated target_version={cfg_version} impacted_cfg_deleted={deleted_cfg}",
                    "vars": {"cfg_version": {"k": "i", "v": [100, 130]}, "deleted_cfg": {"k": "i", "v": [500, 6000]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "cpu_sample", "per_min": 1.0, "scope": "per_host"}, {"id": "gc_stats", "per_min": 0.4, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "cpu_sample", "per_min": 1.0, "scope": "per_host"},
                        {"id": "gc_stats", "per_min": 0.4, "scope": "per_host"},
                        {"id": "process_restart", "per_min": 0.02, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "health_checker",
            "svc": "gfe-health",
            "hosts": ["hc-01", "hc-02"],
            "logs": {
                "hc_failed": {
                    "lvl": "WARN",
                    "msg": "health check failed target={target} check={check} err={err} consecutive_failures={consecutive_failures}",
                    "vars": {
                        "target": {"k": "ch", "v": ["gfe-01", "gfe-02", "gfe-03", "gfe-04", "gfe-05", "gfe-06"]},
                        "check": {"k": "ch", "v": ["gfe_proxy_health"]},
                        "err": {"k": "ch", "v": ["timeout", "connection_refused"]},
                        "consecutive_failures": {"k": "i", "v": [1, 20]},
                    },
                }
            },
            "beh": {"n": {"emit": [{"id": "hc_failed", "per_min": 0.05, "scope": "global"}]}, "f": {"emit": [{"id": "hc_failed", "per_min": 0.05, "scope": "global"}]}},
        },
        {
            "id": "monitoring",
            "svc": "monitoring",
            "hosts": ["mon-01"],
            "logs": {
                "page_sent": {
                    "lvl": "CRITICAL",
                    "msg": "page sent policy={policy} signal={signal} value={value} incident_key={incident_key}",
                    "vars": {
                        "policy": {"k": "ch", "v": ["lb_error_rate", "gfe_health"]},
                        "signal": {"k": "ch", "v": ["http_502_rate", "health_check_fail_rate"]},
                        "value": {"k": "f", "v": [0.1, 1.0]},
                        "incident_key": {"k": "str", "v": "INC-<7digits>"},
                    },
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "client_https_request_ok_n",
                    "rpm": 700.0,
                    "emit": ["gfe.access_200"],
                    "latency_ms": [[20, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "lb_config_push_cycle_n",
                    "rpm": 0.5,
                    "emit": ["config_master.push_start", "config_master.canary_result", "gfe.config_apply", "config_master.push_complete"],
                    "latency_ms": [[30, 120], [50, 250], [200, 1200], [20, 120]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "app_flex_deploy_success_n",
                    "rpm": 1.5,
                    "emit": ["deploy_manager.deploy_start_ok", "config_master.deploy_change_accepted", "deploy_manager.deploy_done_ok"],
                    "latency_ms": [[10, 80], [30, 250], [2000, 45000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "client_https_request_stable_ok_f",
                    "rpm": 525.0,
                    "emit": ["gfe.access_200"],
                    "latency_ms": [[25, 220]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "client_https_request_stable_502_restart_f",
                    "rpm": 145.0,
                    "emit": ["gfe.access_502_restart", "gfe.proxy_error_restart"],
                    "latency_ms": [[10, 500], [1, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "client_https_request_recent_502_config_f",
                    "rpm": 30.0,
                    "emit": ["gfe.access_502_config", "gfe.proxy_error_config"],
                    "latency_ms": [[5, 80], [1, 10]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "lb_config_push_cycle_f",
                    "rpm": 0.5,
                    "emit": ["config_master.push_start", "config_master.canary_result", "gfe.config_apply", "config_master.push_complete"],
                    "latency_ms": [[40, 250], [50, 300], [300, 3000], [20, 150]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "app_flex_deploy_success_f",
                    "rpm": 1.5,
                    "emit": ["deploy_manager.deploy_start_ok", "config_master.deploy_change_accepted", "deploy_manager.deploy_done_ok"],
                    "latency_ms": [[10, 80], [40, 350], [2000, 45000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "app_flex_deploy_fail_paused_f",
                    "rpm": 1.0,
                    "emit": ["deploy_manager.deploy_start_paused", "config_master.deploy_change_rejected_paused", "deploy_manager.deploy_done_paused"],
                    "latency_ms": [[10, 80], [20, 200], [50, 500]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "lb_stale_config_rollback_gfe_gc_20170405"},
    "time": {"total_minutes": 40, "phases": {"n": {"start_min": 0, "end_min": 18}, "f": {"start_min": 18, "end_min": 40}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 18,
                    "rate_multipliers": {
                        "client_https_request_stable_ok_f": 1.28,
                        "client_https_request_stable_502_restart_f": 0.0,
                        "client_https_request_recent_502_config_f": 1.0,
                        "app_flex_deploy_success_f": 1.0,
                        "app_flex_deploy_fail_paused_f": 0.0,
                        "config_master.pause_state": 0.0,
                        "gfe.gc_stats": 0.0,
                        "health_checker.hc_failed": 0.0,
                        "gfe.process_restart": 0.0,
                    },
                    "latency_multipliers": {"client_https_request_stable_ok_f": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [
                        {"ref": "config_master.leader_elected", "count": 1, "hosts": ["cm-02"]},
                        {"ref": "config_master.rollback_to_known_good", "count": 1, "hosts": ["cm-02"]},
                        {"ref": "gfe.bulk_config_reload", "count": 1, "hosts": ["gfe-01"]},
                    ],
                },
                {
                    "order": 2,
                    "at_min": 21,
                    "rate_multipliers": {
                        "client_https_request_stable_ok_f": 1.0,
                        "client_https_request_stable_502_restart_f": 1.0,
                        "gfe.gc_stats": 4.0,
                        "health_checker.hc_failed": 8.0,
                        "gfe.process_restart": 14.0,
                    },
                    "latency_multipliers": {
                        "client_https_request_stable_ok_f": {"p50": 1.1, "p95": 1.6},
                        "client_https_request_stable_502_restart_f": {"p50": 1.0, "p95": 1.2},
                    },
                    "one_shots": [],
                },
                {"order": 3, "at_min": 27, "rate_multipliers": {}, "latency_multipliers": {}, "one_shots": [{"ref": "monitoring.page_sent", "count": 1, "hosts": ["mon-01"]}]},
                {
                    "order": 4,
                    "at_min": 39,
                    "rate_multipliers": {
                        "client_https_request_recent_502_config_f": 0.0,
                        "client_https_request_stable_502_restart_f": 0.1,
                        "client_https_request_stable_ok_f": 1.3,
                        "lb_config_push_cycle_f": 0.0,
                        "config_master.gfs_read_fail": 0.0,
                        "gfe.gc_stats": 0.6,
                        "health_checker.hc_failed": 0.5,
                        "gfe.process_restart": 1.0,
                        "app_flex_deploy_success_f": 0.0,
                        "app_flex_deploy_fail_paused_f": 1.0,
                        "config_master.pause_state": 1.0,
                    },
                    "latency_multipliers": {"client_https_request_stable_ok_f": {"p50": 1.0, "p95": 1.1}},
                    "one_shots": [
                        {"ref": "config_master.ops_switch_master", "count": 1, "hosts": ["cm-01"]},
                        {"ref": "config_master.ops_force_config_push", "count": 1, "hosts": ["cm-01"]},
                        {"ref": "gfe.config_apply_forced", "count": 6, "hosts": ["gfe-01", "gfe-02", "gfe-03", "gfe-04", "gfe-05", "gfe-06"]},
                        {"ref": "config_master.ops_pause_updates", "count": 1, "hosts": ["cm-01"]},
                    ],
                },
            ]
        }
    },
}


# -----------------------------
# Deterministic helpers
# -----------------------------
BASE_TIME = datetime(2017, 4, 5, 0, 0, 0, tzinfo=timezone.utc)


def _h_bytes(s: str) -> bytes:
    return hashlib.md5(s.encode("utf-8")).digest()


def h_u01(key: str) -> float:
    b = _h_bytes(key)
    x = int.from_bytes(b[:8], "big", signed=False)
    return (x % (10**12)) / float(10**12)  # stable [0,1)


def h_int(key: str, low: int, high: int) -> int:
    if high < low:
        low, high = high, low
    u = h_u01(key)
    return low + int(math.floor(u * (high - low + 1)))


def h_choice(key: str, choices: List[Any]) -> Any:
    if not choices:
        return None
    idx = h_int(key, 0, len(choices) - 1)
    return choices[idx]


def h_hex(key: str, length: int) -> str:
    b = _h_bytes("hex|" + key)
    hexs = b.hex()
    if length <= len(hexs):
        return hexs[:length]
    out = hexs
    i = 1
    while len(out) < length:
        out += _h_bytes(f"hex|{key}|{i}").hex()
        i += 1
    return out[:length]


def h_uuid(key: str) -> str:
    b = _h_bytes("uuid|" + key)
    u128 = int.from_bytes(b * 2, "big", signed=False) & ((1 << 128) - 1)
    u = uuid.UUID(int=u128, version=4)
    return str(u)


def h_ip(key: str, cidr: str) -> str:
    net = ipaddress.ip_network(cidr, strict=False)
    if net.num_addresses <= 2:
        return str(net.network_address)
    max_host = int(net.num_addresses) - 2
    off = h_int(key, 1, max_host)
    return str(net.network_address + off)


def stable_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    if frac <= 0:
        return base
    return base + (1 if h_u01("round|" + key) < frac else 0)


_NORMAL = NormalDist()


def sample_lognormal_ms(key: str, p50_ms: float, p95_ms: float) -> int:
    p50 = max(1e-6, float(p50_ms))
    p95 = max(p50, float(p95_ms))
    mu = math.log(p50)
    z95 = 1.6448536269514722
    sigma = (math.log(p95) - mu) / z95 if p95 > 0 else 0.0
    u = h_u01("lnq|" + key)
    q = 0.5 + (u - 0.5) * 0.90  # [0.05, 0.95]
    z = _NORMAL.inv_cdf(q)
    val = math.exp(mu + sigma * z)
    cap = 3.0 * p95
    val = min(val, cap)
    return int(max(1.0, round(val)))


def fmt_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:23] + "Z"


def schedule_uniform_times(start: datetime, end: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    dur_s = max(0.001, (end - start).total_seconds())
    step = dur_s / count
    jitter_cap = min(0.35, step * 0.45)
    out: List[datetime] = []
    for i in range(count):
        base_off = (i + 0.5) * step
        jit = (h_u01(f"jit|{key}|{i}") - 0.5) * 2.0 * jitter_cap
        off = min(max(0.0, base_off + jit), dur_s - 0.001)
        out.append(start + timedelta(seconds=off))
    return out


def clamp_int(val: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(val)))


# -----------------------------
# Indices and control intervals
# -----------------------------
components_by_id: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}


def split_ref(ref: str) -> Tuple[str, str]:
    a, b = ref.split(".", 1)
    return a, b


log_templates: Dict[str, Dict[str, Any]] = {}
for comp in SYSTEM["components"]:
    cid = comp["id"]
    for lid, tpl in comp.get("logs", {}).items():
        log_templates[f"{cid}.{lid}"] = {"component_id": cid, "log_id": lid, **tpl}


flows_by_state_id: Dict[str, Dict[str, Dict[str, Any]]] = {"n": {}, "f": {}}
for st in ["n", "f"]:
    for fdef in SYSTEM["flows"][st]["req"]:
        flows_by_state_id[st][fdef["id"]] = fdef


def phase_window(state: str) -> Tuple[int, int]:
    p = SCENARIO["time"]["phases"][state]
    return int(p["start_min"]), int(p["end_min"])


def active_leader_at_minute(minute: float) -> str:
    if minute < 18:
        return "cm-01"
    if minute < 39:
        return "cm-02"
    return "cm-01"


def prev_leader_for_event(at_min: int) -> str:
    return active_leader_at_minute(at_min - 1e-6)


@dataclass(frozen=True)
class ControlInterval:
    start_min: int
    end_min: int
    flow_rate_mult: Dict[str, float]
    bg_rate_mult: Dict[str, float]
    flow_latency_mult: Dict[str, Dict[str, float]]


def build_failure_intervals() -> List[ControlInterval]:
    f_start, f_end = phase_window("f")
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e.get("order", 0)))
    boundaries = [f_start] + [int(e["at_min"]) for e in events if f_start <= int(e["at_min"]) < f_end] + [f_end]
    boundaries = sorted(set(boundaries))

    flow_mult: Dict[str, float] = {}
    bg_mult: Dict[str, float] = {}
    lat_mult: Dict[str, Dict[str, float]] = {}

    events_by_at: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        at = int(e["at_min"])
        events_by_at.setdefault(at, []).append(e)

    intervals: List[ControlInterval] = []
    for i in range(len(boundaries) - 1):
        s = boundaries[i]
        for e in events_by_at.get(s, []):
            for k, v in e.get("rate_multipliers", {}).items():
                if "." in k:
                    bg_mult[k] = float(v)
                else:
                    flow_mult[k] = float(v)
            for fid, lm in e.get("latency_multipliers", {}).items():
                lat_mult[fid] = {"p50": float(lm.get("p50", 1.0)), "p95": float(lm.get("p95", 1.0))}
        intervals.append(
            ControlInterval(
                start_min=s,
                end_min=boundaries[i + 1],
                flow_rate_mult=dict(flow_mult),
                bg_rate_mult=dict(bg_mult),
                flow_latency_mult=dict(lat_mult),
            )
        )
    return intervals


FAIL_INTERVALS = build_failure_intervals()


# -----------------------------
# Variable binding/rendering
# -----------------------------
def domain_for_var(tpl: Dict[str, Any], state: str, var: str) -> Optional[Dict[str, Any]]:
    sv = tpl.get("state_vars", {}).get(state, {})
    if var in sv:
        return sv[var]
    return tpl.get("vars", {}).get(var)


def gen_from_domain(dom: Dict[str, Any], key: str, bound: Dict[str, Any]) -> Any:
    k = dom["k"]
    v = dom["v"]
    if k == "ch":
        return h_choice(key, list(v))
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return h_int(key, lo, hi)
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        u = h_u01("f|" + key)
        val = lo + u * (hi - lo)
        return f"{val:.2f}"
    if k == "uuid":
        return h_uuid(key)
    if k == "hex":
        return h_hex(key, int(v))
    if k == "ip":
        return h_ip(key, str(v))
    if k == "str":
        s = str(v)
        if "<ver>" in s:
            ver = bound.get("cfg_version")
            if ver is None:
                ver = h_int("ver|" + key, 100, 140)
            return s.replace("<ver>", str(ver))
        if "<7digits>" in s:
            digits = h_int("inc|" + key, 0, 9999999)
            return s.replace("<7digits>", f"{digits:07d}")
        return s
    return str(v)


def render_message(ref: str, state: str, host: str, base_key: str, bound: Dict[str, Any], overrides: Dict[str, Any]) -> str:
    tpl = log_templates[ref]
    needed = set(tpl.get("vars", {}).keys())
    needed |= set(tpl.get("state_vars", {}).get(state, {}).keys())

    if "server" in needed:
        overrides.setdefault("server", host)
    if "host" in needed:
        overrides.setdefault("host", host)

    for k, v in overrides.items():
        bound[k] = v

    for var in sorted(needed):
        if var in bound:
            continue
        dom = domain_for_var(tpl, state, var)
        if dom is None:
            bound[var] = ""
        else:
            bound[var] = gen_from_domain(dom, f"{base_key}|{ref}|{var}", bound)

    if ref == "config_master.canary_result":
        res = str(bound.get("result", "pass"))
        if res == "pass":
            bound["failed_checks"] = 0
        else:
            fc = int(bound.get("failed_checks", 1))
            bound["failed_checks"] = max(1, fc)

    if ref == "config_master.heartbeat":
        leader = bound.get("leader")
        if leader is not None:
            bound["role"] = "leader" if host == leader else "follower"

    return tpl["msg"].format(**bound)


# -----------------------------
# Log emission
# -----------------------------
rows: List[Dict[str, Any]] = []


def emit_log(dt: datetime, ref: str, state: str, host: str, base_key: str, bound: Dict[str, Any], overrides: Optional[Dict[str, Any]] = None) -> None:
    tpl = log_templates[ref]
    cid = tpl["component_id"]
    comp = components_by_id[cid]
    msg = render_message(ref, state, host, base_key, bound, overrides or {})
    rows.append(
        {
            "timestamp_dt": dt,
            "level": tpl["lvl"],
            "message": msg,
            "trace_id": "" if not SYSTEM["tracing"]["on"] else h_hex("trace|" + base_key, 32),
            "service": comp.get("svc", "") or "",
            "host": host or "",
        }
    )


def pick_host(component_id: str, key: str) -> str:
    hosts = components_by_id[component_id].get("hosts", [])
    if not hosts:
        return ""
    return h_choice("hostpick|" + key, hosts)


def background_emit_interval(state: str, start_min: int, end_min: int, bg_mult: Optional[Dict[str, float]] = None) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    dur_min = max(0.0, (end_dt - start_dt).total_seconds() / 60.0)

    for comp in SYSTEM["components"]:
        cid = comp["id"]
        beh = comp.get("beh", {}).get(state, {}).get("emit", [])
        for src in beh:
            lid = src["id"]
            scope = src.get("scope", "per_host")
            per_min = float(src["per_min"])
            ref = f"{cid}.{lid}"

            mult = 1.0
            if state == "f" and bg_mult is not None:
                mult = float(bg_mult.get(ref, 1.0))
            rate = per_min * mult
            if rate <= 0:
                continue

            if scope == "global":
                expected = rate * dur_min
                count = stable_round(expected, f"bg|{state}|{start_min}-{end_min}|{ref}|global")
                times = schedule_uniform_times(start_dt, end_dt, count, f"bg|{state}|{start_min}-{end_min}|{ref}|global")
                for i, t in enumerate(times):
                    host = pick_host(cid, f"bg|{ref}|{start_min}|{i}")
                    base_key = f"bg|{state}|{ref}|{start_min}-{end_min}|{i}"
                    bound: Dict[str, Any] = {}
                    if cid == "config_master":
                        leader = active_leader_at_minute((t - BASE_TIME).total_seconds() / 60.0)
                        bound["leader"] = leader
                    emit_log(t, ref, state, host, base_key, bound, overrides={})
            else:
                for h in comp.get("hosts", []):
                    expected = rate * dur_min
                    count = stable_round(expected, f"bg|{state}|{start_min}-{end_min}|{ref}|{h}")
                    times = schedule_uniform_times(start_dt, end_dt, count, f"bg|{state}|{start_min}-{end_min}|{ref}|{h}")
                    for i, t in enumerate(times):
                        base_key = f"bg|{state}|{ref}|{start_min}-{end_min}|{h}|{i}"
                        bound = {}
                        if cid == "config_master":
                            leader = active_leader_at_minute((t - BASE_TIME).total_seconds() / 60.0)
                            bound["leader"] = leader
                        emit_log(t, ref, state, h, base_key, bound, overrides={})


def _maybe_bind_timing_field_from_delay(state: str, ref: str, delay_ms: int, base_key: str, bound: Dict[str, Any], var_name: str) -> int:
    tpl = log_templates[ref]
    dom = domain_for_var(tpl, state, var_name)
    if not dom or dom.get("k") != "i":
        bound[var_name] = int(delay_ms)
        return int(delay_ms)
    lo, hi = int(dom["v"][0]), int(dom["v"][1])
    delay_ms = clamp_int(int(delay_ms), lo, hi)
    bound[var_name] = int(delay_ms)
    return int(delay_ms)


def flow_instances_interval(
    state: str,
    start_min: int,
    end_min: int,
    flow_mult: Optional[Dict[str, float]] = None,
    lat_mult: Optional[Dict[str, Dict[str, float]]] = None,
) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    dur_min = max(0.0, (end_dt - start_dt).total_seconds() / 60.0)

    fdefs = SYSTEM["flows"][state]["req"]
    for fdef in fdefs:
        fid = fdef["id"]
        rpm = float(fdef["rpm"])
        mult = 1.0
        if state == "f" and flow_mult is not None:
            mult = float(flow_mult.get(fid, 1.0))
        eff_rpm = rpm * mult
        if eff_rpm <= 0:
            continue

        expected = eff_rpm * dur_min
        ninst = stable_round(expected, f"flow|{state}|{start_min}-{end_min}|{fid}")
        starts = schedule_uniform_times(start_dt, end_dt, ninst, f"flow|{state}|{start_min}-{end_min}|{fid}")

        for idx, st_dt in enumerate(starts):
            base_key = f"flow|{state}|{fid}|{start_min}-{end_min}|{idx}"
            bound: Dict[str, Any] = {}

            leader = active_leader_at_minute((st_dt - BASE_TIME).total_seconds() / 60.0)
            lm = {"p50": 1.0, "p95": 1.0}
            if state == "f" and lat_mult is not None and fid in lat_mult:
                lm = {"p50": float(lat_mult[fid].get("p50", 1.0)), "p95": float(lat_mult[fid].get("p95", 1.0))}

            host_for_comp: Dict[str, str] = {}
            gfe_hosts = components_by_id["gfe"]["hosts"]
            if any(ref.startswith("gfe.") for ref in fdef["emit"]):
                host_for_comp["gfe"] = gfe_hosts[idx % len(gfe_hosts)]
            if any(ref.startswith("deploy_manager.") for ref in fdef["emit"]):
                dm_hosts = components_by_id["deploy_manager"]["hosts"]
                host_for_comp["deploy_manager"] = dm_hosts[idx % len(dm_hosts)]
            if any(ref.startswith("config_master.") for ref in fdef["emit"]):
                host_for_comp["config_master"] = leader

            if fid.startswith("client_https_request"):
                bound["method"] = h_choice(base_key + "|method", ["GET", "POST"])
                bound["uri"] = h_choice(base_key + "|uri", ["/", "/api", "/login", "/healthz"])
                bound["vip"] = h_ip(base_key + "|vip", "203.0.113.0/24")
                if fid in ("client_https_request_ok_n", "client_https_request_stable_ok_f"):
                    bound["lb_id"] = h_choice(base_key + "|lb", ["lb-az1", "lb-az2", "lb-az3"])
                    bound["bytes_out"] = h_int(base_key + "|bytes", 200, 40000)
                elif fid == "client_https_request_stable_502_restart_f":
                    bound["lb_id"] = h_choice(base_key + "|lb", ["lb-az1", "lb-az2", "lb-az3"])
                    bound["restart_reason"] = h_choice(base_key + "|rr", ["healthcheck_failed", "watchdog"])
                elif fid == "client_https_request_recent_502_config_f":
                    bound["lb_id"] = h_choice(base_key + "|lb", ["lb-new-7f", "lb-new-a3"])
                    bound["active_cfg_version"] = h_int(base_key + "|acv", 100, 130)

            if fid.startswith("app_flex_deploy"):
                bound["app"] = h_choice(base_key + "|app", ["payments", "storefront", "api"])
                bound["env"] = h_choice(base_key + "|env", ["prod", "staging"])
                bound["change_id"] = h_uuid(base_key + "|change")

            if fid.startswith("lb_config_push_cycle"):
                bound["push_id"] = h_uuid(base_key + "|push")
                bound["leader"] = leader
                if state == "n":
                    bound["cfg_version"] = h_int(base_key + "|cfgv", 120, 140)
                    bound["cfg_age_h"] = f"{(h_u01(base_key + '|age') * 2.0):.2f}"
                else:
                    bound["cfg_version"] = h_int(base_key + "|cfgv", 100, 130)
                    bound["cfg_age_h"] = f"{(8.0 + h_u01(base_key + '|age') * 40.0):.2f}"

            delays_ms: List[int] = []
            for li, (p50, p95) in enumerate(fdef["latency_ms"]):
                p50s = float(p50) * lm["p50"]
                p95s = float(p95) * lm["p95"]
                delays_ms.append(sample_lognormal_ms(f"{base_key}|lat|{li}", p50s, p95s))

            # Bind duration_ms coherently to the modeled "deployment duration" segment and keep it within template domain.
            if any(ref.endswith(".deploy_done_ok") for ref in fdef["emit"]):
                try:
                    done_idx = fdef["emit"].index("deploy_manager.deploy_done_ok")
                except ValueError:
                    done_idx = -1
                if done_idx >= 0 and done_idx < len(delays_ms):
                    delays_ms[done_idx] = _maybe_bind_timing_field_from_delay(state, "deploy_manager.deploy_done_ok", delays_ms[done_idx], base_key, bound, "duration_ms")

            # Bind per-log latency_ms fields (if present) to their corresponding delay segment, clamped to template domain.
            for li, ref in enumerate(fdef["emit"]):
                tpl = log_templates[ref]
                has_latency = ("latency_ms" in (tpl.get("vars", {}) or {})) or ("latency_ms" in (tpl.get("state_vars", {}).get(state, {}) or {}))
                if has_latency and li < len(delays_ms):
                    delays_ms[li] = _maybe_bind_timing_field_from_delay(state, ref, delays_ms[li], base_key, bound, "latency_ms")

            t = st_dt
            for li, ref in enumerate(fdef["emit"]):
                t = t + timedelta(milliseconds=delays_ms[li])
                cid, _ = split_ref(ref)
                host = host_for_comp.get(cid, pick_host(cid, f"{base_key}|{cid}"))
                overrides: Dict[str, Any] = {}
                if cid == "gfe":
                    overrides["server"] = host
                if cid == "config_master":
                    overrides["leader"] = leader
                emit_log(t, ref, state, host, f"{base_key}|emit|{li}", bound, overrides=overrides)


def emit_one_shots() -> None:
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e.get("order", 0)))
    forced_cfg_version = h_int("forced|cfgv", 120, 140)
    forced_cfg_age_h = f"{(h_u01('forced|age') * 2.0):.2f}"

    for e in events:
        at_min = int(e["at_min"])
        base_dt = BASE_TIME + timedelta(minutes=at_min)
        ones = e.get("one_shots", [])
        for os in ones:
            ref = os["ref"]
            count = int(os["count"])
            hosts = list(os.get("hosts") or [])
            times = schedule_uniform_times(base_dt, base_dt + timedelta(seconds=2.0), count, f"oneshot|{at_min}|{ref}")
            for i in range(count):
                host = hosts[i % len(hosts)] if hosts else pick_host(split_ref(ref)[0], f"oneshot|{at_min}|{ref}|{i}")
                bound: Dict[str, Any] = {}
                overrides: Dict[str, Any] = {}

                if ref == "config_master.leader_elected":
                    new_leader = "cm-02"
                    prev = prev_leader_for_event(at_min)
                    bound["leader"] = new_leader
                    bound["prev_leader"] = prev
                    overrides["leader"] = new_leader
                if ref == "config_master.rollback_to_known_good":
                    bound["leader"] = active_leader_at_minute(at_min)
                    v = h_int(f"rollback|{at_min}", 100, 130)
                    bound["revert_to_version"] = v
                    bound["latest_readable_version"] = v
                    overrides["leader"] = bound["leader"]
                if ref == "gfe.bulk_config_reload":
                    v = h_int(f"rollback|{at_min}", 100, 130)
                    bound["cfg_version"] = v
                    bound["deleted_cfg"] = h_int(f"bulkdel|{at_min}", 500, 6000)
                if ref == "monitoring.page_sent":
                    bound["policy"] = "lb_error_rate"
                    bound["signal"] = "http_502_rate"
                    bound["value"] = f"{(0.2 + h_u01('page|value') * 0.5):.2f}"
                    bound["incident_key"] = gen_from_domain({"k": "str", "v": "INC-<7digits>"}, f"page|{at_min}|inc", bound)
                if ref == "config_master.ops_switch_master":
                    bound["from_leader"] = "cm-02"
                    bound["to_leader"] = "cm-01"
                if ref == "config_master.ops_force_config_push":
                    bound["leader"] = "cm-01"
                    bound["cfg_version"] = forced_cfg_version
                    bound["cfg_age_h"] = forced_cfg_age_h
                    overrides["leader"] = "cm-01"
                if ref == "gfe.config_apply_forced":
                    bound["cfg_version"] = forced_cfg_version
                    bound["cfg_age_h"] = forced_cfg_age_h
                    overrides["server"] = host

                emit_log(times[i], ref, "f", host, f"oneshot|{at_min}|{ref}|{i}", bound, overrides=overrides)


# -----------------------------
# Run simulation
# -----------------------------
def main() -> None:
    n_start, n_end = phase_window("n")
    background_emit_interval("n", n_start, n_end, bg_mult=None)
    flow_instances_interval("n", n_start, n_end, flow_mult=None, lat_mult=None)

    for iv in FAIL_INTERVALS:
        background_emit_interval("f", iv.start_min, iv.end_min, bg_mult=iv.bg_rate_mult)
        flow_instances_interval("f", iv.start_min, iv.end_min, flow_mult=iv.flow_rate_mult, lat_mult=iv.flow_latency_mult)

    emit_one_shots()

    df = pd.DataFrame(rows)
    df.sort_values(["timestamp_dt", "service", "host", "level", "message"], inplace=True, kind="mergesort")
    df["timestamp"] = df["timestamp_dt"].apply(fmt_ts)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
