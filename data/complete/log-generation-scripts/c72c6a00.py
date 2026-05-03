import math
import re
import uuid
import hashlib
import ipaddress
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# ----------------------------
# Embedded executable spec
# ----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "gce_external_connectivity"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "netcfg_mgr",
            "svc": "netcfg-mgr",
            "hosts": ["netcfg-1", "netcfg-2"],
            "logs": {
                "cfg_change_requested": {
                    "lvl": "INFO",
                    "msg": "config change requested cfg={cfg} change_id={change_id} actor={actor}",
                    "vars": {
                        "cfg": {"k": "ch", "v": ["cfg-20160411-1450", "cfg-rollback-20160411-1909"]},
                        "change_id": {"k": "hex", "v": 16},
                        "actor": {"k": "ch", "v": ["netops", "sre_oncall"]},
                    },
                },
                "cfg_validation_inconsistency": {
                    "lvl": "WARN",
                    "msg": "validation inconsistency change_id={change_id} detail={detail}",
                    "vars": {
                        "change_id": {"k": "hex", "v": 16},
                        "detail": {"k": "ch", "v": ["ip_block_list_mismatch", "stale_secondary_file", "render_inputs_out_of_sync"]},
                    },
                },
                "canary_failed": {
                    "lvl": "ERROR",
                    "msg": "canary verification failed cfg={cfg} canary_site={site} reason={reason}",
                    "vars": {
                        "cfg": {"k": "ch", "v": ["cfg-20160411-1450"]},
                        "site": {"k": "ch", "v": ["sfo", "iad", "lhr", "sin", "syd", "gru"]},
                        "reason": {"k": "ch", "v": ["no_prefixes_advertised", "external_probe_failed", "bgp_withdraw_detected"]},
                    },
                },
                "rollout_continues": {
                    "lvl": "ERROR",
                    "msg": "rollout controller continuing cfg={cfg} change_id={change_id} canary_state={canary_state}",
                    "vars": {
                        "cfg": {"k": "ch", "v": ["cfg-20160411-1450"]},
                        "change_id": {"k": "hex", "v": 16},
                        "canary_state": {"k": "ch", "v": ["pending", "assumed_pass", "unknown"]},
                    },
                },
                "push_site": {
                    "lvl": "INFO",
                    "msg": "push start cfg={cfg} site={site} push_id={push_id}",
                    "vars": {
                        "cfg": {"k": "ch", "v": ["cfg-20160411-1450", "cfg-rollback-20160411-1909"]},
                        "site": {"k": "ch", "v": ["sfo", "iad", "lhr", "sin", "syd", "gru"]},
                        "push_id": {"k": "hex", "v": 12},
                    },
                },
                "push_site_done": {
                    "lvl": "INFO",
                    "msg": "push done cfg={cfg} site={site} push_id={push_id} result={result} dur_ms={dur_ms}",
                    "vars": {
                        "cfg": {"k": "ch", "v": ["cfg-20160411-1450", "cfg-rollback-20160411-1909"]},
                        "site": {"k": "ch", "v": ["sfo", "iad", "lhr", "sin", "syd", "gru"]},
                        "push_id": {"k": "hex", "v": 12},
                        "result": {"k": "ch", "v": ["ok", "partial"]},
                        "dur_ms": {"k": "i", "v": [150, 1800]},
                    },
                },
                "rollback_started": {
                    "lvl": "WARN",
                    "msg": "rollback initiated target_cfg={cfg} ticket={ticket}",
                    "vars": {
                        "cfg": {"k": "ch", "v": ["cfg-rollback-20160411-1909"]},
                        "ticket": {"k": "str", "v": "INC[0-9]{6}"},
                    },
                },
                "mgr_heartbeat": {
                    "lvl": "DEBUG",
                    "msg": "heartbeat loop_lag_ms={loop_lag_ms}",
                    "vars": {"loop_lag_ms": {"k": "i", "v": [0, 120]}},
                },
            },
            "beh": {
                "n": [{"id": "mgr_heartbeat", "per_min": 0.5, "scope": "per_host"}],
                "f": [{"id": "mgr_heartbeat", "per_min": 0.8, "scope": "per_host"}],
            },
        },
        {
            "id": "edge_bgp_fabric",
            "svc": "edge-bgp",
            "hosts": ["edge-bgp-ctl"],
            "logs": {
                "apply_bad_config": {
                    "lvl": "WARN",
                    "msg": "applied config cfg={cfg} site={site} gce_prefixes={gce_prefixes} vpn_prefixes={vpn_prefixes}",
                    "vars": {
                        "cfg": {"k": "ch", "v": ["cfg-20160411-1450"]},
                        "site": {"k": "ch", "v": ["sfo", "iad", "lhr", "sin", "syd", "gru"]},
                        "gce_prefixes": {"k": "i", "v": [0, 0]},
                        "vpn_prefixes": {"k": "i", "v": [0, 0]},
                    },
                },
                "apply_safe_config": {
                    "lvl": "INFO",
                    "msg": "applied config cfg={cfg} site={site} gce_prefixes={gce_prefixes} vpn_prefixes={vpn_prefixes}",
                    "vars": {
                        "cfg": {"k": "ch", "v": ["cfg-rollback-20160411-1909"]},
                        "site": {"k": "ch", "v": ["sfo", "iad", "lhr", "sin", "syd", "gru"]},
                        "gce_prefixes": {"k": "i", "v": [96, 110]},
                        "vpn_prefixes": {"k": "i", "v": [14, 22]},
                    },
                },
                "bgp_announce_ok": {
                    "lvl": "INFO",
                    "msg": "bgp announcements healthy sites_advertising={sites} gce_prefixes={gce_prefixes} vpn_prefixes={vpn_prefixes}",
                    "vars": {
                        "sites": {"k": "i", "v": [18, 18]},
                        "gce_prefixes": {"k": "i", "v": [100, 110]},
                        "vpn_prefixes": {"k": "i", "v": [16, 20]},
                    },
                },
                "bgp_announce_degraded": {
                    "lvl": "WARN",
                    "msg": "bgp announcements degraded sites_advertising={sites} gce_prefixes={gce_prefixes} vpn_prefixes={vpn_prefixes}",
                    "vars": {
                        "sites": {"k": "i", "v": [1, 10]},
                        "gce_prefixes": {"k": "i", "v": [1, 110]},
                        "vpn_prefixes": {"k": "i", "v": [0, 20]},
                    },
                },
                "bgp_announce_zero": {
                    "lvl": "ERROR",
                    "msg": "bgp announcements missing sites_advertising={sites} gce_prefixes={gce_prefixes} vpn_prefixes={vpn_prefixes}",
                    "vars": {
                        "sites": {"k": "i", "v": [0, 0]},
                        "gce_prefixes": {"k": "i", "v": [0, 0]},
                        "vpn_prefixes": {"k": "i", "v": [0, 0]},
                    },
                },
                "bgp_announce_restored": {
                    "lvl": "INFO",
                    "msg": "bgp announcements restored sites_advertising={sites} gce_prefixes={gce_prefixes} vpn_prefixes={vpn_prefixes}",
                    "vars": {
                        "sites": {"k": "i", "v": [18, 18]},
                        "gce_prefixes": {"k": "i", "v": [100, 110]},
                        "vpn_prefixes": {"k": "i", "v": [16, 20]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "bgp_announce_ok", "per_min": 1.0, "scope": "global"}],
                "f": [
                    {"id": "bgp_announce_degraded", "per_min": 1.2, "scope": "global"},
                    {"id": "bgp_announce_zero", "per_min": 1.2, "scope": "global"},
                    {"id": "bgp_announce_restored", "per_min": 1.0, "scope": "global"},
                ],
            },
        },
        {
            "id": "netmon",
            "svc": "netmon",
            "hosts": ["netmon-1", "netmon-2", "netmon-3"],
            "logs": {
                "probe_ok": {
                    "lvl": "INFO",
                    "msg": "external probe ok target_ip={target_ip} pop={pop} rtt_ms={rtt_ms}",
                    "vars": {
                        "target_ip": {"k": "ip", "v": "35.235.0.0/16"},
                        "pop": {"k": "ch", "v": ["sfo", "iad", "lhr", "sin", "syd", "gru"]},
                        "rtt_ms": {"k": "i", "v": [20, 2500]},
                    },
                },
                "probe_timeout": {
                    "lvl": "ERROR",
                    "msg": "external probe failed target_ip={target_ip} pop={pop} err={err} waited_ms={waited_ms}",
                    "vars": {
                        "target_ip": {"k": "ip", "v": "35.235.0.0/16"},
                        "pop": {"k": "ch", "v": ["sfo", "iad", "lhr", "sin", "syd", "gru"]},
                        "err": {"k": "ch", "v": ["timeout", "no_reply", "tcp_syn_dropped"]},
                        "waited_ms": {"k": "i", "v": [800, 3500]},
                    },
                },
                "probe_retry": {
                    "lvl": "WARN",
                    "msg": "retrying external probe target_ip={target_ip} pop={pop} attempt={attempt} backoff_ms={backoff_ms}",
                    "vars": {
                        "target_ip": {"k": "ip", "v": "35.235.0.0/16"},
                        "pop": {"k": "ch", "v": ["sfo", "iad", "lhr", "sin", "syd", "gru"]},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "backoff_ms": {"k": "i", "v": [150, 1200]},
                    },
                },
                "vpn_probe_retry": {
                    "lvl": "WARN",
                    "msg": "retrying vpn probe region={region} tunnel_id={tunnel_id} attempt={attempt} backoff_ms={backoff_ms}",
                    "vars": {
                        "region": {"k": "ch", "v": ["asia-east1"]},
                        "tunnel_id": {"k": "uuid", "v": None},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "backoff_ms": {"k": "i", "v": [200, 1500]},
                    },
                },
                "probe_summary": {
                    "lvl": "INFO",
                    "msg": "probe summary window_s={window_s} success_pct={success_pct} rtt_p95_ms={rtt_p95_ms}",
                    "vars": {
                        "window_s": {"k": "i", "v": [60, 60]},
                        "success_pct": {"k": "i", "v": [0, 100]},
                        "rtt_p95_ms": {"k": "i", "v": [30, 3000]},
                    },
                },
                "alert_latency_anomaly": {
                    "lvl": "WARN",
                    "msg": "ALERT external latency elevated pop={pop} rtt_p95_ms={rtt_p95_ms}",
                    "vars": {
                        "pop": {"k": "ch", "v": ["sfo", "iad", "lhr", "sin", "syd", "gru"]},
                        "rtt_p95_ms": {"k": "i", "v": [400, 3000]},
                    },
                },
                "alert_vpn_unreachable": {
                    "lvl": "ERROR",
                    "msg": "ALERT vpn unreachable region={region} fail_rate_pct={fail_rate_pct}",
                    "vars": {
                        "region": {"k": "ch", "v": ["asia-east1"]},
                        "fail_rate_pct": {"k": "i", "v": [40, 100]},
                    },
                },
                "alert_inbound_loss": {
                    "lvl": "CRITICAL",
                    "msg": "ALERT inbound internet traffic loss observed loss_pct={loss_pct}",
                    "vars": {"loss_pct": {"k": "i", "v": [80, 100]}},
                },
            },
            "beh": {
                "n": [{"id": "probe_summary", "per_min": 1.0, "scope": "global"}],
                "f": [
                    {"id": "probe_summary", "per_min": 1.0, "scope": "global"},
                    {"id": "alert_latency_anomaly", "per_min": 0.6, "scope": "global"},
                    {"id": "alert_vpn_unreachable", "per_min": 0.7, "scope": "global"},
                    {"id": "alert_inbound_loss", "per_min": 1.8, "scope": "global"},
                ],
            },
        },
        {
            "id": "vpn_gateway",
            "svc": "cloud-vpn",
            "hosts": ["vpn-asia-east1-1", "vpn-asia-east1-2"],
            "logs": {
                "ike_keepalive_ok": {
                    "lvl": "INFO",
                    "msg": "ike keepalive ok region={region} tunnel_id={tunnel_id} rtt_ms={rtt_ms}",
                    "vars": {
                        "region": {"k": "ch", "v": ["asia-east1"]},
                        "tunnel_id": {"k": "uuid", "v": None},
                        "rtt_ms": {"k": "i", "v": [30, 2500]},
                    },
                },
                "ike_keepalive_fail": {
                    "lvl": "ERROR",
                    "msg": "ike keepalive failed region={region} tunnel_id={tunnel_id} err={err} waited_ms={waited_ms}",
                    "vars": {
                        "region": {"k": "ch", "v": ["asia-east1"]},
                        "tunnel_id": {"k": "uuid", "v": None},
                        "err": {"k": "ch", "v": ["no_response", "peer_unreachable", "packet_loss"]},
                        "waited_ms": {"k": "i", "v": [800, 3500]},
                    },
                },
                "vpn_sa_metric": {
                    "lvl": "INFO",
                    "msg": "vpn metric region={region} active_sas={active_sas}",
                    "vars": {"region": {"k": "ch", "v": ["asia-east1"]}, "active_sas": {"k": "i", "v": [500, 5000]}},
                },
            },
            "beh": {
                "n": [{"id": "vpn_sa_metric", "per_min": 0.5, "scope": "per_host"}],
                "f": [{"id": "vpn_sa_metric", "per_min": 0.8, "scope": "per_host"}],
            },
        },
        {
            "id": "sre_ops",
            "svc": "sre-ops",
            "hosts": ["sre-console"],
            "logs": {
                "pager_triggered": {
                    "lvl": "WARN",
                    "msg": "page triggered service={service} reason={reason}",
                    "vars": {
                        "service": {"k": "ch", "v": ["gce-network", "cloud-vpn"]},
                        "reason": {"k": "ch", "v": ["vpn_unreachable", "external_latency_anomaly", "inbound_loss"]},
                    },
                },
                "rollback_approved": {
                    "lvl": "WARN",
                    "msg": "rollback approved cfg={cfg} by={actor}",
                    "vars": {"cfg": {"k": "ch", "v": ["cfg-rollback-20160411-1909"]}, "actor": {"k": "ch", "v": ["sre_oncall"]}},
                },
                "change_freeze_enabled": {
                    "lvl": "INFO",
                    "msg": "network config freeze enabled ticket={ticket}",
                    "vars": {"ticket": {"k": "str", "v": "INC[0-9]{6}"}},
                },
                "change_freeze_status": {
                    "lvl": "INFO",
                    "msg": "network config freeze status enabled={enabled} ticket={ticket}",
                    "vars": {"enabled": {"k": "ch", "v": ["true"]}, "ticket": {"k": "str", "v": "INC[0-9]{6}"}},
                },
                "ops_audit": {
                    "lvl": "DEBUG",
                    "msg": "ops audit heartbeat queue_depth={queue_depth}",
                    "vars": {"queue_depth": {"k": "i", "v": [0, 20]}},
                },
            },
            "beh": {
                "n": [{"id": "ops_audit", "per_min": 0.1, "scope": "global"}, {"id": "change_freeze_status", "per_min": 0.0, "scope": "global"}],
                "f": [{"id": "ops_audit", "per_min": 0.2, "scope": "global"}, {"id": "change_freeze_status", "per_min": 0.4, "scope": "global"}],
            },
        },
    ],
    "flows": {
        "n": [
            {
                "id": "gce_external_probe_ok_n",
                "rpm": 420.0,
                "path": ["netmon", "edge_bgp_fabric"],
                "emit": ["netmon.probe_ok"],
                "latency_ms": [[40, 140]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "vpn_keepalive_ok_n",
                "rpm": 120.0,
                "path": ["netmon", "vpn_gateway"],
                "emit": ["vpn_gateway.ike_keepalive_ok"],
                "latency_ms": [[60, 220]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
        "f": [
            {
                "id": "gce_external_probe_ok_f",
                "rpm": 350.0,
                "path": ["netmon", "edge_bgp_fabric"],
                "emit": ["netmon.probe_ok"],
                "latency_ms": [[80, 280]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "gce_external_probe_timeout_f",
                "rpm": 60.0,
                "path": ["netmon", "edge_bgp_fabric"],
                "emit": ["netmon.probe_timeout"],
                "latency_ms": [[900, 2400]],
                "retry": {"max_attempts": 3, "expected_attempts": 2.5, "emit_per_retry": ["netmon.probe_retry"], "backoff_ms": [[250, 900], [450, 1300]]},
                "trace": False,
            },
            {
                "id": "vpn_keepalive_ok_f",
                "rpm": 90.0,
                "path": ["netmon", "vpn_gateway"],
                "emit": ["vpn_gateway.ike_keepalive_ok"],
                "latency_ms": [[90, 350]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "vpn_keepalive_fail_f",
                "rpm": 30.0,
                "path": ["netmon", "vpn_gateway"],
                "emit": ["vpn_gateway.ike_keepalive_fail"],
                "latency_ms": [[900, 2400]],
                "retry": {"max_attempts": 3, "expected_attempts": 2.0, "emit_per_retry": ["netmon.vpn_probe_retry"], "backoff_ms": [[300, 1100], [500, 1500]]},
                "trace": False,
            },
            {
                "id": "netcfg_push_bad_f",
                "rpm": 8.0,
                "path": ["netcfg_mgr", "edge_bgp_fabric", "netcfg_mgr"],
                "emit": ["netcfg_mgr.push_site", "edge_bgp_fabric.apply_bad_config", "netcfg_mgr.push_site_done"],
                "latency_ms": [[80, 250], [30, 120], [120, 700]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "netcfg_push_rollback_f",
                "rpm": 6.0,
                "path": ["netcfg_mgr", "edge_bgp_fabric", "netcfg_mgr"],
                "emit": ["netcfg_mgr.push_site", "edge_bgp_fabric.apply_safe_config", "netcfg_mgr.push_site_done"],
                "latency_ms": [[80, 250], [30, 120], [120, 700]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "gce_bgp_withdrawal_global_inbound_loss_20160411",
        "time": {"total_minutes": 60, "phases": {"n": {"start_min": 0, "end_min": 30}, "f": {"start_min": 30, "end_min": 60}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 30,
                        "rate_multipliers": {
                            "edge_bgp_fabric.bgp_announce_zero": 0.0,
                            "edge_bgp_fabric.bgp_announce_restored": 0.0,
                            "netmon.alert_latency_anomaly": 0.0,
                            "netmon.alert_vpn_unreachable": 0.0,
                            "netmon.alert_inbound_loss": 0.0,
                            "netcfg_push_rollback_f": 0.0,
                            "sre_ops.change_freeze_status": 0.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "netcfg_mgr.cfg_change_requested", "count": 1, "hosts": ["netcfg-1"]},
                            {"ref": "netcfg_mgr.cfg_validation_inconsistency", "count": 1, "hosts": ["netcfg-1"]},
                            {"ref": "netcfg_mgr.canary_failed", "count": 1, "hosts": ["netcfg-1"]},
                            {"ref": "netcfg_mgr.rollout_continues", "count": 1, "hosts": ["netcfg-1"]},
                        ],
                    },
                    {
                        "order": 2,
                        "at_min": 35,
                        "rate_multipliers": {
                            "vpn_keepalive_ok_f": 0.7,
                            "vpn_keepalive_fail_f": 2.0,
                            "netmon.alert_latency_anomaly": 1.0,
                            "netmon.alert_vpn_unreachable": 1.0,
                        },
                        "latency_multipliers": {
                            "gce_external_probe_ok_f": {"p50": 1.7, "p95": 2.0},
                            "vpn_keepalive_ok_f": {"p50": 1.5, "p95": 2.0},
                        },
                        "one_shots": [{"ref": "sre_ops.pager_triggered", "count": 1, "hosts": ["sre-console"]}],
                    },
                    {
                        "order": 3,
                        "at_min": 42,
                        "rate_multipliers": {
                            "edge_bgp_fabric.bgp_announce_degraded": 0.0,
                            "edge_bgp_fabric.bgp_announce_zero": 1.0,
                            "gce_external_probe_ok_f": 0.1,
                            "gce_external_probe_timeout_f": 6.0,
                            "vpn_keepalive_ok_f": 0.0,
                            "vpn_keepalive_fail_f": 4.0,
                            "netmon.alert_inbound_loss": 1.0,
                        },
                        "latency_multipliers": {
                            "gce_external_probe_timeout_f": {"p50": 1.1, "p95": 1.2},
                            "vpn_keepalive_fail_f": {"p50": 1.1, "p95": 1.2},
                        },
                        "one_shots": [],
                    },
                    {
                        "order": 4,
                        "at_min": 52,
                        "rate_multipliers": {
                            "netcfg_push_bad_f": 0.2,
                            "netcfg_push_rollback_f": 1.0,
                            "edge_bgp_fabric.bgp_announce_degraded": 0.7,
                            "edge_bgp_fabric.bgp_announce_zero": 0.6,
                            "gce_external_probe_ok_f": 0.3,
                            "gce_external_probe_timeout_f": 2.5,
                            "vpn_keepalive_ok_f": 0.1,
                            "vpn_keepalive_fail_f": 2.0,
                            "netmon.alert_inbound_loss": 0.6,
                            "sre_ops.change_freeze_status": 1.0,
                        },
                        "latency_multipliers": {"gce_external_probe_ok_f": {"p50": 1.2, "p95": 1.3}},
                        "one_shots": [
                            {"ref": "sre_ops.rollback_approved", "count": 1, "hosts": ["sre-console"]},
                            {"ref": "netcfg_mgr.rollback_started", "count": 1, "hosts": ["netcfg-2"]},
                            {"ref": "sre_ops.change_freeze_enabled", "count": 1, "hosts": ["sre-console"]},
                        ],
                    },
                    {
                        "order": 5,
                        "at_min": 56,
                        "rate_multipliers": {
                            "edge_bgp_fabric.bgp_announce_zero": 0.0,
                            "edge_bgp_fabric.bgp_announce_degraded": 0.0,
                            "edge_bgp_fabric.bgp_announce_restored": 1.0,
                            "netcfg_push_bad_f": 0.0,
                            "netcfg_push_rollback_f": 0.2,
                            "gce_external_probe_ok_f": 1.1,
                            "gce_external_probe_timeout_f": 0.1,
                            "vpn_keepalive_ok_f": 1.1,
                            "vpn_keepalive_fail_f": 0.1,
                            "netmon.alert_inbound_loss": 0.0,
                            "netmon.alert_vpn_unreachable": 0.0,
                            "netmon.alert_latency_anomaly": 0.2,
                        },
                        "latency_multipliers": {
                            "gce_external_probe_ok_f": {"p50": 0.7, "p95": 0.8},
                            "vpn_keepalive_ok_f": {"p50": 0.7, "p95": 0.8},
                        },
                        "one_shots": [],
                    },
                ]
            }
        },
    }
}

# ----------------------------
# Deterministic helpers
# ----------------------------

SEED = 1337


def isoformat_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond/1000):03d}Z"


def stable_u32(s: str) -> int:
    h = hashlib.md5(s.encode("utf-8")).digest()
    return int.from_bytes(h[:4], "big")


def clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


class DeterministicRounder:
    """Stable low-variance rounding using per-key fractional carry."""

    def __init__(self):
        self.carry: Dict[str, float] = {}

    def count(self, expected: float, key: str) -> int:
        c = self.carry.get(key, 0.0)
        x = expected + c
        n = int(math.floor(x + 1e-12))
        self.carry[key] = x - n
        return n


def lognormal_from_p50_p95(rng: np.random.RandomState, p50: float, p95: float, soft_cap: Optional[float] = None) -> float:
    """Calibrate lognormal where median=p50 and 95th=p95, then soft-cap."""
    p50 = max(0.0, float(p50))
    p95 = max(p50 + 1e-9, float(p95))
    if p50 == 0.0 and p95 == 0.0:
        return 0.0
    if p50 <= 0.0:
        v = float(p95) * 0.5
        return v if soft_cap is None else min(v, soft_cap)
    mu = math.log(p50)
    sigma = (math.log(p95) - mu) / 1.6448536269514722  # z0.95
    sigma = max(1e-6, sigma)
    v = float(rng.lognormal(mean=mu, sigma=sigma))
    if soft_cap is not None and v > soft_cap:
        v = soft_cap + (v - soft_cap) * 0.05
    return v


def schedule_uniform(start_dt: datetime, end_dt: datetime, n: int, key: str) -> List[datetime]:
    if n <= 0:
        return []
    total_s = (end_dt - start_dt).total_seconds()
    step = total_s / n
    jitter_max = min(0.2 * step, 0.35)
    out: List[datetime] = []
    for i in range(n):
        base = (i + 0.5) * step
        u = stable_u32(f"{key}:{i}")
        r = ((u % 2000001) / 1000000.0) - 1.0
        j = r * jitter_max
        t = start_dt + timedelta(seconds=base + j)
        if t < start_dt:
            t = start_dt
        if t >= end_dt:
            t = end_dt - timedelta(milliseconds=1)
        out.append(t)
    return out


def gen_hex(rng: np.random.RandomState, n: int) -> str:
    bytes_len = (n + 1) // 2
    b = rng.bytes(bytes_len)
    s = b.hex()
    return s[:n]


def gen_uuid(rng: np.random.RandomState) -> str:
    val = int.from_bytes(rng.bytes(16), "big")
    return str(uuid.UUID(int=val))


def gen_ip_from_cidr(cidr: str, idx: int) -> str:
    net = ipaddress.ip_network(cidr, strict=False)
    size = net.num_addresses
    offset = 10 + (idx * 131) % max(1, size - 20)
    ip = net.network_address + int(offset)
    return str(ip)


def gen_str_from_hint(rng: np.random.RandomState, hint: str) -> str:
    m = re.fullmatch(r"(INC)\[0-9\]\{(\d+)\}", hint)
    if m:
        prefix, digits = m.group(1), int(m.group(2))
        num = int(rng.randint(0, 10**digits))
        return f"{prefix}{num:0{digits}d}"
    return hint


def choose_host(component: Dict[str, Any], salt: str) -> str:
    hosts = component.get("hosts") or []
    if not hosts:
        return ""
    idx = stable_u32(salt) % len(hosts)
    return hosts[idx]


def get_int_bounds(ref: str, varname: str) -> Optional[Tuple[int, int]]:
    dom = LOG_BY_REF.get(ref, {}).get("vars", {}).get(varname)
    if not dom:
        return None
    if dom.get("k") != "i":
        return None
    lo, hi = int(dom["v"][0]), int(dom["v"][1])
    return lo, hi


def generate_freeze_ticket() -> str:
    # Single deterministic ticket used across rollback_started + freeze-enabled + freeze-status stream.
    return gen_str_from_hint(np.random.RandomState(stable_u32("ticket:52") ^ SEED), "INC[0-9]{6}")


# ----------------------------
# Build indices
# ----------------------------

COMP_BY_ID: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

LOG_BY_REF: Dict[str, Dict[str, Any]] = {}
for comp in SYSTEM["components"]:
    for log_id, log_def in comp["logs"].items():
        LOG_BY_REF[f"{comp['id']}.{log_id}"] = {
            "component_id": comp["id"],
            "service": comp.get("svc") or "",
            "hosts": comp.get("hosts") or [],
            "level": log_def["lvl"],
            "template": log_def["msg"],
            "vars": log_def.get("vars", {}),
        }

FLOWS_BY_STATE: Dict[str, Dict[str, Dict[str, Any]]] = {"n": {}, "f": {}}
for st in ["n", "f"]:
    for fdef in SYSTEM["flows"][st]:
        FLOWS_BY_STATE[st][fdef["id"]] = fdef

# ----------------------------
# Scenario control derivation
# ----------------------------


@dataclass(frozen=True)
class Interval:
    state: str
    start_min: int
    end_min: int
    start_dt: datetime
    end_dt: datetime
    rate_mult_flow: Dict[str, float]
    rate_mult_bg: Dict[str, float]
    latency_mult_flow: Dict[str, Dict[str, float]]


def build_intervals(base_dt: datetime) -> Tuple[List[Interval], List[Dict[str, Any]]]:
    phases = SCENARIO["scenario"]["time"]["phases"]
    n0, n1 = phases["n"]["start_min"], phases["n"]["end_min"]
    f0, f1 = phases["f"]["start_min"], phases["f"]["end_min"]

    events = list(SCENARIO["scenario"]["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e.get("order", 0)))

    intervals: List[Interval] = [
        Interval(
            state="n",
            start_min=n0,
            end_min=n1,
            start_dt=base_dt + timedelta(minutes=n0),
            end_dt=base_dt + timedelta(minutes=n1),
            rate_mult_flow={},
            rate_mult_bg={},
            latency_mult_flow={},
        )
    ]

    boundaries = sorted(set([f0] + [e["at_min"] for e in events] + [f1]))

    active_rate_flow: Dict[str, float] = {}
    active_rate_bg: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}

    by_time: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        by_time.setdefault(e["at_min"], []).append(e)

    for i in range(len(boundaries) - 1):
        b = boundaries[i]
        for e in by_time.get(b, []):
            for k, v in (e.get("rate_multipliers") or {}).items():
                if "." in k:
                    active_rate_bg[k] = float(v)
                else:
                    active_rate_flow[k] = float(v)
            for k, mult in (e.get("latency_multipliers") or {}).items():
                active_lat[k] = {"p50": float(mult.get("p50", 1.0)), "p95": float(mult.get("p95", 1.0))}
        smin, emin = boundaries[i], boundaries[i + 1]
        if smin < f0 or emin > f1:
            continue
        intervals.append(
            Interval(
                state="f",
                start_min=smin,
                end_min=emin,
                start_dt=base_dt + timedelta(minutes=smin),
                end_dt=base_dt + timedelta(minutes=emin),
                rate_mult_flow=dict(active_rate_flow),
                rate_mult_bg=dict(active_rate_bg),
                latency_mult_flow=dict(active_lat),
            )
        )

    return intervals, events


# ----------------------------
# Rendering + simulation
# ----------------------------

def render_message(template: str, values: Dict[str, Any]) -> str:
    def repl(m: re.Match) -> str:
        k = m.group(1)
        v = values.get(k, "")
        return str(v)

    return re.sub(r"\{([a-zA-Z0-9_]+)\}", repl, template)


def gen_from_domain(rng: np.random.RandomState, dom: Dict[str, Any], hint_idx: int = 0) -> Any:
    k = dom["k"]
    v = dom.get("v")
    if k == "ch":
        arr = list(v) if v is not None else [""]
        if not arr:
            return ""
        return arr[int(rng.randint(0, len(arr)))]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        return int(rng.randint(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        if abs(hi - lo) < 1e-12:
            return lo
        return float(lo + (hi - lo) * rng.rand())
    if k == "hex":
        n = int(v)
        return gen_hex(rng, n)
    if k == "uuid":
        return gen_uuid(rng)
    if k == "ip":
        cidr = str(v)
        return gen_ip_from_cidr(cidr, hint_idx)
    if k == "str":
        return gen_str_from_hint(rng, str(v))
    return ""


def compute_stage_probe_summary(interval: Interval) -> Tuple[int, int]:
    if interval.state != "f":
        return 99, 140

    ok_flow = "gce_external_probe_ok_f"
    to_flow = "gce_external_probe_timeout_f"
    ok_base = FLOWS_BY_STATE["f"][ok_flow]["rpm"]
    to_base = FLOWS_BY_STATE["f"][to_flow]["rpm"]
    ok_r = ok_base * interval.rate_mult_flow.get(ok_flow, 1.0)
    to_r = to_base * interval.rate_mult_flow.get(to_flow, 1.0)
    denom = ok_r + to_r
    if denom <= 1e-9:
        success = 0
    else:
        success = int(round(100.0 * ok_r / denom))
    success = int(clamp(success, 0, 100))

    base_p95 = float(FLOWS_BY_STATE["f"][ok_flow]["latency_ms"][0][1])
    lm = interval.latency_mult_flow.get(ok_flow, {"p50": 1.0, "p95": 1.0})
    rtt_p95 = int(round(clamp(base_p95 * float(lm.get("p95", 1.0)), 30, 3000)))
    return success, rtt_p95


def attempt_count_list(rounder: DeterministicRounder, flow_id: str, interval_key: str, n: int, expected: float, max_attempts: int) -> List[int]:
    """Stable two-point mixture around floor/ceil(expected), with deterministic index selection."""
    if n <= 0:
        return []
    low = int(math.floor(expected + 1e-12))
    low = max(1, min(low, max_attempts))
    high = min(max_attempts, low + 1)
    if abs(expected - low) < 1e-9 or high == low:
        return [low] * n
    frac = clamp(expected - low, 0.0, 1.0)
    expected_high = frac * n
    nhigh = rounder.count(expected_high, f"attemptmix:{flow_id}:{interval_key}")
    nhigh = max(0, min(n, nhigh))

    out = [low] * n
    if nhigh > 0:
        used = [False] * n
        for k in range(nhigh):
            pos = int(math.floor((k + 0.5) * n / nhigh))
            pos = max(0, min(n - 1, pos))
            if used[pos]:
                j = pos
                while j < n and used[j]:
                    j += 1
                if j >= n:
                    j = 0
                    while j < n and used[j]:
                        j += 1
                    if j >= n:
                        j = pos
                pos = j
            used[pos] = True
            out[pos] = high
    return out


def host_for_emit(comp_id: str, instance_salt: str) -> str:
    comp = COMP_BY_ID[comp_id]
    return choose_host(comp, f"{comp_id}:{instance_salt}")


def emit_log(rows: List[Dict[str, Any]], ts: datetime, ref: str, values: Dict[str, Any], host_override: Optional[str] = None, trace_id: str = ""):
    log_def = LOG_BY_REF[ref]
    comp_id = log_def["component_id"]
    host = host_override if host_override is not None else host_for_emit(comp_id, values.get("_host_salt", ""))
    msg = render_message(log_def["template"], values)
    rows.append(
        {
            "timestamp": ts,
            "level": log_def["level"],
            "message": msg,
            "trace_id": trace_id,
            "service": log_def["service"],
            "host": host,
        }
    )


def _sample_delay_ms(
    rng: np.random.RandomState,
    ref: str,
    p50: float,
    p95: float,
    lmult: Dict[str, float],
) -> float:
    sp50 = float(p50) * float(lmult.get("p50", 1.0))
    sp95 = float(p95) * float(lmult.get("p95", 1.0))
    cap = 3.0 * float(sp95)

    timing_bounds: Optional[Tuple[int, int]] = None
    for candidate in ("waited_ms", "rtt_ms"):
        b = get_int_bounds(ref, candidate)
        if b is not None:
            timing_bounds = b
            cap = min(cap, float(b[1]))
            break

    delay_ms = lognormal_from_p50_p95(rng, sp50, sp95, soft_cap=cap)
    delay_ms = max(0.0, delay_ms)
    if timing_bounds is not None:
        lo_t, hi_t = timing_bounds
        delay_ms = clamp(delay_ms, float(lo_t), float(hi_t))
    return float(delay_ms)


def _adjust_netcfg_push_durations(delays_ms: List[float], dur_bounds: Tuple[int, int]) -> List[float]:
    """
    Ensure netcfg_mgr.push_site_done's dur_ms (interpreted as elapsed between push_site and push_site_done)
    matches the emitted chronology AND stays within the dur_ms domain.
    In this flow, that's delays_ms[1:] (between push start -> apply -> push done).
    """
    if len(delays_ms) < 2:
        return delays_ms
    dur_lo, dur_hi = dur_bounds
    cur = float(sum(delays_ms[1:]))
    if cur <= 0.0:
        # Degenerate; enforce minimum using last stage.
        delays_ms = list(delays_ms)
        delays_ms[-1] = float(dur_lo)
        return delays_ms

    # Avoid rounding pushing us barely over the bound.
    hi_target = float(dur_hi) - 0.25
    lo_target = float(dur_lo) + 0.25

    if cur > hi_target:
        s = hi_target / cur
    elif cur < lo_target:
        s = lo_target / cur
    else:
        return delays_ms

    adj = list(delays_ms)
    for i in range(1, len(adj)):
        adj[i] = max(0.0, adj[i] * s)
    return adj


def simulate_flow_instance(
    rng: np.random.RandomState,
    rows: List[Dict[str, Any]],
    flow: Dict[str, Any],
    start_ts: datetime,
    interval: Interval,
    instance_idx: int,
    attempts: int,
):
    flow_id = flow["id"]
    emit_refs = list(flow["emit"])
    retry_refs = list(flow["retry"].get("emit_per_retry") or [])
    latency_pairs = list(flow["latency_ms"])
    backoff_pairs = list(flow["retry"].get("backoff_ms") or [])

    inst_salt = f"{flow_id}:{interval.start_min}:{instance_idx}"
    inst_ctx: Dict[str, Any] = {"_host_salt": inst_salt}

    if flow_id.startswith("gce_external_probe_"):
        inst_ctx["target_ip"] = gen_ip_from_cidr("35.235.0.0/16", stable_u32(inst_salt) % 65536)
        pops = LOG_BY_REF["netmon.probe_ok"]["vars"]["pop"]["v"]
        inst_ctx["pop"] = pops[stable_u32(inst_salt + ":pop") % len(pops)]
        if emit_refs and emit_refs[0] == "netmon.probe_timeout":
            errs = LOG_BY_REF["netmon.probe_timeout"]["vars"]["err"]["v"]
            inst_ctx["err"] = errs[stable_u32(inst_salt + ":err") % len(errs)]
    elif flow_id.startswith("vpn_keepalive_"):
        inst_ctx["region"] = "asia-east1"
        inst_ctx["tunnel_id"] = gen_uuid(np.random.RandomState(stable_u32(inst_salt + ":tunnel") ^ SEED))
        if emit_refs and emit_refs[0] == "vpn_gateway.ike_keepalive_fail":
            errs = LOG_BY_REF["vpn_gateway.ike_keepalive_fail"]["vars"]["err"]["v"]
            inst_ctx["err"] = errs[stable_u32(inst_salt + ":err") % len(errs)]
    elif flow_id.startswith("netcfg_push_"):
        sites = LOG_BY_REF["netcfg_mgr.push_site"]["vars"]["site"]["v"]
        inst_ctx["site"] = sites[stable_u32(inst_salt + ":site") % len(sites)]
        inst_ctx["push_id"] = gen_hex(np.random.RandomState(stable_u32(inst_salt + ":push") ^ SEED), 12)
        inst_ctx["cfg"] = "cfg-20160411-1450" if flow_id.endswith("_bad_f") else "cfg-rollback-20160411-1909"

    lmult = interval.latency_mult_flow.get(flow_id, {"p50": 1.0, "p95": 1.0})

    prev_attempt_end = start_ts

    # If present, interpret push done dur_ms as (push_site_done_ts - push_site_ts) in ms.
    push_done_ref = "netcfg_mgr.push_site_done"
    push_dur_bounds = get_int_bounds(push_done_ref, "dur_ms") if push_done_ref in emit_refs else None

    for a in range(1, attempts + 1):
        if a == 1:
            attempt_start = start_ts
        else:
            p50, p95 = backoff_pairs[a - 2]
            cap = 3.0 * float(p95)

            dom_bounds = None
            if retry_refs:
                dom_bounds = get_int_bounds(retry_refs[0], "backoff_ms")
            if dom_bounds is not None:
                lo_b, hi_b = dom_bounds
                cap = min(cap, float(hi_b))

            backoff_ms = lognormal_from_p50_p95(rng, float(p50), float(p95), soft_cap=cap)
            if dom_bounds is not None:
                lo_b, hi_b = dom_bounds
                backoff_ms = clamp(backoff_ms, float(lo_b), float(hi_b))
            else:
                backoff_ms = max(0.0, backoff_ms)

            attempt_start = prev_attempt_end + timedelta(milliseconds=backoff_ms)

            for rr in retry_refs:
                rr_vals = dict(inst_ctx)
                rr_vals["attempt"] = a
                rr_vals["backoff_ms"] = int(round(backoff_ms))
                emit_log(rows, attempt_start + timedelta(milliseconds=1), rr, rr_vals, trace_id="")

        # Sample all stage delays first so any derived timings (dur_ms) match chronology exactly.
        delays_ms: List[float] = []
        for j, ref in enumerate(emit_refs):
            p50, p95 = latency_pairs[j]
            delays_ms.append(_sample_delay_ms(rng, ref, p50, p95, lmult))

        # Fix: keep netcfg push dur_ms compatible with timestamps AND within dur_ms domain.
        if push_dur_bounds is not None:
            delays_ms = _adjust_netcfg_push_durations(delays_ms, push_dur_bounds)

        t = attempt_start
        push_site_ts: Optional[datetime] = None

        for j, ref in enumerate(emit_refs):
            t = t + timedelta(milliseconds=delays_ms[j])
            vals = dict(inst_ctx)

            if ref == "netmon.probe_ok":
                vals["rtt_ms"] = int(round(delays_ms[j]))
            elif ref == "netmon.probe_timeout":
                vals["waited_ms"] = int(round(delays_ms[j]))
                vals.setdefault("err", inst_ctx.get("err", "timeout"))
            elif ref == "vpn_gateway.ike_keepalive_ok":
                vals["rtt_ms"] = int(round(delays_ms[j]))
            elif ref == "vpn_gateway.ike_keepalive_fail":
                vals["waited_ms"] = int(round(delays_ms[j]))
                vals.setdefault("err", inst_ctx.get("err", "no_response"))
            elif ref == "edge_bgp_fabric.apply_bad_config":
                vals["gce_prefixes"] = 0
                vals["vpn_prefixes"] = 0
            elif ref == "netcfg_mgr.push_site":
                push_site_ts = t
            elif ref == "netcfg_mgr.push_site_done":
                # Use the actual timestamp gap since push start; delays were already adjusted to domain bounds.
                if push_site_ts is None:
                    push_site_ts = attempt_start
                dur_ms = int(round((t - push_site_ts).total_seconds() * 1000.0))
                if push_dur_bounds is not None:
                    dur_ms = int(clamp(dur_ms, push_dur_bounds[0], push_dur_bounds[1]))
                vals["dur_ms"] = dur_ms
                vals["result"] = "partial" if (stable_u32(inst_salt + ":res") % 23 == 0) else "ok"
            elif ref == "edge_bgp_fabric.apply_safe_config":
                vals["gce_prefixes"] = int(96 + (stable_u32(inst_salt + ":gce") % (110 - 96 + 1)))
                vals["vpn_prefixes"] = int(14 + (stable_u32(inst_salt + ":vpn") % (22 - 14 + 1)))

            emit_log(rows, t, ref, vals, trace_id="")

        prev_attempt_end = t


def simulate_background(
    rng: np.random.RandomState,
    rows: List[Dict[str, Any]],
    interval: Interval,
    rounder: DeterministicRounder,
    freeze_ticket: str,
):
    for comp in SYSTEM["components"]:
        beh = comp["beh"][interval.state]
        for src in beh:
            log_id = src["id"]
            ref = f"{comp['id']}.{log_id}"
            base_rate = float(src["per_min"])
            scope = src.get("scope") or "per_host"

            mult = 1.0
            if interval.state == "f":
                mult = float(interval.rate_mult_bg.get(ref, 1.0))
            eff_rate = base_rate * mult
            dur_min = float(interval.end_min - interval.start_min)
            if eff_rate <= 0.0 or dur_min <= 0.0:
                continue

            if scope == "global":
                expected = eff_rate * dur_min
                n = rounder.count(expected, f"bg:{ref}:global")
                times = schedule_uniform(interval.start_dt, interval.end_dt, n, f"bg:{ref}:global:{interval.start_min}")
                for i, ts in enumerate(times):
                    host = comp["hosts"][0] if comp.get("hosts") else ""
                    vals: Dict[str, Any] = {"_host_salt": f"bg:{ref}:{interval.start_min}:{i}"}

                    if ref == "netmon.probe_summary":
                        success_pct, rtt_p95 = compute_stage_probe_summary(interval)
                        vals["window_s"] = 60
                        vals["success_pct"] = int(clamp(success_pct, 0, 100))
                        vals["rtt_p95_ms"] = int(clamp(rtt_p95, 30, 3000))
                    elif ref == "netmon.alert_latency_anomaly":
                        pops = LOG_BY_REF[ref]["vars"]["pop"]["v"]
                        vals["pop"] = pops[stable_u32(vals["_host_salt"] + ":pop") % len(pops)]
                        _, derived = compute_stage_probe_summary(interval)
                        vals["rtt_p95_ms"] = int(clamp(max(derived, 400), 400, 3000))
                    elif ref == "netmon.alert_vpn_unreachable":
                        vals["region"] = "asia-east1"
                        fail = 55 if interval.start_min < 42 else (90 if interval.start_min < 56 else 45)
                        vals["fail_rate_pct"] = int(clamp(fail, 40, 100))
                    elif ref == "netmon.alert_inbound_loss":
                        loss = 85 if interval.start_min < 42 else (98 if interval.start_min < 56 else 82)
                        vals["loss_pct"] = int(clamp(loss, 80, 100))
                    elif ref.startswith("edge_bgp_fabric.bgp_announce_degraded"):
                        dom_sites = LOG_BY_REF[ref]["vars"]["sites"]["v"]
                        dom_gce = LOG_BY_REF[ref]["vars"]["gce_prefixes"]["v"]
                        dom_vpn = LOG_BY_REF[ref]["vars"]["vpn_prefixes"]["v"]
                        if interval.start_min < 42:
                            sites = 8
                        elif interval.start_min < 52:
                            sites = 2
                        else:
                            sites = 6
                        vals["sites"] = int(clamp(sites, dom_sites[0], dom_sites[1]))
                        vals["gce_prefixes"] = int(clamp(80, dom_gce[0], dom_gce[1]))
                        vals["vpn_prefixes"] = int(clamp(10, dom_vpn[0], dom_vpn[1]))
                    elif ref == "sre_ops.change_freeze_status":
                        vals["enabled"] = "true"
                        vals["ticket"] = freeze_ticket
                    else:
                        vars_dom = LOG_BY_REF[ref]["vars"]
                        for vn, dom in vars_dom.items():
                            vals[vn] = gen_from_domain(rng, dom, hint_idx=stable_u32(vals["_host_salt"] + ":" + vn) & 0xFFFF)

                    emit_log(rows, ts, ref, vals, host_override=host, trace_id="")
            else:
                for host in comp.get("hosts") or [""]:
                    expected = eff_rate * dur_min
                    n = rounder.count(expected, f"bg:{ref}:{host}")
                    times = schedule_uniform(interval.start_dt, interval.end_dt, n, f"bg:{ref}:{host}:{interval.start_min}")
                    for i, ts in enumerate(times):
                        vals = {"_host_salt": f"bg:{ref}:{host}:{interval.start_min}:{i}"}
                        vars_dom = LOG_BY_REF[ref]["vars"]
                        for vn, dom in vars_dom.items():
                            vals[vn] = gen_from_domain(rng, dom, hint_idx=stable_u32(vals["_host_salt"] + ":" + vn) & 0xFFFF)
                        emit_log(rows, ts, ref, vals, host_override=host, trace_id="")


def simulate_flows(
    rng: np.random.RandomState,
    rows: List[Dict[str, Any]],
    interval: Interval,
    rounder: DeterministicRounder,
):
    flows = SYSTEM["flows"][interval.state]
    dur_min = float(interval.end_min - interval.start_min)
    if dur_min <= 0.0:
        return

    for flow in flows:
        flow_id = flow["id"]
        base_rpm = float(flow["rpm"])
        mult = 1.0
        if interval.state == "f":
            mult = float(interval.rate_mult_flow.get(flow_id, 1.0))
        eff_rpm = base_rpm * mult
        if eff_rpm <= 0.0:
            continue

        expected_instances = eff_rpm * dur_min
        n_instances = rounder.count(expected_instances, f"flowinst:{interval.state}:{flow_id}")
        if n_instances <= 0:
            continue

        starts = schedule_uniform(interval.start_dt, interval.end_dt, n_instances, f"flowstart:{flow_id}:{interval.start_min}")

        r = flow["retry"]
        max_attempts = int(r["max_attempts"])
        expected_attempts = float(r["expected_attempts"])
        attempts_list = attempt_count_list(rounder, flow_id, f"{interval.start_min}-{interval.end_min}", n_instances, expected_attempts, max_attempts)

        for i, st in enumerate(starts):
            simulate_flow_instance(rng, rows, flow, st, interval, i, attempts_list[i])


def simulate_one_shots(
    rng: np.random.RandomState,
    rows: List[Dict[str, Any]],
    base_dt: datetime,
    events: List[Dict[str, Any]],
    freeze_ticket: str,
):
    event_ctx: Dict[int, Dict[str, Any]] = {}
    for e in events:
        at = int(e["at_min"])
        ctx: Dict[str, Any] = {}
        if at == 30:
            ctx["cfg"] = "cfg-20160411-1450"
            ctx["change_id"] = gen_hex(np.random.RandomState(stable_u32("change_id:30") ^ SEED), 16)
            ctx["actor"] = "netops"
        if at == 52:
            ctx["ticket"] = freeze_ticket
        event_ctx[at] = ctx

    for e in events:
        at = int(e["at_min"])
        t0 = base_dt + timedelta(minutes=at)
        for os in (e.get("one_shots") or []):
            ref = os["ref"]
            count = int(os["count"])
            hosts = os.get("hosts") or []
            for i in range(count):
                u = stable_u32(f"oneshot:{ref}:{at}:{i}")
                jitter_ms = int((u % 800) - 400)
                ts = t0 + timedelta(milliseconds=jitter_ms)
                if ts < t0:
                    ts = t0

                vals: Dict[str, Any] = {"_host_salt": f"oneshot:{ref}:{at}:{i}"}
                vals.update(event_ctx.get(at, {}))

                vars_dom = LOG_BY_REF[ref]["vars"]
                for vn, dom in vars_dom.items():
                    if vn in vals:
                        continue
                    if ref == "netcfg_mgr.rollout_continues" and vn == "cfg":
                        vals[vn] = "cfg-20160411-1450"
                    else:
                        vals[vn] = gen_from_domain(rng, dom, hint_idx=stable_u32(vals["_host_salt"] + ":" + vn) & 0xFFFF)

                if ref in ("netcfg_mgr.cfg_validation_inconsistency", "netcfg_mgr.rollout_continues", "netcfg_mgr.cfg_change_requested"):
                    vals["change_id"] = event_ctx.get(30, {}).get("change_id", vals.get("change_id"))
                if ref == "netcfg_mgr.cfg_change_requested":
                    vals["cfg"] = "cfg-20160411-1450"
                    vals["actor"] = "netops"
                if ref == "netcfg_mgr.canary_failed":
                    vals["cfg"] = "cfg-20160411-1450"
                if ref in ("netcfg_mgr.rollback_started", "sre_ops.change_freeze_enabled", "sre_ops.change_freeze_status"):
                    vals["ticket"] = freeze_ticket

                host_override = hosts[0] if hosts else None
                emit_log(rows, ts, ref, vals, host_override=host_override, trace_id="")


# ----------------------------
# Main
# ----------------------------

def main():
    random.seed(SEED)
    rng = np.random.RandomState(SEED)
    base_dt = datetime(2016, 4, 11, 19, 0, 0, tzinfo=timezone.utc)

    freeze_ticket = generate_freeze_ticket()

    intervals, events = build_intervals(base_dt)

    rounder = DeterministicRounder()
    rows: List[Dict[str, Any]] = []

    for interval in intervals:
        simulate_background(rng, rows, interval, rounder, freeze_ticket=freeze_ticket)

    for interval in intervals:
        simulate_flows(rng, rows, interval, rounder)

    simulate_one_shots(rng, rows, base_dt, events, freeze_ticket=freeze_ticket)

    df = pd.DataFrame(rows)
    df["timestamp"] = df["timestamp"].map(isoformat_ms)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)

    nrows = len(df)
    if not (20000 <= nrows <= 100000):
        raise RuntimeError(f"Row count out of target range: {nrows}")

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
