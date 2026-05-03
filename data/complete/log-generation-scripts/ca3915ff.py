import math
import re
import uuid
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd


SYSTEM: Dict[str, Any] = {
    "sys": {"id": "gcp_backbone_corridor_convergence_incident"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "api_edge_uscentral",
            "svc": "api-edge",
            "hosts": ["edge-uc1-a", "edge-uc1-b"],
            "logs": {
                "edge_health": {
                    "lvl": "INFO",
                    "msg": "edge health ok active_conns={active_conns} cpu_pct={cpu_pct}",
                    "vars": {"active_conns": {"k": "i", "v": [800, 9000]}, "cpu_pct": {"k": "i", "v": [10, 85]}},
                },
                "edge_req_gcs": {
                    "lvl": "INFO",
                    "msg": "recv gcs op={op} bucket={bucket} client_ip={client_ip} req_id={req_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["list", "get", "put", "delete"]},
                        "bucket": {"k": "ch", "v": ["photos-prod", "logs-archive", "backups"]},
                        "client_ip": {"k": "ip", "v": "198.51.100.0/24"},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "edge_resp_gcs_ok": {
                    "lvl": "INFO",
                    "msg": "resp gcs status=200 dur_ms={dur_ms} req_id={req_id}",
                    "vars": {"dur_ms": {"k": "i", "v": [15, 8000]}, "req_id": {"k": "uuid", "v": None}},
                },
                "edge_resp_gcs_err": {
                    "lvl": "WARN",
                    "msg": "resp gcs status={status} err={err} dur_ms={dur_ms} req_id={req_id}",
                    "vars": {
                        "status": {"k": "ch", "v": [500, 503]},
                        "err": {"k": "ch", "v": ["upstream_timeout", "connection_reset", "internal_error"]},
                        "dur_ms": {"k": "i", "v": [50, 12000]},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "edge_req_sql": {
                    "lvl": "INFO",
                    "msg": "recv cloudsql op={op} instance={instance} client_ip={client_ip} req_id={req_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["export", "update", "delete"]},
                        "instance": {"k": "ch", "v": ["orders-db", "billing-db", "users-db"]},
                        "client_ip": {"k": "ip", "v": "198.51.100.0/24"},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "edge_resp_sql_ok": {
                    "lvl": "INFO",
                    "msg": "resp cloudsql status=200 dur_ms={dur_ms} req_id={req_id}",
                    "vars": {"dur_ms": {"k": "i", "v": [30, 12000]}, "req_id": {"k": "uuid", "v": None}},
                },
                "edge_resp_sql_err": {
                    "lvl": "WARN",
                    "msg": "resp cloudsql status={status} err={err} dur_ms={dur_ms} req_id={req_id}",
                    "vars": {
                        "status": {"k": "ch", "v": [500, 503]},
                        "err": {"k": "ch", "v": ["upstream_timeout", "unavailable", "deadline_exceeded"]},
                        "dur_ms": {"k": "i", "v": [100, 20000]},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "edge_req_msg": {
                    "lvl": "INFO",
                    "msg": "recv messages op=publish topic={topic} client_ip={client_ip} req_id={req_id}",
                    "vars": {
                        "topic": {"k": "ch", "v": ["events", "alerts", "audit"]},
                        "client_ip": {"k": "ip", "v": "198.51.100.0/24"},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "edge_resp_msg_ok": {
                    "lvl": "INFO",
                    "msg": "resp messages status=200 dur_ms={dur_ms} req_id={req_id}",
                    "vars": {"dur_ms": {"k": "i", "v": [5, 6000]}, "req_id": {"k": "uuid", "v": None}},
                },
                "edge_resp_msg_err": {
                    "lvl": "WARN",
                    "msg": "resp messages status={status} err={err} dur_ms={dur_ms} req_id={req_id}",
                    "vars": {
                        "status": {"k": "ch", "v": [500, 503]},
                        "err": {"k": "ch", "v": ["upstream_timeout", "unavailable"]},
                        "dur_ms": {"k": "i", "v": [50, 12000]},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "edge_health", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "edge_health", "per_min": 0.8, "scope": "per_host"}]},
            },
        },
        {
            "id": "backbone_gateway_central",
            "svc": "backbone-gw",
            "hosts": ["gw-uc1-r1", "gw-uc1-r2"],
            "logs": {
                "link_state_change": {
                    "lvl": "ERROR",
                    "msg": "if={iface} peer={peer} state={state} reason={reason}",
                    "vars": {
                        "iface": {"k": "ch", "v": ["xe-2/0/0", "xe-3/0/0"]},
                        "peer": {"k": "ch", "v": ["pop-uw1-r1"]},
                        "state": {"k": "ch", "v": ["down"]},
                        "reason": {"k": "ch", "v": ["signal_loss", "remote_fault"]},
                    },
                },
                "iface_stats": {
                    "lvl": "INFO",
                    "msg": "if={iface} tx_gbps={tx_gbps} rx_gbps={rx_gbps} drop_ppm={drop_ppm}",
                    "vars": {
                        "iface": {"k": "ch", "v": ["xe-2/0/0", "xe-3/0/0"]},
                        "tx_gbps": {"k": "i", "v": [10, 120]},
                        "rx_gbps": {"k": "i", "v": [10, 120]},
                    },
                    "state_vars": {"n": {"drop_ppm": {"k": "i", "v": [0, 30]}}, "f": {"drop_ppm": {"k": "i", "v": [0, 6000]}}},
                },
                "rib_update": {"lvl": "INFO", "msg": "rib update prefix_count={prefix_count} dur_ms={dur_ms}", "vars": {"prefix_count": {"k": "i", "v": [200, 5000]}, "dur_ms": {"k": "i", "v": [5, 800]}}},
            },
            "beh": {"n": {"emit": [{"id": "iface_stats", "per_min": 1.0, "scope": "per_host"}]}, "f": {"emit": [{"id": "iface_stats", "per_min": 2.0, "scope": "per_host"}, {"id": "rib_update", "per_min": 0.6, "scope": "per_host"}]}},
        },
        {
            "id": "backbone_pop_west",
            "svc": "backbone-pop",
            "hosts": ["pop-uw1-r1"],
            "logs": {"iface_stats": {"lvl": "INFO", "msg": "if={iface} rx_gbps={rx_gbps} drop_ppm={drop_ppm}", "vars": {"iface": {"k": "ch", "v": ["et-0/0/0"]}, "rx_gbps": {"k": "i", "v": [10, 120]}}, "state_vars": {"n": {"drop_ppm": {"k": "i", "v": [0, 25]}}, "f": {"drop_ppm": {"k": "i", "v": [0, 6000]}}}}},
            "beh": {"n": {"emit": [{"id": "iface_stats", "per_min": 1.0, "scope": "per_host"}]}, "f": {"emit": [{"id": "iface_stats", "per_min": 2.0, "scope": "per_host"}]}},
        },
        {
            "id": "te_controller",
            "svc": "te-controller",
            "hosts": ["te-ctrl-1"],
            "logs": {
                "ctrl_tick": {"lvl": "INFO", "msg": "te tick corridor={corridor} pending_changes={pending_changes}", "vars": {"corridor": {"k": "ch", "v": ["us-central1<>us-west1", "us-central1<>europe-west1"]}, "pending_changes": {"k": "i", "v": [0, 50]}}},
                "reconv_start": {"lvl": "WARN", "msg": "reconvergence started corridor={corridor} trigger={trigger}", "vars": {"corridor": {"k": "ch", "v": ["us-central1<>us-west1", "us-central1<>europe-west1"]}, "trigger": {"k": "ch", "v": ["bandwidth_drop", "link_down"]}}},
                "path_selected": {"lvl": "WARN", "msg": "alternate path selected corridor={corridor} path_rank={path_rank}", "vars": {"corridor": {"k": "ch", "v": ["us-central1<>us-west1"]}, "path_rank": {"k": "i", "v": [3, 3]}}},
                "reconv_progress": {"lvl": "INFO", "msg": "reconvergence progress corridor={corridor} pct={pct} pending_flows={pending_flows}", "vars": {"corridor": {"k": "ch", "v": ["us-central1<>us-west1"]}, "pct": {"k": "i", "v": [5, 99]}, "pending_flows": {"k": "i", "v": [50, 5000]}}},
                "reconv_complete": {"lvl": "INFO", "msg": "reconvergence completed corridor={corridor} duration_s={duration_s}", "vars": {"corridor": {"k": "ch", "v": ["us-central1<>us-west1"]}, "duration_s": {"k": "i", "v": [300, 2400]}}},
            },
            "beh": {"n": {"emit": [{"id": "ctrl_tick", "per_min": 1.0, "scope": "global"}]}, "f": {"emit": [{"id": "ctrl_tick", "per_min": 1.0, "scope": "global"}, {"id": "reconv_progress", "per_min": 1.0, "scope": "global"}]}},
        },
        {
            "id": "netmon",
            "svc": "netmon",
            "hosts": ["netmon-1"],
            "logs": {
                "corridor_health": {
                    "lvl": "INFO",
                    "msg": "corridor={corridor} loss_pct={loss_pct} rtt_ms={rtt_ms} avail_bw_gbps={avail_bw_gbps}",
                    "vars": {"corridor": {"k": "ch", "v": ["us-central1<>us-west1", "us-central1<>europe-west1"]}},
                    "state_vars": {"n": {"loss_pct": {"k": "f", "v": [0.0, 0.3]}, "rtt_ms": {"k": "i", "v": [20, 45]}, "avail_bw_gbps": {"k": "i", "v": [80, 140]}}, "f": {"loss_pct": {"k": "f", "v": [0.2, 8.0]}, "rtt_ms": {"k": "i", "v": [30, 160]}, "avail_bw_gbps": {"k": "i", "v": [20, 110]}}},
                },
                "loss_alarm": {"lvl": "WARN", "msg": "ALARM packet_loss corridor={corridor} loss_pct={loss_pct} window_s=60", "vars": {"corridor": {"k": "ch", "v": ["us-central1<>us-west1", "us-central1<>europe-west1"]}, "loss_pct": {"k": "f", "v": [1.0, 12.0]}}},
            },
            "beh": {"n": {"emit": [{"id": "corridor_health", "per_min": 2.0, "scope": "global"}]}, "f": {"emit": [{"id": "corridor_health", "per_min": 4.0, "scope": "global"}, {"id": "loss_alarm", "per_min": 2.0, "scope": "global"}]}},
        },
        {
            "id": "cloud_router_cp",
            "svc": "cloud-router",
            "hosts": ["cr-cp-1"],
            "logs": {"route_update_delay": {"lvl": "INFO", "msg": "global routing update delay_ms={delay_ms} affected_peers={affected_peers}", "vars": {"affected_peers": {"k": "i", "v": [1, 30]}}, "state_vars": {"n": {"delay_ms": {"k": "i", "v": [100, 900]}}, "f": {"delay_ms": {"k": "i", "v": [400, 15000]}}}}},
            "beh": {"n": {"emit": [{"id": "route_update_delay", "per_min": 0.8, "scope": "global"}]}, "f": {"emit": [{"id": "route_update_delay", "per_min": 1.2, "scope": "global"}]}},
        },
        {
            "id": "connectivity_gateway",
            "svc": "connectivity-gw",
            "hosts": ["conn-uc1-1", "conn-uc1-2"],
            "logs": {
                "gw_stats": {"lvl": "INFO", "msg": "conn stats vpn_loss_pct={vpn_loss_pct} ic_loss_pct={ic_loss_pct}", "vars": {}, "state_vars": {"n": {"vpn_loss_pct": {"k": "f", "v": [0.0, 0.3]}, "ic_loss_pct": {"k": "f", "v": [0.0, 0.3]}}, "f": {"vpn_loss_pct": {"k": "f", "v": [0.2, 10.0]}, "ic_loss_pct": {"k": "f", "v": [0.2, 10.0]}}}},
                "vpn_keepalive_ok": {"lvl": "INFO", "msg": "vpn keepalive ok tunnel={tunnel} rtt_ms={rtt_ms}", "vars": {"tunnel": {"k": "ch", "v": ["tunnel-1", "tunnel-2", "tunnel-3"]}, "rtt_ms": {"k": "i", "v": [10, 300]}}},
                "vpn_keepalive_timeout": {"lvl": "WARN", "msg": "vpn keepalive timeout tunnel={tunnel} timeout_ms={timeout_ms}", "vars": {"tunnel": {"k": "ch", "v": ["tunnel-1", "tunnel-2", "tunnel-3"]}, "timeout_ms": {"k": "i", "v": [800, 6000]}}},
                "bfd_ok": {"lvl": "INFO", "msg": "interconnect bfd ok circuit={circuit} rtt_ms={rtt_ms}", "vars": {"circuit": {"k": "ch", "v": ["ic-1", "ic-2"]}, "rtt_ms": {"k": "i", "v": [1, 80]}}},
                "bfd_timeout": {"lvl": "WARN", "msg": "interconnect bfd timeout circuit={circuit} timeout_ms={timeout_ms}", "vars": {"circuit": {"k": "ch", "v": ["ic-1", "ic-2"]}, "timeout_ms": {"k": "i", "v": [200, 2000]}}},
            },
            "beh": {"n": {"emit": [{"id": "gw_stats", "per_min": 0.8, "scope": "per_host"}]}, "f": {"emit": [{"id": "gw_stats", "per_min": 1.2, "scope": "per_host"}]}},
        },
        {
            "id": "gcs_api_west",
            "svc": "gcs-api",
            "hosts": ["gcs-uw1-1", "gcs-uw1-2"],
            "logs": {
                "gcs_ok": {"lvl": "INFO", "msg": "gcs done op={op} bucket={bucket} status=200 dur_ms={dur_ms} req_id={req_id}", "vars": {"op": {"k": "ch", "v": ["list", "get", "put", "delete"]}, "bucket": {"k": "ch", "v": ["photos-prod", "logs-archive", "backups"]}, "dur_ms": {"k": "i", "v": [10, 9000]}, "req_id": {"k": "uuid", "v": None}}},
                "gcs_err": {"lvl": "ERROR", "msg": "gcs done op={op} bucket={bucket} status={status} err={err} dur_ms={dur_ms} req_id={req_id}", "vars": {"op": {"k": "ch", "v": ["list", "get", "put", "delete"]}, "bucket": {"k": "ch", "v": ["photos-prod", "logs-archive", "backups"]}, "status": {"k": "ch", "v": [500, 503]}, "err": {"k": "ch", "v": ["upstream_timeout", "connection_reset", "internal_error"]}, "dur_ms": {"k": "i", "v": [50, 20000]}, "req_id": {"k": "uuid", "v": None}}},
                "gcs_health": {"lvl": "INFO", "msg": "gcs health ok inflight={inflight}", "vars": {"inflight": {"k": "i", "v": [10, 2000]}}},
            },
            "beh": {"n": {"emit": [{"id": "gcs_health", "per_min": 0.4, "scope": "per_host"}]}, "f": {"emit": [{"id": "gcs_health", "per_min": 0.7, "scope": "per_host"}]}},
        },
        {
            "id": "cloud_sql_west",
            "svc": "cloudsql",
            "hosts": ["sql-uw1-1"],
            "logs": {
                "sql_ok": {"lvl": "INFO", "msg": "cloudsql done op={op} instance={instance} status=200 dur_ms={dur_ms} req_id={req_id}", "vars": {"op": {"k": "ch", "v": ["export", "update", "delete"]}, "instance": {"k": "ch", "v": ["orders-db", "billing-db", "users-db"]}, "dur_ms": {"k": "i", "v": [20, 15000]}, "req_id": {"k": "uuid", "v": None}}},
                "sql_err": {"lvl": "ERROR", "msg": "cloudsql done op={op} instance={instance} status={status} err={err} dur_ms={dur_ms} req_id={req_id}", "vars": {"op": {"k": "ch", "v": ["export", "update", "delete"]}, "instance": {"k": "ch", "v": ["orders-db", "billing-db", "users-db"]}, "status": {"k": "ch", "v": [500, 503]}, "err": {"k": "ch", "v": ["unavailable", "deadline_exceeded", "transport_error"]}, "dur_ms": {"k": "i", "v": [100, 25000]}, "req_id": {"k": "uuid", "v": None}}},
                "replication_lag_warn": {"lvl": "WARN", "msg": "replication lag region=us-west1 lag_s={lag_s}", "vars": {"lag_s": {"k": "i", "v": [5, 1800]}}},
                "sql_health": {"lvl": "INFO", "msg": "cloudsql health ok inflight={inflight}", "vars": {"inflight": {"k": "i", "v": [5, 800]}}},
            },
            "beh": {"n": {"emit": [{"id": "sql_health", "per_min": 0.6, "scope": "global"}]}, "f": {"emit": [{"id": "sql_health", "per_min": 0.8, "scope": "global"}, {"id": "replication_lag_warn", "per_min": 0.6, "scope": "global"}]}},
        },
        {
            "id": "messages_west",
            "svc": "messages",
            "hosts": ["msg-uw1-1"],
            "logs": {
                "msg_ok": {"lvl": "INFO", "msg": "messages publish topic={topic} status=200 dur_ms={dur_ms} req_id={req_id}", "vars": {"topic": {"k": "ch", "v": ["events", "alerts", "audit"]}, "dur_ms": {"k": "i", "v": [5, 8000]}, "req_id": {"k": "uuid", "v": None}}},
                "msg_err": {"lvl": "ERROR", "msg": "messages publish topic={topic} status={status} err={err} dur_ms={dur_ms} req_id={req_id}", "vars": {"topic": {"k": "ch", "v": ["events", "alerts", "audit"]}, "status": {"k": "ch", "v": [500, 503]}, "err": {"k": "ch", "v": ["upstream_timeout", "unavailable"]}, "dur_ms": {"k": "i", "v": [50, 20000]}, "req_id": {"k": "uuid", "v": None}}},
                "backlog_warn": {"lvl": "WARN", "msg": "messages backlog elevated backlog_msgs={backlog_msgs} ack_lag_s={ack_lag_s}", "vars": {"backlog_msgs": {"k": "i", "v": [1000, 500000]}, "ack_lag_s": {"k": "i", "v": [5, 900]}}},
                "msg_health": {"lvl": "INFO", "msg": "messages health ok inflight={inflight}", "vars": {"inflight": {"k": "i", "v": [10, 5000]}}},
            },
            "beh": {"n": {"emit": [{"id": "msg_health", "per_min": 0.8, "scope": "global"}]}, "f": {"emit": [{"id": "msg_health", "per_min": 1.0, "scope": "global"}, {"id": "backlog_warn", "per_min": 0.6, "scope": "global"}]}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {"id": "gcs_api_call_n", "rpm": 90.0, "emit": ["api_edge_uscentral.edge_req_gcs", "gcs_api_west.gcs_ok", "api_edge_uscentral.edge_resp_gcs_ok"], "latency_ms": [[0, 2], [25, 180], [2, 12]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "cloudsql_op_n", "rpm": 30.0, "emit": ["api_edge_uscentral.edge_req_sql", "cloud_sql_west.sql_ok", "api_edge_uscentral.edge_resp_sql_ok"], "latency_ms": [[0, 2], [40, 350], [3, 15]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "messages_publish_n", "rpm": 60.0, "emit": ["api_edge_uscentral.edge_req_msg", "messages_west.msg_ok", "api_edge_uscentral.edge_resp_msg_ok"], "latency_ms": [[0, 2], [10, 120], [2, 10]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "vpn_keepalive_n", "rpm": 40.0, "emit": ["connectivity_gateway.vpn_keepalive_ok"], "latency_ms": [[10, 300]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "interconnect_bfd_n", "rpm": 30.0, "emit": ["connectivity_gateway.bfd_ok"], "latency_ms": [[1, 80]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
            ]
        },
        "f": {
            "req": [
                {"id": "gcs_api_call_ok_f", "rpm": 80.0, "emit": ["api_edge_uscentral.edge_req_gcs", "gcs_api_west.gcs_ok", "api_edge_uscentral.edge_resp_gcs_ok"], "latency_ms": [[0, 2], [120, 1800], [5, 40]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "gcs_api_call_err_f", "rpm": 10.0, "emit": ["api_edge_uscentral.edge_req_gcs", "gcs_api_west.gcs_err", "api_edge_uscentral.edge_resp_gcs_err"], "latency_ms": [[0, 2], [600, 6000], [5, 60]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "cloudsql_op_ok_f", "rpm": 27.0, "emit": ["api_edge_uscentral.edge_req_sql", "cloud_sql_west.sql_ok", "api_edge_uscentral.edge_resp_sql_ok"], "latency_ms": [[0, 2], [200, 3000], [5, 60]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "cloudsql_op_err_f", "rpm": 3.0, "emit": ["api_edge_uscentral.edge_req_sql", "cloud_sql_west.sql_err", "api_edge_uscentral.edge_resp_sql_err"], "latency_ms": [[0, 2], [800, 12000], [5, 80]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "messages_publish_ok_f", "rpm": 58.0, "emit": ["api_edge_uscentral.edge_req_msg", "messages_west.msg_ok", "api_edge_uscentral.edge_resp_msg_ok"], "latency_ms": [[0, 2], [80, 1400], [5, 50]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "messages_publish_err_f", "rpm": 12.0, "emit": ["api_edge_uscentral.edge_req_msg", "messages_west.msg_err", "api_edge_uscentral.edge_resp_msg_err"], "latency_ms": [[0, 2], [400, 6000], [5, 80]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "vpn_keepalive_ok_f", "rpm": 34.0, "emit": ["connectivity_gateway.vpn_keepalive_ok"], "latency_ms": [[15, 450]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "vpn_keepalive_fail_f", "rpm": 6.0, "emit": ["connectivity_gateway.vpn_keepalive_timeout"], "latency_ms": [[800, 6000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "interconnect_bfd_ok_f", "rpm": 26.0, "emit": ["connectivity_gateway.bfd_ok"], "latency_ms": [[2, 120]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "interconnect_bfd_fail_f", "rpm": 4.0, "emit": ["connectivity_gateway.bfd_timeout"], "latency_ms": [[200, 2000]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "uscentral_gateway_corridor_packet_loss_may2022",
        "time": {"total_minutes": 45, "phases": {"n": {"start_min": 0, "end_min": 15}, "f": {"start_min": 15, "end_min": 45}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 15,
                        "rate_multipliers": {"messages_publish_err_f": 0.0, "cloudsql_op_err_f": 0.0, "gcs_api_call_err_f": 0.5, "messages_west.backlog_warn": 0.0, "cloud_sql_west.replication_lag_warn": 0.0},
                        "latency_multipliers": {"gcs_api_call_ok_f": {"p50": 1.1, "p95": 1.2}, "vpn_keepalive_ok_f": {"p50": 1.2, "p95": 1.3}},
                        "one_shots": [{"ref": "backbone_gateway_central.link_state_change", "count": 1, "hosts": ["gw-uc1-r1"]}, {"ref": "te_controller.reconv_start", "count": 1, "hosts": ["te-ctrl-1"]}],
                    },
                    {
                        "order": 2,
                        "at_min": 17,
                        "rate_multipliers": {"netmon.loss_alarm": 2.0, "vpn_keepalive_fail_f": 1.3, "interconnect_bfd_fail_f": 1.3, "messages_publish_err_f": 1.0, "messages_west.backlog_warn": 1.0, "te_controller.reconv_progress": 1.3},
                        "latency_multipliers": {"messages_publish_ok_f": {"p50": 1.2, "p95": 1.4}},
                        "one_shots": [{"ref": "te_controller.path_selected", "count": 1, "hosts": ["te-ctrl-1"]}],
                    },
                    {
                        "order": 3,
                        "at_min": 21,
                        "rate_multipliers": {"gcs_api_call_err_f": 1.6, "cloudsql_op_err_f": 1.0, "cloud_sql_west.replication_lag_warn": 1.0, "cloud_router_cp.route_update_delay": 1.6},
                        "latency_multipliers": {"gcs_api_call_ok_f": {"p50": 1.25, "p95": 1.45}, "cloudsql_op_ok_f": {"p50": 1.2, "p95": 1.5}},
                        "one_shots": [],
                    },
                    {
                        "order": 4,
                        "at_min": 33,
                        "rate_multipliers": {"netmon.loss_alarm": 0.2, "vpn_keepalive_fail_f": 0.0, "interconnect_bfd_fail_f": 0.0, "vpn_keepalive_ok_f": 1.2, "interconnect_bfd_ok_f": 1.15, "messages_publish_err_f": 0.0, "messages_west.backlog_warn": 0.2},
                        "latency_multipliers": {"messages_publish_ok_f": {"p50": 0.9, "p95": 1.0}},
                        "one_shots": [],
                    },
                    {
                        "order": 5,
                        "at_min": 35,
                        "rate_multipliers": {"netmon.loss_alarm": 0.0, "vpn_keepalive_fail_f": 0.0, "interconnect_bfd_fail_f": 0.0, "te_controller.reconv_progress": 0.0, "gcs_api_call_err_f": 0.8, "cloudsql_op_err_f": 0.8, "cloud_sql_west.replication_lag_warn": 1.0},
                        "latency_multipliers": {"gcs_api_call_ok_f": {"p50": 0.95, "p95": 1.1}, "cloudsql_op_ok_f": {"p50": 0.95, "p95": 1.2}},
                        "one_shots": [{"ref": "te_controller.reconv_complete", "count": 1, "hosts": ["te-ctrl-1"]}],
                    },
                ]
            }
        },
    }
}


PLACEHOLDER_RE = re.compile(r"{([a-zA-Z_][a-zA-Z0-9_]*)}")


def md5_bytes(s: str) -> bytes:
    return hashlib.md5(s.encode("utf-8")).digest()


def u01(key: str) -> float:
    b = md5_bytes(key)
    n = int.from_bytes(b, "big")
    return (n % (10**12)) / float(10**12)


def det_choice(key: str, values: List[Any]) -> Any:
    if not values:
        return None
    idx = int(u01(key) * len(values))
    if idx == len(values):
        idx -= 1
    return values[idx]


def det_int(key: str, lo: int, hi: int) -> int:
    if hi < lo:
        lo, hi = hi, lo
    if hi == lo:
        return lo
    return lo + int(u01(key) * (hi - lo + 1))


def det_float(key: str, lo: float, hi: float) -> float:
    if hi < lo:
        lo, hi = hi, lo
    return lo + u01(key) * (hi - lo)


def det_hex(key: str, n: int) -> str:
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    if n <= len(h):
        return h[:n]
    out = h
    k = 1
    while len(out) < n:
        out += hashlib.md5((key + f"|{k}").encode("utf-8")).hexdigest()
        k += 1
    return out[:n]


def det_uuid(key: str) -> str:
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    hex_list = list(h)
    hex_list[12] = "4"
    hex_list[16] = det_choice(key + "|variant", list("89ab"))
    h2 = "".join(hex_list)
    return f"{h2[0:8]}-{h2[8:12]}-{h2[12:16]}-{h2[16:20]}-{h2[20:32]}"


def det_ip_from_cidr(key: str, cidr: str) -> str:
    base, prefix = cidr.split("/")
    prefix = int(prefix)
    parts = base.split(".")
    if prefix != 24 or len(parts) != 4:
        return base
    a, b, c, _ = map(int, parts)
    host = 1 + int(u01(key) * 254)
    return f"{a}.{b}.{c}.{host}"


def stable_round_count(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    if frac <= 1e-12:
        return base
    return base + (1 if u01(key + "|round") < frac else 0)


def isoformat_ms(epoch_ms: int) -> str:
    dt = datetime.fromtimestamp(epoch_ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(dt.microsecond/1000):03d}Z"


def clamp_epoch_ms(t: float, lo: float, hi: float) -> int:
    if t < lo:
        t = lo
    if t >= hi:
        t = hi - 1
    return int(round(t))


def schedule_even_ms(start_ms: int, end_ms: int, count: int, key: str) -> List[int]:
    if count <= 0:
        return []
    dur = end_ms - start_ms
    if dur <= 0:
        return [start_ms] * count
    out = []
    for i in range(count):
        frac = (i + 0.5) / count
        t = start_ms + frac * dur
        spacing = dur / max(1, count)
        max_j = min(200.0, spacing * 0.2)
        uj = u01(f"{key}|jitter|{i}")
        jitter = (uj - 0.5) * 2.0 * max_j
        tt = clamp_epoch_ms(t + jitter, float(start_ms), float(end_ms))
        out.append(tt)
    out.sort()
    return out


def sample_skew_bounded_ms(key: str, lo: float, hi: float, skew: float = 1.7) -> int:
    if hi < lo:
        lo, hi = hi, lo
    if hi <= lo:
        return int(round(lo))
    u = u01(key)
    x = lo + (hi - lo) * (u ** skew)  # skew>1 biases toward lo
    return int(round(x))


@dataclass(frozen=True)
class LogTemplate:
    component_id: str
    log_id: str
    level: str
    msg: str
    vars: Dict[str, Any]
    state_vars: Dict[str, Any]


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, LogTemplate], Dict[str, Any], Dict[str, Any]]:
    comps = {c["id"]: c for c in system["components"]}
    templates: Dict[str, LogTemplate] = {}
    for cid, comp in comps.items():
        logs = comp.get("logs", {})
        for lid, spec in logs.items():
            templates[f"{cid}.{lid}"] = LogTemplate(
                component_id=cid,
                log_id=lid,
                level=spec["lvl"],
                msg=spec["msg"],
                vars=spec.get("vars", {}) or {},
                state_vars=spec.get("state_vars", {}) or {},
            )
    flows_by_state: Dict[str, Dict[str, Any]] = {"n": {}, "f": {}}
    for st in ["n", "f"]:
        for f in system["flows"][st]["req"]:
            flows_by_state[st][f["id"]] = f
    return comps, templates, flows_by_state, system.get("tracing", {})


def derive_failure_intervals(
    scenario: Dict[str, Any]
) -> Tuple[List[Tuple[int, int, Dict[str, float], Dict[str, float], Dict[str, Tuple[float, float]]]], List[Dict[str, Any]]]:
    s = scenario["scenario"]
    f_phase = s["time"]["phases"]["f"]
    f_start = int(f_phase["start_min"])
    f_end = int(f_phase["end_min"])
    events = list(s["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e.get("order", 0)))

    active_rate_mult_flow: Dict[str, float] = {}
    active_rate_mult_bg: Dict[str, float] = {}
    active_lat_mult: Dict[str, Tuple[float, float]] = {}

    boundaries = [f_start] + sorted({int(e["at_min"]) for e in events if f_start <= int(e["at_min"]) < f_end}) + [f_end]
    boundaries = sorted(dict.fromkeys(boundaries))

    updates_at: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        updates_at.setdefault(int(e["at_min"]), []).append(e)

    intervals = []
    for i in range(len(boundaries) - 1):
        b0 = boundaries[i]
        b1 = boundaries[i + 1]
        if b0 in updates_at:
            for e in updates_at[b0]:
                for k, v in (e.get("rate_multipliers") or {}).items():
                    if "." in k:
                        active_rate_mult_bg[k] = float(v)
                    else:
                        active_rate_mult_flow[k] = float(v)
                for fid, m in (e.get("latency_multipliers") or {}).items():
                    active_lat_mult[fid] = (float(m["p50"]), float(m["p95"]))
        intervals.append((b0, b1, dict(active_rate_mult_flow), dict(active_rate_mult_bg), dict(active_lat_mult)))
    return intervals, events


def merge_vars_for_state(tmpl: LogTemplate, state: str) -> Dict[str, Any]:
    out = dict(tmpl.vars or {})
    sv = tmpl.state_vars.get(state) or {}
    out.update(sv)
    return out


def get_int_domain(vars_spec: Dict[str, Any], field: str) -> Optional[Tuple[int, int]]:
    spec = vars_spec.get(field)
    if not spec:
        return None
    if spec.get("k") != "i":
        return None
    v = spec.get("v")
    if not v or len(v) != 2:
        return None
    return int(v[0]), int(v[1])


def render_message(tmpl: LogTemplate, state: str, key: str, ctx: Dict[str, Any], overrides: Dict[str, Any]) -> str:
    vars_spec = merge_vars_for_state(tmpl, state)
    placeholders = PLACEHOLDER_RE.findall(tmpl.msg)
    values: Dict[str, Any] = {}

    for name in placeholders:
        if name in overrides:
            values[name] = overrides[name]
            continue
        if name in ctx:
            values[name] = ctx[name]
            continue
        spec = vars_spec.get(name)
        if spec is None:
            values[name] = ""
            continue
        k = spec["k"]
        v = spec.get("v")
        if k == "i":
            lo, hi = int(v[0]), int(v[1])
            values[name] = det_int(f"{key}|{name}", lo, hi)
        elif k == "f":
            lo, hi = float(v[0]), float(v[1])
            values[name] = round(det_float(f"{key}|{name}", lo, hi), 3)
        elif k == "ch":
            values[name] = det_choice(f"{key}|{name}", list(v))
        elif k == "uuid":
            values[name] = det_uuid(f"{key}|{name}")
        elif k == "hex":
            values[name] = det_hex(f"{key}|{name}", int(v))
        elif k == "ip":
            values[name] = det_ip_from_cidr(f"{key}|{name}", str(v))
        elif k == "str":
            values[name] = f"{str(v)[:12]}-{det_hex(f'{key}|{name}', 6)}"
        else:
            values[name] = ""

        # Preserve meaning-bearing context, but don't latch timing-like fields.
        if name not in {"dur_ms", "rtt_ms", "timeout_ms", "delay_ms", "duration_s", "lag_s", "ack_lag_s", "backlog_msgs"}:
            ctx[name] = values[name]

    return tmpl.msg.format(**values)


def emit_row(rows: List[Dict[str, Any]], epoch_ms: int, tmpl: LogTemplate, message: str, trace_id: str, comps: Dict[str, Any], host: str, seq: int) -> None:
    comp = comps[tmpl.component_id]
    rows.append(
        {
            "_t": int(epoch_ms),
            "_seq": int(seq),
            "timestamp": "",
            "level": tmpl.level,
            "message": message,
            "trace_id": trace_id,
            "service": comp.get("svc") or "",
            "host": host or "",
        }
    )


def choose_host_for_component(comp: Dict[str, Any], key: str) -> str:
    hosts = comp.get("hosts") or []
    if not hosts:
        return ""
    return det_choice(key, hosts)


def weighted_corridor_choice(key: str, options: List[str], primary: str, primary_weight: float) -> str:
    if primary not in options or len(options) == 1:
        return det_choice(key, options)
    u = u01(key + "|w")
    if u < primary_weight:
        return primary
    others = [x for x in options if x != primary]
    return det_choice(key + "|other", others)


def apply_special_overrides(tmpl_ref: str) -> Dict[str, Any]:
    if tmpl_ref == "te_controller.reconv_start":
        return {"corridor": "us-central1<>us-west1", "trigger": "link_down"}
    return {}


def simulate_background(
    rows: List[Dict[str, Any]],
    comps: Dict[str, Any],
    templates: Dict[str, LogTemplate],
    state: str,
    start_min: int,
    end_min: int,
    base_epoch_ms: int,
    rate_mult_bg: Optional[Dict[str, float]] = None,
    seq_start: int = 0,
) -> int:
    seq = seq_start
    rate_mult_bg = rate_mult_bg or {}
    start_ms = base_epoch_ms + start_min * 60_000
    end_ms = base_epoch_ms + end_min * 60_000

    for cid in sorted(comps.keys()):
        comp = comps[cid]
        beh = comp.get("beh", {}).get(state)
        if not beh:
            continue
        for emit_spec in beh.get("emit") or []:
            log_id = emit_spec["id"]
            per_min = float(emit_spec["per_min"])
            scope = emit_spec.get("scope", "per_host") or "per_host"
            tmpl_ref = f"{cid}.{log_id}"
            tmpl = templates[tmpl_ref]

            mult = 1.0
            if state == "f":
                mult = float(rate_mult_bg.get(tmpl_ref, 1.0))
            eff_rate = per_min * mult
            if eff_rate <= 0:
                continue

            duration_min = (end_min - start_min)
            expected = eff_rate * duration_min

            if scope == "per_host":
                hosts = comp.get("hosts") or [""]
                for h in hosts:
                    c = stable_round_count(expected, f"bg|{state}|{tmpl_ref}|{h}|{start_min}-{end_min}")
                    times = schedule_even_ms(start_ms, end_ms, c, f"bg|{state}|{tmpl_ref}|{h}|{start_min}-{end_min}")
                    for idx, t in enumerate(times):
                        ctx: Dict[str, Any] = {}
                        overrides: Dict[str, Any] = {}

                        if tmpl_ref == "netmon.corridor_health":
                            opts = merge_vars_for_state(tmpl, state)["corridor"]["v"]
                            overrides["corridor"] = weighted_corridor_choice(f"{tmpl_ref}|{t}|corr", opts, "us-central1<>us-west1", 0.8)
                        if tmpl_ref == "netmon.loss_alarm":
                            opts = tmpl.vars["corridor"]["v"]
                            overrides["corridor"] = weighted_corridor_choice(f"{tmpl_ref}|{t}|corr", opts, "us-central1<>us-west1", 0.9)
                        if tmpl_ref == "te_controller.ctrl_tick":
                            opts = tmpl.vars["corridor"]["v"]
                            overrides["corridor"] = weighted_corridor_choice(f"{tmpl_ref}|{t}|corr", opts, "us-central1<>us-west1", 0.7)

                        msg = render_message(tmpl, state, f"bg|{tmpl_ref}|{h}|{start_min}-{end_min}|{idx}|{t}", ctx, overrides)
                        emit_row(rows, t, tmpl, msg, "", comps, h, seq)
                        seq += 1
            elif scope == "global":
                c = stable_round_count(expected, f"bg|{state}|{tmpl_ref}|global|{start_min}-{end_min}")
                times = schedule_even_ms(start_ms, end_ms, c, f"bg|{state}|{tmpl_ref}|global|{start_min}-{end_min}")
                for idx, t in enumerate(times):
                    host = choose_host_for_component(comp, f"bg_host|{tmpl_ref}|{start_min}-{end_min}|{idx}")
                    ctx = {}
                    overrides: Dict[str, Any] = {}

                    if tmpl_ref == "netmon.corridor_health":
                        opts = merge_vars_for_state(tmpl, state)["corridor"]["v"]
                        overrides["corridor"] = weighted_corridor_choice(f"{tmpl_ref}|{t}|corr", opts, "us-central1<>us-west1", 0.8)
                    if tmpl_ref == "netmon.loss_alarm":
                        opts = tmpl.vars["corridor"]["v"]
                        overrides["corridor"] = weighted_corridor_choice(f"{tmpl_ref}|{t}|corr", opts, "us-central1<>us-west1", 0.9)
                    if tmpl_ref == "te_controller.ctrl_tick":
                        opts = tmpl.vars["corridor"]["v"]
                        overrides["corridor"] = weighted_corridor_choice(f"{tmpl_ref}|{t}|corr", opts, "us-central1<>us-west1", 0.7)

                    msg = render_message(tmpl, state, f"bg|{tmpl_ref}|global|{start_min}-{end_min}|{idx}|{t}", ctx, overrides)
                    emit_row(rows, t, tmpl, msg, "", comps, host, seq)
                    seq += 1
            else:
                c = stable_round_count(expected, f"bg|{state}|{tmpl_ref}|{scope}|{start_min}-{end_min}")
                times = schedule_even_ms(start_ms, end_ms, c, f"bg|{state}|{tmpl_ref}|{scope}|{start_min}-{end_min}")
                for idx, t in enumerate(times):
                    host = choose_host_for_component(comp, f"bg_host|{tmpl_ref}|{start_min}-{end_min}|{idx}")
                    ctx = {}
                    msg = render_message(tmpl, state, f"bg|{tmpl_ref}|{scope}|{start_min}-{end_min}|{idx}|{t}", ctx, {})
                    emit_row(rows, t, tmpl, msg, "", comps, host, seq)
                    seq += 1

    return seq


def compute_attempts(expected: float, max_attempts: int, key: str) -> int:
    if max_attempts <= 1:
        return 1
    e = max(1.0, min(float(max_attempts), float(expected)))
    lo = int(math.floor(e))
    hi = int(math.ceil(e))
    if lo == hi:
        return lo
    frac = e - lo
    return hi if u01(key + "|attempts") < frac else lo


def bind_chain_outcome_ctx(flow_id: str, emit_refs: List[str], key: str) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {}
    ctx["req_id"] = det_uuid(key + "|req_id")
    ctx["client_ip"] = det_ip_from_cidr(key + "|client_ip", "198.51.100.0/24")

    joined = "|".join(emit_refs)
    if "edge_req_gcs" in joined:
        ctx["op"] = det_choice(key + "|op", ["list", "get", "put", "delete"])
        ctx["bucket"] = det_choice(key + "|bucket", ["photos-prod", "logs-archive", "backups"])
    if "edge_req_sql" in joined:
        ctx["op"] = det_choice(key + "|op", ["export", "update", "delete"])
        ctx["instance"] = det_choice(key + "|instance", ["orders-db", "billing-db", "users-db"])
    if "edge_req_msg" in joined or "messages_west.msg_" in joined:
        ctx["topic"] = det_choice(key + "|topic", ["events", "alerts", "audit"])

    if flow_id in {"gcs_api_call_err_f"}:
        ctx["status"] = det_choice(key + "|status", [500, 503])
        ctx["err"] = det_choice(key + "|err", ["upstream_timeout", "connection_reset", "internal_error"])
    if flow_id in {"messages_publish_err_f"}:
        ctx["status"] = det_choice(key + "|status", [500, 503])
        ctx["err"] = det_choice(key + "|err", ["upstream_timeout", "unavailable"])
    if flow_id in {"cloudsql_op_err_f"}:
        ctx["status"] = det_choice(key + "|status", [500, 503])
        ctx["err"] = det_choice(key + "|err", ["unavailable", "deadline_exceeded"])

    return ctx


def simulate_flow_instances(
    rows: List[Dict[str, Any]],
    comps: Dict[str, Any],
    templates: Dict[str, LogTemplate],
    flow_def: Dict[str, Any],
    state: str,
    start_times_ms: List[int],
    latency_mult: Optional[Tuple[float, float]],
    seq_start: int = 0,
) -> int:
    seq = seq_start
    flow_id = flow_def["id"]
    emit_refs = flow_def["emit"]
    lat_pairs = flow_def["latency_ms"]
    retry = flow_def.get("retry", {}) or {}
    max_attempts = int(retry.get("max_attempts", 1))
    expected_attempts = float(retry.get("expected_attempts", 1.0))
    trace_on = bool(flow_def.get("trace", False)) and bool(SYSTEM.get("tracing", {}).get("on", False))

    lat_mult = latency_mult or (1.0, 1.0)

    for inst_idx, start_ms in enumerate(start_times_ms):
        inst_key = f"flow|{state}|{flow_id}|{start_ms}|{inst_idx}"
        trace_id = det_hex(inst_key + "|trace", 32) if trace_on else ""
        attempts = compute_attempts(expected_attempts, max_attempts, inst_key)

        host_for_comp: Dict[str, str] = {}
        for ref in emit_refs + (retry.get("emit_per_retry") or []):
            cid, _ = ref.split(".", 1)
            if cid not in host_for_comp:
                host_for_comp[cid] = choose_host_for_component(comps[cid], inst_key + f"|host|{cid}")

        chain_ctx = bind_chain_outcome_ctx(flow_id, emit_refs, inst_key)

        t_ms = int(start_ms)
        for attempt in range(1, attempts + 1):
            attempt_key = f"{inst_key}|attempt|{attempt}"
            ctx = dict(chain_ctx)

            prev_ms = int(start_ms) if attempt == 1 else int(t_ms)
            last_elapsed = 0

            for j, ref in enumerate(emit_refs):
                tmpl = templates[ref]
                cid, _ = ref.split(".", 1)
                vars_spec = merge_vars_for_state(tmpl, state)

                lo, hi = float(lat_pairs[j][0]), float(lat_pairs[j][1])
                if state == "f":
                    lo *= float(lat_mult[0])
                    hi *= float(lat_mult[1])

                delay_ms = sample_skew_bounded_ms(f"{attempt_key}|lat|{j}", lo, hi, skew=1.7)
                prev_ms = prev_ms + int(delay_ms)

                elapsed_ms = int(prev_ms - int(start_ms))
                timing_field = None
                timing_domain = None
                for field in ("timeout_ms", "rtt_ms", "dur_ms", "delay_ms"):
                    dom = get_int_domain(vars_spec, field)
                    if dom is not None:
                        timing_field = field
                        timing_domain = dom
                        break

                if timing_field is not None and timing_domain is not None:
                    lo_d, hi_d = timing_domain
                    lo_eff = max(int(lo_d), int(last_elapsed))
                    hi_eff = int(hi_d)
                    if hi_eff < lo_eff:
                        clamp_elapsed = lo_eff
                    else:
                        clamp_elapsed = min(max(elapsed_ms, lo_eff), hi_eff)
                    prev_ms = int(start_ms) + int(clamp_elapsed)
                    elapsed_ms = int(clamp_elapsed)

                last_elapsed = max(last_elapsed, elapsed_ms)

                overrides: Dict[str, Any] = {}
                if "dur_ms" in vars_spec:
                    overrides["dur_ms"] = int(elapsed_ms)
                if "rtt_ms" in vars_spec:
                    overrides["rtt_ms"] = int(elapsed_ms)
                if "timeout_ms" in vars_spec:
                    overrides["timeout_ms"] = int(elapsed_ms)
                if "delay_ms" in vars_spec:
                    overrides["delay_ms"] = int(elapsed_ms)

                msg = render_message(tmpl, state, f"{attempt_key}|emit|{j}|{ref}|{prev_ms}", ctx, overrides)
                emit_row(rows, prev_ms, tmpl, msg, trace_id, comps, host_for_comp.get(cid, ""), seq)
                seq += 1

            t_ms = int(prev_ms)
            if attempt < attempts:
                back_pairs = retry.get("backoff_ms") or []
                if back_pairs:
                    p50, p95 = back_pairs[min(attempt - 1, len(back_pairs) - 1)]
                    backoff = sample_skew_bounded_ms(f"{attempt_key}|backoff", float(p50), float(p95), skew=1.3)
                else:
                    backoff = 100
                t_ms = t_ms + int(backoff)

    return seq


def simulate_one_shots(
    rows: List[Dict[str, Any]],
    comps: Dict[str, Any],
    templates: Dict[str, LogTemplate],
    base_epoch_ms: int,
    events: List[Dict[str, Any]],
    seq_start: int = 0,
) -> int:
    seq = seq_start
    # Track emitted reconvergence start time so reconv_complete duration_s can be coherent.
    emitted_times: Dict[str, int] = {}

    for e in sorted(events, key=lambda x: (x["at_min"], x.get("order", 0))):
        at_min = int(e["at_min"])
        t0 = base_epoch_ms + at_min * 60_000
        for shot_idx, shot in enumerate(e.get("one_shots") or []):
            ref = shot["ref"]
            count = int(shot["count"])
            allowed_hosts = shot.get("hosts") or []
            tmpl = templates[ref]
            for i in range(count):
                uj = u01(f"oneshot|{ref}|{at_min}|{shot_idx}|{i}")
                t = t0 + int(round(uj * 400.0))
                comp = comps[tmpl.component_id]
                if allowed_hosts:
                    host = det_choice(f"oneshot_host|{ref}|{at_min}|{i}", allowed_hosts)
                else:
                    host = choose_host_for_component(comp, f"oneshot_host|{ref}|{at_min}|{i}")

                ctx: Dict[str, Any] = {}
                overrides = apply_special_overrides(ref)

                # Cohere reconv_complete.duration_s with the actual modeled reconvergence window.
                if ref == "te_controller.reconv_complete":
                    start_t = emitted_times.get("te_controller.reconv_start")
                    if start_t is not None:
                        duration_s = int(round((t - start_t) / 1000.0))
                        overrides = dict(overrides)
                        overrides["duration_s"] = duration_s
                        # Ensure corridor is stable/deterministic for this marker as in scenario.
                        overrides.setdefault("corridor", "us-central1<>us-west1")

                msg = render_message(tmpl, "f", f"oneshot|{ref}|{at_min}|{i}|{t}", ctx, overrides)
                emit_row(rows, t, tmpl, msg, "", comps, host, seq)
                seq += 1

                if ref == "te_controller.reconv_start":
                    emitted_times["te_controller.reconv_start"] = t

    return seq


def main() -> None:
    random.seed(0)
    np.random.seed(0)

    comps, templates, flows_by_state, _tracing = build_indices(SYSTEM)
    base_time = datetime(2022, 5, 1, 0, 0, 0, tzinfo=timezone.utc)
    base_epoch_ms = int(base_time.timestamp() * 1000)

    n_start = SCENARIO["scenario"]["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"]
    f_start = SCENARIO["scenario"]["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["scenario"]["time"]["phases"]["f"]["end_min"]

    rows: List[Dict[str, Any]] = []
    seq = 0

    # Normal background
    seq = simulate_background(rows, comps, templates, "n", n_start, n_end, base_epoch_ms, rate_mult_bg=None, seq_start=seq)

    # Normal flows
    start_ms_n0 = base_epoch_ms + int(n_start) * 60_000
    end_ms_n0 = base_epoch_ms + int(n_end) * 60_000
    for flow_id in sorted(flows_by_state["n"].keys()):
        fdef = flows_by_state["n"][flow_id]
        expected = float(fdef["rpm"]) * (n_end - n_start)
        count = stable_round_count(expected, f"flow|n|{flow_id}|{n_start}-{n_end}")
        starts = schedule_even_ms(start_ms_n0, end_ms_n0, count, f"flow|n|{flow_id}|{n_start}-{n_end}")
        seq = simulate_flow_instances(rows, comps, templates, fdef, "n", starts, latency_mult=(1.0, 1.0), seq_start=seq)

    # Failure intervals and one-shots
    failure_intervals, events = derive_failure_intervals(SCENARIO)
    seq = simulate_one_shots(rows, comps, templates, base_epoch_ms, events, seq_start=seq)

    # Failure background + flows per interval with persistent controls
    for (b0, b1, rate_mult_flow, rate_mult_bg, lat_mults) in failure_intervals:
        seq = simulate_background(rows, comps, templates, "f", b0, b1, base_epoch_ms, rate_mult_bg=rate_mult_bg, seq_start=seq)

        start_ms = base_epoch_ms + b0 * 60_000
        end_ms = base_epoch_ms + b1 * 60_000
        dur_min = b1 - b0
        for flow_id in sorted(flows_by_state["f"].keys()):
            fdef = flows_by_state["f"][flow_id]
            mult = float(rate_mult_flow.get(flow_id, 1.0))
            rpm_eff = float(fdef["rpm"]) * mult
            expected = rpm_eff * dur_min
            count = stable_round_count(expected, f"flow|f|{flow_id}|{b0}-{b1}|mult={mult}")
            starts = schedule_even_ms(start_ms, end_ms, count, f"flow|f|{flow_id}|{b0}-{b1}")
            lm = lat_mults.get(flow_id, (1.0, 1.0))
            seq = simulate_flow_instances(rows, comps, templates, fdef, "f", starts, latency_mult=lm, seq_start=seq)

    df = pd.DataFrame(rows)
    df.sort_values(by=["_t", "_seq"], inplace=True, kind="mergesort")
    df["timestamp"] = df["_t"].apply(isoformat_ms)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    if not df["timestamp"].is_monotonic_increasing:
        df = df.sort_values(by=["timestamp"], kind="mergesort").reset_index(drop=True)

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
