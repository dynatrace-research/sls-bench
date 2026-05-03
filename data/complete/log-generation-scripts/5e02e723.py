import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


SYSTEM: Dict[str, Any] = {
    "sys": {"id": "monzo_cassandra_scaleup_404s"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["app_api", "internal_edge"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "app_api",
            "svc": "app-api",
            "hosts": ["api-1", "api-2"],
            "logs": {
                "http_in": {
                    "lvl": "INFO",
                    "msg": "inbound {method} {endpoint} user={user_id} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "endpoint": {"k": "ch", "v": ["/login", "/balance"]},
                        "user_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_out_200": {
                    "lvl": "INFO",
                    "msg": "response 200 {endpoint} user={user_id} dur_ms={dur_ms}",
                    "vars": {
                        "endpoint": {"k": "ch", "v": ["/login", "/balance"]},
                        "user_id": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [10, 180]},
                    },
                },
                "http_out_200_stale": {
                    "lvl": "WARN",
                    "msg": "response 200 {endpoint} user={user_id} stale=true stale_age_s={stale_age_s} dur_ms={dur_ms}",
                    "vars": {
                        "endpoint": {"k": "ch", "v": ["/balance"]},
                        "user_id": {"k": "uuid", "v": None},
                        "stale_age_s": {"k": "i", "v": [30, 900]},
                        "dur_ms": {"k": "i", "v": [20, 300]},
                    },
                },
                "http_out_503": {
                    "lvl": "ERROR",
                    "msg": "response 503 {endpoint} user={user_id} err={err} dur_ms={dur_ms}",
                    "vars": {
                        "endpoint": {"k": "ch", "v": ["/login"]},
                        "user_id": {"k": "uuid", "v": None},
                        "err": {"k": "ch", "v": ["backend_data_unavailable", "session_missing"]},
                        "dur_ms": {"k": "i", "v": [20, 350]},
                    },
                },
                "api_metrics": {
                    "lvl": "INFO",
                    "msg": "api_metrics req_rpm={req_rpm} http_5xx_rpm={http_5xx_rpm} p95_ms={p95_ms}",
                    "vars": {
                        "req_rpm": {"k": "i", "v": [200, 450]},
                        "http_5xx_rpm": {"k": "i", "v": [0, 25]},
                        "p95_ms": {"k": "i", "v": [25, 220]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "api_metrics", "per_min": 1.0}]},
                "f": {"emit": [{"id": "api_metrics", "per_min": 1.0}]},
            },
        },
        {
            "id": "account_service",
            "svc": "account-service",
            "hosts": ["acct-1", "acct-2"],
            "logs": {
                "session_read_ok": {
                    "lvl": "INFO",
                    "msg": "session_read user={user_id} replica={replica} cl=QUORUM result=HIT latency_ms={latency_ms}",
                    "vars": {
                        "user_id": {"k": "uuid", "v": None},
                        "replica": {"k": "ch", "v": ["cass-a1", "cass-a2", "cass-a3"]},
                        "latency_ms": {"k": "i", "v": [2, 40]},
                    },
                },
                "session_read_empty": {
                    "lvl": "WARN",
                    "msg": "session_read user={user_id} replica={replica} cl=QUORUM result=MISS latency_ms={latency_ms}",
                    "vars": {
                        "user_id": {"k": "uuid", "v": None},
                        "replica": {"k": "ch", "v": ["cass-n1", "cass-n2", "cass-n3", "cass-n4", "cass-n5", "cass-n6"]},
                        "latency_ms": {"k": "i", "v": [2, 50]},
                    },
                },
                "balance_read_ok": {
                    "lvl": "INFO",
                    "msg": "balance_read user={user_id} replica={replica} cl=QUORUM result=HIT latency_ms={latency_ms}",
                    "vars": {
                        "user_id": {"k": "uuid", "v": None},
                        "replica": {"k": "ch", "v": ["cass-a1", "cass-a2", "cass-a3"]},
                        "latency_ms": {"k": "i", "v": [4, 60]},
                    },
                },
                "balance_read_partial": {
                    "lvl": "WARN",
                    "msg": "balance_read user={user_id} replica={replica} cl=QUORUM result=PARTIAL missing_rows={missing_rows} fallback_cache=true latency_ms={latency_ms}",
                    "vars": {
                        "user_id": {"k": "uuid", "v": None},
                        "replica": {"k": "ch", "v": ["cass-n1", "cass-n2", "cass-n3", "cass-n4", "cass-n5", "cass-n6"]},
                        "missing_rows": {"k": "i", "v": [1, 12]},
                        "latency_ms": {"k": "i", "v": [8, 120]},
                    },
                },
                "svc_metrics": {
                    "lvl": "INFO",
                    "msg": "acct_metrics rpc_rpm={rpc_rpm} cass_timeouts_rpm={cass_timeouts_rpm} p95_ms={p95_ms}",
                    "vars": {
                        "rpc_rpm": {"k": "i", "v": [220, 360]},
                        "cass_timeouts_rpm": {"k": "i", "v": [0, 5]},
                        "p95_ms": {"k": "i", "v": [15, 140]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "svc_metrics", "per_min": 1.0}]},
                "f": {"emit": [{"id": "svc_metrics", "per_min": 1.0}]},
            },
        },
        {
            "id": "internal_edge",
            "svc": "internal-edge",
            "hosts": ["edge-1", "edge-2"],
            "logs": {
                "req_in": {
                    "lvl": "INFO",
                    "msg": "inbound {method} {route} tool={tool} src_ip={src_ip} employee={employee_id} trace={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "route": {"k": "ch", "v": ["/cops/chat", "/deploy/run"]},
                        "tool": {"k": "ch", "v": ["cops_console", "deploy_tool"]},
                        "src_ip": {"k": "ip", "v": "10.0.0.0/8"},
                        "employee_id": {"k": "hex", "v": 8},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "route_ok": {
                    "lvl": "INFO",
                    "msg": "route {route} upstream={upstream} result=OK",
                    "vars": {
                        "route": {"k": "ch", "v": ["/cops/chat", "/deploy/run"]},
                        "upstream": {"k": "ch", "v": ["cops-backend", "deploy-orchestrator"]},
                    },
                },
                "route_404": {
                    "lvl": "WARN",
                    "msg": "route {route} result=404 reason=internal_config_missing cfg_key={cfg_key}",
                    "vars": {"route": {"k": "ch", "v": ["/cops/chat", "/deploy/run"]}, "cfg_key": {"k": "ch", "v": ["private_network_cidr"]}},
                },
                "edge_metrics": {
                    "lvl": "INFO",
                    "msg": "edge_metrics req_rpm={req_rpm} http_4xx_rpm={http_4xx_rpm} p95_ms={p95_ms} view=baseline",
                    "vars": {"req_rpm": {"k": "i", "v": [20, 120]}, "http_4xx_rpm": {"k": "i", "v": [0, 3]}, "p95_ms": {"k": "i", "v": [5, 80]}},
                },
                "edge_metrics_spike": {
                    "lvl": "INFO",
                    "msg": "edge_metrics req_rpm={req_rpm} http_4xx_rpm={http_4xx_rpm} p95_ms={p95_ms} view=incident",
                    "vars": {"req_rpm": {"k": "i", "v": [20, 120]}, "http_4xx_rpm": {"k": "i", "v": [15, 110]}, "p95_ms": {"k": "i", "v": [5, 90]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "edge_metrics", "per_min": 1.0}, {"id": "edge_metrics_spike", "per_min": 0.0}]},
                "f": {"emit": [{"id": "edge_metrics", "per_min": 1.0}, {"id": "edge_metrics_spike", "per_min": 1.0}]},
            },
        },
        {
            "id": "config_service",
            "svc": "config-service",
            "hosts": ["cfg-1", "cfg-2"],
            "logs": {
                "get_ok": {
                    "lvl": "INFO",
                    "msg": "get key={key} result=OK value_hash={value_hash} replica={replica} latency_ms={latency_ms}",
                    "vars": {
                        "key": {"k": "ch", "v": ["private_network_cidr", "employee_allowlist", "deploy_fallback_enabled"]},
                        "value_hash": {"k": "hex", "v": 12},
                        "replica": {"k": "ch", "v": ["cass-a1", "cass-a2", "cass-a3"]},
                        "latency_ms": {"k": "i", "v": [2, 40]},
                    },
                },
                "get_not_found_404": {
                    "lvl": "WARN",
                    "msg": "get key={key} result=NOT_FOUND status=404 replica={replica} latency_ms={latency_ms}",
                    "vars": {
                        "key": {"k": "ch", "v": ["private_network_cidr"]},
                        "replica": {"k": "ch", "v": ["cass-n1", "cass-n2", "cass-n3", "cass-n4", "cass-n5", "cass-n6"]},
                        "latency_ms": {"k": "i", "v": [2, 45]},
                    },
                },
                "rpc_metrics": {
                    "lvl": "INFO",
                    "msg": "cfg_rpc_metrics ok_rpm={ok_rpm} err_5xx_rpm={err_5xx_rpm} p95_ms={p95_ms}",
                    "vars": {"ok_rpm": {"k": "i", "v": [60, 140]}, "err_5xx_rpm": {"k": "i", "v": [0, 3]}, "p95_ms": {"k": "i", "v": [5, 40]}},
                },
            },
            "beh": {"n": {"emit": [{"id": "rpc_metrics", "per_min": 1.0}]}, "f": {"emit": [{"id": "rpc_metrics", "per_min": 1.0}]}},
        },
        {
            "id": "cassandra_cluster",
            "svc": "cassandra",
            "hosts": ["cass-a1", "cass-a2", "cass-a3", "cass-n1", "cass-n2", "cass-n3", "cass-n4", "cass-n5", "cass-n6"],
            "logs": {
                "scale_up_start": {
                    "lvl": "INFO",
                    "msg": "scale_up start add_nodes={add_nodes} auto_bootstrap={auto_bootstrap}",
                    "vars": {"add_nodes": {"k": "i", "v": [6, 6]}, "auto_bootstrap": {"k": "ch", "v": [False]}},
                },
                "node_joined": {
                    "lvl": "INFO",
                    "msg": "node_joined node={node} join_state={join_state}",
                    "vars": {"node": {"k": "ch", "v": ["cass-n1", "cass-n2", "cass-n3", "cass-n4", "cass-n5", "cass-n6"]}, "join_state": {"k": "ch", "v": ["JOINED"]}},
                },
                "ownership_changed": {
                    "lvl": "WARN",
                    "msg": "ring token_ownership_changed new_nodes_active={new_nodes_active}",
                    "vars": {"new_nodes_active": {"k": "i", "v": [3, 6]}},
                },
                "decommission_start": {
                    "lvl": "WARN",
                    "msg": "decommission start node={node}",
                    "vars": {"node": {"k": "ch", "v": ["cass-n1", "cass-n2", "cass-n3", "cass-n4", "cass-n5", "cass-n6"]}},
                },
                "decommission_complete": {
                    "lvl": "INFO",
                    "msg": "decommission complete node={node} duration_min={duration_min}",
                    "vars": {"node": {"k": "ch", "v": ["cass-n1", "cass-n2", "cass-n3", "cass-n4", "cass-n5", "cass-n6"]}, "duration_min": {"k": "i", "v": [8, 10]}},
                },
                "metrics": {
                    "lvl": "INFO",
                    "msg": "cass_metrics read_rps={read_rps} write_rps={write_rps} timeouts_rpm={timeouts_rpm} p95_ms={p95_ms}",
                    "vars": {"read_rps": {"k": "i", "v": [300, 700]}, "write_rps": {"k": "i", "v": [120, 320]}, "timeouts_rpm": {"k": "i", "v": [0, 5]}, "p95_ms": {"k": "i", "v": [5, 35]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "metrics", "per_min": 0.2, "scope": "per_host"}]},
                "f": {"emit": [{"id": "metrics", "per_min": 0.2, "scope": "per_host"}]},
            },
        },
        {
            "id": "mastercard_service",
            "svc": "card-auth-service",
            "hosts": ["mc-1", "mc-2"],
            "logs": {
                "auth_req": {
                    "lvl": "INFO",
                    "msg": "card_auth start id={auth_id} amount_pence={amount_pence} mcc={mcc}",
                    "vars": {"auth_id": {"k": "uuid", "v": None}, "amount_pence": {"k": "i", "v": [100, 50000]}, "mcc": {"k": "ch", "v": [5411, 5812, 5912, 5999]}},
                },
                "auth_ok": {"lvl": "INFO", "msg": "card_auth approved id={auth_id} dur_ms={dur_ms}", "vars": {"auth_id": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [40, 900]}}},
                "auth_fail_bug": {
                    "lvl": "ERROR",
                    "msg": "card_auth failed id={auth_id} err={err} path=unhandled_error_case",
                    "vars": {"auth_id": {"k": "uuid", "v": None}, "err": {"k": "ch", "v": ["upstream_timeout", "upstream_5xx", "parse_error"]}},
                },
                "deploy_version": {"lvl": "INFO", "msg": "deployed version={version} via={via}", "vars": {"version": {"k": "ch", "v": ["2019.07.29.2"]}, "via": {"k": "ch", "v": ["fallback_mechanism"]}}},
                "mc_metrics": {"lvl": "INFO", "msg": "card_metrics auth_rpm={auth_rpm} fail_rpm={fail_rpm} p95_ms={p95_ms}", "vars": {"auth_rpm": {"k": "i", "v": [90, 150]}, "fail_rpm": {"k": "i", "v": [0, 20]}, "p95_ms": {"k": "i", "v": [80, 1200]}}},
            },
            "beh": {"n": {"emit": [{"id": "mc_metrics", "per_min": 1.0}]}, "f": {"emit": [{"id": "mc_metrics", "per_min": 1.0}]}},
        },
        {
            "id": "internal_tooling",
            "svc": None,
            "hosts": ["ops-1"],
            "logs": {
                "tool_req": {
                    "lvl": "INFO",
                    "msg": "tool_req tool={tool} op={op} trace={trace_id}",
                    "vars": {"tool": {"k": "ch", "v": ["cops_console", "deploy_tool"]}, "op": {"k": "ch", "v": ["open_chat", "send_message", "deploy"]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "tool_ok": {"lvl": "INFO", "msg": "tool_resp tool={tool} status=200 dur_ms={dur_ms}", "vars": {"tool": {"k": "ch", "v": ["cops_console", "deploy_tool"]}, "dur_ms": {"k": "i", "v": [20, 300]}}},
                "tool_err_404": {"lvl": "ERROR", "msg": "tool_resp tool={tool} status=404 dur_ms={dur_ms}", "vars": {"tool": {"k": "ch", "v": ["cops_console", "deploy_tool"]}, "dur_ms": {"k": "i", "v": [10, 200]}}},
                "fallback_deploy_used": {"lvl": "WARN", "msg": "deploy_fallback used app={app} version={version} operator={operator}", "vars": {"app": {"k": "ch", "v": ["card-auth-service"]}, "version": {"k": "ch", "v": ["2019.07.29.2"]}, "operator": {"k": "hex", "v": 8}}},
                "manual_cassandra_query": {"lvl": "INFO", "msg": "manual_query cassandra key={key} result={result}", "vars": {"key": {"k": "ch", "v": ["private_network_cidr"]}, "result": {"k": "ch", "v": ["MISSING"]}}},
                "status_page_updated": {"lvl": "INFO", "msg": "status_page updated incident={incident} message_id={message_id}", "vars": {"incident": {"k": "ch", "v": ["degraded_app_and_cards"]}, "message_id": {"k": "hex", "v": 10}}},
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "alerting",
            "svc": "alertmanager",
            "hosts": ["alert-1"],
            "logs": {
                "alert_fired": {
                    "lvl": "CRITICAL",
                    "msg": "alert fired name={name} severity={severity} value={value}",
                    "vars": {"name": {"k": "ch", "v": ["card_fail_rate", "internal_edge_404_rate"]}, "severity": {"k": "ch", "v": ["page", "high"]}, "value": {"k": "f", "v": [0.01, 0.15]}},
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "app_login_ok_n",
                    "rpm": 35.0,
                    "emit": ["app_api.http_in", "account_service.session_read_ok", "app_api.http_out_200"],
                    "latency_ms": [[2, 6], [8, 40], [2, 8]],
                    "trace": True,
                },
                {
                    "id": "app_balance_ok_n",
                    "rpm": 250.0,
                    "emit": ["app_api.http_in", "account_service.balance_read_ok", "app_api.http_out_200"],
                    "latency_ms": [[2, 6], [10, 60], [2, 8]],
                    "trace": True,
                },
                {
                    "id": "internal_tool_ok_n",
                    "rpm": 30.0,
                    "emit": ["internal_tooling.tool_req", "internal_edge.req_in", "config_service.get_ok", "internal_edge.route_ok", "internal_tooling.tool_ok"],
                    "latency_ms": [[1, 4], [1, 5], [2, 20], [1, 5], [5, 60]],
                    "trace": True,
                },
                {"id": "card_auth_ok_n", "rpm": 120.0, "emit": ["mastercard_service.auth_req", "mastercard_service.auth_ok"], "latency_ms": [[5, 20], [40, 900]], "trace": False},
            ]
        },
        "f": {
            "req": [
                {"id": "app_login_ok_f", "rpm": 30.0, "emit": ["app_api.http_in", "account_service.session_read_ok", "app_api.http_out_200"], "latency_ms": [[2, 8], [10, 60], [2, 10]], "trace": True},
                {"id": "app_login_fail_missing_f", "rpm": 15.0, "emit": ["app_api.http_in", "account_service.session_read_empty", "app_api.http_out_503"], "latency_ms": [[2, 8], [10, 80], [2, 12]], "trace": True},
                {"id": "app_balance_ok_f", "rpm": 240.0, "emit": ["app_api.http_in", "account_service.balance_read_ok", "app_api.http_out_200"], "latency_ms": [[2, 8], [12, 80], [2, 12]], "trace": True},
                {"id": "app_balance_stale_f", "rpm": 60.0, "emit": ["app_api.http_in", "account_service.balance_read_partial", "app_api.http_out_200_stale"], "latency_ms": [[2, 8], [15, 120], [2, 15]], "trace": True},
                {"id": "internal_tool_ok_f", "rpm": 10.0, "emit": ["internal_tooling.tool_req", "internal_edge.req_in", "config_service.get_ok", "internal_edge.route_ok", "internal_tooling.tool_ok"], "latency_ms": [[1, 4], [1, 6], [2, 25], [1, 6], [5, 80]], "trace": True},
                {"id": "internal_tool_404_f", "rpm": 25.0, "emit": ["internal_tooling.tool_req", "internal_edge.req_in", "config_service.get_not_found_404", "internal_edge.route_404", "internal_tooling.tool_err_404"], "latency_ms": [[1, 4], [1, 6], [2, 30], [1, 6], [5, 60]], "trace": True},
                {"id": "card_auth_ok_f", "rpm": 115.0, "emit": ["mastercard_service.auth_req", "mastercard_service.auth_ok"], "latency_ms": [[5, 20], [50, 1100]], "trace": False},
                {"id": "card_auth_fail_bug_f", "rpm": 10.0, "emit": ["mastercard_service.auth_req", "mastercard_service.auth_fail_bug"], "latency_ms": [[5, 20], [20, 300]], "trace": False},
            ]
        },
    },
}


SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "cassandra_scaleup_auto_bootstrap_false_incident",
        "time": {"total_minutes": 55, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 55}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 25,
                        "rate_multipliers": {
                            "app_balance_stale_f": 0.0,
                            "app_login_fail_missing_f": 0.0,
                            "internal_tool_404_f": 0.0,
                            "card_auth_fail_bug_f": 0.0,
                            "internal_edge.edge_metrics_spike": 0.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "cassandra_cluster.scale_up_start", "count": 1, "hosts": ["cass-a1"]},
                            {"ref": "cassandra_cluster.node_joined", "count": 6, "hosts": ["cass-n1", "cass-n2", "cass-n3", "cass-n4", "cass-n5", "cass-n6"]},
                        ],
                    },
                    {
                        "order": 2,
                        "at_min": 29,
                        "rate_multipliers": {
                            "app_balance_ok_f": 0.8,
                            "app_balance_stale_f": 1.2,
                            "app_login_ok_f": 0.9,
                            "app_login_fail_missing_f": 1.5,
                            "internal_tool_ok_f": 0.5,
                            "internal_tool_404_f": 1.2,
                            "card_auth_ok_f": 0.95,
                            "card_auth_fail_bug_f": 2.0,
                            "internal_edge.edge_metrics": 0.0,
                            "internal_edge.edge_metrics_spike": 1.0,
                        },
                        "latency_multipliers": {"app_balance_stale_f": {"p50": 1.2, "p95": 1.3}},
                        "one_shots": [
                            {"ref": "cassandra_cluster.ownership_changed", "count": 1, "hosts": ["cass-a2"]},
                            {"ref": "alerting.alert_fired", "count": 2, "hosts": ["alert-1"]},
                        ],
                    },
                    {
                        "order": 3,
                        "at_min": 36,
                        "rate_multipliers": {
                            "app_balance_ok_f": 0.7,
                            "app_balance_stale_f": 1.4,
                            "app_login_ok_f": 0.8,
                            "app_login_fail_missing_f": 1.8,
                            "internal_tool_ok_f": 0.4,
                            "internal_tool_404_f": 1.3,
                            "card_auth_ok_f": 1.0,
                            "card_auth_fail_bug_f": 0.6,
                            "internal_edge.edge_metrics": 0.0,
                            "internal_edge.edge_metrics_spike": 1.0,
                        },
                        "latency_multipliers": {"internal_tool_404_f": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [
                            {"ref": "internal_tooling.status_page_updated", "count": 1, "hosts": ["ops-1"]},
                            {"ref": "internal_tooling.manual_cassandra_query", "count": 1, "hosts": ["ops-1"]},
                            {"ref": "internal_tooling.fallback_deploy_used", "count": 1, "hosts": ["ops-1"]},
                            {"ref": "mastercard_service.deploy_version", "count": 1, "hosts": ["mc-1"]},
                        ],
                    },
                    {
                        "order": 4,
                        "at_min": 43,
                        "rate_multipliers": {
                            "app_balance_ok_f": 0.75,
                            "app_balance_stale_f": 1.3,
                            "app_login_ok_f": 0.8,
                            "app_login_fail_missing_f": 1.6,
                            "internal_tool_ok_f": 0.4,
                            "internal_tool_404_f": 1.2,
                            "internal_edge.edge_metrics": 0.0,
                            "internal_edge.edge_metrics_spike": 1.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [{"ref": "cassandra_cluster.decommission_start", "count": 1, "hosts": ["cass-n1"]}],
                    },
                    {
                        "order": 5,
                        "at_min": 52,
                        "rate_multipliers": {
                            "app_balance_ok_f": 0.9,
                            "app_balance_stale_f": 0.6,
                            "app_login_ok_f": 1.0,
                            "app_login_fail_missing_f": 0.8,
                            "internal_tool_ok_f": 0.8,
                            "internal_tool_404_f": 0.4,
                            "internal_edge.edge_metrics": 0.0,
                            "internal_edge.edge_metrics_spike": 0.4,
                        },
                        "latency_multipliers": {"internal_tool_ok_f": {"p50": 0.9, "p95": 0.9}},
                        "one_shots": [{"ref": "cassandra_cluster.decommission_complete", "count": 1, "hosts": ["cass-n1"]}],
                    },
                ]
            }
        },
    }
}


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def unit(s: str) -> float:
    # Stable [0,1)
    h = hashlib.md5(s.encode("utf-8")).digest()
    x = int.from_bytes(h[:8], "big")  # 64-bit
    return (x % (10**15)) / float(10**15)


def det_int(lo: int, hi: int, key: str) -> int:
    if hi < lo:
        lo, hi = hi, lo
    if lo == hi:
        return lo
    u = unit(key)
    return lo + int(math.floor(u * (hi - lo + 1)))


def det_float(lo: float, hi: float, key: str) -> float:
    if hi < lo:
        lo, hi = hi, lo
    if abs(hi - lo) < 1e-12:
        return float(lo)
    u = unit(key)
    return lo + u * (hi - lo)


def det_choice(values: List[Any], key: str) -> Any:
    if not values:
        return None
    idx = int(math.floor(unit(key) * len(values)))
    if idx == len(values):
        idx = len(values) - 1
    return values[idx]


def det_hex(n: int, key: str) -> str:
    if n <= 0:
        return ""
    out = ""
    k = 0
    while len(out) < n:
        out += md5_hex(f"{key}-hex-{k}")
        k += 1
    return out[:n].lower()


def det_uuid(key: str) -> str:
    h = det_hex(32, f"{key}-uuid")
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def det_ip_cidr_10_8(key: str) -> str:
    a = 10
    b = det_int(0, 255, key + "-b")
    c = det_int(0, 255, key + "-c")
    d = det_int(1, 254, key + "-d")
    return f"{a}.{b}.{c}.{d}"


def expected_to_count(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    rem = expected - base
    return base + (1 if unit(key + "-rem") < rem else 0)


def schedule_times(start: datetime, end: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    duration_s = (end - start).total_seconds()
    if duration_s <= 0:
        return [start for _ in range(count)]
    spacing = duration_s / count
    jitter_amp = min(0.20, max(0.0, spacing * 0.30))
    times: List[datetime] = []
    for i in range(count):
        base_off = (i + 0.5) * spacing
        jitter = (unit(f"{key}-j-{i}") - 0.5) * 2.0 * jitter_amp
        off = base_off + jitter
        if off < 0:
            off = 0.0
        if off > duration_s:
            off = duration_s
        ts = start + timedelta(seconds=off)
        if ts >= end:
            ts = end - timedelta(milliseconds=1)
        times.append(ts)
    return times


def sample_latency_ms(p50: float, p95: float, key: str, cap_mult: float = 2.5) -> int:
    p50 = max(1e-6, float(p50))
    p95 = max(p50, float(p95))
    u = unit(key)
    w = u**1.7
    ratio = p95 / p50 if p50 > 0 else 1.0
    val = p50 * (ratio**w)
    if u > 0.985:
        tail_u = (u - 0.985) / 0.015
        val = max(val, p95 * (1.0 + 1.5 * tail_u))
    cap = cap_mult * p95
    if val > cap:
        val = cap
    return max(1, int(round(val)))


def iso_z(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:23] + "Z"


@dataclass
class Interval:
    state: str
    start_min: int
    end_min: int
    rate_mult: Dict[str, float]
    latency_mult: Dict[str, Dict[str, float]]


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    comp_by_id: Dict[str, Any] = {c["id"]: c for c in system["components"]}
    log_by_ref: Dict[str, Dict[str, Any]] = {}
    for cid, comp in comp_by_id.items():
        for lid, tpl in comp.get("logs", {}).items():
            log_by_ref[f"{cid}.{lid}"] = {"component_id": cid, "log_id": lid, **tpl}

    flow_by_state: Dict[str, Dict[str, Any]] = {"n": {}, "f": {}}
    for st in ["n", "f"]:
        for flow in system["flows"][st]["req"]:
            flow_by_state[st][flow["id"]] = flow
    return comp_by_id, log_by_ref, flow_by_state


def derive_failure_intervals(scenario: Dict[str, Any]) -> Tuple[List[Interval], List[Dict[str, Any]]]:
    sc = scenario["scenario"]
    n_start = sc["time"]["phases"]["n"]["start_min"]
    n_end = sc["time"]["phases"]["n"]["end_min"]
    f_end = sc["time"]["phases"]["f"]["end_min"]

    intervals: List[Interval] = [Interval(state="n", start_min=n_start, end_min=n_end, rate_mult={}, latency_mult={})]

    events = list(sc["phases"]["f"]["events"])
    events.sort(key=lambda e: (e["at_min"], e.get("order", 0)))

    active_rate: Dict[str, float] = {}
    active_lat: Dict[str, Dict[str, float]] = {}
    one_shots: List[Dict[str, Any]] = []

    for i, ev in enumerate(events):
        at = int(ev["at_min"])
        for k, v in ev.get("rate_multipliers", {}).items():
            active_rate[k] = float(v)
        for k, v in ev.get("latency_multipliers", {}).items():
            active_lat[k] = {"p50": float(v.get("p50", 1.0)), "p95": float(v.get("p95", 1.0))}
        for ospec in ev.get("one_shots", []):
            one_shots.append({"at_min": at, **ospec})

        next_at = int(events[i + 1]["at_min"]) if i + 1 < len(events) else f_end
        intervals.append(Interval(state="f", start_min=at, end_min=next_at, rate_mult=dict(active_rate), latency_mult=dict(active_lat)))

    intervals = [iv for iv in intervals if iv.end_min > iv.start_min]
    return intervals, one_shots


def choose_component_host(comp: Dict[str, Any], key: str) -> str:
    hosts = comp.get("hosts") or []
    if not hosts:
        return ""
    if len(hosts) == 1:
        return hosts[0]
    idx = det_int(0, len(hosts) - 1, key + "-host")
    return hosts[idx]


def fill_vars(tpl_vars: Dict[str, Any], preset: Dict[str, Any], base_key: str) -> Dict[str, Any]:
    ctx = dict(preset)
    for var, spec in tpl_vars.items():
        if var in ctx:
            continue
        k = spec["k"]
        v = spec["v"]
        if k == "ch":
            ctx[var] = det_choice(list(v), f"{base_key}-{var}")
        elif k == "i":
            lo, hi = int(v[0]), int(v[1])
            ctx[var] = det_int(lo, hi, f"{base_key}-{var}")
        elif k == "f":
            lo, hi = float(v[0]), float(v[1])
            ctx[var] = round(det_float(lo, hi, f"{base_key}-{var}"), 3)
        elif k == "hex":
            ctx[var] = det_hex(int(v), f"{base_key}-{var}")
        elif k == "uuid":
            ctx[var] = det_uuid(f"{base_key}-{var}")
        elif k == "ip":
            ctx[var] = det_ip_cidr_10_8(f"{base_key}-{var}")
        else:
            ctx[var] = str(v) if v is not None else ""
    return ctx


def render_message(tpl: Dict[str, Any], ctx: Dict[str, Any]) -> str:
    return tpl["msg"].format(**ctx)


def emit_row(rows: List[Dict[str, Any]], ts: datetime, tpl: Dict[str, Any], msg_ctx: Dict[str, Any], trace_id: str, comp: Dict[str, Any], host: str) -> None:
    service = comp.get("svc")
    rows.append(
        {
            "ts": ts,
            "level": tpl["lvl"],
            "message": render_message(tpl, msg_ctx),
            "trace_id": trace_id,
            "service": "" if service is None else str(service),
            "host": host or "",
        }
    )


def plan_flow_context(flow_id: str, inst_idx: int, trace_id: str) -> Dict[str, Any]:
    base = f"flowctx-{flow_id}-{inst_idx}"
    ctx: Dict[str, Any] = {"trace_id": trace_id}

    if flow_id.startswith("app_login_"):
        ctx["method"] = "POST"
        ctx["endpoint"] = "/login"
        ctx["user_id"] = det_uuid(base + "-user")
        if "fail" in flow_id:
            ctx["err"] = "session_missing"
    elif flow_id.startswith("app_balance_"):
        ctx["method"] = "GET"
        ctx["endpoint"] = "/balance"
        ctx["user_id"] = det_uuid(base + "-user")
    elif flow_id.startswith("internal_tool_"):
        tool = "deploy_tool" if unit(base + "-tool") > 0.55 else "cops_console"
        route = "/deploy/run" if tool == "deploy_tool" else "/cops/chat"
        op = "deploy" if tool == "deploy_tool" else ("send_message" if unit(base + "-op") > 0.6 else "open_chat")
        method = "POST" if tool == "deploy_tool" else "GET"
        ctx.update(
            {
                "tool": tool,
                "route": route,
                "op": op,
                "method": method,
                "employee_id": det_hex(8, base + "-emp"),
                "src_ip": det_ip_cidr_10_8(base + "-ip"),
                "key": "private_network_cidr",
                "cfg_key": "private_network_cidr",
            }
        )
        ctx["upstream"] = "deploy-orchestrator" if route == "/deploy/run" else "cops-backend"
    elif flow_id.startswith("card_auth_"):
        auth_id = det_uuid(base + "-auth")
        ctx.update(
            {
                "auth_id": auth_id,
                "amount_pence": det_int(100, 50000, base + "-amt"),
                "mcc": det_choice([5411, 5812, 5912, 5999], base + "-mcc"),
            }
        )
        if "fail" in flow_id:
            ctx["err"] = det_choice(["upstream_timeout", "upstream_5xx", "parse_error"], base + "-err")
    return ctx


def simulate_background(
    rows: List[Dict[str, Any]],
    base_time: datetime,
    interval: Interval,
    comp_by_id: Dict[str, Any],
    log_by_ref: Dict[str, Any],
) -> None:
    start_dt = base_time + timedelta(minutes=interval.start_min)
    end_dt = base_time + timedelta(minutes=interval.end_min)
    duration_min = interval.end_min - interval.start_min

    for cid, comp in comp_by_id.items():
        beh = comp.get("beh", {}).get(interval.state, {})
        for emit_spec in beh.get("emit", []):
            log_id = emit_spec["id"]
            per_min = float(emit_spec.get("per_min", 0.0))
            scope = emit_spec.get("scope", "per_host")

            mult = 1.0
            if interval.state == "f":
                mult = float(interval.rate_mult.get(f"{cid}.{log_id}", 1.0))

            eff_rate = per_min * mult
            if eff_rate <= 0:
                continue

            ref = f"{cid}.{log_id}"
            tpl = log_by_ref[ref]

            if scope == "global":
                expected = eff_rate * duration_min
                count = expected_to_count(expected, f"bgcnt-{ref}-{interval.start_min}")
                times = schedule_times(start_dt, end_dt, count, f"bgtime-{ref}-{interval.start_min}")
                for i, ts in enumerate(times):
                    host = choose_component_host(comp, f"bghost-{ref}-{interval.start_min}-{i}")
                    msg_ctx = fill_vars(tpl.get("vars", {}), {}, f"bg-{ref}-{interval.start_min}-{i}")
                    emit_row(rows, ts, tpl, msg_ctx, "", comp, host)
            else:
                for host in (comp.get("hosts") or [""]):
                    expected = eff_rate * duration_min
                    count = expected_to_count(expected, f"bgcnt-{ref}-{host}-{interval.start_min}")
                    times = schedule_times(start_dt, end_dt, count, f"bgtime-{ref}-{host}-{interval.start_min}")
                    for i, ts in enumerate(times):
                        msg_ctx = fill_vars(tpl.get("vars", {}), {}, f"bg-{ref}-{host}-{interval.start_min}-{i}")
                        emit_row(rows, ts, tpl, msg_ctx, "", comp, host)


def emit_one_shots(
    rows: List[Dict[str, Any]],
    base_time: datetime,
    one_shots: List[Dict[str, Any]],
    comp_by_id: Dict[str, Any],
    log_by_ref: Dict[str, Any],
) -> None:
    for ospec in sorted(one_shots, key=lambda x: (x["at_min"], x["ref"], x["count"])):
        at_min = int(ospec["at_min"])
        ref = ospec["ref"]
        count = int(ospec["count"])
        allowed_hosts = list(ospec.get("hosts") or [])

        tpl = log_by_ref[ref]
        cid = tpl["component_id"]
        comp = comp_by_id[cid]

        base_ts = base_time + timedelta(minutes=at_min)
        if count <= 1:
            times = [base_ts + timedelta(milliseconds=det_int(0, 900, f"os-{ref}-{at_min}-0"))]
        else:
            times = []
            for i in range(count):
                frac = (i + 0.5) / count
                off = frac * 10.0
                jitter = (unit(f"os-{ref}-{at_min}-j{i}") - 0.5) * 0.8
                times.append(base_ts + timedelta(seconds=off + jitter))

        for i in range(count):
            ts = times[i]
            host = allowed_hosts[i % len(allowed_hosts)] if allowed_hosts else choose_component_host(comp, f"os-host-{ref}-{at_min}-{i}")

            preset: Dict[str, Any] = {}
            if ref == "cassandra_cluster.node_joined":
                preset["node"] = host
                preset["join_state"] = "JOINED"
            elif ref == "cassandra_cluster.decommission_start":
                preset["node"] = host
            elif ref == "cassandra_cluster.decommission_complete":
                preset["node"] = host
                preset["duration_min"] = det_int(8, 10, f"os-{ref}-{at_min}-dur")
            elif ref == "cassandra_cluster.scale_up_start":
                preset["add_nodes"] = 6
                preset["auto_bootstrap"] = False
            elif ref == "cassandra_cluster.ownership_changed":
                preset["new_nodes_active"] = det_int(3, 6, f"os-{ref}-{at_min}-act")
            elif ref == "internal_tooling.manual_cassandra_query":
                preset["key"] = "private_network_cidr"
                preset["result"] = "MISSING"
            elif ref == "mastercard_service.deploy_version":
                preset["version"] = "2019.07.29.2"
                preset["via"] = "fallback_mechanism"
            elif ref == "alerting.alert_fired":
                if i % 2 == 0:
                    preset["name"] = "internal_edge_404_rate"
                    preset["severity"] = "page"
                    preset["value"] = 0.12
                else:
                    preset["name"] = "card_fail_rate"
                    preset["severity"] = "high"
                    preset["value"] = 0.06

            msg_ctx = fill_vars(tpl.get("vars", {}), preset, f"os-{ref}-{at_min}-{i}")
            emit_row(rows, ts, tpl, msg_ctx, "", comp, host)


def main() -> None:
    # Explicit seeding for verifier A3. Core simulation is deterministic via hashing,
    # but this guarantees reproducibility if any library uses randomness.
    SEED = 1337
    random.seed(SEED)
    np.random.seed(SEED)

    base_time = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    comp_by_id, log_by_ref, _flow_by_state = build_indices(SYSTEM)
    intervals, one_shots = derive_failure_intervals(SCENARIO)

    rows: List[Dict[str, Any]] = []

    for iv in intervals:
        simulate_background(rows, base_time, iv, comp_by_id, log_by_ref)

    for iv in intervals:
        start_dt = base_time + timedelta(minutes=iv.start_min)
        end_dt = base_time + timedelta(minutes=iv.end_min)
        duration_min = iv.end_min - iv.start_min
        st = iv.state

        for flow in SYSTEM["flows"][st]["req"]:
            flow_id = flow["id"]
            rpm = float(flow["rpm"])
            mult = float(iv.rate_mult.get(flow_id, 1.0)) if st == "f" else 1.0
            eff_rpm = rpm * mult

            expected_instances = eff_rpm * duration_min
            count = expected_to_count(expected_instances, f"flowcnt-{flow_id}-{iv.start_min}-{iv.end_min}")
            if count <= 0:
                continue

            starts = schedule_times(start_dt, end_dt, count, f"flowstarts-{flow_id}-{iv.start_min}-{iv.end_min}")
            for j, st_ts in enumerate(starts):
                inst_idx = det_int(0, 10**9, f"inst-{flow_id}-{iv.start_min}-{j}")

                pinned_start = st_ts

                trace_id = ""
                if SYSTEM["tracing"]["on"] and flow.get("trace", False):
                    trace_id = det_hex(32, f"trace-{flow_id}-{inst_idx}")

                emits = flow["emit"]
                lat_hints = flow["latency_ms"]

                comp_host: Dict[str, str] = {}
                for ref in emits:
                    cid = ref.split(".", 1)[0]
                    if cid not in comp_host:
                        comp_host[cid] = choose_component_host(comp_by_id[cid], f"host-{flow_id}-{inst_idx}-{cid}-{trace_id}")

                lat_mult = iv.latency_mult.get(flow_id, {"p50": 1.0, "p95": 1.0})
                p50m = float(lat_mult.get("p50", 1.0))
                p95m = float(lat_mult.get("p95", 1.0))

                flow_ctx = plan_flow_context(flow_id, inst_idx, trace_id)

                step_delays: List[int] = []
                for si, (ref, (p50, p95)) in enumerate(zip(emits, lat_hints)):
                    tpl = log_by_ref[ref]
                    tpl_vars = tpl.get("vars", {})
                    d = sample_latency_ms(p50 * p50m, p95 * p95m, f"lat-{flow_id}-{inst_idx}-s{si}")
                    if "latency_ms" in tpl_vars and tpl_vars["latency_ms"]["k"] == "i":
                        lo, hi = int(tpl_vars["latency_ms"]["v"][0]), int(tpl_vars["latency_ms"]["v"][1])
                        d = max(lo, min(hi, d))
                    step_delays.append(d)

                final_tpl = log_by_ref[emits[-1]]
                final_vars = final_tpl.get("vars", {})
                if "dur_ms" in final_vars and final_vars["dur_ms"]["k"] == "i":
                    dur_lo, dur_hi = int(final_vars["dur_ms"]["v"][0]), int(final_vars["dur_ms"]["v"][1])
                    dur_ms = sum(step_delays[1:]) if len(step_delays) >= 2 else 0
                    if dur_ms < dur_lo:
                        step_delays[-1] += (dur_lo - dur_ms)
                    elif dur_ms > dur_hi:
                        reduce_by = min(step_delays[-1] - 1, dur_ms - dur_hi)
                        step_delays[-1] -= max(0, int(reduce_by))

                t = pinned_start
                first_log_ts: Optional[datetime] = None
                for si, ref in enumerate(emits):
                    tpl = log_by_ref[ref]
                    cid = tpl["component_id"]
                    comp = comp_by_id[cid]
                    host = comp_host.get(cid, choose_component_host(comp, f"host-{flow_id}-{inst_idx}-{cid}-{trace_id}"))

                    dms = step_delays[si]
                    t = t + timedelta(milliseconds=dms)
                    if si == 0:
                        first_log_ts = t

                    preset = dict(flow_ctx)
                    if "latency_ms" in tpl.get("vars", {}):
                        preset["latency_ms"] = dms
                    if "dur_ms" in tpl.get("vars", {}) and first_log_ts is not None:
                        preset["dur_ms"] = int(round((t - first_log_ts).total_seconds() * 1000.0))

                    if ref.endswith("account_service.session_read_ok"):
                        preset["replica"] = det_choice(["cass-a1", "cass-a2", "cass-a3"], f"{flow_id}-{inst_idx}-rep-ok")
                    elif ref.endswith("account_service.session_read_empty"):
                        preset["replica"] = det_choice(["cass-n1", "cass-n2", "cass-n3", "cass-n4", "cass-n5", "cass-n6"], f"{flow_id}-{inst_idx}-rep-miss")
                    elif ref.endswith("account_service.balance_read_ok"):
                        preset["replica"] = det_choice(["cass-a1", "cass-a2", "cass-a3"], f"{flow_id}-{inst_idx}-rep-bal-ok")
                    elif ref.endswith("account_service.balance_read_partial"):
                        preset["replica"] = det_choice(["cass-n1", "cass-n2", "cass-n3", "cass-n4", "cass-n5", "cass-n6"], f"{flow_id}-{inst_idx}-rep-bal-part")
                    elif ref.endswith("config_service.get_ok") or ref.endswith("config_service.get_not_found_404"):
                        preset["key"] = "private_network_cidr"
                    elif ref.endswith("internal_edge.route_ok"):
                        preset["upstream"] = "deploy-orchestrator" if preset.get("route") == "/deploy/run" else "cops-backend"
                    elif ref.endswith("app_api.http_out_503"):
                        preset["err"] = preset.get("err", "session_missing")
                    elif ref.endswith("app_api.http_in") or ref.endswith("internal_edge.req_in") or ref.endswith("internal_tooling.tool_req"):
                        preset["trace_id"] = trace_id

                    msg_ctx = fill_vars(tpl.get("vars", {}), preset, f"msg-{flow_id}-{inst_idx}-s{si}-{ref}")
                    emit_row(rows, t, tpl, msg_ctx, trace_id, comp, host)

    emit_one_shots(rows, base_time, one_shots, comp_by_id, log_by_ref)

    df = pd.DataFrame(rows)
    df = df.sort_values("ts", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["ts"].apply(iso_z)
    df = df.drop(columns=["ts"])
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    if not (20000 <= len(df) <= 100000):
        raise RuntimeError(f"Row count {len(df)} out of required range [20000,100000].")

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
