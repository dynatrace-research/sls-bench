import math
import re
import hashlib
import uuid
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from statistics import NormalDist
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# -----------------------------
# Embedded normalized model data
# -----------------------------
SYSTEM: Dict[str, Any] = {
    "sys": {"id": "merchant_identity_outage_20170316"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": True, "origins": ["api_gateway"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "api_gateway",
            "svc": "api-gateway",
            "hosts": ["gw-1", "gw-2"],
            "logs": {
                "gw_req_login": {
                    "lvl": "INFO",
                    "msg": "ingress POST /v2/login rid={rid} ip={client_ip} trace={trace_id}",
                    "vars": {
                        "rid": {"k": "uuid", "v": None},
                        "client_ip": {"k": "ip", "v": "10.0.0.0/8"},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "gw_resp_login_200": {
                    "lvl": "INFO",
                    "msg": "egress 200 /v2/login rid={rid} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [60, 400]}},
                        "f": {"dur_ms": {"k": "i", "v": [80, 650]}},
                    },
                },
                "gw_resp_login_503": {
                    "lvl": "WARN",
                    "msg": "egress 503 /v2/login rid={rid} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "rid": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [2000, 9000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "gw_req_charge": {
                    "lvl": "INFO",
                    "msg": "ingress POST /v2/charge rid={rid} merchant={merchant_id} trace={trace_id}",
                    "vars": {
                        "rid": {"k": "uuid", "v": None},
                        "merchant_id": {"k": "ch", "v": ["m_1001", "m_1002", "m_1003", "m_1004"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "gw_resp_charge_200": {
                    "lvl": "INFO",
                    "msg": "egress 200 /v2/charge rid={rid} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                    "state_vars": {
                        "n": {"dur_ms": {"k": "i", "v": [80, 500]}},
                        "f": {"dur_ms": {"k": "i", "v": [90, 700]}},
                    },
                },
                "gw_resp_charge_503": {
                    "lvl": "WARN",
                    "msg": "egress 503 /v2/charge rid={rid} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "rid": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [2500, 10000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "upstream_5xx_metric": {
                    "lvl": "INFO",
                    "msg": "upstream_5xx_rate={rate_perc}% window_s=60",
                    "vars": {},
                    "state_vars": {
                        "n": {"rate_perc": {"k": "i", "v": [0, 2]}},
                        "f": {"rate_perc": {"k": "i", "v": [0, 8]}},
                    },
                },
                "upstream_5xx_metric_high": {
                    "lvl": "INFO",
                    "msg": "upstream_5xx_rate={rate_perc}% window_s=60 mode=incident",
                    "vars": {"rate_perc": {"k": "i", "v": [10, 80]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "upstream_5xx_metric", "per_min": 1.0}]},
                "f": {"emit": [{"id": "upstream_5xx_metric", "per_min": 1.0}, {"id": "upstream_5xx_metric_high", "per_min": 1.0}]},
            },
        },
        {
            "id": "product_api",
            "svc": "product-api",
            "hosts": ["prod-1", "prod-2"],
            "logs": {
                "login_start_no2fa": {
                    "lvl": "INFO",
                    "msg": "login start rid={rid} merchant={merchant_id} 2fa=false",
                    "vars": {"rid": {"k": "uuid", "v": None}, "merchant_id": {"k": "ch", "v": ["m_1001", "m_1002", "m_1003", "m_1004"]}},
                },
                "login_ok_no2fa": {
                    "lvl": "INFO",
                    "msg": "login ok rid={rid} user={user_id} dur_ms={dur_ms}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "user_id": {"k": "ch", "v": ["u_2001", "u_2002", "u_2003", "u_2004"]}},
                    "state_vars": {"n": {"dur_ms": {"k": "i", "v": [40, 250]}}, "f": {"dur_ms": {"k": "i", "v": [50, 320]}}},
                },
                "login_start_2fa": {
                    "lvl": "INFO",
                    "msg": "login start rid={rid} merchant={merchant_id} 2fa=true",
                    "vars": {"rid": {"k": "uuid", "v": None}, "merchant_id": {"k": "ch", "v": ["m_1001", "m_1002", "m_1003", "m_1004"]}},
                },
                "login_ok_2fa": {
                    "lvl": "INFO",
                    "msg": "login ok rid={rid} user={user_id} 2fa=sent dur_ms={dur_ms}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "user_id": {"k": "ch", "v": ["u_2001", "u_2002", "u_2003", "u_2004"]}},
                    "state_vars": {"n": {"dur_ms": {"k": "i", "v": [120, 900]}}, "f": {"dur_ms": {"k": "i", "v": [150, 1100]}}},
                },
                "login_fail_upstream_timeout": {
                    "lvl": "ERROR",
                    "msg": "login failed rid={rid} cause=auth_timeout dur_ms={dur_ms}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [2000, 9000]}},
                },
                "login_fail_2fa_delivery": {
                    "lvl": "WARN",
                    "msg": "login failed rid={rid} cause=2fa_delivery status={status}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "status": {"k": "i", "v": [503, 503]}},
                },
                "charge_start": {
                    "lvl": "INFO",
                    "msg": "charge start rid={rid} merchant={merchant_id} amount_cents={amount_cents}",
                    "vars": {
                        "rid": {"k": "uuid", "v": None},
                        "merchant_id": {"k": "ch", "v": ["m_1001", "m_1002", "m_1003", "m_1004"]},
                        "amount_cents": {"k": "i", "v": [100, 15000]},
                    },
                },
                "charge_ok": {
                    "lvl": "INFO",
                    "msg": "charge ok rid={rid} auth={auth_code} dur_ms={dur_ms}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "auth_code": {"k": "hex", "v": 8}},
                    "state_vars": {"n": {"dur_ms": {"k": "i", "v": [60, 350]}}, "f": {"dur_ms": {"k": "i", "v": [70, 450]}}},
                },
                "charge_fail_upstream_timeout": {
                    "lvl": "ERROR",
                    "msg": "charge failed rid={rid} cause=auth_timeout dur_ms={dur_ms}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "dur_ms": {"k": "i", "v": [2500, 10000]}},
                },
                "worker_metric": {
                    "lvl": "INFO",
                    "msg": "workers busy={busy} total={total} queue={queue}",
                    "vars": {"total": {"k": "i", "v": [200, 200]}, "busy": {"k": "i", "v": [10, 190]}, "queue": {"k": "i", "v": [0, 500]}},
                },
            },
            "beh": {"n": {"emit": [{"id": "worker_metric", "per_min": 0.5}]}, "f": {"emit": [{"id": "worker_metric", "per_min": 0.8}]}},
        },
        {
            "id": "multipass_auth",
            "svc": "multipass",
            "hosts": ["mp-wc-1", "mp-ec-1", "mp-jp-1"],
            "logs": {
                "auth_start": {
                    "lvl": "INFO",
                    "msg": "auth start rid={rid} merchant={merchant_id}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "merchant_id": {"k": "ch", "v": ["m_1001", "m_1002", "m_1003", "m_1004"]}},
                },
                "redis_txn_begin": {
                    "lvl": "DEBUG",
                    "msg": "redis txn begin rid={rid} key={key}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "key": {"k": "str", "v": "redis key like sess:m_1001 or acct:m_1001"}},
                },
                "redis_txn_commit": {
                    "lvl": "DEBUG",
                    "msg": "redis txn commit rid={rid} dur_ms={dur_ms}",
                    "vars": {"rid": {"k": "uuid", "v": None}},
                    "state_vars": {"n": {"dur_ms": {"k": "i", "v": [5, 40]}}, "f": {"dur_ms": {"k": "i", "v": [10, 120]}}},
                },
                "auth_ok": {
                    "lvl": "INFO",
                    "msg": "auth ok rid={rid} principal={principal} dur_ms={dur_ms}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "principal": {"k": "ch", "v": ["merchant", "employee", "device"]}},
                    "state_vars": {"n": {"dur_ms": {"k": "i", "v": [15, 120]}}, "f": {"dur_ms": {"k": "i", "v": [25, 200]}}},
                },
                "auth_timeout": {
                    "lvl": "ERROR",
                    "msg": "auth timeout rid={rid} timeout_ms={timeout_ms} waited_ms={waited_ms}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "timeout_ms": {"k": "i", "v": [2500, 5000]}, "waited_ms": {"k": "i", "v": [2500, 9000]}},
                },
                "redis_txn_retry": {
                    "lvl": "WARN",
                    "msg": "redis txn retry rid={rid} attempt={attempt} err={err}",
                    "vars": {"rid": {"k": "uuid", "v": None}, "attempt": {"k": "i", "v": [2, 500]}, "err": {"k": "ch", "v": ["WATCH_CONFLICT", "TIMEOUT"]}},
                },
                "timeout_metric": {
                    "lvl": "INFO",
                    "msg": "auth_timeouts_1m={count} redis_conflicts_1m={conflicts}",
                    "vars": {},
                    "state_vars": {"n": {"count": {"k": "i", "v": [0, 3]}, "conflicts": {"k": "i", "v": [0, 30]}}, "f": {"count": {"k": "i", "v": [0, 15]}, "conflicts": {"k": "i", "v": [0, 120]}}},
                },
                "timeout_metric_high": {"lvl": "INFO", "msg": "auth_timeouts_1m={count} redis_conflicts_1m={conflicts} mode=incident", "vars": {"count": {"k": "i", "v": [80, 700]}, "conflicts": {"k": "i", "v": [400, 4000]}}},
                "redis_ops_metric": {"lvl": "INFO", "msg": "redis_ops_1m={ops} redis_slowlog_1m={slow}", "vars": {}, "state_vars": {"n": {"ops": {"k": "i", "v": [2000, 9000]}, "slow": {"k": "i", "v": [0, 5]}}, "f": {"ops": {"k": "i", "v": [4000, 20000]}, "slow": {"k": "i", "v": [0, 30]}}}},
                "redis_ops_metric_high": {"lvl": "INFO", "msg": "redis_ops_1m={ops} redis_slowlog_1m={slow} mode=incident", "vars": {"ops": {"k": "i", "v": [20000, 90000]}, "slow": {"k": "i", "v": [50, 500]}}},
                "restart": {"lvl": "WARN", "msg": "service restart requested by {who} build={build}", "vars": {"who": {"k": "ch", "v": ["oncall"]}, "build": {"k": "ch", "v": ["mp-2017.03.16.1", "mp-2017.03.16.2"]}}},
                "deploy_fix": {"lvl": "INFO", "msg": "deploy build={build} change=cap_redis_retries backoff_ms={backoff_ms}", "vars": {"build": {"k": "ch", "v": ["mp-2017.03.16.2"]}, "backoff_ms": {"k": "i", "v": [10, 200]}}},
            },
            "beh": {
                "n": {"emit": [{"id": "timeout_metric", "per_min": 1.0}, {"id": "redis_ops_metric", "per_min": 1.0}, {"id": "redis_txn_retry", "per_min": 0.2}]},
                "f": {"emit": [{"id": "timeout_metric", "per_min": 1.0}, {"id": "timeout_metric_high", "per_min": 1.0}, {"id": "redis_ops_metric", "per_min": 1.0}, {"id": "redis_ops_metric_high", "per_min": 1.0}, {"id": "redis_txn_retry", "per_min": 70.0}]},
            },
        },
        {
            "id": "multipass_redis",
            "svc": "redis",
            "hosts": ["redis-1", "redis-2"],
            "logs": {
                "info_stats": {"lvl": "INFO", "msg": "stats ops={ops} connected_clients={clients} used_cpu={cpu_pct}% used_mem_mb={mem_mb}", "vars": {}, "state_vars": {"n": {"ops": {"k": "i", "v": [20000, 80000]}, "clients": {"k": "i", "v": [50, 400]}, "cpu_pct": {"k": "i", "v": [5, 35]}, "mem_mb": {"k": "i", "v": [2000, 6000]}}, "f": {"ops": {"k": "i", "v": [20000, 450000]}, "clients": {"k": "i", "v": [100, 1500]}, "cpu_pct": {"k": "i", "v": [10, 99]}, "mem_mb": {"k": "i", "v": [3000, 8000]}}}},
                "slowlog_entry": {"lvl": "WARN", "msg": "SLOWLOG cmd={cmd} dur_us={dur_us} key={key}", "vars": {"cmd": {"k": "ch", "v": ["EVAL", "GET", "SET", "WATCH", "MULTI", "EXEC"]}, "key": {"k": "str", "v": "redis key like sess:m_1002"}}, "state_vars": {"n": {"dur_us": {"k": "i", "v": [5000, 20000]}}, "f": {"dur_us": {"k": "i", "v": [20000, 300000]}}}},
                "cpu_saturation_warn": {"lvl": "ERROR", "msg": "redis cpu saturation cpu_pct={cpu_pct}% event=latency_spike", "vars": {"cpu_pct": {"k": "i", "v": [70, 99]}}},
                "monitor_attached": {"lvl": "INFO", "msg": "client MONITOR attached addr={client_ip}", "vars": {"client_ip": {"k": "ip", "v": "10.0.0.0/8"}}},
            },
            "beh": {"n": {"emit": [{"id": "info_stats", "per_min": 1.0}, {"id": "slowlog_entry", "per_min": 0.1}, {"id": "cpu_saturation_warn", "per_min": 0.0}]}, "f": {"emit": [{"id": "info_stats", "per_min": 1.0}, {"id": "slowlog_entry", "per_min": 6.0}, {"id": "cpu_saturation_warn", "per_min": 0.5}]}},
        },
        {
            "id": "roster_identity",
            "svc": "roster",
            "hosts": ["roster-wc-1", "roster-ec-1"],
            "logs": {
                "deploy_started": {"lvl": "INFO", "msg": "deploy started build={build} dc={dc} strategy=one_dc", "vars": {"build": {"k": "ch", "v": ["roster-2017.03.16.7"]}, "dc": {"k": "ch", "v": ["wc"]}}},
                "rollback_completed": {"lvl": "WARN", "msg": "rollback completed build={build} dc={dc}", "vars": {"build": {"k": "ch", "v": ["roster-2017.03.16.6"]}, "dc": {"k": "ch", "v": ["wc"]}}},
                "health_metric": {"lvl": "INFO", "msg": "health ok req_1m={req} err_1m={err}", "vars": {}, "state_vars": {"n": {"req": {"k": "i", "v": [5000, 20000]}, "err": {"k": "i", "v": [0, 20]}}, "f": {"req": {"k": "i", "v": [5000, 20000]}, "err": {"k": "i", "v": [0, 50]}}}},
            },
            "beh": {"n": {"emit": [{"id": "health_metric", "per_min": 1.0}]}, "f": {"emit": [{"id": "health_metric", "per_min": 1.0}]}},
        },
        {
            "id": "sms_gateway",
            "svc": "sms-gateway",
            "hosts": ["sms-1"],
            "logs": {
                "sms_send_req": {"lvl": "INFO", "msg": "sms send req rid={rid} to={msisdn} pool={pool_id}", "vars": {"rid": {"k": "uuid", "v": None}, "msisdn": {"k": "str", "v": "E.164 phone number like +14155550123"}, "pool_id": {"k": "ch", "v": ["pool-a"]}}},
                "sms_send_ok": {"lvl": "INFO", "msg": "sms sent rid={rid} vendor_id={vendor_id} dur_ms={dur_ms}", "vars": {"rid": {"k": "uuid", "v": None}, "vendor_id": {"k": "hex", "v": 12}}, "state_vars": {"n": {"dur_ms": {"k": "i", "v": [200, 1200]}}, "f": {"dur_ms": {"k": "i", "v": [300, 2000]}}}},
                "sms_send_throttled": {"lvl": "WARN", "msg": "sms send failed rid={rid} http=429 retry_after_s={retry_after_s}", "vars": {"rid": {"k": "uuid", "v": None}, "retry_after_s": {"k": "i", "v": [1, 30]}}},
                "vendor_throttle_metric": {"lvl": "INFO", "msg": "vendor_throttles_1m={count} queue_depth={queue_depth}", "vars": {}, "state_vars": {"n": {"count": {"k": "i", "v": [0, 5]}, "queue_depth": {"k": "i", "v": [0, 20]}}, "f": {"count": {"k": "i", "v": [5, 12]}, "queue_depth": {"k": "i", "v": [30, 300]}}}},
                "pool_add_numbers": {"lvl": "INFO", "msg": "sms pool update action=add_numbers pool={pool_id} added={added}", "vars": {"pool_id": {"k": "ch", "v": ["pool-a"]}, "added": {"k": "i", "v": [10, 100]}}},
                "pool_rebalance_complete": {"lvl": "INFO", "msg": "sms pool update action=rebalance_complete pool={pool_id} active_numbers={active_numbers}", "vars": {"pool_id": {"k": "ch", "v": ["pool-a"]}, "active_numbers": {"k": "i", "v": [50, 300]}}},
            },
            "beh": {"n": {"emit": [{"id": "vendor_throttle_metric", "per_min": 1.0, "scope": "global"}]}, "f": {"emit": [{"id": "vendor_throttle_metric", "per_min": 1.0, "scope": "global"}]}},
        },
        {
            "id": "synthetic_monitor",
            "svc": None,
            "hosts": ["extmon-1"],
            "logs": {
                "check_ok": {"lvl": "INFO", "msg": "synthetic check ok target=api duration_ms={dur_ms}", "vars": {}, "state_vars": {"n": {"dur_ms": {"k": "i", "v": [80, 250]}}, "f": {"dur_ms": {"k": "i", "v": [90, 300]}}}},
                "check_fail": {"lvl": "ERROR", "msg": "synthetic check fail target=api err={err} duration_ms={dur_ms}", "vars": {"err": {"k": "ch", "v": ["timeout", "conn_refused", "http_5xx"]}, "dur_ms": {"k": "i", "v": [2000, 8000]}},
                },
                "alert_opened": {"lvl": "CRITICAL", "msg": "ALERT opened id={alert_id} target=api severity=critical", "vars": {"alert_id": {"k": "ch", "v": ["ALERT-2791"]}}},
            },
            "beh": {"n": {"emit": [{"id": "check_ok", "per_min": 1.0, "scope": "global"}, {"id": "check_fail", "per_min": 0.02, "scope": "global"}]}, "f": {"emit": [{"id": "check_ok", "per_min": 1.0, "scope": "global"}, {"id": "check_fail", "per_min": 1.0, "scope": "global"}]}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "payment_charge_ok",
                    "rpm": 80.0,
                    "emit": ["api_gateway.gw_req_charge", "product_api.charge_start", "multipass_auth.auth_start", "multipass_auth.redis_txn_begin", "multipass_auth.redis_txn_commit", "multipass_auth.auth_ok", "product_api.charge_ok", "api_gateway.gw_resp_charge_200"],
                    "latency_ms": [[0, 1], [5, 20], [5, 20], [5, 25], [10, 40], [5, 25], [10, 60], [0, 1]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "merchant_login_ok_no2fa",
                    "rpm": 45.0,
                    "emit": ["api_gateway.gw_req_login", "product_api.login_start_no2fa", "multipass_auth.auth_start", "multipass_auth.redis_txn_begin", "multipass_auth.redis_txn_commit", "multipass_auth.auth_ok", "product_api.login_ok_no2fa", "api_gateway.gw_resp_login_200"],
                    "latency_ms": [[0, 1], [5, 20], [5, 20], [5, 25], [10, 40], [5, 25], [10, 80], [0, 1]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "merchant_login_2fa_ok",
                    "rpm": 10.0,
                    "emit": ["api_gateway.gw_req_login", "product_api.login_start_2fa", "multipass_auth.auth_start", "multipass_auth.redis_txn_begin", "multipass_auth.redis_txn_commit", "multipass_auth.auth_ok", "sms_gateway.sms_send_req", "sms_gateway.sms_send_ok", "product_api.login_ok_2fa", "api_gateway.gw_resp_login_200"],
                    "latency_ms": [[0, 1], [5, 25], [5, 25], [5, 30], [10, 60], [5, 30], [10, 80], [150, 1200], [10, 80], [0, 1]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "payment_charge_timeout",
                    "rpm": 65.0,
                    "emit": ["api_gateway.gw_req_charge", "product_api.charge_start", "multipass_auth.auth_start", "multipass_auth.redis_txn_begin", "multipass_auth.auth_timeout", "product_api.charge_fail_upstream_timeout", "api_gateway.gw_resp_charge_503"],
                    "latency_ms": [[0, 1], [10, 80], [10, 80], [10, 150], [2500, 8000], [10, 80], [0, 1]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "merchant_login_timeout",
                    "rpm": 30.0,
                    "emit": ["api_gateway.gw_req_login", "product_api.login_start_no2fa", "multipass_auth.auth_start", "multipass_auth.redis_txn_begin", "multipass_auth.auth_timeout", "product_api.login_fail_upstream_timeout", "api_gateway.gw_resp_login_503"],
                    "latency_ms": [[0, 1], [10, 80], [10, 80], [10, 150], [2500, 8000], [10, 80], [0, 1]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "payment_charge_ok_after_fix",
                    "rpm": 80.0,
                    "emit": ["api_gateway.gw_req_charge", "product_api.charge_start", "multipass_auth.auth_start", "multipass_auth.redis_txn_begin", "multipass_auth.redis_txn_commit", "multipass_auth.auth_ok", "product_api.charge_ok", "api_gateway.gw_resp_charge_200"],
                    "latency_ms": [[0, 1], [8, 40], [8, 40], [8, 50], [15, 80], [8, 45], [10, 70], [0, 1]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "merchant_login_ok_after_fix",
                    "rpm": 45.0,
                    "emit": ["api_gateway.gw_req_login", "product_api.login_start_no2fa", "multipass_auth.auth_start", "multipass_auth.redis_txn_begin", "multipass_auth.redis_txn_commit", "multipass_auth.auth_ok", "product_api.login_ok_no2fa", "api_gateway.gw_resp_login_200"],
                    "latency_ms": [[0, 1], [8, 40], [8, 40], [8, 50], [15, 80], [8, 45], [10, 90], [0, 1]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "merchant_login_2fa_sms_throttled",
                    "rpm": 12.0,
                    "emit": ["api_gateway.gw_req_login", "product_api.login_start_2fa", "multipass_auth.auth_start", "multipass_auth.redis_txn_begin", "multipass_auth.redis_txn_commit", "multipass_auth.auth_ok", "sms_gateway.sms_send_req", "sms_gateway.sms_send_throttled", "product_api.login_fail_2fa_delivery", "api_gateway.gw_resp_login_503"],
                    "latency_ms": [[0, 1], [8, 50], [8, 50], [8, 60], [15, 120], [8, 60], [10, 80], [200, 1500], [10, 80], [0, 1]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "alert_2791_widespread_outage_compressed"},
    "time": {"total_minutes": 60, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 60}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 25,
                    "rate_multipliers": {
                        "payment_charge_ok_after_fix": 0.0,
                        "merchant_login_ok_after_fix": 0.0,
                        "merchant_login_2fa_sms_throttled": 0.0,
                        "sms_gateway.vendor_throttle_metric": 0.0,
                        "synthetic_monitor.check_ok": 0.0,
                        "api_gateway.upstream_5xx_metric": 0.0,
                        "api_gateway.upstream_5xx_metric_high": 1.0,
                        "multipass_auth.timeout_metric": 0.0,
                        "multipass_auth.timeout_metric_high": 1.0,
                        "multipass_auth.redis_ops_metric": 0.0,
                        "multipass_auth.redis_ops_metric_high": 1.0,
                    },
                    "latency_multipliers": {"payment_charge_timeout": {"p50": 1.0, "p95": 1.0}, "merchant_login_timeout": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [{"ref": "roster_identity.deploy_started", "count": 1, "hosts": ["roster-wc-1"]}, {"ref": "synthetic_monitor.alert_opened", "count": 1, "hosts": ["extmon-1"]}],
                },
                {"order": 2, "at_min": 32, "rate_multipliers": {}, "latency_multipliers": {}, "one_shots": [{"ref": "roster_identity.rollback_completed", "count": 1, "hosts": ["roster-wc-1"]}, {"ref": "multipass_auth.restart", "count": 1, "hosts": ["mp-wc-1"]}, {"ref": "multipass_redis.monitor_attached", "count": 1, "hosts": ["redis-1"]}]},
                {
                    "order": 3,
                    "at_min": 45,
                    "rate_multipliers": {
                        "payment_charge_timeout": 0.2,
                        "merchant_login_timeout": 0.2,
                        "payment_charge_ok_after_fix": 1.0,
                        "merchant_login_ok_after_fix": 1.2,
                        "synthetic_monitor.check_fail": 0.0,
                        "synthetic_monitor.check_ok": 1.0,
                        "multipass_auth.timeout_metric_high": 0.0,
                        "multipass_auth.timeout_metric": 1.0,
                        "multipass_auth.redis_ops_metric_high": 0.0,
                        "multipass_auth.redis_ops_metric": 1.0,
                        "multipass_auth.redis_txn_retry": 0.1,
                        "multipass_redis.slowlog_entry": 0.2,
                        "multipass_redis.cpu_saturation_warn": 0.2,
                        "api_gateway.upstream_5xx_metric_high": 0.0,
                        "api_gateway.upstream_5xx_metric": 1.0,
                    },
                    "latency_multipliers": {"payment_charge_ok_after_fix": {"p50": 1.0, "p95": 1.0}, "merchant_login_ok_after_fix": {"p50": 1.0, "p95": 1.0}},
                    "one_shots": [{"ref": "multipass_auth.deploy_fix", "count": 1, "hosts": ["mp-wc-1"]}],
                },
                {"order": 4, "at_min": 52, "rate_multipliers": {"merchant_login_2fa_sms_throttled": 1.0, "sms_gateway.vendor_throttle_metric": 1.0}, "latency_multipliers": {"merchant_login_2fa_sms_throttled": {"p50": 1.0, "p95": 1.0}}, "one_shots": [{"ref": "sms_gateway.pool_add_numbers", "count": 1, "hosts": ["sms-1"]}]},
            ]
        }
    },
}

# -----------------------------
# Deterministic helpers
# -----------------------------
SEED = 1337
random.seed(SEED)
np.random.seed(SEED)

def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def u01(s: str) -> float:
    h = md5_hex(s)
    x = int(h[:16], 16)
    return (x % (10**12)) / float(10**12)

def stable_round(expected: float, salt: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    return base + (1 if u01(f"round:{salt}") < frac else 0)

def hex_n(s: str, n: int) -> str:
    return md5_hex(s)[:n]

def uuid4_like(s: str) -> str:
    h = hashlib.md5(s.encode("utf-8")).digest()
    b = bytearray(h)
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    u = uuid.UUID(bytes=bytes(b))
    return str(u)

def ip_from_cidr(cidr: str, salt: str) -> str:
    net, bits = cidr.split("/")
    bits = int(bits)
    parts = [int(x) for x in net.split(".")]
    base = (parts[0] << 24) | (parts[1] << 16) | (parts[2] << 8) | parts[3]
    host_bits = 32 - bits
    host_max = (1 << host_bits) - 1
    host = int(u01(f"ip:{salt}") * (host_max + 1)) & host_max
    ip_int = (base & (~host_max)) | host
    return ".".join(str((ip_int >> shift) & 0xFF) for shift in (24, 16, 8, 0))

def clamp_int(x: int, lo: int, hi: int) -> int:
    return lo if x < lo else hi if x > hi else x

NORMAL = NormalDist(0, 1)

def sample_lognormal_ms(p50: float, p95: float, salt: str) -> float:
    if p95 <= 0:
        return 0.0
    u = 0.35 + 0.6 * u01(f"lat_u:{salt}")
    if p50 <= 0:
        return max(0.0, u * p95)
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.6448536269514722
    z = NORMAL.inv_cdf(u)
    val = math.exp(mu + sigma * z)
    soft_cap = 3.0 * p95
    if val > soft_cap:
        excess = val - soft_cap
        val = soft_cap + 0.1 * excess
    return val

PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z0-9_]+)\}")

def isoformat_ms(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"

# -----------------------------
# Indices
# -----------------------------
COMPONENTS: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}
LOGS: Dict[str, Dict[str, Any]] = {}
for cid, comp in COMPONENTS.items():
    for lid, tmpl in comp.get("logs", {}).items():
        LOGS[f"{cid}.{lid}"] = {"component_id": cid, "log_id": lid, **tmpl}

FLOWS: Dict[str, Dict[str, Any]] = {"n": {f["id"]: f for f in SYSTEM["flows"]["n"]["req"]}, "f": {f["id"]: f for f in SYSTEM["flows"]["f"]["req"]}}

# -----------------------------
# Failure intervals (persistent controls)
# -----------------------------
@dataclass(frozen=True)
class FailureInterval:
    start_min: int
    end_min: int
    flow_rate_mult: Dict[str, float]
    bg_rate_mult: Dict[str, float]
    flow_latency_mult: Dict[str, Tuple[float, float]]  # flow_id -> (p50_mult, p95_mult)

def build_failure_intervals() -> List[FailureInterval]:
    fphase = SCENARIO["phases"]["f"]
    events = sorted(fphase["events"], key=lambda e: (e["at_min"], e.get("order", 0)))
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]

    cur_flow_rate: Dict[str, float] = {}
    cur_bg_rate: Dict[str, float] = {}
    cur_lat: Dict[str, Tuple[float, float]] = {}

    boundaries = [f_start] + [e["at_min"] for e in events if f_start <= e["at_min"] < f_end] + [f_end]
    seen = set()
    boundaries2 = []
    for b in boundaries:
        if b not in seen:
            boundaries2.append(b)
            seen.add(b)
    boundaries = boundaries2

    events_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        events_by_min.setdefault(e["at_min"], []).append(e)

    intervals: List[FailureInterval] = []
    for i in range(len(boundaries) - 1):
        start = boundaries[i]
        for ev in events_by_min.get(start, []):
            for k, v in ev.get("rate_multipliers", {}).items():
                if "." in k:
                    cur_bg_rate[k] = float(v)
                else:
                    cur_flow_rate[k] = float(v)
            for fid, lm in ev.get("latency_multipliers", {}).items():
                cur_lat[fid] = (float(lm.get("p50", 1.0)), float(lm.get("p95", 1.0)))

        end = boundaries[i + 1]
        intervals.append(FailureInterval(start_min=start, end_min=end, flow_rate_mult=dict(cur_flow_rate), bg_rate_mult=dict(cur_bg_rate), flow_latency_mult=dict(cur_lat)))
    return intervals

FAILURE_INTERVALS = build_failure_intervals()

# -----------------------------
# Variable generation/rendering
# -----------------------------
def choose_from_list(values: List[Any], salt: str) -> Any:
    if not values:
        return ""
    idx = int(u01(f"ch:{salt}") * len(values)) % len(values)
    return values[idx]

def gen_from_domain(domain: Dict[str, Any], var_name: str, salt: str, state: str, context: Dict[str, Any]) -> Any:
    k = domain.get("k")
    v = domain.get("v")
    if k == "uuid":
        return uuid4_like(f"uuid:{salt}")
    if k == "hex":
        ln = int(v) if v is not None else 16
        return hex_n(f"hex:{salt}", ln)
    if k == "ip":
        return ip_from_cidr(str(v), salt)
    if k == "ch":
        return choose_from_list(list(v), salt)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi < lo:
            lo, hi = hi, lo
        if lo == hi:
            return lo
        return lo + int(u01(f"i:{salt}") * (hi - lo + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        if hi < lo:
            lo, hi = hi, lo
        return lo + (hi - lo) * u01(f"f:{salt}")
    if k == "str":
        hint = str(v) if v is not None else ""
        if var_name == "key":
            merch = context.get("merchant_id") or choose_from_list(["m_1001", "m_1002", "m_1003", "m_1004"], f"{salt}:merch")
            prefix = "sess" if u01(f"keyp:{salt}") < 0.7 else "acct"
            return f"{prefix}:{merch}"
        if var_name == "msisdn":
            base = 14155550000
            offset = int(u01(f"msisdn:{salt}") * 9999)
            return f"+{base + offset:011d}"
        return hint.replace(" ", "_")[:24] + "_" + hex_n(f"str:{salt}", 8)
    return str(domain)

def template_domains(template: Dict[str, Any], state: str) -> Dict[str, Dict[str, Any]]:
    dom = dict(template.get("vars", {}) or {})
    sv = (template.get("state_vars", {}) or {}).get(state, {})
    for k, d in sv.items():
        dom[k] = d
    return dom

def render_message(template: Dict[str, Any], state: str, salt: str, forced: Dict[str, Any], derived: Dict[str, Any]) -> str:
    msg = template["msg"]
    needed = set(PLACEHOLDER_RE.findall(msg))
    doms = template_domains(template, state)
    context = dict(forced)
    context.update(derived)

    for key in sorted(needed):
        if key in context:
            continue
        if key in doms:
            context[key] = gen_from_domain(doms[key], key, f"{salt}:{key}", state, context)
        else:
            context[key] = hex_n(f"fallback:{salt}:{key}", 8)

    if template.get("msg", "").startswith("workers "):
        total = int(context.get("total", 200))
        busy = int(context.get("busy", 0))
        busy = max(0, min(busy, total))
        context["total"] = total
        context["busy"] = busy

    return msg.format(**context)

# -----------------------------
# Scheduling helpers
# -----------------------------
BASE_TIME = datetime(2017, 3, 16, 0, 0, 0, tzinfo=timezone.utc)

def minute_to_dt(minute: int) -> datetime:
    return BASE_TIME + timedelta(minutes=int(minute))

def schedule_times(start_dt: datetime, end_dt: datetime, count: int, salt: str) -> List[datetime]:
    if count <= 0:
        return []
    dur_s = (end_dt - start_dt).total_seconds()
    if dur_s <= 0:
        return [start_dt] * count
    spacing = dur_s / count
    jitter_cap = min(0.2 * spacing, 0.8)
    times: List[datetime] = []
    for i in range(count):
        base = (i + 0.5) / count * dur_s
        jitter = (u01(f"jit:{salt}:{i}") - 0.5) * 2.0 * jitter_cap
        t = start_dt + timedelta(seconds=base + jitter)
        if t < start_dt:
            t = start_dt
        if t >= end_dt:
            t = end_dt - timedelta(milliseconds=1)
        times.append(t)
    return times

# -----------------------------
# Emission mechanics
# -----------------------------
def component_identity(component_id: str) -> Tuple[str, List[str]]:
    comp = COMPONENTS[component_id]
    svc = comp.get("svc")
    service = "" if svc is None else str(svc)
    hosts = list(comp.get("hosts") or [])
    return service, hosts

def pick_host(component_id: str, salt: str, fixed_hosts: Optional[List[str]] = None) -> str:
    _service, hosts = component_identity(component_id)
    use_hosts = fixed_hosts if fixed_hosts is not None else hosts
    if not use_hosts:
        return ""
    return choose_from_list(use_hosts, f"host:{component_id}:{salt}")

def emit_row(rows: List[Dict[str, Any]], ts: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append({"timestamp": isoformat_ms(ts), "level": level, "message": message, "trace_id": trace_id, "service": service, "host": host})

def simulate_background_interval(rows: List[Dict[str, Any]], state: str, start_min: int, end_min: int, bg_rate_mult: Optional[Dict[str, float]] = None) -> None:
    bg_rate_mult = bg_rate_mult or {}
    start_dt = minute_to_dt(start_min)
    end_dt = minute_to_dt(end_min)
    minutes = max(0.0, (end_dt - start_dt).total_seconds() / 60.0)

    for comp_id, comp in COMPONENTS.items():
        beh = (comp.get("beh") or {}).get(state, {})
        for spec in beh.get("emit", []) or []:
            log_id = spec["id"]
            per_min = float(spec["per_min"])
            scope = spec.get("scope", "per_host")
            key = f"{comp_id}.{log_id}"
            mult = 1.0
            if state == "f":
                mult = float(bg_rate_mult.get(key, 1.0))
            eff = per_min * mult
            if eff <= 0 or minutes <= 0:
                continue

            service, hosts = component_identity(comp_id)
            tmpl = LOGS[key]

            if scope == "global":
                expected = eff * minutes
                cnt = stable_round(expected, f"bg:{state}:{key}:{start_min}-{end_min}")
                times = schedule_times(start_dt, end_dt, cnt, f"bg:{state}:{key}:{start_min}-{end_min}")
                for i, ts in enumerate(times):
                    host = hosts[i % len(hosts)] if hosts else ""
                    msg = render_message(tmpl, state, f"bg:{state}:{key}:{start_min}-{end_min}:{i}", forced={}, derived={})
                    emit_row(rows, ts, tmpl["lvl"], msg, "", service, host)
            else:
                for h in hosts:
                    expected = eff * minutes
                    cnt = stable_round(expected, f"bg:{state}:{key}:{h}:{start_min}-{end_min}")
                    times = schedule_times(start_dt, end_dt, cnt, f"bg:{state}:{key}:{h}:{start_min}-{end_min}")
                    for i, ts in enumerate(times):
                        msg = render_message(tmpl, state, f"bg:{state}:{key}:{h}:{start_min}-{end_min}:{i}", forced={}, derived={})
                        emit_row(rows, ts, tmpl["lvl"], msg, "", service, h)

def flow_latency_multiplier(flow_id: str, interval: Optional[FailureInterval]) -> Tuple[float, float]:
    if interval is None:
        return (1.0, 1.0)
    return interval.flow_latency_mult.get(flow_id, (1.0, 1.0))

def get_int_range_for_var(template: Dict[str, Any], state: str, var_name: str) -> Optional[Tuple[int, int]]:
    doms = template_domains(template, state)
    d = doms.get(var_name)
    if not d:
        return None
    if d.get("k") != "i":
        return None
    v = d.get("v")
    if not isinstance(v, (list, tuple)) or len(v) != 2:
        return None
    lo, hi = int(v[0]), int(v[1])
    if hi < lo:
        lo, hi = hi, lo
    return (lo, hi)

def simulate_flow_instances(rows: List[Dict[str, Any]], state: str, flow_id: str, start_times: List[datetime], interval: Optional[FailureInterval]) -> None:
    flow = FLOWS[state][flow_id]
    trace_on = bool(SYSTEM.get("tracing", {}).get("on", False)) and bool(flow.get("trace", False))
    p50m, p95m = flow_latency_multiplier(flow_id, interval)

    emit_refs = list(flow["emit"])
    nlogs = len(emit_refs)

    for idx, start_ts in enumerate(start_times):
        trace_id = hex_n(f"trace:{state}:{flow_id}:{idx}", 32) if trace_on else ""
        rid = uuid4_like(f"rid:{state}:{flow_id}:{idx}")

        merch = choose_from_list(["m_1001", "m_1002", "m_1003", "m_1004"], f"merch:{state}:{flow_id}:{idx}")
        user = choose_from_list(["u_2001", "u_2002", "u_2003", "u_2004"], f"user:{merch}:{state}:{flow_id}:{idx}")
        principal = "merchant" if u01(f"princ:{state}:{flow_id}:{idx}") < 0.88 else choose_from_list(["employee", "device"], f"princ2:{state}:{flow_id}:{idx}")
        amount_cents = 100 + int(u01(f"amt:{state}:{flow_id}:{idx}") * (15000 - 100 + 1))
        auth_code = hex_n(f"authc:{state}:{flow_id}:{idx}", 8)
        vendor_id = hex_n(f"vend:{state}:{flow_id}:{idx}", 12)
        pool_id = "pool-a"
        msisdn = gen_from_domain({"k": "str", "v": "E.164 phone number like +14155550123"}, "msisdn", f"msisdn:{merch}:{state}:{flow_id}:{idx}", state, {"merchant_id": merch})

        # Host stickiness per component in this chain
        comp_hosts: Dict[str, str] = {}
        for ref in emit_refs:
            cid, _lid = ref.split(".", 1)
            if cid not in comp_hosts:
                comp_hosts[cid] = pick_host(cid, f"{trace_id}:{rid}:{cid}")

        # Sample base per-log delays (ms) from latency hints (treated as p50/p95 hints).
        lat = list(flow.get("latency_ms") or [])
        if len(lat) < nlogs:
            lat = lat + [[0, 1]] * (nlogs - len(lat))
        elif len(lat) > nlogs:
            lat = lat[:nlogs]

        delays_ms: List[int] = []
        for li, (p50, p95) in enumerate(lat):
            p50s = float(p50) * p50m
            p95s = float(p95) * p95m
            dm = int(round(sample_lognormal_ms(p50s, p95s, f"lat:{state}:{flow_id}:{idx}:{li}")))
            if li == 0:
                dm = max(0, dm)
            else:
                dm = max(1, dm)
            delays_ms.append(dm)

        pos: Dict[str, int] = {ref: i for i, ref in enumerate(emit_refs)}
        min_delay: List[int] = [0] + [1] * (nlogs - 1)

        def sum_dur(a_idx: int, b_idx: int) -> int:
            if b_idx <= a_idx:
                return 0
            return int(sum(delays_ms[k] for k in range(a_idx + 1, b_idx + 1)))

        def enforce_constraint(anchor_ref: str, target_ref: str, var_name: str, add_idx: Optional[int] = None) -> Optional[Tuple[int, int, int, int, int]]:
            if anchor_ref not in pos or target_ref not in pos:
                return None
            a = pos[anchor_ref]
            b = pos[target_ref]
            if b <= a:
                return None
            rng = get_int_range_for_var(LOGS[target_ref], state, var_name)
            if rng is None:
                return None
            lo, hi = rng
            ai = b if add_idx is None else add_idx
            ai = clamp_int(ai, 0, nlogs - 1)
            return (a, b, lo, hi, ai)

        # Identify anchors (by presence in this flow's emit list)
        gw_req_ref = next((r for r in emit_refs if r.startswith("api_gateway.gw_req_")), None)
        gw_resp_ref = next((r for r in reversed(emit_refs) if r.startswith("api_gateway.gw_resp_")), None)

        auth_start_ref = "multipass_auth.auth_start" if "multipass_auth.auth_start" in pos else None
        auth_ok_ref = "multipass_auth.auth_ok" if "multipass_auth.auth_ok" in pos else None
        auth_timeout_ref = "multipass_auth.auth_timeout" if "multipass_auth.auth_timeout" in pos else None
        txn_begin_ref = "multipass_auth.redis_txn_begin" if "multipass_auth.redis_txn_begin" in pos else None
        txn_commit_ref = "multipass_auth.redis_txn_commit" if "multipass_auth.redis_txn_commit" in pos else None

        charge_start_ref = "product_api.charge_start" if "product_api.charge_start" in pos else None
        charge_ok_ref = "product_api.charge_ok" if "product_api.charge_ok" in pos else None
        charge_fail_ref = "product_api.charge_fail_upstream_timeout" if "product_api.charge_fail_upstream_timeout" in pos else None

        login_start_no2fa_ref = "product_api.login_start_no2fa" if "product_api.login_start_no2fa" in pos else None
        login_start_2fa_ref = "product_api.login_start_2fa" if "product_api.login_start_2fa" in pos else None
        login_ok_no2fa_ref = "product_api.login_ok_no2fa" if "product_api.login_ok_no2fa" in pos else None
        login_ok_2fa_ref = "product_api.login_ok_2fa" if "product_api.login_ok_2fa" in pos else None
        login_fail_timeout_ref = "product_api.login_fail_upstream_timeout" if "product_api.login_fail_upstream_timeout" in pos else None

        sms_req_ref = "sms_gateway.sms_send_req" if "sms_gateway.sms_send_req" in pos else None
        sms_ok_ref = "sms_gateway.sms_send_ok" if "sms_gateway.sms_send_ok" in pos else None

        # Build constraints and enforce them iteratively. Key idea:
        # - don't globally scale every latency step (which can break small internal dur_ms ranges),
        # - instead add/reduce time within the segment for the particular duration-bearing log.
        constraints: List[Tuple[int, int, int, int, int]] = []

        # Inner constraints first (single-step or tight bounds)
        if txn_begin_ref and txn_commit_ref:
            c = enforce_constraint(txn_begin_ref, txn_commit_ref, "dur_ms", add_idx=pos[txn_commit_ref])
            if c:
                constraints.append(c)
                min_delay[c[4]] = max(min_delay[c[4]], c[2])  # txn_commit is single-step here
        if sms_req_ref and sms_ok_ref:
            c = enforce_constraint(sms_req_ref, sms_ok_ref, "dur_ms", add_idx=pos[sms_ok_ref])
            if c:
                constraints.append(c)
                min_delay[c[4]] = max(min_delay[c[4]], c[2])  # sms_send_ok is single-step here
        if auth_start_ref and auth_ok_ref:
            c = enforce_constraint(auth_start_ref, auth_ok_ref, "dur_ms", add_idx=pos[auth_ok_ref])
            if c:
                constraints.append(c)

        # waited_ms for auth_timeout
        if auth_start_ref and auth_timeout_ref:
            c = enforce_constraint(auth_start_ref, auth_timeout_ref, "waited_ms", add_idx=pos[auth_timeout_ref])
            if c:
                constraints.append(c)

        # Product-layer durations
        if charge_start_ref and charge_ok_ref:
            c = enforce_constraint(charge_start_ref, charge_ok_ref, "dur_ms", add_idx=pos[charge_ok_ref])
            if c:
                constraints.append(c)
        if charge_start_ref and charge_fail_ref:
            c = enforce_constraint(charge_start_ref, charge_fail_ref, "dur_ms", add_idx=pos[charge_fail_ref])
            if c:
                constraints.append(c)

        if login_start_no2fa_ref and login_ok_no2fa_ref:
            c = enforce_constraint(login_start_no2fa_ref, login_ok_no2fa_ref, "dur_ms", add_idx=pos[login_ok_no2fa_ref])
            if c:
                constraints.append(c)
        if login_start_no2fa_ref and login_fail_timeout_ref:
            c = enforce_constraint(login_start_no2fa_ref, login_fail_timeout_ref, "dur_ms", add_idx=pos[login_fail_timeout_ref])
            if c:
                constraints.append(c)
        if login_start_2fa_ref and login_ok_2fa_ref:
            c = enforce_constraint(login_start_2fa_ref, login_ok_2fa_ref, "dur_ms", add_idx=pos[login_ok_2fa_ref])
            if c:
                constraints.append(c)

        # Gateway outer duration
        if gw_req_ref and gw_resp_ref:
            c = enforce_constraint(gw_req_ref, gw_resp_ref, "dur_ms", add_idx=pos[gw_resp_ref])
            if c:
                constraints.append(c)

        # Iterate to satisfy min/max, respecting per-delay lower bounds for single-step constraints.
        for _ in range(6):
            changed = False
            for (a, b, lo, hi, add_idx) in constraints:
                cur = sum_dur(a, b)
                if cur < lo:
                    need = lo - cur
                    delays_ms[add_idx] += need
                    changed = True
                elif cur > hi:
                    excess = cur - hi
                    idxs = list(range(a + 1, b + 1))
                    idxs.sort(key=lambda k: (delays_ms[k] - min_delay[k], delays_ms[k]), reverse=True)
                    for k in idxs:
                        avail = delays_ms[k] - min_delay[k]
                        if avail <= 0:
                            continue
                        take = min(avail, excess)
                        delays_ms[k] -= take
                        excess -= take
                        if excess <= 0:
                            break
                    if excess > 0:
                        # Could not fully reduce due to min bounds; leave slight max violation.
                        pass
                    else:
                        changed = True
            if not changed:
                break

        # Materialize timestamps
        times: List[datetime] = []
        t = start_ts
        for li in range(nlogs):
            t = t + timedelta(milliseconds=int(delays_ms[li]))
            times.append(t)

        def dt_ms(a: int, b: int) -> int:
            ms = int(round((times[b] - times[a]).total_seconds() * 1000.0))
            return max(ms, 0)

        # Derived durations injected into messages; these must agree with timestamps.
        derived_durations: Dict[str, Dict[str, Any]] = {}

        # Gateway dur_ms from ingress to response
        if gw_req_ref and gw_resp_ref:
            derived_durations[gw_resp_ref] = {"dur_ms": dt_ms(pos[gw_req_ref], pos[gw_resp_ref])}

        # Product durations
        if charge_start_ref and charge_ok_ref:
            derived_durations[charge_ok_ref] = {"dur_ms": dt_ms(pos[charge_start_ref], pos[charge_ok_ref])}
        if charge_start_ref and charge_fail_ref:
            derived_durations[charge_fail_ref] = {"dur_ms": dt_ms(pos[charge_start_ref], pos[charge_fail_ref])}

        if login_start_no2fa_ref and login_ok_no2fa_ref:
            derived_durations[login_ok_no2fa_ref] = {"dur_ms": dt_ms(pos[login_start_no2fa_ref], pos[login_ok_no2fa_ref])}
        if login_start_no2fa_ref and login_fail_timeout_ref:
            derived_durations[login_fail_timeout_ref] = {"dur_ms": dt_ms(pos[login_start_no2fa_ref], pos[login_fail_timeout_ref])}
        if login_start_2fa_ref and login_ok_2fa_ref:
            derived_durations[login_ok_2fa_ref] = {"dur_ms": dt_ms(pos[login_start_2fa_ref], pos[login_ok_2fa_ref])}

        # Auth/Redis durations
        if txn_begin_ref and txn_commit_ref:
            derived_durations[txn_commit_ref] = {"dur_ms": dt_ms(pos[txn_begin_ref], pos[txn_commit_ref])}
        if auth_start_ref and auth_ok_ref:
            derived_durations[auth_ok_ref] = {"dur_ms": dt_ms(pos[auth_start_ref], pos[auth_ok_ref])}

        # SMS duration
        if sms_req_ref and sms_ok_ref:
            derived_durations[sms_ok_ref] = {"dur_ms": dt_ms(pos[sms_req_ref], pos[sms_ok_ref])}

        # Auth timeout waited/timeout
        if auth_start_ref and auth_timeout_ref:
            waited = dt_ms(pos[auth_start_ref], pos[auth_timeout_ref])
            tmpl_timeout = LOGS["multipass_auth.auth_timeout"]
            dom = template_domains(tmpl_timeout, state)
            lo_to, hi_to = int(dom["timeout_ms"]["v"][0]), int(dom["timeout_ms"]["v"][1])
            lo_w, hi_w = int(dom["waited_ms"]["v"][0]), int(dom["waited_ms"]["v"][1])
            waited = clamp_int(waited, lo_w, hi_w)
            fac = 0.55 + 0.2 * u01(f"tofac:{state}:{flow_id}:{idx}")
            timeout_ms = int(round(waited * fac))
            timeout_ms = clamp_int(timeout_ms, lo_to, hi_to)
            if timeout_ms > waited:
                timeout_ms = waited
            derived_durations[auth_timeout_ref] = {"waited_ms": waited, "timeout_ms": timeout_ms}

        # Emit logs
        for li, ref in enumerate(emit_refs):
            tmpl = LOGS[ref]
            cid, _lid = ref.split(".", 1)
            service, _hosts = component_identity(cid)
            host = comp_hosts.get(cid, "")

            forced_vars: Dict[str, Any] = {"rid": rid}
            derived_vars: Dict[str, Any] = dict(derived_durations.get(ref, {}))

            needed = set(PLACEHOLDER_RE.findall(tmpl["msg"]))
            if "trace_id" in needed:
                forced_vars["trace_id"] = trace_id
            if "merchant_id" in needed:
                forced_vars["merchant_id"] = merch
            if "user_id" in needed:
                forced_vars["user_id"] = user
            if "principal" in needed:
                forced_vars["principal"] = principal
            if "amount_cents" in needed:
                forced_vars["amount_cents"] = amount_cents
            if "auth_code" in needed:
                forced_vars["auth_code"] = auth_code
            if "vendor_id" in needed:
                forced_vars["vendor_id"] = vendor_id
            if "pool_id" in needed:
                forced_vars["pool_id"] = pool_id
            if "msisdn" in needed:
                forced_vars["msisdn"] = msisdn
            if "key" in needed:
                forced_vars["merchant_id"] = merch
            if ref == "product_api.login_fail_2fa_delivery":
                forced_vars["status"] = 503

            msg = render_message(tmpl, state, f"flow:{state}:{flow_id}:{idx}:{li}", forced=forced_vars, derived=derived_vars)
            emit_row(rows, times[li], tmpl["lvl"], msg, trace_id if trace_on else "", service, host)

def simulate_flows_normal(rows: List[Dict[str, Any]]) -> None:
    p = SCENARIO["time"]["phases"]["n"]
    start_min, end_min = p["start_min"], p["end_min"]
    start_dt = minute_to_dt(start_min)
    end_dt = minute_to_dt(end_min)
    minutes = (end_dt - start_dt).total_seconds() / 60.0

    for flow_id, flow in FLOWS["n"].items():
        expected = float(flow["rpm"]) * minutes
        cnt = stable_round(expected, f"flow:n:{flow_id}:{start_min}-{end_min}")
        starts = schedule_times(start_dt, end_dt, cnt, f"flow:n:{flow_id}:{start_min}-{end_min}")
        simulate_flow_instances(rows, "n", flow_id, starts, interval=None)

def simulate_flows_failure(rows: List[Dict[str, Any]]) -> None:
    for interval in FAILURE_INTERVALS:
        start_min, end_min = interval.start_min, interval.end_min
        start_dt = minute_to_dt(start_min)
        end_dt = minute_to_dt(end_min)
        minutes = max(0.0, (end_dt - start_dt).total_seconds() / 60.0)
        if minutes <= 0:
            continue

        for flow_id, flow in FLOWS["f"].items():
            mult = float(interval.flow_rate_mult.get(flow_id, 1.0))
            rpm_eff = float(flow["rpm"]) * mult
            expected = rpm_eff * minutes
            cnt = stable_round(expected, f"flow:f:{flow_id}:{start_min}-{end_min}")
            if cnt <= 0:
                continue
            starts = schedule_times(start_dt, end_dt, cnt, f"flow:f:{flow_id}:{start_min}-{end_min}")
            simulate_flow_instances(rows, "f", flow_id, starts, interval=interval)

def emit_one_shots(rows: List[Dict[str, Any]]) -> None:
    events = sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e.get("order", 0)))
    for ev in events:
        at_min = int(ev["at_min"])
        base_dt = minute_to_dt(at_min)
        for ospec in ev.get("one_shots", []) or []:
            ref = ospec["ref"]
            count = int(ospec["count"])
            allowed_hosts = list(ospec.get("hosts") or [])
            tmpl = LOGS[ref]
            cid, _lid = ref.split(".", 1)
            service, _hosts = component_identity(cid)

            for i in range(count):
                jitter_ms = int(50 + u01(f"oneshot:{ref}:{at_min}:{i}") * 1500)
                ts = base_dt + timedelta(milliseconds=jitter_ms)
                host = allowed_hosts[i % len(allowed_hosts)] if allowed_hosts else pick_host(cid, f"oneshot:{ref}:{at_min}:{i}")
                msg = render_message(tmpl, "f", f"oneshot:{ref}:{at_min}:{i}", forced={}, derived={})
                emit_row(rows, ts, tmpl["lvl"], msg, "", service, host)

# -----------------------------
# Run simulation
# -----------------------------
def main() -> None:
    rows: List[Dict[str, Any]] = []

    n = SCENARIO["time"]["phases"]["n"]
    simulate_background_interval(rows, "n", n["start_min"], n["end_min"], bg_rate_mult=None)

    for interval in FAILURE_INTERVALS:
        simulate_background_interval(rows, "f", interval.start_min, interval.end_min, bg_rate_mult=interval.bg_rate_mult)

    simulate_flows_normal(rows)
    simulate_flows_failure(rows)

    emit_one_shots(rows)

    df = pd.DataFrame(rows, columns=["timestamp", "level", "message", "trace_id", "service", "host"])
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)

    if not (20000 <= len(df) <= 100000):
        raise RuntimeError(f"Row count {len(df)} outside required [20000,100000].")

    df.to_csv("logs.csv", index=False)

if __name__ == "__main__":
    main()
