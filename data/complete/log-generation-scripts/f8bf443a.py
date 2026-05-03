import math
import hashlib
import uuid
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# -----------------------------
# Embedded normalized model data
# -----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "use1_ddb_ec2_nlb_stack"},
    "components": {
        "customer_app": {
            "svc": "app",
            "hosts": ["app-1", "app-2", "app-3"],
            "logs": {
                "ddb_call_start": {
                    "lvl": "INFO",
                    "msg": "ddb_call start op={op} table={table} endpoint={endpoint} trace={trace_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["PutItem", "GetItem", "Query"]},
                        "table": {"k": "ch", "v": ["users", "orders", "sessions"]},
                        "endpoint": {"k": "ch", "v": ["dynamodb.us-east-1.amazonaws.com"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "dns_resolve_fail": {
                    "lvl": "ERROR",
                    "msg": "dns_resolve failed host={host} err={err} cache_ttl_s={ttl_s} trace={trace_id}",
                    "vars": {
                        "host": {"k": "ch", "v": ["dynamodb.us-east-1.amazonaws.com"]},
                        "err": {"k": "ch", "v": ["NO_ANSWER", "SERVFAIL"]},
                        "ttl_s": {"k": "i", "v": [0, 1200]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "ddb_call_ok": {
                    "lvl": "INFO",
                    "msg": "ddb_call ok status=200 op={op} duration_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["PutItem", "GetItem", "Query"]},
                        "dur_ms": {"k": "i", "v": [5, 800]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "retrying": {
                    "lvl": "WARN",
                    "msg": "retrying attempt={attempt} reason={reason} trace={trace_id}",
                    "vars": {
                        "attempt": {"k": "i", "v": [2, 3]},
                        "reason": {"k": "ch", "v": ["dns_fail", "http_503", "timeout"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "ec2_run_instances_req": {
                    "lvl": "INFO",
                    "msg": "run_instances start count={count} itype={itype} az={az} trace={trace_id}",
                    "vars": {
                        "count": {"k": "i", "v": [1, 3]},
                        "itype": {"k": "ch", "v": ["t3.medium", "m5.large", "c6i.large"]},
                        "az": {"k": "ch", "v": ["use1-az1", "use1-az2", "use1-az3"]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "ec2_run_instances_ok": {
                    "lvl": "INFO",
                    "msg": "run_instances ok instance_id={iid} duration_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "iid": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [200, 30000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "ec2_run_instances_err_unavailable": {
                    "lvl": "WARN",
                    "msg": "run_instances failed code=ServiceUnavailable duration_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "dur_ms": {"k": "i", "v": [200, 30000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "ec2_run_instances_err_capacity": {
                    "lvl": "WARN",
                    "msg": "run_instances failed code=InsufficientCapacity duration_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "dur_ms": {"k": "i", "v": [100, 20000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "ec2_run_instances_err_throttle": {
                    "lvl": "WARN",
                    "msg": "run_instances failed code=RequestLimitExceeded duration_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "dur_ms": {"k": "i", "v": [50, 8000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "nlb_connect_ok": {
                    "lvl": "INFO",
                    "msg": "nlb_connect ok lb={lb} rtt_ms={rtt_ms}",
                    "vars": {
                        "lb": {"k": "ch", "v": ["app-lb-a", "app-lb-b"]},
                        "rtt_ms": {"k": "i", "v": [1, 60]},
                    },
                },
                "nlb_connect_err": {
                    "lvl": "ERROR",
                    "msg": "nlb_connect failed lb={lb} err={err}",
                    "vars": {
                        "lb": {"k": "ch", "v": ["app-lb-a", "app-lb-b"]},
                        "err": {"k": "ch", "v": ["ECONNRESET", "ETIMEDOUT"]},
                    },
                },
                "app_metric": {
                    "lvl": "INFO",
                    "msg": "app_metrics ddb_err_rate={ddb_err} ec2_launch_err_rate={ec2_err} nlb_conn_err_rate={nlb_err}",
                    "vars": {
                        "ddb_err": {"k": "f", "v": [0.0, 1.0]},
                        "ec2_err": {"k": "f", "v": [0.0, 1.0]},
                        "nlb_err": {"k": "f", "v": [0.0, 1.0]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "app_metric", "per_min": 1.0, "scope": "global"}],
                "f": [{"id": "app_metric", "per_min": 1.0, "scope": "global"}],
            },
        },
        "route53_controlplane": {
            "svc": "dns",
            "hosts": ["route53-use1-1"],
            "logs": {
                "rrset_change": {
                    "lvl": "INFO",
                    "msg": "route53 change zone={zone} name={name} type=A records={records} ttl_s={ttl_s} action={action}",
                    "vars": {
                        "zone": {"k": "ch", "v": ["Z-DDB-USE1", "Z-NLB-USE1"]},
                        "name": {
                            "k": "ch",
                            "v": [
                                "dynamodb.us-east-1.amazonaws.com",
                                "nlb.app-lb-a.use1.amazonaws.com",
                                "nlb.app-lb-b.use1.amazonaws.com",
                            ],
                        },
                        "records": {"k": "i", "v": [0, 8]},
                        "ttl_s": {"k": "i", "v": [30, 60]},
                        "action": {"k": "ch", "v": ["UPSERT", "DELETE"]},
                    },
                }
            },
            "beh": {
                "n": [{"id": "rrset_change", "per_min": 0.4, "scope": "global"}],
                "f": [{"id": "rrset_change", "per_min": 0.8, "scope": "global"}],
            },
        },
        "ddb_dns_planner": {
            "svc": "ddb-dns-planner",
            "hosts": ["ddb-planner-1"],
            "logs": {
                "plan_generated": {
                    "lvl": "INFO",
                    "msg": "dns_plan_generated gen={gen} endpoints={endpoints} lbs={lbs}",
                    "vars": {
                        "gen": {"k": "i", "v": [980, 1150]},
                        "endpoints": {"k": "i", "v": [2, 3]},
                        "lbs": {"k": "i", "v": [20, 80]},
                    },
                }
            },
            "beh": {
                "n": [{"id": "plan_generated", "per_min": 1.0, "scope": "global"}],
                "f": [{"id": "plan_generated", "per_min": 1.0, "scope": "global"}],
            },
        },
        "ddb_dns_enactor": {
            "svc": "ddb-dns-enactor",
            "hosts": ["ddb-enactor-az1-1", "ddb-enactor-az2-1", "ddb-enactor-az3-1"],
            "logs": {
                "enact_start": {
                    "lvl": "INFO",
                    "msg": "enact_start enactor={enactor} gen={gen} prev_gen={prev_gen} endpoint={endpoint}",
                    "vars": {
                        "enactor": {"k": "ch", "v": ["az1", "az2", "az3"]},
                        "gen": {"k": "i", "v": [980, 1150]},
                        "prev_gen": {"k": "i", "v": [970, 1149]},
                        "endpoint": {
                            "k": "ch",
                            "v": [
                                "dynamodb.us-east-1.amazonaws.com",
                                "dynamodb-ipv6.us-east-1.amazonaws.com",
                                "dynamodb-fips.us-east-1.amazonaws.com",
                            ],
                        },
                    },
                },
                "route53_txn_conflict": {
                    "lvl": "WARN",
                    "msg": "route53_txn_conflict endpoint={endpoint} gen={gen} wait_ms={wait_ms}",
                    "vars": {
                        "endpoint": {
                            "k": "ch",
                            "v": [
                                "dynamodb.us-east-1.amazonaws.com",
                                "dynamodb-ipv6.us-east-1.amazonaws.com",
                                "dynamodb-fips.us-east-1.amazonaws.com",
                            ],
                        },
                        "gen": {"k": "i", "v": [980, 1150]},
                        "wait_ms": {"k": "i", "v": [50, 8000]},
                    },
                },
                "enact_ok": {
                    "lvl": "INFO",
                    "msg": "enact_ok endpoint={endpoint} gen={gen}",
                    "vars": {
                        "endpoint": {
                            "k": "ch",
                            "v": [
                                "dynamodb.us-east-1.amazonaws.com",
                                "dynamodb-ipv6.us-east-1.amazonaws.com",
                                "dynamodb-fips.us-east-1.amazonaws.com",
                            ],
                        },
                        "gen": {"k": "i", "v": [980, 1150]},
                    },
                },
                "cleanup_plans": {
                    "lvl": "INFO",
                    "msg": "cleanup_plans current_gen={gen} deleted={deleted}",
                    "vars": {
                        "gen": {"k": "i", "v": [980, 1150]},
                        "deleted": {"k": "i", "v": [0, 10]},
                    },
                },
                "plan_missing": {
                    "lvl": "ERROR",
                    "msg": "plan_missing gen={gen} endpoint={endpoint} action=skip",
                    "vars": {
                        "gen": {"k": "i", "v": [980, 1150]},
                        "endpoint": {
                            "k": "ch",
                            "v": [
                                "dynamodb.us-east-1.amazonaws.com",
                                "dynamodb-ipv6.us-east-1.amazonaws.com",
                                "dynamodb-fips.us-east-1.amazonaws.com",
                            ],
                        },
                    },
                },
                "automation_disabled": {
                    "lvl": "INFO",
                    "msg": "automation_disabled by={actor} reason={reason}",
                    "vars": {
                        "actor": {"k": "ch", "v": ["oncall"]},
                        "reason": {"k": "ch", "v": ["inconsistent_state", "manual_repair"]},
                    },
                },
                "manual_route53_upsert": {
                    "lvl": "INFO",
                    "msg": "manual_route53_upsert endpoint={endpoint} records={records}",
                    "vars": {
                        "endpoint": {"k": "ch", "v": ["dynamodb.us-east-1.amazonaws.com"]},
                        "records": {"k": "i", "v": [4, 8]},
                    },
                },
            },
            "beh": {
                "n": [
                    {"id": "enact_start", "per_min": 0.6, "scope": "global"},
                    {"id": "enact_ok", "per_min": 0.6, "scope": "global"},
                    {"id": "route53_txn_conflict", "per_min": 0.05, "scope": "global"},
                    {"id": "cleanup_plans", "per_min": 0.2, "scope": "global"},
                ],
                "f": [
                    {"id": "enact_start", "per_min": 0.8, "scope": "global"},
                    {"id": "enact_ok", "per_min": 0.3, "scope": "global"},
                    {"id": "route53_txn_conflict", "per_min": 0.4, "scope": "global"},
                    {"id": "cleanup_plans", "per_min": 0.2, "scope": "global"},
                    {"id": "plan_missing", "per_min": 0.1, "scope": "global"},
                ],
            },
        },
        "ddb_api": {
            "svc": "ddb-api",
            "hosts": ["ddb-api-1", "ddb-api-2"],
            "logs": {
                "http_req": {
                    "lvl": "INFO",
                    "msg": "ddb_api recv op={op} table={table} req_id={rid} trace={trace_id}",
                    "vars": {
                        "op": {"k": "ch", "v": ["PutItem", "GetItem", "Query"]},
                        "table": {"k": "ch", "v": ["users", "orders", "sessions"]},
                        "rid": {"k": "hex", "v": 16},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_200": {
                    "lvl": "INFO",
                    "msg": "ddb_api resp status=200 req_id={rid} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "rid": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [3, 900]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
            },
        },
        "ec2_api": {
            "svc": "ec2-api",
            "hosts": ["ec2-api-1", "ec2-api-2"],
            "logs": {
                "api_recv": {
                    "lvl": "INFO",
                    "msg": "ec2_api recv action=RunInstances req_id={req_id} trace={trace_id}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "trace_id": {"k": "hex", "v": 32}},
                },
                "api_resp_ok": {
                    "lvl": "INFO",
                    "msg": "ec2_api resp status=200 req_id={req_id} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [150, 30000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "api_resp_unavailable": {
                    "lvl": "WARN",
                    "msg": "ec2_api resp status=503 code=ServiceUnavailable req_id={req_id} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [150, 30000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "api_resp_insufficient": {
                    "lvl": "WARN",
                    "msg": "ec2_api resp status=400 code=InsufficientCapacity req_id={req_id} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [80, 20000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "api_resp_throttle": {
                    "lvl": "WARN",
                    "msg": "ec2_api resp status=429 code=RequestLimitExceeded req_id={req_id} dur_ms={dur_ms} trace={trace_id}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [30, 8000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
            },
        },
        "dwfm": {
            "svc": "dwfm",
            "hosts": ["dwfm-az1-1", "dwfm-az2-1", "dwfm-az3-1"],
            "logs": {
                "state_check_fail": {
                    "lvl": "ERROR",
                    "msg": "droplet_state_check failed droplet={droplet} err={err}",
                    "vars": {"droplet": {"k": "hex", "v": 8}, "err": {"k": "ch", "v": ["ddb_dns_fail", "ddb_timeout"]}},
                },
                "lease_timeout": {
                    "lvl": "WARN",
                    "msg": "droplet_lease expired droplet={droplet} age_s={age_s}",
                    "vars": {"droplet": {"k": "hex", "v": 8}, "age_s": {"k": "i", "v": [300, 1800]}},
                },
                "lease_recover": {
                    "lvl": "INFO",
                    "msg": "droplet_lease acquired droplet={droplet} lease_id={lease} dur_ms={dur_ms}",
                    "vars": {
                        "droplet": {"k": "hex", "v": 8},
                        "lease": {"k": "uuid", "v": None},
                        "dur_ms": {"k": "i", "v": [50, 20000]},
                    },
                },
                "queue_depth": {
                    "lvl": "WARN",
                    "msg": "dwfm_queue depth={depth} oldest_age_s={old_s}",
                    "vars": {"depth": {"k": "i", "v": [0, 60000]}, "old_s": {"k": "i", "v": [0, 3600]}},
                },
                "throttle_enabled": {"lvl": "INFO", "msg": "throttle_enabled limit_rps={rps} scope=run_instances", "vars": {"rps": {"k": "i", "v": [5, 50]}}},
                "host_restart": {
                    "lvl": "WARN",
                    "msg": "dwfm_host_restart host={host} mode={mode}",
                    "vars": {
                        "host": {"k": "ch", "v": ["dwfm-az1-1", "dwfm-az2-1", "dwfm-az3-1"]},
                        "mode": {"k": "ch", "v": ["selective", "rolling"]},
                    },
                },
            },
            "beh": {
                "n": [{"id": "queue_depth", "per_min": 0.3}],  # scope omitted => per_host
                "f": [{"id": "state_check_fail", "per_min": 2.0}, {"id": "lease_timeout", "per_min": 0.2}, {"id": "queue_depth", "per_min": 0.5}, {"id": "lease_recover", "per_min": 1.0}],
            },
        },
        "network_manager": {
            "svc": "netmgr",
            "hosts": ["netmgr-1"],
            "logs": {
                "netprop_metric": {"lvl": "INFO", "msg": "netprop_metrics lag_s={lag_s} backlog={backlog}", "vars": {"lag_s": {"k": "i", "v": [0, 1800]}, "backlog": {"k": "i", "v": [0, 60000]}}},
                "netprop_enqueue": {
                    "lvl": "INFO",
                    "msg": "netprop_enqueue instance_id={iid} backlog={backlog} trace={trace_id}",
                    "vars": {"iid": {"k": "uuid", "v": None}, "backlog": {"k": "i", "v": [0, 60000]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "netprop_lag_high": {"lvl": "WARN", "msg": "netprop_lag_high lag_s={lag_s} backlog={backlog}", "vars": {"lag_s": {"k": "i", "v": [300, 1800]}, "backlog": {"k": "i", "v": [5000, 60000]}}},
            },
            "beh": {"n": [{"id": "netprop_metric", "per_min": 1.0, "scope": "global"}], "f": [{"id": "netprop_metric", "per_min": 1.0, "scope": "global"}, {"id": "netprop_lag_high", "per_min": 0.6, "scope": "global"}]},
        },
        "nlb_controlplane": {
            "svc": "nlb",
            "hosts": ["nlbcp-1"],
            "logs": {
                "hc_result": {
                    "lvl": "INFO",
                    "msg": "nlb_healthcheck lb={lb} az={az} result={result} rtt_ms={rtt_ms}",
                    "vars": {"lb": {"k": "ch", "v": ["app-lb-a", "app-lb-b"]}, "az": {"k": "ch", "v": ["use1-az1", "use1-az2", "use1-az3"]}, "result": {"k": "ch", "v": ["pass", "fail"]}, "rtt_ms": {"k": "i", "v": [1, 800]}},
                },
                "az_failover": {"lvl": "WARN", "msg": "nlb_az_failover lb={lb} az={az} removed_pct={pct}", "vars": {"lb": {"k": "ch", "v": ["app-lb-a", "app-lb-b"]}, "az": {"k": "ch", "v": ["use1-az1", "use1-az2", "use1-az3"]}, "pct": {"k": "i", "v": [10, 60]}}},
                "failover_disabled": {"lvl": "INFO", "msg": "nlb_failover_disabled by={actor}", "vars": {"actor": {"k": "ch", "v": ["oncall"]}}},
                "failover_enabled": {"lvl": "INFO", "msg": "nlb_failover_enabled by={actor}", "vars": {"actor": {"k": "ch", "v": ["oncall"]}}},
            },
            "beh": {"n": [{"id": "hc_result", "per_min": 2.0, "scope": "global"}, {"id": "az_failover", "per_min": 0.02, "scope": "global"}], "f": [{"id": "hc_result", "per_min": 2.0, "scope": "global"}, {"id": "az_failover", "per_min": 0.5, "scope": "global"}]},
        },
    },
    "tracing": {"on": True, "origins": ["customer_app"], "trace_id": {"k": "hex", "v": 32}},
    "flows": {
        "n": {
            "ddb_api_call": {"rpm": 120.0, "emit": ["customer_app.ddb_call_start", "ddb_api.http_req", "ddb_api.http_200", "customer_app.ddb_call_ok"], "latency_ms": [[2, 10], [5, 30], [3, 80], [1, 10]], "retry": {"max_attempts": 3, "expected_attempts": 1.05, "emit_per_retry": ["customer_app.retrying"], "backoff_ms": [[50, 200], [120, 500]]}, "trace": True},
            "ec2_run_instances": {
                "rpm": 6.0,
                "emit": ["customer_app.ec2_run_instances_req", "ec2_api.api_recv", "dwfm.lease_recover", "network_manager.netprop_enqueue", "ec2_api.api_resp_ok", "customer_app.ec2_run_instances_ok"],
                "latency_ms": [[2, 10], [5, 30], [30, 200], [10, 80], [100, 800], [1, 10]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": True,
            },
            "nlb_tcp_connect_ok": {"rpm": 60.0, "emit": ["customer_app.nlb_connect_ok"], "latency_ms": [[1, 20]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
        },
        "f": {
            "ddb_api_call_dns_fail": {"rpm": 120.0, "emit": ["customer_app.ddb_call_start", "customer_app.dns_resolve_fail"], "latency_ms": [[2, 10], [1, 20]], "retry": {"max_attempts": 3, "expected_attempts": 2.0, "emit_per_retry": ["customer_app.retrying"], "backoff_ms": [[100, 800], [250, 1500]]}, "trace": True},
            "ddb_api_call_ok_f": {"rpm": 120.0, "emit": ["customer_app.ddb_call_start", "ddb_api.http_req", "ddb_api.http_200", "customer_app.ddb_call_ok"], "latency_ms": [[2, 10], [6, 40], [4, 120], [1, 10]], "retry": {"max_attempts": 3, "expected_attempts": 1.05, "emit_per_retry": ["customer_app.retrying"], "backoff_ms": [[50, 300], [120, 600]]}, "trace": True},
            "ec2_run_instances_service_unavailable": {"rpm": 6.0, "emit": ["customer_app.ec2_run_instances_req", "ec2_api.api_recv", "ec2_api.api_resp_unavailable", "customer_app.ec2_run_instances_err_unavailable"], "latency_ms": [[2, 10], [5, 30], [200, 6000], [1, 10]], "retry": {"max_attempts": 3, "expected_attempts": 1.2, "emit_per_retry": ["customer_app.retrying"], "backoff_ms": [[200, 1500], [500, 2500]]}, "trace": True},
            "ec2_run_instances_insufficient_capacity": {"rpm": 6.0, "emit": ["customer_app.ec2_run_instances_req", "ec2_api.api_recv", "ec2_api.api_resp_insufficient", "customer_app.ec2_run_instances_err_capacity"], "latency_ms": [[2, 10], [5, 30], [80, 2500], [1, 10]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            "ec2_run_instances_throttled": {"rpm": 4.0, "emit": ["customer_app.ec2_run_instances_req", "ec2_api.api_recv", "ec2_api.api_resp_throttle", "customer_app.ec2_run_instances_err_throttle"], "latency_ms": [[2, 10], [5, 30], [30, 1200], [1, 10]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": True},
            "nlb_tcp_connect_ok_f": {"rpm": 60.0, "emit": ["customer_app.nlb_connect_ok"], "latency_ms": [[1, 30]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
            "nlb_tcp_connect_err_f": {"rpm": 15.0, "emit": ["customer_app.nlb_connect_err"], "latency_ms": [[1, 80]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "use1_ddb_dns_race_cascade"},
    "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 18}, "f": {"start_min": 18, "end_min": 50}}},
    "events": [
        {
            "order": 1,
            "at_min": 18,
            "rate_multipliers": {
                "ddb_api_call_ok_f": 0.0,
                "ec2_run_instances_insufficient_capacity": 0.0,
                "ec2_run_instances_throttled": 0.0,
                "nlb_tcp_connect_err_f": 0.0,
                "ddb_dns_enactor.plan_missing": 0.0,
                "dwfm.lease_recover": 0.0,
                "network_manager.netprop_lag_high": 0.0,
                "nlb_controlplane.az_failover": 0.0,
                "route53_controlplane.rrset_change": 2.0,
                "ddb_dns_enactor.route53_txn_conflict": 2.5,
                "ddb_dns_enactor.cleanup_plans": 2.0,
            },
            "latency_multipliers": {"ddb_api_call_dns_fail": {"p50": 1.0, "p95": 1.0}, "ec2_run_instances_service_unavailable": {"p50": 1.4, "p95": 1.8}},
            "one_shots": [],
        },
        {
            "order": 2,
            "at_min": 26,
            "rate_multipliers": {"ddb_dns_enactor.plan_missing": 3.0, "ddb_dns_enactor.route53_txn_conflict": 3.5, "dwfm.lease_timeout": 4.0, "ec2_run_instances_service_unavailable": 1.3, "route53_controlplane.rrset_change": 1.5},
            "latency_multipliers": {"ddb_api_call_dns_fail": {"p50": 1.2, "p95": 1.6}, "ec2_run_instances_service_unavailable": {"p50": 2.0, "p95": 2.5}},
            "one_shots": [],
        },
        {
            "order": 3,
            "at_min": 34,
            "rate_multipliers": {
                "ddb_api_call_dns_fail": 0.3,
                "ddb_api_call_ok_f": 0.7,
                "ec2_run_instances_service_unavailable": 0.2,
                "ec2_run_instances_insufficient_capacity": 1.0,
                "dwfm.lease_recover": 1.0,
                "dwfm.queue_depth": 2.5,
                "route53_controlplane.rrset_change": 0.6,
                "ddb_dns_enactor.enact_start": 0.0,
                "ddb_dns_enactor.enact_ok": 0.0,
                "ddb_dns_enactor.cleanup_plans": 0.0,
                "ddb_dns_enactor.route53_txn_conflict": 0.0,
                "ddb_dns_enactor.plan_missing": 0.0,
            },
            "latency_multipliers": {"ddb_api_call_ok_f": {"p50": 1.1, "p95": 1.2}, "ec2_run_instances_insufficient_capacity": {"p50": 1.3, "p95": 1.5}},
            "one_shots": [{"ref": "ddb_dns_enactor.automation_disabled", "count": 1, "hosts": ["ddb-enactor-az2-1"]}, {"ref": "ddb_dns_enactor.manual_route53_upsert", "count": 1, "hosts": ["ddb-enactor-az2-1"]}],
        },
        {"order": 4, "at_min": 40, "rate_multipliers": {"ddb_api_call_dns_fail": 0.05, "ddb_api_call_ok_f": 0.95, "ec2_run_instances_service_unavailable": 0.0, "route53_controlplane.rrset_change": 0.4}, "latency_multipliers": {"ddb_api_call_ok_f": {"p50": 1.0, "p95": 1.0}}, "one_shots": []},
        {
            "order": 5,
            "at_min": 42,
            "rate_multipliers": {
                "dwfm.lease_recover": 2.0,
                "dwfm.queue_depth": 1.0,
                "ec2_run_instances_insufficient_capacity": 0.5,
                "ec2_run_instances_throttled": 1.0,
                "network_manager.netprop_lag_high": 1.0,
                "nlb_controlplane.az_failover": 2.0,
                "route53_controlplane.rrset_change": 2.0,
                "nlb_tcp_connect_ok_f": 0.8,
                "nlb_tcp_connect_err_f": 1.0,
            },
            "latency_multipliers": {"ec2_run_instances_throttled": {"p50": 1.2, "p95": 1.4}, "nlb_tcp_connect_err_f": {"p50": 1.0, "p95": 1.0}},
            "one_shots": [{"ref": "dwfm.throttle_enabled", "count": 1, "hosts": ["dwfm-az1-1"]}, {"ref": "dwfm.host_restart", "count": 2, "hosts": ["dwfm-az1-1", "dwfm-az2-1"]}],
        },
    ],
}

# -----------------------------
# Deterministic helpers
# -----------------------------

SEED = 1337
random.seed(SEED)
np.random.seed(SEED)


def _sha256_bytes(s: str) -> bytes:
    return hashlib.sha256(f"{SEED}|{s}".encode("utf-8")).digest()


def u01(key: str) -> float:
    b = _sha256_bytes(key)
    x = int.from_bytes(b[:8], "big")
    return (x + 0.5) / 2**64


def hex_n(key: str, n: int) -> str:
    return _sha256_bytes(key).hex()[:n]


def uuid_from_key(key: str) -> str:
    b = bytearray(_sha256_bytes(key)[:16])
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(b)))


def normal_ppf(p: float) -> float:
    p = min(max(p, 1e-12), 1 - 1e-12)
    a = [-3.969683028665376e01, 2.209460984245205e02, -2.759285104469687e02, 1.383577518672690e02, -3.066479806614716e01, 2.506628277459239e00]
    b = [-5.447609879822406e01, 1.615858368580409e02, -1.556989798598866e02, 6.680131188771972e01, -1.328068155288572e01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e00, -2.549732539343734e00, 4.374664141464968e00, 2.938163982698783e00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e00, 3.754408661907416e00]
    plow = 0.02425
    phigh = 1 - plow
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return num / den
    if p > phigh:
        q = math.sqrt(-2 * math.log(1 - p))
        num = (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5])
        den = ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1.0)
        return -num / den
    q = p - 0.5
    r = q * q
    num = (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q
    den = (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1.0)
    return num / den


def sample_lognormal_from_p50_p95(p50: float, p95: float, u: float, cap_mult: float = 2.5) -> float:
    p50 = max(float(p50), 1e-6)
    p95 = max(float(p95), p50 * 1.0001)
    mu = math.log(p50)
    sigma = math.log(p95 / p50) / 1.645
    z = normal_ppf(u)
    x = math.exp(mu + sigma * z)
    cap = cap_mult * p95
    return min(max(x, 0.0), cap)


def choose_from(values: List[Any], key: str) -> Any:
    if not values:
        return ""
    idx = int(math.floor(u01(key) * len(values)))
    if idx >= len(values):
        idx = len(values) - 1
    return values[idx]


def sample_int(lo: int, hi: int, key: str) -> int:
    lo, hi = int(lo), int(hi)
    if hi < lo:
        lo, hi = hi, lo
    if lo == hi:
        return lo
    u = u01(key)
    return lo + int(math.floor(u * (hi - lo + 1)))


def sample_float(lo: float, hi: float, key: str) -> float:
    lo, hi = float(lo), float(hi)
    if hi < lo:
        lo, hi = hi, lo
    return lo + u01(key) * (hi - lo)


def fmt_float(x: float) -> str:
    return f"{x:.3f}"


def iso_utc_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    ms = int(dt.microsecond / 1000)
    return dt.replace(microsecond=ms * 1000).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


# -----------------------------
# Indices
# -----------------------------

COMP = SYSTEM["components"]

LOG_TEMPL: Dict[str, Dict[str, Any]] = {}
for cid, c in COMP.items():
    for lid, lt in c.get("logs", {}).items():
        LOG_TEMPL[f"{cid}.{lid}"] = lt

FLOWS = SYSTEM["flows"]


def get_comp_host(comp_id: str, key: str) -> str:
    hosts = COMP[comp_id].get("hosts", []) or []
    if not hosts:
        return ""
    return choose_from(hosts, key)


def get_svc(comp_id: str) -> str:
    return COMP[comp_id].get("svc", "") or ""


def int_var_bounds(ref: str, var: str) -> Optional[Tuple[int, int]]:
    tmpl = LOG_TEMPL.get(ref)
    if not tmpl:
        return None
    spec = (tmpl.get("vars", {}) or {}).get(var)
    if not spec or spec.get("k") != "i":
        return None
    v = spec.get("v")
    if not isinstance(v, list) or len(v) != 2:
        return None
    return int(v[0]), int(v[1])


# -----------------------------
# Control state derivation
# -----------------------------

def build_failure_intervals() -> List[Dict[str, Any]]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["events"], key=lambda e: (e["at_min"], e.get("order", 0)))

    flow_mult: Dict[str, float] = {}
    bg_mult: Dict[str, float] = {}
    lat_mult: Dict[str, Tuple[float, float]] = {}

    def set_rate(k: str, v: float) -> None:
        if "." in k:
            bg_mult[k] = float(v)
        else:
            flow_mult[k] = float(v)

    def set_lat(fid: str, p50: float, p95: float) -> None:
        lat_mult[fid] = (float(p50), float(p95))

    intervals: List[Dict[str, Any]] = []
    for i, ev in enumerate(events):
        for k, v in (ev.get("rate_multipliers") or {}).items():
            set_rate(k, v)
        for fid, mults in (ev.get("latency_multipliers") or {}).items():
            set_lat(fid, mults.get("p50", 1.0), mults.get("p95", 1.0))

        start = int(ev["at_min"])
        end = int(events[i + 1]["at_min"]) if i + 1 < len(events) else int(f_end)

        intervals.append({"start_min": start, "end_min": end, "flow_mult": dict(flow_mult), "bg_mult": dict(bg_mult), "lat_mult": dict(lat_mult), "one_shots": list(ev.get("one_shots") or [])})
    intervals = [iv for iv in intervals if iv["end_min"] > iv["start_min"] and iv["end_min"] > f_start and iv["start_min"] < f_end]
    return intervals


FAILURE_INTERVALS = build_failure_intervals()

# -----------------------------
# Deterministic count allocation
# -----------------------------

class CarryRounding:
    def __init__(self) -> None:
        self.carry: Dict[str, float] = {}

    def alloc(self, key: str, expected: float) -> int:
        c = self.carry.get(key, 0.0)
        x = expected + c
        n = int(math.floor(x + 1e-12))
        self.carry[key] = x - n
        if n < 0:
            n = 0
        return n


# -----------------------------
# Variable binding/rendering
# -----------------------------

def render_from_template(ref: str, overrides: Dict[str, Any], key_prefix: str) -> Tuple[str, str]:
    tmpl = LOG_TEMPL[ref]
    msg = tmpl["msg"]
    vars_spec = tmpl.get("vars", {}) or {}
    vals: Dict[str, Any] = {}
    for k, spec in vars_spec.items():
        if k in overrides:
            vals[k] = overrides[k]
            continue
        sk = spec.get("k")
        sv = spec.get("v")
        kk = f"{key_prefix}|{ref}|{k}"
        if sk == "ch":
            vals[k] = choose_from(list(sv), kk)
        elif sk == "i":
            lo, hi = int(sv[0]), int(sv[1])
            vals[k] = sample_int(lo, hi, kk)
        elif sk == "f":
            lo, hi = float(sv[0]), float(sv[1])
            vals[k] = fmt_float(sample_float(lo, hi, kk))
        elif sk == "uuid":
            vals[k] = uuid_from_key(kk)
        elif sk == "hex":
            vals[k] = hex_n(kk, int(sv))
        else:
            vals[k] = ""
    return tmpl["lvl"], msg.format(**vals)


# -----------------------------
# Timing sampling/planning per flow attempt
# -----------------------------

def sample_latency_ms(pair: List[float], mult_p50: float, mult_p95: float, key: str) -> int:
    p50 = float(pair[0]) * float(mult_p50)
    p95 = float(pair[1]) * float(mult_p95)
    u = u01(key)
    # cap_mult lowered to reduce unrealistic per-step spikes; additional per-flow caps enforced below
    x = sample_lognormal_from_p50_p95(p50, p95, u, cap_mult=2.0)
    return max(1, int(round(x)))


def clamp_int(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, x))


def reduce_from_end(lat: List[int], idxs: List[int], reduce_by: int, min_lat: List[int]) -> None:
    if reduce_by <= 0:
        return
    for i in reversed(idxs):
        floor = max(1, int(min_lat[i]) if i < len(min_lat) else 1)
        avail = lat[i] - floor
        if avail <= 0:
            continue
        d = min(avail, reduce_by)
        lat[i] -= d
        reduce_by -= d
        if reduce_by <= 0:
            return


def enforce_gap(lat: List[int], min_lat: List[int], idx: int, lo: int, hi: int) -> None:
    if idx < 0 or idx >= len(lat):
        return
    if lo > hi:
        lo, hi = hi, lo
    if lo < 1:
        lo = 1
    if hi < 1:
        hi = 1
    min_lat[idx] = max(min_lat[idx], lo)
    if lat[idx] < lo:
        lat[idx] = lo
    if lat[idx] > hi:
        lat[idx] = hi
    if min_lat[idx] > lat[idx]:
        min_lat[idx] = lat[idx]


def enforce_sum(lat: List[int], min_lat: List[int], idxs: List[int], lo: int, hi: int) -> None:
    idxs = [i for i in idxs if 0 <= i < len(lat)]
    if not idxs:
        return
    if lo > hi:
        lo, hi = hi, lo
    if lo < 0:
        lo = 0
    total = sum(lat[i] for i in idxs)
    if total < lo:
        lat[idxs[-1]] += (lo - total)
        total = lo
    if total > hi:
        reduce_from_end(lat, idxs, total - hi, min_lat)


def plan_attempt_latencies(flow_id: str, emit_refs: List[str], base_latency_pairs: List[List[float]], lat_mult_pair: Tuple[float, float], key_prefix: str) -> List[int]:
    mp50, mp95 = lat_mult_pair
    lat = [sample_latency_ms(base_latency_pairs[i], mp50, mp95, f"{key_prefix}|{flow_id}|lat|{i}") for i in range(len(base_latency_pairs))]
    min_lat = [1 for _ in lat]

    # Lower-bound nudge for some flows (existing behavior)
    if flow_id in ("ec2_run_instances",):
        lat[2] = max(lat[2], 50)
        enforce_sum(lat, min_lat, [2, 3, 4], 150, 10**9)
        enforce_sum(lat, min_lat, [1, 2, 3, 4, 5], 200, 10**9)
    if flow_id in ("ec2_run_instances_insufficient_capacity",):
        enforce_sum(lat, min_lat, [1, 2, 3], 100, 10**9)
    if flow_id in ("ec2_run_instances_throttled",):
        enforce_sum(lat, min_lat, [1, 2, 3], 50, 10**9)
    if flow_id in ("ddb_api_call", "ddb_api_call_ok_f"):
        enforce_sum(lat, min_lat, [1, 2, 3], 5, 10**9)

    # Upper/lower coherence constraints so message-carried durations match timestamp gaps without clamping.
    # Indices are latency_ms elements (gap to each emitted log from previous emitted log).
    if flow_id in ("ddb_api_call", "ddb_api_call_ok_f"):
        b = int_var_bounds("ddb_api.http_200", "dur_ms") or (3, 900)
        enforce_gap(lat, min_lat, 2, b[0], b[1])  # http_req -> http_200
        b2 = int_var_bounds("customer_app.ddb_call_ok", "dur_ms") or (5, 800)
        enforce_sum(lat, min_lat, [1, 2, 3], b2[0], b2[1])  # start -> ok
    elif flow_id == "ec2_run_instances":
        b_lease = int_var_bounds("dwfm.lease_recover", "dur_ms") or (50, 20000)
        enforce_gap(lat, min_lat, 2, b_lease[0], b_lease[1])  # api_recv -> lease_recover
        b_api = int_var_bounds("ec2_api.api_resp_ok", "dur_ms") or (150, 30000)
        enforce_sum(lat, min_lat, [2, 3, 4], b_api[0], b_api[1])  # api_recv -> api_resp_ok
        b_cli = int_var_bounds("customer_app.ec2_run_instances_ok", "dur_ms") or (200, 30000)
        enforce_sum(lat, min_lat, [1, 2, 3, 4, 5], b_cli[0], b_cli[1])  # client start -> ok
    elif flow_id == "ec2_run_instances_service_unavailable":
        b_api = int_var_bounds("ec2_api.api_resp_unavailable", "dur_ms") or (150, 30000)
        enforce_gap(lat, min_lat, 2, b_api[0], b_api[1])  # api_recv -> api_resp_unavailable
        b_cli = int_var_bounds("customer_app.ec2_run_instances_err_unavailable", "dur_ms") or (200, 30000)
        enforce_sum(lat, min_lat, [1, 2, 3], b_cli[0], b_cli[1])  # client start -> err
    elif flow_id == "ec2_run_instances_insufficient_capacity":
        b_api = int_var_bounds("ec2_api.api_resp_insufficient", "dur_ms") or (80, 20000)
        enforce_gap(lat, min_lat, 2, b_api[0], b_api[1])  # api_recv -> api_resp_insufficient
        b_cli = int_var_bounds("customer_app.ec2_run_instances_err_capacity", "dur_ms") or (100, 20000)
        enforce_sum(lat, min_lat, [1, 2, 3], b_cli[0], b_cli[1])  # client start -> err
    elif flow_id == "ec2_run_instances_throttled":
        b_api = int_var_bounds("ec2_api.api_resp_throttle", "dur_ms") or (30, 8000)
        enforce_gap(lat, min_lat, 2, b_api[0], b_api[1])  # api_recv -> api_resp_throttle
        b_cli = int_var_bounds("customer_app.ec2_run_instances_err_throttle", "dur_ms") or (50, 8000)
        enforce_sum(lat, min_lat, [1, 2, 3], b_cli[0], b_cli[1])  # client start -> err
    elif flow_id in ("nlb_tcp_connect_ok", "nlb_tcp_connect_ok_f"):
        b = int_var_bounds("customer_app.nlb_connect_ok", "rtt_ms") or (1, 60)
        enforce_gap(lat, min_lat, 0, b[0], b[1])

    # Final guard: keep latencies positive integers.
    for i in range(len(lat)):
        if lat[i] < 1:
            lat[i] = 1
    return lat


def sample_backoff_ms(pair: List[float], mult_p50: float, mult_p95: float, key: str) -> int:
    p50 = float(pair[0]) * float(mult_p50)
    p95 = float(pair[1]) * float(mult_p95)
    u = u01(key)
    x = sample_lognormal_from_p50_p95(p50, p95, u, cap_mult=3.0)
    return max(1, int(round(x)))


def pick_attempts(expected: float, max_attempts: int, key: str) -> int:
    expected = float(expected)
    max_attempts = int(max_attempts)
    a = int(math.floor(expected + 1e-12))
    a = max(1, min(a, max_attempts))
    b = min(max_attempts, a + 1)
    frac = expected - math.floor(expected + 1e-12)
    if b == a:
        return a
    return b if u01(key) < frac else a


# -----------------------------
# Background variable overrides
# -----------------------------

BASE_TIME = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)


def minute_of(dt: datetime) -> float:
    return (dt - BASE_TIME).total_seconds() / 60.0


def az_from_host(host: str) -> str:
    if "az1" in host:
        return "az1"
    if "az2" in host:
        return "az2"
    if "az3" in host:
        return "az3"
    return "az1"


def bind_background_overrides(ref: str, dt: datetime, host: str, idx: int) -> Dict[str, Any]:
    m = minute_of(dt)
    keyp = f"bg|{ref}|{idx}|{iso_utc_ms(dt)}|{host}"
    overrides: Dict[str, Any] = {}

    if ref == "customer_app.app_metric":
        if m < 18:
            ddb_err = 0.001 + 0.002 * u01(keyp + "|ddb")
            ec2_err = 0.010 + 0.010 * u01(keyp + "|ec2")
            nlb_err = 0.000 + 0.001 * u01(keyp + "|nlb")
        elif m < 34:
            ddb_err = 0.75 + 0.20 * u01(keyp + "|ddb")
            ec2_err = 0.35 + 0.25 * u01(keyp + "|ec2")
            nlb_err = 0.005 + 0.010 * u01(keyp + "|nlb")
        elif m < 40:
            ddb_err = 0.12 + 0.20 * u01(keyp + "|ddb")
            ec2_err = 0.30 + 0.20 * u01(keyp + "|ec2")
            nlb_err = 0.005 + 0.010 * u01(keyp + "|nlb")
        elif m < 42:
            ddb_err = 0.02 + 0.04 * u01(keyp + "|ddb")
            ec2_err = 0.25 + 0.20 * u01(keyp + "|ec2")
            nlb_err = 0.005 + 0.010 * u01(keyp + "|nlb")
        else:
            ddb_err = 0.005 + 0.020 * u01(keyp + "|ddb")
            ec2_err = 0.45 + 0.25 * u01(keyp + "|ec2")
            nlb_err = 0.03 + 0.10 * u01(keyp + "|nlb")
        overrides["ddb_err"] = fmt_float(clamp_int(int(ddb_err * 1000), 0, 1000) / 1000.0)
        overrides["ec2_err"] = fmt_float(clamp_int(int(ec2_err * 1000), 0, 1000) / 1000.0)
        overrides["nlb_err"] = fmt_float(clamp_int(int(nlb_err * 1000), 0, 1000) / 1000.0)

    elif ref == "route53_controlplane.rrset_change":
        ttl = 60 if u01(keyp + "|ttl") < 0.7 else 30
        overrides["ttl_s"] = ttl
        if m < 34:
            overrides["zone"] = "Z-DDB-USE1"
            overrides["name"] = "dynamodb.us-east-1.amazonaws.com"
            empty = u01(keyp + "|empty") < (0.75 if m >= 18 else 0.15)
            overrides["records"] = 0 if empty else (4 if u01(keyp + "|rec") < 0.5 else 8)
            overrides["action"] = "UPSERT" if u01(keyp + "|act") < 0.8 else "DELETE"
        elif m < 42:
            overrides["zone"] = "Z-DDB-USE1"
            overrides["name"] = "dynamodb.us-east-1.amazonaws.com"
            overrides["records"] = 8 if u01(keyp + "|rec") < 0.7 else 4
            overrides["action"] = "UPSERT"
        else:
            if u01(keyp + "|nlb") < 0.65:
                overrides["zone"] = "Z-NLB-USE1"
                overrides["name"] = choose_from(["nlb.app-lb-a.use1.amazonaws.com", "nlb.app-lb-b.use1.amazonaws.com"], keyp + "|name")
                overrides["records"] = sample_int(0, 8, keyp + "|records")
                overrides["action"] = "UPSERT" if u01(keyp + "|act") < 0.85 else "DELETE"
            else:
                overrides["zone"] = "Z-DDB-USE1"
                overrides["name"] = "dynamodb.us-east-1.amazonaws.com"
                overrides["records"] = 8
                overrides["action"] = "UPSERT"

    elif ref == "ddb_dns_planner.plan_generated":
        gen = 980 + int(m * 3) + (idx % 7)
        gen = clamp_int(gen, 980, 1150)
        overrides["gen"] = gen
        overrides["endpoints"] = 3 if u01(keyp + "|ep") < 0.5 else 2
        overrides["lbs"] = sample_int(20, 80, keyp + "|lbs")

    elif ref.startswith("ddb_dns_enactor."):
        if ref == "ddb_dns_enactor.enact_start":
            overrides["enactor"] = az_from_host(host)
            gen = 980 + int(m * 2) + (idx % 5)
            overrides["gen"] = clamp_int(gen, 980, 1150)
            overrides["prev_gen"] = clamp_int(overrides["gen"] - 1, 970, 1149)
            overrides["endpoint"] = "dynamodb.us-east-1.amazonaws.com" if u01(keyp + "|ep") < 0.8 else choose_from(["dynamodb-ipv6.us-east-1.amazonaws.com", "dynamodb-fips.us-east-1.amazonaws.com"], keyp + "|ep2")
        elif ref in ("ddb_dns_enactor.enact_ok", "ddb_dns_enactor.cleanup_plans", "ddb_dns_enactor.plan_missing", "ddb_dns_enactor.route53_txn_conflict"):
            gen = 980 + int(m * 2) + (idx % 6)
            overrides["gen"] = clamp_int(gen, 980, 1150)
            overrides["endpoint"] = "dynamodb.us-east-1.amazonaws.com"
            if ref == "ddb_dns_enactor.cleanup_plans":
                overrides["deleted"] = 0 if m >= 34 else sample_int(1, 10, keyp + "|del")
            if ref == "ddb_dns_enactor.route53_txn_conflict":
                base = 200 if m < 26 else 800
                overrides["wait_ms"] = clamp_int(base + sample_int(0, 7000, keyp + "|w"), 50, 8000)

    elif ref.startswith("dwfm."):
        if ref == "dwfm.state_check_fail":
            overrides["droplet"] = hex_n(keyp + "|d", 8)
            overrides["err"] = "ddb_dns_fail" if m < 34 else ("ddb_timeout" if u01(keyp + "|e") < 0.6 else "ddb_dns_fail")
        elif ref == "dwfm.lease_timeout":
            overrides["droplet"] = hex_n(keyp + "|d", 8)
            age = 600 if m < 34 else (900 if m < 42 else 1200)
            overrides["age_s"] = clamp_int(age + sample_int(-120, 300, keyp + "|a"), 300, 1800)
        elif ref == "dwfm.lease_recover":
            overrides["droplet"] = hex_n(keyp + "|d", 8)
            overrides["lease"] = uuid_from_key(keyp + "|lease")
            base = 200 if m < 34 else (1500 if m < 42 else 4000)
            overrides["dur_ms"] = clamp_int(base + sample_int(0, 8000, keyp + "|dur"), 50, 20000)
        elif ref == "dwfm.queue_depth":
            if m < 18:
                depth = int(50 + 200 * u01(keyp + "|q"))
                old = int(1 + 10 * u01(keyp + "|o"))
            elif m < 34:
                depth = int(200 + 1200 * u01(keyp + "|q"))
                old = int(10 + 60 * u01(keyp + "|o"))
            elif m < 42:
                depth = int(1500 + 15000 * u01(keyp + "|q"))
                old = int(60 + 900 * u01(keyp + "|o"))
            else:
                depth = int(8000 + 40000 * u01(keyp + "|q"))
                old = int(300 + 2400 * u01(keyp + "|o"))
            overrides["depth"] = clamp_int(depth, 0, 60000)
            overrides["old_s"] = clamp_int(old, 0, 3600)
        elif ref == "dwfm.throttle_enabled":
            overrides["rps"] = 10 if u01(keyp + "|r") < 0.5 else 25
        elif ref == "dwfm.host_restart":
            overrides["host"] = host
            overrides["mode"] = "selective" if u01(keyp + "|m") < 0.7 else "rolling"

    elif ref.startswith("network_manager."):
        if ref == "network_manager.netprop_metric":
            if m < 34:
                lag = int(1 + 20 * u01(keyp + "|lag"))
                backlog = int(0 + 200 * u01(keyp + "|b"))
            elif m < 42:
                lag = int(30 + 180 * u01(keyp + "|lag"))
                backlog = int(300 + 2000 * u01(keyp + "|b"))
            else:
                lag = int(400 + 1200 * u01(keyp + "|lag"))
                backlog = int(5000 + 30000 * u01(keyp + "|b"))
            overrides["lag_s"] = clamp_int(lag, 0, 1800)
            overrides["backlog"] = clamp_int(backlog, 0, 60000)
        elif ref == "network_manager.netprop_lag_high":
            lag = int(600 + 900 * u01(keyp + "|lag"))
            backlog = int(8000 + 40000 * u01(keyp + "|b"))
            overrides["lag_s"] = clamp_int(lag, 300, 1800)
            overrides["backlog"] = clamp_int(backlog, 5000, 60000)

    elif ref.startswith("nlb_controlplane."):
        if ref == "nlb_controlplane.hc_result":
            overrides["lb"] = choose_from(["app-lb-a", "app-lb-b"], keyp + "|lb")
            overrides["az"] = choose_from(["use1-az1", "use1-az2", "use1-az3"], keyp + "|az")
            fail_p = 0.03 if m < 42 else 0.35
            result = "fail" if u01(keyp + "|res") < fail_p else "pass"
            overrides["result"] = result
            if result == "fail":
                overrides["rtt_ms"] = clamp_int(150 + sample_int(0, 650, keyp + "|rtt"), 1, 800)
            else:
                overrides["rtt_ms"] = clamp_int(10 + sample_int(0, 80, keyp + "|rtt"), 1, 800)
        elif ref == "nlb_controlplane.az_failover":
            overrides["lb"] = choose_from(["app-lb-a", "app-lb-b"], keyp + "|lb")
            overrides["az"] = choose_from(["use1-az1", "use1-az2", "use1-az3"], keyp + "|az")
            overrides["pct"] = sample_int(10, 60, keyp + "|pct")

    elif ref == "ddb_dns_enactor.automation_disabled":
        overrides["actor"] = "oncall"
        overrides["reason"] = "inconsistent_state"

    elif ref == "ddb_dns_enactor.manual_route53_upsert":
        overrides["endpoint"] = "dynamodb.us-east-1.amazonaws.com"
        overrides["records"] = 8

    return overrides


# -----------------------------
# Flow attempt variable overrides
# -----------------------------

def bind_flow_static_context(flow_id: str, trace_id: str, start_dt: datetime) -> Dict[str, Any]:
    keyp = f"flow|{flow_id}|{trace_id}"
    ctx: Dict[str, Any] = {"trace_id": trace_id, "minute": minute_of(start_dt)}

    if flow_id in ("ddb_api_call", "ddb_api_call_dns_fail", "ddb_api_call_ok_f"):
        op = choose_from(["PutItem", "GetItem", "Query"], keyp + "|op")
        table = choose_from(["users", "orders", "sessions"], keyp + "|table")
        ctx.update({"op": op, "table": table, "endpoint": "dynamodb.us-east-1.amazonaws.com"})

    if flow_id.startswith("ec2_run_instances"):
        ctx["count"] = sample_int(1, 3, keyp + "|count")
        ctx["itype"] = choose_from(["t3.medium", "m5.large", "c6i.large"], keyp + "|itype")
        ctx["az"] = choose_from(["use1-az1", "use1-az2", "use1-az3"], keyp + "|az")
        ctx["iid"] = uuid_from_key(keyp + "|iid")

    if flow_id.startswith("nlb_tcp_connect"):
        ctx["lb"] = choose_from(["app-lb-a", "app-lb-b"], keyp + "|lb")
        if flow_id == "nlb_tcp_connect_err_f":
            ctx["nlb_err"] = choose_from(["ECONNRESET", "ETIMEDOUT"], keyp + "|err")

    return ctx


def reason_for_flow(flow_id: str) -> str:
    if flow_id == "ddb_api_call_dns_fail":
        return "dns_fail"
    if flow_id == "ec2_run_instances_service_unavailable":
        return "http_503"
    return "timeout"


def bind_dns_ttl_for_minute(m: float, key: str) -> int:
    if m < 34:
        base = 30
    elif m < 40:
        base = 600
    else:
        base = 300
    jitter = sample_int(-10, 40, key + "|j")
    return clamp_int(base + jitter, 0, 1200)


def component_host_map_for_flow(trace_id: str, emit_refs: List[str]) -> Dict[str, str]:
    comps = sorted({r.split(".", 1)[0] for r in emit_refs} | {"customer_app"})
    hm: Dict[str, str] = {}
    for cid in comps:
        hm[cid] = get_comp_host(cid, f"host|{cid}|{trace_id}")
    return hm


# -----------------------------
# Scheduling helpers
# -----------------------------

def schedule_evenly(start_dt: datetime, end_dt: datetime, n: int, key: str) -> List[datetime]:
    if n <= 0:
        return []
    dur_s = max(0.0, (end_dt - start_dt).total_seconds())
    if dur_s <= 0:
        return [start_dt] * n
    spacing = dur_s / n
    out: List[datetime] = []
    for i in range(n):
        base = (i + 0.5) * spacing
        jitter = (u01(f"{key}|j|{i}") - 0.5) * min(1.0, 0.2 * spacing)
        t = start_dt + timedelta(seconds=base + jitter)
        if t < start_dt:
            t = start_dt
        if t >= end_dt:
            t = end_dt - timedelta(milliseconds=1)
        out.append(t)
    return out


# -----------------------------
# Emission
# -----------------------------

def emit_row(rows: List[Dict[str, Any]], dt: datetime, level: str, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append({"ts": dt, "level": level, "message": message, "trace_id": trace_id, "service": service, "host": host})


def simulate_background(rows: List[Dict[str, Any]]) -> None:
    cr = CarryRounding()

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]

    def run_interval(state: str, start_min: int, end_min: int, bg_mult: Optional[Dict[str, float]] = None) -> None:
        bg_mult = bg_mult or {}
        start_dt = BASE_TIME + timedelta(minutes=start_min)
        end_dt = BASE_TIME + timedelta(minutes=end_min)
        dur_min = end_min - start_min

        for cid in sorted(COMP.keys()):
            beh_list = (COMP[cid].get("beh", {}) or {}).get(state, []) or []
            for beh in beh_list:
                log_id = beh["id"]
                per_min = float(beh["per_min"])
                scope = beh.get("scope", "per_host")
                ref = f"{cid}.{log_id}"

                mult = 1.0
                if state == "f":
                    mult = float(bg_mult.get(ref, 1.0))
                eff = per_min * mult

                hosts = COMP[cid].get("hosts", []) or []
                if scope == "global":
                    expected = eff * dur_min
                    n = cr.alloc(f"bg|{state}|{ref}|global", expected)
                    times = schedule_evenly(start_dt, end_dt, n, f"bg|{state}|{ref}|global|{start_min}-{end_min}")
                    for i, t in enumerate(times):
                        host = hosts[i % len(hosts)] if hosts else ""
                        overrides = bind_background_overrides(ref, t, host, i)
                        level, msg = render_from_template(ref, overrides, f"bg|{state}|{ref}|{i}")
                        emit_row(rows, t, level, msg, "", get_svc(cid), host)
                else:
                    for h in (hosts if hosts else [""]):
                        expected = eff * dur_min
                        n = cr.alloc(f"bg|{state}|{ref}|{h}", expected)
                        times = schedule_evenly(start_dt, end_dt, n, f"bg|{state}|{ref}|{h}|{start_min}-{end_min}")
                        for i, t in enumerate(times):
                            overrides = bind_background_overrides(ref, t, h, i)
                            level, msg = render_from_template(ref, overrides, f"bg|{state}|{ref}|{h}|{i}")
                            emit_row(rows, t, level, msg, "", get_svc(cid), h)

    run_interval("n", n_start, n_end)

    for iv in FAILURE_INTERVALS:
        run_interval("f", iv["start_min"], iv["end_min"], bg_mult=iv["bg_mult"])


def simulate_one_shots(rows: List[Dict[str, Any]]) -> None:
    for iv in FAILURE_INTERVALS:
        at_min = iv["start_min"]
        t0 = BASE_TIME + timedelta(minutes=at_min)
        for j, ospec in enumerate(iv.get("one_shots", []) or []):
            ref = ospec["ref"]
            count = int(ospec["count"])
            hosts = ospec.get("hosts", []) or []
            cid = ref.split(".", 1)[0]
            svc = get_svc(cid)
            for i in range(count):
                jitter_s = 5.0 * u01(f"oneshot|{ref}|{at_min}|{j}|{i}")
                t = t0 + timedelta(seconds=jitter_s, milliseconds=int(3 * u01(f"oneshot|ms|{ref}|{i}") * 1000) % 10)
                host = hosts[i % len(hosts)] if hosts else get_comp_host(cid, f"oneshot|{ref}|{at_min}|{i}")
                overrides = bind_background_overrides(ref, t, host, i)
                level, msg = render_from_template(ref, overrides, f"oneshot|{ref}|{at_min}|{i}")
                emit_row(rows, t, level, msg, "", svc, host)


def simulate_flows(rows: List[Dict[str, Any]]) -> None:
    cr = CarryRounding()

    def emit_count_for_attempt(flow_id: str, attempt: int, total_attempts: int, emit_refs: List[str]) -> int:
        # For successful DDB flows, model early attempts that may stop at the server-receive stage before retrying.
        if flow_id in ("ddb_api_call", "ddb_api_call_ok_f") and total_attempts > 1 and attempt < total_attempts:
            return min(2, len(emit_refs))
        return len(emit_refs)

    def simulate_flow_batch(state: str, flow_id: str, start_dt: datetime, end_dt: datetime, dur_min: int, flow_mult: float, lat_mult: Dict[str, Tuple[float, float]]) -> None:
        flow = FLOWS[state][flow_id]
        rpm = float(flow["rpm"]) * float(flow_mult)
        expected = rpm * dur_min
        n_instances = cr.alloc(f"flow|{state}|{flow_id}|{start_dt.isoformat()}", expected)
        starts = schedule_evenly(start_dt, end_dt, n_instances, f"flow|{state}|{flow_id}|{start_dt.isoformat()}")

        retry = flow["retry"]
        max_attempts = int(retry["max_attempts"])
        exp_attempts = float(retry["expected_attempts"])
        emit_refs = list(flow["emit"])
        latency_pairs = list(flow["latency_ms"])
        retry_refs = list(retry.get("emit_per_retry", []) or [])
        backoff_pairs = list(retry.get("backoff_ms", []) or [])
        traced = bool(flow.get("trace", False))

        lm = lat_mult.get(flow_id, (1.0, 1.0))

        for idx, sdt in enumerate(starts):
            trace_id = ""
            if SYSTEM["tracing"]["on"] and traced:
                trace_id = hex_n(f"trace|{state}|{flow_id}|{idx}|{iso_utc_ms(sdt)}", 32)

            host_map = component_host_map_for_flow(trace_id or f"notrace|{flow_id}|{idx}", emit_refs + retry_refs)
            static_ctx = bind_flow_static_context(flow_id, trace_id, sdt)

            attempts = pick_attempts(exp_attempts, max_attempts, f"attempts|{flow_id}|{trace_id}|{idx}")

            attempt_start = sdt
            prev_attempt_end: Optional[datetime] = None

            for a in range(1, attempts + 1):
                if a > 1:
                    bo_idx = min(a - 2, len(backoff_pairs) - 1) if backoff_pairs else 0
                    if backoff_pairs:
                        bo_ms = sample_backoff_ms(backoff_pairs[bo_idx], lm[0], lm[1], f"backoff|{flow_id}|{trace_id}|{idx}|{a}")
                    else:
                        bo_ms = 0
                    attempt_start = (prev_attempt_end or attempt_start) + timedelta(milliseconds=bo_ms)

                    for rr_i, rr_ref in enumerate(retry_refs):
                        rcid = rr_ref.split(".", 1)[0]
                        rhost = host_map.get(rcid, get_comp_host(rcid, f"host|{rcid}|{trace_id}|{idx}"))
                        reason = reason_for_flow(flow_id)
                        overrides = {"attempt": a, "reason": reason, "trace_id": trace_id}
                        level, msg = render_from_template(rr_ref, overrides, f"retrylog|{flow_id}|{trace_id}|{idx}|{a}|{rr_i}")
                        emit_row(rows, attempt_start + timedelta(milliseconds=rr_i), level, msg, trace_id, get_svc(rcid), rhost)

                key_prefix = f"flow|{state}|{flow_id}|{trace_id}|{idx}|attempt{a}"
                emit_count = emit_count_for_attempt(flow_id, a, attempts, emit_refs)

                lat_ms_full = plan_attempt_latencies(flow_id, emit_refs, latency_pairs, lm, key_prefix)
                lat_ms = lat_ms_full[:emit_count]

                times: List[datetime] = []
                tcur = attempt_start
                for gap in lat_ms:
                    tcur = tcur + timedelta(milliseconds=int(gap))
                    times.append(tcur)

                per_attempt: Dict[str, Dict[str, Any]] = {}

                def o(ref: str) -> Dict[str, Any]:
                    return per_attempt.setdefault(ref, {})

                for ref in emit_refs[:emit_count]:
                    if "trace_id" in (LOG_TEMPL[ref].get("vars", {}) or {}) and trace_id:
                        o(ref)["trace_id"] = trace_id

                if flow_id in ("ddb_api_call", "ddb_api_call_ok_f"):
                    rid = hex_n(f"rid|{trace_id}|{idx}|{a}", 16)
                    if emit_count >= 1:
                        o("customer_app.ddb_call_start").update({"op": static_ctx["op"], "table": static_ctx["table"], "endpoint": static_ctx["endpoint"], "trace_id": trace_id})
                    if emit_count >= 2:
                        o("ddb_api.http_req").update({"op": static_ctx["op"], "table": static_ctx["table"], "rid": rid, "trace_id": trace_id})
                    if emit_count >= 3:
                        dur = int(round((times[2] - times[1]).total_seconds() * 1000))
                        o("ddb_api.http_200").update({"rid": rid, "dur_ms": dur, "trace_id": trace_id})
                    if emit_count >= 4:
                        dur = int(round((times[3] - times[0]).total_seconds() * 1000))
                        o("customer_app.ddb_call_ok").update({"op": static_ctx["op"], "dur_ms": dur, "trace_id": trace_id})

                if flow_id == "ddb_api_call_dns_fail":
                    if emit_count >= 1:
                        o("customer_app.ddb_call_start").update({"op": static_ctx["op"], "table": static_ctx["table"], "endpoint": static_ctx["endpoint"], "trace_id": trace_id})
                    if emit_count >= 2:
                        ttl_s = bind_dns_ttl_for_minute(minute_of(attempt_start), f"ttl|{trace_id}|{idx}|{a}")
                        err = "NO_ANSWER" if minute_of(attempt_start) < 34 else ("SERVFAIL" if u01(f"dnserr|{trace_id}|{idx}|{a}") < 0.4 else "NO_ANSWER")
                        o("customer_app.dns_resolve_fail").update({"host": "dynamodb.us-east-1.amazonaws.com", "err": err, "ttl_s": ttl_s, "trace_id": trace_id})

                if flow_id == "ec2_run_instances":
                    req_id = hex_n(f"reqid|{trace_id}|{idx}|{a}", 16)
                    droplet = hex_n(f"droplet|{trace_id}|{idx}", 8)
                    lease = uuid_from_key(f"lease|{trace_id}|{idx}|{a}")
                    net_iid = static_ctx["iid"]
                    ec2_api_dur = int(round((times[4] - times[1]).total_seconds() * 1000))
                    client_dur = int(round((times[5] - times[0]).total_seconds() * 1000))
                    lease_dur = int(round((times[2] - times[1]).total_seconds() * 1000))
                    backlog = clamp_int(int(50 + 200 * u01(f"backlog|{trace_id}|{idx}|{a}")), 0, 60000)

                    o("customer_app.ec2_run_instances_req").update({"count": static_ctx["count"], "itype": static_ctx["itype"], "az": static_ctx["az"], "trace_id": trace_id})
                    o("ec2_api.api_recv").update({"req_id": req_id, "trace_id": trace_id})
                    o("dwfm.lease_recover").update({"droplet": droplet, "lease": lease, "dur_ms": lease_dur})
                    o("network_manager.netprop_enqueue").update({"iid": net_iid, "backlog": backlog, "trace_id": trace_id})
                    o("ec2_api.api_resp_ok").update({"req_id": req_id, "dur_ms": ec2_api_dur, "trace_id": trace_id})
                    o("customer_app.ec2_run_instances_ok").update({"iid": static_ctx["iid"], "dur_ms": client_dur, "trace_id": trace_id})

                if flow_id == "ec2_run_instances_service_unavailable":
                    req_id = hex_n(f"reqid|{trace_id}|{idx}|{a}", 16)
                    api_dur = int(round((times[2] - times[1]).total_seconds() * 1000))
                    client_dur = int(round((times[3] - times[0]).total_seconds() * 1000))
                    o("customer_app.ec2_run_instances_req").update({"count": static_ctx["count"], "itype": static_ctx["itype"], "az": static_ctx["az"], "trace_id": trace_id})
                    o("ec2_api.api_recv").update({"req_id": req_id, "trace_id": trace_id})
                    o("ec2_api.api_resp_unavailable").update({"req_id": req_id, "dur_ms": api_dur, "trace_id": trace_id})
                    o("customer_app.ec2_run_instances_err_unavailable").update({"dur_ms": client_dur, "trace_id": trace_id})

                if flow_id == "ec2_run_instances_insufficient_capacity":
                    req_id = hex_n(f"reqid|{trace_id}|{idx}|{a}", 16)
                    api_dur = int(round((times[2] - times[1]).total_seconds() * 1000))
                    client_dur = int(round((times[3] - times[0]).total_seconds() * 1000))
                    o("customer_app.ec2_run_instances_req").update({"count": static_ctx["count"], "itype": static_ctx["itype"], "az": static_ctx["az"], "trace_id": trace_id})
                    o("ec2_api.api_recv").update({"req_id": req_id, "trace_id": trace_id})
                    o("ec2_api.api_resp_insufficient").update({"req_id": req_id, "dur_ms": api_dur, "trace_id": trace_id})
                    o("customer_app.ec2_run_instances_err_capacity").update({"dur_ms": client_dur, "trace_id": trace_id})

                if flow_id == "ec2_run_instances_throttled":
                    req_id = hex_n(f"reqid|{trace_id}|{idx}|{a}", 16)
                    api_dur = int(round((times[2] - times[1]).total_seconds() * 1000))
                    client_dur = int(round((times[3] - times[0]).total_seconds() * 1000))
                    o("customer_app.ec2_run_instances_req").update({"count": static_ctx["count"], "itype": static_ctx["itype"], "az": static_ctx["az"], "trace_id": trace_id})
                    o("ec2_api.api_recv").update({"req_id": req_id, "trace_id": trace_id})
                    o("ec2_api.api_resp_throttle").update({"req_id": req_id, "dur_ms": api_dur, "trace_id": trace_id})
                    o("customer_app.ec2_run_instances_err_throttle").update({"dur_ms": client_dur, "trace_id": trace_id})

                if flow_id in ("nlb_tcp_connect_ok", "nlb_tcp_connect_ok_f"):
                    rtt = int(round(lat_ms[0])) if lat_ms else 1
                    o("customer_app.nlb_connect_ok").update({"lb": static_ctx["lb"], "rtt_ms": rtt})

                if flow_id == "nlb_tcp_connect_err_f":
                    o("customer_app.nlb_connect_err").update({"lb": static_ctx["lb"], "err": static_ctx["nlb_err"]})

                for li in range(emit_count):
                    ref = emit_refs[li]
                    cid = ref.split(".", 1)[0]
                    host = host_map.get(cid, get_comp_host(cid, f"host|{cid}|{trace_id}|{idx}"))
                    overrides = per_attempt.get(ref, {})
                    level, msg = render_from_template(ref, overrides, f"emit|{key_prefix}|{li}")
                    emit_row(rows, times[li], level, msg, trace_id if traced and SYSTEM["tracing"]["on"] else "", get_svc(cid), host)

                prev_attempt_end = times[-1] if times else attempt_start

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    start_dt_n = BASE_TIME + timedelta(minutes=n_start)
    end_dt_n = BASE_TIME + timedelta(minutes=n_end)
    dur_min_n = n_end - n_start
    for flow_id in sorted(FLOWS["n"].keys()):
        simulate_flow_batch("n", flow_id, start_dt_n, end_dt_n, dur_min_n, 1.0, lat_mult={})

    for iv in FAILURE_INTERVALS:
        start_dt_f = BASE_TIME + timedelta(minutes=iv["start_min"])
        end_dt_f = BASE_TIME + timedelta(minutes=iv["end_min"])
        dur_min_f = iv["end_min"] - iv["start_min"]
        for flow_id in sorted(FLOWS["f"].keys()):
            mult = float(iv["flow_mult"].get(flow_id, 1.0))
            simulate_flow_batch("f", flow_id, start_dt_f, end_dt_f, dur_min_f, mult, lat_mult=iv["lat_mult"])


# -----------------------------
# Main
# -----------------------------

def main() -> None:
    rows: List[Dict[str, Any]] = []
    simulate_background(rows)
    simulate_one_shots(rows)
    simulate_flows(rows)

    df = pd.DataFrame(rows)
    df = df.sort_values("ts", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["ts"].apply(iso_utc_ms)
    df = df.drop(columns=["ts"])
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
