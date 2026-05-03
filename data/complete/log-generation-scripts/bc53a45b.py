import re
import math
import uuid
import hashlib
import ipaddress
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from statistics import NormalDist


SYSTEM: Dict[str, Any] = {
    "sys": {"id": "internal_edge_congestion_us_east_1"},
    "tracing": {"on": True, "origins": ["ec2_api_frontend"], "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "autoscale_controller",
            "svc": "autoscale",
            "hosts": ["autoscale-1"],
            "logs": {
                "scale_activity_start": {
                    "lvl": "INFO",
                    "msg": "starting automated capacity scale activity for {service} in {az}",
                    "vars": {
                        "service": {"k": "ch", "v": ["capacity-service"]},
                        "az": {"k": "ch", "v": ["use1-az1", "use1-az2"]},
                    },
                },
                "scale_activity_disabled": {
                    "lvl": "WARN",
                    "msg": "disabled automated scaling activity for {service}; reason={reason}",
                    "vars": {
                        "service": {"k": "ch", "v": ["capacity-service"]},
                        "reason": {"k": "ch", "v": ["incident_mitigation", "unexpected_connection_surge"]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "internal_client_fleet",
            "svc": "client-fleet",
            "hosts": ["int-cli-1", "int-cli-2", "int-cli-3"],
            "logs": {
                "connect_attempt": {
                    "lvl": "INFO",
                    "msg": "conn attempt to {dst_svc} via {edge} timeout_ms={timeout_ms}",
                    "vars": {
                        "dst_svc": {"k": "ch", "v": ["capacity-service"]},
                        "edge": {"k": "ch", "v": ["edge-gw"]},
                        "timeout_ms": {"k": "i", "v": [1500, 5000]},
                    },
                },
                "connect_retry": {
                    "lvl": "WARN",
                    "msg": "retrying connection to {dst_svc} attempt={attempt} backoff_ms={backoff_ms}",
                    "vars": {
                        "dst_svc": {"k": "ch", "v": ["capacity-service"]},
                        "attempt": {"k": "i", "v": [2, 5]},
                    },
                    "state_vars": {
                        "n": {"backoff_ms": {"k": "i", "v": [200, 1200]}},
                        "f": {"backoff_ms": {"k": "i", "v": [20, 120]}},
                    },
                },
                "connect_ok": {
                    "lvl": "INFO",
                    "msg": "connection established to {dst_svc} rtt_ms={rtt_ms}",
                    "vars": {"dst_svc": {"k": "ch", "v": ["capacity-service"]}, "rtt_ms": {"k": "i", "v": [10, 800]}},
                },
                "connect_fail": {
                    "lvl": "ERROR",
                    "msg": "connection failed to {dst_svc} err={err} waited_ms={waited_ms}",
                    "vars": {
                        "dst_svc": {"k": "ch", "v": ["capacity-service"]},
                        "err": {"k": "ch", "v": ["timeout", "reset", "no_route"]},
                        "waited_ms": {"k": "i", "v": [200, 6000]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "edge_gateway",
            "svc": None,
            "hosts": ["edge-gw-a", "edge-gw-b"],
            "logs": {
                "nat_stats": {
                    "lvl": "INFO",
                    "msg": "nat stats conn_attempts_s={conn_attempts_s} active_conns={active_conns} queue_depth={queue_depth} drop_pct={drop_pct}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "conn_attempts_s": {"k": "i", "v": [30, 250]},
                            "active_conns": {"k": "i", "v": [2000, 15000]},
                            "queue_depth": {"k": "i", "v": [0, 60]},
                            "drop_pct": {"k": "f", "v": [0.0, 0.3]},
                        },
                        "f": {
                            "conn_attempts_s": {"k": "i", "v": [200, 2500]},
                            "active_conns": {"k": "i", "v": [10000, 80000]},
                            "queue_depth": {"k": "i", "v": [80, 1200]},
                            "drop_pct": {"k": "f", "v": [0.5, 12.0]},
                        },
                    },
                },
                "conntrack_overflow": {
                    "lvl": "WARN",
                    "msg": "conntrack table pressure active_conns={active_conns} max_conns={max_conns} evictions_s={evictions_s}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "active_conns": {"k": "i", "v": [5000, 15000]},
                            "max_conns": {"k": "i", "v": [200000, 200000]},
                            "evictions_s": {"k": "i", "v": [0, 2]},
                        },
                        "f": {
                            "active_conns": {"k": "i", "v": [20000, 80000]},
                            "max_conns": {"k": "i", "v": [200000, 200000]},
                            "evictions_s": {"k": "i", "v": [10, 400]},
                        },
                    },
                },
                "device_health": {
                    "lvl": "INFO",
                    "msg": "device health cpu_pct={cpu_pct} mem_pct={mem_pct}",
                    "vars": {"cpu_pct": {"k": "i", "v": [10, 95]}, "mem_pct": {"k": "i", "v": [20, 95]}},
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "nat_stats", "per_min": 2.0, "scope": "per_host"},
                        {"id": "device_health", "per_min": 1.0, "scope": "per_host"},
                        {"id": "conntrack_overflow", "per_min": 0.02, "scope": "per_host"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "nat_stats", "per_min": 4.0, "scope": "per_host"},
                        {"id": "device_health", "per_min": 1.0, "scope": "per_host"},
                        {"id": "conntrack_overflow", "per_min": 0.8, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "internal_dns",
            "svc": "internal-dns",
            "hosts": ["dns-a", "dns-b"],
            "logs": {
                "query_recv": {
                    "lvl": "INFO",
                    "msg": "query {qname} type={qtype} from {src} txid={txid}",
                    "vars": {
                        "qname": {
                            "k": "ch",
                            "v": ["ec2.internal", "auth.internal", "monitoring.internal", "deploy.internal", "events.internal"],
                        },
                        "qtype": {"k": "ch", "v": ["A", "AAAA"]},
                        "src": {"k": "ch", "v": ["ec2-api", "monitoring", "deploy", "other-service"]},
                        "txid": {"k": "hex", "v": 8},
                    },
                },
                "reply_ok": {
                    "lvl": "INFO",
                    "msg": "reply {qname} rcode=NOERROR answer_rrs={rrs} latency_ms={latency_ms}",
                    "vars": {
                        "qname": {
                            "k": "ch",
                            "v": ["ec2.internal", "auth.internal", "monitoring.internal", "deploy.internal", "events.internal"],
                        },
                        "rrs": {"k": "i", "v": [1, 6]},
                        "latency_ms": {"k": "i", "v": [1, 1200]},
                    },
                },
                "reply_servfail": {
                    "lvl": "WARN",
                    "msg": "reply {qname} rcode=SERVFAIL latency_ms={latency_ms} reason={reason}",
                    "vars": {
                        "qname": {
                            "k": "ch",
                            "v": ["ec2.internal", "auth.internal", "monitoring.internal", "deploy.internal", "events.internal"],
                        },
                        "latency_ms": {"k": "i", "v": [50, 4000]},
                        "reason": {"k": "ch", "v": ["upstream_timeout", "queue_overflow"]},
                    },
                },
                "dns_health_normal": {
                    "lvl": "INFO",
                    "msg": "dns health qps={qps} servfail_pct={servfail_pct}",
                    "vars": {"qps": {"k": "f", "v": [0.0, 5.0]}, "servfail_pct": {"k": "f", "v": [0.0, 0.2]}},
                },
                "dns_health_degraded": {
                    "lvl": "INFO",
                    "msg": "dns health qps={qps} servfail_pct={servfail_pct}",
                    "vars": {"qps": {"k": "f", "v": [0.5, 12.0]}, "servfail_pct": {"k": "f", "v": [1.0, 30.0]}},
                },
                "dns_health_recovered": {
                    "lvl": "INFO",
                    "msg": "dns health qps={qps} servfail_pct={servfail_pct}",
                    "vars": {"qps": {"k": "f", "v": [0.2, 10.0]}, "servfail_pct": {"k": "f", "v": [0.0, 0.5]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "dns_health_normal", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "dns_health_degraded", "per_min": 1.0, "scope": "per_host"},
                        {"id": "dns_health_recovered", "per_min": 1.0, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "ec2_api_frontend",
            "svc": "ec2-api",
            "hosts": ["ec2-api-1", "ec2-api-2"],
            "logs": {
                "http_req_describe": {
                    "lvl": "INFO",
                    "msg": "request DescribeInstances /ec2 from {client_ip} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "client_ip": {"k": "ip", "v": "198.51.100.0/24"},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_200_describe": {
                    "lvl": "INFO",
                    "msg": "response DescribeInstances status=200 dur_ms={dur_ms} req_id={req_id}",
                    "vars": {"dur_ms": {"k": "i", "v": [20, 1500]}, "req_id": {"k": "uuid", "v": None}},
                },
                "http_503_describe": {
                    "lvl": "ERROR",
                    "msg": "response DescribeInstances status=503 err=UpstreamTimeout dur_ms={dur_ms} req_id={req_id}",
                    "vars": {"dur_ms": {"k": "i", "v": [300, 8000]}, "req_id": {"k": "uuid", "v": None}},
                },
                "http_req_run": {
                    "lvl": "INFO",
                    "msg": "request RunInstances /ec2 from {client_ip} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "client_ip": {"k": "ip", "v": "198.51.100.0/24"},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "http_200_run": {
                    "lvl": "INFO",
                    "msg": "response RunInstances status=200 dur_ms={dur_ms} req_id={req_id}",
                    "vars": {"dur_ms": {"k": "i", "v": [50, 4000]}, "req_id": {"k": "uuid", "v": None}},
                },
                "http_503_run": {
                    "lvl": "ERROR",
                    "msg": "response RunInstances status=503 err=UpstreamTimeout dur_ms={dur_ms} req_id={req_id}",
                    "vars": {"dur_ms": {"k": "i", "v": [500, 12000]}, "req_id": {"k": "uuid", "v": None}},
                },
                "dns_resolve": {
                    "lvl": "DEBUG",
                    "msg": "resolving {qname} for internal call req_id={req_id}",
                    "vars": {
                        "qname": {
                            "k": "ch",
                            "v": ["ec2.internal", "auth.internal", "monitoring.internal", "deploy.internal", "events.internal"],
                        },
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "dns_ok": {
                    "lvl": "DEBUG",
                    "msg": "dns resolved {qname} addr={addr} dur_ms={dur_ms} req_id={req_id}",
                    "vars": {
                        "qname": {
                            "k": "ch",
                            "v": ["ec2.internal", "auth.internal", "monitoring.internal", "deploy.internal", "events.internal"],
                        },
                        "addr": {"k": "ip", "v": "10.0.0.0/16"},
                        "dur_ms": {"k": "i", "v": [1, 1200]},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
                "dns_failed": {
                    "lvl": "WARN",
                    "msg": "dns resolution failed {qname} err={err} waited_ms={waited_ms} req_id={req_id}",
                    "vars": {
                        "qname": {
                            "k": "ch",
                            "v": ["ec2.internal", "auth.internal", "monitoring.internal", "deploy.internal", "events.internal"],
                        },
                        "err": {"k": "ch", "v": ["SERVFAIL", "timeout"]},
                        "waited_ms": {"k": "i", "v": [50, 5000]},
                        "req_id": {"k": "uuid", "v": None},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "ec2_control_plane",
            "svc": "ec2-control-plane",
            "hosts": ["ec2-cp-1", "ec2-cp-2"],
            "logs": {
                "rpc_start_describe": {
                    "lvl": "INFO",
                    "msg": "rpc DescribeInstances recv req_id={req_id} from {caller}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "caller": {"k": "ch", "v": ["ec2-api"]}},
                },
                "rpc_ok_describe": {
                    "lvl": "INFO",
                    "msg": "rpc DescribeInstances ok dur_ms={dur_ms} req_id={req_id}",
                    "vars": {"dur_ms": {"k": "i", "v": [10, 900]}, "req_id": {"k": "uuid", "v": None}},
                },
                "rpc_deadline_describe": {
                    "lvl": "WARN",
                    "msg": "rpc DescribeInstances deadline exceeded waited_ms={waited_ms} req_id={req_id}",
                    "vars": {"waited_ms": {"k": "i", "v": [200, 8000]}, "req_id": {"k": "uuid", "v": None}},
                },
                "rpc_start_run": {
                    "lvl": "INFO",
                    "msg": "rpc RunInstances recv req_id={req_id} from {caller}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "caller": {"k": "ch", "v": ["ec2-api"]}},
                },
                "rpc_ok_run": {
                    "lvl": "INFO",
                    "msg": "rpc RunInstances ok dur_ms={dur_ms} req_id={req_id}",
                    "vars": {"dur_ms": {"k": "i", "v": [30, 2500]}, "req_id": {"k": "uuid", "v": None}},
                },
                "rpc_deadline_run": {
                    "lvl": "WARN",
                    "msg": "rpc RunInstances deadline exceeded waited_ms={waited_ms} req_id={req_id}",
                    "vars": {"waited_ms": {"k": "i", "v": [400, 12000]}, "req_id": {"k": "uuid", "v": None}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "capacity_service",
            "svc": "capacity-service",
            "hosts": ["cap-svc-1", "cap-svc-2"],
            "logs": {
                "conn_accept": {
                    "lvl": "INFO",
                    "msg": "accepted client connection src={src} conn_id={conn_id}",
                    "vars": {"src": {"k": "ch", "v": ["internal-client"]}, "conn_id": {"k": "hex", "v": 12}},
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "monitoring_pipeline",
            "svc": "monitoring",
            "hosts": ["mon-1"],
            "logs": {
                "metric_ingest_lag": {
                    "lvl": "WARN",
                    "msg": "metrics ingestion lag lag_s={lag_s} dropped_samples={dropped}",
                    "vars": {},
                    "state_vars": {
                        "n": {"lag_s": {"k": "i", "v": [0, 10]}, "dropped": {"k": "i", "v": [0, 50]}},
                        "f": {"lag_s": {"k": "i", "v": [60, 3600]}, "dropped": {"k": "i", "v": [100, 20000]}},
                    },
                }
            },
            "beh": {
                "n": {"emit": [{"id": "metric_ingest_lag", "per_min": 0.2, "scope": "global"}]},
                "f": {"emit": [{"id": "metric_ingest_lag", "per_min": 1.0, "scope": "global"}]},
            },
        },
        {
            "id": "ops_tooling",
            "svc": "ops",
            "hosts": ["ops-1"],
            "logs": {
                "dns_reroute_completed": {
                    "lvl": "INFO",
                    "msg": "completed reroute of internal DNS to alternate path change_id={change_id}",
                    "vars": {"change_id": {"k": "hex", "v": 12}},
                },
                "capacity_added": {
                    "lvl": "INFO",
                    "msg": "brought additional edge capacity online device_set={device_set} change_id={change_id}",
                    "vars": {
                        "device_set": {"k": "ch", "v": ["edge-gw-extra-a", "edge-gw-extra-b"]},
                        "change_id": {"k": "hex", "v": 12},
                    },
                },
                "remediation_step_failed": {
                    "lvl": "WARN",
                    "msg": "remediation automation step failed step={step} err={err}",
                    "vars": {
                        "step": {"k": "ch", "v": ["push_network_config", "isolate_top_talker", "add_capacity"]},
                        "err": {"k": "ch", "v": ["timeout", "dns_error", "auth_unavailable"]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "ec2_describe_instances",
                    "rpm": 120.0,
                    "emit": [
                        "ec2_api_frontend.http_req_describe",
                        "ec2_control_plane.rpc_start_describe",
                        "ec2_control_plane.rpc_ok_describe",
                        "ec2_api_frontend.http_200_describe",
                    ],
                    "latency_ms": [[3, 8], [10, 40], [10, 80], [20, 150]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "ec2_run_instances",
                    "rpm": 30.0,
                    "emit": [
                        "ec2_api_frontend.http_req_run",
                        "ec2_control_plane.rpc_start_run",
                        "ec2_control_plane.rpc_ok_run",
                        "ec2_api_frontend.http_200_run",
                    ],
                    "latency_ms": [[5, 15], [20, 80], [40, 250], [60, 400]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "internal_dns_query_ok",
                    "rpm": 80.0,
                    "emit": [
                        "ec2_api_frontend.dns_resolve",
                        "internal_dns.query_recv",
                        "internal_dns.reply_ok",
                        "ec2_api_frontend.dns_ok",
                    ],
                    "latency_ms": [[1, 3], [2, 10], [2, 20], [2, 25]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "internal_client_connect_ok",
                    "rpm": 50.0,
                    "emit": ["internal_client_fleet.connect_attempt", "capacity_service.conn_accept", "internal_client_fleet.connect_ok"],
                    "latency_ms": [[2, 8], [5, 30], [5, 50]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "internal_client_connect_fail",
                    "rpm": 3.0,
                    "emit": ["internal_client_fleet.connect_attempt", "internal_client_fleet.connect_fail"],
                    "latency_ms": [[10, 30], [50, 300]],
                    "retry": {
                        "max_attempts": 3,
                        "expected_attempts": 1.2,
                        "emit_per_retry": ["internal_client_fleet.connect_retry"],
                        "backoff_ms": [[250, 900], [400, 1200]],
                    },
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "ec2_describe_instances",
                    "rpm": 50.0,
                    "emit": [
                        "ec2_api_frontend.http_req_describe",
                        "ec2_control_plane.rpc_start_describe",
                        "ec2_control_plane.rpc_ok_describe",
                        "ec2_api_frontend.http_200_describe",
                    ],
                    "latency_ms": [[10, 50], [50, 300], [80, 700], [120, 1000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "ec2_describe_instances_timeout",
                    "rpm": 90.0,
                    "emit": [
                        "ec2_api_frontend.http_req_describe",
                        "ec2_control_plane.rpc_start_describe",
                        "ec2_control_plane.rpc_deadline_describe",
                        "ec2_api_frontend.http_503_describe",
                    ],
                    "latency_ms": [[20, 80], [200, 1200], [500, 4000], [800, 8000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "ec2_run_instances",
                    "rpm": 3.0,
                    "emit": [
                        "ec2_api_frontend.http_req_run",
                        "ec2_control_plane.rpc_start_run",
                        "ec2_control_plane.rpc_ok_run",
                        "ec2_api_frontend.http_200_run",
                    ],
                    "latency_ms": [[20, 80], [200, 800], [300, 2000], [400, 3000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "ec2_run_instances_timeout",
                    "rpm": 45.0,
                    "emit": [
                        "ec2_api_frontend.http_req_run",
                        "ec2_control_plane.rpc_start_run",
                        "ec2_control_plane.rpc_deadline_run",
                        "ec2_api_frontend.http_503_run",
                    ],
                    "latency_ms": [[30, 120], [400, 2000], [800, 8000], [1200, 12000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": True,
                },
                {
                    "id": "internal_dns_query_ok",
                    "rpm": 20.0,
                    "emit": [
                        "ec2_api_frontend.dns_resolve",
                        "internal_dns.query_recv",
                        "internal_dns.reply_ok",
                        "ec2_api_frontend.dns_ok",
                    ],
                    "latency_ms": [[10, 60], [50, 300], [50, 600], [50, 800]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "internal_dns_query_servfail",
                    "rpm": 120.0,
                    "emit": [
                        "ec2_api_frontend.dns_resolve",
                        "internal_dns.query_recv",
                        "internal_dns.reply_servfail",
                        "ec2_api_frontend.dns_failed",
                    ],
                    "latency_ms": [[20, 120], [100, 800], [200, 3000], [200, 5000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "internal_client_connect_ok",
                    "rpm": 20.0,
                    "emit": ["internal_client_fleet.connect_attempt", "capacity_service.conn_accept", "internal_client_fleet.connect_ok"],
                    "latency_ms": [[20, 120], [50, 300], [50, 500]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "internal_client_connect_fail",
                    "rpm": 120.0,
                    "emit": ["internal_client_fleet.connect_attempt", "internal_client_fleet.connect_fail"],
                    "latency_ms": [[30, 150], [200, 3000]],
                    "retry": {
                        "max_attempts": 5,
                        "expected_attempts": 3.0,
                        "emit_per_retry": ["internal_client_fleet.connect_retry"],
                        "backoff_ms": [[20, 60], [30, 80], [40, 100], [60, 120]],
                    },
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "use1_internal_edge_congestion_dec2021"},
    "time": {"total_minutes": 34, "phases": {"n": {"start_min": 0, "end_min": 16}, "f": {"start_min": 16, "end_min": 34}}},
    "events": [
        {
            "order": 1,
            "at_min": 16,
            "rate_multipliers": {
                "internal_client_connect_fail": 1.5,
                "internal_dns_query_servfail": 1.3,
                "ec2_describe_instances_timeout": 1.2,
                "ec2_run_instances_timeout": 1.2,
                "edge_gateway.conntrack_overflow": 2.0,
                "monitoring_pipeline.metric_ingest_lag": 2.0,
                "internal_dns.dns_health_recovered": 0.0,
            },
            "latency_multipliers": {
                "ec2_describe_instances_timeout": {"p50": 1.5, "p95": 1.4},
                "ec2_run_instances_timeout": {"p50": 1.5, "p95": 1.4},
                "internal_dns_query_servfail": {"p50": 1.4, "p95": 1.4},
            },
            "one_shots": [{"ref": "autoscale_controller.scale_activity_start", "count": 1, "hosts": ["autoscale-1"]}],
        },
        {
            "order": 2,
            "at_min": 24,
            "rate_multipliers": {
                "internal_dns_query_servfail": 0.0,
                "internal_dns_query_ok": 0.2,
                "internal_dns.dns_health_degraded": 0.0,
                "internal_dns.dns_health_recovered": 1.0,
                "internal_client_connect_fail": 1.2,
                "edge_gateway.conntrack_overflow": 1.2,
                "ec2_describe_instances_timeout": 1.0,
                "ec2_run_instances_timeout": 1.0,
                "monitoring_pipeline.metric_ingest_lag": 2.0,
            },
            "latency_multipliers": {
                "ec2_describe_instances_timeout": {"p50": 1.3, "p95": 1.2},
                "ec2_run_instances_timeout": {"p50": 1.3, "p95": 1.2},
            },
            "one_shots": [{"ref": "ops_tooling.dns_reroute_completed", "count": 1, "hosts": ["ops-1"]}],
        },
        {
            "order": 3,
            "at_min": 30,
            "rate_multipliers": {
                "internal_client_connect_fail": 0.7,
                "edge_gateway.conntrack_overflow": 0.5,
                "ec2_describe_instances_timeout": 0.7,
                "ec2_run_instances_timeout": 0.8,
                "monitoring_pipeline.metric_ingest_lag": 1.6,
                "internal_dns.dns_health_recovered": 1.0,
            },
            "latency_multipliers": {
                "ec2_describe_instances_timeout": {"p50": 1.15, "p95": 1.1},
                "ec2_run_instances_timeout": {"p50": 1.15, "p95": 1.1},
            },
            "one_shots": [
                {"ref": "ops_tooling.capacity_added", "count": 1, "hosts": ["ops-1"]},
                {"ref": "autoscale_controller.scale_activity_disabled", "count": 1, "hosts": ["autoscale-1"]},
            ],
        },
    ],
}

PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z0-9_]+)\}")


def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def stable_u01(key: str) -> float:
    h = hashlib.md5(key.encode("utf-8")).digest()
    x = int.from_bytes(h[:8], byteorder="big", signed=False)
    return (x + 1) / (2**64 + 2)  # strictly inside (0,1)


def deterministic_round(expect: float, key: str) -> int:
    if expect <= 0:
        return 0
    n = int(math.floor(expect))
    frac = expect - n
    if frac <= 0:
        return n
    return n + (1 if stable_u01(f"{key}|frac") < frac else 0)


def fmt_ts(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def norminv(u: float) -> float:
    return NormalDist().inv_cdf(u)


def sample_lognormal_ms(p50: float, p95: float, u: float, soft_cap: Optional[float] = None) -> int:
    p50 = max(1e-6, float(p50))
    p95 = max(p50 * 1.000001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - mu) / 1.6448536269514722
    z = norminv(min(max(u, 1e-9), 1 - 1e-9))
    x = math.exp(mu + sigma * z)
    cap = soft_cap if soft_cap is not None else 3.0 * p95
    x = min(x, cap)
    return max(1, int(round(x)))


def even_times(start: datetime, end: datetime, count: int, key_prefix: str) -> List[datetime]:
    if count <= 0:
        return []
    start_ts = start.timestamp()
    end_ts = end.timestamp()
    L = max(0.001, end_ts - start_ts)
    out = []
    for i in range(count):
        frac = (i + 0.5) / count
        base = start_ts + frac * L
        jitter = (stable_u01(f"{key_prefix}|j{i}") - 0.5) * 0.6  # seconds
        t = base + jitter
        if t < start_ts:
            t = start_ts + (i * 0.001)
        if t >= end_ts:
            t = end_ts - ((count - i) * 0.001)
        out.append(datetime.fromtimestamp(t, tz=timezone.utc))
    out.sort()
    return out


def choose_from_list(vals: List[Any], key: str) -> Any:
    if not vals:
        return ""
    u = stable_u01(key)
    idx = int(math.floor(u * len(vals)))
    idx = min(max(idx, 0), len(vals) - 1)
    return vals[idx]


def gen_uuid_like(key: str) -> str:
    b = bytearray(hashlib.md5(key.encode("utf-8")).digest())
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    return str(uuid.UUID(bytes=bytes(b)))


def gen_hex(key: str, length: int) -> str:
    return md5_hex(key)[:length]


def gen_ip(key: str, cidr: str) -> str:
    net = ipaddress.ip_network(cidr, strict=False)
    u = stable_u01(key)
    if isinstance(net, ipaddress.IPv4Network):
        size = net.num_addresses
        if size <= 2:
            return str(net.network_address)
        host_count = size - 2
        offset = 1 + int(math.floor(u * host_count))
        offset = min(max(offset, 1), size - 2)
        return str(ipaddress.IPv4Address(int(net.network_address) + offset))
    offset = int(math.floor(u * net.num_addresses))
    return str(ipaddress.IPv6Address(int(net.network_address) + offset))


def format_value(v: Any, domain: Optional[Dict[str, Any]] = None) -> str:
    if isinstance(v, float):
        s = f"{v:.2f}"
        s = s.rstrip("0").rstrip(".")
        return s
    return str(v)


@dataclass(frozen=True)
class LogRef:
    component_id: str
    log_id: str


def parse_ref(ref: str) -> LogRef:
    if "." not in ref:
        raise ValueError(f"Bad ref: {ref}")
    c, l = ref.split(".", 1)
    return LogRef(c, l)


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[Tuple[str, str], Dict[str, Any]]]:
    comps = {c["id"]: c for c in system["components"]}
    logs = {}
    for c in system["components"]:
        cid = c["id"]
        for lid, tmpl in c.get("logs", {}).items():
            logs[(cid, lid)] = tmpl
    return comps, logs


COMPONENTS, LOG_TEMPLATES = build_indices(SYSTEM)


def get_component_meta(component_id: str) -> Tuple[str, List[str]]:
    c = COMPONENTS[component_id]
    svc = c.get("svc")
    svc_out = "" if svc is None else str(svc)
    hosts = c.get("hosts", []) or []
    return svc_out, hosts


def get_template(component_id: str, log_id: str) -> Dict[str, Any]:
    return LOG_TEMPLATES[(component_id, log_id)]


def template_placeholders(msg: str) -> List[str]:
    return PLACEHOLDER_RE.findall(msg)


def get_domain_for_var(tmpl: Dict[str, Any], var: str, state: str) -> Optional[Dict[str, Any]]:
    if "vars" in tmpl and var in tmpl["vars"]:
        return tmpl["vars"][var]
    if "state_vars" in tmpl and state in tmpl["state_vars"] and var in tmpl["state_vars"][state]:
        return tmpl["state_vars"][state][var]
    return None


def numeric_domain_bounds(domain: Optional[Dict[str, Any]]) -> Optional[Tuple[float, float]]:
    if not domain:
        return None
    k = domain.get("k")
    v = domain.get("v")
    if k in ("i", "f") and isinstance(v, list) and len(v) == 2:
        return float(v[0]), float(v[1])
    return None


def clamp_num(x: float, lo: float, hi: float) -> float:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def clamp_int_to_domain(tmpl: Dict[str, Any], var: str, state: str, val: int) -> int:
    dom = get_domain_for_var(tmpl, var, state)
    b = numeric_domain_bounds(dom)
    if not b:
        return int(val)
    lo, hi = b
    return int(clamp_num(int(val), int(math.floor(lo)), int(math.floor(hi))))


def gen_from_domain(domain: Dict[str, Any], key: str) -> Any:
    k = domain.get("k")
    v = domain.get("v")
    if k == "ch":
        return choose_from_list(list(v), key)
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        u = stable_u01(key)
        return lo + int(math.floor(u * (hi - lo + 1)))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        u = stable_u01(key)
        return lo + (hi - lo) * u
    if k == "uuid":
        return gen_uuid_like(key)
    if k == "hex":
        return gen_hex(key, int(v))
    if k == "ip":
        return gen_ip(key, str(v))
    if k == "str":
        return f"{key[:8]}"
    raise ValueError(f"Unsupported domain: {domain}")


def render_message(component_id: str, log_id: str, state: str, overrides: Dict[str, Any], key_prefix: str) -> Tuple[str, str]:
    tmpl = get_template(component_id, log_id)
    msg = tmpl["msg"]
    vals: Dict[str, Any] = {}
    for ph in template_placeholders(msg):
        if ph in overrides:
            vals[ph] = overrides[ph]
        else:
            dom = get_domain_for_var(tmpl, ph, state)
            if dom is None:
                vals[ph] = ""
            else:
                vals[ph] = gen_from_domain(dom, f"{key_prefix}|{component_id}.{log_id}|{ph}")
    fmt_vals = {k: format_value(v, get_domain_for_var(tmpl, k, state)) for k, v in vals.items()}
    return msg.format(**fmt_vals), tmpl["lvl"]


def choose_host(hosts: List[str], key: str) -> str:
    if not hosts:
        return ""
    return choose_from_list(hosts, key)


def build_failure_intervals() -> List[Dict[str, Any]]:
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["events"], key=lambda e: (e["at_min"], e.get("order", 0)))
    cuts = [f_start] + sorted([e["at_min"] for e in events if f_start <= e["at_min"] < f_end]) + [f_end]
    rate_mult: Dict[str, float] = {}
    lat_mult: Dict[str, Dict[str, float]] = {}
    intervals = []
    for i in range(len(cuts) - 1):
        start_min = cuts[i]
        end_min = cuts[i + 1]
        for e in events:
            if e["at_min"] == start_min:
                for k, v in e.get("rate_multipliers", {}).items():
                    rate_mult[k] = float(v)
                for k, v in e.get("latency_multipliers", {}).items():
                    lat_mult[k] = {"p50": float(v["p50"]), "p95": float(v["p95"])}
        intervals.append({"start_min": start_min, "end_min": end_min, "rate_mult": dict(rate_mult), "lat_mult": dict(lat_mult)})
    return intervals


FAILURE_INTERVALS = build_failure_intervals()


def get_flow_defs(state: str) -> List[Dict[str, Any]]:
    return SYSTEM["flows"][state]["req"]


def get_flow_mult(interval_ctx: Dict[str, Any], flow_id: str) -> float:
    return float(interval_ctx.get("rate_mult", {}).get(flow_id, 1.0))


def get_bg_mult(interval_ctx: Dict[str, Any], comp_id: str, log_id: str) -> float:
    key = f"{comp_id}.{log_id}"
    return float(interval_ctx.get("rate_mult", {}).get(key, 1.0))


def get_latency_mult(interval_ctx: Dict[str, Any], flow_id: str) -> Tuple[float, float]:
    lm = interval_ctx.get("lat_mult", {}).get(flow_id)
    if not lm:
        return 1.0, 1.0
    return float(lm.get("p50", 1.0)), float(lm.get("p95", 1.0))


def allocate_attempt_counts(n_instances: int, expected: float, max_attempts: int, key_prefix: str) -> List[int]:
    if n_instances <= 0:
        return []
    expected = max(1.0, float(expected))
    max_attempts = int(max_attempts)
    lo = int(math.floor(expected))
    hi = int(math.ceil(expected))
    lo = max(1, min(lo, max_attempts))
    hi = max(1, min(hi, max_attempts))
    if lo == hi:
        return [lo] * n_instances
    p_hi = min(1.0, max(0.0, expected - lo))
    out = []
    for i in range(n_instances):
        u = stable_u01(f"{key_prefix}|attmix|{i}")
        out.append(hi if u < p_hi else lo)
    return out


def simulate() -> pd.DataFrame:
    # Determinism requirement: seed both stdlib random and numpy.
    random.seed(7)
    rng = np.random.default_rng(7)  # fixed seed (not relied upon for core determinism)
    _ = rng

    base = datetime(2021, 12, 1, 0, 0, 0, tzinfo=timezone.utc)
    rows: List[Dict[str, str]] = []

    def emit_log(
        ts: datetime,
        comp_id: str,
        log_id: str,
        state: str,
        overrides: Dict[str, Any],
        trace_id: str,
        host_override: Optional[str],
        key_prefix: str,
    ) -> None:
        msg, lvl = render_message(comp_id, log_id, state, overrides, key_prefix)
        svc, hosts = get_component_meta(comp_id)
        host = host_override if host_override is not None else choose_host(hosts, f"{key_prefix}|host|{comp_id}")
        rows.append({"timestamp": fmt_ts(ts), "level": lvl, "message": msg, "trace_id": trace_id, "service": svc, "host": host})

    def plan_attempt_deltas(
        flow_id: str,
        state: str,
        latency_pairs: List[List[float]],
        key_prefix: str,
        delta_bounds: List[Optional[Tuple[int, int]]],
        span_bounds: Optional[Tuple[int, int]],
    ) -> List[int]:
        # Deltas are "delay since previous emitted log in the same attempt" (one per emitted log).
        deltas: List[int] = []
        for j, (p50, p95) in enumerate(latency_pairs):
            u = stable_u01(f"{key_prefix}|delta|{flow_id}|{state}|{j}")
            d = sample_lognormal_ms(p50, p95, u, soft_cap=3.0 * float(p95))
            b = delta_bounds[j] if j < len(delta_bounds) else None
            if b is not None:
                lo, hi = b
                d = int(clamp_num(d, lo, hi))
                d = max(1, d)
            deltas.append(int(d))

        # If the final emitted log carries a total/span timing field (dur_ms/waited_ms),
        # make the chronology match that field's template domain by adjusting deltas.
        if span_bounds is not None and len(deltas) >= 2:
            span_min, span_max = span_bounds
            # "span" is time from first log (index 0) to last log, so it excludes deltas[0].
            span = int(sum(deltas[1:]))

            # First, prefer adjusting the last delta (usually response return hop) since it has no segment timing field.
            if span < span_min:
                deltas[-1] += (span_min - span)
                span = span_min
            elif span > span_max:
                reduce_by = span - span_max
                take = min(reduce_by, max(0, deltas[-1] - 1))
                deltas[-1] -= take
                span -= take

                # If still too large, reduce earlier deltas (from the end) but never below their own bounds.
                if span > span_max:
                    excess = span - span_max
                    for k in range(len(deltas) - 2, 0, -1):
                        if excess <= 0:
                            break
                        b = delta_bounds[k] if k < len(delta_bounds) else None
                        min_k = b[0] if b is not None else 1
                        can = max(0, deltas[k] - max(1, min_k))
                        take2 = min(excess, can)
                        deltas[k] -= take2
                        excess -= take2
                    # If still excess, we leave it; but for this model, bounds make this very unlikely.
        return deltas

    def timing_bounds_for(comp_id: str, log_id: str, var: str, state: str) -> Optional[Tuple[int, int]]:
        tmpl = get_template(comp_id, log_id)
        dom = get_domain_for_var(tmpl, var, state)
        b = numeric_domain_bounds(dom)
        if not b:
            return None
        lo, hi = b
        return int(math.floor(lo)), int(math.floor(hi))

    def simulate_flow_instance(
        flow: Dict[str, Any],
        state: str,
        start_ts: datetime,
        interval_ctx: Optional[Dict[str, Any]],
        instance_index: int,
        instance_key: str,
    ) -> None:
        flow_id = flow["id"]
        trace_on = bool(flow.get("trace", False)) and SYSTEM["tracing"]["on"]
        trace_id = gen_hex(f"{instance_key}|trace", 32) if trace_on else ""

        comp_host: Dict[str, str] = {}
        for ref in flow["emit"]:
            lr = parse_ref(ref)
            _, hosts = get_component_meta(lr.component_id)
            comp_host[lr.component_id] = choose_host(hosts, f"{instance_key}|stickyhost|{lr.component_id}")
        for ref in flow.get("retry", {}).get("emit_per_retry", []):
            lr = parse_ref(ref)
            _, hosts = get_component_meta(lr.component_id)
            if lr.component_id not in comp_host:
                comp_host[lr.component_id] = choose_host(hosts, f"{instance_key}|stickyhost|{lr.component_id}")

        req_id = gen_uuid_like(f"{instance_key}|req_id")
        client_ip = gen_ip(f"{instance_key}|client_ip", "198.51.100.0/24")
        qname = choose_from_list(
            ["ec2.internal", "auth.internal", "monitoring.internal", "deploy.internal", "events.internal"],
            f"{instance_key}|qname",
        )
        qtype = choose_from_list(["A", "AAAA"], f"{instance_key}|qtype")
        dns_src = choose_from_list(["ec2-api", "monitoring", "deploy", "other-service"], f"{instance_key}|dns_src")
        txid = gen_hex(f"{instance_key}|txid", 8)
        addr = gen_ip(f"{instance_key}|addr", "10.0.0.0/16")
        conn_id = gen_hex(f"{instance_key}|conn_id", 12)

        retry_cfg = flow.get("retry", {})
        max_attempts = int(retry_cfg.get("max_attempts", 1))
        expected_attempts = float(retry_cfg.get("expected_attempts", 1.0))
        attempts = allocate_attempt_counts(1, expected_attempts, max_attempts, f"{instance_key}|attempts")[0]

        p50m, p95m = (1.0, 1.0)
        if state == "f" and interval_ctx is not None:
            p50m, p95m = get_latency_mult(interval_ctx, flow_id)

        base_pairs = flow["latency_ms"]
        eff_pairs = [[pair[0] * p50m, pair[1] * p95m] for pair in base_pairs]

        emit_refs = [parse_ref(r) for r in flow["emit"]]
        # Per-delta constraints: constrain deltas that are used as observed timing fields in messages.
        delta_bounds: List[Optional[Tuple[int, int]]] = [None] * len(emit_refs)

        # Segment timing fields:
        for idx, lr in enumerate(emit_refs):
            if lr.component_id == "ec2_control_plane" and lr.log_id.startswith("rpc_ok_"):
                b = timing_bounds_for(lr.component_id, lr.log_id, "dur_ms", state)
                if b:
                    delta_bounds[idx] = b
            elif lr.component_id == "ec2_control_plane" and lr.log_id.startswith("rpc_deadline_"):
                b = timing_bounds_for(lr.component_id, lr.log_id, "waited_ms", state)
                if b:
                    delta_bounds[idx] = b
            elif lr.component_id == "internal_dns" and lr.log_id in ("reply_ok", "reply_servfail"):
                b = timing_bounds_for(lr.component_id, lr.log_id, "latency_ms", state)
                if b:
                    delta_bounds[idx] = b
            elif lr.component_id == "internal_client_fleet" and lr.log_id == "connect_ok":
                b = timing_bounds_for(lr.component_id, lr.log_id, "rtt_ms", state)
                if b:
                    delta_bounds[idx] = b
            elif lr.component_id == "internal_client_fleet" and lr.log_id == "connect_fail":
                b = timing_bounds_for(lr.component_id, lr.log_id, "waited_ms", state)
                if b:
                    # Also ensure connect_attempt.timeout_ms can stay within its [1500,5000] domain
                    # while still being >= waited_ms + 200 (as encoded by our binding logic).
                    timeout_b = timing_bounds_for("internal_client_fleet", "connect_attempt", "timeout_ms", state)
                    if timeout_b:
                        _, tmax = timeout_b
                        b = (b[0], min(b[1], max(1, tmax - 200)))
                    delta_bounds[idx] = b

        # Span timing field constraints: if the final emitted log has dur_ms/waited_ms we bind it to chronology.
        span_bounds: Optional[Tuple[int, int]] = None
        last_lr = emit_refs[-1]
        if last_lr.component_id == "ec2_api_frontend" and last_lr.log_id in (
            "http_200_describe",
            "http_503_describe",
            "http_200_run",
            "http_503_run",
        ):
            span_bounds = timing_bounds_for(last_lr.component_id, last_lr.log_id, "dur_ms", state)
        elif last_lr.component_id == "ec2_api_frontend" and last_lr.log_id == "dns_ok":
            span_bounds = timing_bounds_for(last_lr.component_id, last_lr.log_id, "dur_ms", state)
        elif last_lr.component_id == "ec2_api_frontend" and last_lr.log_id == "dns_failed":
            span_bounds = timing_bounds_for(last_lr.component_id, last_lr.log_id, "waited_ms", state)

        backoff_pairs = retry_cfg.get("backoff_ms", []) or []
        attempt_start = start_ts
        prev_backoff_ms = 0

        for a in range(1, attempts + 1):
            if a >= 2:
                for rr in retry_cfg.get("emit_per_retry", []) or []:
                    lr = parse_ref(rr)
                    overrides = {}
                    if lr.component_id == "internal_client_fleet" and lr.log_id == "connect_retry":
                        # prev_backoff_ms is already bound to be within the template domain.
                        overrides = {"attempt": a, "backoff_ms": prev_backoff_ms}
                    emit_log(
                        ts=attempt_start + timedelta(milliseconds=1),
                        comp_id=lr.component_id,
                        log_id=lr.log_id,
                        state=state,
                        overrides=overrides,
                        trace_id=trace_id,
                        host_override=comp_host.get(lr.component_id, ""),
                        key_prefix=f"{instance_key}|a{a}|retry",
                    )

            deltas = plan_attempt_deltas(flow_id, state, eff_pairs, f"{instance_key}|a{a}", delta_bounds, span_bounds)

            # Build timestamps from the exact deltas to ensure message timing fields match chronology.
            t = attempt_start
            log_ts: List[datetime] = []
            for d in deltas:
                t = t + timedelta(milliseconds=int(d))
                log_ts.append(t)

            # Span is from first emitted log to last emitted log (excludes deltas[0]).
            span_ms = int(sum(deltas[1:])) if len(deltas) >= 2 else 0
            if span_bounds is not None:
                # Should already be in-bounds; keep a final clamp consistent with our adjusted deltas.
                lo, hi = span_bounds
                span_ms = int(clamp_num(span_ms, lo, hi))

            for idx, ref in enumerate(flow["emit"]):
                lr = parse_ref(ref)
                overrides2: Dict[str, Any] = {}

                if lr.component_id == "ec2_api_frontend":
                    if lr.log_id in ("http_req_describe", "http_req_run"):
                        overrides2 = {"client_ip": client_ip, "req_id": req_id, "trace_id": trace_id}
                    elif lr.log_id in ("http_200_describe", "http_503_describe", "http_200_run", "http_503_run"):
                        tmpl = get_template(lr.component_id, lr.log_id)
                        overrides2 = {"req_id": req_id, "dur_ms": clamp_int_to_domain(tmpl, "dur_ms", state, span_ms)}
                    elif lr.log_id == "dns_resolve":
                        overrides2 = {"qname": qname, "req_id": req_id}
                    elif lr.log_id == "dns_ok":
                        tmpl = get_template(lr.component_id, lr.log_id)
                        overrides2 = {"qname": qname, "addr": addr, "req_id": req_id, "dur_ms": clamp_int_to_domain(tmpl, "dur_ms", state, span_ms)}
                    elif lr.log_id == "dns_failed":
                        tmpl = get_template(lr.component_id, lr.log_id)
                        err = "SERVFAIL" if flow_id.endswith("servfail") else "timeout"
                        overrides2 = {"qname": qname, "req_id": req_id, "err": err, "waited_ms": clamp_int_to_domain(tmpl, "waited_ms", state, span_ms)}

                if lr.component_id == "ec2_control_plane":
                    if lr.log_id in ("rpc_start_describe", "rpc_start_run"):
                        overrides2 = {"req_id": req_id, "caller": "ec2-api"}
                    elif lr.log_id in ("rpc_ok_describe", "rpc_ok_run"):
                        seg_ms = int(deltas[idx])
                        tmpl = get_template(lr.component_id, lr.log_id)
                        overrides2 = {"req_id": req_id, "dur_ms": clamp_int_to_domain(tmpl, "dur_ms", state, seg_ms)}
                    elif lr.log_id in ("rpc_deadline_describe", "rpc_deadline_run"):
                        seg_ms = int(deltas[idx])
                        tmpl = get_template(lr.component_id, lr.log_id)
                        overrides2 = {"req_id": req_id, "waited_ms": clamp_int_to_domain(tmpl, "waited_ms", state, seg_ms)}

                if lr.component_id == "internal_dns":
                    if lr.log_id == "query_recv":
                        overrides2 = {"qname": qname, "qtype": qtype, "src": dns_src, "txid": txid}
                    elif lr.log_id == "reply_ok":
                        seg_ms = int(deltas[idx])
                        tmpl = get_template(lr.component_id, lr.log_id)
                        rrs = 1 + int(math.floor(stable_u01(f"{instance_key}|rrs") * 6))
                        overrides2 = {"qname": qname, "rrs": rrs, "latency_ms": clamp_int_to_domain(tmpl, "latency_ms", state, seg_ms)}
                    elif lr.log_id == "reply_servfail":
                        seg_ms = int(deltas[idx])
                        tmpl = get_template(lr.component_id, lr.log_id)
                        reason = choose_from_list(["upstream_timeout", "queue_overflow"], f"{instance_key}|reason")
                        overrides2 = {"qname": qname, "latency_ms": clamp_int_to_domain(tmpl, "latency_ms", state, seg_ms), "reason": reason}

                if lr.component_id == "internal_client_fleet":
                    if lr.log_id == "connect_attempt":
                        # Use delta[1] (time to accept/fail) when present to keep timeout coherent and in-domain.
                        waited_ms = int(deltas[1]) if len(deltas) >= 2 else 0
                        tmpl = get_template(lr.component_id, lr.log_id)
                        base_timeout = int(gen_from_domain({"k": "i", "v": [1500, 5000]}, f"{instance_key}|timeout|a{a}"))
                        timeout_ms = max(base_timeout, waited_ms + 200)
                        timeout_ms = clamp_int_to_domain(tmpl, "timeout_ms", state, timeout_ms)
                        overrides2 = {"timeout_ms": timeout_ms, "dst_svc": "capacity-service", "edge": "edge-gw"}
                    elif lr.log_id == "connect_ok":
                        seg_ms = int(deltas[idx])
                        tmpl = get_template(lr.component_id, lr.log_id)
                        overrides2 = {"dst_svc": "capacity-service", "rtt_ms": clamp_int_to_domain(tmpl, "rtt_ms", state, seg_ms)}
                    elif lr.log_id == "connect_fail":
                        seg_ms = int(deltas[idx])
                        tmpl = get_template(lr.component_id, lr.log_id)
                        if state == "f":
                            err = choose_from_list(["timeout", "timeout", "reset", "no_route"], f"{instance_key}|err|f|a{a}")
                        else:
                            err = choose_from_list(["timeout", "reset", "no_route"], f"{instance_key}|err|n|a{a}")
                        overrides2 = {"dst_svc": "capacity-service", "err": err, "waited_ms": clamp_int_to_domain(tmpl, "waited_ms", state, seg_ms)}

                if lr.component_id == "capacity_service":
                    if lr.log_id == "conn_accept":
                        overrides2 = {"src": "internal-client", "conn_id": conn_id}

                emit_log(
                    ts=log_ts[idx],
                    comp_id=lr.component_id,
                    log_id=lr.log_id,
                    state=state,
                    overrides=overrides2,
                    trace_id=trace_id,
                    host_override=comp_host.get(lr.component_id, ""),
                    key_prefix=f"{instance_key}|a{a}|e{idx}",
                )

            attempt_end = log_ts[-1]
            if a < attempts:
                bi = min(a - 1, len(backoff_pairs) - 1)
                if bi >= 0 and len(backoff_pairs) > 0:
                    p50, p95 = backoff_pairs[bi]
                else:
                    p50, p95 = (50.0, 200.0)
                u = stable_u01(f"{instance_key}|backoff|a{a}")
                bo = sample_lognormal_ms(p50, p95, u, soft_cap=3.0 * float(p95))

                # If the retry marker logs backoff_ms, bind it to the template domain too.
                # This keeps both the message field and the attempt spacing in-bounds.
                retry_refs = retry_cfg.get("emit_per_retry", []) or []
                if any(r.endswith("internal_client_fleet.connect_retry") for r in retry_refs):
                    tmpl = get_template("internal_client_fleet", "connect_retry")
                    bo = clamp_int_to_domain(tmpl, "backoff_ms", state, bo)

                prev_backoff_ms = int(bo)
                attempt_start = attempt_end + timedelta(milliseconds=int(prev_backoff_ms))

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    n_start_ts = base + timedelta(minutes=n_start)
    n_end_ts = base + timedelta(minutes=n_end)

    # Normal background
    for comp in SYSTEM["components"]:
        comp_id = comp["id"]
        beh = comp.get("beh", {}).get("n", {}).get("emit", []) or []
        for src in beh:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            _, hosts = get_component_meta(comp_id)
            if scope == "global":
                expect = per_min * (n_end - n_start)
                count = deterministic_round(expect, f"bg|n|{comp_id}.{log_id}|global")
                times = even_times(n_start_ts, n_end_ts, count, f"bg|n|{comp_id}.{log_id}|global")
                for i, ts in enumerate(times):
                    emit_log(ts, comp_id, log_id, "n", {}, "", None, f"bg|n|{comp_id}.{log_id}|{i}")
            else:
                for h in hosts:
                    expect = per_min * (n_end - n_start)
                    count = deterministic_round(expect, f"bg|n|{comp_id}.{log_id}|{h}")
                    times = even_times(n_start_ts, n_end_ts, count, f"bg|n|{comp_id}.{log_id}|{h}")
                    for i, ts in enumerate(times):
                        emit_log(ts, comp_id, log_id, "n", {}, "", h, f"bg|n|{comp_id}.{log_id}|{h}|{i}")

    # Normal flows
    for flow in get_flow_defs("n"):
        flow_id = flow["id"]
        rpm = float(flow["rpm"])
        expect = rpm * (n_end - n_start)
        n_instances = deterministic_round(expect, f"flow|n|{flow_id}")
        starts = even_times(n_start_ts, n_end_ts, n_instances, f"flow|n|{flow_id}")
        for i, st in enumerate(starts):
            simulate_flow_instance(flow, "n", st, None, i, f"flow|n|{flow_id}|{i}")

    # Failure background per interval
    for interval in FAILURE_INTERVALS:
        start_min = interval["start_min"]
        end_min = interval["end_min"]
        start_ts = base + timedelta(minutes=start_min)
        end_ts = base + timedelta(minutes=end_min)
        dur_min = end_min - start_min
        for comp in SYSTEM["components"]:
            comp_id = comp["id"]
            beh = comp.get("beh", {}).get("f", {}).get("emit", []) or []
            for src in beh:
                log_id = src["id"]
                per_min = float(src["per_min"])
                scope = src.get("scope", "per_host")
                mult = get_bg_mult(interval, comp_id, log_id)
                eff = per_min * mult
                if eff <= 0:
                    continue
                _, hosts = get_component_meta(comp_id)
                if scope == "global":
                    expect = eff * dur_min
                    count = deterministic_round(expect, f"bg|f|{start_min}-{end_min}|{comp_id}.{log_id}|global")
                    times = even_times(start_ts, end_ts, count, f"bg|f|{start_min}-{end_min}|{comp_id}.{log_id}|global")
                    for i, ts in enumerate(times):
                        emit_log(ts, comp_id, log_id, "f", {}, "", None, f"bg|f|{start_min}-{end_min}|{comp_id}.{log_id}|{i}")
                else:
                    for h in hosts:
                        expect = eff * dur_min
                        count = deterministic_round(expect, f"bg|f|{start_min}-{end_min}|{comp_id}.{log_id}|{h}")
                        times = even_times(start_ts, end_ts, count, f"bg|f|{start_min}-{end_min}|{comp_id}.{log_id}|{h}")
                        for i, ts in enumerate(times):
                            emit_log(ts, comp_id, log_id, "f", {}, "", h, f"bg|f|{start_min}-{end_min}|{comp_id}.{log_id}|{h}|{i}")

    # Failure flows per interval
    for interval in FAILURE_INTERVALS:
        start_min = interval["start_min"]
        end_min = interval["end_min"]
        start_ts = base + timedelta(minutes=start_min)
        end_ts = base + timedelta(minutes=end_min)
        dur_min = end_min - start_min
        for flow in get_flow_defs("f"):
            flow_id = flow["id"]
            mult = get_flow_mult(interval, flow_id)
            eff_rpm = float(flow["rpm"]) * mult
            if eff_rpm <= 0:
                continue
            expect = eff_rpm * dur_min
            n_instances = deterministic_round(expect, f"flow|f|{start_min}-{end_min}|{flow_id}")
            starts = even_times(start_ts, end_ts, n_instances, f"flow|f|{start_min}-{end_min}|{flow_id}")
            for i, st in enumerate(starts):
                simulate_flow_instance(flow, "f", st, interval, i, f"flow|f|{start_min}-{end_min}|{flow_id}|{i}")

    # One-shots
    for e in sorted(SCENARIO["events"], key=lambda x: (x["at_min"], x.get("order", 0))):
        at_min = int(e["at_min"])
        event_ts = base + timedelta(minutes=at_min)
        for os in e.get("one_shots", []) or []:
            lr = parse_ref(os["ref"])
            count = int(os["count"])
            allowed_hosts = os.get("hosts", []) or []
            times = []
            for i in range(count):
                jitter = (stable_u01(f"oneshot|{at_min}|{os['ref']}|{i}") - 0.5) * 1.0
                times.append(event_ts + timedelta(seconds=jitter))
            times.sort()
            for i, ts in enumerate(times):
                host = allowed_hosts[i % len(allowed_hosts)] if allowed_hosts else None
                emit_log(ts, lr.component_id, lr.log_id, "f", {}, "", host, f"oneshot|{at_min}|{os['ref']}|{i}")

    df = pd.DataFrame(rows, columns=["timestamp", "level", "message", "trace_id", "service", "host"])
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)

    if list(df.columns) != ["timestamp", "level", "message", "trace_id", "service", "host"]:
        raise RuntimeError("Bad CSV columns")
    if not (20000 <= len(df) <= 100000):
        raise RuntimeError(f"Row count out of range: {len(df)}")
    if not df["timestamp"].is_monotonic_increasing:
        raise RuntimeError("Timestamps not sorted")
    return df


def main() -> None:
    df = simulate()
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
