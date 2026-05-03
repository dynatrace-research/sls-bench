import math
import re
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd


SYSTEM: Dict[str, Any] = {
    "sys": {"id": "code_hosting_dns_stack"},
    "components": [
        {
            "id": "net_edge",
            "svc": "net-edge",
            "hosts": ["edge-1"],
            "logs": {
                "dns_drop": {
                    "lvl": "WARN",
                    "msg": "drop dns packet dst_ip={dst_ip} reason={reason} rule={rule}",
                    "vars": {
                        "dst_ip": {"k": "ch", "v": ["10.0.10.9"]},
                        "reason": {"k": "ch", "v": ["not_whitelisted"]},
                        "rule": {"k": "ch", "v": ["dns_whitelist_v2"]},
                    },
                },
                "acl_update": {
                    "lvl": "INFO",
                    "msg": "applied dns acl rule={rule} allowed_src_set={allowed_src_set} change_id={change_id}",
                    "vars": {
                        "rule": {"k": "ch", "v": ["dns_whitelist_v2"]},
                        "allowed_src_set": {"k": "ch", "v": ["ns_src_ips_v2"]},
                        "change_id": {"k": "hex", "v": 8},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "dns_drop", "per_min": 0.05, "scope": "global"}]},
                "f": {"emit": [{"id": "dns_drop", "per_min": 6.0, "scope": "global"}]},
            },
        },
        {
            "id": "dns_cache_ns",
            "svc": "dns-cache",
            "hosts": ["dns-cache-1"],
            "logs": {
                "query_recv": {
                    "lvl": "INFO",
                    "msg": "recv query id={qid} qname={qname} qtype={qtype} client_ip={client_ip}",
                    "vars": {
                        "qid": {"k": "i", "v": [1000, 9999]},
                        "qname": {
                            "k": "ch",
                            "v": [
                                "github.com",
                                "api.github.com",
                                "gist.github.com",
                                "assets.github.com",
                                "provisioning.service.local",
                            ],
                        },
                        "qtype": {"k": "ch", "v": ["A", "AAAA", "NS"]},
                        "client_ip": {"k": "ip", "v": "198.51.100.0/24"},
                    },
                },
                "upstream_retry": {
                    "lvl": "WARN",
                    "msg": "retry upstream auth={auth} attempt={attempt} backoff_ms={backoff_ms}",
                    "vars": {
                        "auth": {"k": "ch", "v": ["dns-auth-1"]},
                        "attempt": {"k": "i", "v": [2, 2]},
                        "backoff_ms": {"k": "i", "v": [20, 450]},
                    },
                },
                "query_timeout": {
                    "lvl": "ERROR",
                    "msg": "upstream timeout qname={qname} waited_ms={waited_ms} rcode=SERVFAIL",
                    "vars": {
                        "qname": {
                            "k": "ch",
                            "v": [
                                "github.com",
                                "api.github.com",
                                "gist.github.com",
                                "assets.github.com",
                                "provisioning.service.local",
                            ],
                        },
                        "waited_ms": {"k": "i", "v": [900, 2200]},
                    },
                },
                "respond_noerror": {
                    "lvl": "INFO",
                    "msg": "respond qname={qname} rcode=NOERROR answer_count={answer_count} dur_ms={dur_ms}",
                    "vars": {
                        "qname": {
                            "k": "ch",
                            "v": [
                                "github.com",
                                "api.github.com",
                                "gist.github.com",
                                "assets.github.com",
                                "provisioning.service.local",
                            ],
                        },
                        "answer_count": {"k": "i", "v": [1, 10]},
                        "dur_ms": {"k": "i", "v": [1, 400]},
                    },
                },
                "respond_nxdomain": {
                    "lvl": "INFO",
                    "msg": "respond qname={qname} rcode=NXDOMAIN answer_count=0 dur_ms={dur_ms}",
                    "vars": {
                        "qname": {
                            "k": "ch",
                            "v": [
                                "github.com",
                                "api.github.com",
                                "gist.github.com",
                                "assets.github.com",
                                "provisioning.service.local",
                            ],
                        },
                        "dur_ms": {"k": "i", "v": [1, 200]},
                    },
                },
                "dns_stats": {
                    "lvl": "INFO",
                    "msg": "stats qps={qps} upstream_timeouts={upstream_timeouts} servfail={servfail} nxdomain={nxdomain}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "qps": {"k": "i", "v": [2, 7]},
                            "upstream_timeouts": {"k": "i", "v": [0, 5]},
                            "servfail": {"k": "i", "v": [0, 8]},
                            "nxdomain": {"k": "i", "v": [0, 15]},
                        },
                        "f": {
                            "qps": {"k": "i", "v": [2, 10]},
                            "upstream_timeouts": {"k": "i", "v": [5, 180]},
                            "servfail": {"k": "i", "v": [5, 180]},
                            "nxdomain": {"k": "i", "v": [0, 140]},
                        },
                    },
                },
                "dns_stats_spike": {
                    "lvl": "INFO",
                    "msg": "stats spike qps={qps} upstream_timeouts={upstream_timeouts} servfail={servfail} nxdomain={nxdomain}",
                    "vars": {
                        "qps": {"k": "i", "v": [2, 10]},
                        "upstream_timeouts": {"k": "i", "v": [0, 40]},
                        "servfail": {"k": "i", "v": [0, 40]},
                        "nxdomain": {"k": "i", "v": [80, 220]},
                    },
                },
                "nxdomain_alert": {
                    "lvl": "WARN",
                    "msg": "anomaly nxdomain_high nxdomain_1m={nxdomain_1m} qps={qps} top_qname={top_qname}",
                    "vars": {
                        "nxdomain_1m": {"k": "i", "v": [80, 220]},
                        "qps": {"k": "i", "v": [2, 10]},
                        "top_qname": {"k": "ch", "v": ["github.com", "api.github.com", "assets.github.com"]},
                    },
                },
                "reload": {
                    "lvl": "INFO",
                    "msg": "service reload requested_by={requested_by} result={result}",
                    "vars": {"requested_by": {"k": "ch", "v": ["oncall"]}, "result": {"k": "ch", "v": ["ok"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "dns_stats", "per_min": 1.0}]},
                "f": {
                    "emit": [
                        {"id": "dns_stats", "per_min": 1.0},
                        {"id": "dns_stats_spike", "per_min": 1.0},
                        {"id": "nxdomain_alert", "per_min": 0.5},
                    ]
                },
            },
        },
        {
            "id": "dns_auth_ns",
            "svc": "dns-auth",
            "hosts": ["dns-auth-1"],
            "logs": {
                "serve_noerror": {
                    "lvl": "INFO",
                    "msg": "served qname={qname} rcode=NOERROR rrset_size={rrset_size} dur_ms={dur_ms}",
                    "vars": {
                        "qname": {
                            "k": "ch",
                            "v": [
                                "github.com",
                                "api.github.com",
                                "gist.github.com",
                                "assets.github.com",
                                "provisioning.service.local",
                            ],
                        },
                        "rrset_size": {"k": "i", "v": [1, 6]},
                        "dur_ms": {"k": "i", "v": [1, 80]},
                    },
                },
                "serve_nxdomain": {
                    "lvl": "INFO",
                    "msg": "served qname={qname} rcode=NXDOMAIN rrset_size=0 dur_ms={dur_ms}",
                    "vars": {
                        "qname": {
                            "k": "ch",
                            "v": [
                                "github.com",
                                "api.github.com",
                                "gist.github.com",
                                "assets.github.com",
                                "provisioning.service.local",
                            ],
                        },
                        "dur_ms": {"k": "i", "v": [1, 80]},
                    },
                },
                "zone_reload": {
                    "lvl": "WARN",
                    "msg": "zone reload zone={zone} serial={serial} records={records} status={status}",
                    "vars": {"zone": {"k": "ch", "v": ["github.com"]}, "serial": {"k": "i", "v": [2026010801, 2026010810]}},
                    "state_vars": {
                        "n": {"records": {"k": "i", "v": [5000, 8000]}, "status": {"k": "ch", "v": ["loaded"]}},
                        "f": {"records": {"k": "i", "v": [200, 2500]}, "status": {"k": "ch", "v": ["loaded", "error"]}},
                    },
                },
                "auth_stats": {
                    "lvl": "INFO",
                    "msg": "stats qps={qps} nxdomain={nxdomain} zones={zones}",
                    "vars": {},
                    "state_vars": {
                        "n": {"qps": {"k": "i", "v": [1, 6]}, "nxdomain": {"k": "i", "v": [0, 12]}, "zones": {"k": "i", "v": [1, 1]}},
                        "f": {"qps": {"k": "i", "v": [1, 7]}, "nxdomain": {"k": "i", "v": [0, 140]}, "zones": {"k": "i", "v": [1, 1]}},
                    },
                },
                "auth_stats_spike": {
                    "lvl": "INFO",
                    "msg": "stats spike qps={qps} nxdomain={nxdomain} zones={zones}",
                    "vars": {"qps": {"k": "i", "v": [1, 7]}, "nxdomain": {"k": "i", "v": [80, 220]}, "zones": {"k": "i", "v": [1, 1]}},
                },
                "nxdomain_alert": {
                    "lvl": "WARN",
                    "msg": "zone anomaly nxdomain_high zone={zone} nxdomain_1m={nxdomain_1m} records={records}",
                    "vars": {"zone": {"k": "ch", "v": ["github.com"]}, "nxdomain_1m": {"k": "i", "v": [80, 220]}, "records": {"k": "i", "v": [200, 2500]}},
                },
                "reload": {
                    "lvl": "INFO",
                    "msg": "service reload requested_by={requested_by} result={result}",
                    "vars": {"requested_by": {"k": "ch", "v": ["oncall"]}, "result": {"k": "ch", "v": ["ok"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "auth_stats", "per_min": 0.5}]},
                "f": {"emit": [{"id": "auth_stats", "per_min": 0.5}, {"id": "auth_stats_spike", "per_min": 0.5}, {"id": "nxdomain_alert", "per_min": 0.3}]},
            },
        },
        {
            "id": "config_deployer",
            "svc": "deployer",
            "hosts": ["deploy-1"],
            "logs": {
                "puppet_apply": {
                    "lvl": "INFO",
                    "msg": "puppet apply manifest={manifest} changed={changed} run_id={run_id}",
                    "vars": {"manifest": {"k": "ch", "v": ["dns_acl_ip_rollout"]}, "changed": {"k": "i", "v": [1, 25]}, "run_id": {"k": "hex", "v": 10}},
                },
                "dns_deploy_start": {"lvl": "INFO", "msg": "dns deploy start rev={rev} target={target}", "vars": {"rev": {"k": "hex", "v": 7}, "target": {"k": "ch", "v": ["dns-auth-1"]}}},
                "zone_build": {
                    "lvl": "WARN",
                    "msg": "zone build input api_status={api_status} items={items}",
                    "vars": {},
                    "state_vars": {"n": {"api_status": {"k": "ch", "v": ["ok"]}, "items": {"k": "i", "v": [5000, 8000]}}, "f": {"api_status": {"k": "ch", "v": ["dns_timeout", "http_503"]}, "items": {"k": "i", "v": [0, 400]}}},
                },
                "dns_deploy_end": {
                    "lvl": "WARN",
                    "msg": "dns deploy end status={status} removed_records_pct={removed_records_pct}",
                    "vars": {},
                    "state_vars": {
                        "n": {"status": {"k": "ch", "v": ["success"]}, "removed_records_pct": {"k": "f", "v": [0.0, 5.0]}},
                        "f": {"status": {"k": "ch", "v": ["success_with_warnings"]}, "removed_records_pct": {"k": "f", "v": [40.0, 85.0]}},
                    },
                },
                "manual_zone_restore": {"lvl": "INFO", "msg": "manual zone restore zone={zone} records={records} serial={serial}", "vars": {"zone": {"k": "ch", "v": ["github.com"]}, "records": {"k": "i", "v": [5000, 8000]}, "serial": {"k": "i", "v": [2026010805, 2026010815]}}},
                "heartbeat": {"lvl": "DEBUG", "msg": "scheduler heartbeat queue_depth={queue_depth}", "vars": {"queue_depth": {"k": "i", "v": [0, 5]}}},
            },
            "beh": {"n": {"emit": [{"id": "heartbeat", "per_min": 0.05}]}, "f": {"emit": [{"id": "heartbeat", "per_min": 0.05}]}},
        },
        {
            "id": "provisioning_api",
            "svc": "provisioning",
            "hosts": ["prov-1"],
            "logs": {"health": {"lvl": "INFO", "msg": "health ok=1 req_1m={req_1m} p95_ms={p95_ms}", "vars": {"req_1m": {"k": "i", "v": [0, 50]}, "p95_ms": {"k": "i", "v": [5, 120]}}}},
            "beh": {"n": {"emit": [{"id": "health", "per_min": 0.2}]}, "f": {"emit": [{"id": "health", "per_min": 0.2}]}},
        },
        {
            "id": "web_frontend",
            "svc": "web",
            "hosts": ["web-1", "web-2"],
            "logs": {
                "http_req": {"lvl": "INFO", "msg": "http {method} {route} repo={repo} client_ip={client_ip}", "vars": {"method": {"k": "ch", "v": ["GET", "POST"]}, "route": {"k": "ch", "v": ["/alpha.git/info/refs", "/beta.git/git-upload-pack", "/gamma.git/git-receive-pack", "/"]}, "repo": {"k": "ch", "v": ["alpha", "beta", "gamma", "delta", "epsilon"]}, "client_ip": {"k": "ip", "v": "203.0.113.0/24"}}},
                "upstream_err": {"lvl": "ERROR", "msg": "upstream failure backend={backend} repo={repo} err={err}", "vars": {"backend": {"k": "ch", "v": ["fs-01", "fs-02", "fs-03", "fs-04"]}, "repo": {"k": "ch", "v": ["alpha", "beta", "gamma", "delta", "epsilon"]}, "err": {"k": "ch", "v": ["connect_refused", "timeout", "reset"]}}},
                "http_resp_ok": {"lvl": "INFO", "msg": "http completed status=200 repo={repo} dur_ms={dur_ms}", "vars": {"repo": {"k": "ch", "v": ["alpha", "beta", "gamma", "delta", "epsilon"]}, "dur_ms": {"k": "i", "v": [20, 1500]}}},
                "http_resp_err": {"lvl": "WARN", "msg": "http completed status={status} repo={repo} dur_ms={dur_ms}", "vars": {"status": {"k": "i", "v": [502, 503]}, "repo": {"k": "ch", "v": ["alpha", "beta", "gamma", "delta", "epsilon"]}, "dur_ms": {"k": "i", "v": [50, 6000]}}},
                "worker_stats": {
                    "lvl": "INFO",
                    "msg": "workers busy={busy} total={total} queue={queue}",
                    "vars": {},
                    "state_vars": {
                        "n": {"busy": {"k": "i", "v": [5, 40]}, "total": {"k": "i", "v": [48, 48]}, "queue": {"k": "i", "v": [0, 10]}},
                        "f": {"busy": {"k": "i", "v": [10, 48]}, "total": {"k": "i", "v": [48, 48]}, "queue": {"k": "i", "v": [0, 80]}},
                    },
                },
            },
            "beh": {"n": {"emit": [{"id": "worker_stats", "per_min": 0.2}]}, "f": {"emit": [{"id": "worker_stats", "per_min": 0.2}]}},
        },
        {
            "id": "routing_lb",
            "svc": "routing",
            "hosts": ["router-1"],
            "logs": {
                "backend_select": {"lvl": "INFO", "msg": "route repo={repo} backend={backend} decision={decision}", "vars": {"repo": {"k": "ch", "v": ["alpha", "beta", "gamma", "delta", "epsilon"]}, "backend": {"k": "ch", "v": ["fs-01", "fs-02", "fs-03", "fs-04"]}, "decision": {"k": "ch", "v": ["consistent_hash"]}}},
                "conn_refused": {"lvl": "ERROR", "msg": "connect failed backend={backend} errno=ECONNREFUSED in_flight={in_flight} queue={queue}", "vars": {"backend": {"k": "ch", "v": ["fs-01", "fs-02", "fs-03", "fs-04"]}, "in_flight": {"k": "i", "v": [50, 500]}, "queue": {"k": "i", "v": [0, 250]}}},
                "pool_stats": {
                    "lvl": "INFO",
                    "msg": "pool stats total={total} available={available} unhealthy={unhealthy}",
                    "vars": {},
                    "state_vars": {"n": {"total": {"k": "i", "v": [4, 4]}, "available": {"k": "i", "v": [4, 4]}, "unhealthy": {"k": "i", "v": [0, 0]}}, "f": {"total": {"k": "i", "v": [4, 4]}, "available": {"k": "i", "v": [1, 4]}, "unhealthy": {"k": "i", "v": [0, 3]}}},
                },
                "pool_degraded": {"lvl": "WARN", "msg": "pool degraded unhealthy={unhealthy} available={available} reason={reason}", "vars": {"unhealthy": {"k": "i", "v": [1, 4]}, "available": {"k": "i", "v": [0, 3]}, "reason": {"k": "ch", "v": ["conn_refused", "backend_overload"]}}},
                "backend_removed": {"lvl": "INFO", "msg": "removed backend={backend} reason={reason}", "vars": {"backend": {"k": "ch", "v": ["fs-01", "fs-02", "fs-03", "fs-04"]}, "reason": {"k": "ch", "v": ["manual_triage", "conn_refused", "unhealthy"]}}},
            },
            "beh": {"n": {"emit": [{"id": "pool_stats", "per_min": 1.0}]}, "f": {"emit": [{"id": "pool_stats", "per_min": 1.0}, {"id": "pool_degraded", "per_min": 1.0}]}},
        },
        {
            "id": "fileserver_pool",
            "svc": "fileserver",
            "hosts": ["fs-01", "fs-02", "fs-03", "fs-04"],
            "logs": {
                "serve_git": {
                    "lvl": "INFO",
                    "msg": "git op={op} repo={repo} status=200 bytes={bytes} dur_ms={dur_ms}",
                    "vars": {"op": {"k": "ch", "v": ["upload-pack", "receive-pack", "http-get"]}, "repo": {"k": "ch", "v": ["alpha", "beta", "gamma", "delta", "epsilon"]}, "bytes": {"k": "i", "v": [1024, 52428800]}, "dur_ms": {"k": "i", "v": [5, 2500]}},
                },
                "mem_pressure": {
                    "lvl": "WARN",
                    "msg": "resource pressure procs={procs} rss_mb={rss_mb} load1={load1}",
                    "vars": {},
                    "state_vars": {
                        "n": {"procs": {"k": "i", "v": [80, 220]}, "rss_mb": {"k": "i", "v": [2048, 6144]}, "load1": {"k": "f", "v": [0.5, 3.5]}},
                        "f": {"procs": {"k": "i", "v": [300, 4500]}, "rss_mb": {"k": "i", "v": [4096, 32000]}, "load1": {"k": "f", "v": [2.0, 70.0]}},
                    },
                },
                "ha_stonith": {"lvl": "INFO", "msg": "ha failover action=stonith primary={primary} secondary={secondary} reason={reason}", "vars": {"primary": {"k": "ch", "v": ["fs-01", "fs-02", "fs-03", "fs-04"]}, "secondary": {"k": "ch", "v": ["fs-01b", "fs-02b", "fs-03b", "fs-04b"]}, "reason": {"k": "ch", "v": ["memory_exhaustion", "unresponsive"]}}},
                "kill_hung_procs": {"lvl": "INFO", "msg": "operator killed hung processes killed={killed} remaining={remaining}", "vars": {"killed": {"k": "i", "v": [50, 2000]}, "remaining": {"k": "i", "v": [50, 800]}}},
            },
            "beh": {
                "n": {"emit": [{"id": "mem_pressure", "per_min": 0.3, "scope": "per_host"}]},
                "f": {"emit": [{"id": "mem_pressure", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
    ],
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "flows": {
        "n": {
            "req": [
                {
                    "id": "external_dns_query_ok",
                    "rpm": 240.0,
                    "emit": ["dns_cache_ns.query_recv", "dns_auth_ns.serve_noerror", "dns_cache_ns.respond_noerror"],
                    "latency_ms": [[1, 4], [2, 25], [2, 30]],
                    "retry": {"max_attempts": 2, "expected_attempts": 1.05, "emit_per_retry": ["dns_cache_ns.upstream_retry"], "backoff_ms": [[20, 60]]},
                    "trace": False,
                },
                {
                    "id": "web_repo_request_ok",
                    "rpm": 140.0,
                    "emit": ["web_frontend.http_req", "routing_lb.backend_select", "fileserver_pool.serve_git", "web_frontend.http_resp_ok"],
                    "latency_ms": [[1, 3], [2, 10], [10, 600], [20, 1200]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "external_dns_query_timeout",
                    "rpm": 180.0,
                    "emit": ["dns_cache_ns.query_recv", "dns_cache_ns.query_timeout"],
                    "latency_ms": [[1, 4], [1100, 2200]],
                    "retry": {"max_attempts": 2, "expected_attempts": 1.8, "emit_per_retry": ["dns_cache_ns.upstream_retry"], "backoff_ms": [[200, 450]]},
                    "trace": False,
                },
                {
                    "id": "external_dns_query_ok_degraded",
                    "rpm": 80.0,
                    "emit": ["dns_cache_ns.query_recv", "dns_auth_ns.serve_noerror", "dns_cache_ns.respond_noerror"],
                    "latency_ms": [[1, 5], [5, 80], [10, 400]],
                    "retry": {"max_attempts": 2, "expected_attempts": 1.2, "emit_per_retry": ["dns_cache_ns.upstream_retry"], "backoff_ms": [[50, 120]]},
                    "trace": False,
                },
                {
                    "id": "external_dns_query_nxdomain",
                    "rpm": 120.0,
                    "emit": ["dns_cache_ns.query_recv", "dns_auth_ns.serve_nxdomain", "dns_cache_ns.respond_nxdomain"],
                    "latency_ms": [[1, 4], [2, 40], [2, 60]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "web_repo_request_ok_low",
                    "rpm": 40.0,
                    "emit": ["web_frontend.http_req", "routing_lb.backend_select", "fileserver_pool.serve_git", "web_frontend.http_resp_ok"],
                    "latency_ms": [[1, 3], [2, 15], [20, 1200], [30, 2000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "web_repo_request_502",
                    "rpm": 70.0,
                    "emit": ["web_frontend.http_req", "routing_lb.conn_refused", "web_frontend.upstream_err", "web_frontend.http_resp_err"],
                    "latency_ms": [[1, 3], [10, 200], [5, 60], [50, 4000]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "github_dns_outage_progression_slice",
        "time": {"total_minutes": 60, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 60}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 25,
                        "rate_multipliers": {
                            "external_dns_query_nxdomain": 0.0,
                            "web_repo_request_502": 0.0,
                            "net_edge.dns_drop": 3.0,
                            "routing_lb.pool_degraded": 0.0,
                            "dns_cache_ns.nxdomain_alert": 0.0,
                            "dns_auth_ns.nxdomain_alert": 0.0,
                            "dns_cache_ns.dns_stats_spike": 0.0,
                            "dns_auth_ns.auth_stats_spike": 0.0,
                        },
                        "latency_multipliers": {"external_dns_query_timeout": {"p50": 1.0, "p95": 1.0}, "external_dns_query_ok_degraded": {"p50": 1.2, "p95": 1.4}},
                        "one_shots": [
                            {"ref": "net_edge.acl_update", "count": 1, "hosts": ["edge-1"]},
                            {"ref": "config_deployer.puppet_apply", "count": 2, "hosts": ["deploy-1"]},
                            {"ref": "dns_auth_ns.reload", "count": 1, "hosts": ["dns-auth-1"]},
                        ],
                    },
                    {
                        "order": 2,
                        "at_min": 33,
                        "rate_multipliers": {"external_dns_query_timeout": 0.15, "external_dns_query_ok_degraded": 1.5, "net_edge.dns_drop": 0.3},
                        "latency_multipliers": {"external_dns_query_ok_degraded": {"p50": 0.8, "p95": 0.8}},
                        "one_shots": [{"ref": "dns_cache_ns.reload", "count": 1, "hosts": ["dns-cache-1"]}, {"ref": "dns_auth_ns.reload", "count": 1, "hosts": ["dns-auth-1"]}],
                    },
                    {
                        "order": 3,
                        "at_min": 38,
                        "rate_multipliers": {
                            "external_dns_query_nxdomain": 1.0,
                            "external_dns_query_ok_degraded": 0.6,
                            "dns_cache_ns.nxdomain_alert": 6.0,
                            "dns_auth_ns.nxdomain_alert": 6.0,
                            "dns_cache_ns.dns_stats": 0.0,
                            "dns_auth_ns.auth_stats": 0.0,
                            "dns_cache_ns.dns_stats_spike": 1.0,
                            "dns_auth_ns.auth_stats_spike": 1.0,
                        },
                        "latency_multipliers": {"external_dns_query_nxdomain": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [
                            {"ref": "config_deployer.dns_deploy_start", "count": 1, "hosts": ["deploy-1"]},
                            {"ref": "config_deployer.zone_build", "count": 1, "hosts": ["deploy-1"]},
                            {"ref": "config_deployer.dns_deploy_end", "count": 1, "hosts": ["deploy-1"]},
                            {"ref": "dns_auth_ns.zone_reload", "count": 1, "hosts": ["dns-auth-1"]},
                        ],
                    },
                    {
                        "order": 4,
                        "at_min": 46,
                        "rate_multipliers": {
                            "external_dns_query_nxdomain": 0.2,
                            "web_repo_request_502": 1.0,
                            "routing_lb.pool_degraded": 4.0,
                            "fileserver_pool.mem_pressure": 3.0,
                            "dns_cache_ns.nxdomain_alert": 0.5,
                            "dns_auth_ns.nxdomain_alert": 0.5,
                            "dns_cache_ns.dns_stats": 0.6,
                            "dns_auth_ns.auth_stats": 0.6,
                            "dns_cache_ns.dns_stats_spike": 0.0,
                            "dns_auth_ns.auth_stats_spike": 0.0,
                        },
                        "latency_multipliers": {"web_repo_request_502": {"p50": 1.2, "p95": 1.4}, "web_repo_request_ok_low": {"p50": 1.2, "p95": 1.5}},
                        "one_shots": [
                            {"ref": "config_deployer.manual_zone_restore", "count": 1, "hosts": ["deploy-1"]},
                            {"ref": "routing_lb.backend_removed", "count": 5, "hosts": ["router-1"]},
                            {"ref": "fileserver_pool.kill_hung_procs", "count": 2, "hosts": ["fs-02", "fs-03"]},
                            {"ref": "fileserver_pool.ha_stonith", "count": 1, "hosts": ["fs-04"]},
                        ],
                    },
                ]
            }
        },
    }
}


PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z0-9_]+)\}")


def isoformat_ms(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    ms = int(dt.microsecond / 1000)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ms:03d}Z"


def stable_lognormal_from_p50_p95(rng: np.random.RandomState, p50: float, p95: float, cap_factor: float = 3.0) -> float:
    p50 = max(1e-9, float(p50))
    p95 = max(p50 * 1.000001, float(p95))
    mu = math.log(p50)
    sigma = math.log(p95 / p50) / 1.6448536269514722
    x = float(rng.lognormal(mean=mu, sigma=max(1e-9, sigma)))
    soft_cap = cap_factor * p95
    if x > soft_cap:
        x = soft_cap + (x - soft_cap) * 0.15
    return max(0.0, x)


def sample_ip_from_cidr(rng: np.random.RandomState, cidr: str) -> str:
    base, prefix = cidr.split("/")
    prefix = int(prefix)
    parts = [int(x) for x in base.split(".")]
    if prefix == 24:
        last = int(rng.randint(1, 255))
        return f"{parts[0]}.{parts[1]}.{parts[2]}.{last}"
    if prefix == 16:
        o3 = int(rng.randint(0, 256))
        o4 = int(rng.randint(1, 255))
        return f"{parts[0]}.{parts[1]}.{o3}.{o4}"
    if prefix == 8:
        o2 = int(rng.randint(0, 256))
        o3 = int(rng.randint(0, 256))
        o4 = int(rng.randint(1, 255))
        return f"{parts[0]}.{o2}.{o3}.{o4}"
    return base


def sample_from_spec(rng: np.random.RandomState, spec: Dict[str, Any]) -> Any:
    k = spec["k"]
    v = spec["v"]
    if k == "ch":
        return v[int(rng.randint(0, len(v)))]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        return int(rng.randint(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        if abs(hi - lo) < 1e-12:
            return float(lo)
        val = float(lo + (hi - lo) * float(rng.rand()))
        return round(val, 1)
    if k == "hex":
        n = int(v)
        b = rng.bytes((n + 1) // 2)
        return b.hex()[:n]
    if k == "ip":
        return sample_ip_from_cidr(rng, str(v))
    if k == "uuid":
        b = rng.bytes(16).hex()
        return f"{b[0:8]}-{b[8:12]}-4{b[13:16]}-{b[16:20]}-{b[20:32]}"
    return str(v)


def clamp_int(v: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(v)))


def int_domain_from_spec(spec: Optional[Dict[str, Any]]) -> Optional[Tuple[int, int]]:
    if not spec:
        return None
    if spec.get("k") != "i":
        return None
    a, b = spec.get("v", [None, None])
    if a is None or b is None:
        return None
    return int(a), int(b)


@dataclass(frozen=True)
class LogTemplate:
    component_id: str
    log_id: str
    lvl: str
    msg: str
    vars: Dict[str, Any]
    state_vars: Dict[str, Dict[str, Any]]


@dataclass(frozen=True)
class Component:
    id: str
    svc: str
    hosts: List[str]
    beh: Dict[str, Any]
    logs: Dict[str, Any]


@dataclass(frozen=True)
class FlowDef:
    id: str
    rpm: float
    emit: List[str]
    latency_ms: List[List[float]]
    retry: Dict[str, Any]
    trace: bool


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Component], Dict[str, LogTemplate], Dict[Tuple[str, str], FlowDef]]:
    comps: Dict[str, Component] = {}
    logs: Dict[str, LogTemplate] = {}
    for c in system["components"]:
        comp = Component(id=c["id"], svc=c.get("svc", "") or "", hosts=list(c.get("hosts", [])), beh=c.get("beh", {}), logs=c.get("logs", {}))
        comps[comp.id] = comp
        for log_id, lt in comp.logs.items():
            ref = f"{comp.id}.{log_id}"
            logs[ref] = LogTemplate(
                component_id=comp.id,
                log_id=log_id,
                lvl=lt["lvl"],
                msg=lt["msg"],
                vars=dict(lt.get("vars", {})),
                state_vars=dict(lt.get("state_vars", {})),
            )
    flows: Dict[Tuple[str, str], FlowDef] = {}
    for state in ["n", "f"]:
        for f in system["flows"][state]["req"]:
            flows[(state, f["id"])] = FlowDef(
                id=f["id"],
                rpm=float(f["rpm"]),
                emit=list(f["emit"]),
                latency_ms=list(f["latency_ms"]),
                retry=dict(f["retry"]),
                trace=bool(f.get("trace", False)),
            )
    return comps, logs, flows


def plan_attempts(n_instances: int, expected: float, max_attempts: int) -> List[int]:
    if n_instances <= 0:
        return []
    max_attempts = max(1, int(max_attempts))
    expected = max(1.0, min(float(expected), float(max_attempts)))
    lo = int(math.floor(expected))
    hi = int(math.ceil(expected))
    lo = max(1, min(lo, max_attempts))
    hi = max(1, min(hi, max_attempts))
    if lo == hi:
        return [lo] * n_instances
    target_total = expected * n_instances
    n_hi = int(round(target_total - lo * n_instances))
    n_hi = max(0, min(n_instances, n_hi))
    attempts = [lo] * n_instances
    if n_hi == 0:
        return attempts
    used = set()
    for j in range(n_hi):
        idx = int((j + 0.5) * n_instances / n_hi)
        if idx >= n_instances:
            idx = n_instances - 1
        while idx in used and idx + 1 < n_instances:
            idx += 1
        while idx in used and idx - 1 >= 0:
            idx -= 1
        used.add(idx)
        attempts[idx] = hi
    return attempts


def schedule_times_evenly(rng: np.random.RandomState, start: datetime, end: datetime, count: int, jitter_frac: float = 0.25) -> List[datetime]:
    if count <= 0:
        return []
    total_sec = (end - start).total_seconds()
    if total_sec <= 0:
        return [start] * count
    step = total_sec / count
    max_jitter = min(0.5, step * jitter_frac)
    times: List[datetime] = []
    for i in range(count):
        center = (i + 0.5) * step
        jitter = (float(rng.rand()) * 2.0 - 1.0) * max_jitter
        t = start + timedelta(seconds=center + jitter)
        if t < start:
            t = start
        if t >= end:
            t = end - timedelta(milliseconds=1)
        times.append(t)
    return times


class DeterministicCounter:
    def __init__(self) -> None:
        self.acc: Dict[str, float] = {}

    def alloc(self, key: str, expected: float) -> int:
        e = max(0.0, float(expected))
        cur = self.acc.get(key, 0.0) + e
        n = int(math.floor(cur + 1e-12))
        self.acc[key] = cur - n
        return n


def bound_delay_ms(rng: np.random.RandomState, p50: float, p95: float) -> float:
    return stable_lognormal_from_p50_p95(rng, p50, p95, cap_factor=3.0)


def render_message(tpl: LogTemplate, rng: np.random.RandomState, state: str, ctx: Dict[str, Any]) -> str:
    spec_vars: Dict[str, Any] = {}
    spec_vars.update(tpl.vars or {})
    if tpl.state_vars and state in tpl.state_vars:
        spec_vars.update(tpl.state_vars[state] or {})
    needed = set(PLACEHOLDER_RE.findall(tpl.msg))
    for k in needed:
        if k in ctx:
            continue
        if k in spec_vars:
            ctx[k] = sample_from_spec(rng, spec_vars[k])
        else:
            ctx[k] = ""
    return tpl.msg.format(**ctx)


def choose_host_for_emitter(comp: Component, ctx: Dict[str, Any]) -> str:
    if comp.id == "fileserver_pool":
        b = ctx.get("backend")
        if isinstance(b, str) and b in comp.hosts:
            return b
    if not comp.hosts:
        return ""
    stick = ctx.get(f"__host_{comp.id}")
    if isinstance(stick, str) and stick in comp.hosts:
        return stick
    return comp.hosts[0]


def emit_log(rows: List[Dict[str, Any]], when: datetime, tpl: LogTemplate, comps: Dict[str, Component], rng: np.random.RandomState, state: str, ctx: Dict[str, Any], trace_id: str = "") -> None:
    comp = comps[tpl.component_id]
    host = choose_host_for_emitter(comp, ctx)
    msg = render_message(tpl, rng, state, ctx)
    rows.append(
        {
            "timestamp_dt": when,
            "level": tpl.lvl,
            "message": msg,
            "trace_id": trace_id,
            "service": comp.svc or "",
            "host": host or "",
        }
    )


def enforce_indices_bounds(delays: List[float], bounds: Dict[int, Tuple[Optional[float], Optional[float]]]) -> List[float]:
    d = list(delays)
    for idx, (lo, hi) in bounds.items():
        if 0 <= idx < len(d):
            if lo is not None:
                d[idx] = max(float(lo), float(d[idx]))
            if hi is not None:
                d[idx] = min(float(hi), float(d[idx]))
    return d


def enforce_segment_sum_range(
    delays: List[float],
    indices: List[int],
    min_total: Optional[float],
    max_total: Optional[float],
    index_mins: Optional[Dict[int, float]] = None,
) -> List[float]:
    d = list(delays)
    idxs = [i for i in indices if 0 <= i < len(d)]
    if not idxs:
        return d
    index_mins = dict(index_mins or {})

    for i in idxs:
        if i in index_mins:
            d[i] = max(d[i], float(index_mins[i]))

    s = float(sum(d[i] for i in idxs))

    if min_total is not None and s < float(min_total):
        d[idxs[-1]] += float(min_total) - s
        s = float(min_total)

    if max_total is not None and s > float(max_total):
        excess = s - float(max_total)
        for i in reversed(idxs):
            mn = float(index_mins.get(i, 0.0))
            reducible = max(0.0, d[i] - mn)
            take = min(reducible, excess)
            d[i] -= take
            excess -= take
            if excess <= 1e-9:
                break

    return d


def adjust_sum_exact(
    delays: List[float],
    indices: List[int],
    target_total: float,
    index_mins: Optional[Dict[int, float]] = None,
) -> List[float]:
    d = list(delays)
    idxs = [i for i in indices if 0 <= i < len(d)]
    if not idxs:
        return d
    index_mins = dict(index_mins or {})

    cur = float(sum(d[i] for i in idxs))
    diff = float(target_total) - cur
    if abs(diff) < 1e-9:
        return d

    if diff > 0:
        d[idxs[-1]] += diff
        return d

    excess = -diff
    for i in reversed(idxs):
        mn = float(index_mins.get(i, 0.0))
        reducible = max(0.0, d[i] - mn)
        take = min(reducible, excess)
        d[i] -= take
        excess -= take
        if excess <= 1e-9:
            break
    return d


def _bind_request_context(flow_id: str, rng: np.random.RandomState, comps: Dict[str, Component], flow_emit: List[str]) -> Dict[str, Any]:
    ctx_req: Dict[str, Any] = {}

    if any(r.startswith("web_frontend.") for r in flow_emit):
        web_hosts = comps["web_frontend"].hosts
        ctx_req["__host_web_frontend"] = web_hosts[int(rng.randint(0, len(web_hosts)))]
    if any(r.startswith("routing_lb.") for r in flow_emit):
        ctx_req["__host_routing_lb"] = comps["routing_lb"].hosts[0]
    if any(r.startswith("dns_cache_ns.") for r in flow_emit):
        ctx_req["__host_dns_cache_ns"] = comps["dns_cache_ns"].hosts[0]
    if any(r.startswith("dns_auth_ns.") for r in flow_emit):
        ctx_req["__host_dns_auth_ns"] = comps["dns_auth_ns"].hosts[0]

    if flow_id.startswith("external_dns_query"):
        qnames = SYSTEM["components"][1]["logs"]["query_recv"]["vars"]["qname"]["v"]
        if float(rng.rand()) < 0.75:
            qname_choices = ["github.com", "api.github.com", "assets.github.com", "gist.github.com"]
        else:
            qname_choices = qnames
        ctx_req["qname"] = qname_choices[int(rng.randint(0, len(qname_choices)))]
        ctx_req["qtype"] = ["A", "AAAA", "NS"][int(rng.randint(0, 3))]
        ctx_req["client_ip"] = sample_ip_from_cidr(rng, "198.51.100.0/24")
        ctx_req["qid"] = int(rng.randint(1000, 10000))
    elif flow_id.startswith("web_repo_request"):
        repos = ["alpha", "beta", "gamma", "delta", "epsilon"]
        routes = ["/alpha.git/info/refs", "/beta.git/git-upload-pack", "/gamma.git/git-receive-pack", "/"]
        methods = ["GET", "POST"]

        # Bind repo and route coherently. If a route encodes a repo, derive repo from it.
        route = routes[int(rng.randint(0, len(routes)))]
        if route.startswith("/alpha.git/"):
            repo = "alpha"
        elif route.startswith("/beta.git/"):
            repo = "beta"
        elif route.startswith("/gamma.git/"):
            repo = "gamma"
        else:
            repo = repos[int(rng.randint(0, len(repos)))]

        ctx_req["route"] = route
        ctx_req["repo"] = repo
        ctx_req["method"] = methods[int(rng.randint(0, len(methods)))]
        ctx_req["client_ip"] = sample_ip_from_cidr(rng, "203.0.113.0/24")
        backends = ["fs-01", "fs-02", "fs-03", "fs-04"]
        ctx_req["backend"] = backends[int(rng.randint(0, len(backends)))]
        ctx_req["decision"] = "consistent_hash"

        # S5 fix: this flow always emits routing_lb.conn_refused, so keep web upstream err consistent.
        if flow_id == "web_repo_request_502":
            ctx_req["err"] = "connect_refused"

    return ctx_req


def simulate_flow_instance(
    rows: List[Dict[str, Any]],
    rng: np.random.RandomState,
    start_time: datetime,
    state: str,
    flow: FlowDef,
    comps: Dict[str, Component],
    log_templates: Dict[str, LogTemplate],
    latency_mult: Dict[str, float],
    attempts: int,
) -> None:
    """
    Retry model:
    - The "receive" log is emitted once per flow instance.
    - Retry-only logs (emit_per_retry) are emitted on attempts 2..A.
    - Exactly one terminal response/timeout per request (on final attempt).

    Timing binding:
    - dns_cache_ns.respond_* dur_ms reflects recv->respond wall time (within the cache template domain).
    - dns_auth_ns.serve_* dur_ms reflects the auth-side segment and is clamped to the auth template domain,
      and does NOT reuse the cache dur_ms.
    """
    trace_id = ""  # tracing disabled in this scenario

    max_attempts = int(flow.retry.get("max_attempts", 1))
    attempts = max(1, min(int(attempts), max_attempts))

    mult_p50 = float(latency_mult.get("p50", 1.0))
    mult_p95 = float(latency_mult.get("p95", 1.0))

    ctx_req = _bind_request_context(flow.id, rng, comps, flow.emit)

    dns_cache_resp_dur_ms: Optional[int] = None  # used only for dns_cache_ns.respond_* templates

    retry_emit_refs = flow.retry.get("emit_per_retry", []) or []
    backoff_pairs = flow.retry.get("backoff_ms", []) or []
    backoff_tpl_domain: Optional[Tuple[int, int]] = None
    for ref in retry_emit_refs:
        tpl = log_templates.get(ref)
        if tpl:
            dom = int_domain_from_spec((tpl.vars or {}).get("backoff_ms"))
            if dom:
                backoff_tpl_domain = dom
                break

    delays_ms: List[float] = []
    for (p50, p95) in flow.latency_ms:
        p50_s = float(p50) * mult_p50
        p95_s = float(p95) * mult_p95
        delays_ms.append(bound_delay_ms(rng, p50_s, p95_s))

    backoffs: List[int] = []
    attempt_waits: List[float] = []
    if attempts > 1 and retry_emit_refs:
        if len(flow.emit) >= 2:
            base_p50, base_p95 = flow.latency_ms[1]
            for _ in range(attempts):
                attempt_waits.append(bound_delay_ms(rng, float(base_p50) * mult_p50, float(base_p95) * mult_p95))

        for _ in range(2, attempts + 1):
            if backoff_pairs:
                p50_b, p95_b = backoff_pairs[0]
            else:
                p50_b, p95_b = (20.0, 60.0)
            b = stable_lognormal_from_p50_p95(rng, float(p50_b), float(p95_b), cap_factor=3.0)
            b_int = int(round(b))
            if backoff_tpl_domain:
                b_int = clamp_int(b_int, backoff_tpl_domain[0], backoff_tpl_domain[1])
            else:
                b_int = max(0, b_int)
            backoffs.append(b_int)

    if flow.id in ("external_dns_query_ok", "external_dns_query_ok_degraded"):
        tpl_resp = log_templates["dns_cache_ns.respond_noerror"]
        dur_dom = int_domain_from_spec((tpl_resp.vars or {}).get("dur_ms")) or (1, 400)

        if attempts > 1 and retry_emit_refs:
            delay1 = float(attempt_waits[0]) if attempt_waits else float(delays_ms[1])
            for j in range(1, attempts):
                delay1 += float(backoffs[j - 1]) + float(attempt_waits[j])
            delays_ms[1] = delay1

        delays_ms[1] = max(1.0, float(delays_ms[1]))
        delays_ms[2] = max(1.0, float(delays_ms[2]))
        dur_total = float(delays_ms[1] + delays_ms[2])
        dur_target = clamp_int(int(round(dur_total)), dur_dom[0], dur_dom[1])

        min_tail = 1.0
        min_delay1 = 1.0
        if attempts > 1 and retry_emit_refs:
            min_delay1 = float(sum(backoffs) + attempts * 1.0)

        if float(dur_target) < min_delay1 + min_tail:
            dur_target = clamp_int(int(round(min_delay1 + min_tail)), dur_dom[0], dur_dom[1])

        if float(dur_target) - min_tail < delays_ms[1]:
            new_delay1 = max(min_delay1, float(dur_target) - min_tail)
            if attempts > 1 and retry_emit_refs and attempt_waits:
                fixed = float(sum(backoffs))
                desired_attempt_sum = max(attempts * 1.0, new_delay1 - fixed)
                cur_attempt_sum = float(sum(attempt_waits))
                scale = desired_attempt_sum / cur_attempt_sum if cur_attempt_sum > 1e-9 else 1.0
                attempt_waits = [max(1.0, w * scale) for w in attempt_waits]
                delay1 = float(attempt_waits[0])
                for j in range(1, attempts):
                    delay1 += float(backoffs[j - 1]) + float(attempt_waits[j])
                delays_ms[1] = delay1
            else:
                delays_ms[1] = new_delay1

        delays_ms[2] = max(1.0, float(dur_target) - float(delays_ms[1]))
        dur_ms = int(round(float(delays_ms[1] + delays_ms[2])))
        dns_cache_resp_dur_ms = clamp_int(dur_ms, dur_dom[0], dur_dom[1])

    elif flow.id == "external_dns_query_nxdomain":
        tpl_resp = log_templates["dns_cache_ns.respond_nxdomain"]
        dur_dom = int_domain_from_spec((tpl_resp.vars or {}).get("dur_ms")) or (1, 200)

        delays_ms[1] = max(1.0, float(delays_ms[1]))
        delays_ms[2] = max(1.0, float(delays_ms[2]))
        dur_total = float(delays_ms[1] + delays_ms[2])
        dur_target = clamp_int(int(round(dur_total)), dur_dom[0], dur_dom[1])
        if float(dur_target) < 2.0:
            dur_target = 2
        delays_ms[2] = max(1.0, float(dur_target) - float(delays_ms[1]))
        dur_ms = int(round(float(delays_ms[1] + delays_ms[2])))
        dns_cache_resp_dur_ms = clamp_int(dur_ms, dur_dom[0], dur_dom[1])

    elif flow.id == "external_dns_query_timeout":
        tpl_to = log_templates["dns_cache_ns.query_timeout"]
        waited_dom = int_domain_from_spec((tpl_to.vars or {}).get("waited_ms")) or (900, 2200)

        total_wait = max(1.0, float(delays_ms[1]))
        total_wait = float(clamp_int(int(round(total_wait)), waited_dom[0], waited_dom[1]))

        if attempts > 1 and retry_emit_refs:
            if not backoffs:
                backoffs = [0]
            b0 = int(backoffs[0])

            min_attempt_wait = 100.0
            min_total_needed = float(b0) + 2.0 * min_attempt_wait
            if total_wait < min_total_needed:
                grown = min(float(waited_dom[1]), min_total_needed)
                if grown >= min_total_needed:
                    total_wait = grown
                else:
                    b0 = int(max(0.0, total_wait - 2.0 * min_attempt_wait))
                    if backoff_tpl_domain:
                        b0 = clamp_int(b0, backoff_tpl_domain[0], backoff_tpl_domain[1])
                    backoffs[0] = b0

            attempt1 = max(min_attempt_wait, total_wait * 0.55)
            attempt2 = total_wait - float(b0) - attempt1
            if attempt2 < min_attempt_wait:
                deficit = min_attempt_wait - attempt2
                attempt1 = max(min_attempt_wait, attempt1 - deficit)
                attempt2 = total_wait - float(b0) - attempt1
                attempt2 = max(min_attempt_wait, attempt2)

            total_wait = attempt1 + float(b0) + attempt2
            total_wait = float(clamp_int(int(round(total_wait)), waited_dom[0], waited_dom[1]))
            attempt2 = max(min_attempt_wait, total_wait - float(b0) - attempt1)

            attempt_waits = [attempt1, attempt2]
            backoffs[0] = b0
            delays_ms[1] = total_wait
        else:
            delays_ms[1] = total_wait

        waited_ms = int(round(float(delays_ms[1])))
        waited_ms = clamp_int(waited_ms, waited_dom[0], waited_dom[1])
        delays_ms[1] = float(waited_ms)
        ctx_req["waited_ms"] = waited_ms

    elif flow.id in ("web_repo_request_ok", "web_repo_request_ok_low"):
        tpl_resp = log_templates["web_frontend.http_resp_ok"]
        dom = int_domain_from_spec((tpl_resp.vars or {}).get("dur_ms")) or (20, 1500)

        tpl_git = log_templates["fileserver_pool.serve_git"]
        git_dom = int_domain_from_spec((tpl_git.vars or {}).get("dur_ms")) or (5, 2500)
        delays_ms = enforce_indices_bounds(delays_ms, {2: (float(git_dom[0]), float(git_dom[1]))})

        seg_idx = [1, 2, 3]
        index_mins = {2: float(git_dom[0])}
        delays_ms = enforce_segment_sum_range(delays_ms, seg_idx, float(dom[0]), float(dom[1]), index_mins=index_mins)
        dur = int(round(sum(delays_ms[i] for i in seg_idx)))
        dur = clamp_int(dur, dom[0], dom[1])
        delays_ms = adjust_sum_exact(delays_ms, seg_idx, float(dur), index_mins=index_mins)
        ctx_req["dur_ms"] = dur

    elif flow.id == "web_repo_request_502":
        tpl_resp = log_templates["web_frontend.http_resp_err"]
        dom = int_domain_from_spec((tpl_resp.vars or {}).get("dur_ms")) or (50, 6000)
        seg_idx = [1, 2, 3]
        delays_ms = enforce_segment_sum_range(delays_ms, seg_idx, float(dom[0]), float(dom[1]), index_mins=None)
        dur = int(round(sum(delays_ms[i] for i in seg_idx)))
        dur = clamp_int(dur, dom[0], dom[1])
        delays_ms = adjust_sum_exact(delays_ms, seg_idx, float(dur))
        ctx_req["dur_ms"] = dur
        ctx_req["status"] = 502 if float(rng.rand()) < 0.7 else 503

    t_first = start_time + timedelta(milliseconds=float(delays_ms[0] if delays_ms else 0.0))

    ref0 = flow.emit[0]
    tpl0 = log_templates[ref0]
    emit_log(rows, t_first, tpl0, comps, rng, state, dict(ctx_req), trace_id=trace_id)

    if attempts > 1 and retry_emit_refs and attempt_waits:
        t_recv = t_first
        elapsed = 0.0
        for k in range(2, attempts + 1):
            elapsed += float(attempt_waits[k - 2])
            t_retry = t_recv + timedelta(milliseconds=elapsed)
            b_ms = int(backoffs[k - 2]) if (k - 2) < len(backoffs) else 0
            for ref in retry_emit_refs:
                tplr = log_templates[ref]
                ctxr = dict(ctx_req)
                ctxr["attempt"] = k
                ctxr["backoff_ms"] = int(b_ms)
                emit_log(rows, t_retry, tplr, comps, rng, state, ctxr, trace_id=trace_id)
            elapsed += float(b_ms)

    t = t_first
    for j in range(1, len(flow.emit)):
        t = t + timedelta(milliseconds=float(delays_ms[j]))
        ref = flow.emit[j]
        tpl = log_templates[ref]
        ctx_log = dict(ctx_req)

        if ref in ("dns_auth_ns.serve_noerror", "dns_auth_ns.serve_nxdomain"):
            dur_dom = int_domain_from_spec((tpl.vars or {}).get("dur_ms")) or (1, 80)
            seg_gap_ms = float(delays_ms[j])
            auth_dur_ms = clamp_int(int(round(min(seg_gap_ms, float(dur_dom[1])))), dur_dom[0], dur_dom[1])
            ctx_log["dur_ms"] = auth_dur_ms

        if ref in ("dns_cache_ns.respond_noerror", "dns_cache_ns.respond_nxdomain") and dns_cache_resp_dur_ms is not None:
            ctx_log["dur_ms"] = int(dns_cache_resp_dur_ms)

        if ref == "fileserver_pool.serve_git":
            ctx_log["backend"] = ctx_req.get("backend", "fs-01")
            seg_ms = float(delays_ms[2]) if len(delays_ms) >= 3 else 100.0
            dur_dom = int_domain_from_spec((tpl.vars or {}).get("dur_ms")) or (5, 2500)
            ctx_log["dur_ms"] = clamp_int(int(round(seg_ms)), dur_dom[0], dur_dom[1])

        emit_log(rows, t, tpl, comps, rng, state, ctx_log, trace_id=trace_id)


def main() -> None:
    random.seed(1337)
    rng = np.random.RandomState(1337)

    comps, log_templates, flows = build_indices(SYSTEM)

    base_time = datetime(2026, 1, 8, 0, 0, 0, tzinfo=timezone.utc)

    n_start = int(SCENARIO["scenario"]["time"]["phases"]["n"]["start_min"])
    n_end = int(SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"])
    f_start = int(SCENARIO["scenario"]["time"]["phases"]["f"]["start_min"])
    f_end = int(SCENARIO["scenario"]["time"]["phases"]["f"]["end_min"])

    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = [f_start] + [int(e["at_min"]) for e in events if f_start <= int(e["at_min"]) < f_end] + [f_end]
    boundaries = sorted(set(boundaries))
    if boundaries[0] != f_start:
        boundaries = [f_start] + boundaries
    if boundaries[-1] != f_end:
        boundaries = boundaries + [f_end]

    active_rate_mult: Dict[str, float] = {}
    active_lat_mult: Dict[str, Dict[str, float]] = {}

    events_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        events_by_min.setdefault(int(e["at_min"]), []).append(e)

    failure_intervals: List[Dict[str, Any]] = []
    for i in range(len(boundaries) - 1):
        start_min = boundaries[i]
        end_min = boundaries[i + 1]
        for ev in events_by_min.get(start_min, []):
            for k, v in (ev.get("rate_multipliers") or {}).items():
                active_rate_mult[k] = float(v)
            for fid, mv in (ev.get("latency_multipliers") or {}).items():
                active_lat_mult[fid] = {"p50": float(mv.get("p50", 1.0)), "p95": float(mv.get("p95", 1.0))}
        failure_intervals.append(
            {
                "start_min": start_min,
                "end_min": end_min,
                "rate_mult": dict(active_rate_mult),
                "lat_mult": dict(active_lat_mult),
                "events_at_start": events_by_min.get(start_min, []),
            }
        )

    rows: List[Dict[str, Any]] = []
    counter = DeterministicCounter()

    n_start_t = base_time + timedelta(minutes=n_start)
    n_end_t = base_time + timedelta(minutes=n_end)
    n_dur_min = float(n_end - n_start)

    for comp in comps.values():
        beh = comp.beh.get("n", {})
        for src in beh.get("emit", []) or []:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = str(src.get("scope", "per_host"))
            tpl = log_templates[f"{comp.id}.{log_id}"]
            if scope == "global":
                key = f"bg:n:{comp.id}.{log_id}:global"
                count = counter.alloc(key, per_min * n_dur_min)
                times = schedule_times_evenly(rng, n_start_t, n_end_t, count)
                for t in times:
                    ctx = {f"__host_{comp.id}": comp.hosts[0]} if comp.hosts else {}
                    emit_log(rows, t, tpl, comps, rng, "n", ctx, trace_id="")
            else:
                for host in (comp.hosts or [""]):
                    key = f"bg:n:{comp.id}.{log_id}:host:{host}"
                    count = counter.alloc(key, per_min * n_dur_min)
                    times = schedule_times_evenly(rng, n_start_t, n_end_t, count)
                    for t in times:
                        ctx = {f"__host_{comp.id}": host} if host else {}
                        emit_log(rows, t, tpl, comps, rng, "n", ctx, trace_id="")

    for flow in SYSTEM["flows"]["n"]["req"]:
        fdef = flows[("n", flow["id"])]
        expected_instances = fdef.rpm * n_dur_min
        n_instances = counter.alloc(f"flow:n:{fdef.id}", expected_instances)
        start_times = schedule_times_evenly(rng, n_start_t, n_end_t, n_instances, jitter_frac=0.15)
        attempt_counts = plan_attempts(n_instances, float(fdef.retry.get("expected_attempts", 1.0)), int(fdef.retry.get("max_attempts", 1)))
        for st, a in zip(start_times, attempt_counts):
            simulate_flow_instance(rows, rng, st, "n", fdef, comps, log_templates, latency_mult={"p50": 1.0, "p95": 1.0}, attempts=a)

    for interval in failure_intervals:
        istart_min = interval["start_min"]
        iend_min = interval["end_min"]
        istart_t = base_time + timedelta(minutes=istart_min)
        iend_t = base_time + timedelta(minutes=iend_min)
        dur_min = float(iend_min - istart_min)
        rate_mult = interval["rate_mult"]
        lat_mult = interval["lat_mult"]

        for comp in comps.values():
            beh = comp.beh.get("f", {})
            for src in beh.get("emit", []) or []:
                log_id = src["id"]
                per_min = float(src["per_min"])
                scope = str(src.get("scope", "per_host"))
                mult_key = f"{comp.id}.{log_id}"
                mult = float(rate_mult.get(mult_key, 1.0))
                effective = per_min * mult
                tpl = log_templates[f"{comp.id}.{log_id}"]
                if scope == "global":
                    key = f"bg:f:{comp.id}.{log_id}:global"
                    count = counter.alloc(key, effective * dur_min)
                    times = schedule_times_evenly(rng, istart_t, iend_t, count)
                    for t in times:
                        ctx = {f"__host_{comp.id}": comp.hosts[0]} if comp.hosts else {}
                        emit_log(rows, t, tpl, comps, rng, "f", ctx, trace_id="")
                else:
                    for host in (comp.hosts or [""]):
                        key = f"bg:f:{comp.id}.{log_id}:host:{host}"
                        count = counter.alloc(key, effective * dur_min)
                        times = schedule_times_evenly(rng, istart_t, iend_t, count)
                        for t in times:
                            ctx = {f"__host_{comp.id}": host} if host else {}
                            emit_log(rows, t, tpl, comps, rng, "f", ctx, trace_id="")

        for flow in SYSTEM["flows"]["f"]["req"]:
            fdef = flows[("f", flow["id"])]
            mult = float(rate_mult.get(fdef.id, 1.0))
            effective_rpm = fdef.rpm * mult
            expected_instances = effective_rpm * dur_min
            n_instances = counter.alloc(f"flow:f:{fdef.id}", expected_instances)
            start_times = schedule_times_evenly(rng, istart_t, iend_t, n_instances, jitter_frac=0.15)
            attempt_counts = plan_attempts(n_instances, float(fdef.retry.get("expected_attempts", 1.0)), int(fdef.retry.get("max_attempts", 1)))
            lm = lat_mult.get(fdef.id, {"p50": 1.0, "p95": 1.0})
            for st, a in zip(start_times, attempt_counts):
                simulate_flow_instance(rows, rng, st, "f", fdef, comps, log_templates, latency_mult=lm, attempts=a)

        for ev in interval["events_at_start"]:
            at_t = base_time + timedelta(minutes=int(ev["at_min"]))
            for os in ev.get("one_shots", []) or []:
                ref = os["ref"]
                count = int(os["count"])
                allowed_hosts = list(os.get("hosts", [])) or None
                tpl = log_templates[ref]
                comp = comps[tpl.component_id]
                for k in range(count):
                    jitter_ms = int(50 + (k * 100) % 900)
                    t = at_t + timedelta(milliseconds=jitter_ms)
                    ctx: Dict[str, Any] = {}
                    if allowed_hosts:
                        ctx[f"__host_{comp.id}"] = allowed_hosts[k % len(allowed_hosts)]
                    elif comp.hosts:
                        ctx[f"__host_{comp.id}"] = comp.hosts[0]

                    if ref == "dns_auth_ns.zone_reload":
                        ctx["status"] = "loaded"
                        ctx["records"] = int(rng.randint(200, 1200))
                    if ref == "config_deployer.zone_build":
                        ctx["api_status"] = "dns_timeout" if float(rng.rand()) < 0.7 else "http_503"
                        ctx["items"] = int(rng.randint(0, 250))
                    if ref == "config_deployer.dns_deploy_end":
                        ctx["status"] = "success_with_warnings"
                        ctx["removed_records_pct"] = round(float(rng.uniform(40.0, 85.0)), 1)

                    emit_log(rows, t, tpl, comps, rng, "f", ctx, trace_id="")

    df = pd.DataFrame(rows)
    df.sort_values("timestamp_dt", inplace=True, kind="mergesort")
    df["timestamp"] = df["timestamp_dt"].apply(isoformat_ms)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]

    if not (20000 <= len(df) <= 100000):
        raise RuntimeError(f"Row count {len(df)} outside required range 20,000..100,000")
    if not df["timestamp"].is_monotonic_increasing:
        raise RuntimeError("Timestamps are not sorted ascending")

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
