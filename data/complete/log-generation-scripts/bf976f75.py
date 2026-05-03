import hashlib
import math
import random
import string
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "mw_aux_services_external_storage_overload"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "scap",
            "svc": "scap",
            "hosts": ["deploy-1"],
            "logs": {
                "deploy_start": {
                    "lvl": "INFO",
                    "msg": "scap deploy started deploy_id={deploy_id} targets=mw-{api-int,parsoid,jobrunner} flags=-Dbuild_mw_container_image:False",
                    "vars": {"deploy_id": {"k": "hex", "v": 8}},
                }
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "mw_api_int",
            "svc": "mw-api-int",
            "hosts": ["mw-api-int-1"],
            "logs": {
                "http_start": {
                    "lvl": "INFO",
                    "msg": "request start req_id={req_id} method={method} uri={uri}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "uri": {"k": "ch", "v": ["/w/api.php", "/w/rest.php"]},
                    },
                },
                "cache_hit": {
                    "lvl": "DEBUG",
                    "msg": "cache hit key={cache_key} server={server} ttl_s={ttl_s}",
                    "vars": {
                        "cache_key": {"k": "str", "v": "mw:cache:key"},
                        "server": {"k": "ch", "v": ["mcrouter.cache.svc:11213"]},
                        "ttl_s": {"k": "i", "v": [30, 600]},
                    },
                },
                "cache_miss": {
                    "lvl": "DEBUG",
                    "msg": "cache miss key={cache_key} server={server}",
                    "vars": {
                        "cache_key": {"k": "str", "v": "mw:cache:key"},
                        "server": {"k": "ch", "v": ["mcrouter.cache.svc:11213"]},
                    },
                },
                "cache_lookup_failed": {
                    "lvl": "WARN",
                    "msg": "cache lookup failed key={cache_key} server={server} err={err}; falling back to DB",
                    "vars": {
                        "cache_key": {"k": "str", "v": "mw:cache:key"},
                        "server": {"k": "ch", "v": ["127.0.0.1:11213"]},
                        "err": {"k": "ch", "v": ["ECONNREFUSED", "ETIMEDOUT", "ENETUNREACH"]},
                    },
                },
                "db_error": {
                    "lvl": "ERROR",
                    "msg": "db error req_id={req_id} cluster=external_storage op={op} err={db_err} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "op": {"k": "ch", "v": ["SELECT", "UPDATE"]},
                        "db_err": {"k": "ch", "v": ["timeout", "too_many_connections", "lock_wait_timeout"]},
                        "dur_ms": {"k": "i", "v": [500, 60000]},
                    },
                },
                "http_end_ok": {
                    "lvl": "INFO",
                    "msg": "request end req_id={req_id} status=200 dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [20, 60000]}},
                },
                "http_end_err": {
                    "lvl": "INFO",
                    "msg": "request end req_id={req_id} status={status} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "status": {"k": "ch", "v": ["500", "503"]},
                        "dur_ms": {"k": "i", "v": [100, 60000]},
                    },
                },
                "memcached_spam": {
                    "lvl": "ERROR",
                    "msg": "Memcached::set failed server={server} errno={errno} msg={errmsg}",
                    "vars": {
                        "server": {"k": "ch", "v": ["127.0.0.1:11213"]},
                        "errno": {"k": "i", "v": [110, 113]},
                        "errmsg": {"k": "ch", "v": ["Connection timed out", "Connection refused", "No route to host"]},
                    },
                },
                "phpfpm_status": {
                    "lvl": "INFO",
                    "msg": "php-fpm status active={active} idle={idle} queue={queue}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "active": {"k": "i", "v": [5, 25]},
                            "idle": {"k": "i", "v": [5, 40]},
                            "queue": {"k": "i", "v": [0, 10]},
                        },
                        "f": {
                            "active": {"k": "i", "v": [20, 60]},
                            "idle": {"k": "i", "v": [0, 10]},
                            "queue": {"k": "i", "v": [5, 200]},
                        },
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "phpfpm_status", "per_min": 0.3}]},
                "f": {"emit": [{"id": "phpfpm_status", "per_min": 0.5}, {"id": "memcached_spam", "per_min": 250.0}]},
            },
        },
        {
            "id": "mw_parsoid",
            "svc": "mw-parsoid",
            "hosts": ["mw-parsoid-1"],
            "logs": {
                "http_start": {
                    "lvl": "INFO",
                    "msg": "request start req_id={req_id} method={method} uri={uri}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "uri": {"k": "ch", "v": ["/v3/page/html", "/v3/transform/wikitext/to/html"]},
                    },
                },
                "cache_hit": {
                    "lvl": "DEBUG",
                    "msg": "cache hit key={cache_key} server={server} ttl_s={ttl_s}",
                    "vars": {
                        "cache_key": {"k": "str", "v": "parsoid:cache:key"},
                        "server": {"k": "ch", "v": ["mcrouter.cache.svc:11213"]},
                        "ttl_s": {"k": "i", "v": [30, 600]},
                    },
                },
                "cache_miss": {
                    "lvl": "DEBUG",
                    "msg": "cache miss key={cache_key} server={server}",
                    "vars": {
                        "cache_key": {"k": "str", "v": "parsoid:cache:key"},
                        "server": {"k": "ch", "v": ["mcrouter.cache.svc:11213"]},
                    },
                },
                "cache_lookup_failed": {
                    "lvl": "WARN",
                    "msg": "cache lookup failed key={cache_key} server={server} err={err}; falling back to DB",
                    "vars": {
                        "cache_key": {"k": "str", "v": "parsoid:cache:key"},
                        "server": {"k": "ch", "v": ["127.0.0.1:11213"]},
                        "err": {"k": "ch", "v": ["ECONNREFUSED", "ETIMEDOUT", "ENETUNREACH"]},
                    },
                },
                "db_error": {
                    "lvl": "ERROR",
                    "msg": "db error req_id={req_id} cluster=external_storage op={op} err={db_err} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "op": {"k": "ch", "v": ["SELECT", "UPDATE"]},
                        "db_err": {"k": "ch", "v": ["timeout", "too_many_connections", "lock_wait_timeout"]},
                        "dur_ms": {"k": "i", "v": [500, 60000]},
                    },
                },
                "http_end_ok": {
                    "lvl": "INFO",
                    "msg": "request end req_id={req_id} status=200 dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [20, 60000]}},
                },
                "http_end_err": {
                    "lvl": "INFO",
                    "msg": "request end req_id={req_id} status={status} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "status": {"k": "ch", "v": ["500", "503"]},
                        "dur_ms": {"k": "i", "v": [100, 60000]},
                    },
                },
                "memcached_spam": {
                    "lvl": "ERROR",
                    "msg": "Memcached::set failed server={server} errno={errno} msg={errmsg}",
                    "vars": {
                        "server": {"k": "ch", "v": ["127.0.0.1:11213"]},
                        "errno": {"k": "i", "v": [110, 113]},
                        "errmsg": {"k": "ch", "v": ["Connection timed out", "Connection refused", "No route to host"]},
                    },
                },
                "phpfpm_status": {
                    "lvl": "INFO",
                    "msg": "php-fpm status active={active} idle={idle} queue={queue}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "active": {"k": "i", "v": [3, 20]},
                            "idle": {"k": "i", "v": [5, 35]},
                            "queue": {"k": "i", "v": [0, 8]},
                        },
                        "f": {
                            "active": {"k": "i", "v": [15, 50]},
                            "idle": {"k": "i", "v": [0, 8]},
                            "queue": {"k": "i", "v": [5, 150]},
                        },
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "phpfpm_status", "per_min": 0.25}]},
                "f": {"emit": [{"id": "phpfpm_status", "per_min": 0.45}, {"id": "memcached_spam", "per_min": 250.0}]},
            },
        },
        {
            "id": "mw_jobrunner",
            "svc": "mw-jobrunner",
            "hosts": ["mw-jobrunner-1"],
            "logs": {
                "http_start": {
                    "lvl": "INFO",
                    "msg": "job start req_id={req_id} job={job}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "job": {"k": "ch", "v": ["categoryMembershipChange", "refreshLinks", "cirrusUpdate"]},
                    },
                },
                "cache_hit": {
                    "lvl": "DEBUG",
                    "msg": "cache hit key={cache_key} server={server} ttl_s={ttl_s}",
                    "vars": {
                        "cache_key": {"k": "str", "v": "job:cache:key"},
                        "server": {"k": "ch", "v": ["mcrouter.cache.svc:11213"]},
                        "ttl_s": {"k": "i", "v": [30, 600]},
                    },
                },
                "cache_miss": {
                    "lvl": "DEBUG",
                    "msg": "cache miss key={cache_key} server={server}",
                    "vars": {
                        "cache_key": {"k": "str", "v": "job:cache:key"},
                        "server": {"k": "ch", "v": ["mcrouter.cache.svc:11213"]},
                    },
                },
                "cache_lookup_failed": {
                    "lvl": "WARN",
                    "msg": "cache lookup failed key={cache_key} server={server} err={err}; falling back to DB",
                    "vars": {
                        "cache_key": {"k": "str", "v": "job:cache:key"},
                        "server": {"k": "ch", "v": ["127.0.0.1:11213"]},
                        "err": {"k": "ch", "v": ["ECONNREFUSED", "ETIMEDOUT", "ENETUNREACH"]},
                    },
                },
                "db_error": {
                    "lvl": "ERROR",
                    "msg": "db error req_id={req_id} cluster=external_storage op={op} err={db_err} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "op": {"k": "ch", "v": ["SELECT", "UPDATE"]},
                        "db_err": {"k": "ch", "v": ["timeout", "too_many_connections", "lock_wait_timeout"]},
                        "dur_ms": {"k": "i", "v": [500, 60000]},
                    },
                },
                "http_end_ok": {
                    "lvl": "INFO",
                    "msg": "job end req_id={req_id} status=ok dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [50, 60000]}},
                },
                "http_end_err": {
                    "lvl": "INFO",
                    "msg": "job end req_id={req_id} status=error dur_ms={dur_ms}",
                    "vars": {"req_id": {"k": "hex", "v": 16}, "dur_ms": {"k": "i", "v": [200, 60000]}},
                },
                "memcached_spam": {
                    "lvl": "ERROR",
                    "msg": "Memcached::set failed server={server} errno={errno} msg={errmsg}",
                    "vars": {
                        "server": {"k": "ch", "v": ["127.0.0.1:11213"]},
                        "errno": {"k": "i", "v": [110, 113]},
                        "errmsg": {"k": "ch", "v": ["Connection timed out", "Connection refused", "No route to host"]},
                    },
                },
                "phpfpm_status": {
                    "lvl": "INFO",
                    "msg": "php-fpm status active={active} idle={idle} queue={queue}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "active": {"k": "i", "v": [5, 30]},
                            "idle": {"k": "i", "v": [5, 45]},
                            "queue": {"k": "i", "v": [0, 12]},
                        },
                        "f": {
                            "active": {"k": "i", "v": [25, 70]},
                            "idle": {"k": "i", "v": [0, 10]},
                            "queue": {"k": "i", "v": [10, 250]},
                        },
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "phpfpm_status", "per_min": 0.35}]},
                "f": {"emit": [{"id": "phpfpm_status", "per_min": 0.6}, {"id": "memcached_spam", "per_min": 250.0}]},
            },
        },
        {
            "id": "external_storage_db",
            "svc": "extstore-db",
            "hosts": ["extdb-1", "extdb-2"],
            "logs": {
                "pool_stats": {
                    "lvl": "INFO",
                    "msg": "extstore pool active_conns={active_conns} waiting_conns={waiting_conns} p95_wait_ms={p95_wait_ms}",
                    "vars": {},
                    "state_vars": {
                        "n": {
                            "active_conns": {"k": "i", "v": [60, 140]},
                            "waiting_conns": {"k": "i", "v": [0, 8]},
                            "p95_wait_ms": {"k": "i", "v": [1, 25]},
                        },
                        "f": {
                            "active_conns": {"k": "i", "v": [180, 520]},
                            "waiting_conns": {"k": "i", "v": [10, 160]},
                            "p95_wait_ms": {"k": "i", "v": [20, 800]},
                        },
                    },
                },
                "query_timeout": {
                    "lvl": "WARN",
                    "msg": "extstore query timeout client={client} dur_ms={dur_ms}",
                    "vars": {
                        "client": {"k": "ch", "v": ["mw-api-int", "mw-parsoid", "mw-jobrunner"]},
                        "dur_ms": {"k": "i", "v": [2000, 60000]},
                    },
                },
                "too_many_connections": {
                    "lvl": "ERROR",
                    "msg": "extstore connection rejected client={client} active_conns={active_conns}",
                    "vars": {
                        "client": {"k": "ch", "v": ["mw-api-int", "mw-parsoid", "mw-jobrunner"]},
                        "active_conns": {"k": "i", "v": [350, 650]},
                    },
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "pool_stats", "per_min": 1.0},
                        {"id": "query_timeout", "per_min": 0.05},
                        {"id": "too_many_connections", "per_min": 0.01},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "pool_stats", "per_min": 2.0},
                        {"id": "query_timeout", "per_min": 2.0},
                        {"id": "too_many_connections", "per_min": 0.5},
                    ]
                },
            },
        },
        {
            "id": "logstash",
            "svc": "logstash",
            "hosts": ["logstash-1"],
            "logs": {
                "pipeline_lag": {
                    "lvl": "WARN",
                    "msg": "logstash pipeline lag lag_s={lag_s} queue_depth={queue_depth} inflight={inflight} input_eps={input_eps}",
                    "vars": {
                        "lag_s": {"k": "f", "v": [0.0, 600.0]},
                        "queue_depth": {"k": "i", "v": [0, 500000]},
                        "inflight": {"k": "i", "v": [0, 50000]},
                    },
                    "state_vars": {
                        "n": {"input_eps": {"k": "i", "v": [10, 35]}},
                        "f": {"input_eps": {"k": "i", "v": [25, 80]}},
                    },
                },
                "pipeline_drop": {
                    "lvl": "ERROR",
                    "msg": "logstash dropped events dropped={dropped} reason={reason}",
                    "vars": {"dropped": {"k": "i", "v": [100, 20000]}, "reason": {"k": "ch", "v": ["queue_full", "slow_filter"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "pipeline_lag", "per_min": 0.2, "scope": "global"}, {"id": "pipeline_drop", "per_min": 0.02, "scope": "global"}]},
                "f": {"emit": [{"id": "pipeline_lag", "per_min": 1.0, "scope": "global"}, {"id": "pipeline_drop", "per_min": 0.2, "scope": "global"}]},
            },
        },
        {
            "id": "prometheus",
            "svc": "prometheus",
            "hosts": ["prometheus-1"],
            "logs": {
                "scrape_failed": {
                    "lvl": "WARN",
                    "msg": "scrape failed job={job} endpoint={endpoint} err={err}",
                    "vars": {
                        "job": {"k": "ch", "v": ["logstash_prom_exporter", "mediawiki_stats"]},
                        "endpoint": {"k": "ch", "v": ["http://logstash-1:9108/metrics", "http://mw-metrics:9102/metrics"]},
                        "err": {"k": "ch", "v": ["context_deadline_exceeded", "no_data_returned", "connection_reset"]},
                    },
                },
                "alert_firing": {
                    "lvl": "WARN",
                    "msg": "alert firing name={alert} severity={sev} value={value} target={target}",
                    "vars": {
                        "alert": {"k": "ch", "v": ["MediaWikiHighBackendLatency", "ExternalStorageHighConnections"]},
                        "sev": {"k": "ch", "v": ["warning", "critical"]},
                        "value": {"k": "f", "v": [0.5, 120.0]},
                        "target": {"k": "ch", "v": ["mw-api-int", "mw-parsoid", "mw-jobrunner", "extstore"]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "scrape_failed", "per_min": 0.05, "scope": "global"}, {"id": "alert_firing", "per_min": 0.0, "scope": "global"}]},
                "f": {"emit": [{"id": "scrape_failed", "per_min": 0.2, "scope": "global"}, {"id": "alert_firing", "per_min": 0.5, "scope": "global"}]},
            },
        },
        {
            "id": "ops_console",
            "svc": "ops",
            "hosts": ["ops-1"],
            "logs": {
                "reduce_cirrus_parallelism": {
                    "lvl": "INFO",
                    "msg": "config change service=cirrus-streaming-updater param=parallelism from=64 to=16 by=gmodena,brouberol",
                    "vars": {},
                },
                "reduce_changeprop_concurrency": {
                    "lvl": "INFO",
                    "msg": "config change service=changeprop-jobqueue param=categoryMembershipChange_concurrency from=50 to=10 by=amir1",
                    "vars": {},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": {
            "req": [
                {
                    "id": "api_int_request_ok",
                    "rpm": 160,
                    "emit": ["mw_api_int.http_start", "mw_api_int.cache_hit", "mw_api_int.http_end_ok"],
                    "latency_ms": [[1, 3], [2, 6], [20, 140]],
                    "trace": False,
                },
                {
                    "id": "api_int_request_db_error",
                    "rpm": 2,
                    "emit": ["mw_api_int.http_start", "mw_api_int.cache_miss", "mw_api_int.db_error", "mw_api_int.http_end_err"],
                    "latency_ms": [[1, 3], [2, 6], [200, 1500], [300, 2000]],
                    "trace": False,
                },
                {
                    "id": "parsoid_request_ok",
                    "rpm": 60,
                    "emit": ["mw_parsoid.http_start", "mw_parsoid.cache_hit", "mw_parsoid.http_end_ok"],
                    "latency_ms": [[1, 3], [2, 6], [30, 220]],
                    "trace": False,
                },
                {
                    "id": "parsoid_request_db_error",
                    "rpm": 1,
                    "emit": ["mw_parsoid.http_start", "mw_parsoid.cache_miss", "mw_parsoid.db_error", "mw_parsoid.http_end_err"],
                    "latency_ms": [[1, 3], [2, 6], [250, 2000], [300, 2500]],
                    "trace": False,
                },
                {
                    "id": "jobrunner_job_ok",
                    "rpm": 120,
                    "emit": ["mw_jobrunner.http_start", "mw_jobrunner.cache_hit", "mw_jobrunner.http_end_ok"],
                    "latency_ms": [[1, 3], [2, 6], [60, 500]],
                    "trace": False,
                },
                {
                    "id": "jobrunner_job_db_error",
                    "rpm": 2,
                    "emit": ["mw_jobrunner.http_start", "mw_jobrunner.cache_miss", "mw_jobrunner.db_error", "mw_jobrunner.http_end_err"],
                    "latency_ms": [[1, 3], [2, 6], [400, 3000], [500, 3500]],
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {
                    "id": "api_int_request_ok",
                    "rpm": 120,
                    "emit": ["mw_api_int.http_start", "mw_api_int.cache_lookup_failed", "mw_api_int.http_end_ok"],
                    "latency_ms": [[1, 4], [5, 25], [200, 2500]],
                    "trace": False,
                },
                {
                    "id": "api_int_request_db_error",
                    "rpm": 42,
                    "emit": ["mw_api_int.http_start", "mw_api_int.cache_lookup_failed", "mw_api_int.db_error", "mw_api_int.http_end_err"],
                    "latency_ms": [[1, 4], [5, 25], [1200, 12000], [1500, 20000]],
                    "trace": False,
                },
                {
                    "id": "parsoid_request_ok",
                    "rpm": 40,
                    "emit": ["mw_parsoid.http_start", "mw_parsoid.cache_lookup_failed", "mw_parsoid.http_end_ok"],
                    "latency_ms": [[1, 4], [5, 25], [250, 3500]],
                    "trace": False,
                },
                {
                    "id": "parsoid_request_db_error",
                    "rpm": 21,
                    "emit": ["mw_parsoid.http_start", "mw_parsoid.cache_lookup_failed", "mw_parsoid.db_error", "mw_parsoid.http_end_err"],
                    "latency_ms": [[1, 4], [5, 25], [1500, 15000], [2000, 25000]],
                    "trace": False,
                },
                {
                    "id": "jobrunner_job_ok",
                    "rpm": 70,
                    "emit": ["mw_jobrunner.http_start", "mw_jobrunner.cache_lookup_failed", "mw_jobrunner.http_end_ok"],
                    "latency_ms": [[1, 4], [5, 25], [400, 6000]],
                    "trace": False,
                },
                {
                    "id": "jobrunner_job_db_error",
                    "rpm": 52,
                    "emit": ["mw_jobrunner.http_start", "mw_jobrunner.cache_lookup_failed", "mw_jobrunner.db_error", "mw_jobrunner.http_end_err"],
                    "latency_ms": [[1, 4], [5, 25], [2000, 20000], [2500, 30000]],
                    "trace": False,
                },
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {"id": "incident_2025_03_12_extstore_overload_memcached_envvars"},
    "time": {"total_minutes": 44, "phases": {"n": {"start_min": 0, "end_min": 20}, "f": {"start_min": 20, "end_min": 44}}},
    "phases": {
        "f": {
            "events": [
                {
                    "order": 1,
                    "at_min": 20,
                    "rate_multipliers": {"logstash.pipeline_drop": 0.0, "prometheus.scrape_failed": 0.0},
                    "latency_multipliers": {
                        "api_int_request_ok": {"p50": 1.0, "p95": 1.0},
                        "api_int_request_db_error": {"p50": 1.0, "p95": 1.0},
                        "parsoid_request_ok": {"p50": 1.0, "p95": 1.0},
                        "parsoid_request_db_error": {"p50": 1.0, "p95": 1.0},
                        "jobrunner_job_ok": {"p50": 1.0, "p95": 1.0},
                        "jobrunner_job_db_error": {"p50": 1.0, "p95": 1.0},
                    },
                    "one_shots": [{"ref": "scap.deploy_start", "count": 1, "hosts": ["deploy-1"]}],
                },
                {
                    "order": 2,
                    "at_min": 26,
                    "rate_multipliers": {"logstash.pipeline_lag": 4.0, "logstash.pipeline_drop": 1.0, "prometheus.scrape_failed": 1.0},
                    "latency_multipliers": {
                        "api_int_request_ok": {"p50": 1.0, "p95": 1.0},
                        "api_int_request_db_error": {"p50": 1.0, "p95": 1.0},
                        "parsoid_request_ok": {"p50": 1.0, "p95": 1.0},
                        "parsoid_request_db_error": {"p50": 1.0, "p95": 1.0},
                        "jobrunner_job_ok": {"p50": 1.0, "p95": 1.0},
                        "jobrunner_job_db_error": {"p50": 1.0, "p95": 1.0},
                    },
                    "one_shots": [],
                },
                {
                    "order": 3,
                    "at_min": 34,
                    "rate_multipliers": {
                        "parsoid_request_ok": 0.8,
                        "parsoid_request_db_error": 0.8,
                        "jobrunner_job_ok": 0.6,
                        "jobrunner_job_db_error": 0.6,
                        "external_storage_db.query_timeout": 0.6,
                        "logstash.pipeline_lag": 2.0,
                    },
                    "latency_multipliers": {
                        "api_int_request_ok": {"p50": 0.9, "p95": 0.95},
                        "api_int_request_db_error": {"p50": 0.9, "p95": 0.95},
                        "parsoid_request_ok": {"p50": 0.9, "p95": 0.95},
                        "parsoid_request_db_error": {"p50": 0.9, "p95": 0.95},
                        "jobrunner_job_ok": {"p50": 0.9, "p95": 0.95},
                        "jobrunner_job_db_error": {"p50": 0.9, "p95": 0.95},
                    },
                    "one_shots": [
                        {"ref": "ops_console.reduce_cirrus_parallelism", "count": 1, "hosts": ["ops-1"]},
                        {"ref": "ops_console.reduce_changeprop_concurrency", "count": 1, "hosts": ["ops-1"]},
                    ],
                },
            ]
        }
    },
}

SEED = 13371337
BASE_TIME = datetime(2025, 3, 12, 12, 0, 0, tzinfo=timezone.utc)
BASE_MS = int(BASE_TIME.timestamp() * 1000)


def _md5_u64(s: str) -> int:
    h = hashlib.md5(s.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big", signed=False)


def _u01_from_str(s: str) -> float:
    return (_md5_u64(s) % (10**12)) / float(10**12)


def _seed32(s: str) -> int:
    return int(_md5_u64(s) & 0xFFFFFFFF)


def _rng_for(key: str) -> np.random.Generator:
    return np.random.default_rng(_seed32(f"{SEED}:{key}"))


def _ms_to_iso(ms: int) -> str:
    dt = datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{int(ms % 1000):03d}Z"


def det_round(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    n = int(math.floor(expected))
    frac = expected - n
    u = _u01_from_str(f"{SEED}:{key}:round")
    return n + (1 if u < frac else 0)


def spread_offsets(duration_ms: int, count: int, key: str) -> List[int]:
    if count <= 0:
        return []
    spacing = duration_ms / float(count)
    jitter_amp = int(min(500, max(0.0, spacing * 0.30)))
    out = []
    for i in range(count):
        base = (i + 0.5) * spacing
        u = _u01_from_str(f"{SEED}:{key}:jit:{i}") - 0.5
        jit = int(u * 2.0 * jitter_amp)
        off = int(base) + jit
        if off < 0:
            off = 0
        if off >= duration_ms:
            off = duration_ms - 1
        out.append(off)
    out.sort()
    return out


def sample_lognormal_ms(p50: float, p95: float, rng: np.random.Generator, soft_cap_mult: float = 3.0) -> float:
    p50 = max(0.1, float(p50))
    p95 = max(p50 * 1.01, float(p95))
    mu = math.log(p50)
    sigma = math.log(p95 / p50) / 1.6448536269514722
    z = float(rng.normal(0.0, 1.0))
    val = math.exp(mu + sigma * z)
    soft_cap = soft_cap_mult * p95
    if val > soft_cap:
        val = soft_cap + (val - soft_cap) * 0.1
    return val


def clamp(v: float, lo: Optional[float] = None, hi: Optional[float] = None) -> float:
    if lo is not None and v < lo:
        v = lo
    if hi is not None and v > hi:
        v = hi
    return v


def gen_value(domain: Dict[str, Any], rng: np.random.Generator) -> Any:
    k = domain["k"]
    v = domain.get("v")
    if k == "ch":
        choices = list(v)
        idx = int(rng.integers(0, len(choices)))
        return choices[idx]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        return int(rng.integers(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        x = float(rng.uniform(lo, hi))
        return float(f"{x:.2f}")
    if k == "hex":
        n = int(v)
        b = rng.bytes((n + 1) // 2)
        s = b.hex()[:n]
        return s
    if k == "uuid":
        b = rng.bytes(16)
        hexs = b.hex()
        return f"{hexs[0:8]}-{hexs[8:12]}-{hexs[12:16]}-{hexs[16:20]}-{hexs[20:32]}"
    if k == "ip":
        return "127.0.0.1"
    if k == "str":
        base = str(v) if v is not None else "str"
        suffix = gen_value({"k": "hex", "v": 6}, rng)
        return f"{base}:{suffix}"
    return str(v)


def escape_unknown_format_fields(msg: str, allowed_fields: set) -> str:
    fmt = string.Formatter()
    out_parts: List[str] = []
    for lit, field_name, format_spec, conversion in fmt.parse(msg):
        out_parts.append(lit)
        if field_name is None:
            continue
        if field_name in allowed_fields:
            ph = "{" + field_name
            if conversion:
                ph += "!" + conversion
            if format_spec:
                ph += ":" + format_spec
            ph += "}"
            out_parts.append(ph)
        else:
            inner = field_name
            if conversion:
                inner += "!" + conversion
            if format_spec:
                inner += ":" + format_spec
            out_parts.append("{{" + inner + "}}")
    return "".join(out_parts)


@dataclass(frozen=True)
class LogTemplate:
    component_id: str
    log_id: str
    level: str
    msg: str
    fmt_msg: str
    vars: Dict[str, Dict[str, Any]]
    state_vars: Dict[str, Dict[str, Dict[str, Any]]]


def build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, LogTemplate], Dict[Tuple[str, str], Dict[str, Any]]]:
    comps = {c["id"]: c for c in system["components"]}
    tmpl: Dict[str, LogTemplate] = {}
    for cid, c in comps.items():
        for lid, ldef in c.get("logs", {}).items():
            vars_ = ldef.get("vars", {}) or {}
            state_vars_ = ldef.get("state_vars", {}) or {}
            allowed_fields = set(vars_.keys())
            for _st, sv in state_vars_.items():
                allowed_fields.update((sv or {}).keys())
            msg = ldef["msg"]
            fmt_msg = escape_unknown_format_fields(msg, allowed_fields)
            tmpl[f"{cid}.{lid}"] = LogTemplate(
                component_id=cid,
                log_id=lid,
                level=ldef["lvl"],
                msg=msg,
                fmt_msg=fmt_msg,
                vars=vars_,
                state_vars=state_vars_,
            )
    flows: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for state, fdef in system["flows"].items():
        for req in fdef["req"]:
            flows[(state, req["id"])] = req
    return comps, tmpl, flows


def derive_failure_intervals(scenario: Dict[str, Any]) -> List[Dict[str, Any]]:
    fstart = scenario["time"]["phases"]["f"]["start_min"]
    fend = scenario["time"]["phases"]["f"]["end_min"]
    events = sorted(scenario["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = [fstart] + sorted({e["at_min"] for e in events if fstart <= e["at_min"] <= fend}) + [fend]
    boundaries = sorted(set(boundaries))
    intervals = []
    for i in range(len(boundaries) - 1):
        s = boundaries[i]
        e = boundaries[i + 1]
        active_rate: Dict[str, float] = {}
        active_lat: Dict[str, Dict[str, float]] = {}
        for ev in events:
            if ev["at_min"] <= s:
                for k, m in (ev.get("rate_multipliers") or {}).items():
                    active_rate[k] = float(m)
                for fid, mm in (ev.get("latency_multipliers") or {}).items():
                    active_lat[fid] = {"p50": float(mm["p50"]), "p95": float(mm["p95"])}
        intervals.append({"start_min": s, "end_min": e, "rate_mult": active_rate, "lat_mult": active_lat})
    return intervals


def render_message(template: LogTemplate, values: Dict[str, Any]) -> str:
    clean = {}
    for k, v in values.items():
        if isinstance(v, float):
            clean[k] = float(f"{v:.2f}")
        else:
            clean[k] = v
    return template.fmt_msg.format_map(clean)


def choose_host(component: Dict[str, Any], scope: str, host_hint: Optional[str], key: str) -> str:
    hosts = component.get("hosts") or []
    if not hosts:
        return ""
    if host_hint and host_hint in hosts:
        return host_hint
    if scope == "global":
        return hosts[0]
    u = _u01_from_str(f"{SEED}:{key}:host")
    idx = int(math.floor(u * len(hosts))) % len(hosts)
    return hosts[idx]


def emit_row(rows: List[Dict[str, Any]], ts_ms: int, tmpl: LogTemplate, message: str, trace_id: str, service: str, host: str) -> None:
    rows.append(
        {
            "ts_ms": ts_ms,
            "timestamp": _ms_to_iso(ts_ms),
            "level": tmpl.level,
            "message": message,
            "trace_id": trace_id,
            "service": service,
            "host": host,
        }
    )


def simulate_background_emit(
    rows: List[Dict[str, Any]],
    comps: Dict[str, Any],
    templates: Dict[str, LogTemplate],
    state: str,
    interval_start_ms: int,
    interval_end_ms: int,
    component_id: str,
    log_id: str,
    count: int,
    scope: str,
    host: str,
    interval_tag: str,
) -> None:
    comp = comps[component_id]
    tmpl = templates[f"{component_id}.{log_id}"]
    duration_ms = interval_end_ms - interval_start_ms
    offsets = spread_offsets(duration_ms, count, f"bg:{interval_tag}:{component_id}.{log_id}:{host}:{scope}")
    for j, off in enumerate(offsets):
        ts = interval_start_ms + off
        rng = _rng_for(f"bgval:{interval_tag}:{component_id}.{log_id}:{host}:{j}")

        domains: Dict[str, Dict[str, Any]] = {}
        domains.update(tmpl.vars or {})
        if tmpl.state_vars and state in tmpl.state_vars:
            domains.update(tmpl.state_vars[state] or {})

        values: Dict[str, Any] = {}
        for k, dom in domains.items():
            values[k] = gen_value(dom, rng)

        if component_id == "logstash" and log_id == "pipeline_lag":
            minute = int((ts - BASE_MS) // 60000)
            if state == "n":
                values["lag_s"] = float(f"{rng.uniform(0.0, 3.0):.2f}")
                values["queue_depth"] = int(rng.integers(0, 2000))
                values["inflight"] = int(rng.integers(50, 800))
            else:
                if minute < 26:
                    values["lag_s"] = float(f"{rng.uniform(5.0, 40.0):.2f}")
                    values["queue_depth"] = int(rng.integers(5000, 50000))
                    values["inflight"] = int(rng.integers(800, 5000))
                elif minute < 34:
                    values["lag_s"] = float(f"{rng.uniform(80.0, 320.0):.2f}")
                    values["queue_depth"] = int(rng.integers(80000, 420000))
                    values["inflight"] = int(rng.integers(5000, 45000))
                else:
                    values["lag_s"] = float(f"{rng.uniform(30.0, 160.0):.2f}")
                    values["queue_depth"] = int(rng.integers(30000, 220000))
                    values["inflight"] = int(rng.integers(2000, 25000))

        if component_id in ("mw_api_int", "mw_parsoid", "mw_jobrunner") and log_id == "memcached_spam":
            errno = int(values.get("errno", 110))
            if errno == 110:
                values["errmsg"] = "Connection timed out"
            elif errno == 113:
                values["errmsg"] = "No route to host"
            else:
                values["errmsg"] = "Connection refused"

        msg = render_message(tmpl, values)
        emit_row(rows, ts, tmpl, msg, "", comp.get("svc", "") or "", host)


def _adjust_attempt_delays_to_duration_bounds(
    delays_ms: List[int],
    emit_refs: List[str],
    min_elapsed_ms: int,
    max_elapsed_ms: int,
) -> List[int]:
    """
    The completion log's dur_ms is derived from:
        end_log_ms - first_log_ms == sum(delays_ms[1:])

    Ensure this derived duration fits the completion template's [min, max] dur_ms bounds by
    adjusting delays deterministically:
      - cap sum(delays_ms[1:]) <= max_elapsed_ms by scaling down (respecting per-log minimums),
      - ensure sum(delays_ms[1:]) >= min_elapsed_ms by extending the final delay.

    This preserves coherence between message dur_ms and actual timestamp gaps.
    """
    if len(delays_ms) <= 1:
        return delays_ms

    min_elapsed_ms = int(max(0, min_elapsed_ms))
    max_elapsed_ms = int(max(min_elapsed_ms, max_elapsed_ms))

    # --- Max cap (existing behavior) ---
    elapsed = int(sum(delays_ms[1:]))
    if elapsed > max_elapsed_ms:
        factor = max_elapsed_ms / float(elapsed)
        new_delays = delays_ms[:]

        # per-log minimums: db_error must remain within its domain minimum
        mins = [1] * len(delays_ms)
        for i, ref in enumerate(emit_refs):
            if i == 0:
                continue
            if ref.endswith(".db_error"):
                mins[i] = 500
            else:
                mins[i] = 1

        for i in range(1, len(new_delays)):
            scaled = int(round(new_delays[i] * factor))
            if scaled < mins[i]:
                scaled = mins[i]
            new_delays[i] = max(1, scaled)

        elapsed2 = int(sum(new_delays[1:]))
        if elapsed2 > max_elapsed_ms:
            excess = elapsed2 - max_elapsed_ms
            idxs = [i for i in range(1, len(new_delays)) if new_delays[i] > mins[i]]
            idxs.sort(key=lambda i: (-new_delays[i], i))
            for i in idxs:
                if excess <= 0:
                    break
                reducible = new_delays[i] - mins[i]
                if reducible <= 0:
                    continue
                dec = reducible if reducible < excess else excess
                new_delays[i] -= dec
                excess -= dec
        delays_ms = new_delays

    # --- Min floor: extend final delay so completion dur_ms >= template minimum ---
    elapsed = int(sum(delays_ms[1:]))
    if elapsed < min_elapsed_ms:
        need = min_elapsed_ms - elapsed
        # Add all required time to the final delay (between prior log and completion log).
        delays_ms[-1] = int(max(1, delays_ms[-1] + need))

    # Final sanity: keep delays positive
    for i in range(len(delays_ms)):
        if delays_ms[i] < 1:
            delays_ms[i] = 1
    return delays_ms


def _tmpl_dur_bounds_ms(tmpl: LogTemplate) -> Tuple[int, int]:
    dom = (tmpl.vars or {}).get("dur_ms")
    if not dom or dom.get("k") != "i":
        return (0, 60000)
    v = dom.get("v") or [0, 60000]
    try:
        return (int(v[0]), int(v[1]))
    except Exception:
        return (0, 60000)


def simulate_flow_instance(
    rows: List[Dict[str, Any]],
    comps: Dict[str, Any],
    templates: Dict[str, LogTemplate],
    flow: Dict[str, Any],
    state: str,
    start_ms: int,
    latency_mult: Dict[str, float],
    instance_key: str,
) -> None:
    emit_refs: List[str] = list(flow["emit"])
    lat_pairs: List[List[float]] = list(flow["latency_ms"])

    rng = _rng_for(f"flowctx:{state}:{flow['id']}:{instance_key}")

    req_id = gen_value({"k": "hex", "v": 16}, rng)

    comp_id_first, _log_id_first = emit_refs[0].split(".", 1)
    if comp_id_first == "mw_api_int":
        cache_key_hint = SYSTEM["components"][1]["logs"]["cache_hit"]["vars"]["cache_key"]["v"]
    elif comp_id_first == "mw_parsoid":
        cache_key_hint = SYSTEM["components"][2]["logs"]["cache_hit"]["vars"]["cache_key"]["v"]
    else:
        cache_key_hint = SYSTEM["components"][3]["logs"]["cache_hit"]["vars"]["cache_key"]["v"]
    cache_key = f"{cache_key_hint}:{req_id[:6]}"

    db_err = None
    op = None
    status = None
    if any(ref.endswith(".db_error") for ref in emit_refs):
        op = gen_value({"k": "ch", "v": ["SELECT", "UPDATE"]}, rng)
        db_err = gen_value({"k": "ch", "v": ["timeout", "too_many_connections", "lock_wait_timeout"]}, rng)
        status = "503" if db_err in ("timeout", "too_many_connections") else "500"
    else:
        status = "200"

    method = None
    uri = None
    job = None
    if comp_id_first in ("mw_api_int", "mw_parsoid"):
        method = gen_value({"k": "ch", "v": ["GET", "POST"]}, rng)
        if comp_id_first == "mw_api_int":
            uri = gen_value({"k": "ch", "v": ["/w/api.php", "/w/rest.php"]}, rng)
        else:
            uri = gen_value({"k": "ch", "v": ["/v3/page/html", "/v3/transform/wikitext/to/html"]}, rng)
    elif comp_id_first == "mw_jobrunner":
        job = gen_value({"k": "ch", "v": ["categoryMembershipChange", "refreshLinks", "cirrusUpdate"]}, rng)

    delays_ms: List[int] = []
    for i, pair in enumerate(lat_pairs):
        p50 = float(pair[0]) * float(latency_mult.get("p50", 1.0))
        p95 = float(pair[1]) * float(latency_mult.get("p95", 1.0))
        v = sample_lognormal_ms(p50, p95, rng)
        ref = emit_refs[i]
        if ref.endswith(".db_error"):
            v = clamp(v, lo=500.0, hi=60000.0)
        delays_ms.append(int(max(1, round(v))))

    # Ensure completion dur_ms matches timestamp gaps and respects template bounds.
    end_ref = emit_refs[-1]
    end_tmpl = templates[end_ref]
    min_dur, max_dur = _tmpl_dur_bounds_ms(end_tmpl)
    delays_ms = _adjust_attempt_delays_to_duration_bounds(delays_ms, emit_refs, min_elapsed_ms=min_dur, max_elapsed_ms=max_dur)

    current_ms = start_ms
    first_log_ms = None
    end_log_ms = None

    host_by_comp: Dict[str, str] = {}

    for i, ref in enumerate(emit_refs):
        comp_id, log_id = ref.split(".", 1)
        comp = comps[comp_id]
        tmpl = templates[ref]

        current_ms += delays_ms[i]
        if first_log_ms is None:
            first_log_ms = current_ms
        end_log_ms = current_ms

        if comp_id not in host_by_comp:
            host_by_comp[comp_id] = choose_host(comp, "per_host", None, f"flow:{instance_key}:{comp_id}")
        host = host_by_comp[comp_id]

        domains: Dict[str, Dict[str, Any]] = {}
        domains.update(tmpl.vars or {})
        if tmpl.state_vars and state in tmpl.state_vars:
            domains.update(tmpl.state_vars[state] or {})

        values: Dict[str, Any] = {}
        row_rng = _rng_for(f"flowval:{state}:{flow['id']}:{instance_key}:{i}")
        for k, dom in domains.items():
            values[k] = gen_value(dom, row_rng)

        if "req_id" in tmpl.msg:
            values["req_id"] = req_id
        if log_id in ("cache_hit", "cache_miss", "cache_lookup_failed"):
            values["cache_key"] = cache_key
        if log_id == "http_start":
            if "method" in tmpl.msg and method is not None:
                values["method"] = method
            if "uri" in tmpl.msg and uri is not None:
                values["uri"] = uri
            if "job" in tmpl.msg and job is not None:
                values["job"] = job
        if log_id == "db_error":
            values["op"] = op
            values["db_err"] = db_err
            values["dur_ms"] = int(delays_ms[i])
        if log_id in ("http_end_ok", "http_end_err"):
            assert first_log_ms is not None and end_log_ms is not None
            total_ms = int(end_log_ms - first_log_ms)
            if "dur_ms" in tmpl.msg:
                # total_ms is constructed to fit the template's [min,max] bounds.
                values["dur_ms"] = total_ms
            if log_id == "http_end_err" and "status" in tmpl.msg:
                values["status"] = status

        msg = render_message(tmpl, values)
        emit_row(rows, current_ms, tmpl, msg, "", comp.get("svc", "") or "", host)


def main() -> None:
    random.seed(SEED)
    np.random.seed(SEED)

    comps, templates, _flows = build_indices(SYSTEM)

    n_start = SCENARIO["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["time"]["phases"]["n"]["end_min"]
    f_start = SCENARIO["time"]["phases"]["f"]["start_min"]
    f_end = SCENARIO["time"]["phases"]["f"]["end_min"]

    rows: List[Dict[str, Any]] = []

    # --- Normal phase background ---
    n_start_ms = BASE_MS + n_start * 60_000
    n_end_ms = BASE_MS + n_end * 60_000
    n_dur_min = (n_end_ms - n_start_ms) / 60_000.0
    for cid, comp in comps.items():
        for emit in comp.get("beh", {}).get("n", {}).get("emit", []):
            log_id = emit["id"]
            per_min = float(emit["per_min"])
            scope = emit.get("scope", "per_host")
            if scope == "global":
                key = f"n:bg:{cid}.{log_id}:global:{n_start}-{n_end}"
                cnt = det_round(per_min * n_dur_min, key)
                host = choose_host(comp, "global", None, key)
                simulate_background_emit(rows, comps, templates, "n", n_start_ms, n_end_ms, cid, log_id, cnt, "global", host, f"{n_start}-{n_end}")
            else:
                for h in comp.get("hosts") or [""]:
                    key = f"n:bg:{cid}.{log_id}:{h}:{n_start}-{n_end}"
                    cnt = det_round(per_min * n_dur_min, key)
                    simulate_background_emit(rows, comps, templates, "n", n_start_ms, n_end_ms, cid, log_id, cnt, "per_host", h, f"{n_start}-{n_end}")

    # --- Normal phase flows ---
    for req in SYSTEM["flows"]["n"]["req"]:
        fid = req["id"]
        rpm = float(req["rpm"])
        expected_instances = rpm * (n_end - n_start)
        cnt = det_round(expected_instances, f"n:flow:{fid}:{n_start}-{n_end}")
        duration_ms = n_end_ms - n_start_ms
        start_offsets = spread_offsets(duration_ms, cnt, f"n:flowstarts:{fid}:{n_start}-{n_end}")
        for idx, off in enumerate(start_offsets):
            start_ms = n_start_ms + off
            simulate_flow_instance(rows, comps, templates, req, "n", start_ms, {"p50": 1.0, "p95": 1.0}, instance_key=f"n:{fid}:{n_start}-{n_end}:{idx}")

    # --- Failure phase intervals (piecewise controls) ---
    failure_intervals = derive_failure_intervals(SCENARIO)

    for interval in failure_intervals:
        smin = int(interval["start_min"])
        emin = int(interval["end_min"])
        s_ms = BASE_MS + smin * 60_000
        e_ms = BASE_MS + emin * 60_000
        dur_min = (e_ms - s_ms) / 60_000.0
        rate_mult = interval["rate_mult"]
        lat_mult = interval["lat_mult"]
        interval_tag = f"{smin}-{emin}"

        # Background in failure state
        for cid, comp in comps.items():
            for emit in comp.get("beh", {}).get("f", {}).get("emit", []):
                log_id = emit["id"]
                per_min = float(emit["per_min"])
                scope = emit.get("scope", "per_host")
                src_key = f"{cid}.{log_id}"
                mult = float(rate_mult.get(src_key, 1.0))
                eff_per_min = per_min * mult
                if eff_per_min <= 0:
                    continue
                if scope == "global":
                    key = f"f:bg:{src_key}:global:{interval_tag}"
                    cnt = det_round(eff_per_min * dur_min, key)
                    host = choose_host(comp, "global", None, key)
                    simulate_background_emit(rows, comps, templates, "f", s_ms, e_ms, cid, log_id, cnt, "global", host, interval_tag)
                else:
                    for h in comp.get("hosts") or [""]:
                        key = f"f:bg:{src_key}:{h}:{interval_tag}"
                        cnt = det_round(eff_per_min * dur_min, key)
                        simulate_background_emit(rows, comps, templates, "f", s_ms, e_ms, cid, log_id, cnt, "per_host", h, interval_tag)

        # Flows in failure state
        for req in SYSTEM["flows"]["f"]["req"]:
            fid = req["id"]
            rpm = float(req["rpm"])
            mult = float(rate_mult.get(fid, 1.0))
            eff_rpm = rpm * mult
            if eff_rpm <= 0:
                continue
            expected_instances = eff_rpm * (emin - smin)
            cnt = det_round(expected_instances, f"f:flow:{fid}:{interval_tag}")
            duration_ms = e_ms - s_ms
            start_offsets = spread_offsets(duration_ms, cnt, f"f:flowstarts:{fid}:{interval_tag}")

            lm = lat_mult.get(fid, {"p50": 1.0, "p95": 1.0})
            for idx, off in enumerate(start_offsets):
                start_ms = s_ms + off
                simulate_flow_instance(rows, comps, templates, req, "f", start_ms, lm, instance_key=f"f:{fid}:{interval_tag}:{idx}")

    # --- One-shots ---
    for ev in sorted(SCENARIO["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"])):
        at_min = int(ev["at_min"])
        at_ms = BASE_MS + at_min * 60_000
        for os in ev.get("one_shots") or []:
            ref = os["ref"]
            comp_id, log_id = ref.split(".", 1)
            comp = comps[comp_id]
            tmpl = templates[ref]
            count = int(os["count"])
            hosts = os.get("hosts") or (comp.get("hosts") or [""])
            duration_ms = 5_000
            offsets = spread_offsets(duration_ms, count, f"oneshot:{ref}:{at_min}")
            for i, off in enumerate(offsets):
                host = hosts[i % len(hosts)] if hosts else choose_host(comp, "global", None, f"oneshot:{ref}:{i}")
                ts = at_ms + off
                rng = _rng_for(f"oneshotval:{ref}:{at_min}:{i}")
                values = {}
                domains = {}
                domains.update(tmpl.vars or {})
                for k, dom in domains.items():
                    values[k] = gen_value(dom, rng)
                msg = render_message(tmpl, values)
                emit_row(rows, ts, tmpl, msg, "", comp.get("svc", "") or "", host)

    # --- Finalize CSV ---
    df = pd.DataFrame(rows)
    df.sort_values(["ts_ms", "service", "host", "level", "message"], inplace=True, kind="mergesort")
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]].reset_index(drop=True)

    nrows = len(df)
    if not (20000 <= nrows <= 100000):
        raise RuntimeError(f"Row count out of target range: {nrows}")

    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
