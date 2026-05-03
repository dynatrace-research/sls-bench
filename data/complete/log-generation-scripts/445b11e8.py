import hashlib
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

import numpy as np
import pandas as pd

# Fixed seeds for verifier-required reproducibility (even though the simulator is hash-deterministic).
random.seed(0)
np.random.seed(0)

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "sessionstore_cassandra_quorum_outage"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "mediawiki_app",
            "svc": "mediawiki",
            "hosts": ["mw1", "mw2", "mw3"],
            "logs": {
                "heartbeat": {
                    "lvl": "INFO",
                    "msg": "heartbeat app=mediawiki build={build} heap_used_mb={heap_mb}",
                    "vars": {"build": {"k": "ch", "v": ["1.42.0-wmf.15"]}, "heap_mb": {"k": "i", "v": [800, 2600]}},
                },
                "page_request": {
                    "lvl": "INFO",
                    "msg": "page_request req_id={req_id} path=/wiki/{page} user={user} session={session_state}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "page": {"k": "ch", "v": ["Main_Page", "Help:Editing", "Project:About", "Talk:Sandbox"]},
                        "user": {"k": "ch", "v": ["anon", "logged_in", "bot"]},
                    },
                    "state_vars": {
                        "n": {"session_state": {"k": "ch", "v": ["present"]}},
                        "f": {"session_state": {"k": "ch", "v": ["present", "missing"]}},
                    },
                },
                "page_response_200": {
                    "lvl": "INFO",
                    "msg": "page_response req_id={req_id} status=200 dur_ms={dur_ms} cache={cache}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [10, 6000]},
                        "cache": {"k": "ch", "v": ["hit", "miss"]},
                    },
                },
                "edit_submit": {
                    "lvl": "INFO",
                    "msg": "edit_submit req_id={req_id} page={page} user={user}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "page": {"k": "ch", "v": ["Main_Page", "Help:Editing", "Project:About", "Talk:Sandbox"]},
                        "user": {"k": "ch", "v": ["anon", "logged_in", "bot"]},
                    },
                },
                "edit_result_ok": {
                    "lvl": "INFO",
                    "msg": "edit_result req_id={req_id} result=success rev_id={rev_id} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "rev_id": {"k": "i", "v": [9000000, 9050000]},
                        "dur_ms": {"k": "i", "v": [150, 5000]},
                    },
                },
                "edit_result_fail": {
                    "lvl": "WARN",
                    "msg": "edit_result req_id={req_id} result=failed reason={reason} upstream_status={upstream_status} dur_ms={dur_ms}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "reason": {"k": "ch", "v": ["session_lost", "sessionstore_5xx", "token_mismatch"]},
                        "upstream_status": {"k": "i", "v": [500, 503]},
                        "dur_ms": {"k": "i", "v": [200, 9000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "heartbeat", "per_min": 0.2, "scope": "per_host"}]},
                "f": {"emit": [{"id": "heartbeat", "per_min": 0.2, "scope": "per_host"}]},
            },
        },
        {
            "id": "sessionstore_api",
            "svc": "sessionstore",
            "hosts": ["ss1", "ss2", "ss3", "ss4"],
            "logs": {
                "http_get_200": {
                    "lvl": "INFO",
                    "msg": "http_access req_id={req_id} method=GET route=/v1/session status=200 dur_ms={dur_ms} dc={dc}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [2, 3000]},
                        "dc": {"k": "ch", "v": ["eqiad", "codfw"]},
                    },
                },
                "http_get_500": {
                    "lvl": "ERROR",
                    "msg": "http_access req_id={req_id} method=GET route=/v1/session status=500 dur_ms={dur_ms} dc={dc}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [10, 8000]},
                        "dc": {"k": "ch", "v": ["eqiad", "codfw"]},
                    },
                },
                "http_put_200": {
                    "lvl": "INFO",
                    "msg": "http_access req_id={req_id} method=PUT route=/v1/session status=200 dur_ms={dur_ms} dc={dc} bytes_in={bytes_in}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [3, 4000]},
                        "dc": {"k": "ch", "v": ["eqiad", "codfw"]},
                        "bytes_in": {"k": "i", "v": [200, 6000]},
                    },
                },
                "http_put_500": {
                    "lvl": "ERROR",
                    "msg": "http_access req_id={req_id} method=PUT route=/v1/session status=500 dur_ms={dur_ms} dc={dc} bytes_in={bytes_in}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "dur_ms": {"k": "i", "v": [20, 12000]},
                        "dc": {"k": "ch", "v": ["eqiad", "codfw"]},
                        "bytes_in": {"k": "i", "v": [200, 6000]},
                    },
                },
                "cassandra_op_ok": {
                    "lvl": "INFO",
                    "msg": "cassandra_op req_id={req_id} op={op} consistency=QUORUM result=OK dur_ms={dur_ms} dc={dc}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "op": {"k": "ch", "v": ["read", "write"]},
                        "dur_ms": {"k": "i", "v": [1, 2000]},
                        "dc": {"k": "ch", "v": ["eqiad", "codfw"]},
                    },
                },
                "cassandra_op_fail": {
                    "lvl": "ERROR",
                    "msg": "cassandra_op req_id={req_id} op={op} consistency=QUORUM result=ERR err={err} waited_ms={waited_ms} dc={dc}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "op": {"k": "ch", "v": ["read", "write"]},
                        "err": {"k": "ch", "v": ["UnavailableException", "NoHostAvailable", "ReadTimeout", "WriteTimeout"]},
                        "waited_ms": {"k": "i", "v": [50, 6000]},
                        "dc": {"k": "ch", "v": ["eqiad", "codfw"]},
                    },
                },
                "retrying_cassandra": {
                    "lvl": "WARN",
                    "msg": "retrying_cassandra req_id={req_id} op={op} attempt={attempt} backoff_ms={backoff_ms} err={err}",
                    "vars": {
                        "req_id": {"k": "hex", "v": 16},
                        "op": {"k": "ch", "v": ["read", "write"]},
                        "attempt": {"k": "i", "v": [2, 3]},
                        "backoff_ms": {"k": "i", "v": [20, 400]},
                        "err": {"k": "ch", "v": ["UnavailableException", "NoHostAvailable", "ReadTimeout", "WriteTimeout"]},
                    },
                },
                "pool_health": {
                    "lvl": "INFO",
                    "msg": "pool_health state={state} live_hosts={live_hosts} required_quorum={required_quorum} dc={dc}",
                    "vars": {"required_quorum": {"k": "i", "v": [4, 4]}, "dc": {"k": "ch", "v": ["eqiad", "codfw"]}},
                    "state_vars": {
                        "n": {"state": {"k": "ch", "v": ["OK"]}, "live_hosts": {"k": "i", "v": [6, 6]}},
                        "f": {"state": {"k": "ch", "v": ["DEGRADED"]}, "live_hosts": {"k": "i", "v": [1, 3]}},
                    },
                },
                "pool_health_ok": {"lvl": "INFO", "msg": "pool_health state=OK live_hosts=6 required_quorum=4 dc={dc}", "vars": {"dc": {"k": "ch", "v": ["eqiad", "codfw"]}}},
            },
            "beh": {
                "n": {"emit": [{"id": "pool_health", "per_min": 0.2, "scope": "per_host"}]},
                "f": {"emit": [{"id": "pool_health", "per_min": 0.5, "scope": "per_host"}, {"id": "pool_health_ok", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        {
            "id": "cassandra_cluster",
            "svc": "cassandra",
            "hosts": ["cass-a1", "cass-a2", "cass-a3", "cass-b1", "cass-b2", "cass-b3"],
            "logs": {
                "disk_usage": {
                    "lvl": "INFO",
                    "msg": "disk_usage node={node} mount=/var/lib/cassandra free_pct={free_pct} used_gb={used_gb} pending_compactions={pending_compactions}",
                    "vars": {"node": {"k": "ch", "v": ["cass-a1", "cass-a2", "cass-a3", "cass-b1", "cass-b2", "cass-b3"]}},
                    "state_vars": {
                        "n": {"free_pct": {"k": "i", "v": [30, 55]}, "used_gb": {"k": "i", "v": [350, 650]}, "pending_compactions": {"k": "i", "v": [0, 12]}},
                        "f": {"free_pct": {"k": "i", "v": [0, 5]}, "used_gb": {"k": "i", "v": [700, 980]}, "pending_compactions": {"k": "i", "v": [10, 120]}},
                    },
                },
                "disk_usage_recovered": {
                    "lvl": "INFO",
                    "msg": "disk_usage node={node} mount=/var/lib/cassandra free_pct={free_pct} used_gb={used_gb} pending_compactions={pending_compactions}",
                    "vars": {
                        "node": {"k": "ch", "v": ["cass-a1", "cass-a2", "cass-a3", "cass-b1", "cass-b2", "cass-b3"]},
                        "free_pct": {"k": "i", "v": [35, 65]},
                        "used_gb": {"k": "i", "v": [50, 220]},
                        "pending_compactions": {"k": "i", "v": [0, 25]},
                    },
                },
                "compaction_backlog": {
                    "lvl": "WARN",
                    "msg": "compaction_backlog node={node} pending_compactions={pending_compactions} oldest_sstable_age_h={oldest_age_h}",
                    "vars": {
                        "node": {"k": "ch", "v": ["cass-a1", "cass-a2", "cass-a3", "cass-b1", "cass-b2", "cass-b3"]},
                        "pending_compactions": {"k": "i", "v": [0, 140]},
                        "oldest_age_h": {"k": "i", "v": [1, 96]},
                    },
                },
                "commitlog_no_space": {
                    "lvl": "CRITICAL",
                    "msg": "commitlog_error node={node} error='No space left on device' action=shutdown",
                    "vars": {"node": {"k": "ch", "v": ["cass-a1", "cass-a2", "cass-a3", "cass-b1", "cass-b2", "cass-b3"]}},
                },
                "node_marked_down": {
                    "lvl": "WARN",
                    "msg": "gossip_event node={node} state=DOWN reason={reason}",
                    "vars": {
                        "node": {"k": "ch", "v": ["cass-a1", "cass-a2", "cass-a3", "cass-b1", "cass-b2", "cass-b3"]},
                        "reason": {"k": "ch", "v": ["disk_full_shutdown", "io_error"]},
                    },
                },
                "truncate_sessions": {
                    "lvl": "WARN",
                    "msg": "operator_action action=truncate_keyspace keyspace=sessionstore dc={dc} result=OK freed_gb={freed_gb}",
                    "vars": {"dc": {"k": "ch", "v": ["eqiad", "codfw"]}, "freed_gb": {"k": "i", "v": [400, 950]}},
                },
                "cassandra_restart": {
                    "lvl": "INFO",
                    "msg": "service_restart node={node} svc=cassandra reason={reason}",
                    "vars": {
                        "node": {"k": "ch", "v": ["cass-a1", "cass-a2", "cass-a3", "cass-b1", "cass-b2", "cass-b3"]},
                        "reason": {"k": "ch", "v": ["post_truncate", "disk_full_recovery"]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "disk_usage", "per_min": 1.0, "scope": "per_host"}, {"id": "compaction_backlog", "per_min": 0.05, "scope": "per_host"}]},
                "f": {"emit": [{"id": "disk_usage", "per_min": 1.0, "scope": "per_host"}, {"id": "compaction_backlog", "per_min": 0.2, "scope": "per_host"}, {"id": "disk_usage_recovered", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        {
            "id": "monitoring_alerts",
            "svc": "alerting",
            "hosts": ["mon1"],
            "logs": {
                "alert_fired_sessionstore_5xx": {"lvl": "CRITICAL", "msg": "alert_fired name=SessionStoreErrorRateHigh severity=page summary={summary}", "vars": {"summary": {"k": "str", "v": "short_summary"}}},
                "alert_fired_edit_failures": {"lvl": "CRITICAL", "msg": "alert_fired name=MediaWikiEditFailures severity=critical summary={summary}", "vars": {"summary": {"k": "str", "v": "short_summary"}}},
                "page_sent": {"lvl": "CRITICAL", "msg": "page_sent name=SessionStoreErrorRateHigh target=batphone incident={incident}", "vars": {"incident": {"k": "i", "v": [5900, 6000]}}},
                "alert_active_sessionstore_5xx": {"lvl": "WARN", "msg": "alert_active name=SessionStoreErrorRateHigh severity=page since_min={since_min}", "vars": {"since_min": {"k": "i", "v": [0, 60]}}},
                "alert_active_edit_failures": {"lvl": "WARN", "msg": "alert_active name=MediaWikiEditFailures severity=critical since_min={since_min}", "vars": {"since_min": {"k": "i", "v": [0, 60]}}},
                "alert_resolved_sessionstore_5xx": {"lvl": "INFO", "msg": "alert_resolved name=SessionStoreErrorRateHigh", "vars": {}},
                "alert_resolved_edit_failures": {"lvl": "INFO", "msg": "alert_resolved name=MediaWikiEditFailures", "vars": {}},
            },
            "beh": {
                "n": {"emit": [{"id": "alert_active_sessionstore_5xx", "per_min": 0.0, "scope": "global"}, {"id": "alert_active_edit_failures", "per_min": 0.0, "scope": "global"}]},
                "f": {"emit": [{"id": "alert_active_sessionstore_5xx", "per_min": 0.25, "scope": "global"}, {"id": "alert_active_edit_failures", "per_min": 0.25, "scope": "global"}]},
            },
        },
        {
            "id": "oncall_ops",
            "svc": "ops",
            "hosts": ["ops1"],
            "logs": {
                "ack_page": {"lvl": "INFO", "msg": "oncall_ack user={user} channel=batphone", "vars": {"user": {"k": "ch", "v": ["swfrench", "urandom"]}}},
                "investigation_note": {
                    "lvl": "INFO",
                    "msg": "investigation_note user={user} note={note}",
                    "vars": {"user": {"k": "ch", "v": ["swfrench", "urandom"]}, "note": {"k": "ch", "v": ["checking_sessionstore_errors", "suspect_cassandra", "confirmed_quorum_loss", "disk_full_root_cause"]}},
                },
                "decision_log": {"lvl": "WARN", "msg": "decision user={user} action={action} scope={scope}", "vars": {"user": {"k": "ch", "v": ["swfrench"]}, "action": {"k": "ch", "v": ["wipe_sessionstore_cassandra_data"]}, "scope": {"k": "ch", "v": ["both_dcs"]}}},
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "flows": {
        "n": [
            {
                "id": "mw_page_view_ok",
                "rpm": 140.0,
                "emit": ["mediawiki_app.page_request", "sessionstore_api.http_get_200", "mediawiki_app.page_response_200"],
                "latency_ms": [[1, 3], [6, 25], [20, 120]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "mw_edit_save_ok",
                "rpm": 35.0,
                "emit": ["mediawiki_app.edit_submit", "sessionstore_api.http_put_200", "mediawiki_app.edit_result_ok"],
                "latency_ms": [[1, 3], [10, 40], [120, 350]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "ss_cassandra_read_sample_ok",
                "rpm": 20.0,
                "emit": ["sessionstore_api.cassandra_op_ok"],
                "latency_ms": [[4, 20]],
                "retry": {"max_attempts": 2, "expected_attempts": 1.05, "emit_per_retry": ["sessionstore_api.retrying_cassandra"], "backoff_ms": [[10, 30]]},
                "trace": False,
            },
            {
                "id": "ss_cassandra_write_sample_ok",
                "rpm": 6.0,
                "emit": ["sessionstore_api.cassandra_op_ok"],
                "latency_ms": [[6, 30]],
                "retry": {"max_attempts": 2, "expected_attempts": 1.05, "emit_per_retry": ["sessionstore_api.retrying_cassandra"], "backoff_ms": [[10, 30]]},
                "trace": False,
            },
        ],
        "f": [
            {
                "id": "mw_page_view_sessionstore_5xx",
                "rpm": 140.0,
                "emit": ["mediawiki_app.page_request", "sessionstore_api.http_get_500", "mediawiki_app.page_response_200"],
                "latency_ms": [[1, 3], [80, 1200], [30, 250]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "mw_page_view_ok_post_wipe",
                "rpm": 140.0,
                "emit": ["mediawiki_app.page_request", "sessionstore_api.http_get_200", "mediawiki_app.page_response_200"],
                "latency_ms": [[1, 3], [8, 35], [20, 140]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "mw_edit_save_500",
                "rpm": 35.0,
                "emit": ["mediawiki_app.edit_submit", "sessionstore_api.http_put_500", "mediawiki_app.edit_result_fail"],
                "latency_ms": [[1, 3], [120, 2200], [400, 4500]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "mw_edit_save_ok_post_wipe",
                "rpm": 35.0,
                "emit": ["mediawiki_app.edit_submit", "sessionstore_api.http_put_200", "mediawiki_app.edit_result_ok"],
                "latency_ms": [[1, 3], [12, 60], [140, 420]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "ss_cassandra_read_sample_fail",
                "rpm": 20.0,
                "emit": ["sessionstore_api.cassandra_op_fail"],
                "latency_ms": [[60, 900]],
                "retry": {"max_attempts": 3, "expected_attempts": 2.2, "emit_per_retry": ["sessionstore_api.retrying_cassandra"], "backoff_ms": [[50, 200], [100, 400]]},
                "trace": False,
            },
            {
                "id": "ss_cassandra_write_sample_fail",
                "rpm": 6.0,
                "emit": ["sessionstore_api.cassandra_op_fail"],
                "latency_ms": [[80, 1300]],
                "retry": {"max_attempts": 3, "expected_attempts": 2.2, "emit_per_retry": ["sessionstore_api.retrying_cassandra"], "backoff_ms": [[60, 240], [120, 480]]},
                "trace": False,
            },
            {
                "id": "ss_cassandra_read_sample_ok_post_wipe",
                "rpm": 20.0,
                "emit": ["sessionstore_api.cassandra_op_ok"],
                "latency_ms": [[6, 30]],
                "retry": {"max_attempts": 2, "expected_attempts": 1.05, "emit_per_retry": ["sessionstore_api.retrying_cassandra"], "backoff_ms": [[15, 45]]},
                "trace": False,
            },
            {
                "id": "ss_cassandra_write_sample_ok_post_wipe",
                "rpm": 6.0,
                "emit": ["sessionstore_api.cassandra_op_ok"],
                "latency_ms": [[8, 40]],
                "retry": {"max_attempts": 2, "expected_attempts": 1.05, "emit_per_retry": ["sessionstore_api.retrying_cassandra"], "backoff_ms": [[20, 60]]},
                "trace": False,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "incident_2025_03_31_sessionstore_unavailability",
        "time": {"total_minutes": 60, "phases": {"n": {"start_min": 0, "end_min": 18}, "f": {"start_min": 18, "end_min": 60}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 18,
                        "rate_multipliers": {
                            "mw_page_view_ok_post_wipe": 0.0,
                            "mw_edit_save_ok_post_wipe": 0.0,
                            "ss_cassandra_read_sample_ok_post_wipe": 0.0,
                            "ss_cassandra_write_sample_ok_post_wipe": 0.0,
                            "sessionstore_api.pool_health_ok": 0.0,
                            "cassandra_cluster.disk_usage_recovered": 0.0,
                            "monitoring_alerts.alert_active_sessionstore_5xx": 0.0,
                            "monitoring_alerts.alert_active_edit_failures": 0.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "cassandra_cluster.commitlog_no_space", "count": 3, "hosts": ["cass-a2", "cass-b1", "cass-b2"]},
                            {"ref": "cassandra_cluster.node_marked_down", "count": 3, "hosts": ["cass-a2", "cass-b1", "cass-b2"]},
                        ],
                    },
                    {
                        "order": 2,
                        "at_min": 23,
                        "rate_multipliers": {"monitoring_alerts.alert_active_sessionstore_5xx": 1.0, "monitoring_alerts.alert_active_edit_failures": 1.0},
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "monitoring_alerts.alert_fired_edit_failures", "count": 1, "hosts": ["mon1"]},
                            {"ref": "monitoring_alerts.alert_fired_sessionstore_5xx", "count": 1, "hosts": ["mon1"]},
                            {"ref": "monitoring_alerts.page_sent", "count": 1, "hosts": ["mon1"]},
                        ],
                    },
                    {
                        "order": 3,
                        "at_min": 29,
                        "rate_multipliers": {"sessionstore_api.pool_health": 2.5},
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "oncall_ops.ack_page", "count": 1, "hosts": ["ops1"]},
                            {"ref": "oncall_ops.investigation_note", "count": 1, "hosts": ["ops1"]},
                        ],
                    },
                    {
                        "order": 4,
                        "at_min": 42,
                        "rate_multipliers": {"cassandra_cluster.compaction_backlog": 2.0, "sessionstore_api.pool_health": 3.0},
                        "latency_multipliers": {
                            "ss_cassandra_read_sample_fail": {"p50": 1.3, "p95": 1.6},
                            "ss_cassandra_write_sample_fail": {"p50": 1.3, "p95": 1.6},
                            "mw_edit_save_500": {"p50": 1.2, "p95": 1.5},
                        },
                        "one_shots": [
                            {"ref": "cassandra_cluster.commitlog_no_space", "count": 1, "hosts": ["cass-b3"]},
                            {"ref": "cassandra_cluster.node_marked_down", "count": 1, "hosts": ["cass-b3"]},
                            {"ref": "oncall_ops.investigation_note", "count": 2, "hosts": ["ops1"]},
                        ],
                    },
                    {"order": 5, "at_min": 52, "rate_multipliers": {}, "latency_multipliers": {}, "one_shots": [{"ref": "oncall_ops.decision_log", "count": 1, "hosts": ["ops1"]}]},
                    {
                        "order": 6,
                        "at_min": 56,
                        "rate_multipliers": {
                            "mw_page_view_ok_post_wipe": 1.0,
                            "mw_page_view_sessionstore_5xx": 0.02,
                            "mw_edit_save_ok_post_wipe": 1.0,
                            "mw_edit_save_500": 0.02,
                            "ss_cassandra_read_sample_ok_post_wipe": 1.0,
                            "ss_cassandra_read_sample_fail": 0.05,
                            "ss_cassandra_write_sample_ok_post_wipe": 1.0,
                            "ss_cassandra_write_sample_fail": 0.05,
                            "sessionstore_api.pool_health_ok": 1.0,
                            "sessionstore_api.pool_health": 0.0,
                            "cassandra_cluster.disk_usage_recovered": 1.0,
                            "cassandra_cluster.disk_usage": 0.0,
                            "cassandra_cluster.compaction_backlog": 0.1,
                            "monitoring_alerts.alert_active_sessionstore_5xx": 0.0,
                            "monitoring_alerts.alert_active_edit_failures": 0.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "cassandra_cluster.truncate_sessions", "count": 2, "hosts": ["cass-a1", "cass-b1"]},
                            {"ref": "cassandra_cluster.cassandra_restart", "count": 2, "hosts": ["cass-a1", "cass-b1"]},
                            {"ref": "monitoring_alerts.alert_resolved_sessionstore_5xx", "count": 1, "hosts": ["mon1"]},
                            {"ref": "monitoring_alerts.alert_resolved_edit_failures", "count": 1, "hosts": ["mon1"]},
                        ],
                    },
                ]
            }
        },
    }
}


# -------------------------
# Deterministic helpers
# -------------------------
def _md5_bytes(s: str) -> bytes:
    return hashlib.md5(s.encode("utf-8")).digest()


def h_u01(key: str) -> float:
    b = _md5_bytes(key)
    x = int.from_bytes(b[:8], byteorder="big", signed=False)
    return (x % (10**12)) / float(10**12)


def h_i(key: str) -> int:
    b = _md5_bytes(key)
    return int.from_bytes(b[:8], byteorder="big", signed=False)


def det_hex(n: int, key: str) -> str:
    hx = hashlib.md5(key.encode("utf-8")).hexdigest()
    while len(hx) < n:
        hx += hashlib.md5((hx + key).encode("utf-8")).hexdigest()
    return hx[:n]


def dt_to_iso_ms(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def clamp_int(v: int, lo: int, hi: int) -> int:
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def sample_ms_from_pair(pair: List[float], key: str, mult_p50: float = 1.0, mult_p95: float = 1.0, hard_cap: float | None = None) -> int:
    p50, p95 = float(pair[0]) * mult_p50, float(pair[1]) * mult_p95
    if p50 <= 0:
        p50 = 1.0
    if p95 < p50:
        p95 = p50
    ratio = p95 / p50 if p50 > 0 else 1.0
    if ratio < 1.0001:
        val = p50
    else:
        u = 0.55 + 0.4 * h_u01(key)  # [0.55, 0.95)
        alpha = (u - 0.55) / 0.4  # [0, 1)
        alpha *= 1.2  # mild >p95 tail
        val = p50 * (ratio**alpha)
    soft_cap = p95 * 2.5
    if hard_cap is not None:
        soft_cap = min(soft_cap, float(hard_cap))
    val = min(max(val, 1.0), soft_cap)
    return int(round(val))


def schedule_times(start_dt: datetime, end_dt: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    dur = (end_dt - start_dt).total_seconds()
    if dur <= 0:
        return [start_dt] * count
    spacing = dur / count
    jitter_max = min(0.4, spacing * 0.2)
    times: List[datetime] = []
    for i in range(count):
        base = start_dt + timedelta(seconds=(i + 0.5) * spacing)
        jitter = (h_u01(f"{key}:{i}") - 0.5) * 2.0 * jitter_max
        t = base + timedelta(seconds=jitter)
        if t < start_dt:
            t = start_dt + timedelta(milliseconds=1)
        if t >= end_dt:
            t = end_dt - timedelta(milliseconds=1)
        times.append(t)
    return times


def schedule_oneshot_times(at_dt: datetime, count: int, key: str) -> List[datetime]:
    times: List[datetime] = []
    for i in range(count):
        off = (h_u01(f"{key}:oneshot:{i}") - 0.5) * 4.0
        t = at_dt + timedelta(seconds=off)
        times.append(t)
    return times


def adjust_sum_to_domain(d_ms: List[int], indices: List[int], dom_min: int, dom_max: int) -> None:
    """
    Adjust the final element's delay so that sum(d_ms[i] for i in indices) falls within [dom_min, dom_max].
    This preserves chronology/message coherence for observed timing fields that are later bound to that sum.
    """
    if not indices:
        return
    s = sum(d_ms[i] for i in indices)
    last = indices[-1]
    if s > dom_max:
        excess = s - dom_max
        d_ms[last] = max(1, d_ms[last] - excess)
    elif s < dom_min:
        need = dom_min - s
        d_ms[last] = d_ms[last] + need


# -------------------------
# Indices
# -------------------------
COMP: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}

LOG_TPL: Dict[str, Dict[str, Any]] = {}
for c in SYSTEM["components"]:
    for log_id, tpl in c["logs"].items():
        LOG_TPL[f'{c["id"]}.{log_id}'] = tpl


def infer_dc_from_host(host: str) -> str:
    if host.startswith("cass-a") or host in ("ss1", "ss2"):
        return "eqiad"
    if host.startswith("cass-b") or host in ("ss3", "ss4"):
        return "codfw"
    return "eqiad"


def parse_ref(ref: str) -> Tuple[str, str]:
    comp_id, log_id = ref.split(".", 1)
    return comp_id, log_id


# -------------------------
# Controls (failure phase)
# -------------------------
def build_failure_intervals() -> List[Dict[str, Any]]:
    fstart = SCENARIO["scenario"]["time"]["phases"]["f"]["start_min"]
    fend = SCENARIO["scenario"]["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = [fstart] + sorted({e["at_min"] for e in events if fstart <= e["at_min"] <= fend}) + [fend]
    boundaries = [boundaries[0]] + [m for i, m in enumerate(boundaries[1:], 1) if m != boundaries[i - 1]]

    rate_mult: Dict[str, float] = {}
    latency_mult: Dict[str, Dict[str, float]] = {}

    e_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        e_by_min.setdefault(e["at_min"], []).append(e)

    intervals: List[Dict[str, Any]] = []
    for i in range(len(boundaries) - 1):
        start_m = boundaries[i]
        end_m = boundaries[i + 1]
        for e in e_by_min.get(start_m, []):
            for k, v in e.get("rate_multipliers", {}).items():
                rate_mult[k] = float(v)
            for fk, fv in e.get("latency_multipliers", {}).items():
                latency_mult[fk] = {"p50": float(fv.get("p50", 1.0)), "p95": float(fv.get("p95", 1.0))}
        intervals.append({"start_min": start_m, "end_min": end_m, "rate_mult": dict(rate_mult), "latency_mult": dict(latency_mult)})
    return intervals


FAIL_INTERVALS = build_failure_intervals()


def get_rate_multiplier(rate_mult_map: Dict[str, float], source_key: str) -> float:
    return float(rate_mult_map.get(source_key, 1.0))


def get_latency_multiplier(lat_mult_map: Dict[str, Dict[str, float]], flow_id: str) -> Tuple[float, float]:
    lm = lat_mult_map.get(flow_id)
    if not lm:
        return 1.0, 1.0
    return float(lm.get("p50", 1.0)), float(lm.get("p95", 1.0))


# -------------------------
# Domain rendering
# -------------------------
def domain_pick(dom: Dict[str, Any], key: str) -> Any:
    k = dom.get("k")
    v = dom.get("v")
    if k == "ch":
        opts = list(v)
        if not opts:
            return ""
        return opts[h_i(key) % len(opts)]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi < lo:
            lo, hi = hi, lo
        if lo == hi:
            return lo
        u = h_u01(key)
        return lo + int(math.floor(u * (hi - lo + 1)))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        if hi < lo:
            lo, hi = hi, lo
        if abs(hi - lo) < 1e-12:
            return lo
        u = h_u01(key)
        return lo + u * (hi - lo)
    if k == "hex":
        n = int(v)
        return det_hex(n, key)
    if k == "uuid":
        hx = det_hex(32, key)
        return f"{hx[:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"
    if k == "ip":
        return "127.0.0.1"
    if k == "str":
        return str(v)
    return ""


def render_log(ref: str, state: str, bind: Dict[str, Any]) -> Tuple[str, str, str]:
    tpl = LOG_TPL[ref]
    msg = tpl["msg"]
    vars_def = dict(tpl.get("vars", {}))
    state_vars = tpl.get("state_vars", {}).get(state, {})
    vars_def.update(state_vars)

    ctx: Dict[str, Any] = {}
    for name, dom in vars_def.items():
        ctx[name] = domain_pick(dom, f"{ref}:{state}:{bind.get('_key', '')}:{name}")

    for k, v in bind.items():
        if not k.startswith("_"):
            ctx[k] = v

    rendered = msg.format(**ctx)
    lvl = tpl["lvl"]
    return lvl, rendered, ctx.get("req_id", "")


# -------------------------
# Simulation core
# -------------------------
BASE_TIME = datetime(2025, 3, 31, 2, 40, 0, tzinfo=timezone.utc)

rows: List[Dict[str, Any]] = []
carry: Dict[str, float] = {}


def alloc_count(expected: float, key: str) -> int:
    c = carry.get(key, 0.0)
    x = expected + c
    n = int(math.floor(x + 1e-12))
    carry[key] = x - n
    if n < 0:
        n = 0
        carry[key] = 0.0
    return n


def emit_row(ts: datetime, level: str, message: str, service: str, host: str, trace_id: str = "") -> None:
    rows.append({"timestamp": ts, "level": level, "message": message, "trace_id": trace_id, "service": service, "host": host})


def choose_host(component_id: str, chain_key: str) -> str:
    hosts = COMP[component_id].get("hosts", [])
    if not hosts:
        return ""
    return hosts[h_i(f"{component_id}:{chain_key}") % len(hosts)]


def background_emit_interval(state: str, start_min: int, end_min: int, rate_mult_map: Dict[str, float] | None) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    dur_min = (end_dt - start_dt).total_seconds() / 60.0

    for comp_id, comp in COMP.items():
        beh = comp.get("beh", {}).get(state, {}).get("emit", [])
        for src in beh:
            log_id = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            key_base = f"bg:{state}:{start_min}-{end_min}:{comp_id}.{log_id}"
            mult_key = f"{comp_id}.{log_id}"
            mult = 1.0
            if rate_mult_map is not None:
                mult = get_rate_multiplier(rate_mult_map, mult_key)
            eff_per_min = per_min * mult
            if eff_per_min <= 0:
                continue

            if scope == "global":
                expected = eff_per_min * dur_min
                cnt = alloc_count(expected, key_base)
                times = schedule_times(start_dt, end_dt, cnt, key_base)
                for i, t in enumerate(times):
                    host = choose_host(comp_id, f"{key_base}:{i}")
                    bind: Dict[str, Any] = {"_key": f"{key_base}:{i}"}
                    if comp_id == "monitoring_alerts" and log_id.startswith("alert_active_"):
                        since = int(math.floor(((t - BASE_TIME).total_seconds() / 60.0) - 23.0))
                        bind["since_min"] = clamp_int(since, 0, 60)
                    lvl, msg, _ = render_log(f"{comp_id}.{log_id}", state, bind)
                    emit_row(t, lvl, msg, comp.get("svc", "") or "", host)
            else:
                for host in comp.get("hosts", []):
                    expected = eff_per_min * dur_min
                    cnt = alloc_count(expected, f"{key_base}:{host}")
                    times = schedule_times(start_dt, end_dt, cnt, f"{key_base}:{host}")
                    for i, t in enumerate(times):
                        bind = {"_key": f"{key_base}:{host}:{i}"}
                        if comp_id == "cassandra_cluster" and log_id in (
                            "disk_usage",
                            "disk_usage_recovered",
                            "compaction_backlog",
                            "commitlog_no_space",
                            "node_marked_down",
                            "cassandra_restart",
                        ):
                            bind["node"] = host
                        if comp_id == "sessionstore_api" and log_id in ("pool_health", "pool_health_ok"):
                            bind["dc"] = infer_dc_from_host(host)
                        if comp_id == "monitoring_alerts" and log_id.startswith("alert_active_"):
                            since = int(math.floor(((t - BASE_TIME).total_seconds() / 60.0) - 23.0))
                            bind["since_min"] = clamp_int(since, 0, 60)
                        lvl, msg, _ = render_log(f"{comp_id}.{log_id}", state, bind)
                        emit_row(t, lvl, msg, comp.get("svc", "") or "", host)


def attempt_count_for_flow(flow: Dict[str, Any], inst_key: str) -> int:
    r = flow["retry"]
    m = int(r["max_attempts"])
    e = float(r["expected_attempts"])
    if m <= 1:
        return 1
    base = int(math.floor(e + 1e-12))
    base = max(1, min(base, m))
    frac = max(0.0, min(1.0, e - base))
    if base >= m:
        return m
    u = h_u01(f"{inst_key}:attempts")
    return base + 1 if u < frac else base


def host_map_for_flow(flow: Dict[str, Any], chain_key: str) -> Dict[str, str]:
    comps = set(parse_ref(r)[0] for r in (flow.get("emit", []) + flow.get("retry", {}).get("emit_per_retry", [])))
    return {cid: choose_host(cid, chain_key) for cid in comps}


def _is_single_ok_cassandra_with_retries(flow: Dict[str, Any]) -> bool:
    r = flow.get("retry", {})
    if int(r.get("max_attempts", 1)) <= 1:
        return False
    emit_refs = flow.get("emit", [])
    if len(emit_refs) != 1:
        return False
    return emit_refs[0].endswith(".cassandra_op_ok")


def simulate_flow_instance(state: str, flow: Dict[str, Any], start_ts: datetime, start_min: int, latency_mult: Tuple[float, float], inst_index: int) -> None:
    flow_id = flow["id"]
    inst_key = f"flow:{state}:{flow_id}:{start_min}:{inst_index}:{dt_to_iso_ms(start_ts)}"
    hm = host_map_for_flow(flow, inst_key)

    req_id = det_hex(16, f"{inst_key}:req_id")

    mw_user = None
    mw_page = None
    if any(r.startswith("mediawiki_app.") for r in flow["emit"]):
        mw_user = domain_pick(LOG_TPL["mediawiki_app.page_request"]["vars"]["user"], f"{inst_key}:mw_user")
        mw_page = domain_pick(LOG_TPL["mediawiki_app.page_request"]["vars"]["page"], f"{inst_key}:mw_page")

    session_state = None
    if flow_id in ("mw_page_view_sessionstore_5xx", "mw_page_view_ok_post_wipe"):
        if mw_user == "logged_in":
            thresh = 0.7 if flow_id == "mw_page_view_sessionstore_5xx" else 0.85
        elif mw_user == "bot":
            thresh = 0.25 if flow_id == "mw_page_view_sessionstore_5xx" else 0.35
        else:
            thresh = 0.10 if flow_id == "mw_page_view_sessionstore_5xx" else 0.15
        session_state = "missing" if h_u01(f"{inst_key}:sess") < thresh else "present"
    elif flow_id == "mw_page_view_ok":
        session_state = "present"

    ss_host = hm.get("sessionstore_api", "")
    ss_dc = infer_dc_from_host(ss_host) if ss_host else domain_pick(LOG_TPL["sessionstore_api.http_get_200"]["vars"]["dc"], f"{inst_key}:dc")

    cass_op = None
    if flow_id in ("ss_cassandra_read_sample_ok", "ss_cassandra_read_sample_fail", "ss_cassandra_read_sample_ok_post_wipe"):
        cass_op = "read"
    elif flow_id in ("ss_cassandra_write_sample_ok", "ss_cassandra_write_sample_fail", "ss_cassandra_write_sample_ok_post_wipe"):
        cass_op = "write"

    cass_err = None
    if any(ref.endswith(".cassandra_op_fail") for ref in flow["emit"]):
        cass_err = domain_pick(LOG_TPL["sessionstore_api.cassandra_op_fail"]["vars"]["err"], f"{inst_key}:cass_err")

    edit_fail_reason = None
    edit_fail_up = None
    if flow_id == "mw_edit_save_500":
        u = h_u01(f"{inst_key}:edit_fail_reason")
        if u < 0.85:
            edit_fail_reason = "sessionstore_5xx"
        elif u < 0.95:
            edit_fail_reason = "session_lost"
        else:
            edit_fail_reason = "token_mismatch"
        # Coherence fix: this flow always emits SessionStore status=500, so upstream_status must be 500.
        edit_fail_up = 500

    attempts = attempt_count_for_flow(flow, inst_key)

    # For sampled Cassandra "OK" flows that can retry, we emit the terminal OK only once on the final attempt.
    single_ok_emit_on_last = _is_single_ok_cassandra_with_retries(flow)

    # Ensure retry logs have a coherent err value when retries occur, even if this flow does not emit cassandra_op_fail.
    if single_ok_emit_on_last and attempts > 1 and cass_err is None:
        cass_err = domain_pick(LOG_TPL["sessionstore_api.cassandra_op_fail"]["vars"]["err"], f"{inst_key}:cass_err_for_retry")

    retry_tpl = LOG_TPL.get("sessionstore_api.retrying_cassandra")
    retry_backoff_min = 1
    retry_backoff_max = None
    if retry_tpl and "backoff_ms" in retry_tpl.get("vars", {}):
        # Enforce the retry log's explicit backoff domain for S5 correctness.
        rb = retry_tpl["vars"]["backoff_ms"]["v"]
        retry_backoff_min = int(rb[0])
        retry_backoff_max = int(rb[1])

    prev_attempt_end = start_ts

    for attempt in range(1, attempts + 1):
        if attempt == 1:
            attempt_start = start_ts
        else:
            bo_pair = flow["retry"]["backoff_ms"][attempt - 2]
            bo_ms = sample_ms_from_pair(bo_pair, f"{inst_key}:backoff:{attempt}", 1.0, 1.0, hard_cap=retry_backoff_max)
            bo_ms = clamp_int(int(bo_ms), int(retry_backoff_min), int(retry_backoff_max if retry_backoff_max is not None else bo_ms))
            attempt_start = prev_attempt_end + timedelta(milliseconds=bo_ms)

            for retry_ref in flow["retry"].get("emit_per_retry", []):
                r_comp, _ = parse_ref(retry_ref)
                bind: Dict[str, Any] = {
                    "_key": f"{inst_key}:retrylog:{attempt}:{retry_ref}",
                    "req_id": req_id,
                    "attempt": attempt,
                    "backoff_ms": bo_ms,
                }
                if cass_op is not None:
                    bind["op"] = cass_op
                if cass_err is not None:
                    bind["err"] = cass_err
                lvl, msg, _ = render_log(retry_ref, state, bind)
                emit_row(attempt_start, lvl, msg, COMP[r_comp].get("svc", "") or "", hm.get(r_comp, ""))

        # Choose per-log delays for this attempt.
        d_ms: List[int] = []
        p50m, p95m = latency_mult
        for li, pair in enumerate(flow["latency_ms"]):
            d = sample_ms_from_pair(pair, f"{inst_key}:attempt:{attempt}:lat:{li}", mult_p50=p50m, mult_p95=p95m)
            d_ms.append(int(d))

        # Keep MediaWiki reported durations coherent with the emitted timestamps by adjusting the final leg.
        if len(flow.get("emit", [])) == 3 and flow["emit"][0].startswith("mediawiki_app.") and flow["emit"][-1].startswith("mediawiki_app."):
            last_ref = flow["emit"][-1]
            _, last_log = parse_ref(last_ref)
            if last_log == "page_response_200":
                dom = LOG_TPL["mediawiki_app.page_response_200"]["vars"]["dur_ms"]["v"]
                adjust_sum_to_domain(d_ms, [1, 2], int(dom[0]), int(dom[1]))
            elif last_log == "edit_result_ok":
                dom = LOG_TPL["mediawiki_app.edit_result_ok"]["vars"]["dur_ms"]["v"]
                adjust_sum_to_domain(d_ms, [1, 2], int(dom[0]), int(dom[1]))
            elif last_log == "edit_result_fail":
                dom = LOG_TPL["mediawiki_app.edit_result_fail"]["vars"]["dur_ms"]["v"]
                adjust_sum_to_domain(d_ms, [1, 2], int(dom[0]), int(dom[1]))

        # Existing: ensure edit OK has at least its minimum duration (redundant after generic adjust, but harmless).
        if flow_id == "mw_edit_save_ok":
            total = d_ms[1] + d_ms[2]
            if total < 150:
                d_ms[2] += (150 - total)

        emit_refs = flow["emit"]
        attempt_end = attempt_start + timedelta(milliseconds=int(sum(d_ms)))

        # For "single OK with retries" flows, only the final attempt emits the OK op log.
        if single_ok_emit_on_last and attempt != attempts:
            prev_attempt_end = attempt_end
            continue

        # Emit the attempt's log chain.
        t = attempt_start
        emit_ts: List[datetime] = []
        for i, _ref in enumerate(emit_refs):
            t = t + timedelta(milliseconds=d_ms[i])
            emit_ts.append(t)

        mw_total_ms = None
        if len(emit_refs) == 3 and emit_refs[0].startswith("mediawiki_app.") and emit_refs[-1].startswith("mediawiki_app."):
            mw_total_ms = int(d_ms[1] + d_ms[2])

        ss_http_dur_ms = None
        if len(emit_refs) == 3 and emit_refs[1].startswith("sessionstore_api.http_"):
            ss_http_dur_ms = int(d_ms[1])

        cass_wait_or_dur = None
        if len(emit_refs) == 1 and emit_refs[0].startswith("sessionstore_api.cassandra_op_"):
            cass_wait_or_dur = int(d_ms[0])

        for i, ref in enumerate(emit_refs):
            comp_id, log_id = parse_ref(ref)
            bind = {"_key": f"{inst_key}:attempt:{attempt}:emit:{i}:{ref}", "req_id": req_id}

            if comp_id == "mediawiki_app":
                if log_id in ("page_request", "edit_submit"):
                    if mw_user is not None:
                        bind["user"] = mw_user
                    if mw_page is not None:
                        bind["page"] = mw_page
                    if log_id == "page_request" and session_state is not None:
                        bind["session_state"] = session_state
                if log_id == "page_response_200" and mw_total_ms is not None:
                    bind["dur_ms"] = clamp_int(mw_total_ms, 10, 6000)
                if log_id == "edit_result_ok" and mw_total_ms is not None:
                    bind["dur_ms"] = clamp_int(mw_total_ms, 150, 5000)
                if log_id == "edit_result_fail" and mw_total_ms is not None:
                    bind["dur_ms"] = clamp_int(mw_total_ms, 200, 9000)
                    if edit_fail_reason is not None:
                        bind["reason"] = edit_fail_reason
                    if edit_fail_up is not None:
                        bind["upstream_status"] = edit_fail_up

            if comp_id == "sessionstore_api":
                if log_id in ("http_get_200", "http_get_500", "http_put_200", "http_put_500"):
                    bind["dc"] = ss_dc
                    if ss_http_dur_ms is not None:
                        if log_id == "http_get_200":
                            bind["dur_ms"] = clamp_int(ss_http_dur_ms, 2, 3000)
                        elif log_id == "http_get_500":
                            bind["dur_ms"] = clamp_int(ss_http_dur_ms, 10, 8000)
                        elif log_id == "http_put_200":
                            bind["dur_ms"] = clamp_int(ss_http_dur_ms, 3, 4000)
                            bind["bytes_in"] = domain_pick(LOG_TPL[ref]["vars"]["bytes_in"], f"{inst_key}:bytes_in")
                        elif log_id == "http_put_500":
                            bind["dur_ms"] = clamp_int(ss_http_dur_ms, 20, 12000)
                            bind["bytes_in"] = domain_pick(LOG_TPL[ref]["vars"]["bytes_in"], f"{inst_key}:bytes_in")
                if log_id in ("cassandra_op_ok", "cassandra_op_fail"):
                    bind["dc"] = ss_dc
                    if cass_op is not None:
                        bind["op"] = cass_op
                    if log_id == "cassandra_op_ok" and cass_wait_or_dur is not None:
                        bind["dur_ms"] = clamp_int(cass_wait_or_dur, 1, 2000)
                    if log_id == "cassandra_op_fail" and cass_wait_or_dur is not None:
                        if cass_err is not None:
                            bind["err"] = cass_err
                        bind["waited_ms"] = clamp_int(cass_wait_or_dur, 50, 6000)

            lvl, msg, _ = render_log(ref, state, bind)
            emit_row(emit_ts[i], lvl, msg, COMP[comp_id].get("svc", "") or "", hm.get(comp_id, ""))

        prev_attempt_end = emit_ts[-1] if emit_ts else attempt_end


def flows_emit_interval(state: str, start_min: int, end_min: int, rate_mult_map: Dict[str, float] | None, lat_mult_map: Dict[str, Dict[str, float]] | None) -> None:
    start_dt = BASE_TIME + timedelta(minutes=start_min)
    end_dt = BASE_TIME + timedelta(minutes=end_min)
    dur_min = (end_dt - start_dt).total_seconds() / 60.0

    flow_list = SYSTEM["flows"][state]
    for flow in flow_list:
        fid = flow["id"]
        rpm = float(flow["rpm"])
        mult = 1.0
        if rate_mult_map is not None:
            mult = get_rate_multiplier(rate_mult_map, fid)
        eff_rpm = rpm * mult
        if eff_rpm <= 0:
            continue

        expected = eff_rpm * dur_min
        cnt = alloc_count(expected, f"flowcnt:{state}:{fid}")
        starts = schedule_times(start_dt, end_dt, cnt, f"flowstart:{state}:{start_min}-{end_min}:{fid}")
        for i, st in enumerate(starts):
            if state == "f":
                p50m, p95m = get_latency_multiplier(lat_mult_map or {}, fid)
            else:
                p50m, p95m = (1.0, 1.0)
            simulate_flow_instance(state, flow, st, start_min, (p50m, p95m), i)


def emit_one_shots() -> None:
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for e in events:
        at_min = int(e["at_min"])
        at_dt = BASE_TIME + timedelta(minutes=at_min)
        for os in e.get("one_shots", []):
            ref = os["ref"]
            cnt = int(os["count"])
            allowed_hosts = list(os.get("hosts", []))
            comp_id, log_id = parse_ref(ref)
            comp = COMP[comp_id]
            svc = comp.get("svc", "") or ""
            times = schedule_oneshot_times(at_dt, cnt, f"oneshot:{at_min}:{ref}")
            for i in range(cnt):
                host = allowed_hosts[i % len(allowed_hosts)] if allowed_hosts else choose_host(comp_id, f"oneshot:{at_min}:{ref}:{i}")
                bind: Dict[str, Any] = {"_key": f"oneshot:{at_min}:{ref}:{i}"}
                if comp_id == "cassandra_cluster" and log_id in ("commitlog_no_space", "node_marked_down", "cassandra_restart"):
                    bind["node"] = host
                if comp_id == "cassandra_cluster" and log_id == "truncate_sessions":
                    bind["dc"] = infer_dc_from_host(host)
                lvl, msg, _ = render_log(ref, "f", bind)
                emit_row(times[i], lvl, msg, svc, host)


# -------------------------
# Run simulation
# -------------------------
n_start = SCENARIO["scenario"]["time"]["phases"]["n"]["start_min"]
n_end = SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"]
background_emit_interval("n", n_start, n_end, rate_mult_map=None)
flows_emit_interval("n", n_start, n_end, rate_mult_map=None, lat_mult_map=None)

for seg in FAIL_INTERVALS:
    s = int(seg["start_min"])
    e = int(seg["end_min"])
    background_emit_interval("f", s, e, rate_mult_map=seg["rate_mult"])
    flows_emit_interval("f", s, e, rate_mult_map=seg["rate_mult"], lat_mult_map=seg["latency_mult"])

emit_one_shots()

# -------------------------
# Output logs.csv
# -------------------------
df = pd.DataFrame(rows)
df.sort_values("timestamp", inplace=True, kind="mergesort")
df["timestamp"] = df["timestamp"].apply(dt_to_iso_ms)
df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
df.to_csv("logs.csv", index=False)
