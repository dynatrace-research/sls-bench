import hashlib
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# Fixed seeds for reproducibility (the simulator uses deterministic hashing + seeded RNGs)
random.seed(0)
np.random.seed(0)

SYSTEM: Dict[str, Any] = {
    "sys": {"id": "buildkite_dashboard_2016"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False},
    "components": {
        "dashboard_elb": {
            "svc": "aws-elb",
            "hosts": ["elb-dashboard"],
            "logs": {
                "req_ok": {
                    "lvl": "INFO",
                    "msg": "ELB request {method} {url_path} -> {status} target={target} latency_ms={lat_ms} req={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "url_path": {"k": "str", "v": "dashboard path"},
                        "status": {"k": "ch", "v": [200, 302]},
                        "target": {"k": "ch", "v": ["dash-1", "dash-2", "dash-3"]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                    "state_vars": {
                        "n": {"lat_ms": {"k": "i", "v": [10, 250]}},
                        "f": {"lat_ms": {"k": "i", "v": [200, 5000]}},
                    },
                },
                "req_no_healthy": {
                    "lvl": "WARN",
                    "msg": "ELB request {method} {url_path} -> 503 no_healthy_backends latency_ms={lat_ms} req={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "url_path": {"k": "str", "v": "dashboard path"},
                        "lat_ms": {"k": "i", "v": [1, 120]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "hc_access": {
                    "lvl": "INFO",
                    "msg": "ELB healthcheck GET /healthcheck target={target} req={req_id}",
                    "vars": {
                        "target": {"k": "ch", "v": ["dash-1", "dash-2", "dash-3"]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "hc_result_ok": {
                    "lvl": "INFO",
                    "msg": "ELB healthcheck result target={target} status=200 latency_ms={lat_ms} req={req_id}",
                    "vars": {
                        "target": {"k": "ch", "v": ["dash-1", "dash-2", "dash-3"]},
                        "lat_ms": {"k": "i", "v": [1, 800]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "hc_result_500": {
                    "lvl": "WARN",
                    "msg": "ELB healthcheck result target={target} status=500 latency_ms={lat_ms} req={req_id}",
                    "vars": {
                        "target": {"k": "ch", "v": ["dash-1", "dash-2", "dash-3"]},
                        "lat_ms": {"k": "i", "v": [50, 5000]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "dashboard_app": {
            "svc": "buildkite-web",
            "hosts": ["dash-1", "dash-2", "dash-3"],
            "logs": {
                "req_started": {
                    "lvl": "INFO",
                    "msg": "Started {method} {url_path} controller={controller} action={action} req={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "url_path": {"k": "str", "v": "dashboard path"},
                        "controller": {"k": "ch", "v": ["sessions", "builds", "docs"]},
                        "action": {"k": "ch", "v": ["new", "show", "index"]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                },
                "req_completed_ok": {
                    "lvl": "INFO",
                    "msg": "Completed {status} in {dur_ms}ms req={req_id}",
                    "vars": {"status": {"k": "ch", "v": [200, 302]}, "req_id": {"k": "hex", "v": 16}},
                    "state_vars": {"n": {"dur_ms": {"k": "i", "v": [30, 800]}}, "f": {"dur_ms": {"k": "i", "v": [500, 12000]}}},
                },
                "healthcheck_ok": {
                    "lvl": "INFO",
                    "msg": "Healthcheck OK dur_ms={dur_ms} req={req_id}",
                    "vars": {"req_id": {"k": "hex", "v": 16}},
                    "state_vars": {"n": {"dur_ms": {"k": "i", "v": [5, 40]}}, "f": {"dur_ms": {"k": "i", "v": [20, 3000]}}},
                },
                "healthcheck_db_timeout": {
                    "lvl": "ERROR",
                    "msg": "Healthcheck failed: database ping timeout waited_ms={waited_ms} shard=two req={req_id}",
                    "vars": {"waited_ms": {"k": "i", "v": [200, 5000]}, "req_id": {"k": "hex", "v": 16}},
                },
                "healthcheck_old_shard_error": {
                    "lvl": "ERROR",
                    "msg": "Healthcheck failed: database shard missing shard=one err=\"{err}\" req={req_id}",
                    "vars": {"err": {"k": "ch", "v": ["relation does not exist", "connection refused"]}, "req_id": {"k": "hex", "v": 16}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "pgbouncer_central": {
            "svc": "pgbouncer",
            "hosts": ["pgb-1"],
            "logs": {
                "pool_stats": {
                    "lvl": "INFO",
                    "msg": "pgbouncer stats db=buildkite active={active} waiting={waiting} max_client_conn={max_conn} avg_wait_ms={avg_wait_ms}",
                    "vars": {"active": {"k": "i", "v": [50, 600]}, "max_conn": {"k": "i", "v": [800, 1200]}},
                    "state_vars": {"n": {"waiting": {"k": "i", "v": [0, 5]}, "avg_wait_ms": {"k": "i", "v": [0, 10]}}, "f": {"waiting": {"k": "i", "v": [20, 300]}, "avg_wait_ms": {"k": "i", "v": [50, 2000]}}},
                },
                "pool_stats_recovered": {
                    "lvl": "INFO",
                    "msg": "pgbouncer stats db=buildkite active={active} waiting={waiting} max_client_conn={max_conn} avg_wait_ms={avg_wait_ms} profile=recovered",
                    "vars": {"active": {"k": "i", "v": [40, 450]}, "waiting": {"k": "i", "v": [0, 20]}, "max_conn": {"k": "i", "v": [800, 1200]}, "avg_wait_ms": {"k": "i", "v": [0, 80]}},
                },
                "server_connect_failed": {
                    "lvl": "WARN",
                    "msg": "pgbouncer server connect failed db=buildkite err=\"{err}\"",
                    "vars": {"err": {"k": "ch", "v": ["timeout", "too many connections"]}},
                },
                "config_change": {
                    "lvl": "INFO",
                    "msg": "pgbouncer config change pooling_mode={mode} reason=\"{reason}\"",
                    "vars": {"mode": {"k": "ch", "v": ["per_host"]}, "reason": {"k": "ch", "v": ["rollback_from_central"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "pool_stats", "per_min": 1.0, "scope": "per_host"}]},
                "f": {
                    "emit": [
                        {"id": "pool_stats", "per_min": 1.0, "scope": "per_host"},
                        {"id": "pool_stats_recovered", "per_min": 1.0, "scope": "per_host"},
                        {"id": "server_connect_failed", "per_min": 0.2, "scope": "per_host"},
                    ]
                },
            },
        },
        "postgres_rds": {
            "svc": "aws-rds-postgres",
            "hosts": ["rds-primary"],
            "logs": {
                "rds_metrics": {
                    "lvl": "INFO",
                    "msg": "rds metrics cpu_pct={cpu_pct} conn_used={conn_used} qps={qps} profile=overloaded",
                    "vars": {"qps": {"k": "i", "v": [500, 4000]}},
                    "state_vars": {"n": {"cpu_pct": {"k": "i", "v": [10, 45]}, "conn_used": {"k": "i", "v": [80, 250]}}, "f": {"cpu_pct": {"k": "i", "v": [80, 100]}, "conn_used": {"k": "i", "v": [250, 800]}}},
                },
                "rds_metrics_recovered": {
                    "lvl": "INFO",
                    "msg": "rds metrics cpu_pct={cpu_pct} conn_used={conn_used} qps={qps} profile=recovered",
                    "vars": {"qps": {"k": "i", "v": [800, 4500]}},
                    "state_vars": {"n": {"cpu_pct": {"k": "i", "v": [10, 45]}, "conn_used": {"k": "i", "v": [80, 250]}}, "f": {"cpu_pct": {"k": "i", "v": [25, 70]}, "conn_used": {"k": "i", "v": [120, 450]}}},
                },
                "connection_timeout": {
                    "lvl": "WARN",
                    "msg": "postgres connection timeout from={client} timeout_ms={timeout_ms}",
                    "vars": {"client": {"k": "ch", "v": ["pgb-1", "dash-1", "agent-1"]}, "timeout_ms": {"k": "i", "v": [200, 5000]}},
                },
                "slow_query": {
                    "lvl": "WARN",
                    "msg": "slow query tag={tag} dur_ms={dur_ms}",
                    "vars": {"tag": {"k": "ch", "v": ["healthcheck_select1", "dashboard_login", "build_log_fetch"]}, "dur_ms": {"k": "i", "v": [500, 8000]}},
                },
                "modify_instance": {
                    "lvl": "INFO",
                    "msg": "rds modify instance_id=buildkite from={from} to={to} apply_immediately=true",
                    "vars": {"from": {"k": "ch", "v": ["r3.2xlarge"]}, "to": {"k": "ch", "v": ["m4.10xlarge"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "rds_metrics", "per_min": 1.0, "scope": "global"}, {"id": "slow_query", "per_min": 0.1, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "rds_metrics", "per_min": 1.0, "scope": "global"},
                        {"id": "rds_metrics_recovered", "per_min": 1.0, "scope": "global"},
                        {"id": "connection_timeout", "per_min": 1.2, "scope": "global"},
                        {"id": "slow_query", "per_min": 0.6, "scope": "global"},
                    ]
                },
            },
        },
        "agent_api": {
            "svc": "buildkite-agent-api",
            "hosts": ["agent-1", "agent-2"],
            "logs": {
                "agent_req": {
                    "lvl": "INFO",
                    "msg": "agent-api {method} {url_path} -> {status} dur_ms={dur_ms} req={req_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST"]},
                        "url_path": {"k": "ch", "v": ["/agent/ping", "/agent/jobs/next"]},
                        "status": {"k": "ch", "v": [200, 204, 500]},
                        "req_id": {"k": "hex", "v": 16},
                    },
                    "state_vars": {"n": {"dur_ms": {"k": "i", "v": [20, 300]}}, "f": {"dur_ms": {"k": "i", "v": [30, 800]}}},
                },
                "agent_retry": {
                    "lvl": "WARN",
                    "msg": "agent-api retry attempt={attempt} reason=\"{reason}\" req={req_id}",
                    "vars": {"attempt": {"k": "i", "v": [2, 3]}, "reason": {"k": "ch", "v": ["upstream timeout", "db busy"]}, "req_id": {"k": "hex", "v": 16}},
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        "autoscaling": {
            "svc": "aws-asg",
            "hosts": ["asg-dashboard"],
            "logs": {
                "instance_launch": {
                    "lvl": "INFO",
                    "msg": "asg launch instance={instance_id} ami={ami} reason=\"{reason}\"",
                    "vars": {"instance_id": {"k": "str", "v": "i-<hex8>"}, "ami": {"k": "str", "v": "ami-<hex8>"}, "reason": {"k": "ch", "v": ["replace_unhealthy", "scale_out"]}},
                },
                "instance_terminate": {
                    "lvl": "WARN",
                    "msg": "asg terminate instance={instance_id} reason=\"{reason}\"",
                    "vars": {"instance_id": {"k": "str", "v": "i-<hex8>"}, "reason": {"k": "ch", "v": ["failed_elb_healthcheck", "user_initiated"]}},
                },
                "userdata_fetch_rev_failed": {
                    "lvl": "ERROR",
                    "msg": "userdata fetch deploy revision failed url={url} err=\"{err}\"",
                    "vars": {"url": {"k": "ch", "v": ["https://buildkite.com/_secret/version"]}, "err": {"k": "ch", "v": ["connection refused", "timeout", "503 Service Unavailable"]}},
                },
                "desired_capacity_set": {
                    "lvl": "INFO",
                    "msg": "asg desired capacity set to {desired} reason=\"{reason}\"",
                    "vars": {"desired": {"k": "i", "v": [1, 10]}, "reason": {"k": "ch", "v": ["mitigation_lock", "scale_policy"]}},
                },
                "register_instance_to_elb": {
                    "lvl": "INFO",
                    "msg": "registered instance={instance_id} to elb=dashboard",
                    "vars": {"instance_id": {"k": "str", "v": "i-<hex8>"}},
                },
                "launch_failed_rate_limit": {
                    "lvl": "ERROR",
                    "msg": "ec2 api error api=RunInstances code=RequestLimitExceeded retry_after_s={retry_s}",
                    "vars": {"retry_s": {"k": "i", "v": [30, 300]}},
                },
                "ami_rollout_started": {
                    "lvl": "INFO",
                    "msg": "ami rollout started ami={ami} revision_source={rev_src} reason=\"{reason}\"",
                    "vars": {"ami": {"k": "str", "v": "ami-<hex8>"}, "rev_src": {"k": "ch", "v": ["s3_file"]}, "reason": {"k": "ch", "v": ["fix_bootstrap_revision_source"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "instance_launch", "per_min": 0.02, "scope": "global"}, {"id": "instance_terminate", "per_min": 0.02, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "instance_launch", "per_min": 0.4, "scope": "global"},
                        {"id": "instance_terminate", "per_min": 0.4, "scope": "global"},
                        {"id": "userdata_fetch_rev_failed", "per_min": 0.2, "scope": "global"},
                        {"id": "launch_failed_rate_limit", "per_min": 0.05, "scope": "global"},
                    ]
                },
            },
        },
        "monitoring": {
            "svc": "monitoring",
            "hosts": ["monitor-1"],
            "logs": {
                "probe_result": {
                    "lvl": "INFO",
                    "msg": "probe {probe} status={status} rtt_ms={rtt_ms}",
                    "vars": {"probe": {"k": "ch", "v": ["dashboard_login", "docs_page"]}, "rtt_ms": {"k": "i", "v": [20, 8000]}},
                    "state_vars": {"n": {"status": {"k": "ch", "v": ["ok"]}}, "f": {"status": {"k": "ch", "v": ["critical", "timeout"]}}},
                },
                "pagerduty_incident": {
                    "lvl": "ERROR",
                    "msg": "pagerduty incident opened service=dashboard severity={sev} incident_id={inc_id}",
                    "vars": {"sev": {"k": "ch", "v": ["high", "critical"]}, "inc_id": {"k": "hex", "v": 8}},
                },
                "pagerduty_escalation": {
                    "lvl": "WARN",
                    "msg": "pagerduty escalation step={step} result={result} incident_id={inc_id}",
                    "vars": {"step": {"k": "i", "v": [1, 3]}, "result": {"k": "ch", "v": ["no_ack", "notified_secondary"]}, "inc_id": {"k": "hex", "v": 8}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "probe_result", "per_min": 2.0, "scope": "global"}, {"id": "pagerduty_escalation", "per_min": 0.0, "scope": "global"}]},
                "f": {"emit": [{"id": "probe_result", "per_min": 2.0, "scope": "global"}, {"id": "pagerduty_escalation", "per_min": 0.2, "scope": "global"}]},
            },
        },
    },
    "flows": {
        "n": {
            "req": [
                {
                    "id": "dashboard_login",
                    "rpm": 120.0,
                    "emit": ["dashboard_app.req_started", "dashboard_app.req_completed_ok", "dashboard_elb.req_ok"],
                    "latency_ms": [[1, 10], [30, 800], [1, 20]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "dashboard_view_build_log",
                    "rpm": 80.0,
                    "emit": ["dashboard_app.req_started", "dashboard_app.req_completed_ok", "dashboard_elb.req_ok"],
                    "latency_ms": [[1, 10], [80, 1200], [1, 25]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "dashboard_elb_healthcheck",
                    "rpm": 12.0,
                    "emit": ["dashboard_elb.hc_access", "dashboard_app.healthcheck_ok", "dashboard_elb.hc_result_ok"],
                    "latency_ms": [[1, 3], [1, 10], [1, 30]],
                    "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                    "trace": False,
                },
                {
                    "id": "agent_job_poll",
                    "rpm": 300.0,
                    "emit": ["agent_api.agent_req"],
                    "latency_ms": [[20, 300]],
                    "retry": {"max_attempts": 3, "expected_attempts": 1.05, "emit_per_retry": ["agent_api.agent_retry"], "backoff_ms": [[50, 200], [200, 800]]},
                    "trace": False,
                },
            ]
        },
        "f": {
            "req": [
                {"id": "dashboard_login_503", "rpm": 150.0, "emit": ["dashboard_elb.req_no_healthy"], "latency_ms": [[1, 80]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "dashboard_view_build_log_503", "rpm": 100.0, "emit": ["dashboard_elb.req_no_healthy"], "latency_ms": [[1, 120]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "dashboard_login_slow", "rpm": 20.0, "emit": ["dashboard_app.req_started", "dashboard_app.req_completed_ok", "dashboard_elb.req_ok"], "latency_ms": [[5, 80], [500, 12000], [5, 60]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "dashboard_elb_healthcheck_fail_db", "rpm": 12.0, "emit": ["dashboard_elb.hc_access", "dashboard_app.healthcheck_db_timeout", "dashboard_elb.hc_result_500"], "latency_ms": [[1, 3], [200, 5000], [10, 100]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "dashboard_elb_healthcheck_fail_old_shard", "rpm": 12.0, "emit": ["dashboard_elb.hc_access", "dashboard_app.healthcheck_old_shard_error", "dashboard_elb.hc_result_500"], "latency_ms": [[1, 3], [5, 80], [5, 80]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "dashboard_elb_healthcheck_ok", "rpm": 12.0, "emit": ["dashboard_elb.hc_access", "dashboard_app.healthcheck_ok", "dashboard_elb.hc_result_ok"], "latency_ms": [[1, 3], [20, 3000], [1, 120]], "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []}, "trace": False},
                {"id": "agent_job_poll", "rpm": 300.0, "emit": ["agent_api.agent_req"], "latency_ms": [[30, 800]], "retry": {"max_attempts": 3, "expected_attempts": 1.3, "emit_per_retry": ["agent_api.agent_retry"], "backoff_ms": [[100, 400], [400, 1500]]}, "trace": False},
            ]
        },
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "buildkite_outage_aug_2016_compressed",
        "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 25,
                        "rate_multipliers": {
                            "dashboard_login_slow": 0.0,
                            "dashboard_elb_healthcheck_fail_old_shard": 0.0,
                            "dashboard_elb_healthcheck_ok": 0.0,
                            "autoscaling.userdata_fetch_rev_failed": 0.0,
                            "autoscaling.launch_failed_rate_limit": 0.0,
                            "postgres_rds.rds_metrics_recovered": 0.0,
                            "pgbouncer_central.pool_stats_recovered": 0.0,
                        },
                        "latency_multipliers": {"dashboard_elb_healthcheck_fail_db": {"p50": 1.2, "p95": 1.2}, "agent_job_poll": {"p50": 1.1, "p95": 1.2}},
                        "one_shots": [{"ref": "monitoring.pagerduty_incident", "count": 1, "hosts": ["monitor-1"]}],
                    },
                    {
                        "order": 2,
                        "at_min": 32,
                        "rate_multipliers": {
                            "dashboard_elb_healthcheck_fail_db": 0.6,
                            "dashboard_elb_healthcheck_fail_old_shard": 0.4,
                            "dashboard_elb_healthcheck_ok": 0.0,
                            "autoscaling.instance_launch": 15.0,
                            "autoscaling.instance_terminate": 15.0,
                            "autoscaling.userdata_fetch_rev_failed": 15.0,
                        },
                        "latency_multipliers": {"dashboard_elb_healthcheck_fail_old_shard": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [],
                    },
                    {
                        "order": 3,
                        "at_min": 40,
                        "rate_multipliers": {
                            "dashboard_login_503": 0.6,
                            "dashboard_view_build_log_503": 0.7,
                            "dashboard_login_slow": 1.0,
                            "dashboard_elb_healthcheck_fail_db": 0.5,
                            "dashboard_elb_healthcheck_fail_old_shard": 0.3,
                            "dashboard_elb_healthcheck_ok": 0.2,
                            "autoscaling.instance_launch": 2.0,
                            "autoscaling.instance_terminate": 2.0,
                            "autoscaling.userdata_fetch_rev_failed": 2.0,
                        },
                        "latency_multipliers": {"dashboard_login_slow": {"p50": 1.2, "p95": 1.3}},
                        "one_shots": [
                            {"ref": "autoscaling.desired_capacity_set", "count": 1, "hosts": ["asg-dashboard"]},
                            {"ref": "autoscaling.register_instance_to_elb", "count": 2, "hosts": ["asg-dashboard"]},
                        ],
                    },
                    {
                        "order": 4,
                        "at_min": 46,
                        "rate_multipliers": {
                            "autoscaling.launch_failed_rate_limit": 20.0,
                            "autoscaling.instance_launch": 0.2,
                            "autoscaling.instance_terminate": 0.3,
                            "autoscaling.userdata_fetch_rev_failed": 0.2,
                        },
                        "latency_multipliers": {},
                        "one_shots": [],
                    },
                    {
                        "order": 5,
                        "at_min": 48,
                        "rate_multipliers": {
                            "dashboard_elb_healthcheck_ok": 0.9,
                            "dashboard_elb_healthcheck_fail_db": 0.1,
                            "dashboard_elb_healthcheck_fail_old_shard": 0.0,
                            "dashboard_login_503": 0.1,
                            "dashboard_view_build_log_503": 0.1,
                            "dashboard_login_slow": 3.0,
                            "postgres_rds.rds_metrics": 0.0,
                            "postgres_rds.rds_metrics_recovered": 1.0,
                            "postgres_rds.connection_timeout": 0.2,
                            "postgres_rds.slow_query": 0.2,
                            "pgbouncer_central.pool_stats": 0.0,
                            "pgbouncer_central.pool_stats_recovered": 1.0,
                            "pgbouncer_central.server_connect_failed": 0.2,
                            "autoscaling.userdata_fetch_rev_failed": 0.1,
                            "autoscaling.instance_launch": 0.5,
                            "autoscaling.instance_terminate": 0.5,
                            "autoscaling.launch_failed_rate_limit": 0.2,
                        },
                        "latency_multipliers": {"dashboard_login_slow": {"p50": 0.7, "p95": 0.6}, "agent_job_poll": {"p50": 0.95, "p95": 0.95}},
                        "one_shots": [
                            {"ref": "postgres_rds.modify_instance", "count": 1, "hosts": ["rds-primary"]},
                            {"ref": "autoscaling.ami_rollout_started", "count": 1, "hosts": ["asg-dashboard"]},
                            {"ref": "pgbouncer_central.config_change", "count": 1, "hosts": ["pgb-1"]},
                        ],
                    },
                ]
            }
        },
    }
}


def stable_u32(s: str) -> int:
    h = hashlib.md5(s.encode("utf-8")).hexdigest()
    return int(h[:8], 16)


def stable_uniform01(s: str) -> float:
    return stable_u32(s) / 2**32


def hex_from_key(nchars: int, key: str) -> str:
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    if nchars <= len(h):
        return h[:nchars]
    out = h
    while len(out) < nchars:
        out += hashlib.md5((out + key).encode("utf-8")).hexdigest()
    return out[:nchars]


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def alloc_count(expected: float, key: str) -> int:
    if expected <= 0:
        return 0
    base = int(math.floor(expected))
    frac = expected - base
    u = stable_uniform01(f"round:{key}")
    return base + (1 if frac > u else 0)


def schedule_even_times(start_dt: datetime, end_dt: datetime, n: int, key: str) -> List[datetime]:
    if n <= 0:
        return []
    duration_s = (end_dt - start_dt).total_seconds()
    offsets = (np.arange(n) + 0.5) * (duration_s / n)
    rng = np.random.default_rng(1_000_003 + stable_u32(f"jitter:{key}"))
    jitter = rng.uniform(-0.2, 0.2, size=n)
    offsets = np.clip(offsets + jitter, 0.0, max(0.0, duration_s - 1e-3))
    return [start_dt + timedelta(seconds=float(x)) for x in offsets]


def lognormal_from_p50_p95(p50: float, p95: float, rng: np.random.Generator) -> float:
    p50 = max(1e-6, float(p50))
    p95 = max(p50 * 1.000001, float(p95))
    mu = math.log(p50)
    sigma = (math.log(p95) - math.log(p50)) / 1.6448536269514722
    return float(rng.lognormal(mean=mu, sigma=max(1e-9, sigma)))


def sample_ms_pair(
    pair: List[float],
    mult_p50: float,
    mult_p95: float,
    rng: np.random.Generator,
    hard_min: Optional[float] = None,
    hard_cap: Optional[float] = None,
) -> int:
    """
    Sample a positive latency in ms from a lognormal calibrated to [p50,p95] hints,
    then (optionally) clip it into [hard_min, hard_cap]. This keeps message timing
    fields consistent with timestamp gaps when those fields have bounded domains.
    """
    p50 = max(1.0, float(pair[0]) * float(mult_p50))
    p95 = max(p50, float(pair[1]) * float(mult_p95))
    x = lognormal_from_p50_p95(p50, p95, rng)

    cap = 2.5 * p95
    if hard_cap is not None:
        cap = min(cap, float(hard_cap))
    if hard_min is not None:
        x = max(x, float(hard_min))
    x = min(x, cap)
    return int(max(1, round(x)))


def choose_from_domain(dom: Dict[str, Any], key: str, state: Optional[str] = None) -> Any:
    k = dom["k"]
    v = dom["v"]
    if k == "ch":
        idx = stable_u32(f"ch:{key}") % len(v)
        return v[idx]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if hi <= lo:
            return lo
        u = stable_uniform01(f"i:{key}")
        return int(lo + math.floor(u * (hi - lo + 1)))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        u = stable_uniform01(f"f:{key}")
        return lo + (hi - lo) * u
    if k == "hex":
        return hex_from_key(int(v), f"hex:{key}")
    if k == "uuid":
        h = hex_from_key(32, f"uuid:{key}")
        return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:]}"
    if k == "str":
        hint = str(v)
        if "i-<hex8>" in hint or key.endswith("instance_id"):
            return "i-" + hex_from_key(8, f"inst:{key}")
        if "ami-<hex8>" in hint or key.endswith("ami"):
            return "ami-" + hex_from_key(8, f"ami:{key}")
        return hint
    if k == "ip":
        return "127.0.0.1"
    return ""


def template_placeholders(msg: str) -> List[str]:
    out: List[str] = []
    i = 0
    while i < len(msg):
        if msg[i] == "{":
            j = msg.find("}", i + 1)
            if j != -1:
                name = msg[i + 1 : j]
                if name.isidentifier():
                    out.append(name)
                i = j + 1
            else:
                i += 1
        else:
            i += 1
    return out


@dataclass
class LogRow:
    ts: datetime
    level: str
    message: str
    trace_id: str
    service: str
    host: str


COMP = SYSTEM["components"]
LOG_TEMPLATES: Dict[str, Dict[str, Any]] = {}
PLACEHOLDERS: Dict[str, List[str]] = {}

for cid, cinfo in COMP.items():
    for lid, tmpl in cinfo.get("logs", {}).items():
        ref = f"{cid}.{lid}"
        LOG_TEMPLATES[ref] = tmpl
        PLACEHOLDERS[ref] = template_placeholders(tmpl["msg"])


def derive_failure_segments() -> List[Dict[str, Any]]:
    fstart = SCENARIO["scenario"]["time"]["phases"]["f"]["start_min"]
    fend = SCENARIO["scenario"]["time"]["phases"]["f"]["end_min"]
    events = sorted(SCENARIO["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))

    boundaries = [fstart] + [e["at_min"] for e in events] + [fend]
    boundaries = sorted(set(boundaries))

    rate_flow: Dict[str, float] = {}
    rate_bg: Dict[str, float] = {}
    lat_flow: Dict[str, Dict[str, float]] = {}

    segs: List[Dict[str, Any]] = []
    events_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        events_by_min.setdefault(int(e["at_min"]), []).append(e)

    for i in range(len(boundaries) - 1):
        s = int(boundaries[i])
        e = int(boundaries[i + 1])

        for ev in events_by_min.get(s, []):
            for k, m in (ev.get("rate_multipliers") or {}).items():
                if "." in k:
                    rate_bg[k] = float(m)
                else:
                    rate_flow[k] = float(m)
            for fk, mult in (ev.get("latency_multipliers") or {}).items():
                lat_flow[fk] = {"p50": float(mult.get("p50", 1.0)), "p95": float(mult.get("p95", 1.0))}

        ones = []
        for ev in events_by_min.get(s, []):
            for os in ev.get("one_shots") or []:
                ones.append({"at_min": s, **os})

        segs.append({"start_min": s, "end_min": e, "rate_flow": dict(rate_flow), "rate_bg": dict(rate_bg), "lat_flow": dict(lat_flow), "one_shots": ones})
    return segs


FAILURE_SEGS = derive_failure_segments()


def emit_log(rows: List[LogRow], when: datetime, ref: str, state: str, bound: Dict[str, Any], host_override: Optional[str] = None) -> None:
    tmpl = LOG_TEMPLATES[ref]
    cid, _lid = ref.split(".", 1)
    svc = COMP[cid]["svc"]
    host = host_override if host_override is not None else (COMP[cid]["hosts"][0] if COMP[cid].get("hosts") else "")
    msg = tmpl["msg"]

    values: Dict[str, Any] = {}
    values.update(bound)

    for name in PLACEHOLDERS[ref]:
        if name in values:
            continue
        dom = None
        if "vars" in tmpl and name in tmpl["vars"]:
            dom = tmpl["vars"][name]
        elif "state_vars" in tmpl and state in tmpl["state_vars"] and name in tmpl["state_vars"][state]:
            dom = tmpl["state_vars"][state][name]
        if dom is None:
            values[name] = ""
        else:
            values[name] = choose_from_domain(dom, key=f"{ref}:{name}:{iso_z(when)}", state=state)

    try:
        rendered = msg.format(**values)
    except Exception:
        rendered = msg + " " + " ".join(f"{k}={v}" for k, v in sorted(values.items()))

    rows.append(LogRow(ts=when, level=tmpl["lvl"], message=rendered, trace_id="", service=svc, host=host))


def choose_component_host(component_id: str, flow_id: str, inst_key: str) -> str:
    hosts = COMP[component_id].get("hosts") or [""]
    idx = stable_u32(f"host:{component_id}:{flow_id}:{inst_key}") % len(hosts)
    return hosts[idx]


def flow_base_context(flow_id: str, inst_key: str) -> Dict[str, Any]:
    if flow_id in ("dashboard_login", "dashboard_login_slow", "dashboard_login_503"):
        return {"method": "GET", "url_path": "/login", "controller": "sessions", "action": "new", "status": 200}
    if flow_id in ("dashboard_view_build_log", "dashboard_view_build_log_503"):
        bid = 1000 + (stable_u32(f"build:{inst_key}") % 9000)
        return {"method": "GET", "url_path": f"/builds/{bid}/log", "controller": "builds", "action": "show", "status": 200}
    if flow_id.startswith("dashboard_elb_healthcheck"):
        return {}
    if flow_id == "agent_job_poll":
        p = "/agent/jobs/next" if (stable_u32(f"agentpath:{inst_key}") % 5) != 0 else "/agent/ping"
        return {"method": "GET", "url_path": p}
    return {}


def attempts_for_expected(expected: float, max_attempts: int, key: str) -> int:
    if max_attempts <= 1 or expected <= 1.0 + 1e-9:
        return 1
    if max_attempts == 2:
        p2 = max(0.0, min(1.0, expected - 1.0))
        u = stable_uniform01(f"att:{key}")
        return 2 if u < p2 else 1
    extra = max(0.0, min(float(max_attempts - 1), expected - 1.0))
    if extra <= 0.20:
        p3 = 0.0
        p2 = min(1.0, extra)
    else:
        p3 = min(0.05, extra / 2.0)
        p2 = max(0.0, min(1.0, extra - 2.0 * p3))
    p1 = max(0.0, 1.0 - p2 - p3)
    u = stable_uniform01(f"att:{key}")
    if u < p1:
        return 1
    if u < p1 + p2:
        return 2
    return 3


def timing_bounds_for_leg(ref: str, state: str) -> Optional[Tuple[int, int]]:
    # Bounds that must match the timestamp gap because the message carries the observed timing.
    if ref == "dashboard_app.req_completed_ok":
        lo, hi = LOG_TEMPLATES[ref]["state_vars"][state]["dur_ms"]["v"]
        return int(lo), int(hi)
    if ref == "dashboard_app.healthcheck_ok":
        lo, hi = LOG_TEMPLATES[ref]["state_vars"][state]["dur_ms"]["v"]
        return int(lo), int(hi)
    if ref == "dashboard_app.healthcheck_db_timeout":
        lo, hi = LOG_TEMPLATES[ref]["vars"]["waited_ms"]["v"]
        return int(lo), int(hi)
    if ref == "agent_api.agent_req":
        lo, hi = LOG_TEMPLATES[ref]["state_vars"][state]["dur_ms"]["v"]
        return int(lo), int(hi)
    if ref == "dashboard_elb.req_no_healthy":
        lo, hi = LOG_TEMPLATES[ref]["vars"]["lat_ms"]["v"]
        return int(lo), int(hi)
    return None


def simulate_flow_instance(rows: List[LogRow], state: str, flow_def: Dict[str, Any], start_ts: datetime, lat_mult: Dict[str, float], inst_key: str) -> None:
    flow_id = flow_def["id"]
    retry = flow_def["retry"]
    max_attempts = int(retry["max_attempts"])
    expected_attempts = float(retry["expected_attempts"])
    n_attempts = attempts_for_expected(expected_attempts, max_attempts, key=f"{flow_id}:{inst_key}")

    base_ctx = flow_base_context(flow_id, inst_key)
    req_id = hex_from_key(16, f"req:{state}:{flow_id}:{inst_key}")
    base_ctx = dict(base_ctx)
    base_ctx["req_id"] = req_id

    host_map: Dict[str, str] = {}

    def host_for(cid: str) -> str:
        if cid not in host_map:
            host_map[cid] = choose_component_host(cid, flow_id, inst_key)
        return host_map[cid]

    app_host = host_for("dashboard_app") if any(ref.startswith("dashboard_app.") for ref in flow_def["emit"]) else None
    if app_host is not None:
        base_ctx["target"] = app_host

    current = start_ts
    for attempt in range(1, n_attempts + 1):
        attempt_key = f"{inst_key}:a{attempt}"
        rng = np.random.default_rng(2_000_003 + stable_u32(f"flowrng:{state}:{flow_id}:{attempt_key}"))

        if attempt >= 2:
            for rref in retry.get("emit_per_retry") or []:
                emit_log(
                    rows,
                    current + timedelta(milliseconds=1),
                    rref,
                    state,
                    bound={"attempt": attempt, "reason": "db busy" if (attempt % 2 == 0) else "upstream timeout", "req_id": req_id},
                    host_override=host_for(rref.split(".", 1)[0]),
                )

        attempt_ctx = dict(base_ctx)
        if flow_id == "agent_job_poll":
            if n_attempts == 1:
                u = stable_uniform01(f"agent500:{attempt_key}")
                status = 500 if u < 0.01 else (204 if attempt_ctx["url_path"] == "/agent/jobs/next" else 200)
            else:
                status = 500 if attempt != n_attempts else (204 if attempt_ctx["url_path"] == "/agent/jobs/next" else 200)
            attempt_ctx["status"] = status

        # Sample inter-log deltas, clipping to any message-carried timing domains to keep timestamps and fields consistent.
        deltas_ms: List[int] = []
        for idx, pair in enumerate(flow_def["latency_ms"]):
            ref = flow_def["emit"][idx]
            bounds = timing_bounds_for_leg(ref, state)
            hard_min = float(bounds[0]) if bounds else None
            hard_cap = float(bounds[1]) if bounds else None
            deltas_ms.append(sample_ms_pair(pair, lat_mult["p50"], lat_mult["p95"], rng, hard_min=hard_min, hard_cap=hard_cap))

        # Ensure ELB "req_ok latency_ms" can fit within its bounded domain by scaling all legs if needed.
        if any(ref == "dashboard_elb.req_ok" for ref in flow_def["emit"]):
            hi = int(LOG_TEMPLATES["dashboard_elb.req_ok"]["state_vars"][state]["lat_ms"]["v"][1])
            total = sum(deltas_ms)
            if total > hi and total > 0:
                scale = hi / float(total)
                deltas_ms = [max(1, int(round(d * scale))) for d in deltas_ms]

        # Ensure healthcheck result "latency_ms" equals the actual time between access and result, and stays within its domain.
        if len(flow_def["emit"]) >= 3 and flow_def["emit"][0] == "dashboard_elb.hc_access" and flow_def["emit"][2].startswith("dashboard_elb.hc_result"):
            result_ref = flow_def["emit"][2]
            lo, hi = LOG_TEMPLATES[result_ref]["vars"]["lat_ms"]["v"]
            lo_i, hi_i = int(lo), int(hi)
            # resp is time from hc_access to hc_result
            resp = deltas_ms[1] + deltas_ms[2]

            # If too low, extend the last leg.
            if resp < lo_i:
                deltas_ms[2] += (lo_i - resp)
                resp = lo_i

            # If too high, reduce last leg then app leg (without violating its own timing bounds if applicable).
            if resp > hi_i:
                excess = resp - hi_i
                # reduce last leg down to 1ms
                red2 = min(excess, max(0, deltas_ms[2] - 1))
                deltas_ms[2] -= red2
                excess -= red2

                if excess > 0:
                    # reduce app leg, but not below its message-carried minimum if present
                    app_ref = flow_def["emit"][1]
                    app_bounds = timing_bounds_for_leg(app_ref, state)
                    app_min = int(app_bounds[0]) if app_bounds else 1
                    red1 = min(excess, max(0, deltas_ms[1] - app_min))
                    deltas_ms[1] -= red1
                    excess -= red1

                resp = deltas_ms[1] + deltas_ms[2]
                # Final guard: if still high, shrink last leg further (should be rare/unreachable with above)
                if resp > hi_i:
                    deltas_ms[2] = max(1, deltas_ms[2] - (resp - hi_i))
                    resp = deltas_ms[1] + deltas_ms[2]

        t = current
        for idx, ref in enumerate(flow_def["emit"]):
            cid, _lid = ref.split(".", 1)
            t = t + timedelta(milliseconds=int(deltas_ms[idx]))
            bound = dict(attempt_ctx)

            if ref == "dashboard_app.req_completed_ok":
                bound["dur_ms"] = int(deltas_ms[idx])
                bound.setdefault("status", bound.get("status", 200))
            elif ref == "dashboard_elb.req_ok":
                bound["lat_ms"] = int(sum(deltas_ms))
                bound.setdefault("status", bound.get("status", 200))
                bound.setdefault("target", app_host or choose_from_domain(LOG_TEMPLATES[ref]["vars"]["target"], key=f"{flow_id}:{inst_key}:target"))
            elif ref == "dashboard_elb.req_no_healthy":
                bound["lat_ms"] = int(deltas_ms[idx])
            elif ref == "dashboard_app.healthcheck_ok":
                bound["dur_ms"] = int(deltas_ms[idx])
            elif ref == "dashboard_app.healthcheck_db_timeout":
                bound["waited_ms"] = int(deltas_ms[idx])
            elif ref in ("dashboard_elb.hc_result_ok", "dashboard_elb.hc_result_500"):
                bound["lat_ms"] = int(deltas_ms[1] + deltas_ms[2]) if len(deltas_ms) >= 3 else int(deltas_ms[idx])
                bound.setdefault("target", app_host or choose_from_domain(LOG_TEMPLATES[ref]["vars"]["target"], key=f"{flow_id}:{inst_key}:target"))
            elif ref == "agent_api.agent_req":
                bound["dur_ms"] = int(deltas_ms[idx])
                if "status" not in bound:
                    bound["status"] = 204 if bound.get("url_path") == "/agent/jobs/next" else 200

            if "instance_id" in PLACEHOLDERS.get(ref, []) and "instance_id" not in bound:
                bound["instance_id"] = "i-" + hex_from_key(8, f"inst:{ref}:{attempt_key}:{iso_z(t)}")
            if "ami" in PLACEHOLDERS.get(ref, []) and "ami" not in bound:
                bound["ami"] = "ami-" + hex_from_key(8, f"ami:{ref}:{attempt_key}:{iso_z(t)}")

            emit_log(rows, t, ref, state, bound=bound, host_override=host_for(cid))

        if attempt < n_attempts:
            bo_pair = retry["backoff_ms"][attempt - 1] if attempt - 1 < len(retry.get("backoff_ms") or []) else [100, 500]
            bo_ms = sample_ms_pair(bo_pair, 1.0, 1.0, rng)
            current = t + timedelta(milliseconds=int(bo_ms))
        else:
            current = t


def get_flow_latency_multiplier(seg: Dict[str, Any], flow_id: str) -> Dict[str, float]:
    m = seg["lat_flow"].get(flow_id)
    if not m:
        return {"p50": 1.0, "p95": 1.0}
    return {"p50": float(m.get("p50", 1.0)), "p95": float(m.get("p95", 1.0))}


def get_rate_multiplier_flow(seg: Dict[str, Any], flow_id: str) -> float:
    return float(seg["rate_flow"].get(flow_id, 1.0))


def get_rate_multiplier_bg(seg: Dict[str, Any], source: str) -> float:
    return float(seg["rate_bg"].get(source, 1.0))


def simulate() -> pd.DataFrame:
    base_time = datetime(2016, 8, 10, 12, 0, 0, tzinfo=timezone.utc)

    n_start = SCENARIO["scenario"]["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"]

    rows: List[LogRow] = []

    n_start_dt = base_time + timedelta(minutes=n_start)
    n_end_dt = base_time + timedelta(minutes=n_end)
    n_dur = n_end - n_start

    # Normal background emissions
    for cid, cinfo in COMP.items():
        for src in cinfo.get("beh", {}).get("n", {}).get("emit", []):
            lid = src["id"]
            per_min = float(src["per_min"])
            scope = src.get("scope", "per_host")
            if per_min <= 0:
                continue
            if scope == "global":
                expected = per_min * n_dur
                cnt = alloc_count(expected, key=f"bg:n:{cid}.{lid}:global")
                times = schedule_even_times(n_start_dt, n_end_dt, cnt, key=f"bg:n:{cid}.{lid}:global")
                host = cinfo["hosts"][0] if cinfo.get("hosts") else ""
                for t in times:
                    emit_log(rows, t, f"{cid}.{lid}", "n", bound={}, host_override=host)
            else:
                for host in cinfo.get("hosts", []):
                    expected = per_min * n_dur
                    cnt = alloc_count(expected, key=f"bg:n:{cid}.{lid}:{host}")
                    times = schedule_even_times(n_start_dt, n_end_dt, cnt, key=f"bg:n:{cid}.{lid}:{host}")
                    for t in times:
                        emit_log(rows, t, f"{cid}.{lid}", "n", bound={}, host_override=host)

    # Normal flows
    for fdef in SYSTEM["flows"]["n"]["req"]:
        flow_id = fdef["id"]
        expected = float(fdef["rpm"]) * n_dur
        cnt = alloc_count(expected, key=f"flow:n:{flow_id}")
        starts = schedule_even_times(n_start_dt, n_end_dt, cnt, key=f"flow:n:{flow_id}")
        for i, st in enumerate(starts):
            inst_key = f"{flow_id}:n:{i}"
            simulate_flow_instance(rows, "n", fdef, st, {"p50": 1.0, "p95": 1.0}, inst_key)

    # Failure segments: one-shots, background, flows
    for seg in FAILURE_SEGS:
        seg_start_dt = base_time + timedelta(minutes=seg["start_min"])
        seg_end_dt = base_time + timedelta(minutes=seg["end_min"])
        seg_dur = seg["end_min"] - seg["start_min"]

        # One-shots at the segment start (event time)
        for os in seg.get("one_shots", []):
            ref = os["ref"]
            count = int(os["count"])
            allowed_hosts = os.get("hosts") or []
            at_dt = base_time + timedelta(minutes=int(os["at_min"]))
            times = schedule_even_times(at_dt, at_dt + timedelta(seconds=1), count, key=f"oneshot:{ref}:{os['at_min']}")
            for j, t in enumerate(times):
                host = allowed_hosts[j % len(allowed_hosts)] if allowed_hosts else None
                emit_log(rows, t, ref, "f", bound={}, host_override=host)

        # Failure background emissions (only failure-state sources are modulated)
        for cid, cinfo in COMP.items():
            for src in cinfo.get("beh", {}).get("f", {}).get("emit", []):
                lid = src["id"]
                per_min = float(src["per_min"])
                scope = src.get("scope", "per_host")
                source_key = f"{cid}.{lid}"
                mult = get_rate_multiplier_bg(seg, source_key)
                eff = per_min * mult
                if eff <= 0:
                    continue
                if scope == "global":
                    expected = eff * seg_dur
                    cnt = alloc_count(expected, key=f"bg:f:{source_key}:global:{seg['start_min']}-{seg['end_min']}")
                    times = schedule_even_times(seg_start_dt, seg_end_dt, cnt, key=f"bg:f:{source_key}:global:{seg['start_min']}")
                    host = cinfo["hosts"][0] if cinfo.get("hosts") else ""
                    for t in times:
                        emit_log(rows, t, source_key, "f", bound={}, host_override=host)
                else:
                    for host in cinfo.get("hosts", []):
                        expected = eff * seg_dur
                        cnt = alloc_count(expected, key=f"bg:f:{source_key}:{host}:{seg['start_min']}-{seg['end_min']}")
                        times = schedule_even_times(seg_start_dt, seg_end_dt, cnt, key=f"bg:f:{source_key}:{host}:{seg['start_min']}")
                        for t in times:
                            emit_log(rows, t, source_key, "f", bound={}, host_override=host)

        # Failure flows (modulated)
        for fdef in SYSTEM["flows"]["f"]["req"]:
            flow_id = fdef["id"]
            mult = get_rate_multiplier_flow(seg, flow_id)
            eff_rpm = float(fdef["rpm"]) * mult
            if eff_rpm <= 0:
                continue
            expected = eff_rpm * seg_dur
            cnt = alloc_count(expected, key=f"flow:f:{flow_id}:{seg['start_min']}-{seg['end_min']}")
            starts = schedule_even_times(seg_start_dt, seg_end_dt, cnt, key=f"flow:f:{flow_id}:{seg['start_min']}")
            lat_mult = get_flow_latency_multiplier(seg, flow_id)
            for i, st in enumerate(starts):
                inst_key = f"{flow_id}:f:{seg['start_min']}:{i}"
                simulate_flow_instance(rows, "f", fdef, st, lat_mult, inst_key)

    df = pd.DataFrame([{"timestamp": r.ts, "level": r.level, "message": r.message, "trace_id": r.trace_id, "service": r.service, "host": r.host} for r in rows])
    df = df.sort_values("timestamp", kind="mergesort").reset_index(drop=True)
    df["timestamp"] = df["timestamp"].apply(iso_z)
    df = df[["timestamp", "level", "message", "trace_id", "service", "host"]]
    return df


def main() -> None:
    df = simulate()
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
