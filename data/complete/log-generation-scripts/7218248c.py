"""
Log-stream simulator for a Travis CI-like platform under a DB TRUNCATE incident.

Plan / structure
1) Embed inputs
   - SYSTEM: components (service/hosts), log templates (lvl/msg/vars/state_vars), behaviors (background emit rates),
     tracing config, and request flows for normal (n) and failure (f).
   - SCENARIO: incident timeline with normal/failure phase bounds, failure events (rate/latency multipliers + one-shots),
     plus required reference fields (flows + manifestations).

2) Deterministic RNG
   - Fixed seeds for Python's random and NumPy's Generator.
   - Deterministic UUID generation from RNG bits (do NOT use uuid.uuid4()).

3) Time model
   - Base time maps scenario minute 0 to 2026-03-13T12:00:00.000Z.
   - Simulate minute-by-minute (start inclusive, end exclusive) for background emissions and flow arrivals.
   - Flow logs use intra-request latency chains (lognormal derived from p50/p95), so logs may spill past minute/phase
     boundaries; this is allowed by the scenario rules.

4) Failure event controller
   - At failure start, all flow and background multipliers default to 1.0.
   - Each failure event overrides specified multipliers at its at_min and they persist until overridden again.
   - One-shots are emitted exactly at the event minute (with sub-second jitter) and are NOT affected by multipliers.
   - Latency multipliers (p50/p95) are applied to all per-log latency pairs within that flow for requests starting in
     the minute where the multipliers are active.

5) Emission mechanisms (and only these)
   - Background: components[].beh.<state>.emit[] with per_min and scope (per_host/global).
   - Flows: flows.<state>.req[].emit[] once per request instance, with ordered latencies.
   - One-shots: scenario.phases.f.events[].one_shots[] at event time.

6) Variable sampling & coherence
   - For each log template, fill placeholders using:
     a) Flow-level context overrides for coherent chains (same req_id/trace_id/user_id/token_id/etc within a flow).
     b) Otherwise sample from the template domains (vars + state_vars).
   - Keep causal consistency (e.g., stale-token flow forces db_user_id != sub_user_id and action=force_logout).

7) Tracing behavior
   - SYSTEM.tracing.on is true, but flows can opt out with trace:false. For such flows, trace_id is left empty in both
     the CSV trace_id field and in any {trace_id} placeholders.

8) Output
   - Collect all emitted rows, sort by timestamp, write logs.csv with required columns.

Notes
- Templates with state-dependent variables use `state_vars[state]` (e.g., postgres_primary.sql_exec has {rows} only there).
- Snapshot IDs for restore logs are kept consistent across restore_started/restore_progress/restore_completed.
- For the stale-token flow, the UI user_id and JWT subject user_id are intentionally kept identical per request instance.
"""

from __future__ import annotations

import math
import random
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


# -----------------------------
# Embedded inputs (SYSTEM/SCENARIO)
# -----------------------------

SYSTEM: Dict[str, Any] = {
    "sys": {
        "id": "travis_ci_platform",
        "desc": (
            "A hosted CI/CD platform that authenticates users via GitHub, renders a web UI, receives webhook events, "
            "enqueues build jobs, and executes them on worker fleets. The platform relies on a PostgreSQL primary "
            "database for user and build metadata and a Redis-backed job queue for dispatching jobs to workers. "
            "During the incident, the production database tables were truncated (emptied), services briefly continued "
            "against an almost empty dataset (creating new user rows with non-reset sequences), and later the platform "
            "was taken offline for restore. After restore, some clients held tokens pointing at post-truncate user IDs, "
            "causing session/user mismatches."
        ),
    },
    "states": {"n": "normal", "f": "failure"},
    "components": [
        {
            "id": "edge_router",
            "name": "Edge Router (nginx/Envoy)",
            "svc": "edge-router",
            "hosts": ["edge-1", "edge-2"],
            "to": [
                {"dst": "web_app", "proto": "https", "desc": "Routes browser traffic to the web UI service."},
                {"dst": "api_service", "proto": "https", "desc": "Routes API and webhook traffic to the API service."},
            ],
            "logs": {
                "request_received": {
                    "desc": "Request received at the edge before proxying upstream.",
                    "lvl": "INFO",
                    "msg": "ingress {method} {path} ip={client_ip} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "method": {"k": "ch", "v": ["GET", "POST", "PUT", "DELETE"]},
                        "path": {"k": "str", "v": "path"},
                        "client_ip": {"k": "ip", "v": "0.0.0.0/0"},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "upstream_unavailable": {
                    "desc": "Edge returns 503 because upstream is down or intentionally drained.",
                    "lvl": "ERROR",
                    "msg": "upstream unavailable route={route} status=503 req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "route": {"k": "ch", "v": ["web_app", "api_service"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "nginx_stub_status": {
                    "desc": "Periodic status line used for dashboards.",
                    "lvl": "DEBUG",
                    "msg": "stub_status active={active} reading={reading} writing={writing} waiting={waiting}",
                    "vars": {
                        "active": {"k": "i", "v": [50, 5000]},
                        "reading": {"k": "i", "v": [0, 200]},
                        "writing": {"k": "i", "v": [0, 500]},
                        "waiting": {"k": "i", "v": [0, 5000]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "nginx_stub_status", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "nginx_stub_status", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        {
            "id": "web_app",
            "name": "Web Application (travis-web backend)",
            "svc": "web-app",
            "hosts": ["web-1", "web-2"],
            "to": [{"dst": "api_service", "proto": "https", "desc": "Calls the API service for auth and data."}],
            "logs": {
                "route_login": {
                    "desc": "Web app handles the login route and delegates auth to the API.",
                    "lvl": "INFO",
                    "msg": "route /login provider=github req_id={req_id} trace_id={trace_id}",
                    "vars": {"req_id": {"k": "uuid", "v": None}, "trace_id": {"k": "hex", "v": 32}},
                },
                "session_established": {
                    "desc": "Web app stores a token and considers the session established.",
                    "lvl": "INFO",
                    "msg": "session established user_id={user_id} token_id={token_id} profile_state={profile_state} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "user_id": {"k": "i", "v": [1, 7000000]},
                        "token_id": {"k": "uuid", "v": None},
                        "profile_state": {"k": "ch", "v": ["complete", "blank"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "render_start": {
                    "desc": "Start rendering dashboard; will call API for profile data.",
                    "lvl": "INFO",
                    "msg": "render dashboard user_id={user_id} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "user_id": {"k": "i", "v": [1, 7000000]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "render_ok": {
                    "desc": "Dashboard rendered with expected data.",
                    "lvl": "INFO",
                    "msg": "render ok user_id={user_id} repos={repo_count} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "user_id": {"k": "i", "v": [1, 7000000]},
                        "repo_count": {"k": "i", "v": [0, 5000]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "render_blank_profile": {
                    "desc": "Dashboard rendered without user data (blank profile).",
                    "lvl": "WARN",
                    "msg": "render blank profile user_id={user_id} reason={reason} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "user_id": {"k": "i", "v": [1, 7000000]},
                        "reason": {"k": "ch", "v": ["user_missing", "no_repos", "api_empty_response"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "forced_logout": {
                    "desc": "Web app forces logout due to token/user mismatch.",
                    "lvl": "WARN",
                    "msg": "forced logout token_id={token_id} reason={reason} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "token_id": {"k": "uuid", "v": None},
                        "reason": {"k": "ch", "v": ["jwt_user_mismatch", "user_not_found", "token_revoked"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "heartbeat": {
                    "desc": "Periodic app heartbeat/health log.",
                    "lvl": "DEBUG",
                    "msg": "heartbeat version={version} rss_mb={rss_mb}",
                    "vars": {"version": {"k": "str", "v": "semver"}, "rss_mb": {"k": "i", "v": [150, 2000]}},
                },
                "shutdown_initiated": {
                    "desc": "One-shot marker when the web app is intentionally taken offline.",
                    "lvl": "INFO",
                    "msg": "shutdown initiated reason={reason}",
                    "vars": {"reason": {"k": "ch", "v": ["incident_response", "maintenance_mode"]}},
                },
                "startup_complete": {
                    "desc": "One-shot marker when the web app comes back online.",
                    "lvl": "INFO",
                    "msg": "startup complete version={version}",
                    "vars": {"version": {"k": "str", "v": "semver"}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "heartbeat", "per_min": 1.0, "scope": "per_host"}]},
                "f": {"emit": [{"id": "heartbeat", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "api_service",
            "name": "API Service (Rails)",
            "svc": "api-service",
            "hosts": ["api-1", "api-2", "api-3"],
            "to": [
                {"dst": "postgres_primary", "proto": "jdbc", "desc": "Reads/writes users, repos, builds, tokens and job state."},
                {"dst": "job_queue", "proto": "redis", "desc": "Enqueues build jobs for worker consumption."},
                {"dst": "web_app", "proto": "https", "desc": "Logical response hop back to the web app."},
            ],
            "logs": {
                "login_ok": {
                    "desc": "Successful authentication callback processing.",
                    "lvl": "INFO",
                    "msg": "login ok github_id={github_id} user_id={user_id} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "github_id": {"k": "i", "v": [1, 400000000]},
                        "user_id": {"k": "i", "v": [1, 7000000]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "user_created_after_truncate": {
                    "desc": "User row created because expected record was missing after data loss.",
                    "lvl": "WARN",
                    "msg": "user created after truncate github_id={github_id} new_user_id={user_id} seq_last_value={seq_last_value} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "github_id": {"k": "i", "v": [1, 400000000]},
                        "user_id": {"k": "i", "v": [1, 7000000]},
                        "seq_last_value": {"k": "i", "v": [1000, 8000000]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "jwt_issued": {
                    "desc": "Issues a signed token to the client for later API calls.",
                    "lvl": "INFO",
                    "msg": "jwt issued token_id={token_id} sub_user_id={user_id} exp_s={exp_s} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "token_id": {"k": "uuid", "v": None},
                        "user_id": {"k": "i", "v": [1, 7000000]},
                        "exp_s": {"k": "i", "v": [900, 2592000]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "profile_query": {
                    "desc": "API begins fetching profile data from the database.",
                    "lvl": "DEBUG",
                    "msg": "profile query user_id={user_id} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "user_id": {"k": "i", "v": [1, 7000000]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "profile_ok": {
                    "desc": "API returned profile data successfully.",
                    "lvl": "INFO",
                    "msg": "profile ok user_id={user_id} repos={repo_count} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "user_id": {"k": "i", "v": [1, 7000000]},
                        "repo_count": {"k": "i", "v": [0, 5000]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "profile_not_found": {
                    "desc": "API could not find profile data for a user (tables emptied / missing row).",
                    "lvl": "WARN",
                    "msg": "profile not found user_id={user_id} reason={reason} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "user_id": {"k": "i", "v": [1, 7000000]},
                        "reason": {"k": "ch", "v": ["user_row_missing", "repos_missing", "db_empty"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "webhook_received": {
                    "desc": "GitHub webhook received and validated.",
                    "lvl": "INFO",
                    "msg": "webhook received event={event} repo_id={repo_id} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "event": {"k": "ch", "v": ["push", "pull_request", "cron"]},
                        "repo_id": {"k": "i", "v": [1, 90000000]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "build_enqueued": {
                    "desc": "API enqueued a build job in Redis after recording metadata in DB.",
                    "lvl": "INFO",
                    "msg": "build enqueued repo_id={repo_id} build_id={build_id} queue={queue} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "repo_id": {"k": "i", "v": [1, 90000000]},
                        "build_id": {"k": "i", "v": [1, 200000000]},
                        "queue": {"k": "ch", "v": ["builds-ec2", "builds-gce", "builds-macos"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "enqueue_failed_db": {
                    "desc": "Enqueue failed because DB writes violated constraints or required rows were missing.",
                    "lvl": "ERROR",
                    "msg": "enqueue failed db repo_id={repo_id} err={err} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "repo_id": {"k": "i", "v": [1, 90000000]},
                        "err": {"k": "str", "v": "err"},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "authn_from_jwt": {
                    "desc": "API authenticates a request by looking up the JWT subject in DB.",
                    "lvl": "DEBUG",
                    "msg": "authn from jwt token_id={token_id} sub_user_id={sub_user_id} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "token_id": {"k": "uuid", "v": None},
                        "sub_user_id": {"k": "i", "v": [1, 7000000]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "jwt_user_mismatch": {
                    "desc": "Token subject does not match restored DB user mapping (wrong user / missing user).",
                    "lvl": "ERROR",
                    "msg": "jwt user mismatch token_id={token_id} sub_user_id={sub_user_id} db_user_id={db_user_id} action={action} req_id={req_id} trace_id={trace_id}",
                    "vars": {
                        "token_id": {"k": "uuid", "v": None},
                        "sub_user_id": {"k": "i", "v": [1, 7000000]},
                        "db_user_id": {"k": "i", "v": [0, 7000000]},
                        "action": {"k": "ch", "v": ["deny_401", "force_logout"]},
                        "req_id": {"k": "uuid", "v": None},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "healthcheck": {
                    "desc": "Periodic health check including basic DB connectivity status.",
                    "lvl": "DEBUG",
                    "msg": "health ok db={db} puma_workers={puma_workers}",
                    "vars": {"db": {"k": "ch", "v": ["ok", "degraded"]}, "puma_workers": {"k": "i", "v": [2, 32]}},
                },
                "pool_stats": {
                    "desc": "Periodic ActiveRecord pool stats snapshot.",
                    "lvl": "DEBUG",
                    "msg": "db pool size={size} busy={busy} dead={dead} waiting={waiting}",
                    "vars": {
                        "size": {"k": "i", "v": [5, 100]},
                        "busy": {"k": "i", "v": [0, 100]},
                        "dead": {"k": "i", "v": [0, 20]},
                        "waiting": {"k": "i", "v": [0, 200]},
                    },
                },
                "scheduled_job_missed": {
                    "desc": "Background warning when scheduled jobs are not being fired (e.g., scheduler down).",
                    "lvl": "WARN",
                    "msg": "scheduled job missed name={job_name} expected_interval_s={interval_s}",
                    "vars": {"job_name": {"k": "ch", "v": ["cron_build_trigger", "repo_sync", "cache_prune"]}, "interval_s": {"k": "i", "v": [60, 3600]}},
                },
                "shutdown_initiated": {
                    "desc": "One-shot marker when the API is intentionally taken offline.",
                    "lvl": "INFO",
                    "msg": "shutdown initiated reason={reason}",
                    "vars": {"reason": {"k": "ch", "v": ["incident_response", "maintenance_mode"]}},
                },
                "startup_complete": {
                    "desc": "One-shot marker when the API comes back online.",
                    "lvl": "INFO",
                    "msg": "startup complete version={version} migrated={migrated}",
                    "vars": {"version": {"k": "str", "v": "semver"}, "migrated": {"k": "ch", "v": ["true", "false"]}},
                },
            },
            "beh": {
                "n": {
                    "emit": [
                        {"id": "healthcheck", "per_min": 1.0, "scope": "per_host"},
                        {"id": "pool_stats", "per_min": 0.5, "scope": "per_host"},
                    ]
                },
                "f": {
                    "emit": [
                        {"id": "healthcheck", "per_min": 1.0, "scope": "per_host"},
                        {"id": "pool_stats", "per_min": 0.5, "scope": "per_host"},
                        {"id": "scheduled_job_missed", "per_min": 0.2, "scope": "per_host"},
                    ]
                },
            },
        },
        {
            "id": "postgres_primary",
            "name": "PostgreSQL Primary",
            "svc": "postgres",
            "hosts": ["pg-1"],
            "to": [
                {"dst": "api_service", "proto": "tcp", "desc": "Logical response path to the API service."},
                {"dst": "build_scheduler", "proto": "tcp", "desc": "Logical response path to the scheduler."},
                {"dst": "worker_pool", "proto": "tcp", "desc": "Logical response path to workers."},
                {"dst": "github_syncer", "proto": "tcp", "desc": "Logical response path to the syncer."},
            ],
            "logs": {
                "sql_exec": {
                    "desc": "Successful SQL execution (sampled); rows may be 0 when tables are empty.",
                    "lvl": "DEBUG",
                    "msg": "sql ok db={db} duration_ms={duration_ms} rows={rows} app={app} pid={pid}",
                    "vars": {
                        "db": {"k": "ch", "v": ["travis_production"]},
                        "duration_ms": {"k": "i", "v": [1, 2000]},
                        "app": {"k": "ch", "v": ["api-service", "build-scheduler", "worker", "github-syncer"]},
                        "pid": {"k": "i", "v": [1000, 99999]},
                    },
                    "state_vars": {"n": {"rows": {"k": "i", "v": [0, 50000]}}, "f": {"rows": {"k": "i", "v": [0, 50]}}},
                },
                "sql_error": {
                    "desc": "SQL error (sampled), commonly constraint violations when referenced rows are missing.",
                    "lvl": "ERROR",
                    "msg": "sql error db={db} sqlstate={sqlstate} err={err} duration_ms={duration_ms} app={app} pid={pid}",
                    "vars": {
                        "db": {"k": "ch", "v": ["travis_production"]},
                        "sqlstate": {"k": "ch", "v": ["23503", "23505", "40001"]},
                        "err": {"k": "str", "v": "err"},
                        "duration_ms": {"k": "i", "v": [1, 5000]},
                        "app": {"k": "ch", "v": ["api-service", "build-scheduler", "worker", "github-syncer"]},
                        "pid": {"k": "i", "v": [1000, 99999]},
                    },
                },
                "ddl_truncate_all_tables": {
                    "desc": "One-shot log for the TRUNCATE execution.",
                    "lvl": "CRITICAL",
                    "msg": "statement executed client_ip={client_ip} app={app} duration_ms={duration_ms} statement={statement}",
                    "vars": {
                        "client_ip": {"k": "ip", "v": "0.0.0.0/0"},
                        "app": {"k": "ch", "v": ["psql", "ruby-test-suite", "unknown"]},
                        "duration_ms": {"k": "i", "v": [1, 900000]},
                        "statement": {"k": "str", "v": "sql"},
                    },
                },
                "restore_progress": {
                    "desc": "Periodic restore/provisioning progress log while recovering from snapshot.",
                    "lvl": "INFO",
                    "msg": "restore in progress pct={pct} eta_min={eta_min} snapshot_id={snapshot_id}",
                    "vars": {"pct": {"k": "i", "v": [0, 100]}, "eta_min": {"k": "i", "v": [0, 600]}, "snapshot_id": {"k": "str", "v": "snapshot-id"}},
                },
                "restore_started": {"desc": "One-shot marker for restore start.", "lvl": "INFO", "msg": "restore started snapshot_id={snapshot_id}", "vars": {"snapshot_id": {"k": "str", "v": "snapshot-id"}}},
                "restore_completed": {
                    "desc": "One-shot marker for restore completion.",
                    "lvl": "INFO",
                    "msg": "restore completed snapshot_id={snapshot_id} recovered_lag_min={lag_min}",
                    "vars": {"snapshot_id": {"k": "str", "v": "snapshot-id"}, "lag_min": {"k": "i", "v": [0, 60]}},
                },
                "checkpoint_complete": {
                    "desc": "Periodic checkpoint completion log.",
                    "lvl": "DEBUG",
                    "msg": "checkpoint complete buffers_written={buffers} wal_mb={wal_mb}",
                    "vars": {"buffers": {"k": "i", "v": [1000, 500000]}, "wal_mb": {"k": "i", "v": [10, 5000]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "checkpoint_complete", "per_min": 0.2, "scope": "global"}]},
                "f": {
                    "emit": [
                        {"id": "checkpoint_complete", "per_min": 0.2, "scope": "global"},
                        {"id": "restore_progress", "per_min": 2.0, "scope": "global"},
                    ]
                },
            },
        },
        {
            "id": "job_queue",
            "name": "Job Queue (Redis)",
            "svc": "job-queue",
            "hosts": ["redis-1", "redis-2", "redis-3"],
            "to": [{"dst": "worker_pool", "proto": "redis", "desc": "Workers connect to Redis to dequeue jobs."}],
            "logs": {
                "enqueue": {
                    "desc": "Job enqueued to a Redis list/stream.",
                    "lvl": "INFO",
                    "msg": "enqueue queue={queue} job_id={job_id} build_id={build_id} repo_id={repo_id} trace_id={trace_id}",
                    "vars": {
                        "queue": {"k": "ch", "v": ["builds-ec2", "builds-gce", "builds-macos"]},
                        "job_id": {"k": "i", "v": [1, 400000000]},
                        "build_id": {"k": "i", "v": [1, 200000000]},
                        "repo_id": {"k": "i", "v": [1, 90000000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "dequeue": {
                    "desc": "Worker dequeues a job for execution.",
                    "lvl": "INFO",
                    "msg": "dequeue queue={queue} job_id={job_id} trace_id={trace_id}",
                    "vars": {"queue": {"k": "ch", "v": ["builds-ec2", "builds-gce", "builds-macos"]}, "job_id": {"k": "i", "v": [1, 400000000]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "queue_depth": {
                    "desc": "Periodic depth metric per queue.",
                    "lvl": "INFO",
                    "msg": "queue depth queue={queue} depth={depth}",
                    "vars": {"queue": {"k": "ch", "v": ["builds-ec2", "builds-gce", "builds-macos"]}, "depth": {"k": "i", "v": [0, 200000]}},
                },
                "redis_latency_warn": {
                    "desc": "Periodic warning when Redis latency rises.",
                    "lvl": "WARN",
                    "msg": "redis latency high p95_ms={p95_ms} ops_s={ops_s}",
                    "vars": {"p95_ms": {"k": "i", "v": [1, 2000]}, "ops_s": {"k": "i", "v": [100, 200000]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "queue_depth", "per_min": 1.0, "scope": "per_host"}, {"id": "redis_latency_warn", "per_min": 0.2, "scope": "per_host"}]},
                "f": {"emit": [{"id": "queue_depth", "per_min": 1.5, "scope": "per_host"}, {"id": "redis_latency_warn", "per_min": 0.5, "scope": "per_host"}]},
            },
        },
        {
            "id": "worker_pool",
            "name": "Build Worker Pool",
            "svc": "worker",
            "hosts": ["worker-1", "worker-2", "worker-3", "worker-4", "worker-5"],
            "to": [
                {"dst": "job_queue", "proto": "redis", "desc": "Dequeues build jobs."},
                {"dst": "postgres_primary", "proto": "jdbc", "desc": "Writes build state and metadata to PostgreSQL."},
            ],
            "logs": {
                "job_start": {
                    "desc": "Worker begins executing a job.",
                    "lvl": "INFO",
                    "msg": "job start queue={queue} job_id={job_id} build_id={build_id} repo_id={repo_id} trace_id={trace_id}",
                    "vars": {
                        "queue": {"k": "ch", "v": ["builds-ec2", "builds-gce", "builds-macos"]},
                        "job_id": {"k": "i", "v": [1, 400000000]},
                        "build_id": {"k": "i", "v": [1, 200000000]},
                        "repo_id": {"k": "i", "v": [1, 90000000]},
                        "trace_id": {"k": "hex", "v": 32},
                    },
                },
                "job_finish": {
                    "desc": "Worker finishes a job and reports result.",
                    "lvl": "INFO",
                    "msg": "job finish job_id={job_id} result={result} duration_s={duration_s} trace_id={trace_id}",
                    "vars": {"job_id": {"k": "i", "v": [1, 400000000]}, "result": {"k": "ch", "v": ["passed", "failed", "errored"]}, "duration_s": {"k": "i", "v": [10, 7200]}, "trace_id": {"k": "hex", "v": 32}},
                },
                "job_fail_db": {
                    "desc": "Worker cannot write/read required DB state for a job (e.g., record missing).",
                    "lvl": "ERROR",
                    "msg": "job failed db job_id={job_id} err={err} trace_id={trace_id}",
                    "vars": {"job_id": {"k": "i", "v": [1, 400000000]}, "err": {"k": "str", "v": "err"}, "trace_id": {"k": "hex", "v": 32}},
                },
                "worker_heartbeat": {
                    "desc": "Periodic worker heartbeat.",
                    "lvl": "DEBUG",
                    "msg": "heartbeat running_jobs={running} idle={idle} rss_mb={rss_mb}",
                    "vars": {"running": {"k": "i", "v": [0, 50]}, "idle": {"k": "i", "v": [0, 50]}, "rss_mb": {"k": "i", "v": [200, 6000]}},
                },
                "db_reconnect_attempt": {
                    "desc": "Periodic DB reconnect attempts under failure or heavy load.",
                    "lvl": "WARN",
                    "msg": "db reconnect attempt outcome={outcome} backoff_ms={backoff_ms}",
                    "vars": {"outcome": {"k": "ch", "v": ["success", "timeout", "refused"]}, "backoff_ms": {"k": "i", "v": [50, 60000]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "worker_heartbeat", "per_min": 2.0, "scope": "per_host"}, {"id": "db_reconnect_attempt", "per_min": 0.1, "scope": "per_host"}]},
                "f": {"emit": [{"id": "worker_heartbeat", "per_min": 2.0, "scope": "per_host"}, {"id": "db_reconnect_attempt", "per_min": 1.0, "scope": "per_host"}]},
            },
        },
        {
            "id": "build_scheduler",
            "name": "Cron/Scheduler Service",
            "svc": "build-scheduler",
            "hosts": ["sched-1", "sched-2"],
            "to": [
                {"dst": "postgres_primary", "proto": "jdbc", "desc": "Queries scheduled builds and records scheduling state."},
                {"dst": "job_queue", "proto": "redis", "desc": "Enqueues cron-triggered build jobs."},
            ],
            "logs": {
                "cron_trigger": {"desc": "Scheduler begins a cron scan/tick.", "lvl": "INFO", "msg": "cron tick window_min={window_min} trace_id={trace_id}", "vars": {"window_min": {"k": "i", "v": [1, 60]}, "trace_id": {"k": "hex", "v": 32}}},
                "cron_enqueued": {"desc": "Cron-triggered build enqueued.", "lvl": "INFO", "msg": "cron enqueued repo_id={repo_id} build_id={build_id} queue={queue} trace_id={trace_id}", "vars": {"repo_id": {"k": "i", "v": [1, 90000000]}, "build_id": {"k": "i", "v": [1, 200000000]}, "queue": {"k": "ch", "v": ["builds-ec2", "builds-gce", "builds-macos"]}, "trace_id": {"k": "hex", "v": 32}}},
                "cron_scan_complete": {"desc": "Cron scan completes; can find zero repos after truncation.", "lvl": "INFO", "msg": "cron scan complete candidates={candidates} enqueued={enqueued} trace_id={trace_id}", "vars": {"candidates": {"k": "i", "v": [0, 500000]}, "enqueued": {"k": "i", "v": [0, 500000]}, "trace_id": {"k": "hex", "v": 32}}},
                "cron_query_failed": {"desc": "Cron scan failed due to DB errors.", "lvl": "ERROR", "msg": "cron query failed err={err} trace_id={trace_id}", "vars": {"err": {"k": "str", "v": "err"}, "trace_id": {"k": "hex", "v": 32}}},
                "sched_heartbeat": {"desc": "Scheduler heartbeat/leadership log.", "lvl": "DEBUG", "msg": "heartbeat leader={leader} tick_lag_s={tick_lag_s}", "vars": {"leader": {"k": "ch", "v": ["true", "false"]}, "tick_lag_s": {"k": "i", "v": [0, 3600]}}},
                "scheduler_not_running": {"desc": "Background warning that the scheduler process is not running after restore.", "lvl": "ERROR", "msg": "scheduler not running expected=true last_seen_s={last_seen_s}", "vars": {"last_seen_s": {"k": "i", "v": [0, 86400]}}},
                "scheduler_not_started_after_restore": {"desc": "One-shot marker when on-call realizes scheduler was not restarted.", "lvl": "WARN", "msg": "scheduler restart missed after restore action={action}", "vars": {"action": {"k": "ch", "v": ["restart_pending", "manual_restart_required"]}}},
            },
            "beh": {
                "n": {"emit": [{"id": "sched_heartbeat", "per_min": 0.5, "scope": "per_host"}]},
                "f": {"emit": [{"id": "sched_heartbeat", "per_min": 0.5, "scope": "per_host"}, {"id": "scheduler_not_running", "per_min": 0.2, "scope": "per_host"}]},
            },
        },
        {
            "id": "github_syncer",
            "name": "GitHub User Syncer",
            "svc": "github-syncer",
            "hosts": ["sync-1"],
            "to": [
                {"dst": "github_api", "proto": "https", "desc": "Calls GitHub API to fetch user/org membership."},
                {"dst": "postgres_primary", "proto": "jdbc", "desc": "Upserts users and orgs into PostgreSQL."},
            ],
            "logs": {
                "sync_start": {"desc": "Start a sync cycle for a GitHub account.", "lvl": "INFO", "msg": "sync start github_id={github_id} trace_id={trace_id}", "vars": {"github_id": {"k": "i", "v": [1, 400000000]}, "trace_id": {"k": "hex", "v": 32}}},
                "upsert_missing_user": {"desc": "Syncer created a user row because the expected user was missing (e.g., after truncation).", "lvl": "WARN", "msg": "upsert missing user github_id={github_id} assigned_user_id={user_id} reason={reason} trace_id={trace_id}", "vars": {"github_id": {"k": "i", "v": [1, 400000000]}, "user_id": {"k": "i", "v": [1, 7000000]}, "reason": {"k": "ch", "v": ["user_row_missing", "org_row_missing", "db_empty"]}, "trace_id": {"k": "hex", "v": 32}}},
                "sync_complete": {"desc": "Sync cycle completed successfully.", "lvl": "INFO", "msg": "sync complete github_id={github_id} upserts={upserts} trace_id={trace_id}", "vars": {"github_id": {"k": "i", "v": [1, 400000000]}, "upserts": {"k": "i", "v": [0, 5000]}, "trace_id": {"k": "hex", "v": 32}}},
                "sync_error": {"desc": "Sync cycle failed due to DB errors or API errors.", "lvl": "ERROR", "msg": "sync error github_id={github_id} err={err} trace_id={trace_id}", "vars": {"github_id": {"k": "i", "v": [1, 400000000]}, "err": {"k": "str", "v": "err"}, "trace_id": {"k": "hex", "v": 32}}},
                "sync_heartbeat": {"desc": "Periodic process heartbeat.", "lvl": "DEBUG", "msg": "heartbeat last_cycle_s={last_cycle_s}", "vars": {"last_cycle_s": {"k": "i", "v": [0, 3600]}}},
            },
            "beh": {"n": {"emit": [{"id": "sync_heartbeat", "per_min": 0.5, "scope": "global"}]}, "f": {"emit": [{"id": "sync_heartbeat", "per_min": 0.5, "scope": "global"}]}},
        },
        {
            "id": "github_api",
            "name": "GitHub API (external)",
            "svc": None,
            "hosts": [],
            "to": [{"dst": "postgres_primary", "proto": "https", "desc": "Logical hop representing sync pipeline continuing to DB writes."}],
            "logs": {
                "api_call_ok": {"desc": "Successful GitHub API request (sampled).", "lvl": "INFO", "msg": "github api ok endpoint={endpoint} status={status} remaining={remaining} trace_id={trace_id}", "vars": {"endpoint": {"k": "ch", "v": ["/user", "/user/orgs", "/orgs/{org}/memberships/{user}", "/rate_limit"]}, "status": {"k": "ch", "v": [200, 304]}, "remaining": {"k": "i", "v": [0, 5000]}, "trace_id": {"k": "hex", "v": 32}}},
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
    ],
    "tracing": {"on": True, "origins": ["edge_router", "build_scheduler", "job_queue"], "trace_id": {"k": "hex", "v": 32}},
    "flows": {
        "n": {
            "desc": "Normal traffic includes user logins and dashboard loads, webhooks, cron builds, workers, and GitHub sync cycles.",
            "req": [
                {"id": "user_sign_in_success", "rpm": 50.0, "path": ["edge_router", "web_app", "api_service", "web_app"], "emit": ["edge_router.request_received", "web_app.route_login", "api_service.login_ok", "api_service.jwt_issued", "web_app.session_established"], "latency_ms": [[0, 1], [2, 12], [30, 150], [1, 8], [10, 80]], "trace": True},
                {"id": "load_dashboard_success", "rpm": 150.0, "path": ["edge_router", "web_app", "api_service", "postgres_primary", "api_service", "web_app"], "emit": ["edge_router.request_received", "web_app.render_start", "api_service.profile_query", "postgres_primary.sql_exec", "api_service.profile_ok", "web_app.render_ok"], "latency_ms": [[0, 1], [3, 20], [10, 80], [8, 60], [5, 40], [20, 120]], "trace": True},
                {"id": "webhook_build_trigger_success", "rpm": 25.0, "path": ["edge_router", "api_service", "postgres_primary", "api_service", "job_queue"], "emit": ["edge_router.request_received", "api_service.webhook_received", "postgres_primary.sql_exec", "api_service.build_enqueued", "job_queue.enqueue"], "latency_ms": [[0, 1], [5, 25], [15, 120], [5, 30], [2, 15]], "trace": True},
                {"id": "cron_trigger_enqueue", "rpm": 8.0, "path": ["build_scheduler", "postgres_primary", "build_scheduler", "job_queue"], "emit": ["build_scheduler.cron_trigger", "postgres_primary.sql_exec", "build_scheduler.cron_enqueued", "job_queue.enqueue"], "latency_ms": [[0, 1], [20, 250], [5, 30], [2, 15]], "trace": True},
                {"id": "worker_job_success", "rpm": 35.0, "path": ["job_queue", "worker_pool", "postgres_primary", "worker_pool"], "emit": ["job_queue.dequeue", "worker_pool.job_start", "postgres_primary.sql_exec", "worker_pool.job_finish"], "latency_ms": [[0, 5], [5, 40], [50, 800], [120000, 600000]], "trace": True},
                {"id": "github_user_sync", "rpm": 5.0, "path": ["github_syncer", "github_api", "postgres_primary", "github_syncer"], "emit": ["github_syncer.sync_start", "github_api.api_call_ok", "postgres_primary.sql_exec", "github_syncer.sync_complete"], "latency_ms": [[0, 1], [40, 300], [20, 400], [5, 50]], "trace": False},
            ],
        },
        "f": {
            "desc": "Failure flows cover empty-data window, offline 503s, restore, token mismatches, backlog draining, and sync variants.",
            "req": [
                {"id": "user_sign_in_new_record", "rpm": 50.0, "path": ["edge_router", "web_app", "api_service", "web_app"], "emit": ["edge_router.request_received", "web_app.route_login", "api_service.user_created_after_truncate", "api_service.jwt_issued", "web_app.session_established"], "latency_ms": [[0, 1], [2, 12], [40, 250], [1, 10], [10, 100]], "trace": True},
                {"id": "load_dashboard_blank_profile", "rpm": 150.0, "path": ["edge_router", "web_app", "api_service", "postgres_primary", "api_service", "web_app"], "emit": ["edge_router.request_received", "web_app.render_start", "api_service.profile_query", "postgres_primary.sql_exec", "api_service.profile_not_found", "web_app.render_blank_profile"], "latency_ms": [[0, 1], [3, 25], [10, 120], [5, 120], [2, 20], [10, 120]], "trace": True},
                {"id": "webhook_build_trigger_db_error", "rpm": 25.0, "path": ["edge_router", "api_service", "postgres_primary", "api_service"], "emit": ["edge_router.request_received", "api_service.webhook_received", "postgres_primary.sql_error", "api_service.enqueue_failed_db"], "latency_ms": [[0, 1], [5, 30], [15, 250], [2, 60]], "trace": True},
                {"id": "cron_tick_zero_candidates", "rpm": 8.0, "path": ["build_scheduler", "postgres_primary", "build_scheduler"], "emit": ["build_scheduler.cron_trigger", "postgres_primary.sql_exec", "build_scheduler.cron_scan_complete"], "latency_ms": [[0, 1], [20, 300], [2, 40]], "trace": True},
                {"id": "worker_job_db_error", "rpm": 35.0, "path": ["job_queue", "worker_pool", "postgres_primary", "worker_pool"], "emit": ["job_queue.dequeue", "worker_pool.job_start", "postgres_primary.sql_exec", "worker_pool.job_fail_db"], "latency_ms": [[0, 5], [5, 40], [50, 800], [200, 5000]], "trace": True},
                {"id": "api_offline_503", "rpm": 200.0, "path": ["edge_router"], "emit": ["edge_router.upstream_unavailable"], "latency_ms": [[0, 3]], "trace": True},
                {"id": "api_request_with_stale_token", "rpm": 80.0, "path": ["edge_router", "web_app", "api_service", "postgres_primary", "api_service", "web_app"], "emit": ["edge_router.request_received", "web_app.render_start", "api_service.authn_from_jwt", "postgres_primary.sql_exec", "api_service.jwt_user_mismatch", "web_app.forced_logout"], "latency_ms": [[0, 1], [3, 25], [5, 80], [10, 250], [2, 60], [5, 120]], "trace": True},
                {"id": "webhook_build_trigger_success_degraded", "rpm": 25.0, "path": ["edge_router", "api_service", "postgres_primary", "api_service", "job_queue"], "emit": ["edge_router.request_received", "api_service.webhook_received", "postgres_primary.sql_exec", "api_service.build_enqueued", "job_queue.enqueue"], "latency_ms": [[0, 1], [5, 30], [25, 400], [10, 120], [2, 20]], "trace": True},
                {"id": "worker_job_slow_success", "rpm": 40.0, "path": ["job_queue", "worker_pool", "postgres_primary", "worker_pool"], "emit": ["job_queue.dequeue", "worker_pool.job_start", "postgres_primary.sql_exec", "worker_pool.job_finish"], "latency_ms": [[0, 5], [5, 40], [80, 1500], [240000, 1200000]], "trace": True},
                {"id": "github_user_sync_recreate_users", "rpm": 5.0, "path": ["github_syncer", "github_api", "postgres_primary", "github_syncer"], "emit": ["github_syncer.sync_start", "github_api.api_call_ok", "postgres_primary.sql_exec", "github_syncer.upsert_missing_user", "github_syncer.sync_complete"], "latency_ms": [[0, 1], [40, 300], [20, 450], [1, 20], [5, 80]], "trace": False},
                {"id": "github_user_sync_ok", "rpm": 5.0, "path": ["github_syncer", "github_api", "postgres_primary", "github_syncer"], "emit": ["github_syncer.sync_start", "github_api.api_call_ok", "postgres_primary.sql_exec", "github_syncer.sync_complete"], "latency_ms": [[0, 1], [40, 300], [30, 600], [5, 80]], "trace": False},
            ],
        },
    },
    "assumptions": [
        "Architecture inferred from Travis CI patterns; boundaries adapted for a compact causal graph.",
        "Logical response edges are used to make request paths mechanically checkable while emitting DB logs mid-flow.",
        "TRUNCATE semantics modeled as 'tables emptied but relations remain': reads succeed with empty results, writes may fail with constraint violations.",
        "postgres_primary.sql_exec uses state-dependent {rows} domain via state_vars.",
        "Cron activity during empty-data window modeled as successful scan with zero candidates.",
        "GitHub sync during empty-data window modeled as succeeding while recreating users via upsert_missing_user.",
        "Timeline compressed to 50 minutes while preserving ordering: truncate -> empty-data window -> offline -> restore -> token mismatch/backlog/scheduler missed.",
        "During offline interval, app/scheduler background logs suppressed via scenario rate multipliers.",
    ],
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "travis_ci_db_truncate_20180313",
        "title": "Production PostgreSQL tables truncated; empty-data window creates new users; restore leads to token/user mismatch",
        "states": {"n": "normal", "f": "failure"},
        "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}}},
        "phases": {
            "n": {
                "flows": [
                    "user_sign_in_success",
                    "load_dashboard_success",
                    "webhook_build_trigger_success",
                    "cron_trigger_enqueue",
                    "worker_job_success",
                    "github_user_sync",
                ],
                "manifestation": [
                    "edge_router.request_received",
                    "api_service.profile_ok",
                    "job_queue.queue_depth",
                    "worker_pool.worker_heartbeat",
                ],
            },
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 25,
                        "component": "postgres_primary",
                        "flows": [
                            "user_sign_in_new_record",
                            "load_dashboard_blank_profile",
                            "webhook_build_trigger_db_error",
                            "cron_tick_zero_candidates",
                            "worker_job_db_error",
                            "github_user_sync_recreate_users",
                        ],
                        "rate_multipliers": {
                            "api_offline_503": 0.0,
                            "api_request_with_stale_token": 0.0,
                            "webhook_build_trigger_success_degraded": 0.0,
                            "worker_job_slow_success": 0.0,
                            "github_user_sync_ok": 0.0,
                            "postgres_primary.restore_progress": 0.0,
                            "build_scheduler.scheduler_not_running": 0.0,
                        },
                        "one_shots": [{"ref": "postgres_primary.ddl_truncate_all_tables", "count": 1, "hosts": ["pg-1"]}],
                        "manifestation": [
                            "postgres_primary.ddl_truncate_all_tables",
                            "api_service.profile_not_found",
                            "web_app.render_blank_profile",
                            "api_service.user_created_after_truncate",
                            "github_syncer.upsert_missing_user",
                        ],
                    },
                    {
                        "order": 2,
                        "at_min": 35,
                        "component": "api_service",
                        "flows": ["api_offline_503"],
                        "rate_multipliers": {
                            "api_offline_503": 2.5,
                            "user_sign_in_new_record": 0.0,
                            "load_dashboard_blank_profile": 0.0,
                            "webhook_build_trigger_db_error": 0.0,
                            "cron_tick_zero_candidates": 0.0,
                            "worker_job_db_error": 0.0,
                            "github_user_sync_recreate_users": 0.0,
                            "github_user_sync_ok": 0.0,
                            "api_service.healthcheck": 0.0,
                            "api_service.pool_stats": 0.0,
                            "api_service.scheduled_job_missed": 0.0,
                            "web_app.heartbeat": 0.0,
                            "build_scheduler.sched_heartbeat": 0.0,
                        },
                        "one_shots": [
                            {"ref": "api_service.shutdown_initiated", "count": 3, "hosts": ["api-1", "api-2", "api-3"]},
                            {"ref": "web_app.shutdown_initiated", "count": 2, "hosts": ["web-1", "web-2"]},
                        ],
                        "manifestation": ["edge_router.upstream_unavailable", "api_service.shutdown_initiated", "web_app.shutdown_initiated"],
                    },
                    {
                        "order": 3,
                        "at_min": 38,
                        "component": "postgres_primary",
                        "flows": ["api_offline_503"],
                        "rate_multipliers": {"postgres_primary.restore_progress": 1.0, "job_queue.queue_depth": 2.0, "job_queue.redis_latency_warn": 2.0},
                        "one_shots": [{"ref": "postgres_primary.restore_started", "count": 1, "hosts": ["pg-1"]}],
                        "manifestation": ["postgres_primary.restore_progress", "edge_router.upstream_unavailable", "job_queue.queue_depth"],
                    },
                    {
                        "order": 4,
                        "at_min": 45,
                        "component": "api_service",
                        "flows": ["api_request_with_stale_token", "webhook_build_trigger_success_degraded", "worker_job_slow_success", "worker_job_db_error", "github_user_sync_ok"],
                        "rate_multipliers": {
                            "api_offline_503": 0.6,
                            "api_request_with_stale_token": 1.0,
                            "webhook_build_trigger_success_degraded": 1.0,
                            "worker_job_slow_success": 2.0,
                            "worker_job_db_error": 0.3,
                            "github_user_sync_ok": 1.0,
                            "github_user_sync_recreate_users": 0.2,
                            "postgres_primary.restore_progress": 0.0,
                            "api_service.healthcheck": 1.0,
                            "api_service.pool_stats": 1.0,
                            "api_service.scheduled_job_missed": 1.0,
                            "web_app.heartbeat": 1.0,
                            "build_scheduler.sched_heartbeat": 0.0,
                            "build_scheduler.scheduler_not_running": 0.0,
                        },
                        "latency_multipliers": {"worker_job_slow_success": {"p50": 1.2, "p95": 1.5}, "api_request_with_stale_token": {"p50": 1.1, "p95": 1.4}},
                        "one_shots": [
                            {"ref": "postgres_primary.restore_completed", "count": 1, "hosts": ["pg-1"]},
                            {"ref": "api_service.startup_complete", "count": 3, "hosts": ["api-1", "api-2", "api-3"]},
                            {"ref": "web_app.startup_complete", "count": 2, "hosts": ["web-1", "web-2"]},
                        ],
                        "manifestation": ["api_service.jwt_user_mismatch", "web_app.forced_logout", "job_queue.queue_depth", "worker_pool.db_reconnect_attempt"],
                    },
                    {
                        "order": 5,
                        "at_min": 48,
                        "component": "build_scheduler",
                        "flows": [],
                        "rate_multipliers": {"api_offline_503": 0.2, "build_scheduler.scheduler_not_running": 3.0, "api_service.scheduled_job_missed": 5.0},
                        "one_shots": [{"ref": "build_scheduler.scheduler_not_started_after_restore", "count": 1, "hosts": ["sched-1"]}],
                        "manifestation": ["build_scheduler.scheduler_not_running", "api_service.scheduled_job_missed", "job_queue.queue_depth"],
                    },
                ],
                "steady": [
                    {
                        "component": "api_service",
                        "manifestation": ["api_service.jwt_user_mismatch", "web_app.forced_logout"],
                        "condition": "API online but stale JWT subjects cause mismatches and forced logouts.",
                        "user_impact": "Users forced to log out and re-authenticate; some requests denied.",
                    },
                    {
                        "component": "build_scheduler",
                        "manifestation": ["build_scheduler.scheduler_not_running", "api_service.scheduled_job_missed"],
                        "condition": "Scheduler not running after restore, cron builds not triggered.",
                        "user_impact": "Cron builds missing/delayed.",
                    },
                    {
                        "component": "job_queue",
                        "manifestation": ["job_queue.queue_depth", "job_queue.redis_latency_warn", "worker_pool.db_reconnect_attempt"],
                        "condition": "Backlog remains high; Redis latency elevated.",
                        "user_impact": "Builds delayed; status lags.",
                    },
                ],
                "flows": [
                    "user_sign_in_new_record",
                    "load_dashboard_blank_profile",
                    "webhook_build_trigger_db_error",
                    "cron_tick_zero_candidates",
                    "worker_job_db_error",
                    "api_offline_503",
                    "api_request_with_stale_token",
                    "webhook_build_trigger_success_degraded",
                    "worker_job_slow_success",
                    "github_user_sync_recreate_users",
                    "github_user_sync_ok",
                ],
                "manifestation": [
                    "postgres_primary.ddl_truncate_all_tables",
                    "api_service.profile_not_found",
                    "web_app.render_blank_profile",
                    "api_service.user_created_after_truncate",
                    "github_syncer.upsert_missing_user",
                    "postgres_primary.restore_progress",
                    "edge_router.upstream_unavailable",
                    "api_service.jwt_user_mismatch",
                    "build_scheduler.scheduler_not_running",
                    "api_service.scheduled_job_missed",
                    "job_queue.queue_depth",
                    "worker_pool.db_reconnect_attempt",
                ],
            },
        },
        "assumptions": [
            "Incident details were inferred and scaled for realistic diagnostic logs.",
            "The scenario compresses real-time hours into minutes.",
            "Failure phase uses explicit flow suppression/activation to match narrative.",
        ],
    }
}


# -----------------------------
# Deterministic RNG + helpers
# -----------------------------

SEED = 1337
random.seed(SEED)
NP = np.random.default_rng(SEED)

BASE_TIME = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)

PLACEHOLDER_RE = re.compile(r"\{([a-zA-Z0-9_]+)\}")


def iso_z(dt: datetime) -> str:
    dt = dt.astimezone(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def rand_hex(n: int) -> str:
    alphabet = "0123456789abcdef"
    return "".join(alphabet[int(NP.integers(0, 16))] for _ in range(n))


def rand_uuid_str() -> str:
    hi = int(NP.integers(0, 1 << 64, dtype=np.uint64))
    lo = int(NP.integers(0, 1 << 64, dtype=np.uint64))
    u = uuid.UUID(int=((hi << 64) | lo), version=4)
    return str(u)


def sample_ip(cidr: str) -> str:
    if cidr == "0.0.0.0/0":
        val = int(NP.integers(0, 1 << 32, dtype=np.uint32))
    else:
        val = int(NP.integers(0, 1 << 32, dtype=np.uint32))
    return ".".join(str((val >> shift) & 0xFF) for shift in (24, 16, 8, 0))


def sample_lognormal_ms(p50: float, p95: float) -> float:
    p50 = float(p50)
    p95 = float(p95)
    if p50 <= 0 and p95 <= 0:
        return 0.0
    p50 = max(p50, 0.1)
    p95 = max(p95, p50)
    if p95 == p50:
        sigma = 0.01
    else:
        sigma = (math.log(p95) - math.log(p50)) / 1.645
        sigma = max(sigma, 0.01)
    mu = math.log(p50)
    x = float(NP.lognormal(mean=mu, sigma=sigma))
    softcap = max(1.0, 3.0 * p95)
    if x > softcap:
        x = softcap + 0.1 * (x - softcap)
    return max(0.0, x)


def weighted_choice(items: List[Any], weights: List[float]) -> Any:
    w = np.array(weights, dtype=float)
    w = w / w.sum()
    idx = int(NP.choice(len(items), p=w))
    return items[idx]


def clamp_int(x: float, lo: int, hi: int) -> int:
    return int(max(lo, min(hi, int(round(x)))))


RESTORE_SNAPSHOT_ID = "snap-" + rand_hex(8)


# -----------------------------
# Lookups / pre-processing
# -----------------------------

COMP_BY_ID: Dict[str, Dict[str, Any]] = {c["id"]: c for c in SYSTEM["components"]}
FLOW_BY_ID: Dict[str, Dict[str, Any]] = {}
for st in ["n", "f"]:
    for fl in SYSTEM["flows"][st]["req"]:
        FLOW_BY_ID[fl["id"]] = fl


def get_template(component_id: str, log_id: str) -> Dict[str, Any]:
    comp = COMP_BY_ID[component_id]
    return comp["logs"][log_id]


def comp_service(component_id: str) -> str:
    svc = COMP_BY_ID[component_id]["svc"]
    return svc if svc is not None else ""


def choose_host(component_id: str) -> str:
    hosts = COMP_BY_ID[component_id]["hosts"]
    if not hosts:
        return ""
    return hosts[int(NP.integers(0, len(hosts)))]


def validate_paths() -> None:
    for st in ["n", "f"]:
        for fl in SYSTEM["flows"][st]["req"]:
            path = fl["path"]
            for a, b in zip(path, path[1:]):
                tos = [e["dst"] for e in COMP_BY_ID[a].get("to", [])]
                if b not in tos:
                    raise ValueError(f"Flow path invalid: {fl['id']} has hop {a}->{b} not in {tos}")


validate_paths()


# -----------------------------
# Failure controller
# -----------------------------

@dataclass
class ActiveMultipliers:
    flow_rate: Dict[str, float]
    bg_rate: Dict[str, float]  # key "component.log_id"
    flow_latency: Dict[str, Tuple[float, float]]  # flow_id -> (p50_mult, p95_mult)


def build_failure_controller() -> Tuple[Dict[int, Dict[str, Any]], ActiveMultipliers]:
    events = SCENARIO["scenario"]["phases"]["f"]["events"]
    events_by_min: Dict[int, Dict[str, Any]] = {int(e["at_min"]): e for e in events}

    flow_rate = {fl["id"]: 1.0 for fl in SYSTEM["flows"]["f"]["req"]}

    bg_rate: Dict[str, float] = {}
    for comp in SYSTEM["components"]:
        cid = comp["id"]
        for e in comp.get("beh", {}).get("f", {}).get("emit", []):
            bg_rate[f"{cid}.{e['id']}"] = 1.0

    flow_latency = {fl["id"]: (1.0, 1.0) for fl in SYSTEM["flows"]["f"]["req"]}

    return events_by_min, ActiveMultipliers(flow_rate=flow_rate, bg_rate=bg_rate, flow_latency=flow_latency)


EVENTS_BY_MIN, ACTIVE = build_failure_controller()


def apply_failure_event(at_min: int, rows: List[Dict[str, Any]]) -> None:
    e = EVENTS_BY_MIN.get(at_min)
    if not e:
        return

    for k, v in e.get("rate_multipliers", {}).items():
        if "." in k:
            ACTIVE.bg_rate[k] = float(v)
        else:
            ACTIVE.flow_rate[k] = float(v)

    for flow_id, mult in e.get("latency_multipliers", {}).items():
        ACTIVE.flow_latency[flow_id] = (float(mult.get("p50", 1.0)), float(mult.get("p95", 1.0)))

    one_shots = e.get("one_shots", []) or []
    if not one_shots:
        return

    event_time = BASE_TIME + timedelta(minutes=at_min)
    for shot in one_shots:
        ref = shot["ref"]
        comp_id, log_id = ref.split(".", 1)
        count = int(shot["count"])
        hosts = shot.get("hosts")
        if hosts is None:
            hosts = COMP_BY_ID[comp_id]["hosts"][:] if COMP_BY_ID[comp_id]["hosts"] else [""]

        for i in range(count):
            host = hosts[i % len(hosts)] if hosts else ""
            ts = event_time + timedelta(milliseconds=float(NP.integers(0, 900)))
            row = emit_log(
                ts,
                comp_id,
                log_id,
                state="f",
                trace_id="",
                host_override=host,
                flow_ctx=None,
                extra_ctx={"_minute": at_min},
            )
            rows.append(row)


# -----------------------------
# Variable generation (domain sampling + narrative bias)
# -----------------------------

def semver() -> str:
    major = int(NP.integers(1, 4))
    minor = int(NP.integers(0, 30))
    patch = int(NP.integers(0, 50))
    return f"{major}.{minor}.{patch}"


def snapshot_id() -> str:
    return RESTORE_SNAPSHOT_ID


def sample_str(hint: str) -> str:
    if hint == "path":
        paths = ["/", "/login", "/dashboard", "/settings", "/api/v3/profile", "/api/v3/repos", "/webhook"]
        return str(paths[int(NP.integers(0, len(paths)))])
    if hint == "semver":
        return semver()
    if hint == "snapshot-id":
        return snapshot_id()
    if hint == "sql":
        stmts = [
            "TRUNCATE TABLE users, repos, builds, tokens, memberships;",
            "TRUNCATE TABLE builds, build_requests, jobs, job_states;",
            "TRUNCATE TABLE users, repos, tokens;",
        ]
        return stmts[int(NP.integers(0, len(stmts)))]
    if hint == "err":
        errs = [
            "PG::ForeignKeyViolation: insert or update on table \"builds\" violates foreign key constraint",
            "PG::UniqueViolation: duplicate key value violates unique constraint",
            "ActiveRecord::RecordNotFound: Couldn't find Build with 'id'=",
            "PG::SerializationFailure: could not serialize access due to concurrent update",
            "Redis::TimeoutError: Connection timed out",
        ]
        e = errs[int(NP.integers(0, len(errs)))]
        if "id'=" in e:
            e = e + str(int(NP.integers(1, 200000000)))
        return e
    return f"{hint}-{rand_hex(6)}"


def sample_from_domain(kind: str, domain: Any) -> Any:
    if kind == "i":
        lo, hi = int(domain[0]), int(domain[1])
        return int(NP.integers(lo, hi + 1))
    if kind == "f":
        lo, hi = float(domain[0]), float(domain[1])
        return float(lo + (hi - lo) * NP.random())
    if kind == "ch":
        return domain[int(NP.integers(0, len(domain)))]
    if kind == "uuid":
        return rand_uuid_str()
    if kind == "hex":
        return rand_hex(int(domain))
    if kind == "ip":
        if domain is None:
            return sample_ip("0.0.0.0/0")
        return sample_ip(str(domain))
    if kind == "str":
        return sample_str(str(domain))
    raise ValueError(f"Unknown var kind: {kind}")


def estimate_queue_depth(minute: int, state: str) -> int:
    if state == "n":
        base = 500 + 1500 * (0.5 + NP.random())
    else:
        if minute < 35:
            base = 6000 + (minute - 25) * 400 + 800 * NP.random()
        elif minute < 38:
            base = 15000 + (minute - 35) * 5000 + 2000 * NP.random()
        elif minute < 45:
            base = 30000 + (minute - 38) * 8000 + 4000 * NP.random()
        elif minute < 48:
            base = 90000 - (minute - 45) * 6000 + 3000 * NP.random()
        else:
            base = 72000 - (minute - 48) * 3000 + 2000 * NP.random()
    return clamp_int(base, 0, 200000)


def estimate_redis_latency(depth: int, state: str) -> Tuple[int, int]:
    if state == "n":
        p95 = clamp_int(2 + depth / 1000 + 10 * NP.random(), 1, 2000)
        ops = clamp_int(5000 + depth * 2 + 20000 * NP.random(), 100, 200000)
    else:
        p95 = clamp_int(10 + depth / 200 + 50 * NP.random(), 1, 2000)
        ops = clamp_int(20000 + depth * 3 + 50000 * NP.random(), 100, 200000)
    return p95, ops


# -----------------------------
# Flow context / coherent value selection
# -----------------------------

@dataclass
class FlowContext:
    flow_id: str
    state: str
    start_min: int
    trace_id: str
    req_id: str
    host_for_component: Dict[str, str]
    values: Dict[str, Any]


def make_flow_context(flow_id: str, state: str, start_min: int) -> FlowContext:
    fl = FLOW_BY_ID[flow_id]
    traced = bool(fl.get("trace", False))
    trace_id = rand_hex(32) if (SYSTEM["tracing"]["on"] and traced) else ""
    req_id = rand_uuid_str()

    host_for_component: Dict[str, str] = {}
    for cid in set(fl["path"]):
        host_for_component[cid] = choose_host(cid)

    v: Dict[str, Any] = {"trace_id": trace_id, "req_id": req_id}

    if flow_id in ("user_sign_in_success", "user_sign_in_new_record"):
        v["method"] = "POST"
        v["path"] = "/login"
        v["github_id"] = int(NP.integers(1, 400000000))
        if flow_id == "user_sign_in_new_record":
            v["user_id"] = int(NP.integers(6000000, 7000001))
            v["seq_last_value"] = int(min(8000000, v["user_id"] + int(NP.integers(10, 1500))))
            v["profile_state"] = "blank"
        else:
            v["user_id"] = int(NP.integers(1, 5000000))
            v["profile_state"] = "complete"
        v["token_id"] = rand_uuid_str()
        v["exp_s"] = int(NP.integers(3600, 2592000))

    elif flow_id in ("load_dashboard_success", "load_dashboard_blank_profile", "api_request_with_stale_token"):
        v["method"] = "GET"
        v["path"] = "/dashboard"
        v["user_id"] = int(NP.integers(1, 7000001))

        if flow_id == "load_dashboard_success":
            v["repo_count"] = int(NP.integers(1, 2000))
            v["_pg_rows_hint"] = int(NP.integers(5, 2000))
        elif flow_id == "load_dashboard_blank_profile":
            v["reason"] = "db_empty"
            v["_web_blank_reason"] = "api_empty_response"
            v["_pg_rows_hint"] = int(NP.integers(0, 2))
        else:
            # Stale-token flow: UI user_id and JWT subject must align within the same request instance.
            stale_user_id = int(NP.integers(6000000, 7000001))
            v["user_id"] = stale_user_id
            v["token_id"] = rand_uuid_str()
            v["sub_user_id"] = stale_user_id

            if NP.random() < 0.7:
                v["db_user_id"] = 0
            else:
                dbid = int(NP.integers(1, 7000001))
                if dbid == v["sub_user_id"]:
                    dbid = 0
                v["db_user_id"] = dbid
            v["action"] = "force_logout"
            v["_forced_logout_reason"] = "jwt_user_mismatch"
            v["_pg_rows_hint"] = int(NP.integers(0, 50))

    elif flow_id in ("webhook_build_trigger_success", "webhook_build_trigger_db_error", "webhook_build_trigger_success_degraded"):
        v["method"] = "POST"
        v["path"] = "/webhook"
        v["event"] = weighted_choice(["push", "pull_request", "cron"], [0.75, 0.2, 0.05])
        v["repo_id"] = int(NP.integers(1, 90000001))
        v["build_id"] = int(NP.integers(1, 200000001))
        v["queue"] = weighted_choice(["builds-ec2", "builds-gce", "builds-macos"], [0.7, 0.25, 0.05])
        v["job_id"] = int(NP.integers(1, 400000001))
        if flow_id == "webhook_build_trigger_db_error":
            v["_pg_rows_hint"] = int(NP.integers(0, 2))
            v["sqlstate"] = weighted_choice(["23503", "23505", "40001"], [0.7, 0.25, 0.05])
            v["err"] = "PG::ForeignKeyViolation: insert or update on table \"builds\" violates foreign key constraint"
        else:
            v["_pg_rows_hint"] = int(NP.integers(1, 20))

    elif flow_id in ("cron_trigger_enqueue", "cron_tick_zero_candidates"):
        v["window_min"] = int(NP.integers(5, 31))
        v["queue"] = weighted_choice(["builds-ec2", "builds-gce", "builds-macos"], [0.7, 0.25, 0.05])
        v["repo_id"] = int(NP.integers(1, 90000001))
        v["build_id"] = int(NP.integers(1, 200000001))
        if flow_id == "cron_tick_zero_candidates":
            v["candidates"] = int(NP.integers(0, 2))
            v["enqueued"] = 0
            v["_pg_rows_hint"] = int(NP.integers(0, 2))
        else:
            v["_pg_rows_hint"] = int(NP.integers(1, 300))

    elif flow_id in ("worker_job_success", "worker_job_db_error", "worker_job_slow_success"):
        v["queue"] = weighted_choice(["builds-ec2", "builds-gce", "builds-macos"], [0.7, 0.25, 0.05])
        v["job_id"] = int(NP.integers(1, 400000001))
        v["build_id"] = int(NP.integers(1, 200000001))
        v["repo_id"] = int(NP.integers(1, 90000001))
        if flow_id in ("worker_job_success", "worker_job_slow_success"):
            v["result"] = weighted_choice(["passed", "failed", "errored"], [0.85, 0.12, 0.03])
        else:
            v["err"] = "ActiveRecord::RecordNotFound: Couldn't find Build with 'id'=" + str(v["build_id"])
        v["_pg_rows_hint"] = int(NP.integers(0, 5 if state == "f" else 200))

    elif flow_id in ("github_user_sync", "github_user_sync_recreate_users", "github_user_sync_ok"):
        v["github_id"] = int(NP.integers(1, 400000000))
        v["endpoint"] = weighted_choice(["/user", "/user/orgs", "/rate_limit"], [0.5, 0.45, 0.05])
        v["status"] = weighted_choice([200, 304], [0.9, 0.1])
        v["remaining"] = int(NP.integers(0, 5001))
        v["_pg_rows_hint"] = int(NP.integers(0, 3 if state == "f" else 200))
        v["upserts"] = int(NP.integers(0, 4000))
        if flow_id == "github_user_sync_recreate_users":
            v["reason"] = weighted_choice(["db_empty", "user_row_missing", "org_row_missing"], [0.6, 0.3, 0.1])
            v["user_id"] = int(NP.integers(6000000, 7000001))

    elif flow_id == "api_offline_503":
        v["route"] = weighted_choice(["web_app", "api_service"], [0.6, 0.4])
        v["req_id"] = rand_uuid_str()

    return FlowContext(flow_id=flow_id, state=state, start_min=start_min, trace_id=trace_id, req_id=req_id, host_for_component=host_for_component, values=v)


# -----------------------------
# Log emission
# -----------------------------

def emit_log(
    ts: datetime,
    component_id: str,
    log_id: str,
    state: str,
    trace_id: str,
    host_override: Optional[str],
    flow_ctx: Optional[FlowContext],
    extra_ctx: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    tmpl = get_template(component_id, log_id)
    msg_t = tmpl["msg"]
    vars_def = tmpl.get("vars", {}) or {}
    state_vars_def = tmpl.get("state_vars", {}) or {}
    placeholders = PLACEHOLDER_RE.findall(msg_t)

    ctx_vals: Dict[str, Any] = {}
    if flow_ctx is not None:
        ctx_vals.update(flow_ctx.values)
    if extra_ctx:
        ctx_vals.update(extra_ctx)

    resolved: Dict[str, Any] = {}

    if component_id == "postgres_primary" and flow_ctx is not None:
        if flow_ctx.flow_id.startswith("cron_"):
            ctx_vals.setdefault("app", "build-scheduler")
        elif flow_ctx.flow_id.startswith("worker_"):
            ctx_vals.setdefault("app", "worker")
        elif flow_ctx.flow_id.startswith("github_user_sync"):
            ctx_vals.setdefault("app", "github-syncer")
        else:
            ctx_vals.setdefault("app", "api-service")

    for name in placeholders:
        if name in ctx_vals:
            resolved[name] = ctx_vals[name]
            continue

        d = None
        if state_vars_def and name in (state_vars_def.get(state, {}) or {}):
            d = state_vars_def[state][name]
        elif name in vars_def:
            d = vars_def[name]

        if d is None:
            raise KeyError(f"Missing variable definition for placeholder '{name}' in {component_id}.{log_id}")

        k = d["k"]
        v = d["v"]

        if component_id == "postgres_primary" and log_id == "sql_exec" and name == "rows":
            hint = ctx_vals.get("_pg_rows_hint")
            if hint is not None:
                lo, hi = int(v[0]), int(v[1])
                resolved[name] = int(max(lo, min(hi, hint)))
                continue

        if component_id == "postgres_primary" and name == "duration_ms" and "_last_delay_ms" in ctx_vals:
            gap = float(ctx_vals["_last_delay_ms"])
            cap = max(1.0, min(5000.0, gap * 1.3))
            resolved[name] = clamp_int(0.6 * cap * NP.random() + 1, int(v[0]), int(min(v[1], cap)))
            continue

        if component_id == "worker_pool" and log_id == "job_finish" and name == "duration_s" and "_job_duration_ms" in ctx_vals:
            dur_s = int(max(10, float(ctx_vals["_job_duration_ms"]) / 1000.0))
            resolved[name] = clamp_int(dur_s, int(v[0]), int(v[1]))
            continue

        if component_id == "job_queue" and log_id == "queue_depth" and name == "depth" and "_minute" in ctx_vals:
            resolved[name] = estimate_queue_depth(int(ctx_vals["_minute"]), state)
            continue

        if component_id == "job_queue" and log_id == "redis_latency_warn" and name in ("p95_ms", "ops_s") and "_minute" in ctx_vals:
            depth = estimate_queue_depth(int(ctx_vals["_minute"]), state)
            p95, ops = estimate_redis_latency(depth, state)
            resolved[name] = p95 if name == "p95_ms" else ops
            continue

        if component_id == "api_service" and log_id == "healthcheck" and name == "db" and "_minute" in ctx_vals:
            m = int(ctx_vals["_minute"])
            if state == "n":
                resolved[name] = "ok"
            else:
                resolved[name] = "degraded" if (35 <= m < 45 or NP.random() < 0.3) else "ok"
            continue

        if component_id == "worker_pool" and log_id == "db_reconnect_attempt" and name == "outcome" and "_minute" in ctx_vals:
            m = int(ctx_vals["_minute"])
            if state == "n":
                resolved[name] = weighted_choice(["success", "timeout", "refused"], [0.95, 0.04, 0.01])
            else:
                if 35 <= m < 45:
                    resolved[name] = weighted_choice(["success", "timeout", "refused"], [0.4, 0.5, 0.1])
                else:
                    resolved[name] = weighted_choice(["success", "timeout", "refused"], [0.65, 0.3, 0.05])
            continue

        if component_id == "postgres_primary" and log_id == "restore_progress" and name in ("pct", "eta_min") and "_minute" in ctx_vals:
            m = int(ctx_vals["_minute"])
            frac = 0.0 if m < 38 else (min(1.0, (m - 38) / 7.0))
            pct = clamp_int(frac * 100 + 3 * (NP.random() - 0.5), 0, 100)
            eta = clamp_int((1.0 - frac) * 30 + 3 * NP.random(), 0, 600)
            resolved[name] = pct if name == "pct" else eta
            continue

        if component_id == "postgres_primary" and log_id in ("restore_progress", "restore_started", "restore_completed") and name == "snapshot_id":
            resolved[name] = snapshot_id()
            continue

        resolved[name] = sample_from_domain(k, v)

    if component_id == "web_app" and log_id == "render_blank_profile":
        if flow_ctx and "_web_blank_reason" in flow_ctx.values:
            resolved["reason"] = flow_ctx.values["_web_blank_reason"]
    if component_id == "web_app" and log_id == "forced_logout":
        if flow_ctx and "_forced_logout_reason" in flow_ctx.values:
            resolved["reason"] = flow_ctx.values["_forced_logout_reason"]

    message = msg_t.format(**resolved)

    return {
        "_ts": ts,
        "timestamp": iso_z(ts),
        "level": tmpl["lvl"],
        "message": message[:1000],
        "trace_id": trace_id if trace_id else "",
        "service": comp_service(component_id),
        "host": host_override if host_override is not None else choose_host(component_id),
    }


# -----------------------------
# Simulation
# -----------------------------

def simulate() -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []

    n_end = int(SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"])
    total = int(SCENARIO["scenario"]["time"]["total_minutes"])

    bg_sources: Dict[str, Dict[str, List[Dict[str, Any]]]] = {"n": {}, "f": {}}
    for comp in SYSTEM["components"]:
        cid = comp["id"]
        for st in ["n", "f"]:
            bg_sources[st][cid] = comp.get("beh", {}).get(st, {}).get("emit", []) or []

    for minute in range(total):
        state = "n" if minute < n_end else "f"
        minute_start = BASE_TIME + timedelta(minutes=minute)

        if state == "f" and minute in EVENTS_BY_MIN:
            apply_failure_event(minute, rows)

        # ---- Flows ----
        flows = SYSTEM["flows"][state]["req"]
        for fl in flows:
            flow_id = fl["id"]
            rpm = float(fl["rpm"])
            if state == "f":
                rpm *= float(ACTIVE.flow_rate.get(flow_id, 1.0))
            if rpm <= 0:
                continue

            n_req = int(NP.poisson(rpm))
            if n_req <= 0:
                continue

            lat_p50_mult, lat_p95_mult = (1.0, 1.0)
            if state == "f":
                lat_p50_mult, lat_p95_mult = ACTIVE.flow_latency.get(flow_id, (1.0, 1.0))

            for _ in range(n_req):
                start_offset_s = float(NP.random() * 60.0)
                t = minute_start + timedelta(seconds=start_offset_s, milliseconds=float(NP.integers(0, 1000)))

                ctx = make_flow_context(flow_id=flow_id, state=state, start_min=minute)

                def host_for(cid: str) -> str:
                    return ctx.host_for_component.get(cid, choose_host(cid))

                prev_ts = t
                for i, ref in enumerate(fl["emit"]):
                    comp_id, log_id = ref.split(".", 1)
                    base_p50, base_p95 = fl["latency_ms"][i]
                    eff_p50 = float(base_p50) * lat_p50_mult
                    eff_p95 = float(base_p95) * lat_p95_mult
                    delay_ms = sample_lognormal_ms(eff_p50, eff_p95)

                    ts = prev_ts + timedelta(milliseconds=delay_ms)
                    prev_ts = ts

                    extra_ctx: Dict[str, Any] = {"_minute": minute, "_last_delay_ms": delay_ms}

                    if comp_id == "worker_pool" and log_id == "job_finish":
                        extra_ctx["_job_duration_ms"] = delay_ms

                    if comp_id == "api_service" and log_id == "jwt_issued":
                        ctx.values.setdefault("token_id", ctx.values.get("token_id", rand_uuid_str()))
                        ctx.values.setdefault("user_id", ctx.values.get("user_id", int(NP.integers(1, 7000001))))
                    if comp_id == "web_app" and log_id == "session_established":
                        ctx.values.setdefault("token_id", ctx.values.get("token_id", rand_uuid_str()))
                        ctx.values.setdefault("user_id", ctx.values.get("user_id", int(NP.integers(1, 7000001))))
                    if comp_id == "api_service" and log_id == "authn_from_jwt":
                        ctx.values.setdefault("token_id", ctx.values.get("token_id", rand_uuid_str()))
                        ctx.values.setdefault("sub_user_id", ctx.values.get("sub_user_id", int(NP.integers(1, 7000001))))
                    if comp_id == "api_service" and log_id == "jwt_user_mismatch":
                        ctx.values.setdefault("token_id", ctx.values.get("token_id", rand_uuid_str()))
                        sub = int(ctx.values.get("sub_user_id", int(NP.integers(1, 7000001))))
                        ctx.values["sub_user_id"] = sub
                        dbid = int(ctx.values.get("db_user_id", 0))
                        if dbid == sub:
                            dbid = 0
                        ctx.values["db_user_id"] = dbid
                        ctx.values.setdefault("action", "force_logout")
                    if comp_id == "web_app" and log_id == "forced_logout":
                        ctx.values.setdefault("token_id", ctx.values.get("token_id", rand_uuid_str()))

                    if comp_id == "edge_router" and log_id == "request_received":
                        ctx.values.setdefault("client_ip", sample_ip("0.0.0.0/0"))

                    if comp_id == "api_service" and log_id == "enqueue_failed_db":
                        ctx.values.setdefault("repo_id", int(NP.integers(1, 90000001)))
                        ctx.values.setdefault("err", "PG::ForeignKeyViolation: insert or update on table \"builds\" violates foreign key constraint")

                    row = emit_log(
                        ts=ts,
                        component_id=comp_id,
                        log_id=log_id,
                        state=state,
                        trace_id=ctx.trace_id,
                        host_override=host_for(comp_id),
                        flow_ctx=ctx,
                        extra_ctx=extra_ctx,
                    )
                    rows.append(row)

        # ---- Background emissions ----
        for comp in SYSTEM["components"]:
            cid = comp["id"]
            emits = bg_sources[state][cid]
            if not emits:
                continue

            for e in emits:
                log_id = e["id"]
                base_per_min = float(e["per_min"])
                scope = e.get("scope", "per_host")

                mult = 1.0
                if state == "f":
                    mult = float(ACTIVE.bg_rate.get(f"{cid}.{log_id}", 1.0))

                eff_per_min = base_per_min * mult
                if eff_per_min <= 0:
                    continue

                hosts = COMP_BY_ID[cid]["hosts"]
                if scope == "global":
                    k = int(NP.poisson(eff_per_min))
                    for _ in range(k):
                        offset = float(NP.random() * 60.0)
                        ts = minute_start + timedelta(seconds=offset, milliseconds=float(NP.integers(0, 1000)))
                        host = choose_host(cid) if hosts else ""
                        row = emit_log(
                            ts=ts,
                            component_id=cid,
                            log_id=log_id,
                            state=state,
                            trace_id="",
                            host_override=host,
                            flow_ctx=None,
                            extra_ctx={"_minute": minute},
                        )
                        rows.append(row)
                else:
                    host_list = hosts if hosts else [""]
                    for h in host_list:
                        k = int(NP.poisson(eff_per_min))
                        for _ in range(k):
                            offset = float(NP.random() * 60.0)
                            ts = minute_start + timedelta(seconds=offset, milliseconds=float(NP.integers(0, 1000)))
                            row = emit_log(
                                ts=ts,
                                component_id=cid,
                                log_id=log_id,
                                state=state,
                                trace_id="",
                                host_override=h,
                                flow_ctx=None,
                                extra_ctx={"_minute": minute},
                            )
                            rows.append(row)

    rows.sort(key=lambda r: r["_ts"])
    for r in rows:
        r.pop("_ts", None)

    return pd.DataFrame(rows, columns=["timestamp", "level", "message", "trace_id", "service", "host"])


def main() -> None:
    df = simulate()
    df.to_csv("logs.csv", index=False)


if __name__ == "__main__":
    main()
