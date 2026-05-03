import math
import hashlib
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional

import numpy as np
import pandas as pd

# -----------------------------
# Embedded normalized model data
# -----------------------------
SYSTEM: Dict[str, Any] = {
    "sys": {"id": "billing_inflight_balance_system"},
    "states": {"n": "normal", "f": "failure"},
    "tracing": {"on": False, "trace_id": {"k": "hex", "v": 32}},
    "components": [
        {
            "id": "usage_ingest",
            "svc": "usage-ingest",
            "hosts": ["ingest-1", "ingest-2"],
            "logs": {
                "billable_event": {
                    "lvl": "INFO",
                    "msg": "billable event {event_id} account={acct} type={usage_type} units={units}",
                    "vars": {
                        "event_id": {"k": "uuid", "v": None},
                        "acct": {"k": "hex", "v": 8},
                        "usage_type": {"k": "ch", "v": ["sms", "voice"]},
                        "units": {"k": "i", "v": [1, 5]},
                    },
                },
                "ingest_backlog": {
                    "lvl": "INFO",
                    "msg": "ingest backlog queue_depth={depth} oldest_event_s={age_s}",
                    "vars": {
                        "depth": {"k": "i", "v": [0, 20000]},
                        "age_s": {"k": "i", "v": [0, 300]},
                    },
                },
            },
            "beh": {
                "n": {"emit": [{"id": "ingest_backlog", "per_min": 1.0}]},
                "f": {"emit": [{"id": "ingest_backlog", "per_min": 1.0}]},
            },
        },
        {
            "id": "billing_api",
            "svc": "billing-api",
            "hosts": ["bill-1", "bill-2", "bill-3", "bill-4"],
            "logs": {
                "apply_charge_start": {
                    "lvl": "INFO",
                    "msg": "apply charge start event={event_id} account={acct} amount_cents={amount_cents}",
                    "vars": {
                        "event_id": {"k": "uuid", "v": None},
                        "acct": {"k": "hex", "v": 8},
                        "amount_cents": {"k": "i", "v": [1, 50]},
                    },
                },
                "balance_update_ok": {
                    "lvl": "INFO",
                    "msg": "inflight balance updated account={acct} delta_cents={delta_cents} new_balance_cents={new_balance_cents}",
                    "vars": {
                        "acct": {"k": "hex", "v": 8},
                        "delta_cents": {"k": "i", "v": [-5000, 5000]},
                        "new_balance_cents": {"k": "i", "v": [0, 20000]},
                    },
                },
                "redis_get_timeout": {
                    "lvl": "WARN",
                    "msg": "redis GET balance timeout account={acct} waited_ms={waited_ms}",
                    "vars": {
                        "acct": {"k": "hex", "v": 8},
                        "waited_ms": {"k": "i", "v": [100, 2000]},
                    },
                },
                "balance_write_readonly": {
                    "lvl": "ERROR",
                    "msg": "redis SET balance failed (READONLY) account={acct} delta_cents={delta_cents}",
                    "vars": {
                        "acct": {"k": "hex", "v": 8},
                        "delta_cents": {"k": "i", "v": [-5000, 5000]},
                    },
                },
                "pricing_deferred_timeout": {
                    "lvl": "WARN",
                    "msg": "pricing deferred account={acct} reason=redis_timeout queued_event={event_id}",
                    "vars": {
                        "acct": {"k": "hex", "v": 8},
                        "event_id": {"k": "uuid", "v": None},
                    },
                },
                "pricing_deferred_offline": {
                    "lvl": "WARN",
                    "msg": "pricing deferred account={acct} reason=system_offline queued_event={event_id}",
                    "vars": {
                        "acct": {"k": "hex", "v": 8},
                        "event_id": {"k": "uuid", "v": None},
                    },
                },
                "charge_applied": {
                    "lvl": "INFO",
                    "msg": "charge applied account={acct} ledger_txn={txn_id}",
                    "vars": {"acct": {"k": "hex", "v": 8}, "txn_id": {"k": "hex", "v": 12}},
                },
                "recharge_requested_zero_balance": {
                    "lvl": "INFO",
                    "msg": "auto-recharge requested account={acct} target_cents={target_cents} reason=zero_balance",
                    "vars": {"acct": {"k": "hex", "v": 8}, "target_cents": {"k": "i", "v": [500, 5000]}},
                },
                "recharge_requested_balance_unwritable": {
                    "lvl": "INFO",
                    "msg": "auto-recharge requested account={acct} target_cents={target_cents} reason=balance_unwritable",
                    "vars": {"acct": {"k": "hex", "v": 8}, "target_cents": {"k": "i", "v": [500, 5000]}},
                },
                "manual_payment_received": {
                    "lvl": "INFO",
                    "msg": "manual payment received account={acct} payment_id={payment_id} amount_cents={amount_cents}",
                    "vars": {
                        "acct": {"k": "hex", "v": 8},
                        "payment_id": {"k": "hex", "v": 12},
                        "amount_cents": {"k": "i", "v": [500, 20000]},
                    },
                },
                "payment_balance_update_failed": {
                    "lvl": "ERROR",
                    "msg": "payment posted but balance not updated account={acct} payment_id={payment_id} err={err}",
                    "vars": {
                        "acct": {"k": "hex", "v": 8},
                        "payment_id": {"k": "hex", "v": 12},
                        "err": {"k": "ch", "v": ["readonly"]},
                    },
                },
                "account_suspended": {
                    "lvl": "WARN",
                    "msg": "account suspended account={acct} reason={reason} payment_id={payment_id}",
                    "vars": {
                        "acct": {"k": "hex", "v": 8},
                        "reason": {"k": "ch", "v": ["card_declined_after_retries"]},
                        "payment_id": {"k": "hex", "v": 12},
                    },
                },
                "pricing_lag_metric": {
                    "lvl": "INFO",
                    "msg": "pricing lag seconds={lag_s} pending_events={pending}",
                    "vars": {},
                    "state_vars": {
                        "n": {"lag_s": {"k": "i", "v": [0, 30]}, "pending": {"k": "i", "v": [0, 5000]}},
                        "f": {"lag_s": {"k": "i", "v": [60, 1800]}, "pending": {"k": "i", "v": [5000, 50000]}},
                    },
                },
                "health_check": {
                    "lvl": "INFO",
                    "msg": "health ok version={version} active_workers={workers}",
                    "vars": {"version": {"k": "ch", "v": ["1.18.0"]}, "workers": {"k": "i", "v": [20, 80]}},
                },
                "billing_mode_changed": {
                    "lvl": "INFO",
                    "msg": "billing mode set to {mode} by={actor}",
                    "vars": {"mode": {"k": "ch", "v": ["online", "offline"]}, "actor": {"k": "ch", "v": ["oncall"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "health_check", "per_min": 2.0}, {"id": "pricing_lag_metric", "per_min": 1.0}]},
                "f": {"emit": [{"id": "health_check", "per_min": 2.0}, {"id": "pricing_lag_metric", "per_min": 1.0}]},
            },
        },
        {
            "id": "auto_recharge_worker",
            "svc": "auto-recharge",
            "hosts": ["recharge-1", "recharge-2"],
            "logs": {
                "recharge_job_start": {
                    "lvl": "INFO",
                    "msg": "recharge job start account={acct} payment_id={payment_id} amount_cents={amount_cents}",
                    "vars": {
                        "acct": {"k": "hex", "v": 8},
                        "payment_id": {"k": "hex", "v": 12},
                        "amount_cents": {"k": "i", "v": [500, 5000]},
                    },
                },
                "recharge_job_result_ok": {
                    "lvl": "INFO",
                    "msg": "recharge job result account={acct} payment_id={payment_id} gateway_status=approved error=none",
                    "vars": {"acct": {"k": "hex", "v": 8}, "payment_id": {"k": "hex", "v": 12}},
                },
                "recharge_job_result_balance_write_failed": {
                    "lvl": "INFO",
                    "msg": "recharge job result account={acct} payment_id={payment_id} gateway_status=approved error=balance_write_failed",
                    "vars": {"acct": {"k": "hex", "v": 8}, "payment_id": {"k": "hex", "v": 12}},
                },
                "recharge_job_result_declined": {
                    "lvl": "INFO",
                    "msg": "recharge job result account={acct} payment_id={payment_id} gateway_status=declined error=card_declined",
                    "vars": {"acct": {"k": "hex", "v": 8}, "payment_id": {"k": "hex", "v": 12}},
                },
                "worker_heartbeat": {
                    "lvl": "INFO",
                    "msg": "worker heartbeat inflight_jobs={jobs}",
                    "vars": {"jobs": {"k": "i", "v": [0, 500]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "worker_heartbeat", "per_min": 1.0}]},
                "f": {"emit": [{"id": "worker_heartbeat", "per_min": 1.0}]},
            },
        },
        {
            "id": "payment_gateway",
            "svc": None,
            "hosts": ["gateway"],
            "logs": {
                "charge_request": {
                    "lvl": "INFO",
                    "msg": "charge request payment_id={payment_id} account={acct} amount_cents={amount_cents} card_last4={last4}",
                    "vars": {
                        "payment_id": {"k": "hex", "v": 12},
                        "acct": {"k": "hex", "v": 8},
                        "amount_cents": {"k": "i", "v": [500, 20000]},
                        "last4": {"k": "i", "v": [1000, 9999]},
                    },
                },
                "charge_response_approved": {
                    "lvl": "INFO",
                    "msg": "charge response payment_id={payment_id} status=approved auth_code={auth_code}",
                    "vars": {"payment_id": {"k": "hex", "v": 12}, "auth_code": {"k": "hex", "v": 6}},
                },
                "charge_response_declined": {
                    "lvl": "INFO",
                    "msg": "charge response payment_id={payment_id} status=declined decline_code={decline_code}",
                    "vars": {
                        "payment_id": {"k": "hex", "v": 12},
                        "decline_code": {"k": "ch", "v": ["do_not_honor", "lost_card"]},
                    },
                },
            },
            "beh": {"n": {"emit": []}, "f": {"emit": []}},
        },
        {
            "id": "redis_master",
            "svc": "billing-redis",
            "hosts": ["redis-master-1"],
            "logs": {
                "stats": {
                    "lvl": "INFO",
                    "msg": "stats used_memory_mb={mem_mb} connected_slaves={slaves} ops_per_sec={ops}",
                    "vars": {
                        "mem_mb": {"k": "i", "v": [0, 12000]},
                        "slaves": {"k": "i", "v": [0, 3]},
                        "ops": {"k": "i", "v": [500, 12000]},
                    },
                },
                "replica_sync_start": {
                    "lvl": "INFO",
                    "msg": "replica sync start replica={replica} runid={runid}",
                    "vars": {
                        "replica": {"k": "ch", "v": ["redis-replica-1", "redis-replica-2", "redis-replica-3"]},
                        "runid": {"k": "hex", "v": 40},
                    },
                },
                "replica_sync_backlog": {
                    "lvl": "WARN",
                    "msg": "replica sync backlog bytes={bytes} replicas_in_sync={replicas_in_sync} cpu_pct={cpu_pct}",
                    "vars": {
                        "bytes": {"k": "i", "v": [10000000, 400000000]},
                        "replicas_in_sync": {"k": "i", "v": [1, 3]},
                        "cpu_pct": {"k": "i", "v": [20, 100]},
                    },
                },
                "command_timeout": {
                    "lvl": "WARN",
                    "msg": "client timeout cmd={cmd} client={client} blocked_ms={blocked_ms}",
                    "vars": {
                        "cmd": {"k": "ch", "v": ["GET", "SET", "MULTI", "EXEC"]},
                        "client": {"k": "ch", "v": ["billing_api", "replica"]},
                        "blocked_ms": {"k": "i", "v": [50, 5000]},
                    },
                },
                "restart": {
                    "lvl": "INFO",
                    "msg": "redis-server restarting pid={pid} config={config}",
                    "vars": {"pid": {"k": "i", "v": [1000, 65000]}, "config": {"k": "ch", "v": ["/etc/redis/redis-slave.conf"]}},
                },
                "aof_open_failed": {
                    "lvl": "ERROR",
                    "msg": "AOF open failed file={file} err={err}",
                    "vars": {"file": {"k": "ch", "v": ["/var/lib/redis/appendonly.aof"]}, "err": {"k": "ch", "v": ["No such file or directory"]}},
                },
                "db_loaded": {
                    "lvl": "INFO",
                    "msg": "RDB loaded keys={keys} expires={expires} mem_mb={mem_mb}",
                    "vars": {"keys": {"k": "i", "v": [0, 2000]}, "expires": {"k": "i", "v": [0, 100]}, "mem_mb": {"k": "i", "v": [0, 200]}},
                },
                "role_changed": {
                    "lvl": "WARN",
                    "msg": "role set to {role} master_host={master_host} master_port={master_port} readonly={readonly}",
                    "vars": {
                        "role": {"k": "ch", "v": ["slave"]},
                        "master_host": {"k": "ch", "v": ["127.0.0.1"]},
                        "master_port": {"k": "i", "v": [6379, 6379]},
                        "readonly": {"k": "ch", "v": ["yes"]},
                    },
                },
                "role_status": {
                    "lvl": "INFO",
                    "msg": "role status role={role} readonly={readonly} master={master}",
                    "vars": {"role": {"k": "ch", "v": ["slave"]}, "readonly": {"k": "ch", "v": ["yes"]}, "master": {"k": "ch", "v": ["127.0.0.1:6379"]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "stats", "per_min": 1.0}, {"id": "command_timeout", "per_min": 0.1}]},
                "f": {
                    "emit": [
                        {"id": "stats", "per_min": 1.0},
                        {"id": "replica_sync_start", "per_min": 0.2},
                        {"id": "replica_sync_backlog", "per_min": 0.3},
                        {"id": "command_timeout", "per_min": 0.3},
                        {"id": "role_status", "per_min": 2.0},
                    ]
                },
            },
        },
        {
            "id": "redis_replicas",
            "svc": "billing-redis",
            "hosts": ["redis-replica-1", "redis-replica-2", "redis-replica-3"],
            "logs": {
                "link_down": {
                    "lvl": "WARN",
                    "msg": "replica link down replica={replica} master={master} reason={reason}",
                    "vars": {
                        "replica": {"k": "ch", "v": ["redis-replica-1", "redis-replica-2", "redis-replica-3"]},
                        "master": {"k": "ch", "v": ["redis-master-1"]},
                        "reason": {"k": "ch", "v": ["network_unreachable", "connection_reset"]},
                    },
                },
                "fullsync_requested": {
                    "lvl": "INFO",
                    "msg": "replica requesting full resync replica={replica} offset={offset}",
                    "vars": {"replica": {"k": "ch", "v": ["redis-replica-1", "redis-replica-2", "redis-replica-3"]}, "offset": {"k": "i", "v": [0, 999999999]}},
                },
            },
            "beh": {
                "n": {"emit": [{"id": "link_down", "per_min": 0.01}]},
                "f": {"emit": [{"id": "link_down", "per_min": 0.2}, {"id": "fullsync_requested", "per_min": 0.2}]},
            },
        },
        {
            "id": "ledger_db",
            "svc": "billing-ledger",
            "hosts": ["ledger-1", "ledger-2"],
            "logs": {
                "ledger_write_ok": {
                    "lvl": "INFO",
                    "msg": "ledger write ok txn={txn_id} account={acct} amount_cents={amount_cents} source={source}",
                    "vars": {"txn_id": {"k": "hex", "v": 12}, "acct": {"k": "hex", "v": 8}, "amount_cents": {"k": "i", "v": [-5000, 20000]}, "source": {"k": "ch", "v": ["usage", "auto_recharge", "manual_payment"]}},
                },
                "ledger_write_slow": {
                    "lvl": "WARN",
                    "msg": "ledger write slow txn={txn_id} took_ms={took_ms} account={acct}",
                    "vars": {"txn_id": {"k": "hex", "v": 12}, "took_ms": {"k": "i", "v": [200, 5000]}, "acct": {"k": "hex", "v": 8}},
                },
                "db_health": {
                    "lvl": "INFO",
                    "msg": "db pool ok active_conns={active} waiters={waiters}",
                    "vars": {"active": {"k": "i", "v": [1, 80]}, "waiters": {"k": "i", "v": [0, 30]}},
                },
            },
            "beh": {"n": {"emit": [{"id": "db_health", "per_min": 0.5}, {"id": "ledger_write_slow", "per_min": 0.05}]}, "f": {"emit": [{"id": "db_health", "per_min": 0.5}, {"id": "ledger_write_slow", "per_min": 0.1}]}},
        },
        {
            "id": "billing_monitor",
            "svc": "billing-monitor",
            "hosts": ["mon-1"],
            "logs": {
                "monitor_tick": {"lvl": "DEBUG", "msg": "monitor tick checks={checks}", "vars": {"checks": {"k": "i", "v": [5, 30]}}},
                "anomaly_detected": {"lvl": "ERROR", "msg": "billing anomaly detected metric={metric} value={value} threshold={threshold} window_min={window_min}", "vars": {"metric": {"k": "ch", "v": ["charges_per_account", "suspended_accounts"]}, "value": {"k": "i", "v": [5, 500]}, "threshold": {"k": "i", "v": [3, 100]}, "window_min": {"k": "i", "v": [1, 10]}}},
                "alert_sent": {"lvl": "CRITICAL", "msg": "pager alert sent incident={incident} service={service} summary={summary}", "vars": {"incident": {"k": "hex", "v": 6}, "service": {"k": "ch", "v": ["billing", "redis"]}, "summary": {"k": "str", "v": "short_summary"}}},
                "refund_job_started": {"lvl": "INFO", "msg": "refund processing started batch={batch} count={count}", "vars": {"batch": {"k": "hex", "v": 8}, "count": {"k": "i", "v": [50, 5000]}}},
                "refund_progress": {"lvl": "INFO", "msg": "refund progress batch={batch} processed={processed} remaining={remaining}", "vars": {"batch": {"k": "hex", "v": 8}, "processed": {"k": "i", "v": [0, 20000]}, "remaining": {"k": "i", "v": [0, 20000]}}},
            },
            "beh": {
                "n": {"emit": [{"id": "monitor_tick", "per_min": 1.0, "scope": "global"}]},
                "f": {"emit": [{"id": "monitor_tick", "per_min": 1.0, "scope": "global"}, {"id": "refund_progress", "per_min": 2.0, "scope": "global"}]},
            },
        },
    ],
    "flows": {
        "n": [
            {
                "id": "usage_billable_item_n",
                "rpm": 250.0,
                "emit": ["usage_ingest.billable_event", "billing_api.apply_charge_start", "billing_api.balance_update_ok", "ledger_db.ledger_write_ok", "billing_api.charge_applied"],
                "latency_ms": [[1, 4], [2, 8], [3, 15], [5, 25], [1, 5]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "customer_payment_n",
                "rpm": 2.0,
                "emit": ["billing_api.manual_payment_received", "payment_gateway.charge_request", "payment_gateway.charge_response_approved", "billing_api.balance_update_ok", "ledger_db.ledger_write_ok", "billing_api.charge_applied"],
                "latency_ms": [[2, 10], [20, 80], [50, 200], [5, 30], [10, 60], [2, 10]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "auto_recharge_charge_n",
                "rpm": 5.0,
                "emit": ["auto_recharge_worker.recharge_job_start", "payment_gateway.charge_request", "payment_gateway.charge_response_approved", "billing_api.balance_update_ok", "ledger_db.ledger_write_ok", "auto_recharge_worker.recharge_job_result_ok"],
                "latency_ms": [[2, 10], [20, 80], [50, 200], [5, 40], [10, 80], [2, 20]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
        "f": [
            {
                "id": "usage_billable_item_f_slow_ok",
                "rpm": 240.0,
                "emit": ["usage_ingest.billable_event", "billing_api.apply_charge_start", "billing_api.balance_update_ok", "ledger_db.ledger_write_ok", "billing_api.charge_applied"],
                "latency_ms": [[1, 4], [5, 25], [15, 200], [15, 150], [5, 25]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "usage_billable_item_f_timeout",
                "rpm": 10.0,
                "emit": ["usage_ingest.billable_event", "billing_api.apply_charge_start", "billing_api.redis_get_timeout", "billing_api.pricing_deferred_timeout"],
                "latency_ms": [[1, 4], [5, 25], [200, 1200], [1, 5]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "usage_billable_item_f_readonly_no_recharge",
                "rpm": 245.0,
                "emit": ["usage_ingest.billable_event", "billing_api.apply_charge_start", "billing_api.balance_write_readonly", "ledger_db.ledger_write_ok"],
                "latency_ms": [[1, 4], [3, 12], [3, 25], [10, 70]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "usage_billable_item_f_readonly_autorecharge",
                "rpm": 5.0,
                "emit": ["usage_ingest.billable_event", "billing_api.apply_charge_start", "billing_api.balance_write_readonly", "ledger_db.ledger_write_ok", "billing_api.recharge_requested_zero_balance"],
                "latency_ms": [[1, 4], [3, 12], [3, 25], [10, 70], [2, 12]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "usage_billable_item_f_readonly_autorecharge_unwritable",
                "rpm": 5.0,
                "emit": ["usage_ingest.billable_event", "billing_api.apply_charge_start", "billing_api.balance_write_readonly", "ledger_db.ledger_write_ok", "billing_api.recharge_requested_balance_unwritable"],
                "latency_ms": [[1, 4], [3, 12], [3, 25], [10, 70], [2, 12]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "usage_billable_item_f_offline",
                "rpm": 250.0,
                "emit": ["usage_ingest.billable_event", "billing_api.pricing_deferred_offline"],
                "latency_ms": [[1, 4], [2, 10]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "customer_payment_f_readonly",
                "rpm": 2.0,
                "emit": ["billing_api.manual_payment_received", "payment_gateway.charge_request", "payment_gateway.charge_response_approved", "billing_api.payment_balance_update_failed", "ledger_db.ledger_write_ok"],
                "latency_ms": [[2, 10], [20, 80], [50, 250], [3, 20], [10, 80]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "auto_recharge_charge_f_readonly",
                "rpm": 5.0,
                "emit": ["auto_recharge_worker.recharge_job_start", "payment_gateway.charge_request", "payment_gateway.charge_response_approved", "billing_api.balance_write_readonly", "ledger_db.ledger_write_ok", "auto_recharge_worker.recharge_job_result_balance_write_failed"],
                "latency_ms": [[2, 10], [20, 80], [50, 250], [3, 25], [10, 80], [2, 20]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
            {
                "id": "auto_recharge_declined_suspend_f",
                "rpm": 0.2,
                "emit": ["auto_recharge_worker.recharge_job_start", "payment_gateway.charge_request", "payment_gateway.charge_response_declined", "billing_api.account_suspended", "auto_recharge_worker.recharge_job_result_declined"],
                "latency_ms": [[2, 10], [20, 80], [50, 250], [2, 20], [2, 20]],
                "retry": {"max_attempts": 1, "expected_attempts": 1.0, "emit_per_retry": [], "backoff_ms": []},
                "trace": False,
            },
        ],
    },
}

SCENARIO: Dict[str, Any] = {
    "scenario": {
        "id": "billing_redis_readonly_auto_recharge_spike",
        "time": {"total_minutes": 50, "phases": {"n": {"start_min": 0, "end_min": 25}, "f": {"start_min": 25, "end_min": 50}}},
        "phases": {
            "f": {
                "events": [
                    {
                        "order": 1,
                        "at_min": 25,
                        "rate_multipliers": {
                            "redis_replicas.link_down": 20.0,
                            "redis_replicas.fullsync_requested": 20.0,
                            "redis_master.replica_sync_start": 10.0,
                            "redis_master.role_status": 0.0,
                            "billing_monitor.refund_progress": 0.0,
                            "usage_billable_item_f_readonly_no_recharge": 0.0,
                            "usage_billable_item_f_readonly_autorecharge": 0.0,
                            "usage_billable_item_f_readonly_autorecharge_unwritable": 0.0,
                            "usage_billable_item_f_offline": 0.0,
                            "usage_billable_item_f_timeout": 1.0,
                            "customer_payment_f_readonly": 0.0,
                            "auto_recharge_charge_f_readonly": 0.0,
                            "auto_recharge_declined_suspend_f": 0.0,
                        },
                        "latency_multipliers": {"usage_billable_item_f_slow_ok": {"p50": 1.2, "p95": 1.5}},
                        "one_shots": [],
                    },
                    {
                        "order": 2,
                        "at_min": 29,
                        "rate_multipliers": {
                            "redis_replicas.link_down": 2.0,
                            "redis_replicas.fullsync_requested": 2.0,
                            "redis_master.replica_sync_backlog": 8.0,
                            "redis_master.command_timeout": 6.0,
                            "usage_billable_item_f_timeout": 3.0,
                            "usage_billable_item_f_slow_ok": 0.92,
                        },
                        "latency_multipliers": {"usage_billable_item_f_slow_ok": {"p50": 2.0, "p95": 3.0}},
                        "one_shots": [{"ref": "billing_monitor.alert_sent", "count": 1, "hosts": ["mon-1"]}],
                    },
                    {
                        "order": 3,
                        "at_min": 31,
                        "rate_multipliers": {
                            "redis_master.command_timeout": 1.0,
                            "redis_master.role_status": 1.0,
                            "usage_billable_item_f_slow_ok": 0.0,
                            "usage_billable_item_f_timeout": 0.0,
                            "usage_billable_item_f_readonly_no_recharge": 1.0,
                            "usage_billable_item_f_readonly_autorecharge": 1.0,
                            "usage_billable_item_f_readonly_autorecharge_unwritable": 0.0,
                            "customer_payment_f_readonly": 1.0,
                            "auto_recharge_charge_f_readonly": 1.0,
                            "auto_recharge_declined_suspend_f": 1.0,
                        },
                        "latency_multipliers": {
                            "usage_billable_item_f_readonly_no_recharge": {"p50": 1.0, "p95": 1.2},
                            "usage_billable_item_f_readonly_autorecharge": {"p50": 1.0, "p95": 1.2},
                        },
                        "one_shots": [
                            {"ref": "redis_master.restart", "count": 1, "hosts": ["redis-master-1"]},
                            {"ref": "redis_master.aof_open_failed", "count": 1, "hosts": ["redis-master-1"]},
                            {"ref": "redis_master.db_loaded", "count": 1, "hosts": ["redis-master-1"]},
                            {"ref": "redis_master.role_changed", "count": 1, "hosts": ["redis-master-1"]},
                        ],
                    },
                    {
                        "order": 4,
                        "at_min": 36,
                        "rate_multipliers": {"usage_billable_item_f_readonly_autorecharge": 3.0, "auto_recharge_charge_f_readonly": 3.0, "auto_recharge_declined_suspend_f": 1.0},
                        "latency_multipliers": {},
                        "one_shots": [{"ref": "billing_monitor.anomaly_detected", "count": 1, "hosts": ["mon-1"]}, {"ref": "billing_monitor.alert_sent", "count": 1, "hosts": ["mon-1"]}],
                    },
                    {
                        "order": 5,
                        "at_min": 39,
                        "rate_multipliers": {
                            "usage_billable_item_f_readonly_no_recharge": 0.0,
                            "usage_billable_item_f_readonly_autorecharge": 0.0,
                            "usage_billable_item_f_readonly_autorecharge_unwritable": 0.0,
                            "customer_payment_f_readonly": 0.0,
                            "auto_recharge_charge_f_readonly": 0.0,
                            "auto_recharge_declined_suspend_f": 0.0,
                            "usage_billable_item_f_offline": 1.0,
                        },
                        "latency_multipliers": {"usage_billable_item_f_offline": {"p50": 1.0, "p95": 1.0}},
                        "one_shots": [{"ref": "billing_api.billing_mode_changed", "count": 1, "hosts": ["bill-1"]}],
                    },
                    {
                        "order": 6,
                        "at_min": 45,
                        "rate_multipliers": {
                            "usage_billable_item_f_offline": 0.0,
                            "usage_billable_item_f_readonly_no_recharge": 1.0,
                            "usage_billable_item_f_readonly_autorecharge": 0.0,
                            "usage_billable_item_f_readonly_autorecharge_unwritable": 1.0,
                            "auto_recharge_charge_f_readonly": 1.0,
                            "customer_payment_f_readonly": 1.0,
                            "auto_recharge_declined_suspend_f": 1.0,
                        },
                        "latency_multipliers": {
                            "usage_billable_item_f_readonly_no_recharge": {"p50": 1.0, "p95": 1.2},
                            "usage_billable_item_f_readonly_autorecharge_unwritable": {"p50": 1.0, "p95": 1.2},
                        },
                        "one_shots": [{"ref": "billing_api.billing_mode_changed", "count": 1, "hosts": ["bill-2"]}],
                    },
                    {
                        "order": 7,
                        "at_min": 48,
                        "rate_multipliers": {
                            "usage_billable_item_f_readonly_no_recharge": 0.0,
                            "usage_billable_item_f_readonly_autorecharge": 0.0,
                            "usage_billable_item_f_readonly_autorecharge_unwritable": 0.0,
                            "auto_recharge_charge_f_readonly": 0.0,
                            "customer_payment_f_readonly": 0.0,
                            "auto_recharge_declined_suspend_f": 0.0,
                            "usage_billable_item_f_offline": 1.0,
                            "billing_monitor.refund_progress": 1.0,
                        },
                        "latency_multipliers": {},
                        "one_shots": [
                            {"ref": "billing_monitor.anomaly_detected", "count": 1, "hosts": ["mon-1"]},
                            {"ref": "billing_api.billing_mode_changed", "count": 1, "hosts": ["bill-3"]},
                            {"ref": "billing_monitor.refund_job_started", "count": 1, "hosts": ["mon-1"]},
                        ],
                    },
                ]
            }
        },
    }
}

# -----------------------------
# Simulator
# -----------------------------
SEED = 1337
random.seed(SEED)
_rng = np.random.RandomState(SEED)


@dataclass
class Controls:
    rate_mult: Dict[str, float]
    latency_mult: Dict[str, Dict[str, float]]  # flow_id -> {p50:..., p95:...}


def _md5_u32(s: str) -> int:
    return int(hashlib.md5(s.encode("utf-8")).hexdigest()[:8], 16)


def _ts(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _gen_hex(n: int) -> str:
    b = _rng.bytes((n + 1) // 2)
    return b.hex()[:n].lower()


def _gen_uuid4_like() -> str:
    b = bytearray(_rng.bytes(16))
    b[6] = (b[6] & 0x0F) | 0x40
    b[8] = (b[8] & 0x3F) | 0x80
    hx = bytes(b).hex()
    return f"{hx[0:8]}-{hx[8:12]}-{hx[12:16]}-{hx[16:20]}-{hx[20:32]}"


def _choose_domain(dom: Dict[str, Any], key: str) -> Any:
    k = dom["k"]
    v = dom["v"]
    if k == "i":
        lo, hi = int(v[0]), int(v[1])
        if lo == hi:
            return lo
        return int(_rng.randint(lo, hi + 1))
    if k == "f":
        lo, hi = float(v[0]), float(v[1])
        if lo == hi:
            return lo
        return float(lo + (_rng.rand() * (hi - lo)))
    if k == "ch":
        choices = list(v)
        if not choices:
            return ""
        return choices[_md5_u32(f"{key}:{len(choices)}") % len(choices)]
    if k == "uuid":
        return _gen_uuid4_like()
    if k == "hex":
        return _gen_hex(int(v))
    if k == "ip":
        return "127.0.0.1"
    if k == "str":
        hint = str(v)
        if hint == "short_summary":
            options = [
                "redis replication storm causing billing timeouts",
                "redis readonly after restart; auto-recharge spike",
                "billing anomaly recurrence; refunds starting",
            ]
            return options[_md5_u32(f"{key}:{hint}") % len(options)]
        return hint
    return ""


def _sample_latency_ms(p50: float, p95: float, key: str) -> int:
    if p50 <= 0:
        p50 = 1.0
    if p95 < p50:
        p95 = p50
    u = (_md5_u32(key) % 10000) / 10000.0
    q = u**2
    ratio = p95 / p50 if p50 > 0 else 1.0
    val = p50 * (ratio**q)
    return max(1, int(round(val)))


class Allocator:
    def __init__(self):
        self.carry: Dict[str, float] = {}

    def alloc(self, expected: float, key: str) -> int:
        if expected <= 0:
            return 0
        base = int(math.floor(expected))
        frac = expected - base
        c = self.carry.get(key, 0.0)
        total = frac + c
        add = 1 if total >= 1.0 else 0
        self.carry[key] = total - add
        return base + add


def _schedule_even(start: datetime, end: datetime, count: int, key: str) -> List[datetime]:
    if count <= 0:
        return []
    total_ms = int((end - start).total_seconds() * 1000)
    if total_ms <= 0:
        return [start] * count
    out = []
    for i in range(count):
        base_off = int(((i + 0.5) / count) * total_ms)
        jitter = ((_md5_u32(f"{key}:{i}") % 401) - 200)
        off = max(0, min(total_ms - 1, base_off + jitter))
        out.append(start + timedelta(milliseconds=off))
    return out


def _build_indices(system: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    comp_by_id = {c["id"]: c for c in system["components"]}
    log_tpl = {}
    for cid, comp in comp_by_id.items():
        for lid, tpl in comp.get("logs", {}).items():
            log_tpl[f"{cid}.{lid}"] = tpl
    flows_by_state = {"n": {}, "f": {}}
    for st in ["n", "f"]:
        for flow in system["flows"][st]:
            flows_by_state[st][flow["id"]] = flow
    return comp_by_id, log_tpl, flows_by_state


def _failure_intervals(scenario: Dict[str, Any]) -> List[Dict[str, Any]]:
    ph = scenario["scenario"]["time"]["phases"]["f"]
    start_min, end_min = int(ph["start_min"]), int(ph["end_min"])
    events = sorted(scenario["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    boundaries = [start_min] + [int(e["at_min"]) for e in events if start_min <= int(e["at_min"]) < end_min] + [end_min]
    boundaries = sorted(set(boundaries))

    current_rate: Dict[str, float] = {}
    current_lat: Dict[str, Dict[str, float]] = {}

    ev_by_min: Dict[int, List[Dict[str, Any]]] = {}
    for e in events:
        ev_by_min.setdefault(int(e["at_min"]), []).append(e)

    intervals = []
    for i in range(len(boundaries) - 1):
        s = boundaries[i]
        for e in ev_by_min.get(s, []):
            for k, v in e.get("rate_multipliers", {}).items():
                current_rate[k] = float(v)
            for fk, fv in e.get("latency_multipliers", {}).items():
                current_lat[fk] = {"p50": float(fv.get("p50", 1.0)), "p95": float(fv.get("p95", 1.0))}
        intervals.append(
            {"start_min": s, "end_min": boundaries[i + 1], "controls": Controls(rate_mult=dict(current_rate), latency_mult=dict(current_lat))}
        )
    return intervals


def _host_for_component(comp: Dict[str, Any], sticky_key: str) -> str:
    hosts = comp.get("hosts") or []
    if not hosts:
        return ""
    idx = _md5_u32(f"{comp['id']}:{sticky_key}") % len(hosts)
    return hosts[idx]


def _emit_log(
    rows: List[Tuple[datetime, int, Dict[str, str]]],
    seq: int,
    dt: datetime,
    ref: str,
    state: str,
    comp_by_id: Dict[str, Any],
    log_tpl: Dict[str, Any],
    ctx: Dict[str, Any],
    host_override: Optional[str] = None,
    force_vars: Optional[Dict[str, Any]] = None,
) -> int:
    comp_id, _log_id = ref.split(".", 1)
    comp = comp_by_id[comp_id]
    tpl = log_tpl[ref]

    values: Dict[str, Any] = {}
    values.update({k: v for k, v in ctx.items() if not k.startswith("__")})
    if force_vars:
        values.update(force_vars)

    if "state_vars" in tpl:
        stvars = tpl["state_vars"].get(state, {})
        for k, dom in stvars.items():
            if k not in values:
                values[k] = _choose_domain(dom, f"{ref}:{k}:{seq}")

    for k, dom in tpl.get("vars", {}).items():
        if k not in values:
            values[k] = _choose_domain(dom, f"{ref}:{k}:{seq}")

    msg = tpl["msg"].format(**values)
    lvl = tpl["lvl"]
    service = comp["svc"] if comp.get("svc") else ""
    host = host_override if host_override is not None else _host_for_component(comp, ctx.get("__sticky", ""))

    rows.append((dt, seq, {"timestamp": "", "level": lvl, "message": msg, "trace_id": "", "service": service, "host": host}))
    return seq + 1


def _bind_flow_context(flow_id: str, state: str, start_dt: datetime) -> Dict[str, Any]:
    ctx: Dict[str, Any] = {}
    ctx["__sticky"] = f"{flow_id}:{_md5_u32(start_dt.isoformat())}:{_gen_hex(8)}"
    ctx["acct"] = _gen_hex(8)

    ctx["event_id"] = _gen_uuid4_like()
    ctx["txn_id"] = _gen_hex(12)
    ctx["payment_id"] = _gen_hex(12)

    ctx["usage_type"] = "sms" if (_md5_u32(ctx["acct"]) % 2 == 0) else "voice"
    ctx["units"] = int(1 + (_md5_u32(ctx["event_id"]) % 5))

    if flow_id.startswith("usage_billable_item_"):
        ctx["amount_cents"] = int(1 + (_md5_u32(ctx["event_id"]) % 50))
        ctx["delta_cents"] = -int(ctx["amount_cents"])
        ctx["new_balance_cents"] = int((_md5_u32(ctx["acct"] + "bal") % 20000))
        ctx["source"] = "usage"
        ctx["target_cents"] = int(500 + (_md5_u32(ctx["acct"] + "tgt") % (5000 - 500 + 1)))
    elif flow_id.startswith("customer_payment_"):
        # Keep coherence AND respect declared balance_update_ok delta_cents domain [-5000, 5000].
        # The model's manual_payment_received allows up to 20000, but we choose a coherent subset <= 5000.
        amt = int(500 + (_md5_u32(ctx["payment_id"]) % (5000 - 500 + 1)))
        ctx["amount_cents"] = amt
        ctx["delta_cents"] = int(amt)
        ctx["new_balance_cents"] = int((_md5_u32(ctx["acct"] + "bal") % 20000))
        ctx["source"] = "manual_payment"
    elif flow_id.startswith("auto_recharge_"):
        amt = int(500 + (_md5_u32(ctx["payment_id"]) % (5000 - 500 + 1)))
        ctx["amount_cents"] = amt
        ctx["delta_cents"] = int(amt)
        ctx["new_balance_cents"] = int((_md5_u32(ctx["acct"] + "bal") % 20000))
        ctx["source"] = "auto_recharge"
    else:
        ctx["amount_cents"] = int(1 + (_md5_u32(ctx["acct"]) % 50))

    ctx["last4"] = int(1000 + (_md5_u32(ctx["payment_id"] + "l4") % 9000))
    ctx["auth_code"] = _gen_hex(6)
    ctx["decline_code"] = "do_not_honor" if (_md5_u32(ctx["payment_id"] + "dc") % 2 == 0) else "lost_card"

    ctx["err"] = "readonly"
    ctx["reason"] = "card_declined_after_retries"
    return ctx


def _simulate_flows(
    rows: List[Tuple[datetime, int, Dict[str, str]]],
    seq: int,
    state: str,
    interval_start: datetime,
    interval_end: datetime,
    controls: Controls,
    flows_by_state: Dict[str, Dict[str, Any]],
    comp_by_id: Dict[str, Any],
    log_tpl: Dict[str, Any],
    alloc: Allocator,
) -> int:
    duration_min = (interval_end - interval_start).total_seconds() / 60.0
    for flow_id in sorted(flows_by_state[state].keys()):
        flow = flows_by_state[state][flow_id]
        rpm = float(flow["rpm"])
        mult = float(controls.rate_mult.get(flow_id, 1.0)) if state == "f" else 1.0
        eff_rpm = rpm * mult
        expected = eff_rpm * duration_min
        count = alloc.alloc(expected, f"flow:{state}:{flow_id}")

        starts = _schedule_even(interval_start, interval_end, count, f"flowstart:{state}:{flow_id}:{interval_start.isoformat()}")

        latm = controls.latency_mult.get(flow_id, {"p50": 1.0, "p95": 1.0}) if state == "f" else {"p50": 1.0, "p95": 1.0}
        p50m, p95m = float(latm.get("p50", 1.0)), float(latm.get("p95", 1.0))

        for i, st_dt in enumerate(starts):
            ctx = _bind_flow_context(flow_id, state, st_dt)
            t = st_dt
            for j, ref in enumerate(flow["emit"]):
                p50, p95 = flow["latency_ms"][j]
                d_ms = _sample_latency_ms(float(p50) * p50m, float(p95) * p95m, f"lat:{flow_id}:{i}:{j}:{ref}")
                t = t + timedelta(milliseconds=d_ms)

                force_vars = None
                if ref == "billing_api.redis_get_timeout":
                    force_vars = {"waited_ms": int(d_ms)}
                if ref == "ledger_db.ledger_write_ok":
                    force_vars = dict(force_vars or {})
                    force_vars["source"] = ctx.get("source", "usage")
                    force_vars["amount_cents"] = int(ctx.get("amount_cents", 0))

                seq = _emit_log(rows, seq, t, ref, state, comp_by_id, log_tpl, ctx, host_override=None, force_vars=force_vars)
    return seq


def _simulate_background(
    rows: List[Tuple[datetime, int, Dict[str, str]]],
    seq: int,
    state: str,
    interval_start: datetime,
    interval_end: datetime,
    controls: Controls,
    comp_by_id: Dict[str, Any],
    log_tpl: Dict[str, Any],
    alloc: Allocator,
    refund_batch: Dict[str, Any],
) -> int:
    duration_min = (interval_end - interval_start).total_seconds() / 60.0

    for comp_id in [c["id"] for c in SYSTEM["components"]]:
        comp = comp_by_id[comp_id]
        beh = comp.get("beh", {}).get(state, {}).get("emit", [])
        if not beh:
            continue

        for em in beh:
            log_id = em["id"]
            per_min = float(em["per_min"])
            scope = em.get("scope", "per_host")
            ref = f"{comp_id}.{log_id}"

            mult = float(controls.rate_mult.get(ref, 1.0)) if state == "f" else 1.0
            eff = per_min * mult

            if scope == "global":
                expected = eff * duration_min
                count = alloc.alloc(expected, f"bg:{state}:{ref}:global")
                times = _schedule_even(interval_start, interval_end, count, f"bg:{state}:{ref}:global:{interval_start.isoformat()}")

                for k, t in enumerate(times):
                    ctx: Dict[str, Any] = {"__sticky": f"bg:{ref}:{interval_start.isoformat()}:{k}"}

                    force_vars = None
                    if ref == "billing_monitor.refund_progress":
                        if refund_batch.get("batch") is None:
                            refund_batch["batch"] = _gen_hex(8)
                            refund_batch["total"] = int(500 + (_md5_u32(refund_batch["batch"]) % 1500))
                            refund_batch["done"] = 0
                        step = max(1, int(refund_batch["total"] / 5))
                        new_done = min(refund_batch["total"], refund_batch["done"] + step)
                        force_vars = {"batch": refund_batch["batch"], "processed": int(new_done), "remaining": int(max(0, refund_batch["total"] - new_done))}
                        refund_batch["done"] = new_done

                    if ref == "billing_api.pricing_lag_metric" and state == "f":
                        minutes_since_f = int((t - interval_start).total_seconds() / 60.0)
                        lag = 60 + int(min(1740, minutes_since_f * 120 + (_md5_u32(f"{ref}:{k}") % 60)))
                        pending = 5000 + int(min(45000, minutes_since_f * 2500 + (_md5_u32(f"{ref}:p:{k}") % 5000)))
                        force_vars = dict(force_vars or {})
                        force_vars.update({"lag_s": lag, "pending": pending})

                    seq = _emit_log(rows, seq, t, ref, state, comp_by_id, log_tpl, ctx, host_override=_host_for_component(comp, ctx["__sticky"]), force_vars=force_vars)
            else:
                hosts = comp.get("hosts") or [""]
                for h in hosts:
                    expected = eff * duration_min
                    count = alloc.alloc(expected, f"bg:{state}:{ref}:host:{h}")
                    times = _schedule_even(interval_start, interval_end, count, f"bg:{state}:{ref}:{h}:{interval_start.isoformat()}")

                    for k, t in enumerate(times):
                        ctx = {"__sticky": f"bg:{ref}:{h}:{interval_start.isoformat()}:{k}"}
                        force_vars: Optional[Dict[str, Any]] = None

                        if ref == "usage_ingest.ingest_backlog":
                            if state == "n":
                                depth = int(_md5_u32(f"{ref}:d:{k}") % 3000)
                                age = int(_md5_u32(f"{ref}:a:{k}") % 30)
                            else:
                                depth = int(_md5_u32(f"{ref}:d:{interval_start.isoformat()}:{k}") % 20000)
                                age = int(_md5_u32(f"{ref}:a:{interval_start.isoformat()}:{k}") % 300)
                            force_vars = {"depth": depth, "age_s": age}

                        if comp_id == "redis_replicas" and log_id in ("link_down", "fullsync_requested"):
                            force_vars = dict(force_vars or {})
                            force_vars["replica"] = h

                        seq = _emit_log(rows, seq, t, ref, state, comp_by_id, log_tpl, ctx, host_override=h, force_vars=force_vars)

    return seq


def _emit_one_shots(
    rows: List[Tuple[datetime, int, Dict[str, str]]],
    seq: int,
    base_time: datetime,
    scenario: Dict[str, Any],
    comp_by_id: Dict[str, Any],
    log_tpl: Dict[str, Any],
    refund_batch: Dict[str, Any],
) -> int:
    events = sorted(scenario["scenario"]["phases"]["f"]["events"], key=lambda e: (e["at_min"], e["order"]))
    for e in events:
        at_min = int(e["at_min"])
        at_dt = base_time + timedelta(minutes=at_min)
        for os in e.get("one_shots", []):
            ref = os["ref"]
            count = int(os["count"])
            allowed_hosts = os.get("hosts", [])
            for i in range(count):
                jitter_ms = (_md5_u32(f"oneshot:{ref}:{at_min}:{i}") % 900)
                t = at_dt + timedelta(milliseconds=jitter_ms)
                ctx = {"__sticky": f"oneshot:{ref}:{at_min}:{i}"}
                host = None
                if allowed_hosts:
                    host = allowed_hosts[i % len(allowed_hosts)]

                force_vars = None
                if ref == "billing_api.billing_mode_changed":
                    mode = "offline" if at_min in (39, 48) else "online"
                    force_vars = {"mode": mode, "actor": "oncall"}
                elif ref == "billing_monitor.alert_sent":
                    service = "redis" if at_min in (29,) else "billing"
                    if at_min == 29:
                        summary = "redis replication storm causing billing timeouts"
                    elif at_min == 36:
                        summary = "redis readonly after restart; auto-recharge spike"
                    else:
                        summary = "billing anomaly recurrence; refunds starting"
                    force_vars = {"service": service, "summary": summary}
                elif ref == "billing_monitor.anomaly_detected":
                    metric = "charges_per_account"
                    value = 250 if at_min == 36 else 120
                    threshold = 50 if at_min == 36 else 30
                    window_min = 5
                    force_vars = {"metric": metric, "value": value, "threshold": threshold, "window_min": window_min}
                elif ref == "redis_master.db_loaded":
                    force_vars = {"keys": int(_md5_u32("db_loaded:keys") % 30), "expires": int(_md5_u32("db_loaded:exp") % 5), "mem_mb": int(_md5_u32("db_loaded:mem") % 20)}
                elif ref == "redis_master.role_changed":
                    force_vars = {"role": "slave", "master_host": "127.0.0.1", "master_port": 6379, "readonly": "yes"}
                elif ref == "billing_monitor.refund_job_started":
                    if refund_batch.get("batch") is None:
                        refund_batch["batch"] = _gen_hex(8)
                        refund_batch["total"] = int(800 + (_md5_u32(refund_batch["batch"]) % 2200))
                        refund_batch["done"] = 0
                    force_vars = {"batch": refund_batch["batch"], "count": int(refund_batch["total"])}

                seq = _emit_log(rows, seq, t, ref, "f", comp_by_id, log_tpl, ctx, host_override=host, force_vars=force_vars)
    return seq


def main() -> None:
    base_time = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    comp_by_id, log_tpl, flows_by_state = _build_indices(SYSTEM)

    alloc = Allocator()
    rows: List[Tuple[datetime, int, Dict[str, str]]] = []
    seq = 0

    n_start = SCENARIO["scenario"]["time"]["phases"]["n"]["start_min"]
    n_end = SCENARIO["scenario"]["time"]["phases"]["n"]["end_min"]
    n_s = base_time + timedelta(minutes=int(n_start))
    n_e = base_time + timedelta(minutes=int(n_end))
    normal_controls = Controls(rate_mult={}, latency_mult={})

    refund_batch: Dict[str, Any] = {"batch": None, "total": None, "done": 0}

    seq = _simulate_background(rows, seq, "n", n_s, n_e, normal_controls, comp_by_id, log_tpl, alloc, refund_batch)
    seq = _simulate_flows(rows, seq, "n", n_s, n_e, normal_controls, flows_by_state, comp_by_id, log_tpl, alloc)

    failure_intervals = _failure_intervals(SCENARIO)
    for itv in failure_intervals:
        s = base_time + timedelta(minutes=int(itv["start_min"]))
        e = base_time + timedelta(minutes=int(itv["end_min"]))
        controls = itv["controls"]
        seq = _simulate_background(rows, seq, "f", s, e, controls, comp_by_id, log_tpl, alloc, refund_batch)
        seq = _simulate_flows(rows, seq, "f", s, e, controls, flows_by_state, comp_by_id, log_tpl, alloc)

    seq = _emit_one_shots(rows, seq, base_time, SCENARIO, comp_by_id, log_tpl, refund_batch)

    rows_sorted = sorted(rows, key=lambda x: (x[0], x[1]))
    out = []
    for dt, _, rec in rows_sorted:
        rec["timestamp"] = _ts(dt)
        rec["trace_id"] = rec.get("trace_id", "") or ""
        rec["service"] = rec.get("service", "") or ""
        rec["host"] = rec.get("host", "") or ""
        out.append(rec)

    df = pd.DataFrame(out, columns=["timestamp", "level", "message", "trace_id", "service", "host"])
    df.to_csv("logs.csv", index=False)

    nrows = len(df)
    if not (20000 <= nrows <= 100000):
        raise RuntimeError(f"Row count out of target range: {nrows}")


if __name__ == "__main__":
    main()
