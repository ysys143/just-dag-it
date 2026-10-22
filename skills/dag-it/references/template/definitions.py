"""A parameter sweep run as a DAG: deterministic runs, an agent only on failure.

Copy this folder into the project and replace the parts marked REPLACE: the
command one combination runs, the parameter space, the stderr fragments that
mean a harmless transient error, and the summary check. Everything else is
the machinery: retries by rule, a runtime cap, a summary once every
combination exists, an escalation record (and, if configured, an agent) when
a run fails, and an optional release of paid resources once the sweep is idle.

Run it with DAGSTER_HOME set to a folder that holds dagster.yaml:
    DAGSTER_HOME=$PWD/.dagster dagster dev -f definitions.py
"""

import json
import os
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import dagster as dg

# Results and escalation records stay in the project; DAGSTER_HOME keeps only
# Dagster's own run records.
ROOT = Path(__file__).resolve().parent
RESULTS = Path(os.environ.get("SWEEP_RESULTS", ROOT / "results"))
ESCALATIONS = Path(os.environ.get("SWEEP_ESCALATIONS", ROOT / "escalations"))

# REPLACE: the command one combination runs. Each part is formatted with the
# partition's dimensions and {out}, the folder it writes result.json to.
RUN_COMMAND = [sys.executable, str(ROOT / "fake_experiment.py"), "--size", "{size}", "--k", "{k}", "--out", "{out}"]

# REPLACE: the parameter space, one partition per combination.
SWEEP = dg.MultiPartitionsDefinition(
    {
        "size": dg.StaticPartitionsDefinition(["1000", "10000"]),
        "k": dg.StaticPartitionsDefinition(["10", "100"]),
    }
)

# REPLACE: stderr fragments of failures with a known, harmless cause. These
# are retried by rule; any other failure stops at once and is escalated.
TRANSIENT = ("Connection reset", "Connection timed out", "Temporary failure in name resolution")

RETRY = dg.RetryPolicy(max_retries=2, delay=int(os.environ.get("SWEEP_RETRY_DELAY", "30")), backoff=dg.Backoff.EXPONENTIAL)

# Every sweep run is stopped after this long; enforced by run monitoring
# (run_monitoring.enabled in dagster.yaml).
MAX_RUNTIME_SECONDS = int(os.environ.get("SWEEP_MAX_RUNTIME", str(2 * 60 * 60)))


@dg.asset(partitions_def=SWEEP, retry_policy=RETRY)
def sweep_result(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    key = context.partition_key
    # A multi-partitioned asset gets a MultiPartitionKey, such as "10|1000" for k=10, size=1000.
    params: dict[str, str] = key.keys_by_dimension if isinstance(key, dg.MultiPartitionKey) else {}
    out = RESULTS / "__".join(f"{name}={value}" for name, value in sorted(params.items()))
    out.mkdir(parents=True, exist_ok=True)
    command = [part.format(out=out, **params) for part in RUN_COMMAND]
    proc = subprocess.run(command, capture_output=True, text=True)
    (out / "stdout.log").write_text(proc.stdout)
    (out / "stderr.log").write_text(proc.stderr)
    if proc.returncode != 0:
        raise dg.Failure(
            description=f"exit {proc.returncode}: {shlex.join(command)}",
            metadata={"stderr_tail": dg.MetadataValue.text(proc.stderr[-2000:]), "out": dg.MetadataValue.path(str(out))},
            allow_retries=any(fragment in proc.stderr for fragment in TRANSIENT),
        )

    result = json.loads((out / "result.json").read_text())
    numbers = {name: value for name, value in result.items() if isinstance(value, (int, float))}
    return dg.MaterializeResult(metadata={**numbers, "out": dg.MetadataValue.path(str(out))})


# Runs by itself once every combination has a result, and again when one is
# redone; evaluated by sweep_automation below.
@dg.asset(deps=[sweep_result], automation_condition=dg.AutomationCondition.eager())
def sweep_summary() -> dg.MaterializeResult:
    rows = [json.loads(path.read_text()) for path in sorted(RESULTS.glob("*/result.json"))]
    path = RESULTS / "summary.json"
    path.write_text(json.dumps(rows, indent=2))
    return dg.MaterializeResult(metadata={"rows": len(rows), "path": dg.MetadataValue.path(str(path))})


# REPLACE: what makes the sweep's outcome acceptable.
@dg.asset_check(asset=sweep_summary)
def every_combination_present() -> dg.AssetCheckResult:
    rows = json.loads((RESULTS / "summary.json").read_text())
    expected = len(SWEEP.get_partition_keys())
    return dg.AssetCheckResult(passed=len(rows) == expected, metadata={"rows": len(rows), "expected": expected})


# Dagster's own default automation sensor starts stopped; this one starts running.
sweep_automation = dg.AutomationConditionSensorDefinition(
    "sweep_automation",
    target=dg.AssetSelection.assets(sweep_summary),
    default_status=dg.DefaultSensorStatus.RUNNING,
)


def _spawn(argv: list[str], log: Path) -> None:
    """Start a command detached, its output in `log`.

    Sensors run inside Dagster's gRPC server; a child forked there writes
    gRPC's fork warnings to its stderr before exec. The shell opens the log
    only after that, so the log holds the command's own output.
    """
    log.parent.mkdir(parents=True, exist_ok=True)
    subprocess.Popen(
        ["/bin/sh", "-c", 'exec "$@" >"$0" 2>&1', str(log), *argv],
        cwd=ROOT,
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )


sweep_job = dg.define_asset_job(
    "sweep_job",
    selection=[sweep_result],
    partitions_def=SWEEP,
    tags={"dagster/max_runtime": str(MAX_RUNTIME_SECONDS)},
)


# For a sweep that must recur (a nightly regression). Stopped by default:
# turn it on in the UI, or set RUNNING here, once the person agreed to its cost.
@dg.schedule(job=sweep_job, cron_schedule="0 2 * * *", execution_timezone="Asia/Seoul", default_status=dg.DefaultScheduleStatus.STOPPED)
def nightly_sweep(context: dg.ScheduleEvaluationContext):
    stamp = context.scheduled_execution_time.strftime("%Y%m%d%H%M")
    for key in SWEEP.get_partition_keys():
        yield dg.RunRequest(run_key=f"{stamp}:{key}", partition_key=key)


def _step_failures(context: dg.RunFailureSensorContext) -> list[dict]:
    failures = []
    for event in context.get_step_failure_events():
        data = event.step_failure_data
        user = data.user_failure_data
        failures.append(
            {
                "step": event.step_key,
                "error": data.error.message.strip() if data.error else None,
                "description": user.description if user else None,
                "metadata": {key: getattr(value, "value", str(value)) for key, value in (user.metadata if user else {}).items()},
            }
        )
    return failures


# The one place an agent enters: a run failed in a way no rule handled.
@dg.run_failure_sensor(monitored_jobs=[sweep_job], default_status=dg.DefaultSensorStatus.RUNNING)
def escalate_failure(context: dg.RunFailureSensorContext) -> None:
    run = context.dagster_run
    record = {
        "run_id": run.run_id,
        "job": run.job_name,
        "partition": run.tags.get("dagster/partition"),
        "failed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "message": context.failure_event.message,
        "steps": _step_failures(context),
        "results": str(RESULTS),
    }
    ESCALATIONS.mkdir(parents=True, exist_ok=True)
    path = ESCALATIONS / f"{run.run_id}.json"
    path.write_text(json.dumps(record, indent=2))

    # Optional: start an agent on the record, detached so the sensor returns at
    # once. "{record}" in the command becomes the record's path.
    command = os.environ.get("ESCALATE_COMMAND")
    if command:
        _spawn([part.replace("{record}", str(path)) for part in shlex.split(command)], ESCALATIONS / f"{run.run_id}.agent.log")


ACTIVE = [
    dg.DagsterRunStatus.QUEUED,
    dg.DagsterRunStatus.NOT_STARTED,
    dg.DagsterRunStatus.STARTING,
    dg.DagsterRunStatus.STARTED,
]


# Optional: release paid resources (a GPU machine) once no sweep run is left.
# A downstream asset would not run after a failure; this does.
@dg.sensor(minimum_interval_seconds=int(os.environ.get("SWEEP_IDLE_CHECK_SECONDS", "600")), default_status=dg.DefaultSensorStatus.RUNNING)
def release_when_idle(context: dg.SensorEvaluationContext):
    command = os.environ.get("TEARDOWN_COMMAND")
    if not command:
        return dg.SkipReason("TEARDOWN_COMMAND is not set")
    if context.instance.get_run_records(dg.RunsFilter(job_name=sweep_job.name, statuses=ACTIVE), limit=1):
        return dg.SkipReason("sweep runs are still active")
    last = context.instance.get_run_records(dg.RunsFilter(job_name=sweep_job.name), limit=1)
    if not last or last[0].dagster_run.run_id == context.cursor:
        return dg.SkipReason("no sweep run since the last release")

    _spawn(shlex.split(command), RESULTS / "release.log")
    context.update_cursor(last[0].dagster_run.run_id)
    return dg.SkipReason(f"released resources after run {last[0].dagster_run.run_id}")


defs = dg.Definitions(
    assets=[sweep_result, sweep_summary],
    asset_checks=[every_combination_present],
    jobs=[sweep_job],
    schedules=[nightly_sweep],
    sensors=[sweep_automation, escalate_failure, release_when_idle],
)
