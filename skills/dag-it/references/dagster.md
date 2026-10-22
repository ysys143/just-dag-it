# Dagster for an agent's repeated work

Tested with Dagster 1.13.26 on Python 3.13 (macOS). Check `dagster --version` and adjust when the project uses another release.

## Setup

```sh
uv init --python 3.13 sweep && cd sweep        # or an existing project
uv add dagster dagster-webserver
cp <this skill>/references/template/{definitions.py,dagster.yaml} .
mkdir -p .dagster && mv dagster.yaml .dagster/
export DAGSTER_HOME=$PWD/.dagster              # persistent run records
uv run dagster definitions validate -f definitions.py
uv run dagster dev -f definitions.py           # UI on http://127.0.0.1:3000, daemon included
```

`DAGSTER_HOME` must point at a folder that keeps `dagster.yaml`. When it is unset, `dagster dev` keeps its records in a temporary folder and loses them, and with them which combinations finished, when it stops.

Pin a Python the release supports; 3.12 and 3.13 are safe choices.

## From need to Dagster

| Need | Dagster | In the template |
| --- | --- | --- |
| One unit of work per combination | `MultiPartitionsDefinition` on an `@asset` | `SWEEP`, `sweep_result` |
| Wrap an existing command | `subprocess.run` inside the asset; return `MaterializeResult(metadata=...)` | `RUN_COMMAND` |
| Retry only known transient errors | `RetryPolicy` on the asset; `Failure(allow_retries=False)` for everything else | `TRANSIENT`, `RETRY` |
| Cap runtime | run tag `dagster/max_runtime` (seconds) with `run_monitoring.enabled` | `MAX_RUNTIME_SECONDS`, `dagster.yaml` |
| Limit parallel runs | `concurrency.runs.max_concurrent_runs` | `dagster.yaml` |
| Build the summary once everything exists | `AutomationCondition.eager()` plus an `AutomationConditionSensorDefinition` that is running | `sweep_summary`, `sweep_automation` |
| Judge the outcome | `@asset_check` returning `AssetCheckResult` | `every_combination_present` |
| Call an agent on failure | `@run_failure_sensor` | `escalate_failure` |
| Release paid resources when idle | `@sensor` that checks for active runs | `release_when_idle` |
| Recur on a schedule | `@schedule(cron_schedule=..., execution_timezone=...)` yielding a `RunRequest` per partition | `nightly_sweep` |

## Running

```sh
# every combination
uv run dagster job backfill -f definitions.py -j sweep_job --partitions "10|1000,10|10000,100|1000,100|10000" --noprompt
# only the ones that failed, after their cause is handled
uv run dagster job backfill -f definitions.py -j sweep_job --partitions "100|10000" --noprompt
```

A multi-partition key joins the dimension values in dimension-name order with `|`: `k=10, size=1000` is `10|1000`. List them with `SWEEP.get_partition_keys()`. The UI's backfill dialog can also select only failed and missing partitions.

## Calling an agent

The failure sensor always writes `escalations/<run_id>.json`: the partition, the run's failure message, each failed step's error, description and stderr tail, and the results folder. With `ESCALATE_COMMAND` set it also starts that command detached, `{record}` replaced by the record's path, its output in `escalations/<run_id>.agent.log`.

```sh
# Claude Code, headless, allowed to read and to edit the project's state files.
# The prompt comes before --allowedTools, which takes every value after it.
export ESCALATE_COMMAND='claude -p "A sweep run failed. Read {record} and the logs it points to. Find the cause. Do not start runs or change the sweep. Write the cause and a proposed fix to PHASE.md under Needs you, then stop." --allowedTools Read,Grep,Glob,Edit'

# Codex, read-only sandbox
export ESCALATE_COMMAND='codex exec -s read-only "A sweep run failed. Read {record} and the logs it points to; report the cause and a proposed fix."'

# Another running session, over xsm
export ESCALATE_COMMAND='xsm send <session> --text "sweep run failed: {record}"'
```

Start with the record alone. Let an agent act on its own only after its escalations have been right a few times, and keep its permissions to what the prompt asks for.

## Keeping it running

Schedules and sensors run only while the daemon runs. For more than a working session, run the daemon as a service and start the UI when needed (`uv run dagster-webserver -f definitions.py -p 3000`).

macOS (`~/Library/LaunchAgents/<label>.plist`, then `launchctl bootstrap gui/$(id -u) <plist>`):

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>local.sweep.dagster-daemon</string>
  <key>WorkingDirectory</key><string>/path/to/sweep</string>
  <key>ProgramArguments</key>
  <array>
    <string>/path/to/sweep/.venv/bin/dagster-daemon</string>
    <string>run</string><string>-f</string><string>definitions.py</string>
  </array>
  <key>EnvironmentVariables</key>
  <dict><key>DAGSTER_HOME</key><string>/path/to/sweep/.dagster</string></dict>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>StandardErrorPath</key><string>/path/to/sweep/.dagster/daemon.log</string>
</dict>
</plist>
```

Linux (`~/.config/systemd/user/sweep-dagster.service`, then `systemctl --user enable --now sweep-dagster`):

```ini
[Service]
WorkingDirectory=/path/to/sweep
Environment=DAGSTER_HOME=/path/to/sweep/.dagster
ExecStart=/path/to/sweep/.venv/bin/dagster-daemon run -f definitions.py
Restart=always

[Install]
WantedBy=default.target
```

A laptop that sleeps pauses the daemon. A schedule missed while it was down is caught up for at most 5 ticks by default; put long or time-bound work on a machine that stays up.

## Traps found while testing the template

- **Dagster's default automation sensor starts stopped.** `AutomationCondition` does nothing until a running automation sensor evaluates it; define one with `default_status=RUNNING` (`sweep_automation`) or turn it on in the UI.
- **Schedules start stopped** (`DefaultScheduleStatus.STOPPED`). Turn one on only once the person agreed to what it costs.
- **Sensors run on a tick**, every 30 seconds by default. An escalation follows a failure by up to that long.
- **A run stopped for exceeding its cap has no step failure**, only the run's message ("Exceeded maximum runtime of N seconds."); the record still names the partition.
- **Commands started from a sensor run inside Dagster's gRPC server.** A child forked there writes gRPC fork warnings to its stderr; start it through a shell that opens its log after the fork, as `_spawn` does, or the log fills with them.
- **A downstream step does not run after a failure.** Cleanup that must always happen belongs in a sensor.
- **Without `DAGSTER_HOME`** every record is temporary, see Setup.
