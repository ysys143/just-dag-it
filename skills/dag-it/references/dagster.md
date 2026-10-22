# Dagster for an agent's repeated work

Tested with Dagster 1.13.26 on Python 3.13 (macOS). Check `dagster --version` and adjust when the project uses another release.

## Setup

```sh
uv init --python 3.13 sweep && cd sweep        # or an existing project
uv add dagster dagster-webserver
cp <this skill>/references/template/{definitions.py,escalate.py,fake_experiment.py} .   # fake_experiment.py until RUN_COMMAND points at the real command
mkdir -p .dagster && cp <this skill>/references/template/dagster.yaml .dagster/
cat > env.sh <<'EOF'
export DAGSTER_HOME=$PWD/.dagster              # persistent run records
EOF
source env.sh
uv run dagster definitions validate -f definitions.py
uv run dagster dev -f definitions.py           # UI on http://127.0.0.1:3000, daemon included
```

`DAGSTER_HOME` must point at a folder that keeps `dagster.yaml`. When it is unset, `dagster dev` keeps its records in a temporary folder and loses them, and with them which combinations finished, when it stops.

Keep every variable in `env.sh` and source it before `dagster dev` and before every CLI command. A run's tags, `dagster/max_runtime` among them, are set by the process that creates the run, so a backfill started from a shell without them gets the defaults.

Pin a Python the release supports; 3.12 and 3.13 are safe choices. In 1.13 `dagster definitions validate` warns that it will be replaced by `dg check defs` (from the separate `dagster-dg-cli` package); it still works.

### Variables the template reads

| Variable | Default | Used for |
| --- | --- | --- |
| `DAGSTER_HOME` | none (temporary) | Dagster's run records and `dagster.yaml` |
| `SWEEP_RESULTS` | `./results` | One folder per combination, `summary.json`, `release.log` |
| `SWEEP_ESCALATIONS` | `./escalations` | Escalation records, diagnoses, messages and their logs |
| `SWEEP_RETRY_DELAY` | 30 | Seconds before the first retry of a transient error; doubles each time |
| `SWEEP_MAX_RUNTIME` | 7200 | Runtime cap of each sweep run, in seconds |
| `SWEEP_IDLE_CHECK_SECONDS` | 600 | How often `release_when_idle` checks for active runs |
| `DIAGNOSE_COMMAND` | unset (no diagnosis) | One-shot read-only diagnosis of a failed run, see Calling an agent |
| `DIAGNOSE_TIMEOUT` | 600 | Seconds before the diagnosis is cut off |
| `REPORT_COMMAND` | unset (no report) | Hands the failure and its diagnosis to the sweep's owner, see Calling an agent |
| `TEARDOWN_COMMAND` | unset (no release) | Command that releases paid resources when no run is active |

## From need to Dagster

The last column points into `dagster-expert/`, the official Dagster skill copied next to this file; read it for anything the template does not show.

| Need | Dagster | In the template | Official reference |
| --- | --- | --- | --- |
| One unit of work per combination | `MultiPartitionsDefinition` on an `@asset` | `SWEEP`, `sweep_result` | `references/assets/INDEX.md` |
| Wrap an existing command | `subprocess.run` inside the asset; return `MaterializeResult(metadata=...)` | `RUN_COMMAND` | `references/integrations/dagster-ssh/INDEX.md` for a command on a remote machine |
| Retry only known transient errors | `RetryRequested(max_retries, seconds_to_wait)` raised when stderr matches the list; `Failure` otherwise; no `RetryPolicy` | `TRANSIENT`, `MAX_TRANSIENT_RETRIES` | not covered |
| Keep logs of a run that is stopped | The command's stdout and stderr opened as files, not captured in memory | `sweep_result` | not covered |
| Cap runtime | run tag `dagster/max_runtime` (seconds) with `run_monitoring.enabled` | `MAX_RUNTIME_SECONDS`, `dagster.yaml` | not covered |
| Limit parallel runs | `concurrency.runs.max_concurrent_runs` | `dagster.yaml` | not covered |
| Build the summary once everything exists | `AutomationCondition.eager()` plus an `AutomationConditionSensorDefinition` that is running | `sweep_summary`, `sweep_automation` | `references/automation/declarative-automation/INDEX.md` |
| Judge the outcome | `@asset_check` returning `AssetCheckResult` | `every_combination_present` | `references/assets/INDEX.md` |
| Call an agent on failure | `@run_failure_sensor` writing a record, then `escalate.py`: a one-shot diagnosis and a report to the owner | `escalate_failure`, `escalate.py` | `references/automation/sensors/run-status-sensors.md` |
| Release paid resources when idle | `@sensor` that checks for active runs | `release_when_idle` | `references/automation/sensors/basic-sensors.md` |
| Recur on a schedule | `@schedule(cron_schedule=..., execution_timezone=...)` yielding a `RunRequest` per partition | `nightly_sweep` | `references/automation/schedules.md`, `references/automation/choosing-automation.md` |

## The official skill and the `dg` CLI

`dagster-expert/` is written for projects created with `create-dagster` and driven by the `dg` CLI (`dg dev`, `dg launch`, `dg check defs`; package `dagster-dg-cli`), and for Dagster Plus. This template is a single `definitions.py` run with the `dagster` CLI, which is what was tested here. Both are the same Dagster: in a `create-dagster` project, put the template's definitions under the project's definitions folder and use the `dg` commands from `references/cli/` (this combination was not tested here). The parts of `dagster-expert/` about Dagster Plus (`references/cli/api/`, `references/cli/plus/`, `references/deployment/`) do not apply to a local instance.

## Running

```sh
# every combination
uv run dagster job backfill -f definitions.py -j sweep_job --partitions "10|1000,10|10000,100|1000,100|10000" --noprompt
# only the ones that failed, after their cause is handled
uv run dagster job backfill -f definitions.py -j sweep_job --partitions "100|10000" --noprompt
```

A multi-partition key joins the dimension values in dimension-name order with `|`: `k=10, size=1000` is `10|1000`. List them with `SWEEP.get_partition_keys()`. The UI's backfill dialog can also select only failed and missing partitions.

A backfill started from the CLI submits its runs and is marked completed at once; follow the work in the Runs list and in the asset's partition grid, not on the Backfills page.

## Calling an agent

The failure sensor always writes `escalations/<run_id>.json`: the partition, the run's failure message, each failed step's error, description and stderr tail, the combination's folder, its log files and the tail of its stderr. Nothing else happens until `DIAGNOSE_COMMAND` or `REPORT_COMMAND` is set; then the sensor starts `escalate.py` with the record, detached, its log in `escalations/<run_id>.escalate.log`:

1. **A one-shot diagnosis.** `DIAGNOSE_COMMAND` runs once: a headless agent session with read-only tools. It reads the record and the logs, and its answer (cause, evidence, class, a fingerprint copied from the logs, next step) becomes `<run_id>.diagnosis.md`. It cannot run or change anything, and it times out after `DIAGNOSE_TIMEOUT` seconds.
2. **A report to the owner.** `REPORT_COMMAND` hands `<run_id>.message.md` (what failed, the record, the diagnosis) to the sweep's owner: the agent session that set the sweep up, which knows why it exists. The owner decides, and reruns only that combination or asks the person. Only that combination waits; the others keep running.

The outcome of both steps is added to the record under `escalation`. Placeholders in both commands: `{record}`, `{diagnosis}`, `{message}`, `{prompt}` (the fixed diagnosis prompt in `escalate.py`). `$VARS` are expanded.

Not used: a plain LLM call without tools (it sees only what the prompt carries), and a headless session that may change files or rerun (nobody watches it, and several can start at once).

### The diagnosis, without the person's global instructions

The diagnosis follows the project's instructions (its CLAUDE.md or AGENTS.md) but not the person's own global ones, such as a reply language or tool preferences, which would change the fixed answer format. Both commands below use the logged-in subscription, not an API key.

Claude Code:

```sh
export DIAGNOSE_COMMAND='claude -p {prompt} --model haiku --tools Read,Grep,Glob --setting-sources project,local --strict-mcp-config --disable-slash-commands --no-session-persistence'
```

- `--setting-sources project,local` leaves out every user-level setting: the person's CLAUDE.md, but also their hooks, permission rules and plugins. That is why `--tools` (the tools that exist), not `--allowedTools`, keeps the session to read-only tools. The project's CLAUDE.md still loads.
- Not `--bare`: it authenticates only with `ANTHROPIC_API_KEY`.
- Measured: the global instructions' rules were gone and a project-only codeword was still known; a real escalation was diagnosed in about 5 s, and from inside the daemon the message reached the owner about 30 s after the failure, sensor tick included.

Codex:

```sh
CODEX_HOME=$HOME/.codex-headless codex login        # once, by the person: a home with no AGENTS.md
export DIAGNOSE_COMMAND='env CODEX_HOME=$HOME/.codex-headless codex exec -m gpt-6-luna -s read-only --ephemeral --skip-git-repo-check -o {diagnosis} {prompt}'
```

- Codex reads the global instructions from `$CODEX_HOME/AGENTS.md` and has no switch for them (`--ignore-user-config` skips only `config.toml`). A home without that file leaves them out while the project's AGENTS.md still loads; checked with `codex debug prompt-input`, which prints what the model would see without calling it.
- Log in to that home on its own. Do not copy or link another home's `auth.json`: a token refresh in one home could invalidate the other.
- Skills in `~/.agents/skills` are still listed whatever the home; they are descriptions, not rules.
- Not tested end to end in this form; the same `codex exec` flags were, in the default home.

### The report to the owner session

Over [xsm](https://github.com/ysys143/xsm) or any command that delivers a message:

```sh
export REPORT_COMMAND='xsm send <owner> --kind task --text-file {message} --wait 5'
```

- Name the owner where the project keeps its state (PHASE.md, under Agents). The owner closes the task with `xsm send --reply-to <id> --outcome succeeded|failed`; a delivery receipt says only that the message arrived, not that anyone acted.
- xsm names the sender after the agent session the sending process descends from. A daemon started from an agent session, or from a tmux server that session started, sends as that session. When that session is the owner itself, skip `REPORT_COMMAND` and have it watch `escalations/` for `*.message.md` instead. A daemon started by launchd or systemd was not tested.
- A send to a session in another project is refused (`out of scope`) until the person connects the two (`xsm link` or `xsm reach`).
- With no owner, leave `REPORT_COMMAND` unset: the record, the diagnosis and the message wait in `escalations/` for the person, or use a notification command there instead.

Start with records and diagnoses alone. Report to an owner once the diagnoses have been right a few times.

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

- **Dagster's default automation sensor starts stopped.** `AutomationCondition` does nothing until a running automation sensor evaluates it; define one with `default_status=RUNNING` (`sweep_automation`) or turn it on in the UI. The official reference says the same (`dagster-expert/references/automation/declarative-automation/INDEX.md`).
- **Schedules start stopped** (`DefaultScheduleStatus.STOPPED`). Turn one on only once the person agreed to what it costs.
- **Sensors run on a tick**, every 30 seconds by default. An escalation follows a failure by up to that long.
- **A `RetryPolicy` on the asset retries every exception**, not only the transient errors: a bug in the asset that fails to read its result was attempted three times. When a run is stopped by its cap, the policy also logs a retry request (`STEP_UP_FOR_RETRY`) that never runs. Raising `RetryRequested` only for listed errors avoids both.
- **A run stopped for exceeding its cap has no step failure**, only the run's message ("Exceeded maximum runtime of N seconds."). The record still names the partition, its folder and its logs.
- **Output captured in memory is lost when a run is stopped.** `capture_output=True` writes nothing if the run is interrupted; open the log files before starting the command.
- **`release_when_idle` releases once per idle stretch**: after every batch of runs ends, failures left or not, and again only after a new run. Its ticks show as Skipped in the UI with the release in the reason; `release.log` gets one timestamped entry per release.
- **Commands started from a sensor run inside Dagster's gRPC server.** A child forked there writes gRPC fork warnings to its stderr; start it through a shell that opens its log after the fork, as `_spawn` does, or the log fills with them.
- **A downstream step does not run after a failure.** Cleanup that must always happen belongs in a sensor.
- **Without `DAGSTER_HOME`** every record is temporary, see Setup.
