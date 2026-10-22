"""Hand one failed sweep run on: a one-shot diagnosis, then a report to the sweep's owner.

The escalate_failure sensor starts this detached with the record's path, so
the sensor returns at once:

    python escalate.py escalations/<run_id>.json

1. DIAGNOSE_COMMAND runs once: a headless agent session with read-only tools
   that reads the record and the logs it names. What it prints (or writes to
   {diagnosis}) becomes <run_id>.diagnosis.md.
2. REPORT_COMMAND hands <run_id>.message.md, which names the record and holds
   the diagnosis, to the owner: usually the agent session that set the sweep
   up, over xsm. The owner decides what to do and reruns only this combination.

Either may be unset. Nothing here reruns or changes anything. The outcome of
both steps is added to the record under "escalation".

Placeholders in both commands: {record}, {diagnosis}, {message}, {prompt}.
$VARS in the commands are expanded first; the placeholders are not.
"""

import json
import os
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

REPORT_TIMEOUT_SECONDS = 60
DIAGNOSIS_LINES_IN_MESSAGE = 40

# The diagnosis is read by another agent, so its form is fixed here, in English,
# whatever language the person's own instructions ask for.
PROMPT = """A run of a parameter sweep failed. You are a one-shot, read-only diagnostician: you cannot run, rerun or change anything, and nobody will answer questions.
Read the escalation record at {record} and the log files it lists. Reply in English, in at most 12 lines, in this form:
CAUSE: <one sentence>
EVIDENCE: <a file path with a line, or a short quote from the logs>
CLASS: transient | real | limit | unknown
FINGERPRINT: <a short stable fragment copied verbatim from the logs or the failure message>
NEXT: <what the sweep's owner should do>
Say "unknown" rather than guess beyond the files."""


def command(template: str, values: dict[str, str]) -> list[str]:
    parts = []
    for part in shlex.split(template):
        part = os.path.expandvars(part)
        for key, value in values.items():
            part = part.replace("{" + key + "}", value)
        parts.append(part)
    return parts


def diagnose(values: dict[str, str], diagnosis: Path) -> str:
    template = os.environ.get("DIAGNOSE_COMMAND")
    if not template:
        return "not configured"
    timeout = int(os.environ.get("DIAGNOSE_TIMEOUT", "600"))
    try:
        proc = subprocess.run(command(template, values), stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        diagnosis.write_text(f"Diagnosis timed out after {timeout} s.\n")
        return "timed out"
    except OSError as error:
        diagnosis.write_text(f"Diagnosis could not start: {error}\n")
        return f"could not start: {error}"
    sys.stderr.write(proc.stderr)
    # A command that writes {diagnosis} itself (codex exec -o) keeps its file.
    if not diagnosis.exists() or diagnosis.stat().st_size == 0:
        diagnosis.write_text(proc.stdout)
    return f"exit {proc.returncode}"


def write_message(record: dict, record_path: Path, diagnosis: Path | None, message: Path) -> None:
    lines = [
        f"Sweep run failed: partition {record.get('partition')}, run {record.get('run_id')}.",
        str(record.get("message") or "").strip(),
        "",
        f"Record: {record_path}",
    ]
    if diagnosis is not None and diagnosis.exists():
        body = diagnosis.read_text(errors="replace").strip().splitlines()[:DIAGNOSIS_LINES_IN_MESSAGE]
        lines += [f"Diagnosis (one-shot, read-only): {diagnosis}", "---", *body, "---"]
    else:
        lines += ["No diagnosis was made."]
    lines += [
        "",
        "Nothing was rerun, and the other combinations keep running. Decide: fix the cause and rerun only "
        "this combination, or ask the person. Reply when it is handled.",
    ]
    message.write_text("\n".join(lines) + "\n")


def report(values: dict[str, str]) -> str:
    template = os.environ.get("REPORT_COMMAND")
    if not template:
        return "not configured"
    try:
        proc = subprocess.run(
            command(template, values), stdin=subprocess.DEVNULL, capture_output=True, text=True, timeout=REPORT_TIMEOUT_SECONDS
        )
    except subprocess.TimeoutExpired:
        return "timed out"
    except OSError as error:
        return f"could not start: {error}"
    sys.stderr.write(proc.stdout + proc.stderr)
    return f"exit {proc.returncode}"


def main(argv: list[str]) -> int:
    record_path = Path(argv[1]).resolve()
    record = json.loads(record_path.read_text())
    run_id = record["run_id"]
    diagnosis = record_path.with_name(f"{run_id}.diagnosis.md")
    message = record_path.with_name(f"{run_id}.message.md")
    values = {"record": str(record_path), "diagnosis": str(diagnosis), "message": str(message)}
    values["prompt"] = PROMPT.replace("{record}", str(record_path))

    diagnosed = diagnose(values, diagnosis)
    write_message(record, record_path, diagnosis if diagnosed != "not configured" else None, message)
    reported = report(values)

    record["escalation"] = {
        "diagnosis": str(diagnosis) if diagnosis.exists() else None,
        "diagnose": diagnosed,
        "message": str(message),
        "report": reported,
        "at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    record_path.write_text(json.dumps(record, indent=2))
    print(f"diagnose: {diagnosed}; report: {reported}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
