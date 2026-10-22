"""Checks the template in-process: a rule-based retry, a failure and its
escalation record, the sweep, and the summary check.

    uv run --with dagster python tests/test_template.py
"""

import importlib.util
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

TEMPLATE = Path(__file__).resolve().parents[1] / "skills" / "dag-it" / "references" / "template"


def load(work: Path):
    os.environ["SWEEP_RESULTS"] = str(work / "results")
    os.environ["SWEEP_ESCALATIONS"] = str(work / "escalations")
    os.environ["SWEEP_RETRY_DELAY"] = "0"
    os.environ.pop("ESCALATE_COMMAND", None)
    spec = importlib.util.spec_from_file_location("definitions", work / "definitions.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main() -> None:
    import dagster as dg

    work = Path(tempfile.mkdtemp(prefix="just-dag-it-"))
    try:
        for name in ("definitions.py", "fake_experiment.py"):
            shutil.copy(TEMPLATE / name, work / name)
        t = load(work)
        dg.Definitions.validate_loadable(t.defs)
        instance = dg.DagsterInstance.ephemeral()
        keys = t.SWEEP.get_partition_keys()
        assert len(keys) == 4, keys

        # A transient error is retried by rule and the run succeeds; no agent is needed.
        (work / "transient-1000-10").write_text("")
        transient_key = dg.MultiPartitionKey({"size": "1000", "k": "10"})
        retried = dg.materialize([t.sweep_result], partition_key=transient_key, instance=instance)
        assert retried.success
        assert len([e for e in retried.all_events if e.event_type_value == "STEP_UP_FOR_RETRY"]) == 1

        # One combination fails with a non-transient error: no retry, an escalation record.
        (work / "fail-10000-100").write_text("")
        failed_key = dg.MultiPartitionKey({"size": "10000", "k": "100"})
        failed = dg.materialize([t.sweep_result], partition_key=failed_key, instance=instance, raise_on_error=False)
        assert not failed.success
        retries = [e for e in failed.all_events if e.event_type_value == "STEP_UP_FOR_RETRY"]
        assert retries == [], "a non-transient failure must not be retried"

        failure = next(e for e in failed.all_events if e.event_type_value == "PIPELINE_FAILURE")
        context = dg.build_run_status_sensor_context(
            sensor_name="escalate_failure", dagster_event=failure, dagster_instance=instance, dagster_run=failed.dagster_run
        ).for_run_failure()
        t.escalate_failure(context)
        record = json.loads((work / "escalations" / f"{failed.run_id}.json").read_text())
        assert record["steps"][0]["step"] == "sweep_result", record
        assert "out of memory" in record["steps"][0]["metadata"]["stderr_tail"], record
        assert record["steps"][0]["description"].startswith("exit 1:"), record
        # The record points at the combination's folder and its logs, written while it ran.
        assert record["out"] == str(work / "results" / "k=100__size=10000"), record
        assert [Path(p).name for p in record["logs"]] == ["stdout.log", "stderr.log"], record
        assert "out of memory" in record["stderr_tail"], record
        assert "start size=10000 k=100" in (work / "results" / "k=100__size=10000" / "stdout.log").read_text()

        # An exception in the asset itself (no result.json) is not retried either.
        (work / "noresult-1000-100").write_text("")
        broken = dg.materialize(
            [t.sweep_result], partition_key=dg.MultiPartitionKey({"size": "1000", "k": "100"}), instance=instance, raise_on_error=False
        )
        assert not broken.success
        assert [e for e in broken.all_events if e.event_type_value == "STEP_UP_FOR_RETRY"] == [], "an unexpected exception must not be retried"
        (work / "noresult-1000-100").unlink()

        # The cause is fixed; every combination runs; the summary check passes.
        (work / "fail-10000-100").unlink()
        for key in keys:
            assert dg.materialize([t.sweep_result], partition_key=key, instance=instance).success, key
        summary = dg.materialize([t.sweep_summary, t.every_combination_present], instance=instance)
        checks = summary.get_asset_check_evaluations()
        assert [c.passed for c in checks] == [True], checks
        assert len(json.loads((work / "results" / "summary.json").read_text())) == 4

        print(
            "template: transient error retried once; other failure and an unexpected exception not retried; "
            "escalation record with folder and logs; 4 combinations; summary check passed"
        )
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
