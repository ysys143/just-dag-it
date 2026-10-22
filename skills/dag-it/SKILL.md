---
name: dag-it
description: Decide whether repeated, deterministic work (parameter sweeps, benchmarks, regressions, data pipelines) should leave the agent's loop and run as a DAG, and if so move it into Dagster so that runs, retries, limits and progress need no LLM and an agent is called only when a run fails, or to design, add or analyze. Use when an agent keeps babysitting long or repeated runs, when a sweep loses track of what finished, or when the person asks to automate an experiment loop.
---

# Run it as a DAG

An agent that runs every step of a repeated experiment spends its context and tokens on work that has no decision in it, and the longer it runs the more it loses track of what already finished. Move that work into a DAG engine. The engine runs, retries, limits and reports; the agent is called for four things only:

| The agent does | The engine does |
| --- | --- |
| Design the experiment: parameters, steps, success checks | Run every combination, in order, as often as asked |
| Add an experiment: a new parameter value or step | Retry failures whose cause is known and harmless |
| Analyze results the checks cannot judge | Stop runs that exceed their time, release idle resources |
| Handle a failure no rule covers | Remember what finished; report progress |

## 1. Decide

Move work into a DAG when **all** of these hold:

- The same procedure ran at least twice, or will run many times, with only parameters or inputs changing.
- Every step is a command or a function: given its inputs it needs no judgment to run.
- Success can be checked by code: an exit code, a file, a number within bounds.

And **at least one** of these makes the engine worth its upkeep:

- Many combinations or long runs, where an agent watching them would fill its context.
- Partial failures, where only what failed should run again.
- Known transient errors (a dropped connection, a license server, a busy GPU) that a rule can retry.
- A recurring schedule (nightly regression), or paid resources that must stop when idle.

Do not move it when the next step depends on what the last one showed (exploration, root-cause analysis, code changes, design): that is the agent's work. Choose the lightest rung that is enough, and say which and why:

| Rung | Enough when |
| --- | --- |
| Run the command directly | It runs once or rarely |
| A script or Makefile target that skips combinations whose output already exists | It repeats, but needs no schedule, retry rules, failure alerts or progress view |
| A DAG (Dagster) | It needs any of: resume by combination, retry rules, a runtime cap, a failure hook that calls an agent, a schedule, idle resource release, a progress view |

## 2. Inventory

From the project's history (episodes, PHASE.md, shell history, previous runs) write down, before any code:

- the command that runs one combination, and where it writes its result;
- the parameter space (each dimension and its values);
- what makes one result acceptable, and what makes the whole sweep acceptable;
- failures seen so far: which were transient (and their stderr text), which were real;
- resources the runs hold (a GPU machine, a license) and what releases them;
- how long one run takes, and how many may run at once.

Confirm the parameter space and the acceptance checks with the person (AskUserQuestion) before building; they decide what the sweep costs.

## 3. Build

Copy `references/template/` (`definitions.py`, `dagster.yaml`) into the project and replace its parts marked REPLACE. Wrap the existing command; do not rewrite its logic inside the DAG. `references/dagster.md` has the setup, the mapping from each need to Dagster, and the traps that were hit when this template was tested.

Keep these properties:

- One partition per combination, so finished combinations are never rerun by accident.
- Retries only for errors listed as transient; every other failure stops at once (`Failure(allow_retries=False)`).
- A runtime cap on every run, and run concurrency matching the resource (one GPU, one run).
- The failure sensor writes an escalation record; starting an agent from it is opt-in.
- Cleanup of paid resources in a sensor, never as a downstream step: a downstream step does not run after a failure.
- `DAGSTER_HOME` set to a persistent folder; without it every record of what finished is lost when the process stops.

## 4. Verify before handing over

1. `dagster definitions validate -f definitions.py`
2. One combination succeeds end to end.
3. A forced transient error is retried and succeeds; a forced real error is not retried and writes an escalation record.
4. A run past its cap is stopped.
5. After every combination exists, the summary and its checks run by themselves.

Report each with its evidence (run ids, the record path). Starting the full sweep, turning a schedule on, or starting the daemon as a service needs the person's agreement: they cost money or keep running.

## 5. After the move: how the agent behaves

- Progress, retries and limits are the engine's. Do not poll runs in a loop or re-report progress the UI already shows.
- When resuming, reconnect to the existing runs or backfill. A combination that finished stays finished; one that failed is summarized from its record, and rerun only after its cause is handled.
- Never start new runs, a new backfill, or a schedule the person did not approve.
- When called with an escalation record: read it and the logs it points to, find the cause, and either fix it and rerun only the failed combinations, or, if the fix needs a decision (more budget, a changed parameter space), stop and ask. Record what happened where the project keeps its state (PHASE.md and an episode log, when the project has them).
- When the same failure is escalated twice, make it a rule: add its stderr text to the transient list if it is harmless, or a check that fails earlier and clearer if it is not.

## Done when

- The decision (rung and reason) and the inventory were stated and agreed.
- The checks in step 4 passed, each with its evidence.
- The person knows how to see progress (the UI or the status command) and what will call an agent.
