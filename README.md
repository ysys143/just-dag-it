# just-dag-it

A skill that tells an agent when repeated work should leave its loop and run as a DAG, and how to move it into [Dagster](https://dagster.io).

An agent that runs every step of a parameter sweep, a benchmark or a nightly regression spends its context and tokens on steps with no decision in them, and the longer it runs the more it loses track of what already finished. A DAG engine runs those steps deterministically. The agent is called for four things only: to design the experiment, to add to it, to analyze what the checks cannot judge, and to handle a failure no rule covers.

## What the skill does

`/just-dag-it:dag-it` walks an agent through:

1. **Decide.** Whether the work qualifies (repeated, deterministic, checkable by code) and which rung is enough: run it directly, a script that skips finished outputs, or a DAG.
2. **Inventory.** The command, the parameter space, the acceptance checks, known transient errors, the resources held; confirmed with you before anything is built.
3. **Build.** From a tested template: one partition per combination, retries only for listed transient errors, a runtime cap, a summary and its checks once everything exists, an escalation record (and optionally an agent) on failure, and release of paid resources when idle.
4. **Verify.** Each property is checked with evidence before the sweep is handed over.
5. **Behave.** After the move the agent stops polling, never reruns what finished, and starts nothing you did not approve.

## Install

```text
/plugin install just-dag-it --marketplace ysys143/just-dag-it
```

The skill is plain Markdown with a Python template, so other agents can use it too: copy `skills/dag-it/` to where they read skills (for Codex, `~/.agents/skills/`).

## The template

`skills/dag-it/references/template/` holds `definitions.py`, `dagster.yaml` and `fake_experiment.py`, a stand-in experiment so the template runs as is. `skills/dag-it/references/dagster.md` covers setup, the mapping from each need to Dagster, how to call Claude Code, Codex or another session on failure, running the daemon as a service, and the traps found while testing.

`skills/dag-it/references/dagster-expert/` is the official Dagster skill from [dagster-io/skills](https://github.com/dagster-io/skills), copied whole so the agent reads Dagster's own reference instead of writing its APIs from memory: partitions, automation, sensors, schedules, integrations (SSH, Docker, Kubernetes and 40 more), and the `dg` CLI. Its `SKILL.md` is renamed `INDEX.md` so it never loads as a second `dagster-expert` skill next to the official plugin; `UPSTREAM.md` there records the commit it was copied from.

Tested with Dagster 1.13.26 on Python 3.13, in process and against a running `dagster dev` with its daemon:

| Scenario | Result |
| --- | --- |
| A transient error (`Connection reset`) | Retried once by rule, then succeeded; no agent called |
| Any other error | Not retried; escalation record written and the agent command started with its path |
| An exception in the asset itself (no result file) | Not retried |
| A run past its 20-second cap | Stopped by run monitoring, no retry logged; its partial logs kept and named in the escalation record |
| Only the failed combinations rerun | Succeeded |
| Every combination present | Summary built by the automation sensor and its check passed |
| No sweep run left | Resource release command ran once per idle stretch, one timestamped entry each |

A separate agent session that loaded the skill as a project skill and followed it on a 3 × 3 sweep, with the Dagster UI open, reached the same results; its findings are what version 0.1.1 fixed.

```sh
uv run --with dagster python tests/test_template.py
```

## With context-fold

[context-fold](https://github.com/ysys143/context-fold) keeps an agent's own window and project state (INTENT.md, PHASE.md, episodes) in order. Its `consolidate` skill promotes a repeated procedure into a project skill; when the procedure is deterministic, this skill is the next step, from a skill the agent reads each time to code the engine runs. Each works without the other.

## Credits

The split between a deterministic DAG and an agent called only for design, additions, analysis and exceptions follows advice shared in a Korean AI developer community chat about running experiments with agents.

## License

MIT, except `skills/dag-it/references/dagster-expert/`, which is Apache-2.0 (Copyright 2025 Dagster Labs, Inc.); see `NOTICE`.
