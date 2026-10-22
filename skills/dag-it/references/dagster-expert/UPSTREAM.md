# Upstream

This folder is a copy of the official Dagster skill `dagster-expert`.

| | |
| --- | --- |
| Repository | https://github.com/dagster-io/skills |
| Path | `plugins/dagster/skills/dagster-expert/` |
| Commit | `ed0248b1b6b70710a8785e3ff4880b44496c0f94` (2026-10-20) |
| License | Apache-2.0, in `LICENSE` here |
| Copyright | Copyright 2025 Dagster Labs, Inc. (as stated in its `LICENSE`) |

Changes from upstream:

- `SKILL.md` is renamed to `INDEX.md`, so that this copy is never loaded as a second skill named `dagster-expert` next to the official plugin. Its links still resolve, since every reference sits below it.
- `LICENSE` and this file are added. Nothing else is changed.

To update, copy the same path from a newer commit, rename `SKILL.md` again, and record the new commit here. When the official `dagster` plugin is installed (`/plugin install dagster@dagster`), its `dagster-expert` skill carries the same material and may be newer.
