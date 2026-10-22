# Verify the consumer that will run

Keep the original command, environment and resource controller. A wrapper's
unit test cannot establish that the existing caller will admit the same work.
Use local fake or CPU inputs with the real consumer entrypoints before a paid
run; retain the command, first failure, run ID, logs and final record. Fake
results establish orchestration behavior, not workload correctness or speed.

## Entry tests and repeated work

Feed an entry or pilot test through the same admission function as the grid,
with a repetition count lower than the grid's required count. It should follow
its declared entry policy. Feed a grid cell with that count through the same
function and verify rejection. Do not globally weaken the repetition check to
make the entry test pass.

## Resume without rewriting history

Start with completed combinations and their verified output acknowledgements,
then retain an explicit not-started record for a later combination. Declare
which combination may resume and bind the declaration to the original run and
receipts. Exercise both that combination and another missing combination.
Completed combinations must not execute again, and their receipts must stay
unchanged. Keep prior failures and not-started attempts as history.

Separately inject an actual workload failure and an ambiguous transport
outcome. Neither is a known harmless transient merely because it has no success
record. Require reconciliation of the original attempt and receipts, and an
explicit retry decision for a real failure. Test a missing or changed receipt
as a rejection, not as work to silently repeat. Resume only the failed or
missing combinations selected by the declaration.

## Completion, export and independent cleanup

Drive the independent controller's real decision function with selected work
that is running, not started, actually failed, stopped, completely finished,
and finished with export still pending. Check the resulting reason as well as
the cleanup decision. A stopped run may trigger failure cleanup without ever
becoming a successful sweep. A completed partition needs its validated output
and acknowledgement; all selected partitions and checks are required for the
summary. Submitting a backfill does not establish either condition.

Verify the original cap and cleanup reserve still take precedence, including
after failure or export trouble. Resource release belongs in the independent
controller or sensor, not a downstream success step. Exercise idle cleanup
after failure and ensure repeated idle ticks do not release a resource twice.
Use only owned local fake resources for these checks. Keep future paid
execution disabled until its concrete resource and budget authority exists;
do not inherit an earlier run's grant or reset its clock.
