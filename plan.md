Path variables — wherever a $name below appears, use its value:
$materialize=~/materialize
$sandbox=~/mz-sandbox

Implement the plan at plans/tasks/materialize/branch/2026-08-14-vibe-branch-spike/00_driver.md (a path in a checked-out repo). Use AT MOST 2 subagents at a time.

1. Read the plan fully. `cd $materialize` and create the branch. As your FIRST action, write the plan's path to `$EMBER_PLAN_MARKER`.
2. FAST local checks first — do NOT run a full `cargo build` at this point (tens of GB, slow). Use TARGETED checks on the crates you changed: `cargo check -p <crate> -j 6`.
3. Once targeted checks pass, run a full build and the relevant SLT tests from the plan file. Iterate on failures. Do NOT open a PR unless asked.
4. Summarize the changes, the branch name, and test status, then stop.
