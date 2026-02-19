---
name: commit-and-pr-materialize
description: Generates Git commit messages and PR descriptions for the Materialize repo following the project style guide. Use when the user asks for a commit message, PR description, to summarize changes for commit/PR, to draft a squashed commit message, or when they mention "commit style", "PR description", or "Materialize commits."
---

# Materialize commits and PRs

Generate commit messages and PR descriptions that follow [doc/developer/guide-changes.md](doc/developer/guide-changes.md). Materialize uses **squash merging**: the PR description becomes the squashed commit on `main`, so use the same structure for both.

## Style guide (guide-changes.md)

- **First line:** One short imperative sentence summarizing the change.
- **Body:** After a blank line, detailed description: why the change was made, alternatives, future work. Wrap at 72 characters; blank lines between paragraphs.
- **No vague messages:** Avoid "Fix bug." Be specific.
- **Close issues:** Include e.g. `Fixes database-issues#1234` in the PR/commit body so GitHub closes the issue on merge.

## Preferred structure (commit and PR description)

### Subject line

```
[<issue-number>] [<Area>] <Imperative summary>
```

- **Issue:** Optional. database-issues number, e.g. `[10089]` or `10006`. GitHub may append `(#34981)` on merge.
- **Area:** CODEOWNERS / stack area, e.g. `[Adapter]`, `[Planner]`, `[pgrepr]`, `[Storage]`.
- **Summary:** Imperative, specific; one line.

### Body sections

Use in order when they add value:

1. **Problem:** What was wrong (user-visible symptom, panic, or incorrect behavior). Optionally a short error/repro block.
2. **Solution:** Bullet list of what was done (types, signatures, call sites, refactors).
3. **Testing:** What was added or run (e.g. new pgtest, .slt, testdrive, manual).
4. **Manual:** (Optional) Copy-pastable session (e.g. `materialize=>` output) in a fenced code block.

Use consistent headers: either `Problem:` / `Solution:` / `Testing:` / `Manual:` or `## Problem:` etc.

## Template

```
[<issue>] [<Area>] <Imperative one-line summary>

Problem:
<What was broken or missing and why it mattered.>

Solution:
- <First change>
- <Second change>
- <Call sites / follow-ups>

Testing:
<New or existing tests; how to run if non-obvious.>

Manual:   # optional
```
<session output if helpful>
```

Fixes database-issues#<N>   # if applicable
```

## Example (squashed commit / PR description)

```
[10089] [Adapter] Fix COPY panic on invalid range by making into_datum return Result

Problem:
into_datum had no way to report errors, so COPY with a malformed range
(e.g. [7,3) with lower > upper) caused a panic instead of a user-visible error.

Solution:
- Introduce IntoDatumError (wrapping InvalidRangeError and InvalidArrayError)
and make Value::into_datum return Result<Datum, IntoDatumError>.
- Add into_datum_decode_error for decode/parameter contexts with a consistent
error message
- Propagate the error at all call sites (pgcopy, pgwire bind, environmentd
HTTP SQL params, storage-operators CSV).
- Extract value_range_to_datum_range for shared range conversion.

Testing:
- New pgtest copy-from-range.pt: positive COPY cases (valid ranges) and
  negative case (invalid range [7,3) expects ErrorResponse).

Manual:
```
materialize=> create table t(r int4range);
...
>> (5, 3)
>> ERROR:  unable to decode column: range lower bound must be less than or equal to range upper bound
```
```

## PR-specific (guide-changes.md)

- **One review unit per PR:** One semantic change per PR. If the description reads like "do X and do Y and do Z", split into multiple PRs.
- **Scope:** Prefer one team/CODEOWNERS scope per PR, especially for larger changes (e.g. parser vs planner).
- **Release notes:** If the change is user-visible (SQL behavior, source/sink behavior, CLI flags, panic fixes, output format changes), add a release note in the PR template. Phrase in imperative, completing "This release will...". See guide-changes.md for what requires a release note.

## Shorter commits / PRs

For small changes, a one-line subject plus a short paragraph is enough. Still use imperative and be specific; omit Problem/Solution/Testing when they don't add information.

## When generating

1. Infer **Area** from files/crates changed (adapter, planner, pgrepr, pgwire, storage-operators, etc.).
2. Infer **issue number** from branch name, PR title, or context; omit if unknown.
3. Use the actual diff or described changes for Problem and Solution; do not invent details.
4. For PRs, add release note and "Fixes database-issues#N" when applicable.
