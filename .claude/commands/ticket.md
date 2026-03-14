# Resolve a BunnyMQ implementation ticket

Implement ticket **T-$ARGUMENTS** from scratch and open a pull request for it.

## Step 1 — Locate and read the ticket

Find the ticket file:
```
find docs/tickets -name "T-$ARGUMENTS-*.md" | head -1
```

Read it in full. Extract:
- **Title** (first `#` heading)
- **Milestone** (from frontmatter)
- **Scope** (what files to create/modify and what logic to add)
- **Dependencies** (T-NNN tickets that must be done first — verify they are merged to main before proceeding; if not, stop and tell the user which tickets are missing)
- **Definition of done** (checklist — every item must pass before opening the PR)
- **Tests required** (specific test function names to implement)
- **Notes** (implementation hints, gotchas)

## Step 2 — Read referenced design documents

The ticket's Context section links to specific design doc sections. Read every linked section in full before writing any code. Do not rely on the ticket body alone — the design docs contain the exact algorithms, field names, and type definitions. Key docs live in `docs/design/`.

Also read any existing code the ticket modifies (check the Scope section for file paths).

## Step 3 — Check dependencies

For each ticket listed in the **Dependencies** section:
- Verify the corresponding files/packages exist in the codebase (the dependency ticket was already implemented).
- If a dependency is missing, stop and tell the user: "T-$ARGUMENTS depends on T-NNN which has not been implemented yet. Please implement T-NNN first."

## Step 4 — Create a branch

Switch to main and pull the latest changes before branching:
```
git switch main
git pull
```

Then create a branch named after the ticket file slug:
```
git checkout -b ticket/T-$ARGUMENTS-<slug>
```

Where `<slug>` is the part of the filename after `T-$ARGUMENTS-` with the `.md` stripped (e.g., `T-015-batch-encoder-decoder.md` → branch `ticket/T-015-batch-encoder-decoder`).

Verify you are on the new branch and that it is based on the latest `main`.

## Step 5 — Implement

Follow the **Scope** section exactly. Key rules:
- Create files where the Scope says "create"; modify files where it says "modify/add to".
- Match type names, method signatures, and field names exactly as written in the referenced design doc sections. Do not invent names.
- Implement all methods listed in Scope — do not leave stubs unless the ticket explicitly says a method is a no-op (e.g., `Flush`, `Commit` in manual mode).
- Write **all tests listed in the Tests required section** with the exact function names given. Each test must be in the `_test.go` file for its package.
- Do not add features, refactors, or abstractions beyond what the ticket describes.
- Do not write comments that describe what the code does. Only write a comment when the WHY is non-obvious (hidden constraint, subtle invariant, workaround).
- Integration tests (build tags `integration` or `integration,docker`) must start with `//go:build integration` or `//go:build integration,docker`.

## Step 6 — Verify the Definition of done

Go through every checkbox in the **Definition of done** section:

1. Run `go build ./...` — must pass with zero errors.
2. Run the test command listed in the DoD (usually `go test ./<package>/...`). All tests must pass.
3. For each functional criterion in the DoD (e.g., "After one Append, counter increments"), verify it is covered by a test.
4. If a DoD item requires a running cluster or docker-compose (integration tests), note it in the PR description as "requires docker-compose cluster" and skip the live run — CI will cover it.

Fix any failures before proceeding to the PR.

## Step 6.5 — Run the linter

```bash
golangci-lint run ./...
```

All issues must be resolved before opening the PR. CI runs golangci-lint and will fail the PR if there are any issues.

## Step 7 — Commit

Stage and commit all new and modified files:
```
git add <files listed in Scope>
git commit -m "T-$ARGUMENTS: <ticket title>

<one-sentence summary of what was implemented>

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

Do not use `git add -A` or `git add .` — add files explicitly by name to avoid committing generated artifacts or IDE files.

## Step 8 — Open a pull request

Push the branch and create a PR:

```
git push -u origin ticket/T-$ARGUMENTS-<slug>
gh pr create \
  --assignee sunnyyssh \
  --title "T-$ARGUMENTS: <ticket title>" \
  --body "$(cat <<'EOF'
## Ticket

[T-$ARGUMENTS: <ticket title>](docs/tickets/<milestone-dir>/<filename>.md)

**Milestone:** <milestone>
**Effort:** <effort>

## What this PR does

<2-3 sentences from the ticket Goal + Scope>

## Files changed

<bulleted list of new/modified files>

## Definition of done

<paste the DoD checklist verbatim from the ticket, with completed items checked>

## Tests added

<list of test function names from Tests required, with package path>

EOF
)"
```

Return the PR URL to the user.

## What NOT to do

- Do not modify files outside the ticket's Scope unless fixing a compile error introduced by this ticket.
- Do not open a PR if `go build ./...` or the package tests fail.
- Do not combine multiple tickets into one PR — one ticket, one PR.
- Do not touch `docs/tickets/`, `docs/design/`, or `docs/REQUIREMENTS.md` — these are immutable inputs.
- Do not skip tests — every test named in **Tests required** must be implemented and passing.
