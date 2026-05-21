# Create BunnyMQ implementation tickets

Decompose a feature request into one or more ticket files in `docs/tickets/` and write them to disk.

Feature request (may be empty or vague — clarify before proceeding): **$ARGUMENTS**

---

## Step 1 — Read project context

Run these in parallel before doing anything else:

1. Find the current highest ticket number so you know where to continue:
   ```
   find docs/tickets -name "T-[0-9]*.md" | grep -oE 'T-[0-9]+' | sort -t- -k2 -n | tail -1
   ```

2. List existing milestone directories (you will need to pick one or create a new one):
   ```
   ls docs/tickets/
   ```

3. Read a recent ticket in full to internalise the exact format — find the file with the highest number and read it:
   ```
   find docs/tickets -name "T-[0-9]*.md" | sort -t- -k2 -n | tail -1
   ```

4. If `$ARGUMENTS` names specific packages, files, or design docs — read them now so you can write accurate Scope and Context sections. Also read `pkg/client/` types if the request touches the client library, or `docs/design/` sections if a design doc is directly relevant.

---

## Step 2 — Clarify requirements

Examine what you read. If `$ARGUMENTS` is concrete enough to plan tickets without guessing (clear feature, identifiable files, obvious milestone), proceed to Step 3.

Otherwise, ask the user targeted questions before doing any planning. Use the AskUserQuestion tool for multiple-choice decisions (milestone selection, effort, yes/no choices). Ask open-ended questions as plain text. Never ask more than 4 questions in one turn.

Things to clarify when they are not obvious from the codebase:

- **What**: the feature in one sentence — what does the user want implemented?
- **Scope boundary**: one PR or several? Any natural split points the user already has in mind?
- **Milestone**: which existing M-N does this belong to, or is this a new area that needs its own milestone directory?
- **Constraints**: naming conventions, interface shapes already decided, must-not-break areas, integration test requirements?

Do NOT ask about things you can read from the code. Do NOT ask "Is this correct?" — just proceed once you have enough to plan.

---

## Step 3 — Propose the decomposition

Before writing any files, present the planned tickets to the user in this format:

```
Proposed tickets:

T-NNN   <Title>         Effort: S    Deps: none
T-NNN+1 <Title>         Effort: M    Deps: T-NNN
T-NNN+2 <Title>         Effort: M    Deps: T-NNN
```

Add one sentence explaining the dependency order and why the breakdown is sensible. Then ask the user to confirm or request changes. Do not proceed to Step 4 until the user explicitly approves (a message like "looks good", "go ahead", "yes", or similar counts).

---

## Step 4 — Write ticket files

For each confirmed ticket, write a file at `docs/tickets/<milestone-dir>/T-NNN-<slug>.md`.

**Path rules:**
- Milestone dir: pick from the `ls docs/tickets/` output. Use an existing directory if the feature belongs to that milestone's scope. Create a new `M<N>-<short-name>/` directory (e.g. `M6-cli/`, `M7-observability/`) only if none of the existing milestones fits.
- File name: `T-NNN-<slug>.md` where slug is the ticket title in lowercase, spaces replaced with hyphens, maximum 5 words (e.g. `T-072-grpc-auth-interceptor.md`).
- NNN: assign numbers sequentially starting from (current max + 1). If you are creating three tickets: NNN, NNN+1, NNN+2.

**Ticket template — fill every section, no "TBD" anywhere:**

```markdown
# T-NNN: <Title>

**Milestone:** <Full milestone label, e.g. "M5 — Integration and polish">
**Effort:** <XS | S | M | L>
**Status:** TODO

## Goal

<One paragraph. Written as a capability statement: "Implement X so that Y can Z."
State what the ticket delivers, not how.>

## Context

<Why this ticket exists. Which design doc section it implements (link it).
Which existing packages it extends. 2–4 sentences.>

References:
- [`pkg/path/file.go`](../../pkg/path/file.go) — what it provides
- [`docs/design/NN-name.md`](../../docs/design/NN-name.md) — relevant section

## Scope

<Bulleted list of every file to create or modify.
For each file: state the package, then list the types/functions/methods with their exact signatures.
"Create" for new files, "Modify" for existing ones.>

- **Create** `path/to/file.go` — `package name`:
  - `TypeName` — description
  - `func FuncName(args) (returns)` — description
- **Modify** `path/to/existing.go` — what to add or change

## Out of scope

<At least one concrete thing explicitly excluded from this ticket with a brief reason.>

## Definition of done

<Verifiable checklist. Every item must be checkable without running a full cluster unless marked as integration-only.>

- [ ] `go build ./...` passes with zero errors.
- [ ] `go test ./path/to/package/...` passes.
- [ ] <Functional check — e.g., "TopicInfo printed by `topic describe` contains all six fields.">
- [ ] <Additional checks as needed>

## Tests required

<List of exact Go test function names. Each name must be a real `TestXxx` identifier, not a category.
One sentence per test describing what it verifies.>

- `TestFunctionName` — verifies that ...
- `TestFunctionName2` — verifies that ...

## Dependencies

<List other T-NNN tickets whose output (files, types, packages) this ticket's code imports or calls.
Write "None" if the ticket is standalone.>

- T-NNN (<package or file that must exist before this ticket can compile>).

## Notes

<Non-obvious implementation constraints, chosen algorithms, workarounds, gotchas.
At least two sentences. Do not describe what the code does — explain why a non-obvious choice was made.>
```

---

## Quality rules for every ticket you write

- **Scope must be complete**: every file the implementer will touch must appear. No implied files.
- **Tests required must name real functions**: `TestParseAcks`, not "unit tests for flag parsing".
- **Definition of done must be verifiable**: "prints a table with NAME column" is verifiable; "works correctly" is not.
- **Notes must be non-trivial**: at least one constraint or gotcha that would surprise an implementer reading only the Scope.
- **No cross-ticket scope bleed**: if ticket A and ticket B both need a helper, assign it to whichever ticket produces the package first, and reference it in the other ticket's Dependencies.

---

## After writing the files

List every file path you created and echo the final dependency graph (one line per ticket, with arrows). Do not open PRs or create branches — ticket files only.
