# AI Coding Context Discipline — Setup & Maintenance Prompt

Use this prompt at the start of a project (or paste it into Claude Code / any AI coding tool) to set up and enforce the 5-file context system, so the AI stops hallucinating, forgetting decisions, or redoing/undoing past work.

---

I want you to set up and permanently follow a context-discipline system for this codebase using 5 files. Do the following:

## STEP 1 — CREATE (IF MISSING) OR AUDIT (IF PRESENT)

Check the project root for these 5 files. Create any that don't exist, using the templates below. If they exist, read them fully before doing anything else and tell me if they look stale or contradict what you find in the actual code.

1. **`CLAUDE.md`** — Project map & rules
   - Project purpose (1–2 sentences)
   - Tech stack (languages, frameworks, versions)
   - Folder/module structure and what each part does
   - How to run, build, test, and lint
   - Coding conventions (naming, style, patterns to follow)
   - Explicit "do NOT do this" list (e.g. don't touch X without asking, don't change the DB schema directly, don't add new dependencies without approval)

2. **`ARCHITECTURE.md`** — How it fits together
   - High-level data flow / request flow
   - Key components and their responsibilities
   - Why major design decisions were made this way (short rationale, not just description)
   - Known constraints (performance, security, legacy compatibility)

3. **`DECISIONS.md`** — Lightweight decision log (ADR-style)
   - One entry per non-obvious choice: date, decision, alternatives considered, why this one won
   - Append-only — never delete old entries, even if later reversed (add a new entry noting the reversal instead)

4. **`SESSION_LOG.md`** (or `CHANGELOG.md`) — Running work log
   - One entry per work session: date, what was changed, why, what's left half-done
   - Plain language, skimmable in under a minute

5. **`TODO.md`** (or `KNOWN_ISSUES.md`)
   - Open tasks, known bugs, deliberately deferred work
   - Mark clearly what is intentionally left as-is vs. actually broken

## STEP 2 — WORKING RULES (apply every session, no exceptions)

- **Before writing or changing any code**, read all 5 files. Do not proceed on assumptions if something is already documented.
- **Before proposing an architectural or dependency change**, check `DECISIONS.md` first — if it's already been considered and rejected, say so instead of re-proposing it.
- **Never silently "fix" something listed in `TODO.md`/`KNOWN_ISSUES.md` as intentional** — ask first.
- **At the end of every session**, before finishing:
  - Add an entry to `SESSION_LOG.md` (what changed, why, what's unfinished)
  - Add an entry to `DECISIONS.md` if a meaningful choice was made
  - Update `TODO.md` (remove completed items, add new known issues)
  - Update `CLAUDE.md`/`ARCHITECTURE.md` only if the actual structure/conventions changed
- **If you're not sure something is true about the codebase** (a file's purpose, whether a function is still used, why something was built a certain way), say so explicitly and check the files or the code — never state it as fact from assumption.
- **Flag contradictions**: if the code doesn't match what these files say, stop and tell me which is out of date rather than guessing which is correct.

## STEP 3 — MY REQUEST FORMAT

When I ask for a change, structure your response as:
1. What you checked in the 5 files first
2. What you're about to do and why
3. The actual change
4. What you updated in the 5 files afterward

## STEP 4 — KEEP IT LEAN

These files must stay short and current — stale docs are worse than no docs. If any file grows unwieldy, prune or archive completed/old entries rather than letting it balloon.

---

**Goal:** every session starts with real context instead of assumptions, every non-obvious decision is traceable, nothing gets silently reversed or re-litigated, and I can pick up exactly where things left off without reconstructing history from memory.
