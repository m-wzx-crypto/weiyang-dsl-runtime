# Weiyang DSL Runtime — Roadmap

> Where this engine is going, and the rules it will not break on the way.
> Every milestone below is gated by tests, not by prose. There are deliberately
> no dates: a milestone is done when its gate is green.

---

## 1. What this engine is

A JSON-defined, event-sourced execution engine. Its design assumption is unusual
and worth stating plainly:

> **The actor may be probabilistic.**

Traditional workflow engines assume every actor is deterministic — a person clicks
approve, a service returns success or failure. The engine's job is routing plus
persistence. Once the actor may be a language model, that assumption fails, and a
different set of guarantees becomes necessary:

| Because the actor may… | the engine must… | status |
| --- | --- | --- |
| …not do what you meant | declare the candidate set up front; an out-of-bounds choice is structurally unreachable | **done** — bounded agency |
| …emit malformed output | validate output against a declared schema before it can touch state | **done** |
| …not be explainable later | record *which* model, *which* prompt version, *which* person produced each decision | **missing** |
| …change under you | reconstruct any past execution exactly, and compare it against another | **partial** — fold/resume exist, comparison does not |
| …cost money or cause harm | make cost and blast radius declared, first-class limits | **missing** |
| …fail as a normal mode | treat failure rate as an observable, not as an exception branch | **missing** |

The engine is already good at the first two rows. **Most of this roadmap is about
the other four.**

---

## 2. Design principles (non-negotiable)

1. **Bounded agency.** A model may only choose among the candidates a definition
   declares — routes, tools, parameter schemas. An out-of-bounds choice cannot
   reach a state transition; it escalates instead.
2. **Model output enters state only through events.** Every inference result is
   schema-validated, journaled, and *then* allowed to drive the state machine.
   There is no side channel.
3. **The core has zero LLM dependencies.** `dsl/` defines contracts; model clients
   live in the host. Non-negotiable, because it is what keeps the kernel embeddable.
4. **Deterministic control plane.** The same journal folds to the same state.
   Model non-determinism is frozen in the log, never in the fold.
5. **The journal is append-only and is the single source of truth.** Summaries,
   snapshots and indexes are rebuildable caches; when they disagree with the log,
   the log wins.
6. **Every milestone ships value on its own.** No pure groundwork releases.

---

## 3. The direction

Three shifts, in dependency order. They are one direction, not three projects:
each one is what makes the next one meaningful.

### 3.1 From tenants to principals

Today the engine knows a `tenant_id` string. It does not know **who** approved a
step, or **which model and prompt version** produced a decision. Every occurrence
is journaled, but the entries carry no signature.

For a workflow engine this is fine. For an engine that sits underneath AI-driven
operations it is the central defect — because the only interesting questions are
*who was allowed to do this* and *who is answerable for it*, and the engine as it
stands cannot answer either.

**Target:** humans and agents become first-class principals, on the same footing
as the process itself.

### 3.2 From definitions to versioned, attributed rules

A process definition is a rule. Rules change. Today a definition has a version
*string*, but nothing records who proposed the change, who owns the rule, or what
the change actually altered.

**Target:** every definition version records its proposer and its accountable
owner; the version history is immutable and diffable; activating a version is
itself a journaled fact. Changing how the system behaves becomes as reviewable as
changing code — and as attributable.

### 3.3 From logs to evidence

`Fold`/`Resume` already rebuild a live instance from its log. What is missing is
the part that makes the log *useful to someone who was not there*: comparison
between two runs, and an export that can be handed to a third party.

**Target:** any past execution can be reconstructed exactly, compared against
another execution of the same definition, and exported as a self-verifying bundle.

---

## 4. Milestones

Dependency-ordered. Each block lists its gate — the test that must pass before the
milestone is considered done.

### M1 — Principals

Introduce a principal model: a human or an agent, with a stable identity.
- Decisions carry attribution: approving human; model + version + prompt version
  for inferences.
- An occurrence cannot be recorded without a principal where one is required.
- The identity model survives fold, snapshot and resume.

**Gate:** a test asserts that a decision recorded without a principal is rejected,
and that a folded instance reproduces principals field-for-field.

### M2 — Versioned, attributed rules

- Definition versions carry proposer + accountable owner.
- Version history is immutable, ordered, and diffable.
- Activation (`draft → active`) requires both fields and is journaled.

**Gate:** a test asserts an activation without proposer/owner fails, and that two
versions of the same definition produce a stable structural diff.

### M3 — Evidence

- Replay any instance as of any point; compare two executions of one definition
  (routing agreement, escalation rate, cost).
- Export an evidence bundle: definition hash, version history, execution log,
  principal attribution — hash-chained and verifiable offline.

**Gate:** an exported bundle re-verifies against its own hash chain, and tampering
with any entry is detected.

### M4 — Bounded capability

Extend bounded agency from routing to tools.
- `ai` nodes declare a tool whitelist; each tool's parameters are schema-validated.
- The tool-call loop is bounded by `max_iterations` **and** an effect budget —
  amount, rate, and blast radius — not only a token/cost budget.
- A composed sequence of individually-legal calls that exceeds the declared effect
  budget is itself a violation, not just a single out-of-bounds call.

**Gate:** tests for (a) an undeclared tool being unreachable, (b) an effect-budget
breach halting the loop, (c) a trajectory of legal calls that breaches the budget
being caught.

### M5 — Declared autonomy and provable limits

- Each definition declares an autonomy level; execution enforces it.
- Limits (amount, frequency, reach) are checked by the **static analyzer at deploy
  time**, so "can this process ever move more than ¥X" is answerable without
  running it.
- Level changes are journaled facts, derived from execution history rather than
  configured by hand.

**Gate:** a definition whose declared limits can be exceeded is rejected before it
can be activated.

### M6 — Intent units

A definition node may declare a complete unit of work: goal, constraints,
acceptance criteria, resource budget, responsible principal, deadline. Units
decompose into subprocesses.

**Gate:** an acceptance criterion that is declared but unverifiable fails
validation at deploy time.

### Parallel track — Service form

The engine is currently an embeddable Go module with no `main` package. Hosts in
other languages cannot use it. A runnable server plus thin client SDKs is
distribution work, not kernel work, and can start whenever there is a host that
needs it.

---

## 5. Non-goals

Explicitly out of scope, so that scope creep has a name:

1. **Not a general agent framework.** We do not compete with libraries that help
   you write agents. We are what runs underneath them.
2. **Not a low-code canvas.** Visual orchestration is a different product with a
   different buyer.
3. **No model training or fine-tuning.** The engine consumes model capability; it
   does not produce it.
4. **No LLM SDK in the core** (principle 3).
5. **No self-built vector store or embedding service.** Retrieval is a host
   concern; Qdrant is already in the infrastructure layer.
6. **No model-generated rules without a gate.** Anything a model generates must
   pass validation and an approval step before it can go active.

---

## 6. How progress is reported

A milestone is not "in progress" or "nearly done" — it is open or its gate is
green. Where a claim in this document can be checked by running something, the
check is expected to live in the test suite:

- `go test ./... -race` green, in CI, for **every** module in the repository.
  `dsl/` and `store-postgres/` are separate modules; CI today builds and tests
  only `dsl/`. Bringing `store-postgres/` into CI is a prerequisite for M1, not a
  follow-up to it — M1 changes the shape of what the journal stores, and the
  Postgres store is what stores it.
- Fold exactness enforced by property tests, not convention.
- Every interception (out-of-bounds choice, schema violation, budget breach,
  human override) appends a journaled occurrence of a declared kind — asserted by
  test, not by documentation.

If this file and the test suite disagree, the test suite is right.
