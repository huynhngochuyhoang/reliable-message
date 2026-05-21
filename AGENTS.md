# AGENTS.md

Behavioral guidelines to reduce common LLM coding mistakes. Merge with project-specific instructions as needed.

**Tradeoff:** These guidelines bias toward caution over speed. For trivial tasks, use judgment.

## 1. Think Before Coding

**Do not assume. Do not hide uncertainty. Surface tradeoffs.**

Before implementing:

* State assumptions explicitly. If uncertain, ask.
* If multiple interpretations exist, present them instead of choosing silently.
* If a simpler approach exists, mention it. Push back when appropriate.
* If something is unclear, stop and ask for clarification.

## 2. Simplicity First

**Write the minimum code required to solve the problem. Avoid speculative design.**

* Do not add features beyond the request.
* Do not introduce abstractions for single-use code.
* Do not add configurability or flexibility unless requested.
* Avoid handling impossible scenarios.
* If the solution feels overengineered, simplify it.

Ask yourself:

> "Would a senior engineer consider this unnecessarily complex?"

If yes, rewrite it more simply.

## 3. Surgical Changes

**Modify only what is necessary.**

When editing existing code:

* Do not "improve" unrelated code, comments, or formatting.
* Do not refactor unrelated areas.
* Match the existing project style, even if you would design it differently.
* If unrelated dead code is noticed, mention it instead of removing it.

When your changes create unused code:

* Remove imports, variables, or functions made unused by your change.
* Do not remove pre-existing dead code unless explicitly asked.

Rule:

> Every changed line should directly support the requested task.

## 4. Goal-Driven Execution

**Define success criteria and verify them.**

Convert requests into measurable goals:

* "Add validation" → Write failing tests for invalid input, then make them pass.
* "Fix the bug" → Reproduce the bug with a test, then fix it.
* "Refactor X" → Ensure behavior is unchanged before and after refactoring.

For multi-step tasks, define a lightweight execution plan:

```text
1. [Step] → verify: [check]
2. [Step] → verify: [check]
3. [Step] → verify: [check]
```

Strong success criteria reduce ambiguity and avoid unnecessary rework.

---

These guidelines are effective when:

* Diffs contain fewer unnecessary changes
* Overengineering decreases
* Clarifying questions happen before implementation instead of after mistakes

## 5. Test First for Reliability Features

**For concurrency, messaging, retry, reactive, RPC, or threading related changes: tests come before implementation.**

Required workflow:

```text
1. Write failing tests first
2. Reproduce the expected behavior or bug
3. Implement the minimum code required
4. Verify all tests pass
5. Verify no unrelated behavior changed
```

Do not implement:

* concurrency code
* retry code
* reactive bridges
* thread coordination
* ack/nack flows
* outbox logic
* RPC timeout handling

without tests.

Required test coverage for messaging/reliability features:

```text
success flow
failure flow
timeout flow
duplicate handling
retry behavior
ack behavior
event-loop safety
concurrency limits
```

If a feature cannot be tested properly:

* stop
* explain why
* ask for clarification

---

## 6. Reactive and Concurrency Safety

**Do not introduce hidden blocking behavior.**

Never:

* call `block()` on Netty event loop threads
* execute blocking I/O inside Reactor pipelines without explicit isolation
* wrap blocking code with `Mono.just(...)`
* pretend blocking infrastructure is reactive
* create unbounded thread pools
* create unbounded queues
* use unbounded `flatMap`

If blocking infrastructure is required:

* isolate it behind a dedicated executor/scheduler
* document it explicitly
* expose metrics
* apply concurrency limits

Required mindset:

```text
async != non-blocking
virtual threads != reactive
Mono != safe by default
```

For WebFlux code:

* preserve Reactor Context
* avoid hidden scheduler switching
* avoid accidental thread hopping
* make concurrency boundaries explicit

---

## 7. Messaging and RPC Semantics

**Do not mix event messaging semantics with RPC semantics.**

Event messaging:

```text
publish/consume
outbox
retry queues
DLQ
eventual consistency
idempotency
```

RPC:

```text
request/response
timeout
retry
circuit breaker
bulkhead
```

Rules:

* Do not add outbox to normal RPC flows
* Do not use AsyncRabbitTemplate for event publishing
* Do not model synchronous RPC as eventual event flow
* Keep RPC abstractions separate from event messaging abstractions

Required separation:

```text
ReliablePublisher != ReliableRpcClient
```

---

## 8. Explicit Architecture Boundaries

**Do not hide infrastructure behavior behind misleading abstractions.**

Bad examples:

```text
"fully reactive RabbitMQ" built on blocking Spring AMQP
universal transport abstraction hiding Kafka vs RPC semantics
fake async wrappers around blocking code
```

Good examples:

```text
blocking bridge
RPC bridge
event messaging adapter
hybrid mode
```

Always document:

* blocking boundaries
* thread ownership
* retry semantics
* delivery guarantees
* limitations

---

## 9. Small PR Discipline

**Prefer many small correct changes over large risky implementations.**

For complex features:

* split into small milestones
* commit infrastructure first
* validate each layer independently

Preferred order:

```text
config
executor/scheduler
concurrency guard
tests
core implementation
metrics/tracing
failure handling
integration tests
```

Avoid:

* giant multi-purpose PRs
* mixing refactor + feature work
* changing architecture and business logic simultaneously

---

## 10. Review Before Implementation

For non-trivial tasks, first produce:

```text
- assumptions
- files to change
- classes to add
- tests to add
- risks
- what will NOT be implemented
```

Do not start implementation until the plan is internally consistent.

If the task is ambiguous:

* stop
* ask clarification questions
* do not silently guess architecture

---

## 11. Reliability and Failure Semantics

**Failure handling is part of the feature, not an afterthought.**

Every messaging/concurrency feature must define:

```text
what happens on timeout
what happens on retry exhaustion
what happens on duplicate delivery
what happens on partial failure
what happens on reconnect
what happens on executor saturation
```

Never leave failure semantics implicit.

---

## 12. Production Honesty

**Do not claim stronger guarantees than the implementation actually provides.**

Do not claim:

* fully reactive
* exactly-once
* non-blocking
* guaranteed ordering
* lossless delivery

unless the implementation truly guarantees it.

Prefer explicit wording:

```text
blocking bridge
at-least-once
best-effort ordering
hybrid mode
```

Clarity is more important than marketing language.