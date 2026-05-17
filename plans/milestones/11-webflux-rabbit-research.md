# Milestone 11 - WebFlux Rabbit Research

## Design Reference

- Source: [reliable-message-design.md - 5.7 WebFlux RabbitMQ Adapter](../../reliable-message-design.md#57-webflux-rabbitmq-adapter)
- Runtime rules: [reliable-message-design.md - Runtime Rules](../../reliable-message-design.md#runtime-rules)
- Roadmap: [reliable-message-design.md - Milestone 11 - WebFlux Rabbit Research](../../reliable-message-design.md#milestone-11---webflux-rabbit-research)

## Goal

Decide whether RabbitMQ WebFlux support can be provided honestly without claiming fake non-blocking behavior.

## Scope

- Evaluate non-blocking RabbitMQ client options
- Assess Spring AMQP limitations for reactive usage
- Decide support level and positioning
- Document constraints and tradeoffs

## Out Of Scope

- Production WebFlux RabbitMQ implementation before research is complete
- Blocking listener container hidden behind a reactive API
- Claims of fully non-blocking RabbitMQ support without evidence

## Deliverables

- Research notes
- Recommendation: support, experimental support, or no support
- If supported, proposed module boundary for `reliable-message-rabbit-webflux`
- Documented limitations

## Verification

- Recommendation identifies client/runtime behavior clearly
- Blocking paths are documented if any exist
- Final decision aligns with the design rule: do not fake full non-blocking behavior

## Status

- [ ] Not started
- [ ] In progress
- [ ] Done
