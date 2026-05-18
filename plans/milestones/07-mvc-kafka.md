# Milestone 7 - MVC Kafka

## Design Reference

- Source: [reliable-message-design.md - 4.7 MVC Kafka Adapter](../../reliable-message-design.md#47-mvc-kafka-adapter)
- Transport rules: [reliable-message-design.md - Transport Rules](../../reliable-message-design.md#transport-rules)
- Roadmap: [reliable-message-design.md - Milestone 7 - MVC Kafka](../../reliable-message-design.md#milestone-7---mvc-kafka)

## Goal

Add blocking MVC Kafka support with reliable publish and consume semantics.

## Scope

- `reliable-message-kafka-mvc`
- Kafka publisher
- Kafka listener
- Kafka DLT
- Kafka retry topics
- Manual offset commit
- Partition key support

## Out Of Scope

- Reactor Kafka
- WebFlux listener APIs
- RabbitMQ behavior changes

## Deliverables

- Kafka publisher implementation
- Kafka listener container integration
- Retry topic naming and routing
- DLT routing
- Manual offset commit after success
- Partition key as Kafka record key

## Verification

- Published records use configured topic prefix
- Partition key is used as Kafka record key
- Offsets commit only after successful handler execution
- Failed records follow retry topic and DLT behavior

## Status

- [ ] Not started
- [ ] In progress
- [x] Done
