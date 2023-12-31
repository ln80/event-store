
# Event Store
[![Coverage Status](https://coveralls.io/repos/github/ln80/event-store/badge.svg?branch=main)](https://coveralls.io/github/ln80/event-store?branch=main)
![ci status](https://github.com/ln80/event-store/actions/workflows/module.yaml/badge.svg)

A **serverless-first** kit that simplifies the use of **event-sourcing** and **event-logging** patterns.

It offers two main components:
- A Go module that represents the Go library;
- A Serverless Application that deals with events storage, indexing, and forwarding;


### Features:
- Multi-tenancy friendly stream design that supports global streams with time-stamp and version-based sub-streams.
- Push and pull-based subscription support for global streams.
- Crypto shredding support for Personal data (PII) at the event level.
- Strong consistent ordering for version-based streams (best-effort ordering for global streams)


### Event Store Implementations:

#### Elastic
Built on top of Dynamodb, Lambda functions, and SNS topics.


#### In-Memory
Simplified implementation for testing purposes.


### Event Encoding Formats:

#### JSON
The default encoding format; lacks support for event schema evolution.

#### AVRO
With a built-in AWS Glue schema registry integration, event schemas can evolve while ensuring backward compatibility in a fail-fast approach.

