# Event Store

A **serverless-first** kit that simplifies the use of **event-sourcing** and **event-logging** patterns.

It offers two main components:
- A Go module that represents the Go library;
- A Serverless Application that deals with events storage, indexing, and forwarding;


### Features:
- Multi-tenancy friendly stream design that supports global streams with time-stamp and version based sub-streams.
- Push and pull based subscription support for global streams.
- Crypto shredding support for Personal data (PII) at the event level.
- Strong consistent ordering for version-based streams (best-effort ordering for global streams)


### Implementations:

#### Elastic
Built on top of Dynamodb, Lambda functions, and SNS topics.


#### In-Memory
A simplified implementation for testing purposes.




