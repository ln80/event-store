
# Event Store
[![Coverage Status](https://coveralls.io/repos/github/ln80/event-store/badge.svg?branch=main)](https://coveralls.io/github/ln80/event-store?branch=main)
![ci status](https://github.com/ln80/event-store/actions/workflows/module.yaml/badge.svg)

A core implementation for **event-sourcing** and **event-logging** patterns.


### Features:
- Multi-tenancy friendly stream design that supports global streams with time-stamp and version-based sub-streams.
- Push and pull-based subscription support for global streams.
- Crypto shredding support for Personal data (PII) at the event level.
- Strong consistent ordering for version-based streams (best-effort ordering for global streams)
- Event versioning leveraging AVRO encoding format.


### Event Store Implementations:

#### In-Memory
Simplified implementation for testing purposes.


### Event Encoding Formats:

#### JSON:
- The default encoding format for simplicity and ease of use. However, it does not support schema evolution, making it less suitable for systems that require long-term maintenance and backward compatibility.

#### AVRO:
- A more robust encoding format that supports **schema evolution**, making it ideal for systems that need to evolve over time. **AVRO** provides efficient serialization and deserialization while ensuring backward and forward compatibility between different versions of events.
