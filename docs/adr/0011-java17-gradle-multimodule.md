# ADR 0011 — Java 17 + Gradle multi-module + JUnit 5 + AssertJ

**Status:** Accepted

## Context
The repo is one of a paper arc (GFS, RADOS, Colossus, BlueStore) that should share a toolchain so a reader transfers conventions directly.

## Decision
Java 17, Gradle multi-module, JUnit 5 + AssertJ, one focused class per concept, records for value types, sealed interfaces for exhaustive enums, defensive byte-array copies. Pattern-switch avoided (preview in 17); instanceof chains used instead.

## Consequences
+ Consistent with sibling repos; portable. − Java 17 lacks some later ergonomics (virtual threads, record patterns).
