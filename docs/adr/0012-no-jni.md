# ADR 0012 — No JNI

**Status:** Accepted

## Context
JNI dependencies (rocksdbjni, native zstd, ISA-L) would obscure the architecture behind opaque native blobs.

## Decision
Pure JDK + Gradle. No native dependencies anywhere in the build. CRC32C uses java.util.zip.CRC32C.

## Consequences
+ A reader can open the JAR and trace every architectural claim to Java source. − Performance is not representative; that is explicitly a non-goal.
