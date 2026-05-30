package colossus.dmclock;

/** A request paired with the virtual-time tags assigned to it at submission. */
public record TaggedRequest(Request request, Tag tag) {}
