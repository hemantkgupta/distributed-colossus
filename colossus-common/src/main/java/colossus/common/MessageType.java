package colossus.common;

/**
 * Wire message tags. The numeric code is the single byte written in the frame header
 * (§4.2 of the implementation plan). Grouped by the plane boundary the message crosses.
 */
public enum MessageType {
    // Client → Curator
    OPEN_FILE(0x10),
    CREATE_FILE(0x11),
    ALLOCATE_CHUNK(0x12),
    GET_CHUNK_LOCATIONS(0x13),
    EXTEND_LEASE(0x14),
    CLOSE_FILE(0x15),
    STAT(0x16),
    DELETE_FILE(0x17),

    // Curator → Client
    OPEN_FILE_RESPONSE(0x20),
    CREATE_FILE_RESPONSE(0x21),
    ALLOCATE_CHUNK_RESPONSE(0x22),
    GET_CHUNK_LOCATIONS_RESPONSE(0x23),
    EXTEND_LEASE_RESPONSE(0x24),
    CLOSE_FILE_RESPONSE(0x25),
    STAT_RESPONSE(0x26),
    DELETE_FILE_RESPONSE(0x27),
    WRONG_SHARD_REDIRECT(0x2E),
    ERROR_RESPONSE(0x2F),

    // Client → D-server
    READ_EXTENT(0x30),
    PUSH_BYTES(0x31),
    COMMIT_WRITE(0x32),

    // D-server → Client
    READ_EXTENT_RESPONSE(0x40),
    PUSH_BYTES_RESPONSE(0x41),
    COMMIT_WRITE_RESPONSE(0x42),
    STALE_PLACEMENT_REPLY(0x4F),

    // D-server → D-server (daisy chain)
    REPLICATE(0x50),
    REPLICATE_ACK(0x51),

    // Curator/Custodian/client → BigTable
    BT_PUT(0x60),
    BT_GET(0x61),
    BT_CAS(0x62),
    BT_SCAN(0x63),

    // BigTable → caller
    BT_RESPONSE(0x70),

    // BigTable tablet-server ↔ tablet-server (replication)
    BT_REPLICATE_PUT(0x78),
    BT_REPLICATE_ACK(0x79),

    // Custodian → D-server
    CUSTODIAN_DISPATCH(0x80),
    CUSTODIAN_DISPATCH_ACK(0x81),

    // D-server → BigTable (heartbeat status row) / control planes scan it
    DSERVER_HEARTBEAT(0x90),

    // Curator → D-server (lease grants)
    GRANT_LEASE(0xA0),
    REVOKE_LEASE(0xA1),

    // Curator → Curator (ownership handoff)
    SHARD_OWNERSHIP_CLAIM(0xB0),
    SHARD_OWNERSHIP_RELEASE(0xB1);

    private final int code;

    MessageType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    private static final MessageType[] BY_CODE = new MessageType[256];

    static {
        for (MessageType t : values()) {
            BY_CODE[t.code] = t;
        }
    }

    public static MessageType fromCode(int code) {
        MessageType t = (code >= 0 && code < 256) ? BY_CODE[code] : null;
        if (t == null) {
            throw new IllegalArgumentException("Unknown message type code: 0x" + Integer.toHexString(code));
        }
        return t;
    }
}
