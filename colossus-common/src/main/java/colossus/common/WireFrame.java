package colossus.common;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * The outer framing for every RPC (§4.1):
 *
 * <pre>
 * +--------------+--------+----------+----------------+----------+
 * | length (4B)  | type   | reqId    | shard / epoch  | payload  |
 * | big-endian   | (1 B)  | (4 B)    | (4 B)          | (rest)   |
 * +--------------+--------+----------+----------------+----------+
 * </pre>
 *
 * {@code length} is the total frame length including itself. {@code shardOrEpoch} is
 * interpreted per type: the target shard id for Curator RPCs, the client's cached
 * placement epoch for D-server RPCs, 0 otherwise.
 */
public record WireFrame(MessageType type, int reqId, int shardOrEpoch, byte[] payload) {

    private static final int HEADER_AFTER_LENGTH = 1 + 4 + 4; // type + reqId + shard/epoch

    public WireFrame {
        payload = payload.clone();
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    public static WireFrame of(MessageType type, int reqId, int shardOrEpoch, byte[] payload) {
        return new WireFrame(type, reqId, shardOrEpoch, payload);
    }

    public byte[] encode() {
        int total = 4 + HEADER_AFTER_LENGTH + payload.length;
        WireWriter w = new WireWriter();
        w.i32(total).u8(type.code()).i32(reqId).i32(shardOrEpoch);
        byte[] head = w.toByteArray();
        byte[] out = new byte[total];
        System.arraycopy(head, 0, out, 0, head.length);
        System.arraycopy(payload, 0, out, head.length, payload.length);
        return out;
    }

    /** Decode a complete frame from a byte[] produced by {@link #encode()}. */
    public static WireFrame decode(byte[] framed) {
        WireReader r = new WireReader(framed);
        int total = r.i32();
        if (total != framed.length) {
            throw new IllegalStateException("frame length mismatch: header=" + total + " actual=" + framed.length);
        }
        MessageType type = MessageType.fromCode(r.u8());
        int reqId = r.i32();
        int shardOrEpoch = r.i32();
        int payloadLen = total - 4 - HEADER_AFTER_LENGTH;
        byte[] payload = new byte[payloadLen];
        for (int i = 0; i < payloadLen; i++) {
            payload[i] = (byte) r.u8();
        }
        return new WireFrame(type, reqId, shardOrEpoch, payload);
    }

    public void writeTo(OutputStream out) throws IOException {
        out.write(encode());
        out.flush();
    }

    /** Read exactly one frame from a stream. Returns null on clean EOF before any bytes. */
    public static WireFrame readFrom(InputStream in) throws IOException {
        DataInputStream din = new DataInputStream(in);
        int total;
        try {
            total = din.readInt();
        } catch (EOFException eof) {
            return null;
        }
        if (total < 4 + HEADER_AFTER_LENGTH) {
            throw new IOException("bad frame length: " + total);
        }
        byte[] framed = new byte[total];
        framed[0] = (byte) (total >>> 24);
        framed[1] = (byte) (total >>> 16);
        framed[2] = (byte) (total >>> 8);
        framed[3] = (byte) total;
        din.readFully(framed, 4, total - 4);
        return decode(framed);
    }

    public WireReader reader() {
        return new WireReader(payload);
    }
}
