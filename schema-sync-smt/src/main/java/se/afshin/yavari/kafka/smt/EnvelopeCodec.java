package se.afshin.yavari.kafka.smt;

import java.nio.ByteBuffer;

/**
 * Wire-format codec for schema-registry envelopes. Each registry family frames a record
 * as {@code [0x00 magic][schema id][payload]} but with a different id width:
 * <ul>
 *   <li>{@link #APICURIO}: 8-byte big-endian {@code globalId} (Apicurio serdes default).
 *   <li>{@link #CONFLUENT}: 4-byte big-endian {@code schemaId} (Confluent wire format,
 *       also served by Apicurio's {@code ccompat} API).
 * </ul>
 * Source and target codecs may differ; {@link #encode} always re-frames from the
 * payload offset reported by the source codec's {@link #parse}.
 */
public enum EnvelopeCodec {
    APICURIO(8),
    CONFLUENT(4);

    private static final byte MAGIC = 0x00;

    private final int idWidth;

    EnvelopeCodec(int idWidth) {
        this.idWidth = idWidth;
    }

    /** Parsed header: the schema id and the offset at which the payload starts. */
    public record Parsed(long id, int payloadOffset) {}

    /** Header length for this codec (magic byte + id). */
    public int headerLength() {
        return 1 + idWidth;
    }

    /** Parses the envelope header, or returns {@code null} when {@code bytes} is not an
     *  envelope of this format (null, too short, or wrong magic byte). */
    public Parsed parse(byte[] bytes) {
        if (bytes == null || bytes.length < headerLength() || bytes[0] != MAGIC) return null;
        ByteBuffer bb = ByteBuffer.wrap(bytes, 1, idWidth);
        long id = idWidth == 8 ? bb.getLong() : bb.getInt();
        return new Parsed(id, headerLength());
    }

    /** Builds a new envelope of this format carrying {@code id} and the payload of
     *  {@code src} starting at {@code payloadOffset}. */
    public byte[] encode(long id, byte[] src, int payloadOffset) {
        int payloadLen = src.length - payloadOffset;
        ByteBuffer out = ByteBuffer.allocate(headerLength() + payloadLen);
        out.put(MAGIC);
        if (idWidth == 8) {
            out.putLong(id);
        } else {
            if (id < Integer.MIN_VALUE || id > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("Schema id " + id
                        + " does not fit the 4-byte Confluent envelope");
            }
            out.putInt((int) id);
        }
        out.put(src, payloadOffset, payloadLen);
        return out.array();
    }

    /** Case-insensitive lookup used by the SMT config. */
    public static EnvelopeCodec of(String name) {
        return valueOf(name.trim().toUpperCase());
    }
}
