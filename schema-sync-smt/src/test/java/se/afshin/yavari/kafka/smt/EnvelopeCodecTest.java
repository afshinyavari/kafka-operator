package se.afshin.yavari.kafka.smt;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class EnvelopeCodecTest {

    private static final byte[] PAYLOAD = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);

    private static byte[] apicurio(long id) {
        ByteBuffer bb = ByteBuffer.allocate(9 + PAYLOAD.length);
        bb.put((byte) 0).putLong(id).put(PAYLOAD);
        return bb.array();
    }

    private static byte[] confluent(int id) {
        ByteBuffer bb = ByteBuffer.allocate(5 + PAYLOAD.length);
        bb.put((byte) 0).putInt(id).put(PAYLOAD);
        return bb.array();
    }

    @Test
    void apicurioParsesGlobalIdAndPayloadOffset() {
        EnvelopeCodec.Parsed p = EnvelopeCodec.APICURIO.parse(apicurio(42L));
        assertThat(p.id()).isEqualTo(42L);
        assertThat(p.payloadOffset()).isEqualTo(9);
    }

    @Test
    void apicurioRejectsShortOrNonMagicBytes() {
        assertThat(EnvelopeCodec.APICURIO.parse(new byte[8])).isNull();
        assertThat(EnvelopeCodec.APICURIO.parse("hello world!".getBytes(StandardCharsets.UTF_8))).isNull();
        assertThat(EnvelopeCodec.APICURIO.parse(null)).isNull();
    }

    @Test
    void confluentParsesInt32SchemaId() {
        EnvelopeCodec.Parsed p = EnvelopeCodec.CONFLUENT.parse(confluent(7));
        assertThat(p.id()).isEqualTo(7L);
        assertThat(p.payloadOffset()).isEqualTo(5);
    }

    @Test
    void confluentRejectsShortOrNonMagicBytes() {
        assertThat(EnvelopeCodec.CONFLUENT.parse(new byte[4])).isNull();
        assertThat(EnvelopeCodec.CONFLUENT.parse("hello".getBytes(StandardCharsets.UTF_8))).isNull();
    }

    @Test
    void confluentPayloadToApicurioEnvelopeReframesHeader() {
        byte[] in = confluent(7);
        EnvelopeCodec.Parsed p = EnvelopeCodec.CONFLUENT.parse(in);
        byte[] out = EnvelopeCodec.APICURIO.encode(1234L, in, p.payloadOffset());
        assertThat(out).isEqualTo(apicurio(1234L));
    }

    @Test
    void apicurioPayloadToConfluentEnvelopeReframesHeader() {
        byte[] in = apicurio(99L);
        EnvelopeCodec.Parsed p = EnvelopeCodec.APICURIO.parse(in);
        byte[] out = EnvelopeCodec.CONFLUENT.encode(3L, in, p.payloadOffset());
        assertThat(out).isEqualTo(confluent(3));
    }

    @Test
    void confluentEncodeRejectsIdsOutsideInt32() {
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> EnvelopeCodec.CONFLUENT.encode(1L << 40, confluent(1), 5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void parseByName() {
        assertThat(EnvelopeCodec.of("apicurio")).isSameAs(EnvelopeCodec.APICURIO);
        assertThat(EnvelopeCodec.of("CONFLUENT")).isSameAs(EnvelopeCodec.CONFLUENT);
    }
}
