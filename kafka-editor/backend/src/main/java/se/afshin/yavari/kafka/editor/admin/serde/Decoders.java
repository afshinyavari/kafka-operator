package se.afshin.yavari.kafka.editor.admin.serde;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

/** Static helpers that convert between typed payloads and human-readable text. */
public final class Decoders {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_HEX_BYTES = 256;

    private Decoders() {
    }

    /** Avro binary -> Avro JSON text. */
    public static String decodeAvro(String schemaText, byte[] payload)
            throws Exception {
        Schema schema = new Schema.Parser().parse(schemaText);
        GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
        Object value =
                reader.read(null, DecoderFactory.get().binaryDecoder(payload, null));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out, true);
        reader.getData().createDatumWriter(schema).write(value, encoder);
        encoder.flush();
        return out.toString(StandardCharsets.UTF_8);
    }

    /** Avro JSON text -> Avro binary (for schema-aware produce). */
    public static byte[] encodeAvro(String schemaText, String json)
            throws Exception {
        Schema schema = new Schema.Parser().parse(schemaText);
        GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
        Object datum =
                reader.read(null, DecoderFactory.get().jsonDecoder(schema, json));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<Object>(schema).write(datum, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    static String prettyJson(byte[] payload) throws Exception {
        Object tree = MAPPER.readTree(payload);
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(tree);
    }

    /** UTF-8 round-trip — only safe on bytes already validated by isLikelyUtf8. */
    static String utf8(byte[] payload) {
        return new String(payload, StandardCharsets.UTF_8);
    }

    static boolean isLikelyUtf8(byte[] bytes) {
        if (bytes.length == 0) {
            return false;
        }
        CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        String text;
        try {
            text = decoder.decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException e) {
            return false;
        }
        int total = text.length();
        if (total == 0) {
            return false;
        }
        int printable = 0;
        for (int i = 0; i < total; i++) {
            char c = text.charAt(i);
            if (c >= 0x20 && c < 0x7F) {
                printable++;
            } else if (c == '\n' || c == '\r' || c == '\t') {
                printable++;
            } else if (c >= 0xA0 && c <= 0xFFFC) {
                printable++;
            }
        }
        return printable * 100 / total >= 95;
    }

    static String hexDump(byte[] bytes) {
        int n = Math.min(bytes.length, MAX_HEX_BYTES);
        StringBuilder sb = new StringBuilder(n * 4);
        for (int row = 0; row < n; row += 16) {
            sb.append(String.format("%08x  ", row));
            int end = Math.min(row + 16, n);
            for (int i = row; i < end; i++) {
                sb.append(String.format("%02x ", bytes[i]));
            }
            for (int i = end; i < row + 16; i++) {
                sb.append("   ");
            }
            sb.append(" |");
            for (int i = row; i < end; i++) {
                char c = (char) (bytes[i] & 0xff);
                sb.append((c >= 0x20 && c < 0x7F) ? c : '.');
            }
            sb.append("|\n");
        }
        if (bytes.length > MAX_HEX_BYTES) {
            sb.append(String.format("… (%d more bytes)%n", bytes.length - MAX_HEX_BYTES));
        }
        return sb.toString();
    }
}
