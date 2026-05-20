package se.afshin.yavari.kafka.ui.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;

import java.io.ByteArrayOutputStream;

/** Small static helpers that turn typed payloads into human-readable text. */
final class Decoders {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int MAX_HEX_BYTES = 256;

    private Decoders() {}

    static String decodeAvro(String schemaText, byte[] payload) throws Exception {
        Schema schema = new Schema.Parser().parse(schemaText);
        GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
        Object value = reader.read(null, DecoderFactory.get().binaryDecoder(payload, null));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        JsonEncoder enc = EncoderFactory.get().jsonEncoder(schema, out, true);
        reader.getData().createDatumWriter(schema).write(value, enc);
        enc.flush();
        return out.toString(java.nio.charset.StandardCharsets.UTF_8);
    }

    static String prettyJson(byte[] payload) throws Exception {
        Object tree = MAPPER.readTree(payload);
        return MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(tree);
    }

    static String prettyJson(String json) throws Exception {
        return prettyJson(json.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /** UTF-8 round-trip — only safe to call on bytes you already validated. */
    static String utf8(byte[] payload) {
        return new String(payload, java.nio.charset.StandardCharsets.UTF_8);
    }

    static boolean isLikelyUtf8(byte[] bytes) {
        if (bytes.length == 0) return false;
        // Strict UTF-8 decode — any malformed sequence rules this strategy out.
        java.nio.charset.CharsetDecoder dec = java.nio.charset.StandardCharsets.UTF_8.newDecoder()
                .onMalformedInput(java.nio.charset.CodingErrorAction.REPORT)
                .onUnmappableCharacter(java.nio.charset.CodingErrorAction.REPORT);
        String s;
        try {
            s = dec.decode(java.nio.ByteBuffer.wrap(bytes)).toString();
        } catch (java.nio.charset.CharacterCodingException e) {
            return false;
        }
        int total = s.length();
        if (total == 0) return false;
        int ok = 0;
        for (int i = 0; i < total; i++) {
            char c = s.charAt(i);
            if (c >= 0x20 && c < 0x7F) ok++;
            else if (c == '\n' || c == '\r' || c == '\t') ok++;
            else if (c >= 0xA0 && c <= 0xFFFC) ok++; // common non-ASCII printable
        }
        return ok * 100 / total >= 95;
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
            for (int i = end; i < row + 16; i++) sb.append("   ");
            sb.append(" |");
            for (int i = row; i < end; i++) {
                char c = (char) (bytes[i] & 0xff);
                sb.append((c >= 0x20 && c < 0x7F) ? c : '.');
            }
            sb.append("|\n");
        }
        if (bytes.length > MAX_HEX_BYTES) {
            sb.append(String.format("… (%d more bytes)\n", bytes.length - MAX_HEX_BYTES));
        }
        return sb.toString();
    }
}
