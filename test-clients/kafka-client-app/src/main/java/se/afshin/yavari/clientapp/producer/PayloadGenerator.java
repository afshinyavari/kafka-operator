package se.afshin.yavari.clientapp.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import se.afshin.yavari.clientapp.avro.Event;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** Produces {@link Event}s with an increasing sequence, and renders them as JSON. */
public final class PayloadGenerator {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String hostname;
    private final Clock clock;
    private final AtomicLong sequence = new AtomicLong();

    public PayloadGenerator(String hostname, Clock clock) {
        this.hostname = hostname;
        this.clock = clock;
    }

    public static PayloadGenerator forThisHost() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            host = "unknown";
        }
        return new PayloadGenerator(host, Clock.systemUTC());
    }

    public Event next() {
        long seq = sequence.getAndIncrement();
        return Event.newBuilder()
                .setId(UUID.randomUUID().toString())
                .setSequence(seq)
                .setTimestamp(clock.millis())
                .setMessage("hello from " + hostname + " #" + seq)
                .build();
    }

    /** Only the four business fields; the Avro class's schema/specificData getters are not serialized. */
    public static String toJson(Event e) {
        ObjectNode n = MAPPER.createObjectNode();
        n.put("id", e.getId());
        n.put("sequence", e.getSequence());
        n.put("timestamp", e.getTimestamp());
        n.put("message", e.getMessage());
        return n.toString();
    }
}
