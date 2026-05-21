package se.afshin.yavari.kafka.ui.messages;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.quarkus.security.Authenticated;
import jakarta.inject.Inject;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import se.afshin.yavari.kafka.ui.audit.AuditLog;
import se.afshin.yavari.kafka.ui.cluster.ClusterCoordinates;
import se.afshin.yavari.kafka.ui.cluster.ClusterRegistry;
import se.afshin.yavari.kafka.ui.web.Toasts;
import se.afshin.yavari.kafka.ui.web.UserContext;

import java.util.LinkedHashMap;
import java.util.Map;

@Path("/clusters/{id}/topics/{name}/produce")
@Authenticated
public class ProduceResource {

    private static final ObjectMapper JSON = new ObjectMapper();

    @Inject ClusterRegistry registry;
    @Inject ProduceService service;
    @Inject UserContext user;
    @Inject AuditLog audit;
    @Inject Toasts toasts;

    @POST
    @Produces(MediaType.TEXT_HTML)
    public Response produce(@PathParam("id") String id,
                            @PathParam("name") String topic,
                            @FormParam("key") String key,
                            @FormParam("value") String value,
                            @FormParam("valueType") String valueType,
                            @FormParam("partition") String partitionRaw,
                            @FormParam("headers") String headersRaw) {
        cluster(id);

        if (value == null) value = "";
        if ("json".equalsIgnoreCase(valueType) && !value.isBlank()) {
            try { JSON.readTree(value); }
            catch (Exception e) { return toasts.error("Value is not valid JSON: " + e.getMessage()); }
        }

        Integer partition = null;
        if (partitionRaw != null && !partitionRaw.isBlank()) {
            try { partition = Integer.parseInt(partitionRaw.trim()); }
            catch (NumberFormatException e) {
                return toasts.error("Partition must be a number or blank.");
            }
            if (partition < 0) return toasts.error("Partition must be >= 0.");
        }

        Map<String, String> headers;
        try { headers = parseHeaders(headersRaw); }
        catch (IllegalArgumentException e) { return toasts.error(e.getMessage()); }

        try {
            ProduceService.SendResult r = service.send(id, topic, partition, key, value, headers);
            audit.success(user.username(), "message.produce", id + "/" + topic,
                    Map.of("partition", r.partition(), "offset", r.offset()));
            return toasts.toastOnly("success",
                    "Sent to partition " + r.partition() + " at offset " + r.offset() + ".");
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "message.produce", id + "/" + topic, reason);
            return toasts.error("Produce failed: " + reason);
        }
    }

    private ClusterCoordinates cluster(String id) {
        return registry.byId(id).orElseThrow(() -> new NotFoundException("Unknown cluster: " + id));
    }

    static Map<String, String> parseHeaders(String raw) {
        Map<String, String> out = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) return out;
        for (String line : raw.split("\\r?\\n")) {
            String s = line.trim();
            if (s.isEmpty()) continue;
            int eq = s.indexOf('=');
            if (eq <= 0) {
                throw new IllegalArgumentException(
                        "Invalid header '" + s + "' — expected key=value, one per line.");
            }
            out.put(s.substring(0, eq).trim(), s.substring(eq + 1));
        }
        return out;
    }

    static String describe(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause != cause.getCause()) cause = cause.getCause();
        String type = cause.getClass().getSimpleName();
        String msg = cause.getMessage();
        return msg == null || msg.isBlank() ? type : type + ": " + msg;
    }
}
