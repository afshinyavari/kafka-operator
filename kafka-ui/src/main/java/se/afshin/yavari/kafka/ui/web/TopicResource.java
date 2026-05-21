package se.afshin.yavari.kafka.ui.web;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import io.quarkus.security.Authenticated;
import jakarta.inject.Inject;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import se.afshin.yavari.kafka.ui.audit.AuditLog;
import se.afshin.yavari.kafka.ui.cluster.ClusterCoordinates;
import se.afshin.yavari.kafka.ui.cluster.ClusterRegistry;
import se.afshin.yavari.kafka.ui.service.TopicService;
import se.afshin.yavari.kafka.ui.service.TopicService.TopicDetail;
import se.afshin.yavari.kafka.ui.service.TopicService.TopicSummary;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

@Path("/clusters/{id}/topics")
@Authenticated
public class TopicResource {

    private static final Pattern TOPIC_NAME = Pattern.compile("[a-zA-Z0-9._-]{1,249}");

    @Inject ClusterRegistry registry;
    @Inject TopicService topicService;
    @Inject UserContext user;
    @Inject AuditLog audit;

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance list(ClusterCoordinates cluster,
                                                   List<TopicSummary> topics,
                                                   String username,
                                                   String errorMessage,
                                                   String successMessage);

        public static native TemplateInstance detail(ClusterCoordinates cluster,
                                                     TopicDetail topic,
                                                     String username,
                                                     String errorMessage,
                                                     String successMessage);
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance list(@PathParam("id") String id,
                                 @QueryParam("error") String error,
                                 @QueryParam("success") String success)
            throws ExecutionException, InterruptedException {
        ClusterCoordinates c = cluster(id);
        return Templates.list(c, topicService.list(id, user.rbac(c.namespace())), user.username(), error, success);
    }

    @GET
    @Path("/{name}")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance detail(@PathParam("id") String id, @PathParam("name") String name,
                                   @QueryParam("error") String error,
                                   @QueryParam("success") String success)
            throws ExecutionException, InterruptedException {
        ClusterCoordinates c = cluster(id);
        return Templates.detail(c, topicService.describe(id, name, user.rbac(c.namespace())),
                user.username(), error, success);
    }

    @POST
    @Path("")
    public Response create(@PathParam("id") String id,
                           @FormParam("name") String name,
                           @FormParam("partitions") Integer partitions,
                           @FormParam("replication") Integer replication,
                           @FormParam("configs") String configsRaw) {
        cluster(id);
        if (name == null || !TOPIC_NAME.matcher(name).matches()) {
            return redirect("/clusters/" + id + "/topics",
                    "Invalid topic name; allowed: letters, digits, dot, underscore, dash, 1-249 chars.");
        }
        if (partitions == null || partitions < 1) {
            return redirect("/clusters/" + id + "/topics", "Partitions must be >= 1.");
        }
        if (replication == null || replication < 1) {
            return redirect("/clusters/" + id + "/topics", "Replication factor must be >= 1.");
        }

        Map<String, String> configs;
        try {
            configs = parseConfigs(configsRaw);
        } catch (IllegalArgumentException e) {
            return redirect("/clusters/" + id + "/topics", e.getMessage());
        }

        try {
            topicService.create(id, name, partitions, replication.shortValue(), configs);
            audit.success(user.username(), "topic.create", id + "/" + name,
                    Map.of("partitions", partitions, "rf", replication));
            return redirectSuccess("/clusters/" + id + "/topics", "Topic '" + name + "' created.");
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "topic.create", id + "/" + name, reason);
            return redirect("/clusters/" + id + "/topics", "Create failed: " + reason);
        }
    }

    @POST
    @Path("/{name}/configs")
    public Response alterConfigs(@PathParam("id") String id, @PathParam("name") String name,
                                 @FormParam("configs") String configsRaw) {
        cluster(id);
        Map<String, String> changes;
        try {
            changes = parseConfigs(configsRaw);
        } catch (IllegalArgumentException e) {
            return redirect("/clusters/" + id + "/topics/" + name, e.getMessage());
        }
        if (changes.isEmpty()) {
            return redirect("/clusters/" + id + "/topics/" + name, "No config changes provided.");
        }

        try {
            topicService.alterConfigs(id, name, changes);
            audit.success(user.username(), "topic.alterConfigs", id + "/" + name,
                    Map.of("keys", changes.keySet()));
            return redirectSuccess("/clusters/" + id + "/topics/" + name,
                    "Updated " + changes.size() + " config key(s).");
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "topic.alterConfigs", id + "/" + name, reason);
            return redirect("/clusters/" + id + "/topics/" + name, "Alter configs failed: " + reason);
        }
    }

    @POST
    @Path("/{name}/delete")
    public Response delete(@PathParam("id") String id, @PathParam("name") String name,
                           @FormParam("confirm") String confirm) {
        cluster(id);
        if (!name.equals(confirm)) {
            return redirect("/clusters/" + id + "/topics/" + name,
                    "Delete cancelled: confirmation did not match the topic name.");
        }
        try {
            topicService.delete(id, name);
            audit.success(user.username(), "topic.delete", id + "/" + name, Map.of());
            return redirectSuccess("/clusters/" + id + "/topics",
                    "Topic '" + name + "' deleted.");
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "topic.delete", id + "/" + name, reason);
            return redirect("/clusters/" + id + "/topics/" + name, "Delete failed: " + reason);
        }
    }

    /* ---------- helpers ---------- */

    private ClusterCoordinates cluster(String id) {
        return registry.byId(id).orElseThrow(() -> new NotFoundException("Unknown cluster: " + id));
    }

    /** Parses comma-separated {@code key=value} pairs into a map. Empty input → empty map. */
    static Map<String, String> parseConfigs(String raw) {
        Map<String, String> out = new LinkedHashMap<>();
        if (raw == null || raw.isBlank()) return out;
        for (String chunk : raw.split(",")) {
            String s = chunk.trim();
            if (s.isEmpty()) continue;
            int eq = s.indexOf('=');
            if (eq <= 0 || eq == s.length() - 1) {
                throw new IllegalArgumentException(
                        "Invalid config entry '" + s + "' — expected key=value (use comma between entries).");
            }
            String k = s.substring(0, eq).trim();
            String v = s.substring(eq + 1).trim();
            out.put(k, v);
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

    private static Response redirect(String base, String error) {
        return Response.seeOther(URI.create(base + "?error=" + enc(error))).build();
    }

    private static Response redirectSuccess(String base, String success) {
        return Response.seeOther(URI.create(base + "?success=" + enc(success))).build();
    }

    private static String enc(String s) {
        return s == null ? "" : URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
