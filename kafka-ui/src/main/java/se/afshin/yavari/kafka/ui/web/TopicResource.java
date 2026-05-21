package se.afshin.yavari.kafka.ui.web;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import io.quarkus.security.Authenticated;
import jakarta.inject.Inject;
import jakarta.ws.rs.FormParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.HeaderParam;
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
import se.afshin.yavari.kafka.ui.service.TopicService;
import se.afshin.yavari.kafka.ui.service.TopicService.TopicDetail;
import se.afshin.yavari.kafka.ui.service.TopicService.TopicSummary;

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
    @Inject Toasts toasts;

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance list(ClusterCoordinates cluster,
                                                   List<TopicSummary> topics,
                                                   String username);
        public static native TemplateInstance list$body(ClusterCoordinates cluster,
                                                        List<TopicSummary> topics,
                                                        String username);

        public static native TemplateInstance detail(ClusterCoordinates cluster,
                                                     TopicDetail topic,
                                                     String username);
        public static native TemplateInstance detail$body(ClusterCoordinates cluster,
                                                          TopicDetail topic,
                                                          String username);
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance list(@PathParam("id") String id,
                                 @HeaderParam("HX-Request") String hx)
            throws ExecutionException, InterruptedException {
        ClusterCoordinates c = cluster(id);
        List<TopicSummary> topics = topicService.list(id, user.rbac(c.namespace()));
        return "true".equals(hx)
                ? Templates.list$body(c, topics, user.username())
                : Templates.list(c, topics, user.username());
    }

    @GET
    @Path("/{name}")
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance detail(@PathParam("id") String id, @PathParam("name") String name,
                                   @HeaderParam("HX-Request") String hx)
            throws ExecutionException, InterruptedException {
        ClusterCoordinates c = cluster(id);
        TopicDetail t = topicService.describe(id, name, user.rbac(c.namespace()));
        return "true".equals(hx)
                ? Templates.detail$body(c, t, user.username())
                : Templates.detail(c, t, user.username());
    }

    @POST
    @Path("")
    @Produces(MediaType.TEXT_HTML)
    public Response create(@PathParam("id") String id,
                           @FormParam("name") String name,
                           @FormParam("partitions") Integer partitions,
                           @FormParam("replication") Integer replication,
                           @FormParam("configs") String configsRaw) {
        ClusterCoordinates c = cluster(id);
        if (name == null || !TOPIC_NAME.matcher(name).matches()) {
            return toasts.error("Invalid topic name; allowed: letters, digits, dot, underscore, dash, 1-249 chars.");
        }
        if (partitions == null || partitions < 1) return toasts.error("Partitions must be >= 1.");
        if (replication == null || replication < 1) return toasts.error("Replication factor must be >= 1.");

        Map<String, String> configs;
        try { configs = parseConfigs(configsRaw); }
        catch (IllegalArgumentException e) { return toasts.error(e.getMessage()); }

        try {
            topicService.create(id, name, partitions, replication.shortValue(), configs);
            audit.success(user.username(), "topic.create", id + "/" + name,
                    Map.of("partitions", partitions, "rf", replication));
            return refreshedList(c, "Topic '" + name + "' created.");
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "topic.create", id + "/" + name, reason);
            return toasts.error("Create failed: " + reason);
        }
    }

    @POST
    @Path("/{name}/configs")
    @Produces(MediaType.TEXT_HTML)
    public Response alterConfigs(@PathParam("id") String id, @PathParam("name") String name,
                                 @FormParam("configs") String configsRaw) {
        ClusterCoordinates c = cluster(id);
        Map<String, String> changes;
        try { changes = parseConfigs(configsRaw); }
        catch (IllegalArgumentException e) { return toasts.error(e.getMessage()); }
        if (changes.isEmpty()) return toasts.error("No config changes provided.");

        try {
            topicService.alterConfigs(id, name, changes);
            audit.success(user.username(), "topic.alterConfigs", id + "/" + name,
                    Map.of("keys", changes.keySet()));
            TopicDetail t = topicService.describe(id, name, user.rbac(c.namespace()));
            String body = Templates.detail$body(c, t, user.username()).render();
            return toasts.success(body, "Updated " + changes.size() + " config key(s).");
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "topic.alterConfigs", id + "/" + name, reason);
            return toasts.error("Alter configs failed: " + reason);
        }
    }

    @POST
    @Path("/{name}/delete")
    @Produces(MediaType.TEXT_HTML)
    public Response delete(@PathParam("id") String id, @PathParam("name") String name,
                           @FormParam("confirm") String confirm) {
        ClusterCoordinates c = cluster(id);
        if (!name.equals(confirm)) {
            return toasts.error("Delete cancelled: confirmation did not match the topic name.");
        }
        try {
            topicService.delete(id, name);
            audit.success(user.username(), "topic.delete", id + "/" + name, Map.of());
            return refreshedListPushUrl(c, "Topic '" + name + "' deleted.");
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "topic.delete", id + "/" + name, reason);
            return toasts.error("Delete failed: " + reason);
        }
    }

    /* ---------- helpers ---------- */

    private ClusterCoordinates cluster(String id) {
        return registry.byId(id).orElseThrow(() -> new NotFoundException("Unknown cluster: " + id));
    }

    private Response refreshedList(ClusterCoordinates c, String successMessage)
            throws ExecutionException, InterruptedException {
        List<TopicSummary> topics = topicService.list(c.id(), user.rbac(c.namespace()));
        String body = Templates.list$body(c, topics, user.username()).render();
        return toasts.success(body, successMessage);
    }

    private Response refreshedListPushUrl(ClusterCoordinates c, String successMessage)
            throws ExecutionException, InterruptedException {
        List<TopicSummary> topics = topicService.list(c.id(), user.rbac(c.namespace()));
        String body = Templates.list$body(c, topics, user.username()).render();
        return toasts.successPushUrl(body, successMessage, "/clusters/" + c.id() + "/topics");
    }

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
            out.put(s.substring(0, eq).trim(), s.substring(eq + 1).trim());
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
