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
import se.afshin.yavari.kafka.ui.service.GroupService;
import se.afshin.yavari.kafka.ui.service.GroupService.GroupSummary;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Path("/clusters/{id}/groups")
@Authenticated
public class GroupResource {

    @Inject ClusterRegistry registry;
    @Inject GroupService groupService;
    @Inject UserContext user;
    @Inject AuditLog audit;
    @Inject Toasts toasts;

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance list(ClusterCoordinates cluster,
                                                   List<GroupSummary> groups,
                                                   String username);
        public static native TemplateInstance list$body(ClusterCoordinates cluster,
                                                        List<GroupSummary> groups,
                                                        String username);
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance list(@PathParam("id") String id,
                                 @HeaderParam("HX-Request") String hx)
            throws ExecutionException, InterruptedException {
        ClusterCoordinates c = cluster(id);
        List<GroupSummary> groups = groupService.list(id);
        return "true".equals(hx)
                ? Templates.list$body(c, groups, user.username())
                : Templates.list(c, groups, user.username());
    }

    @POST
    @Path("/{groupId}/delete")
    @Produces(MediaType.TEXT_HTML)
    public Response delete(@PathParam("id") String id, @PathParam("groupId") String groupId,
                           @FormParam("confirm") String confirm) {
        ClusterCoordinates c = cluster(id);
        if (!groupId.equals(confirm)) {
            return toasts.error("Delete cancelled: confirmation did not match the group ID.");
        }
        try {
            groupService.deleteGroup(id, groupId);
            audit.success(user.username(), "group.delete", id + "/" + groupId, Map.of());
            return refreshed(c, "Group '" + groupId + "' deleted.");
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "group.delete", id + "/" + groupId, reason);
            return toasts.error("Delete failed: " + reason);
        }
    }

    @POST
    @Path("/{groupId}/reset-offsets")
    @Produces(MediaType.TEXT_HTML)
    public Response resetOffsets(@PathParam("id") String id, @PathParam("groupId") String groupId,
                                 @FormParam("topic") String topic,
                                 @FormParam("partition") Integer partition,
                                 @FormParam("target") String target,
                                 @FormParam("offset") Long explicitOffset) {
        ClusterCoordinates c = cluster(id);
        if (topic == null || topic.isBlank() || partition == null) {
            return toasts.error("Topic and partition are required.");
        }
        GroupService.ResetTarget rt;
        try { rt = GroupService.ResetTarget.valueOf(target == null ? "" : target.toUpperCase()); }
        catch (IllegalArgumentException e) {
            return toasts.error("Target must be EARLIEST, LATEST, or OFFSET.");
        }
        try {
            long applied = groupService.resetOffsets(id, groupId, topic, partition, rt,
                    explicitOffset == null ? 0L : explicitOffset);
            audit.success(user.username(), "group.resetOffsets", id + "/" + groupId,
                    Map.of("topic", topic, "partition", partition, "target", rt.name(), "offset", applied));
            return refreshed(c, "Reset " + groupId + " on " + topic + "/" + partition
                    + " to offset " + applied + ".");
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "group.resetOffsets", id + "/" + groupId, reason);
            return toasts.error("Reset failed: " + reason);
        }
    }

    /* helpers */

    private ClusterCoordinates cluster(String id) {
        return registry.byId(id).orElseThrow(() -> new NotFoundException("Unknown cluster: " + id));
    }

    private Response refreshed(ClusterCoordinates c, String msg)
            throws ExecutionException, InterruptedException {
        List<GroupSummary> groups = groupService.list(c.id());
        String body = Templates.list$body(c, groups, user.username()).render();
        return toasts.success(body, msg);
    }

    static String describe(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause != cause.getCause()) cause = cause.getCause();
        String type = cause.getClass().getSimpleName();
        String msg = cause.getMessage();
        return msg == null || msg.isBlank() ? type : type + ": " + msg;
    }
}
