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
import se.afshin.yavari.kafka.ui.service.GroupService;
import se.afshin.yavari.kafka.ui.service.GroupService.GroupSummary;

import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance list(ClusterCoordinates cluster,
                                                   List<GroupSummary> groups,
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
        return Templates.list(c, groupService.list(id), user.username(), error, success);
    }

    @POST
    @Path("/{groupId}/delete")
    public Response delete(@PathParam("id") String id, @PathParam("groupId") String groupId,
                           @FormParam("confirm") String confirm) {
        cluster(id);
        if (!groupId.equals(confirm)) {
            return errorBack(id, "Delete cancelled: confirmation did not match the group ID.");
        }
        try {
            groupService.deleteGroup(id, groupId);
            audit.success(user.username(), "group.delete", id + "/" + groupId, Map.of());
            return successBack(id, "Group '" + groupId + "' deleted.");
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "group.delete", id + "/" + groupId, reason);
            return errorBack(id, "Delete failed: " + reason);
        }
    }

    @POST
    @Path("/{groupId}/reset-offsets")
    public Response resetOffsets(@PathParam("id") String id, @PathParam("groupId") String groupId,
                                 @FormParam("topic") String topic,
                                 @FormParam("partition") Integer partition,
                                 @FormParam("target") String target,
                                 @FormParam("offset") Long explicitOffset) {
        cluster(id);
        if (topic == null || topic.isBlank() || partition == null) {
            return errorBack(id, "Topic and partition are required.");
        }
        GroupService.ResetTarget rt;
        try {
            rt = GroupService.ResetTarget.valueOf(target == null ? "" : target.toUpperCase());
        } catch (IllegalArgumentException e) {
            return errorBack(id, "Target must be EARLIEST, LATEST, or OFFSET.");
        }
        try {
            long applied = groupService.resetOffsets(id, groupId, topic, partition, rt,
                    explicitOffset == null ? 0L : explicitOffset);
            audit.success(user.username(), "group.resetOffsets", id + "/" + groupId,
                    Map.of("topic", topic, "partition", partition, "target", rt.name(), "offset", applied));
            return successBack(id, "Reset " + groupId + " on " + topic + "/" + partition
                    + " to offset " + applied + ".");
        } catch (Exception e) {
            String reason = describe(e);
            audit.failure(user.username(), "group.resetOffsets", id + "/" + groupId, reason);
            return errorBack(id, "Reset failed: " + reason);
        }
    }

    /* helpers */

    private ClusterCoordinates cluster(String id) {
        return registry.byId(id).orElseThrow(() -> new NotFoundException("Unknown cluster: " + id));
    }

    static String describe(Throwable t) {
        Throwable cause = t;
        while (cause.getCause() != null && cause != cause.getCause()) cause = cause.getCause();
        String type = cause.getClass().getSimpleName();
        String msg = cause.getMessage();
        return msg == null || msg.isBlank() ? type : type + ": " + msg;
    }

    private static Response successBack(String id, String msg) {
        return Response.seeOther(URI.create("/clusters/" + id + "/groups?success=" + enc(msg))).build();
    }

    private static Response errorBack(String id, String msg) {
        return Response.seeOther(URI.create("/clusters/" + id + "/groups?error=" + enc(msg))).build();
    }

    private static String enc(String s) {
        return s == null ? "" : URLEncoder.encode(s, StandardCharsets.UTF_8);
    }
}
