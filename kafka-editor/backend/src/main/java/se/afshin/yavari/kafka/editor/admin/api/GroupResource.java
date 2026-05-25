package se.afshin.yavari.kafka.editor.admin.api;

import java.util.List;

import io.smallrye.common.annotation.Blocking;

import jakarta.inject.Inject;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import se.afshin.yavari.kafka.editor.admin.AdminApiException;
import se.afshin.yavari.kafka.editor.admin.ApiResponse;
import se.afshin.yavari.kafka.editor.admin.ConnQuery;
import se.afshin.yavari.kafka.editor.admin.dto.GroupOffsets;
import se.afshin.yavari.kafka.editor.admin.dto.GroupSummary;
import se.afshin.yavari.kafka.editor.admin.dto.ResetOffsetsRequest;
import se.afshin.yavari.kafka.editor.admin.service.GroupService;
import se.afshin.yavari.kafka.editor.api.ConnectionConfig;

/** Consumer-group endpoints: list, lag, reset-offsets, delete. */
@Path("/api/admin/groups")
public class GroupResource {

    @Inject
    GroupService groupService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<List<GroupSummary>> list(@BeanParam ConnQuery conn) {
        return ApiResponse.ok(groupService.list(conn.toConfig()));
    }

    @GET
    @Path("/{id}/offsets")
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<GroupOffsets> offsets(
            @PathParam("id") String id,
            @BeanParam ConnQuery conn) {
        return ApiResponse.ok(groupService.offsets(conn.toConfig(), id));
    }

    @POST
    @Path("/{id}/reset-offsets")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<Void> resetOffsets(
            @PathParam("id") String id,
            ResetOffsetsRequest request) {
        if (request == null) {
            throw new AdminApiException(400, "BAD_REQUEST", "Missing request body.");
        }
        groupService.resetOffsets(connectionOf(request.connection()), id,
                request.topic(), request.partition(), request.target(),
                request.offset(), request.timestamp());
        return ApiResponse.ok(null);
    }

    @DELETE
    @Path("/{id}")
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<Void> delete(
            @PathParam("id") String id,
            @BeanParam ConnQuery conn) {
        groupService.delete(conn.toConfig(), id);
        return ApiResponse.ok(null);
    }

    private static ConnectionConfig connectionOf(ConnectionConfig connection) {
        return connection != null
                ? connection
                : new ConnectionConfig(null, null, null);
    }
}
