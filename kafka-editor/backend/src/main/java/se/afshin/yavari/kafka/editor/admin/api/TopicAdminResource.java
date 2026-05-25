package se.afshin.yavari.kafka.editor.admin.api;

import io.quarkus.security.Authenticated;

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
import se.afshin.yavari.kafka.editor.admin.dto.AddPartitionsRequest;
import se.afshin.yavari.kafka.editor.admin.dto.AlterConfigRequest;
import se.afshin.yavari.kafka.editor.admin.dto.CreateTopicRequest;
import se.afshin.yavari.kafka.editor.admin.dto.TopicDetail;
import se.afshin.yavari.kafka.editor.admin.dto.TopicMetrics;
import se.afshin.yavari.kafka.editor.admin.dto.TopicSummary;
import se.afshin.yavari.kafka.editor.admin.service.TopicAdminService;
import se.afshin.yavari.kafka.editor.api.ConnectionConfig;

/** Topic browsing, lifecycle and metrics endpoints. */
@Authenticated
@Path("/api/admin/topics")
public class TopicAdminResource {

    @Inject
    TopicAdminService topicService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<List<TopicSummary>> list(@BeanParam ConnQuery conn) {
        return ApiResponse.ok(topicService.list(conn.toConfig()));
    }

    @GET
    @Path("/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<TopicDetail> describe(
            @PathParam("name") String name,
            @BeanParam ConnQuery conn) {
        return ApiResponse.ok(topicService.describe(conn.toConfig(), name));
    }

    @GET
    @Path("/{name}/metrics")
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<TopicMetrics> metrics(
            @PathParam("name") String name,
            @BeanParam ConnQuery conn) {
        return ApiResponse.ok(topicService.metrics(conn.toConfig(), name));
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<Void> create(CreateTopicRequest request) {
        if (request == null) {
            throw new AdminApiException(400, "BAD_REQUEST", "Missing request body.");
        }
        topicService.create(
                connectionOf(request.connection()),
                request.name(),
                request.partitions() == null ? 1 : request.partitions(),
                request.replicationFactor() == null
                        ? 1 : request.replicationFactor().shortValue(),
                request.configs());
        return ApiResponse.ok(null);
    }

    @POST
    @Path("/{name}/config")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<Void> alterConfig(
            @PathParam("name") String name,
            AlterConfigRequest request) {
        if (request == null) {
            throw new AdminApiException(400, "BAD_REQUEST", "Missing request body.");
        }
        topicService.alterConfigs(connectionOf(request.connection()), name,
                request.changes());
        return ApiResponse.ok(null);
    }

    @POST
    @Path("/{name}/partitions")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<Void> addPartitions(
            @PathParam("name") String name,
            AddPartitionsRequest request) {
        if (request == null || request.totalCount() == null) {
            throw new AdminApiException(400, "BAD_REQUEST",
                    "Missing the new partition count.");
        }
        topicService.addPartitions(connectionOf(request.connection()), name,
                request.totalCount());
        return ApiResponse.ok(null);
    }

    @DELETE
    @Path("/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<Void> delete(
            @PathParam("name") String name,
            @BeanParam ConnQuery conn) {
        topicService.delete(conn.toConfig(), name);
        return ApiResponse.ok(null);
    }

    private static ConnectionConfig connectionOf(ConnectionConfig connection) {
        return connection != null
                ? connection
                : new ConnectionConfig(null, null, null);
    }
}
