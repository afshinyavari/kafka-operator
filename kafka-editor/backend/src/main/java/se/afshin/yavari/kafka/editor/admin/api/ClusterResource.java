package se.afshin.yavari.kafka.editor.admin.api;

import io.quarkus.security.Authenticated;

import io.smallrye.common.annotation.Blocking;

import jakarta.inject.Inject;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import se.afshin.yavari.kafka.editor.admin.ApiResponse;
import se.afshin.yavari.kafka.editor.admin.ConnQuery;
import se.afshin.yavari.kafka.editor.admin.dto.ClusterOverview;
import se.afshin.yavari.kafka.editor.admin.service.ClusterService;

/** Cluster overview for the Cluster view's dashboard. */
@Authenticated
@Path("/api/admin/cluster")
public class ClusterResource {

    @Inject
    ClusterService clusterService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<ClusterOverview> overview(@BeanParam ConnQuery conn) {
        return ApiResponse.ok(clusterService.overview(conn.toConfig()));
    }
}
