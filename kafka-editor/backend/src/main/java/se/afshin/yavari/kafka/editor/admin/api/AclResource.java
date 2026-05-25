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
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import se.afshin.yavari.kafka.editor.admin.AdminApiException;
import se.afshin.yavari.kafka.editor.admin.ApiResponse;
import se.afshin.yavari.kafka.editor.admin.ConnQuery;
import se.afshin.yavari.kafka.editor.admin.dto.AclEntry;
import se.afshin.yavari.kafka.editor.admin.dto.AclWriteRequest;
import se.afshin.yavari.kafka.editor.admin.service.AclService;
import se.afshin.yavari.kafka.editor.api.ConnectionConfig;

/**
 * Native Kafka ACL endpoints. A broker without an authorizer yields a
 * {@code capability: unsupported} response (HTTP 200) rather than an error.
 */
@Authenticated
@Path("/api/admin/acls")
public class AclResource {

    @Inject
    AclService aclService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<List<AclEntry>> list(@BeanParam ConnQuery conn) {
        try {
            return ApiResponse.ok(aclService.list(conn.toConfig()));
        } catch (AdminApiException e) {
            return capabilityOrThrow(e);
        }
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<Void> create(AclWriteRequest request) {
        if (request == null || request.acl() == null) {
            throw new AdminApiException(400, "BAD_REQUEST", "Missing ACL.");
        }
        try {
            aclService.create(connectionOf(request.connection()), request.acl());
            return ApiResponse.ok(null);
        } catch (AdminApiException e) {
            return capabilityOrThrow(e);
        }
    }

    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<Integer> delete(AclWriteRequest request) {
        if (request == null || request.acl() == null) {
            throw new AdminApiException(400, "BAD_REQUEST", "Missing ACL.");
        }
        try {
            int removed = aclService.delete(connectionOf(request.connection()),
                    request.acl());
            return ApiResponse.ok(removed);
        } catch (AdminApiException e) {
            return capabilityOrThrow(e);
        }
    }

    /** Turn an UNSUPPORTED failure into a capability response; rethrow others. */
    private static <T> ApiResponse<T> capabilityOrThrow(AdminApiException e) {
        if ("UNSUPPORTED".equals(e.kind())) {
            return ApiResponse.unsupported(
                    "ACLs are not enabled on this broker (no authorizer configured).");
        }
        throw e;
    }

    private static ConnectionConfig connectionOf(ConnectionConfig connection) {
        return connection != null
                ? connection
                : new ConnectionConfig(null, null, null);
    }
}
