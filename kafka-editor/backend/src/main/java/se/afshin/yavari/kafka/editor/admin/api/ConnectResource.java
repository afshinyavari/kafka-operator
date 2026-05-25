package se.afshin.yavari.kafka.editor.admin.api;

import com.fasterxml.jackson.databind.JsonNode;

import io.smallrye.common.annotation.Blocking;

import jakarta.inject.Inject;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import se.afshin.yavari.kafka.editor.admin.AdminApiException;
import se.afshin.yavari.kafka.editor.admin.ApiResponse;
import se.afshin.yavari.kafka.editor.admin.ConnQuery;
import se.afshin.yavari.kafka.editor.admin.dto.ConnectRequest;
import se.afshin.yavari.kafka.editor.admin.service.ConnectProxyService;

/**
 * Kafka Connect endpoints. With no Connect URL configured, every call returns
 * {@code capability: not_configured} (HTTP 200) instead of failing.
 */
@Path("/api/admin/connect")
public class ConnectResource {

    private static final String NOT_CONFIGURED =
            "No Kafka Connect URL is set for this cluster.";

    @Inject
    ConnectProxyService connect;

    @GET
    @Path("/connectors")
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<JsonNode> connectors(@BeanParam ConnQuery conn) {
        if (blank(conn.connect)) {
            return ApiResponse.notConfigured(NOT_CONFIGURED);
        }
        return ApiResponse.ok(connect.connectors(conn.connect));
    }

    @GET
    @Path("/connectors/{name}/status")
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<JsonNode> status(
            @PathParam("name") String name,
            @BeanParam ConnQuery conn) {
        if (blank(conn.connect)) {
            return ApiResponse.notConfigured(NOT_CONFIGURED);
        }
        return ApiResponse.ok(connect.status(conn.connect, name));
    }

    @GET
    @Path("/connectors/{name}/config")
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<JsonNode> config(
            @PathParam("name") String name,
            @BeanParam ConnQuery conn) {
        if (blank(conn.connect)) {
            return ApiResponse.notConfigured(NOT_CONFIGURED);
        }
        return ApiResponse.ok(connect.config(conn.connect, name));
    }

    @POST
    @Path("/connectors")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<JsonNode> create(ConnectRequest request) {
        String url = connectUrl(request);
        if (url == null) {
            return ApiResponse.notConfigured(NOT_CONFIGURED);
        }
        if (request.config() == null) {
            throw new AdminApiException(400, "BAD_REQUEST",
                    "A connector definition is required.");
        }
        return ApiResponse.ok(connect.create(url, request.config().toString()));
    }

    @PUT
    @Path("/connectors/{name}/config")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<JsonNode> updateConfig(
            @PathParam("name") String name,
            ConnectRequest request) {
        String url = connectUrl(request);
        if (url == null) {
            return ApiResponse.notConfigured(NOT_CONFIGURED);
        }
        if (request.config() == null) {
            throw new AdminApiException(400, "BAD_REQUEST",
                    "A connector config is required.");
        }
        return ApiResponse.ok(
                connect.updateConfig(url, name, request.config().toString()));
    }

    @POST
    @Path("/connectors/{name}/restart")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<JsonNode> restart(
            @PathParam("name") String name,
            ConnectRequest request) {
        String url = connectUrl(request);
        return url == null
                ? ApiResponse.notConfigured(NOT_CONFIGURED)
                : ApiResponse.ok(connect.restart(url, name));
    }

    @PUT
    @Path("/connectors/{name}/pause")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<JsonNode> pause(
            @PathParam("name") String name,
            ConnectRequest request) {
        String url = connectUrl(request);
        return url == null
                ? ApiResponse.notConfigured(NOT_CONFIGURED)
                : ApiResponse.ok(connect.pause(url, name));
    }

    @PUT
    @Path("/connectors/{name}/resume")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<JsonNode> resume(
            @PathParam("name") String name,
            ConnectRequest request) {
        String url = connectUrl(request);
        return url == null
                ? ApiResponse.notConfigured(NOT_CONFIGURED)
                : ApiResponse.ok(connect.resume(url, name));
    }

    @DELETE
    @Path("/connectors/{name}")
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<JsonNode> delete(
            @PathParam("name") String name,
            @BeanParam ConnQuery conn) {
        if (blank(conn.connect)) {
            return ApiResponse.notConfigured(NOT_CONFIGURED);
        }
        return ApiResponse.ok(connect.delete(conn.connect, name));
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }

    private static String connectUrl(ConnectRequest request) {
        if (request == null || request.connection() == null) {
            return null;
        }
        return request.connection().connectUrlOrNull();
    }
}
