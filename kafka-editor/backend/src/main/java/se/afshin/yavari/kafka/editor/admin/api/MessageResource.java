package se.afshin.yavari.kafka.editor.admin.api;

import io.quarkus.security.Authenticated;

import java.util.ArrayList;
import java.util.List;

import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Multi;

import jakarta.inject.Inject;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;

import se.afshin.yavari.kafka.editor.admin.AdminApiException;
import se.afshin.yavari.kafka.editor.admin.ApiResponse;
import se.afshin.yavari.kafka.editor.admin.ConnQuery;
import se.afshin.yavari.kafka.editor.admin.dto.MessagePage;
import se.afshin.yavari.kafka.editor.admin.dto.ProduceAvroRequest;
import se.afshin.yavari.kafka.editor.admin.dto.ProduceRequest;
import se.afshin.yavari.kafka.editor.admin.dto.RenderedRecord;
import se.afshin.yavari.kafka.editor.admin.dto.ReplayRequest;
import se.afshin.yavari.kafka.editor.admin.dto.SearchResult;
import se.afshin.yavari.kafka.editor.admin.dto.SeekMode;
import se.afshin.yavari.kafka.editor.admin.dto.SendResult;
import se.afshin.yavari.kafka.editor.admin.service.MessageBrowseService;
import se.afshin.yavari.kafka.editor.admin.service.ProduceService;
import se.afshin.yavari.kafka.editor.api.ConnectionConfig;
import org.jboss.resteasy.reactive.RestStreamElementType;

/** Message browsing, live tail, search, produce and replay endpoints. */
@Authenticated
@Path("/api/admin/messages")
public class MessageResource {

    @Inject
    MessageBrowseService browseService;

    @Inject
    ProduceService produceService;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<MessagePage> browse(
            @BeanParam ConnQuery conn,
            @QueryParam("topic") String topic,
            @QueryParam("partition") @DefaultValue("0") int partition,
            @QueryParam("seek") @DefaultValue("LATEST") SeekMode seek,
            @QueryParam("offset") Long offset,
            @QueryParam("timestamp") Long timestamp,
            @QueryParam("size") @DefaultValue("50") int size) {
        requireTopic(topic);
        return ApiResponse.ok(browseService.page(conn.toConfig(), topic,
                partition, seek, offset, timestamp, size));
    }

    @GET
    @Path("/partitions")
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<List<Integer>> partitions(
            @BeanParam ConnQuery conn,
            @QueryParam("topic") String topic) {
        requireTopic(topic);
        return ApiResponse.ok(browseService.partitionsOf(conn.toConfig(), topic));
    }

    @GET
    @Path("/tail")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    @RestStreamElementType(MediaType.APPLICATION_JSON)
    public Multi<RenderedRecord> tail(
            @BeanParam ConnQuery conn,
            @QueryParam("topic") String topic,
            @QueryParam("partitions") String partitions) {
        requireTopic(topic);
        return browseService.tail(conn.toConfig(), topic,
                parsePartitions(partitions));
    }

    @GET
    @Path("/search")
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<SearchResult> search(
            @BeanParam ConnQuery conn,
            @QueryParam("topic") String topic,
            @QueryParam("partition") @DefaultValue("0") int partition,
            @QueryParam("query") String query,
            @QueryParam("field") @DefaultValue("all") String field,
            @QueryParam("caseSensitive") @DefaultValue("false") boolean caseSensitive,
            @QueryParam("scanLimit") @DefaultValue("1000") int scanLimit) {
        requireTopic(topic);
        return ApiResponse.ok(browseService.search(conn.toConfig(), topic,
                partition, query, field, caseSensitive, scanLimit));
    }

    @POST
    @Path("/produce")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<SendResult> produce(ProduceRequest request) {
        if (request == null) {
            throw new AdminApiException(400, "BAD_REQUEST", "Missing request body.");
        }
        return ApiResponse.ok(produceService.send(
                connectionOf(request.connection()), request));
    }

    @POST
    @Path("/replay")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<SendResult> replay(ReplayRequest request) {
        if (request == null) {
            throw new AdminApiException(400, "BAD_REQUEST", "Missing request body.");
        }
        return ApiResponse.ok(produceService.replay(
                connectionOf(request.connection()), request));
    }

    @POST
    @Path("/produce-avro")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    @Blocking
    public ApiResponse<SendResult> produceAvro(ProduceAvroRequest request) {
        if (request == null) {
            throw new AdminApiException(400, "BAD_REQUEST", "Missing request body.");
        }
        return ApiResponse.ok(produceService.sendAvro(
                connectionOf(request.connection()), request));
    }

    private static void requireTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new AdminApiException(400, "BAD_REQUEST", "A topic is required.");
        }
    }

    private static List<Integer> parsePartitions(String csv) {
        List<Integer> out = new ArrayList<>();
        if (csv == null || csv.isBlank()) {
            return out;
        }
        for (String part : csv.split(",")) {
            String trimmed = part.trim();
            if (!trimmed.isEmpty()) {
                try {
                    out.add(Integer.parseInt(trimmed));
                } catch (NumberFormatException ignored) {
                    // skip a malformed partition id
                }
            }
        }
        return out;
    }

    private static ConnectionConfig connectionOf(ConnectionConfig connection) {
        return connection != null
                ? connection
                : new ConnectionConfig(null, null, null);
    }
}
