package se.afshin.yavari.kafka.editor.api;

import io.quarkus.security.Authenticated;

import java.time.Duration;

import io.smallrye.mutiny.Multi;

import jakarta.inject.Inject;
import jakarta.ws.rs.BadRequestException;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;

import se.afshin.yavari.kafka.editor.run.MetricsSnapshot;
import se.afshin.yavari.kafka.editor.run.RunResult;
import se.afshin.yavari.kafka.editor.run.RunService;
import org.jboss.resteasy.reactive.RestStreamElementType;

/** REST API for running topologies and streaming their metrics. */
@Authenticated
@Path("/api")
public class RunResource {

    private static final int DEFAULT_RECORDS = 20;
    private static final int MAX_RECORDS = 10_000;

    @Inject
    RunService runService;

    /** Start a run — test mode returns counts directly, live mode returns a runId. */
    @POST
    @Path("/run")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public RunResult run(RunRequest request) {
        if (request == null || request.document() == null) {
            throw new BadRequestException("Missing topology document");
        }
        if ("live".equals(request.mode())) {
            ConnectionConfig connection = request.connection();
            String bootstrap = connection != null
                    ? connection.bootstrapServersOrDefault()
                    : "localhost:9092";
            String registry =
                    connection != null ? connection.schemaRegistryUrlOrNull() : null;
            boolean generate = Boolean.TRUE.equals(request.generateInput());
            return runService.runLive(request.document(), bootstrap, registry,
                    request.envVars(), generate);
        }
        int records = request.recordsPerSource() != null
                && request.recordsPerSource() > 0
                ? Math.min(request.recordsPerSource(), MAX_RECORDS)
                : DEFAULT_RECORDS;
        return runService.runTest(request.document(), records);
    }

    /** Server-sent stream of a live run's per-node metrics (once per second). */
    @GET
    @Path("/runs/{runId}/metrics")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    @RestStreamElementType(MediaType.APPLICATION_JSON)
    public Multi<MetricsSnapshot> metrics(@PathParam("runId") String runId) {
        return Multi.createFrom()
                .ticks()
                .every(Duration.ofSeconds(1))
                .onItem()
                .transform(tick -> runService.snapshot(runId));
    }

    /** Stop a live run. */
    @DELETE
    @Path("/runs/{runId}")
    public void stop(@PathParam("runId") String runId) {
        runService.stop(runId);
    }
}
