package se.afshin.yavari.kafka.operator.rebalance;

import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;

/**
 * {@link CruiseControlClient} over the JDK {@link HttpClient}. Cruise Control's REST API is
 * asynchronous: a long-running request returns HTTP 202 plus a {@code User-Task-ID} header;
 * re-issuing the identical request with that id as a request header returns HTTP 200 and the
 * cached result once ready.
 */
@ApplicationScoped
public class HttpCruiseControlClient implements CruiseControlClient {

    private static final Logger LOG = Logger.getLogger(HttpCruiseControlClient.class);

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(5);
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);
    private static final String USER_TASK_HEADER = "User-Task-ID";

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(CONNECT_TIMEOUT)
            .build();

    @Override
    public CruiseControlResponse rebalance(CruiseControlEndpoint endpoint, RebalanceParams params,
                                           boolean dryrun, String userTaskId) {
        String url = endpoint.baseUrl() + params.endpointPath() + "?" + params.queryString(dryrun);
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(url))
                .timeout(REQUEST_TIMEOUT)
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.noBody());
        if (userTaskId != null && !userTaskId.isBlank()) {
            rb.header(USER_TASK_HEADER, userTaskId);
        }
        applyAuth(rb, endpoint);

        HttpResponse<String> resp = send(rb.build());
        String taskId = resp.headers().firstValue(USER_TASK_HEADER).orElse(userTaskId);
        int code = resp.statusCode();
        if (code == 200) {
            return new CruiseControlResponse(taskId, Status.COMPLETED,
                    CruiseControlResponseParser.parseSummary(resp.body()));
        }
        if (code == 202) {
            return new CruiseControlResponse(taskId, Status.IN_PROGRESS, Map.of());
        }
        LOG.warnf("Cruise Control %s returned HTTP %d", params.endpointPath(), code);
        return new CruiseControlResponse(taskId, Status.ERROR,
                Map.of("message", CruiseControlResponseParser.parseErrorMessage(resp.body(), code)));
    }

    @Override
    public CruiseControlState state(CruiseControlEndpoint endpoint) {
        String url = endpoint.baseUrl() + "/kafkacruisecontrol/state?json=true&substates=executor";
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(url))
                .timeout(REQUEST_TIMEOUT)
                .header("Accept", "application/json")
                .GET();
        applyAuth(rb, endpoint);

        HttpResponse<String> resp = send(rb.build());
        if (resp.statusCode() != 200) {
            throw new CruiseControlException("Cruise Control /state returned HTTP " + resp.statusCode());
        }
        return new CruiseControlState(CruiseControlResponseParser.parseExecutorIdle(resp.body()));
    }

    @Override
    public void stopExecution(CruiseControlEndpoint endpoint) {
        String url = endpoint.baseUrl() + "/kafkacruisecontrol/stop_proposal_execution?json=true";
        HttpRequest.Builder rb = HttpRequest.newBuilder(URI.create(url))
                .timeout(REQUEST_TIMEOUT)
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.noBody());
        applyAuth(rb, endpoint);

        HttpResponse<String> resp = send(rb.build());
        if (resp.statusCode() != 200 && resp.statusCode() != 202) {
            throw new CruiseControlException(
                    "Cruise Control stop_proposal_execution returned HTTP " + resp.statusCode());
        }
    }

    private HttpResponse<String> send(HttpRequest request) {
        try {
            return http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new CruiseControlException(
                    "Cannot reach Cruise Control at " + request.uri() + ": " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CruiseControlException("Interrupted while calling Cruise Control", e);
        }
    }

    private static void applyAuth(HttpRequest.Builder rb, CruiseControlEndpoint endpoint) {
        if (endpoint.hasAuth()) {
            String token = Base64.getEncoder().encodeToString(
                    (endpoint.username() + ":" + endpoint.password()).getBytes(StandardCharsets.UTF_8));
            rb.header("Authorization", "Basic " + token);
        }
    }
}
