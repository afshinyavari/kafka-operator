package se.afshin.yavari.kafka.operator.rebalance;

import java.util.Map;

/**
 * Seam over the Cruise Control REST API. An interface so the {@link RebalanceStateMachine}
 * unit tests can mock it — no live Cruise Control required. The only production
 * implementation is {@link HttpCruiseControlClient}.
 */
public interface CruiseControlClient {

    /**
     * Issues a rebalance / add_broker / remove_broker request.
     *
     * <p>Cruise Control is async: a fresh request (null {@code userTaskId}) returns a
     * {@code User-Task-ID} and {@link Status#IN_PROGRESS}; re-issuing the identical request
     * with that id polls for the result, ultimately {@link Status#COMPLETED}.
     *
     * @param dryrun     true to generate a proposal only; false to execute
     * @param userTaskId Cruise Control User-Task-ID to resume, or null for a fresh request
     */
    CruiseControlResponse rebalance(CruiseControlEndpoint endpoint, RebalanceParams params,
                                    boolean dryrun, String userTaskId);

    /** GET /state — used to detect when a running execution has finished. */
    CruiseControlState state(CruiseControlEndpoint endpoint);

    /** POST /stop_proposal_execution — aborts an in-flight rebalance. */
    void stopExecution(CruiseControlEndpoint endpoint);

    enum Status { IN_PROGRESS, COMPLETED, ERROR }

    /**
     * @param userTaskId the Cruise Control User-Task-ID (echoed back for a fresh request)
     * @param status     IN_PROGRESS while computing, COMPLETED when the result is ready,
     *                   ERROR on an HTTP error response
     * @param summary    flattened proposal summary when COMPLETED, or {@code {message: ...}}
     *                   describing the error when ERROR
     */
    record CruiseControlResponse(String userTaskId, Status status, Map<String, String> summary) {}

    /** @param executorIdle true when no rebalance execution is in progress. */
    record CruiseControlState(boolean executorIdle) {}

    /** Resolved Cruise Control REST endpoint. {@code username}/{@code password} are null
     *  unless the REST API is secured with basic auth. */
    record CruiseControlEndpoint(String baseUrl, String username, String password) {
        public boolean hasAuth() {
            return username != null && !username.isBlank();
        }
    }
}
