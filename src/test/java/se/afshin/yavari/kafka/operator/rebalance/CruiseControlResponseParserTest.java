package se.afshin.yavari.kafka.operator.rebalance;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CruiseControlResponseParserTest {

    @Test
    void parseSummaryFlattensScalarFields() {
        String body = """
                {
                  "summary": {
                    "numReplicaMovements": 12,
                    "dataToMoveMB": 480,
                    "monitoredPartitionsPercentage": 100.0,
                    "onDemandBalancednessScoreBefore": 78.5,
                    "onDemandBalancednessScoreAfter": 95.2,
                    "excludedTopics": []
                  },
                  "proposals": [{"topic": "orders"}],
                  "version": 1
                }
                """;
        Map<String, String> summary = CruiseControlResponseParser.parseSummary(body);

        assertThat(summary).containsEntry("numReplicaMovements", "12");
        assertThat(summary).containsEntry("dataToMoveMB", "480");
        assertThat(summary).containsEntry("onDemandBalancednessScoreAfter", "95.2");
        // Non-scalar fields (arrays/objects) are dropped.
        assertThat(summary).doesNotContainKey("excludedTopics");
        assertThat(summary).doesNotContainKey("proposals");
    }

    @Test
    void parseSummaryReturnsEmptyWhenNoSummary() {
        assertThat(CruiseControlResponseParser.parseSummary("{\"progress\":[]}")).isEmpty();
        assertThat(CruiseControlResponseParser.parseSummary("")).isEmpty();
        assertThat(CruiseControlResponseParser.parseSummary("not json")).isEmpty();
    }

    @Test
    void parseExecutorIdleTrueWhenNoTaskInProgress() {
        assertThat(CruiseControlResponseParser.parseExecutorIdle(
                "{\"ExecutorState\":{\"state\":\"NO_TASK_IN_PROGRESS\"}}")).isTrue();
    }

    @Test
    void parseExecutorIdleFalseWhenTaskRunning() {
        assertThat(CruiseControlResponseParser.parseExecutorIdle(
                "{\"ExecutorState\":{\"state\":\"INTER_BROKER_REPLICA_MOVEMENT_TASK_IN_PROGRESS\"}}"))
                .isFalse();
    }

    @Test
    void parseExecutorIdleTrueWhenSubstateMissing() {
        assertThat(CruiseControlResponseParser.parseExecutorIdle("{\"MonitorState\":{}}")).isTrue();
    }

    @Test
    void parseErrorMessageExtractsErrorField() {
        String msg = CruiseControlResponseParser.parseErrorMessage(
                "{\"errorMessage\":\"Not enough valid windows\"}", 500);
        assertThat(msg).isEqualTo("Not enough valid windows");
    }

    @Test
    void parseErrorMessageFallsBackToStatusCode() {
        assertThat(CruiseControlResponseParser.parseErrorMessage("", 503)).isEqualTo("HTTP 503");
    }
}
