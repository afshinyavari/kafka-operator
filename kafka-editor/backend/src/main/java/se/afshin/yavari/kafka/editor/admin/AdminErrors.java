package se.afshin.yavari.kafka.editor.admin;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidRequestException;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

/**
 * Awaits Kafka {@link KafkaFuture}s and translates their failures into
 * {@link AdminApiException}s with a sensible HTTP status. Centralising this
 * keeps every admin service free of repetitive {@code try/get/catch} blocks.
 */
public final class AdminErrors {

    private static final long AWAIT_TIMEOUT_S = 20;

    private AdminErrors() {
    }

    /** Block on a Kafka future, translating any failure. */
    public static <T> T await(KafkaFuture<T> future) {
        try {
            return future.get(AWAIT_TIMEOUT_S, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            throw translate(e.getCause() != null ? e.getCause() : e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AdminApiException(500, "INTERNAL", "The operation was interrupted.");
        } catch (TimeoutException e) {
            throw new AdminApiException(504, "TIMEOUT",
                    "The broker did not respond in time. Is it reachable?");
        }
    }

    /** Map a Kafka/runtime throwable to an {@link AdminApiException}. */
    public static AdminApiException translate(Throwable t) {
        if (t instanceof AdminApiException already) {
            return already;
        }
        String msg = t.getMessage() != null ? t.getMessage() : t.toString();
        if (t instanceof AuthorizationException) {
            return new AdminApiException(403, "AUTH", msg);
        }
        if (t instanceof TopicExistsException) {
            return new AdminApiException(409, "CONFLICT", msg);
        }
        if (t instanceof UnknownTopicOrPartitionException
                || t instanceof GroupIdNotFoundException
                || t instanceof UnknownMemberIdException) {
            return new AdminApiException(404, "NOT_FOUND", msg);
        }
        if (t instanceof org.apache.kafka.common.errors.TimeoutException) {
            return new AdminApiException(504, "TIMEOUT",
                    "The broker did not respond in time. Is it reachable?");
        }
        if (t instanceof SecurityDisabledException) {
            return new AdminApiException(400, "UNSUPPORTED", msg);
        }
        if (t instanceof InvalidConfigurationException
                || t instanceof InvalidRequestException
                || t instanceof InvalidPartitionsException
                || t instanceof IllegalArgumentException) {
            return new AdminApiException(400, "BAD_REQUEST", msg);
        }
        return new AdminApiException(500, "INTERNAL", msg);
    }
}
