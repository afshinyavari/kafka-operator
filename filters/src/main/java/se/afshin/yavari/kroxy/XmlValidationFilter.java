package se.afshin.yavari.kroxy;

import io.kroxylicious.proxy.filter.FilterContext;
import io.kroxylicious.proxy.filter.ProduceRequestFilter;
import io.kroxylicious.proxy.filter.RequestFilterResult;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class XmlValidationFilter implements ProduceRequestFilter {
    private static final Logger log = LoggerFactory.getLogger(XmlValidationFilter.class);

    private final XmlSchemaStore schemaStore;

    public XmlValidationFilter(XmlSchemaStore schemaStore) {
        this.schemaStore = schemaStore;
    }

    @Override
    public CompletionStage<RequestFilterResult> onProduceRequest(
            short apiVersion,
            RequestHeaderData header,
            ProduceRequestData body,
            FilterContext context) {

        List<CompletableFuture<ValidationResult>> futures = collectValidations(body);

        if (futures.isEmpty()) {
            return context.forwardRequest(header, body);
        }

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
            .thenCompose(ignored -> {
                Optional<String> firstError = futures.stream()
                    .map(CompletableFuture::join)
                    .filter(r -> !r.isValid())
                    .map(ValidationResult::getErrorMessage)
                    .findFirst();

                if (firstError.isEmpty()) {
                    return context.forwardRequest(header, body);
                }

                String errorMsg = "XML schema validation failed: " + firstError.get();
                log.debug("Rejecting produce request — {}", errorMsg);
                return CompletableFuture.completedFuture(
                    context.requestFilterResultBuilder()
                        .shortCircuitResponse(buildErrorResponse(body, errorMsg))
                        .build()
                );
            });
    }

    private List<CompletableFuture<ValidationResult>> collectValidations(ProduceRequestData body) {
        List<CompletableFuture<ValidationResult>> futures = new ArrayList<>();

        for (ProduceRequestData.TopicProduceData topicData : body.topicData()) {
            Optional<XmlValidator> maybeValidator = schemaStore.getValidator(topicData.name());
            if (maybeValidator.isEmpty()) continue;

            XmlValidator validator = maybeValidator.get();
            for (ProduceRequestData.PartitionProduceData partition : topicData.partitionData()) {
                if (!(partition.records() instanceof MemoryRecords memRecords)) continue;

                for (var batch : memRecords.batches()) {
                    for (Record record : batch) {
                        if (!record.hasValue()) continue;
                        byte[] bytes = toBytes(record.value());
                        futures.add(CompletableFuture.supplyAsync(
                            () -> validator.validate(bytes),
                            schemaStore.validationExecutor
                        ));
                    }
                }
            }
        }

        return futures;
    }

    private ProduceResponseData buildErrorResponse(ProduceRequestData request, String errorMessage) {
        ProduceResponseData response = new ProduceResponseData();
        for (ProduceRequestData.TopicProduceData topicData : request.topicData()) {
            ProduceResponseData.TopicProduceResponse topicResponse =
                new ProduceResponseData.TopicProduceResponse().setName(topicData.name());
            for (ProduceRequestData.PartitionProduceData partition : topicData.partitionData()) {
                topicResponse.partitionResponses().add(
                    new ProduceResponseData.PartitionProduceResponse()
                        .setIndex(partition.index())
                        .setErrorCode(Errors.INVALID_RECORD.code())
                        .setErrorMessage(errorMessage)
                );
            }
            response.responses().add(topicResponse);
        }
        return response;
    }

    private static byte[] toBytes(ByteBuffer buffer) {
        ByteBuffer dup = buffer.duplicate();
        byte[] bytes = new byte[dup.remaining()];
        dup.get(bytes);
        return bytes;
    }
}
