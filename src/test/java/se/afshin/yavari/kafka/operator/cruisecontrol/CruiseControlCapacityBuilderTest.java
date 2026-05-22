package se.afshin.yavari.kafka.operator.cruisecontrol;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.CruiseControlCapacityConfig;

import static org.assertj.core.api.Assertions.assertThat;

class CruiseControlCapacityBuilderTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final CruiseControlCapacityBuilder builder = new CruiseControlCapacityBuilder();

    @Test
    void emitsWildcardDefaultEntryWithDefaults() throws Exception {
        JsonNode entry = MAPPER.readTree(builder.build(null))
                .get("brokerCapacities").get(0);

        assertThat(entry.get("brokerId").asText()).isEqualTo("-1");
        JsonNode cap = entry.get("capacity");
        assertThat(cap.get("DISK").asText()).isEqualTo("100000");
        assertThat(cap.get("CPU").asText()).isEqualTo("100");
        assertThat(cap.get("NW_IN").asText()).isEqualTo("100000");
        assertThat(cap.get("NW_OUT").asText()).isEqualTo("100000");
    }

    @Test
    void appliesConfiguredOverrides() throws Exception {
        CruiseControlCapacityConfig cap = new CruiseControlCapacityConfig();
        cap.setDisk("500000");
        cap.setCpu(8.0);
        cap.setInboundNetwork("250000");

        JsonNode capacity = MAPPER.readTree(builder.build(cap))
                .get("brokerCapacities").get(0).get("capacity");

        assertThat(capacity.get("DISK").asText()).isEqualTo("500000");
        assertThat(capacity.get("CPU").asText()).isEqualTo("8");
        assertThat(capacity.get("NW_IN").asText()).isEqualTo("250000");
        // Unset field keeps its default.
        assertThat(capacity.get("NW_OUT").asText()).isEqualTo("100000");
    }

    @Test
    void trimNumberDropsTrailingZero() {
        assertThat(CruiseControlCapacityBuilder.trimNumber(4.0)).isEqualTo("4");
        assertThat(CruiseControlCapacityBuilder.trimNumber(2.5)).isEqualTo("2.5");
    }
}
