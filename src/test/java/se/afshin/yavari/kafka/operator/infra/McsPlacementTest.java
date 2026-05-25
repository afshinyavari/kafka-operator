package se.afshin.yavari.kafka.operator.infra;

import org.junit.jupiter.api.Test;
import se.afshin.yavari.kafka.operator.crd.McsConfig;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class McsPlacementTest {

    private static McsConfig enabledMcs() {
        McsConfig cfg = new McsConfig();
        cfg.setEnabled(true);
        return cfg;
    }

    @Test
    void proceedWhenMcsDisabledAndNoTargets() {
        assertEquals(McsPlacement.Decision.PROCEED,
                McsPlacement.decide(null, null, "A"));
    }

    @Test
    void proceedWhenMcsEnabledAndLocalIsTarget() {
        assertEquals(McsPlacement.Decision.PROCEED,
                McsPlacement.decide(enabledMcs(), List.of("A", "B"), "A"));
    }

    @Test
    void skipWhenMcsEnabledAndLocalNotInTargets() {
        assertEquals(McsPlacement.Decision.SKIP,
                McsPlacement.decide(enabledMcs(), List.of("B", "C"), "A"));
    }

    @Test
    void invalidWhenTargetsSetButMcsDisabled() {
        assertEquals(McsPlacement.Decision.INVALID_TARGETS_WITHOUT_MCS,
                McsPlacement.decide(null, List.of("A"), "A"));
    }

    @Test
    void invalidWhenMcsEnabledWithoutTargets() {
        assertEquals(McsPlacement.Decision.INVALID_MCS_WITHOUT_TARGETS,
                McsPlacement.decide(enabledMcs(), null, "A"));
        assertEquals(McsPlacement.Decision.INVALID_MCS_WITHOUT_TARGETS,
                McsPlacement.decide(enabledMcs(), List.of(), "A"));
    }
}
