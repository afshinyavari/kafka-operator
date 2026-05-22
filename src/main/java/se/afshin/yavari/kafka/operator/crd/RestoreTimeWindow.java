package se.afshin.yavari.kafka.operator.crd;

import io.fabric8.generator.annotation.ValidationRule;

/** Optional point-in-time-recovery window. Both bounds are epoch milliseconds and map
 *  to the osodevops restore config {@code restore.time_window_start}/{@code _end}. */
@ValidationRule(
        value = "!has(self.startMillis) || !has(self.endMillis) || self.startMillis <= self.endMillis",
        message = "timeWindow.startMillis must be <= timeWindow.endMillis"
)
public class RestoreTimeWindow {

    private Long startMillis;
    private Long endMillis;

    public Long getStartMillis() { return startMillis; }
    public void setStartMillis(Long startMillis) { this.startMillis = startMillis; }

    public Long getEndMillis() { return endMillis; }
    public void setEndMillis(Long endMillis) { this.endMillis = endMillis; }
}
