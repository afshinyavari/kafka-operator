package se.afshin.yavari.kafka.operator.crd;

/** One task slot inside a connector. Trace is truncated to ~2 KiB on write to keep the
 *  status object inside the etcd budget when many tasks fail simultaneously. */
public class KafkaConnectorTaskStatus {

    private Integer id;
    private String state;
    private String workerId;
    private String trace;

    public KafkaConnectorTaskStatus() {}

    public KafkaConnectorTaskStatus(Integer id, String state, String workerId, String trace) {
        this.id = id;
        this.state = state;
        this.workerId = workerId;
        this.trace = trace;
    }

    public Integer getId() { return id; }
    public void setId(Integer id) { this.id = id; }

    public String getState() { return state; }
    public void setState(String state) { this.state = state; }

    public String getWorkerId() { return workerId; }
    public void setWorkerId(String workerId) { this.workerId = workerId; }

    public String getTrace() { return trace; }
    public void setTrace(String trace) { this.trace = trace; }
}
