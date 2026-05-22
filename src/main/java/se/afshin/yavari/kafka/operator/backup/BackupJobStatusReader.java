package se.afshin.yavari.kafka.operator.backup;

import io.fabric8.kubernetes.api.model.batch.v1.CronJob;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.KubernetesClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import se.afshin.yavari.kafka.operator.crd.KafkaBackupStatus;

import java.util.Comparator;
import java.util.List;

/** Reads {@code CronJob}/{@code Job} status back into a CR's status. */
@ApplicationScoped
public class BackupJobStatusReader {

    @Inject KubernetesClient client;

    /** Populates last-run fields of a {@link KafkaBackupStatus} from the CronJob and its
     *  most recent child Job. */
    public void populateBackup(KafkaBackupStatus status, String name, String namespace) {
        CronJob cronJob = client.batch().v1().cronjobs()
                .inNamespace(namespace).withName(name).get();
        if (cronJob != null && cronJob.getStatus() != null) {
            status.setLastScheduleTime(cronJob.getStatus().getLastScheduleTime());
            if (cronJob.getStatus().getLastSuccessfulTime() != null) {
                status.setLastSuccessfulBackupTime(cronJob.getStatus().getLastSuccessfulTime());
            }
            status.setActiveBackupCount(cronJob.getStatus().getActive() != null
                    ? cronJob.getStatus().getActive().size() : 0);
        }
        List<Job> jobs = client.batch().v1().jobs().inNamespace(namespace)
                .withLabel("app", "kafka-backup")
                .withLabel("app.instance", name)
                .list().getItems();
        jobs.stream()
                .max(Comparator.comparing(j -> j.getMetadata().getCreationTimestamp() != null
                        ? j.getMetadata().getCreationTimestamp() : ""))
                .ifPresent(latest -> {
                    status.setLastJobName(latest.getMetadata().getName());
                    status.setLastJobResult(result(latest));
                });
    }

    /** Maps a Job's status to a coarse result. */
    public static KafkaBackupStatus.JobResult result(Job job) {
        if (succeeded(job)) {
            return KafkaBackupStatus.JobResult.SUCCEEDED;
        }
        if (failed(job)) {
            return KafkaBackupStatus.JobResult.FAILED;
        }
        if (job != null && job.getStatus() != null
                && job.getStatus().getActive() != null && job.getStatus().getActive() >= 1) {
            return KafkaBackupStatus.JobResult.RUNNING;
        }
        return KafkaBackupStatus.JobResult.UNKNOWN;
    }

    /** True once a Job has at least one successful pod completion. */
    public static boolean succeeded(Job job) {
        return job != null && job.getStatus() != null
                && job.getStatus().getSucceeded() != null
                && job.getStatus().getSucceeded() >= 1;
    }

    /** True once a Job carries a terminal {@code Failed} condition (backoff exhausted or
     *  deadline exceeded) — distinct from a transient pod failure that may still retry. */
    public static boolean failed(Job job) {
        if (job == null || job.getStatus() == null || job.getStatus().getConditions() == null) {
            return false;
        }
        return job.getStatus().getConditions().stream()
                .anyMatch(c -> "Failed".equals(c.getType()) && "True".equals(c.getStatus()));
    }
}
