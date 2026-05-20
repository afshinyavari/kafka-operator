package se.afshin.yavari.kafka.ui.messages;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import io.quarkus.security.Authenticated;
import jakarta.inject.Inject;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import se.afshin.yavari.kafka.ui.cluster.ClusterCoordinates;
import se.afshin.yavari.kafka.ui.cluster.ClusterRegistry;
import se.afshin.yavari.kafka.ui.messages.MessageBrowserService.Page;
import se.afshin.yavari.kafka.ui.messages.MessageBrowserService.SeekMode;
import se.afshin.yavari.kafka.ui.messages.MessageBrowserService.SeekSpec;

import java.util.List;

@Path("/clusters/{id}/topics/{name}/messages")
@Authenticated
public class MessageBrowserResource {

    @Inject ClusterRegistry registry;
    @Inject MessageBrowserService browser;
    @Inject se.afshin.yavari.kafka.ui.web.UserContext user;

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance page(ClusterCoordinates cluster, String topic,
                                                   List<Integer> partitions, Page page,
                                                   String seek, int partition, long value, int latestN,
                                                   String username);

        public static native TemplateInstance rows(Page page);
    }

    @GET
    @Produces(MediaType.TEXT_HTML)
    public TemplateInstance page(@PathParam("id") String id,
                                 @PathParam("name") String name,
                                 @QueryParam("partition") @DefaultValue("0") int partition,
                                 @QueryParam("seek") @DefaultValue("LATEST") String seek,
                                 @QueryParam("offset") @DefaultValue("0") long offset,
                                 @QueryParam("timestamp") @DefaultValue("0") long timestamp,
                                 @QueryParam("latestN") @DefaultValue("50") int latestN,
                                 @QueryParam("size") @DefaultValue("50") int size,
                                 @QueryParam("fragment") @DefaultValue("false") boolean fragment) {
        ClusterCoordinates c = registry.byId(id)
                .orElseThrow(() -> new NotFoundException("Unknown cluster: " + id));

        SeekMode mode = parseMode(seek);
        long seekValue = switch (mode) {
            case OFFSET -> offset;
            case TIMESTAMP -> timestamp;
            case LATEST -> 0L;
        };
        SeekSpec spec = new SeekSpec(mode, partition, seekValue, latestN);
        List<Integer> partitions = browser.partitionsOf(id, name);
        Page p = browser.page(id, name, spec, size);
        if (fragment) {
            return Templates.rows(p);
        }
        return Templates.page(c, name, partitions, p, mode.name(), partition, seekValue, latestN, user.username());
    }

    private static SeekMode parseMode(String s) {
        try { return SeekMode.valueOf(s.toUpperCase()); }
        catch (Exception e) { return SeekMode.LATEST; }
    }
}
