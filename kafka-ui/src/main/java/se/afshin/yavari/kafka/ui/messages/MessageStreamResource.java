package se.afshin.yavari.kafka.ui.messages;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import io.quarkus.security.Authenticated;
import io.smallrye.common.annotation.Blocking;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import org.jboss.resteasy.reactive.RestStreamElementType;
import se.afshin.yavari.kafka.ui.cluster.ClusterRegistry;
import se.afshin.yavari.kafka.ui.messages.MessageBrowserService.RenderedRecord;

import java.util.ArrayList;
import java.util.List;

/**
 * SSE live-tail of a topic. Emits one HTML row fragment per new record so
 * htmx's sse extension can swap it into the browser table.
 */
@Path("/clusters/{id}/topics/{name}/messages/stream")
@Authenticated
public class MessageStreamResource {

    @Inject ClusterRegistry registry;
    @Inject MessageBrowserService browser;

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance row(RenderedRecord row);
    }

    @GET
    @Produces(MediaType.SERVER_SENT_EVENTS)
    @RestStreamElementType(MediaType.TEXT_HTML)
    @Blocking
    public Multi<String> stream(@PathParam("id") String id,
                                @PathParam("name") String name,
                                @QueryParam("partition") String partitionParam) {
        registry.byId(id).orElseThrow(() -> new NotFoundException("Unknown cluster: " + id));
        List<Integer> parts = parsePartitions(partitionParam);
        return browser.tail(id, name, parts)
                .map(r -> Templates.row(r).render());
    }

    private static List<Integer> parsePartitions(String s) {
        List<Integer> out = new ArrayList<>();
        if (s == null || s.isBlank()) return out;
        for (String x : s.split(",")) {
            try { out.add(Integer.parseInt(x.trim())); } catch (NumberFormatException ignored) {}
        }
        return out;
    }
}
