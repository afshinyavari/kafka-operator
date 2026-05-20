package se.afshin.yavari.kafka.ui.web;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.TemplateInstance;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import org.apache.kafka.common.errors.AuthorizationException;
import org.jboss.logging.Logger;

/**
 * Translates a Kafka {@link AuthorizationException} (typically raised by
 * {@code AdminClient} when the proxy denies an op) into a 403 HTML fragment
 * htmx can swap into the page. Other errors fall through to Quarkus's default
 * exception handling.
 */
@Provider
public class AuthorizationExceptionMapper implements ExceptionMapper<AuthorizationException> {

    private static final Logger LOG = Logger.getLogger(AuthorizationExceptionMapper.class);

    @CheckedTemplate
    static class Templates {
        public static native TemplateInstance forbidden(String message);
    }

    @Override
    public Response toResponse(AuthorizationException ex) {
        LOG.debugf(ex, "Forwarding authorization denial as 403");
        return Response.status(Response.Status.FORBIDDEN)
                .type(MediaType.TEXT_HTML)
                .entity(Templates.forbidden(ex.getMessage()).render())
                .build();
    }
}
