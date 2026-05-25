package se.afshin.yavari.kafka.editor.admin;

import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;

/**
 * Renders {@link AdminApiException}s as JSON {@link ApiError} responses. Scoped
 * to that one exception type, so run/registry endpoints keep their existing
 * error behaviour.
 */
@Provider
public class AdminExceptionMapper implements ExceptionMapper<AdminApiException> {

    @Override
    public Response toResponse(AdminApiException e) {
        return Response.status(e.status())
                .type(MediaType.APPLICATION_JSON)
                .entity(new ApiError(e.getMessage(), null, e.kind()))
                .build();
    }
}
