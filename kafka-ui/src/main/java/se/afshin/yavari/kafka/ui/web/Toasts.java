package se.afshin.yavari.kafka.ui.web;

import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

/**
 * Renders an htmx out-of-band Bootstrap toast and assembles JAX-RS responses
 * that combine a body fragment with one or more toast notifications.
 *
 * Conventions used by {@code app.js}:
 * <ul>
 *   <li>{@code HX-Trigger: closeModal} — closes any visible Bootstrap modal.</li>
 *   <li>{@code HX-Reswap: none} — used on error responses so the form keeps its
 *       content and only the toast lands.</li>
 *   <li>{@code HX-Push-Url} — updates the browser URL when the response body
 *       belongs to a different page (e.g. after a delete).</li>
 * </ul>
 */
@ApplicationScoped
public class Toasts {

    @Inject @Location("_toast_oob.html") Template toastOob;

    public String oob(String level, String message) {
        return toastOob.data("level", level, "message", message).render();
    }

    public Response success(String bodyFragment, String message) {
        return Response.ok(bodyFragment + oob("success", message), MediaType.TEXT_HTML)
                .header("HX-Trigger", "closeModal")
                .build();
    }

    /**
     * Successful action that lands the user on a different URL (e.g. delete
     * navigates back to the parent list).
     */
    public Response successPushUrl(String bodyFragment, String message, String pushUrl) {
        return Response.ok(bodyFragment + oob("success", message), MediaType.TEXT_HTML)
                .header("HX-Trigger", "closeModal")
                .header("HX-Push-Url", pushUrl)
                .build();
    }

    /** Only a toast, no body change. Used by Produce so the modal stays open. */
    public Response toastOnly(String level, String message) {
        return Response.ok(oob(level, message), MediaType.TEXT_HTML)
                .header("HX-Reswap", "none")
                .build();
    }

    public Response error(String message) {
        return toastOnly("danger", message);
    }
}
