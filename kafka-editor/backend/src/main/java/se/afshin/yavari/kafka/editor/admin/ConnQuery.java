package se.afshin.yavari.kafka.editor.admin;

import jakarta.ws.rs.QueryParam;

import se.afshin.yavari.kafka.editor.api.ConnectionConfig;

/**
 * Connection details carried on GET requests as query parameters. GET / SSE
 * endpoints cannot use a request body, so {@code ?bootstrap=&registry=&connect=}
 * is the uniform transport; write endpoints carry the same shape in their JSON
 * body. Used as a JAX-RS {@code @BeanParam}.
 */
public class ConnQuery {

    @QueryParam("bootstrap")
    public String bootstrap;

    @QueryParam("registry")
    public String registry;

    @QueryParam("connect")
    public String connect;

    public ConnectionConfig toConfig() {
        return new ConnectionConfig(bootstrap, registry, connect);
    }
}
