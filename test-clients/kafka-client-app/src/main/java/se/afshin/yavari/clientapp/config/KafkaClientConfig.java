package se.afshin.yavari.clientapp.config;

import java.util.Properties;

/** {@code KAFKA_*} environment → kafka-clients {@link Properties}. */
public record KafkaClientConfig(String bootstrapServers, SecurityProtocol protocol, TlsStores tls,
                                OAuth oauth, String clientId) {

    public static final String DEFAULT_CLIENT_ID = "kafka-client-app";
    public static final String TLS_PREFIX = "KAFKA_TLS_";
    static final String OAUTH_CALLBACK_HANDLER = "io.strimzi.kafka.oauth.client.JaasClientOauthLoginCallbackHandler";
    static final String OAUTH_LOGIN_MODULE = "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule";

    public enum SecurityProtocol { PLAINTEXT, SSL, SASL_SSL }

    public record OAuth(String tokenEndpoint, String clientId, String clientSecret, String scope) {}

    public static KafkaClientConfig from(Env env, Problems problems) {
        String bootstrap = env.require("KAFKA_BOOTSTRAP_SERVERS", problems);
        SecurityProtocol protocol = env.getEnum("KAFKA_SECURITY_PROTOCOL", SecurityProtocol.class,
                SecurityProtocol.SSL, problems);
        TlsStores tls = TlsStores.resolve(env, TLS_PREFIX, problems);
        String clientId = env.get("KAFKA_CLIENT_ID", DEFAULT_CLIENT_ID);
        OAuth oauth = null;

        if (protocol != SecurityProtocol.PLAINTEXT && !tls.hasTruststore()) {
            problems.add(TLS_PREFIX + "TRUSTSTORE_PATH (or TLS_TRUSTSTORE_PATH) is required for KAFKA_SECURITY_PROTOCOL=" + protocol);
        }
        if (protocol == SecurityProtocol.SASL_SSL) {
            oauth = new OAuth(
                    env.require("KAFKA_OAUTH_TOKEN_ENDPOINT", problems),
                    env.require("KAFKA_OAUTH_CLIENT_ID", problems),
                    env.require("KAFKA_OAUTH_CLIENT_SECRET", problems),
                    env.get("KAFKA_OAUTH_SCOPE").orElse(null));
        }
        return new KafkaClientConfig(bootstrap, protocol, tls, oauth, clientId);
    }

    /** Base properties for a producer or consumer; {@code clientIdSuffix} is e.g. {@code -producer}. */
    public Properties toProperties(String clientIdSuffix) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrapServers);
        p.put("client.id", clientId + clientIdSuffix);
        p.put("security.protocol", protocol.name());
        switch (protocol) {
            case PLAINTEXT -> { }
            case SSL -> {
                putTruststore(p);
                if (tls.hasKeystore()) {
                    putIfSet(p, "ssl.keystore.location", tls.keystorePath());
                    putIfSet(p, "ssl.keystore.password", tls.keystorePassword());
                    putIfSet(p, "ssl.keystore.type", tls.keystoreType());
                }
            }
            case SASL_SSL -> {
                putTruststore(p);
                p.put("sasl.mechanism", "OAUTHBEARER");
                p.put("sasl.login.callback.handler.class", OAUTH_CALLBACK_HANDLER);
                p.put("sasl.jaas.config", jaasConfig());
            }
        }
        return p;
    }

    private void putTruststore(Properties p) {
        putIfSet(p, "ssl.truststore.location", tls.truststorePath());
        putIfSet(p, "ssl.truststore.password", tls.truststorePassword());
        putIfSet(p, "ssl.truststore.type", tls.truststoreType());
    }

    private static void putIfSet(Properties p, String key, String value) {
        if (value != null) p.put(key, value);
    }

    private String jaasConfig() {
        StringBuilder sb = new StringBuilder(OAUTH_LOGIN_MODULE).append(" required");
        option(sb, "oauth.token.endpoint.uri", oauth.tokenEndpoint());
        option(sb, "oauth.client.id", oauth.clientId());
        option(sb, "oauth.client.secret", oauth.clientSecret());
        option(sb, "oauth.scope", oauth.scope());
        option(sb, "oauth.ssl.truststore.location", tls.truststorePath());
        option(sb, "oauth.ssl.truststore.password", tls.truststorePassword());
        option(sb, "oauth.ssl.truststore.type", tls.truststoreType());
        return sb.append(';').toString();
    }

    private static void option(StringBuilder sb, String key, String value) {
        if (value == null) return;
        sb.append(' ').append(key).append("=\"")
          .append(value.replace("\\", "\\\\").replace("\"", "\\\""))
          .append('"');
    }
}
