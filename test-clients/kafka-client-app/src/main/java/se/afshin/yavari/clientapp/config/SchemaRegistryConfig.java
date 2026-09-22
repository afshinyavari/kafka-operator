package se.afshin.yavari.clientapp.config;

import java.util.Locale;

/**
 * {@code <PREFIX>_SCHEMA_*} environment for one side (producer or consumer).
 * {@code prefix} is {@code PRODUCER_SCHEMA_} or {@code CONSUMER_SCHEMA_}.
 */
public record SchemaRegistryConfig(RegistryType type, String url, AuthMode auth, TlsStores tls,
                                   Oidc oidc, boolean autoRegister, String group) {

    public enum RegistryType { APICURIO, CONFLUENT }
    public enum AuthMode { NONE, MTLS, OIDC }
    public record Oidc(String clientId, String clientSecret, String tokenEndpoint, String scope) {}

    public static final String DEFAULT_GROUP = "default";

    public static SchemaRegistryConfig from(Env env, String prefix, Problems problems) {
        RegistryType type = env.getEnum(prefix + "REGISTRY_TYPE", RegistryType.class, RegistryType.APICURIO, problems);
        String url = env.require(prefix + "URL", problems);
        AuthMode auth = env.getEnum(prefix + "AUTH", AuthMode.class, AuthMode.NONE, problems);
        TlsStores tls = TlsStores.resolve(env, prefix + "TLS_", problems);
        boolean autoRegister = env.getBoolean(prefix + "AUTO_REGISTER", true, problems);
        String group = env.get(prefix + "GROUP", DEFAULT_GROUP);
        Oidc oidc = null;

        if (auth == AuthMode.MTLS && !tls.hasKeystore()) {
            problems.add(prefix + "TLS_KEYSTORE_PATH (or TLS_KEYSTORE_PATH) is required for " + prefix + "AUTH=mtls");
        }
        if (auth == AuthMode.OIDC) {
            if (type == RegistryType.CONFLUENT) {
                problems.add(prefix + "AUTH=oidc is not supported for " + prefix + "REGISTRY_TYPE=confluent (use none or mtls)");
            }
            oidc = new Oidc(
                    env.require(prefix + "CLIENT_ID", problems),
                    env.require(prefix + "CLIENT_SECRET", problems),
                    env.require(prefix + "TOKEN_ENDPOINT", problems),
                    env.get(prefix + "SCOPE").orElse(null));
        }
        return new SchemaRegistryConfig(type, url, auth, tls, oidc, autoRegister, group);
    }

    public boolean https() {
        return url != null && url.toLowerCase(Locale.ROOT).startsWith("https://");
    }
}
