package se.afshin.yavari.kroxy.auth.oauth;

import io.kroxylicious.proxy.authentication.SaslSubjectBuilder;
import io.kroxylicious.proxy.authentication.SaslSubjectBuilderService;
import io.kroxylicious.proxy.plugin.Plugin;

@Plugin(configType = JwtGroupSaslSubjectBuilderConfig.class)
public class JwtGroupSaslSubjectBuilderService implements SaslSubjectBuilderService<JwtGroupSaslSubjectBuilderConfig> {

    @Override
    public void initialize(JwtGroupSaslSubjectBuilderConfig config) {}

    @Override
    public SaslSubjectBuilder build() {
        return new JwtGroupSaslSubjectBuilder();
    }
}
